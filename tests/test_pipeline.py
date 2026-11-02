import json, os
import pytest
import backfill, build_live, collect
from mxq import archive, gate, state, store, validate
from mxq.config import gate_config
from conftest import FakeClient, kl, kl_csv, kls, ms, run_backfill, seed_archive

MIN = 60_000
BTC = "BTCUSDT"


def manifest(w):
    return store.Manifest.load(w.remote, w.wd)


def rd(w, name):
    return store.read(w.remote.fetch(name, str(w.tmp / "chk")))


# ---------------- backfill ----------------
def test_backfill_happy_path(world):
    seed_archive(world)
    run_backfill(world)
    man = manifest(world)
    f = man.files["klines_1m_BTCUSDT_2024-01.parquet"]
    assert f["bulk_verified"] and f["rows"] == 351 and f["missing"] == 0
    assert man.files["mark_1m_BTCUSDT_2024-01.parquet"]["rows"] == 351
    assert man.files["funding_BTCUSDT.parquet"]["rows"] == 1
    m = man.files["metrics_BTCUSDT_2024-01.parquet"]
    assert m["rows"] == 24 and len(m["missing_days"]) == 30       # 30 days had no archive file -> recorded
    assert man.latest("klines_1m", BTC) == ms(2024, 1, 31, 23, 50)


def test_backfill_is_resumable_and_idempotent(world, monkeypatch):
    seed_archive(world)
    run_backfill(world)
    sid = manifest(world).snapshot_id()
    calls = []
    orig = world.arch.get
    monkeypatch.setattr("mxq.http.get", lambda u, *a, **k: (calls.append(u), orig(u))[1])
    run_backfill(world)
    assert manifest(world).snapshot_id() == sid
    assert not [u for u in calls if "klines" in u or "markPrice" in u]   # verified months not re-downloaded


def test_bad_checksum_aborts_and_stores_nothing(world):
    seed_archive(world)
    world.arch.add(archive.url("klines", BTC, "2024-01"), kl_csv(kls(ms(2024, 1, 31, 18), 351)), bad=True)
    with pytest.raises(archive.ChecksumError):
        run_backfill(world)
    assert "klines_1m_BTCUSDT_2024-01.parquet" not in world.remote.names()
    assert "klines_1m_BTCUSDT_2024-01.parquet" not in manifest(world).files


def test_latest_month_may_be_pending_but_older_month_404_is_fatal(world):
    seed_archive(world)
    with pytest.raises(RuntimeError, match="archive 404"):
        run_backfill(world, start=(2023, 12))                 # Dec has no file and is not the latest month -> fatal


def test_pending_latest_month_is_tolerated(world):
    seed_archive(world)
    world.arch.drop(archive.url("klines", BTC, "2024-01"))
    world.arch.drop(archive.url("markPriceKlines", BTC, "2024-01"))
    run_backfill(world)
    assert "klines_1m_BTCUSDT_2024-01.parquet" not in manifest(world).files


def test_bad_ohlc_in_archive_is_rejected(world):
    seed_archive(world)
    a = kls(ms(2024, 1, 31, 18), 351)
    a[10][2] = "1.0"
    world.arch.add(archive.url("klines", BTC, "2024-01"), kl_csv(a))
    with pytest.raises(RuntimeError, match="bad OHLC"):
        run_backfill(world)


# ---------------- collect ----------------
def test_collect_requires_backfill_first(world):
    errs = collect.collect(world.remote, world.wd, FakeClient(world.NOW), symbols=[BTC], log=lambda *a: None)
    assert any("run backfill first" in e for e in errs)
    assert state.load(world.remote, world.wd)["consecutive_failures"] == 1


def test_collect_merges_across_month_boundary_without_dups_or_gaps(world):
    seed_archive(world)
    run_backfill(world)
    client = FakeClient(world.NOW)
    assert collect.collect(world.remote, world.wd, client, symbols=[BTC], log=lambda *a: None) == []
    jan, feb = rd(world, "klines_1m_BTCUSDT_2024-01.parquet"), rd(world, "klines_1m_BTCUSDT_2024-02.parquet")
    assert jan.num_rows == 360 and feb.num_rows == 10                  # 18:00..23:59 and 00:00..00:09
    assert jan["source"].to_pylist().count("bulk") == 351 and jan["source"].to_pylist().count("api") == 9
    assert feb["open_time"].to_pylist()[-1] == ms(2024, 2, 1, 0, 9)      # unclosed 00:10 candle excluded
    for t in (jan, feb):
        assert validate.continuity(t, "open_time", MIN)["dups"] == 0
    assert validate.continuity(jan, "open_time", MIN)["missing"] == 0
    assert rd(world, "mark_1m_BTCUSDT_2024-02.parquet").num_rows == 10
    assert rd(world, "funding_BTCUSDT.parquet").num_rows == 2           # bulk 16:00 + api 00:00
    met = rd(world, "metrics_BTCUSDT_2024-02.parquet")
    assert met.num_rows == 2 and max(met["ts"].to_pylist()) <= world.NOW - 300_000   # partial bucket never stored
    st = state.load(world.remote, world.wd)
    assert st["consecutive_failures"] == 0 and st["last_ok"] == world.NOW


def test_collect_is_idempotent_and_incremental(world):
    seed_archive(world)
    run_backfill(world)
    collect.collect(world.remote, world.wd, FakeClient(world.NOW), symbols=[BTC], log=lambda *a: None)
    sid = manifest(world).snapshot_id()
    collect.collect(world.remote, world.wd, FakeClient(world.NOW), symbols=[BTC], log=lambda *a: None)
    assert manifest(world).snapshot_id() == sid                         # same inputs -> identical data
    collect.collect(world.remote, world.wd, FakeClient(world.NOW + 5 * MIN), symbols=[BTC], log=lambda *a: None)
    assert rd(world, "klines_1m_BTCUSDT_2024-02.parquet").num_rows == 15


def test_api_failure_is_recorded_and_corrupts_nothing(world):
    seed_archive(world)
    run_backfill(world)
    before = manifest(world).snapshot_id()
    for _ in range(2):
        errs = collect.collect(world.remote, world.wd, FakeClient(world.NOW, fail="HTTP 451 restricted location"),
                               symbols=[BTC], log=lambda *a: None)
        assert len(errs) == 4 and all("451" in e for e in errs)
    assert manifest(world).snapshot_id() == before
    st = state.load(world.remote, world.wd)
    assert st["consecutive_failures"] == 2 and "451" in st["last_error"]


def test_connectivity_failure_in_main_exits_nonzero_and_records(world, monkeypatch):
    class Dead(FakeClient):
        def sync_time(self):
            raise RuntimeError("HTTP 451")
    monkeypatch.setattr(collect, "Client", lambda: Dead(world.NOW))
    monkeypatch.setattr(collect.remote_mod, "from_env", lambda: world.remote)
    monkeypatch.setenv("WORKDIR", world.wd)
    with pytest.raises(SystemExit) as e:
        collect.main()
    assert e.value.code == 1
    assert state.load(world.remote, world.wd)["consecutive_failures"] == 1


def test_late_bulk_file_reconciles_with_api_data(world):
    """Collector fills a month from the API; when the bulk file appears it must win and be cross-checked."""
    world.arch.add(archive.url("klines", BTC, "2023-12"), kl_csv(kls(ms(2023, 12, 31, 18), 351)))
    world.arch.add(archive.url("markPriceKlines", BTC, "2023-12"), kl_csv(kls(ms(2023, 12, 31, 18), 351)))
    world.arch.add(archive.url("fundingRate", BTC, "2023-12"),
                   "calc_time,funding_interval_hours,last_funding_rate\n" + f"{ms(2023, 12, 31, 16)},8,0.0001\n")
    run_backfill(world, start=(2023, 12))                                # Jan bulk not published yet
    collect.collect(world.remote, world.wd, FakeClient(world.NOW), symbols=[BTC], log=lambda *a: None)
    jan = rd(world, "klines_1m_BTCUSDT_2024-01.parquet")
    assert jan.num_rows == 44_640 and set(jan["source"].to_pylist()) == {"api"}
    bulk = kls(ms(2024, 1, 1), 44_640)
    tampered = round(float(bulk[100][1]) + 0.3, 6)
    bulk[100][4] = str(tampered)                                         # one disagreeing (but valid) close
    world.arch.add(archive.url("klines", BTC, "2024-01"), kl_csv(bulk))
    world.arch.add(archive.url("markPriceKlines", BTC, "2024-01"), kl_csv(kls(ms(2024, 1, 1), 44_640)))
    run_backfill(world, start=(2023, 12))
    f = manifest(world).files["klines_1m_BTCUSDT_2024-01.parquet"]
    assert f["bulk_verified"] and f["reconcile"] == {"overlap": 44_640, "close_mismatch": 1, "api_only": 0, "bulk_only": 0}
    jan = rd(world, "klines_1m_BTCUSDT_2024-01.parquet")
    assert set(jan["source"].to_pylist()) == {"bulk"} and tampered in jan["close"].to_pylist()


# ---------------- build_live + gate + alerts ----------------
def ready(world, client=None):
    seed_archive(world)
    run_backfill(world)
    collect.collect(world.remote, world.wd, client or FakeClient(world.NOW), symbols=[BTC], log=lambda *a: None)


def cfg1():
    c = gate_config()
    c["symbols"] = [BTC]
    return c


def test_build_live_outputs_and_gate_open_when_fresh(world):
    ready(world)
    out = str(world.tmp / "live")
    g, alert = build_live.build(world.remote, world.wd, out, world.NOW, cfg1(), [BTC])
    assert g == {"signals_allowed": True, "reasons": []} and alert is None
    status = json.load(open(f"{out}/status.json"))
    assert status["schema"] == 1 and len(status["snapshot_id"]) == 16
    assert set(status["series"]) == {f"{d}:{BTC}" for d in ("klines_1m", "mark_1m", "funding", "metrics")}
    chart = json.load(open(f"{out}/chart_{BTC}.json"))
    assert set(chart["tf"]) == {"1m", "5m", "15m", "1h", "4h", "1d"}
    last1m = chart["tf"]["1m"][-1]
    assert last1m[0] == ms(2024, 2, 1, 0, 9) and len(last1m) == 6
    assert chart["tf"]["1h"][-1][0] == ms(2024, 1, 31, 23)               # 00:00 hour still open -> excluded
    assert chart["mark_last"] > 0 and len(chart["funding"]) == 2 and chart["oi"]


def test_gate_closes_on_gap_stale_data_and_failures(world):
    ready(world, FakeClient(world.NOW, skip={ms(2024, 2, 1, 0, 3)}))      # exchange dropped one minute
    out = str(world.tmp / "live")
    g, _ = build_live.build(world.remote, world.wd, out, world.NOW, cfg1(), [BTC])
    assert not g["signals_allowed"] and f"klines_1m:{BTC} gaps: 1" in g["reasons"] and f"mark_1m:{BTC} gaps: 1" in g["reasons"]


def test_gate_closes_when_collector_goes_silent(world):
    ready(world)
    out = str(world.tmp / "live")
    assert build_live.build(world.remote, world.wd, out, world.NOW, cfg1(), [BTC])[0]["signals_allowed"]
    g, _ = build_live.build(world.remote, world.wd, out, world.NOW + 2 * 3600_000, cfg1(), [BTC])
    assert not g["signals_allowed"] and f"klines_1m:{BTC} stale" in g["reasons"]


def test_alert_dedupe_and_recovery(world):
    ready(world)
    out = str(world.tmp / "live")
    late = world.NOW + 2 * 3600_000
    a1 = build_live.build(world.remote, world.wd, out, late, cfg1(), [BTC])[1]
    a2 = build_live.build(world.remote, world.wd, out, late + 15 * MIN, cfg1(), [BTC])[1]
    a3 = build_live.build(world.remote, world.wd, out, late + 7 * 3600_000, cfg1(), [BTC])[1]
    assert a1 and "SUSPENDED" in a1 and a2 is None and a3 and "SUSPENDED" in a3
    # data becomes fresh again -> one recovery message
    collect.collect(world.remote, world.wd, FakeClient(world.NOW + 2 * 3600_000 + 7 * 3600_000), symbols=[BTC], log=lambda *a: None)
    fresh = world.NOW + 9 * 3600_000
    g, a4 = build_live.build(world.remote, world.wd, out, fresh, cfg1(), [BTC])
    assert g["signals_allowed"] and a4 and "RESUMED" in a4


def test_status_gate_matches_independent_evaluation(world):
    ready(world)
    out = str(world.tmp / "live")
    g, _ = build_live.build(world.remote, world.wd, out, world.NOW, cfg1(), [BTC])
    status = json.load(open(f"{out}/status.json"))
    assert gate.evaluate(status, cfg1(), world.NOW) == g
