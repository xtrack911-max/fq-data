import json, os
import pytest
import catchup, build_live
from mxq import archive, http, state, store
from mxq.config import gate_config
from conftest import (add_daily, kl_csv, kls, metrics_csv, ms, run_backfill, seed_archive)

BTC, DAY = "BTCUSDT", 86_400_000


def manifest(w):
    return store.Manifest.load(w.remote, w.wd)


def rd(w, name):
    return store.read(w.remote.fetch(name, str(w.tmp / "chk")))


def go(w, now=None):
    return catchup.run(w.remote, w.wd, symbols=[BTC], now=now or w.NOW, log=lambda *a: None)


def spy(w, monkeypatch):
    calls = []
    orig = w.arch.get
    monkeypatch.setattr(http, "get", lambda u, *a, **k: (calls.append(u), orig(u))[1])
    return calls


def ready(w):
    seed_archive(w)
    run_backfill(w)
    add_daily(w, "klines", "2024-01-31", ms(2024, 1, 31))
    add_daily(w, "markPriceKlines", "2024-01-31", ms(2024, 1, 31))


def test_daily_files_complete_a_partial_day_and_unavailable_days_are_remembered(world, monkeypatch):
    ready(world)
    calls = spy(world, monkeypatch)
    assert go(world) == []
    assert not [u for u in calls if "2024-02-01" in u and "/daily/" in u]           # today's file cannot exist yet
    jan = rd(world, "klines_1m_BTCUSDT_2024-01.parquet")
    assert jan.num_rows == 1440 and jan["open_time"].to_pylist()[0] == ms(2024, 1, 31)
    f = manifest(world).files["klines_1m_BTCUSDT_2024-01.parquet"]
    assert f["missing"] == 0 and f["bulk_verified"] is True and len(f["unavailable_days"]) == 30
    sid = manifest(world).snapshot_id()
    calls.clear()
    assert go(world) == []
    assert not [u for u in calls if "/daily/klines/" in u and "2024-01-0" in u]      # no re-asking for known-missing days
    assert manifest(world).snapshot_id() == sid
    calls.clear()
    go(world, world.NOW + 8 * DAY)                                                    # after a week they are retried
    assert len([u for u in calls if "/daily/klines/" in u and "2024-01-" in u and not u.endswith("CHECKSUM")]) >= 30


def test_next_day_extends_prices_and_metrics(world):
    ready(world)
    assert go(world) == []
    add_daily(world, "klines", "2024-02-01", ms(2024, 2, 1))
    add_daily(world, "markPriceKlines", "2024-02-01", ms(2024, 2, 1))
    world.arch.add(archive.url("metrics", BTC, "2024-02-01"), metrics_csv(ms(2024, 2, 1), 288))
    assert go(world, world.NOW + DAY) == []
    feb, met = rd(world, "klines_1m_BTCUSDT_2024-02.parquet"), rd(world, "metrics_BTCUSDT_2024-02.parquet")
    assert feb.num_rows == 1440 and set(feb["source"].to_pylist()) == {"bulk"}
    assert met.num_rows == 288 and max(met["ts"].to_pylist()) == ms(2024, 2, 1, 23, 55)
    assert manifest(world).latest("klines_1m", BTC) == ms(2024, 2, 1, 23, 59)


def test_known_gap_day_is_healed_and_flags_survive(world):
    seed_archive(world)
    full = [a for a in kls(ms(2024, 1, 1), 31 * 1440) if not ms(2024, 1, 15) <= int(a[0]) < ms(2024, 1, 16)]
    for kind in ("klines", "markPriceKlines"):
        world.arch.add(archive.url(kind, BTC, "2024-01"), kl_csv(full))
    run_backfill(world)
    f = manifest(world).files["klines_1m_BTCUSDT_2024-01.parquet"]
    assert f["missing"] == 1440 and f["bulk_verified"]
    for kind in ("klines", "markPriceKlines"):
        add_daily(world, kind, "2024-01-15", ms(2024, 1, 15))
    assert go(world) == []
    f = manifest(world).files["klines_1m_BTCUSDT_2024-01.parquet"]
    assert f["rows"] == 31 * 1440 and f["missing"] == 0 and f["bulk_verified"] is True
    assert manifest(world).files["mark_1m_BTCUSDT_2024-01.parquet"]["missing"] == 0


def test_bad_checksum_changes_nothing_then_recovers(world):
    ready(world)
    add_daily(world, "klines", "2024-01-31", ms(2024, 1, 31), bad=True)
    name = "klines_1m_BTCUSDT_2024-01.parquet"
    before = manifest(world).files[name]["sha256"]
    errs = go(world)
    assert errs and any("sha256 mismatch" in e for e in errs)
    assert manifest(world).files[name]["sha256"] == before                  # the corrupt download never reached storage
    assert rd(world, name).num_rows == 351                                  # (mark price is a separate, healthy component)
    assert state.load(world.remote, world.wd)["consecutive_failures"] == 1
    add_daily(world, "klines", "2024-01-31", ms(2024, 1, 31))
    assert go(world) == [] and state.load(world.remote, world.wd)["consecutive_failures"] == 0


def test_needs_backfill_first(world):
    errs = go(world)
    assert errs and all("run backfill first" in e for e in errs[:2])


def test_delayed_profile_accepts_day_old_data_where_live_profile_refuses(world):
    ready(world)
    assert go(world) == []
    out, live, delayed = str(world.tmp / "site"), gate_config("live"), gate_config("delayed")
    for c in (live, delayed):
        c["symbols"] = [BTC]
    assert delayed["live"] is False and live["live"] is True
    g0 = build_live.build(world.remote, world.wd, out, world.NOW, delayed, [BTC])
    assert g0[0]["signals_allowed"] and g0[2] is True                                   # first build publishes
    later = world.NOW + DAY
    assert not build_live.build(world.remote, world.wd, out, later, live, [BTC])[0]["signals_allowed"]
    assert build_live.build(world.remote, world.wd, out, later, delayed, [BTC])[0]["signals_allowed"]
    assert build_live.build(world.remote, world.wd, out, later + 3600_000, delayed, [BTC])[2] is False   # nothing changed
    assert build_live.build(world.remote, world.wd, out, later + 8 * 3600_000, delayed, [BTC])[2] is True  # 6h refresh
    for f in ("index.html", "crypto/index.html", "crypto/gate.js", ".nojekyll", "crypto/data/status.json",
              "crypto/data/config.json", f"crypto/data/chart_{BTC}.json"):
        assert os.path.exists(os.path.join(out, f)), f
    assert json.load(open(f"{out}/crypto/data/config.json"))["live"] is False


def test_new_data_triggers_republish_even_within_six_hours(world):
    ready(world)
    assert go(world) == []
    out, delayed = str(world.tmp / "site"), gate_config("delayed")
    delayed["symbols"] = [BTC]
    t0 = world.NOW + DAY - 2 * 3600_000
    assert build_live.build(world.remote, world.wd, out, t0, delayed, [BTC])[2] is True
    assert build_live.build(world.remote, world.wd, out, t0 + 60_000, delayed, [BTC])[2] is False
    add_daily(world, "klines", "2024-02-01", ms(2024, 2, 1))                      # new day arrives
    add_daily(world, "markPriceKlines", "2024-02-01", ms(2024, 2, 1))
    assert go(world, world.NOW + DAY) == []
    assert build_live.build(world.remote, world.wd, out, world.NOW + DAY, delayed, [BTC])[2] is True   # only 2h since last publish
