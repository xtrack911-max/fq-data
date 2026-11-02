import pytest
from mxq import store, validate, archive
from mxq.remote import DirRemote
from conftest import kl, kls, ms

MIN = 60_000


def tbl(arrs, source="api", ds="klines_1m"):
    return store.table(ds, archive.kline_rows(arrs, ds, source, 1))


def test_bad_ohlc_detected():
    good = tbl(kls(ms(2024, 1, 1), 5))
    assert validate.bad_ohlc("klines_1m", good) == 0
    bad = kl(ms(2024, 1, 1, 1))
    bad[2] = "50.0"                                   # high below open/close
    neg = kl(ms(2024, 1, 1, 2))
    neg[3] = "-1"                                     # non-positive low
    assert validate.bad_ohlc("klines_1m", tbl([bad, neg] + kls(ms(2024, 1, 1), 2))) == 2


def test_continuity_counts_missing_and_dups():
    t = tbl(kls(ms(2024, 1, 1), 10, skip={ms(2024, 1, 1, 0, 3), ms(2024, 1, 1, 0, 4)}))
    c = validate.continuity(t, "open_time", MIN)
    assert c == {"rows": 8, "dups": 0, "missing": 2}
    dup = store.table("klines_1m", archive.kline_rows(kls(ms(2024, 1, 1), 3) + kls(ms(2024, 1, 1), 1), "klines_1m", "api", 1))
    assert validate.continuity(dup, "open_time", MIN)["dups"] == 1


def test_spike_counter():
    a = kls(ms(2024, 1, 1), 3)
    a[2][4] = "300.0"
    assert validate.spikes(tbl(a)) == 1


def test_merge_precedence_and_ordering():
    t0 = ms(2024, 1, 1)
    api_old = tbl([kl(t0), kl(t0 + MIN)], "api")
    bulk = tbl([kl(t0 + MIN)], "bulk")
    bulk = bulk.set_column(bulk.schema.get_field_index("close"), "close", __import__("pyarrow").array([777.0]))
    m = store.merge("klines_1m", api_old, bulk)
    assert m.num_rows == 2 and m["open_time"].to_pylist() == [t0, t0 + MIN]
    assert m["close"].to_pylist()[1] == 777.0 and m["source"].to_pylist() == ["api", "bulk"]
    # an existing bulk row is never overwritten by a later api row
    api_new = tbl([kl(t0 + MIN)], "api")
    m2 = store.merge("klines_1m", m, api_new)
    assert m2["close"].to_pylist()[1] == 777.0 and m2["source"].to_pylist()[1] == "bulk"
    # existing api row beats a later api row; merge is idempotent
    m3 = store.merge("klines_1m", m2, m2)
    assert m3.num_rows == 2 and m3.equals(m2)


def test_parquet_roundtrip_and_manifest(tmp_path):
    remote, wd = DirRemote(str(tmp_path / "r")), str(tmp_path / "w")
    import os
    os.makedirs(wd)
    t = tbl(kls(ms(2024, 1, 1), 5))
    p = os.path.join(wd, "a.parquet")
    sha = store.write(p, t)
    assert store.read(p).equals(t) and len(sha) == 64
    man = store.Manifest()
    man.record("klines_1m_BTCUSDT_2024-01.parquet", "klines_1m", "BTCUSDT", t, sha)
    assert man.latest("klines_1m", "BTCUSDT") == ms(2024, 1, 1) + 4 * MIN
    assert man.latest("klines_1m", "ETHUSDT") is None
    sid = man.snapshot_id()
    man.files["klines_1m_BTCUSDT_2024-01.parquet"]["built_at"] = 0       # snapshot id ignores build time
    assert man.snapshot_id() == sid
    man.save(remote, wd)
    assert store.Manifest.load(remote, wd).snapshot_id() == sid
    man.files["klines_1m_BTCUSDT_2024-01.parquet"]["sha256"] = "x"       # but changes with content
    assert man.snapshot_id() != sid
