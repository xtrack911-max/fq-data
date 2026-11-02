import os
from mxq import archive, store
from mxq.resample import resample
from conftest import kls, ms

MIN = 60_000


def write(tmp_path, arrs):
    p = os.path.join(tmp_path, "k.parquet")
    store.write(p, store.table("klines_1m", archive.kline_rows(arrs, "klines_1m", "bulk", 1)))
    return p


def reference(arrs, tf):
    buckets = {}
    for a in arrs:
        buckets.setdefault(int(a[0]) - int(a[0]) % tf, []).append(a)
    out = []
    for t, rs in sorted(buckets.items()):
        if len(rs) == tf // MIN:
            out.append([t, float(rs[0][1]), max(float(r[2]) for r in rs), min(float(r[3]) for r in rs),
                        float(rs[-1][4]), sum(float(r[5]) for r in rs)])
    return out


def test_matches_reference_for_all_timeframes(tmp_path):
    arrs = kls(ms(2024, 1, 1), 24 * 60 * 2)           # two full days
    p = write(tmp_path, arrs)
    end = ms(2024, 1, 3)
    for tf in (300_000, 900_000, 3_600_000, 14_400_000, 86_400_000):
        got = resample([p], tf, 10_000, end)
        ref = reference(arrs, tf)
        assert len(got) == len(ref) and all(abs(g - r) < 1e-9 for gr, rr in zip(got, ref) for g, r in zip(gr, rr))


def test_incomplete_and_unfinished_buckets_dropped(tmp_path):
    arrs = kls(ms(2024, 1, 1), 120, skip={ms(2024, 1, 1, 0, 7)})     # 5m bucket 00:05 has a hole
    p = write(tmp_path, arrs)
    got = resample([p], 300_000, 100, ms(2024, 1, 1, 1, 58))
    ts = [g[0] for g in got]
    assert ms(2024, 1, 1, 0, 5) not in ts                # incomplete -> dropped, never faked
    assert ms(2024, 1, 1, 1, 55) not in ts               # ends after end_ms -> still open
    assert ms(2024, 1, 1, 1, 50) in ts


def test_tail_limit_and_empty(tmp_path):
    p = write(tmp_path, kls(ms(2024, 1, 1), 600))
    got = resample([p], 3_600_000, 3, ms(2024, 1, 1, 10))
    assert len(got) == 3 and got[-1][0] == ms(2024, 1, 1, 9)
    assert resample([], 60_000, 5, 0) == []
