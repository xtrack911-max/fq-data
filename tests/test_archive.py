import hashlib, io, zipfile
import pytest
from mxq import archive
from conftest import kl, kls, kl_csv, ms


def zipped(text):
    b = io.BytesIO()
    with zipfile.ZipFile(b, "w") as z:
        z.writestr("x.csv", text)
    return b.getvalue()


def test_parse_klines_with_and_without_header():
    arrs = kls(ms(2024, 1, 1), 3)
    for hdr in (True, False):
        rows = archive.parse_klines(zipped(kl_csv(arrs, header=hdr)), "klines_1m", 5)
        assert len(rows) == 3 and rows[0]["open_time"] == ms(2024, 1, 1) and rows[0]["source"] == "bulk"
        assert rows[1]["close_time"] == rows[1]["open_time"] + 59999 and rows[0]["trades"] == 10


def test_microsecond_timestamps_normalised():
    a = kl(ms(2025, 1, 1))
    a[0], a[6] = a[0] * 1000, a[6] * 1000
    r = archive.parse_klines(zipped(kl_csv([a], header=False)), "klines_1m", 0)[0]
    assert r["open_time"] == ms(2025, 1, 1) and r["close_time"] == ms(2025, 1, 1) + 59999


def test_mark_rows_have_no_volume_fields():
    r = archive.parse_klines(zipped(kl_csv(kls(ms(2024, 1, 1), 1))), "mark_1m", 0)[0]
    assert "volume" not in r and r["close"] > 0


def test_checksum_ok_bad_missing_and_404(fake_archive):
    u = "https://x/a.zip"
    fake_archive.add(u, "1,2\n")
    assert archive.fetch_verified(u)
    fake_archive.add(u, "1,2\n", bad=True)
    with pytest.raises(archive.ChecksumError, match="mismatch"):
        archive.fetch_verified(u)
    fake_archive.files.pop(u + ".CHECKSUM")
    with pytest.raises(archive.ChecksumError, match="unavailable"):
        archive.fetch_verified(u)
    assert archive.fetch_verified("https://x/none.zip") is None


def test_funding_columns_found_by_name_even_if_reordered():
    txt = "last_funding_rate,calc_time,funding_interval_hours\n0.0003,1700000000000,8\n"
    r = archive.parse_funding(zipped(txt), 0)[0]
    assert r["rate"] == 0.0003 and r["funding_time"] == 1700000000000


def test_metrics_datetime_parse_and_unexpected_header_fails():
    ok = ("create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,"
          "sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio\n"
          "2024-01-01 00:05:00,BTCUSDT,10,500,1.1,1.2,1.3,0.9\n")
    r = archive.parse_metrics(zipped(ok), 0)[0]
    assert r["ts"] == ms(2024, 1, 1, 0, 5) and r["oi"] == 10.0 and r["ls_global"] == 1.3
    with pytest.raises(ValueError, match="unexpected metrics header"):
        archive.parse_metrics(zipped("time,foo\n1,2\n"), 0)
