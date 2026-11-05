import csv, hashlib, io, zipfile
from datetime import datetime, timezone
from . import http
from .config import ARCHIVE


class ChecksumError(RuntimeError):
    pass


def url(kind, sym, period, itv="1m", daily=False):
    if kind in ("klines", "markPriceKlines"):
        scope = "daily" if daily else "monthly"
        return f"{ARCHIVE}/{scope}/{kind}/{sym}/{itv}/{sym}-{itv}-{period}.zip"
    if kind == "fundingRate":
        return f"{ARCHIVE}/monthly/fundingRate/{sym}/{sym}-fundingRate-{period}.zip"
    if kind == "metrics":                      # period = YYYY-MM-DD (daily files)
        return f"{ARCHIVE}/daily/metrics/{sym}/{sym}-metrics-{period}.zip"
    raise ValueError(kind)


def fetch_verified(u):
    """zip bytes with sha256 verified against the .CHECKSUM file, or None on 404."""
    status, body = http.get(u)
    if status == 404:
        return None
    if status != 200:
        raise http.HttpError(status, u)
    cs, cbody = http.get(u + ".CHECKSUM")
    if cs != 200:
        raise ChecksumError(f"checksum file unavailable ({cs}): {u}")
    want = cbody.decode().split()[0].lower()
    if hashlib.sha256(body).hexdigest() != want:
        raise ChecksumError(f"sha256 mismatch: {u}")
    return body


def _num(x):
    return None if x in ("", None) else float(x)


def _ms(x):
    s = str(x).strip()
    if s.isdigit():
        v = int(s)
        return v // 1000 if v > 10**14 else v          # microsecond files
    dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _csv(zbytes):
    z = zipfile.ZipFile(io.BytesIO(zbytes))
    with z.open(z.namelist()[0]) as f:
        return [r for r in csv.reader(io.TextIOWrapper(f, "utf-8")) if r]


def kline_rows(arrays, ds, source, ingested_at):
    """Shared by archive CSV rows and API arrays (same 12-field layout)."""
    out = []
    for a in arrays:
        r = {"open_time": _ms(a[0]), "open": float(a[1]), "high": float(a[2]),
             "low": float(a[3]), "close": float(a[4]), "close_time": _ms(a[6]),
             "source": source, "ingested_at": ingested_at}
        if ds == "klines_1m":
            r.update(volume=float(a[5]), quote_volume=_num(a[7]), trades=int(a[8]),
                     taker_buy_base=_num(a[9]), taker_buy_quote=_num(a[10]))
        out.append(r)
    return out


def parse_klines(zbytes, ds, ingested_at):
    rows = [r for r in _csv(zbytes) if r[0].strip().isdigit()]     # skips header if present
    return kline_rows(rows, ds, "bulk", ingested_at)


def parse_funding(zbytes, ingested_at):
    rows, hdr = _csv(zbytes), None
    if rows and not rows[0][0].strip().isdigit():
        hdr, rows = [c.strip().lower() for c in rows[0]], rows[1:]
    ti = hdr.index("calc_time") if hdr and "calc_time" in hdr else 0
    ri = hdr.index("last_funding_rate") if hdr and "last_funding_rate" in hdr else 2
    return [{"funding_time": _ms(r[ti]), "rate": float(r[ri]), "mark_price": None,
             "source": "bulk", "ingested_at": ingested_at} for r in rows]


METRIC_COLS = {"oi": "sum_open_interest", "oi_value": "sum_open_interest_value",
               "ls_top_acct": "count_toptrader_long_short_ratio",
               "ls_top_pos": "sum_toptrader_long_short_ratio",
               "ls_global": "count_long_short_ratio",
               "taker_ls_vol": "sum_taker_long_short_vol_ratio"}


def parse_metrics(zbytes, ingested_at):
    rows = _csv(zbytes)
    hdr = [c.strip().lower() for c in rows[0]]
    if "create_time" not in hdr:                      # fail loudly, never guess columns
        raise ValueError("unexpected metrics header: " + ",".join(hdr))
    idx = {k: (hdr.index(v) if v in hdr else None) for k, v in METRIC_COLS.items()}
    ti = hdr.index("create_time")
    out = []
    for r in rows[1:]:
        d = {"ts": _ms(r[ti]), "source": "bulk", "ingested_at": ingested_at}
        for k, i in idx.items():
            d[k] = _num(r[i]) if i is not None else None
        out.append(d)
    return out
