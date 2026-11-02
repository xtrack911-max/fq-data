import json, os
from datetime import datetime, timezone

HERE = os.path.dirname(__file__)
SYMBOLS = ["BTCUSDT", "ETHUSDT"]
START = (2020, 1)            # first month to backfill
METRICS_START = (2022, 1)    # archive "metrics" start is unverified; probe.py checks it
MIN_MS = 60_000
TF_MS = {"1m": 60_000, "5m": 300_000, "15m": 900_000,
         "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}
FAPI = "https://fapi.binance.com"
ARCHIVE = "https://data.binance.vision/data/futures/um"


def gate_config():
    with open(os.path.join(HERE, "gate_config.json")) as f:
        return json.load(f)


def month_key(ms):
    t = datetime.fromtimestamp(ms / 1000, timezone.utc)
    return f"{t.year}-{t.month:02d}"


def prev_month_key(now_ms, n=1):
    t = datetime.fromtimestamp(now_ms / 1000, timezone.utc)
    y, m = t.year, t.month - n
    while m < 1:
        y, m = y - 1, m + 12
    return f"{y}-{m:02d}"


def months_between(start, end_key):
    y, m = start
    out = []
    while f"{y}-{m:02d}" <= end_key:
        out.append(f"{y}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def days_in_month(key):
    y, m = map(int, key.split("-"))
    ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
    return [f"{key}-{d:02d}" for d in range(1, (datetime(ny, nm, 1) - datetime(y, m, 1)).days + 1)]
