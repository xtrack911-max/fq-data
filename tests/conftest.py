import hashlib, io, os, sys, zipfile
from datetime import datetime, timezone
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from mxq import http                      # noqa: E402
from mxq.remote import DirRemote          # noqa: E402

MIN = 60_000


def ms(y, mo, d, h=0, mi=0, s=0):
    return int(datetime(y, mo, d, h, mi, s, tzinfo=timezone.utc).timestamp() * 1000)


def price(t):
    return 100 + ((t // MIN) % 50) * 0.1


def kl(t):
    p = price(t)
    return [t, f"{p}", f"{p + 0.5}", f"{p - 0.5}", f"{p + 0.1}", "1.5", t + 59999, "150.0", 10, "0.7", "70.0", "0"]


def kls(start, n, skip=()):
    return [kl(start + i * MIN) for i in range(n) if start + i * MIN not in skip]


KL_HDR = "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore"


def kl_csv(arrs, header=True):
    return "\n".join(([KL_HDR] if header else []) + [",".join(str(x) for x in a) for a in arrs]) + "\n"


class FakeArchive:
    def __init__(self):
        self.files = {}

    def add(self, url, text, bad=False):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            z.writestr("data.csv", text)
        body = buf.getvalue()
        sha = "0" * 64 if bad else hashlib.sha256(body).hexdigest()
        self.files[url] = body
        self.files[url + ".CHECKSUM"] = f"{sha}  {url.split('/')[-1]}\n".encode()

    def drop(self, url):
        self.files.pop(url, None)
        self.files.pop(url + ".CHECKSUM", None)

    def get(self, url, params=None, timeout=30, retries=3):
        if url in self.files:
            return 200, self.files[url]
        return 404, b"not found"


@pytest.fixture
def fake_archive(monkeypatch):
    fa = FakeArchive()
    monkeypatch.setattr(http, "get", fa.get)
    return fa


class FakeClient:
    def __init__(self, now, skip=(), fail=None):
        self._now, self.skip, self.fail = now, set(skip), fail

    def now(self):
        return self._now

    def sync_time(self):
        pass

    def _page(self, start):
        if self.fail:
            raise RuntimeError(self.fail)
        out, t = [], start
        while t <= self._now and len(out) < 1500:
            if t not in self.skip:
                out.append(kl(t))
            t += MIN
        return out

    def klines(self, sym, start):
        return self._page(start)

    def mark_klines(self, sym, start):
        return self._page(start)

    def funding(self, sym, start):
        if self.fail:
            raise RuntimeError(self.fail)
        step, lo = 8 * 3600_000, start or self._now - 86_400_000
        t = lo + (-lo) % step
        out = []
        while t <= self._now:
            out.append({"fundingTime": t, "fundingRate": "0.0001", "markPrice": "100.0"})
            t += step
        return out

    def metrics(self, sym, start):
        if self.fail:
            raise RuntimeError(self.fail)
        step, lo = 300_000, start or self._now - 3 * 3600_000
        t = lo + (-lo) % step
        out = []
        while t <= self._now:
            out.append({"ts": t, "oi": 1000.0, "oi_value": 5e7, "ls_top_acct": 1.1,
                        "ls_top_pos": 1.2, "ls_global": 1.3, "taker_ls_vol": 0.9})
            t += step
        return out


class World:
    pass


@pytest.fixture
def world(tmp_path, fake_archive):
    w = World()
    w.remote, w.wd, w.arch = DirRemote(str(tmp_path / "remote")), str(tmp_path / "work"), fake_archive
    w.NOW = ms(2024, 2, 1, 0, 10, 30)
    w.tmp = tmp_path
    return w


def seed_archive(w, sym="BTCUSDT"):
    """Jan-2024 bulk files holding only the last 6h of January (351 one-minute rows)."""
    from mxq import archive
    start = ms(2024, 1, 31, 18)
    for kind in ("klines", "markPriceKlines"):
        w.arch.add(archive.url(kind, sym, "2024-01"), kl_csv(kls(start, 351)))
    w.arch.add(archive.url("fundingRate", sym, "2024-01"),
               "calc_time,funding_interval_hours,last_funding_rate\n" + f"{ms(2024, 1, 31, 16)},8,0.0001\n")
    rows = "".join(f"{datetime.fromtimestamp((ms(2024, 1, 31, 22) + i * 300_000) / 1000, timezone.utc):%Y-%m-%d %H:%M:%S},"
                   f"{sym},1000,50000000,1.1,1.2,1.3,0.9\n" for i in range(24))
    w.arch.add(archive.url("metrics", sym, "2024-01-31"),
               "create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,"
               "sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio\n" + rows)


def run_backfill(w, syms=("BTCUSDT",), start=(2024, 1)):
    import backfill
    backfill.run(w.remote, w.wd, symbols=list(syms), start=start, metrics_start=(2024, 1),
                 now=w.NOW, workers=2, log=lambda *a: None)


METRICS_HDR = ("create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,"
               "sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio\n")


def metrics_csv(start_ms, n, sym="BTCUSDT"):
    return METRICS_HDR + "".join(
        f"{datetime.fromtimestamp((start_ms + i * 300_000) / 1000, timezone.utc):%Y-%m-%d %H:%M:%S},"
        f"{sym},1000,50000000,1.1,1.2,1.3,0.9\n" for i in range(n))


def add_daily(w, kind, day, start, n=1440, sym="BTCUSDT", bad=False):
    from mxq import archive
    w.arch.add(archive.url(kind, sym, day, daily=True), kl_csv(kls(start, n)), bad=bad)
