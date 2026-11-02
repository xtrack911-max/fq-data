import time
from . import http
from .config import FAPI


class Client:
    def __init__(self):
        self.offset = 0

    def sync_time(self):
        t0 = int(time.time() * 1000)
        st = http.get_json(FAPI + "/fapi/v1/time")["serverTime"]
        t1 = int(time.time() * 1000)
        self.offset = st - (t0 + t1) // 2
        return self.offset

    def now(self):
        return int(time.time() * 1000) + self.offset

    def _kl(self, path, sym, start):
        return http.get_json(FAPI + path, {"symbol": sym, "interval": "1m",
                                           "startTime": start, "limit": 1500})

    def klines(self, sym, start):
        return self._kl("/fapi/v1/klines", sym, start)

    def mark_klines(self, sym, start):
        return self._kl("/fapi/v1/markPriceKlines", sym, start)

    def funding(self, sym, start):
        p = {"symbol": sym, "limit": 1000}
        if start:
            p["startTime"] = start
        return http.get_json(FAPI + "/fapi/v1/fundingRate", p)

    def metrics(self, sym, start):
        kw = {"symbol": sym, "period": "5m", "limit": 500}
        if start:
            kw["startTime"] = start
        oi = http.get_json(FAPI + "/futures/data/openInterestHist", kw)

        def m(path, field):
            return {int(x["timestamp"]): float(x[field]) for x in http.get_json(FAPI + path, kw)}
        gl = m("/futures/data/globalLongShortAccountRatio", "longShortRatio")
        ta = m("/futures/data/topLongShortAccountRatio", "longShortRatio")
        tp = m("/futures/data/topLongShortPositionRatio", "longShortRatio")
        tk = m("/futures/data/takerlongshortRatio", "buySellRatio")
        out = []
        for x in oi:
            ts = int(x["timestamp"])
            out.append({"ts": ts, "oi": float(x["sumOpenInterest"]),
                        "oi_value": float(x["sumOpenInterestValue"]),
                        "ls_top_acct": ta.get(ts), "ls_top_pos": tp.get(ts),
                        "ls_global": gl.get(ts), "taker_ls_vol": tk.get(ts)})
        return out
