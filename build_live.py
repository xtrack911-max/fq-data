"""Builds the dashboard feed (status.json + chart_<SYM>.json + config.json) and the freshness gate."""
import argparse, json, os, shutil, time
import duckdb
from mxq import gate, remote as remote_mod, state, store
from mxq.config import gate_config, month_key, prev_month_key, MIN_MS
from mxq.resample import resample

TAILS = {"1m": 1000, "5m": 1000, "15m": 1000, "1h": 1000, "4h": 500, "1d": 90}
TF = {"1m": 60_000, "5m": 300_000, "15m": 900_000, "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}
REALERT_MS = 6 * 3_600_000


def _lst(files):
    return "[" + ",".join("'" + f.replace("'", "''") + "'" for f in files) + "]"


def local_files(remote, wd, man, ds, sym, months):
    names = man.names(ds, sym)
    if ds in store.MONTHLY:
        keep = {store.fname(ds, sym, m) for m in months}
        names = [n for n in names if n in keep]
    out = []
    for n in names:
        p = remote.fetch(n, wd)
        if p:
            out.append(p)
    return out


def build(remote, wd, out_dir, now, cfg=None, symbols=None):
    cfg = cfg or gate_config()
    symbols = symbols or cfg["symbols"]
    man, st = store.Manifest.load(remote, wd), state.load(remote, wd)
    months = [month_key(now), prev_month_key(now, 1), prev_month_key(now, 2)]
    os.makedirs(out_dir, exist_ok=True)
    series, win = {}, cfg["recent_window_min"]
    for sym in symbols:
        con = duckdb.connect()
        files = {ds: local_files(remote, wd, man, ds, sym, months)
                 for ds in ("klines_1m", "mark_1m", "metrics", "funding")}
        for ds in ("klines_1m", "mark_1m"):
            if files[ds]:
                lo, lc = con.execute(f"SELECT max(open_time), max(close_time) FROM read_parquet({_lst(files[ds])})").fetchone()
                cnt = con.execute(f"SELECT count(*) FROM read_parquet({_lst(files[ds])}) WHERE open_time > {lo - win * MIN_MS}").fetchone()[0]
                series[f"{ds}:{sym}"] = {"last_close": lc, "recent_missing": max(0, win - cnt)}
        for ds, col in (("funding", "funding_time"), ("metrics", "ts")):
            if files[ds]:
                series[f"{ds}:{sym}"] = {"last_ts": con.execute(f"SELECT max({col}) FROM read_parquet({_lst(files[ds])})").fetchone()[0]}
        chart = {"symbol": sym, "generated_at": now,
                 "tf": {k: resample(files["klines_1m"], TF[k], TAILS[k], now) for k in TAILS}}
        if files["mark_1m"]:
            chart["mark_last"] = con.execute(f"SELECT close FROM read_parquet({_lst(files['mark_1m'])}) ORDER BY open_time DESC LIMIT 1").fetchone()[0]
        chart["funding"] = [list(r) for r in con.execute(f"SELECT funding_time, rate FROM read_parquet({_lst(files['funding'])}) ORDER BY 1 DESC LIMIT 30").fetchall()][::-1] if files["funding"] else []
        chart["oi"] = [list(r) for r in con.execute(f"SELECT ts, oi_value FROM read_parquet({_lst(files['metrics'])}) ORDER BY 1 DESC LIMIT 300").fetchall()][::-1] if files["metrics"] else []
        with open(os.path.join(out_dir, f"chart_{sym}.json"), "w") as f:
            json.dump(chart, f, separators=(",", ":"))
    status = {"schema": 1, "generated_at": now, "snapshot_id": man.snapshot_id(),
              "collector": {"last_ok": st.get("last_ok"), "last_run": st.get("last_run"),
                            "consecutive_failures": st.get("consecutive_failures", 0),
                            "last_error": st.get("last_error")},
              "series": series}
    g = gate.evaluate(status, cfg, now)
    status["gate"] = g
    for name, obj in (("status.json", status), ("config.json", cfg)):
        with open(os.path.join(out_dir, name), "w") as f:
            json.dump(obj, f, indent=1)
    alert = None
    prev = st.get("gate_allowed")
    if not g["signals_allowed"] and (prev is not False or now - st.get("last_alert", 0) > REALERT_MS):
        alert, st["last_alert"] = "SIGNALS SUSPENDED: " + "; ".join(g["reasons"][:6]), now
    elif g["signals_allowed"] and prev is False:
        alert = "Signals RESUMED: data fresh again"
    st["gate_allowed"] = g["signals_allowed"]
    state.save(remote, wd, st)
    return g, alert


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="live_out")
    a = ap.parse_args()
    g, alert = build(remote_mod.from_env(), os.environ.get("WORKDIR", "work"), a.out, int(time.time() * 1000))
    print(json.dumps(g))
    if alert:
        with open("alert.txt", "w") as f:
            f.write(alert)
