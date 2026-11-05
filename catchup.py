"""Delayed catch-up from DAILY archive files (works from GitHub; needs no live Binance API).
Fills klines_1m / mark_1m / metrics up to yesterday (UTC) and tries to heal known missing days."""
import os, sys, time
import duckdb
import backfill, collect
from mxq import archive, remote as remote_mod, store
from mxq.config import SYMBOLS, START, day_key, days_in_month, month_key, months_between, prev_month_key
from mxq.store import fname

KIND = {"klines_1m": "klines", "mark_1m": "markPriceKlines"}
DAY_MS, RETRY_MS = 86_400_000, 7 * 86_400_000


def _ym(ms):
    y, m = month_key(ms).split("-")
    return int(y), int(m)


def missing_days(path, mon, now):
    """Days of `mon` up to yesterday that hold fewer than 1440 one-minute rows."""
    last_day = day_key(now - DAY_MS)          # today's daily file does not exist yet
    days = [d for d in days_in_month(mon) if d <= last_day]
    have = {}
    if path:
        q = f"SELECT strftime(epoch_ms(open_time), '%Y-%m-%d'), count(*) FROM '{path}' GROUP BY 1"
        have = dict(duckdb.sql(q).fetchall())
    return [d for d in days if have.get(d, 0) < 1440]


def candles(remote, wd, man, sym, ds, now, log):
    latest = man.latest(ds, sym)
    if latest is None:
        raise collect.NotReady(f"{ds}:{sym} has no data yet - run backfill first")
    months = {n.rsplit("_", 1)[1][:7] for n in man.names(ds, sym) if man.files[n].get("missing")}
    months |= set(months_between(_ym(latest), month_key(now)))
    total = 0
    for mon in sorted(months):
        name = fname(ds, sym, mon)
        have = name in man.files and man.files[name].get("rows")
        path = remote.fetch(name, wd) if have else None
        unavail = dict(man.files.get(name, {}).get("unavailable_days", {}))
        rows = []
        for day in missing_days(path, mon, now):
            if day in unavail and now - unavail[day] < RETRY_MS:
                continue                                   # asked recently; Binance has no file for it
            body = archive.fetch_verified(archive.url(KIND[ds], sym, day, daily=True))
            if body is None:
                unavail[day] = now
                continue
            unavail.pop(day, None)
            rows += archive.parse_klines(body, ds, now)
        if rows:
            collect._merge_into(remote, wd, man, ds, sym, name, rows, unavailable_days=unavail)
        elif have:
            man.files[name]["unavailable_days"] = unavail
        total += len(rows)
    return total


def metrics(remote, wd, man, sym, now, log):
    last = man.latest("metrics", sym)
    if last is None:
        raise collect.NotReady(f"metrics:{sym} has no data yet - run backfill first")
    first, yday = day_key(last + 300_000), day_key(now - DAY_MS)
    days = [d for m in months_between(_ym(last), month_key(now)) for d in days_in_month(m) if first <= d <= yday]
    by = {}
    for day in days:
        body = archive.fetch_verified(archive.url("metrics", sym, day))
        if body:
            for r in archive.parse_metrics(body, now):
                by.setdefault(month_key(r["ts"]), []).append(r)
    for mon, rs in by.items():
        collect._merge_into(remote, wd, man, "metrics", sym, fname("metrics", sym, mon), rs)
    return sum(len(v) for v in by.values())


def funding_start(man, sym):
    """Resume from the first month the manifest already covers (never re-scan history that was not asked for)."""
    done = man.files.get(fname("funding", sym), {}).get("bulk_months")
    if not done:
        return START
    y, m = min(done).split("-")
    return int(y), int(m)


def run(remote, wd, symbols=SYMBOLS, now=None, log=print):
    now = now or int(time.time() * 1000)
    man, errors, last_complete = store.Manifest.load(remote, wd), [], prev_month_key(now, 1)
    for sym in symbols:
        jobs = [(f"{ds}:{sym}", lambda ds=ds, sym=sym: candles(remote, wd, man, sym, ds, now, log)) for ds in KIND]
        jobs.append((f"metrics:{sym}", lambda sym=sym: metrics(remote, wd, man, sym, now, log)))
        jobs.append((f"funding:{sym}", lambda sym=sym: backfill.funding(remote, wd, man, sym, now, funding_start(man, sym), log)))
        jobs += [(f"monthly:{ds}:{sym}", lambda ds=ds, sym=sym: backfill.kline_month(remote, wd, man, sym, ds, last_complete, now, log))
                 for ds in KIND]
        for name, fn in jobs:
            try:
                log(f"ok   {name}: {fn()}")
            except Exception as e:
                errors.append(f"{name}: {e}")
                log(f"FAIL {name}: {e}")
    man.save(remote, wd)
    collect.record_state(remote, wd, now, errors)
    return errors


if __name__ == "__main__":
    sys.exit(1 if run(remote_mod.from_env(), os.environ.get("WORKDIR", "work")) else 0)
