"""Archive (data.binance.vision) -> verified monthly Parquet files in the data store. Resumable."""
import os, sys, time
from concurrent.futures import ThreadPoolExecutor
import duckdb
from mxq import archive, remote as remote_mod, store, validate
from mxq.config import MIN_MS, START, METRICS_START, SYMBOLS, days_in_month, months_between, prev_month_key
from mxq.store import fname

KIND = {"klines_1m": "klines", "mark_1m": "markPriceKlines"}


def reconcile(existing, new, key="open_time"):
    """Compare api-built rows against bulk rows for the same month (cross-source check)."""
    con = duckdb.connect()
    con.register("a", existing)
    con.register("b", new)
    both, diff = con.execute(
        f"SELECT count(*), coalesce(sum(CASE WHEN abs(a.close - b.close) > 1e-9 * b.close THEN 1 ELSE 0 END), 0) "
        f"FROM a JOIN b USING ({key})").fetchone()
    return {"overlap": both, "close_mismatch": int(diff),
            "api_only": existing.num_rows - both, "bulk_only": new.num_rows - both}


def kline_month(remote, wd, man, sym, ds, mon, now, log):
    name = fname(ds, sym, mon)
    if man.files.get(name, {}).get("bulk_verified"):
        return "skip"
    u = archive.url(KIND[ds], sym, mon)
    body = archive.fetch_verified(u)
    if body is None:
        if mon == prev_month_key(now, 1):
            log(f"pending (not published yet): {u}")
            return "pending"
        raise RuntimeError(f"archive 404 for {u}")
    new = store.table(ds, archive.parse_klines(body, ds, now))
    if new.num_rows == 0:
        raise RuntimeError(f"empty archive file {u}")
    if validate.bad_ohlc(ds, new):
        raise RuntimeError(f"bad OHLC rows in {u}")
    extra, existing = {}, None
    if name in man.files:
        p = remote.fetch(name, wd)
        existing = store.read(p) if p else None
    if existing is not None and existing.num_rows:
        extra["reconcile"] = reconcile(existing, new)
    merged = store.merge(ds, existing, new)
    cont = validate.continuity(merged, "open_time", MIN_MS)
    path = os.path.join(wd, name)
    sha = store.write(path, merged)
    man.record(name, ds, sym, merged, sha, bulk_verified=True, missing=cont["missing"],
               spikes=validate.spikes(merged), **extra)
    remote.push(path)
    return "ok"


def funding(remote, wd, man, sym, now, start, log):
    name, done = fname("funding", sym), set(man.files.get(fname("funding", sym), {}).get("bulk_months", []))
    rows = []
    for mon in months_between(start, prev_month_key(now, 1)):
        if mon in done:
            continue
        body = archive.fetch_verified(archive.url("fundingRate", sym, mon))
        if body is None:
            if mon == prev_month_key(now, 1):
                continue
            raise RuntimeError(f"funding archive 404 {sym} {mon}")
        rows += archive.parse_funding(body, now)
        done.add(mon)
    if not rows:
        return
    p = remote.fetch(name, wd) if name in man.files else None
    merged = store.merge("funding", store.read(p) if p else None, store.table("funding", rows))
    path = os.path.join(wd, name)
    sha = store.write(path, merged)
    man.record(name, "funding", sym, merged, sha, bulk_months=sorted(done))
    remote.push(path)


def metrics_month(remote, wd, man, sym, mon, now, workers, log):
    name = fname("metrics", sym, mon)
    if man.files.get(name, {}).get("bulk_complete"):
        return
    days = days_in_month(mon)

    def one(day):
        body = archive.fetch_verified(archive.url("metrics", sym, day))
        return day, (archive.parse_metrics(body, now) if body else None)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        results = list(ex.map(one, days))
    rows = [r for _, rs in results if rs for r in rs]
    missing_days = [d for d, rs in results if rs is None]
    if not rows:
        man.files[name] = {"ds": "metrics", "sym": sym, "rows": 0, "bulk_complete": True,
                           "missing_days": missing_days}
        return
    p = remote.fetch(name, wd) if name in man.files and man.files[name].get("rows") else None
    merged = store.merge("metrics", store.read(p) if p else None, store.table("metrics", rows))
    path = os.path.join(wd, name)
    sha = store.write(path, merged)
    man.record(name, "metrics", sym, merged, sha, bulk_complete=True, missing_days=missing_days)
    remote.push(path)


def run(remote, wd, symbols=SYMBOLS, start=START, metrics_start=METRICS_START, now=None,
        workers=6, log=print):
    now = now or int(time.time() * 1000)
    man = store.Manifest.load(remote, wd)
    last = prev_month_key(now, 1)
    n = 0
    for sym in symbols:
        for mon in months_between(start, last):
            for ds in KIND:
                r = kline_month(remote, wd, man, sym, ds, mon, now, log)
                log(f"{r:7} {sym} {ds} {mon}")
                n += 1
                if n % 10 == 0:
                    man.save(remote, wd)
        funding(remote, wd, man, sym, now, start, log)
        for mon in months_between(metrics_start, last):
            metrics_month(remote, wd, man, sym, mon, now, workers, log)
            log(f"metrics {sym} {mon}")
        man.save(remote, wd)
    man.save(remote, wd)
    log("backfill complete; snapshot " + man.snapshot_id())


if __name__ == "__main__":
    run(remote_mod.from_env(), os.environ.get("WORKDIR", "work"))
