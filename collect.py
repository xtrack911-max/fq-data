"""Live tail: API -> closed 1m candles, mark price, funding, OI/long-short -> Parquet. Idempotent."""
import os, sys
from mxq import archive, remote as remote_mod, state, store, validate
from mxq.binance_api import Client
from mxq.config import MIN_MS, SYMBOLS, month_key
from mxq.store import fname

MAX_PAGES = 80          # 80 x 1500 min ~ 83 days of catch-up per run


class NotReady(RuntimeError):
    pass


def _merge_into(remote, wd, man, ds, sym, name, rows, **extra):
    new = store.table(ds, rows)
    if ds in ("klines_1m", "mark_1m") and validate.bad_ohlc(ds, new):
        raise RuntimeError(f"bad candle from api in {name}")
    p = remote.fetch(name, wd) if name in man.files and man.files[name].get("rows") else None
    merged = store.merge(ds, store.read(p) if p else None, new)
    key = store.KEY[ds]
    miss = validate.continuity(merged, key, MIN_MS)["missing"] if ds in ("klines_1m", "mark_1m") else None
    path = os.path.join(wd, name)
    sha = store.write(path, merged)
    keep = {k: v for k, v in man.files.get(name, {}).items() if k in ("bulk_verified", "bulk_complete", "bulk_months")}
    man.record(name, ds, sym, merged, sha, **keep, **({"missing": miss} if miss is not None else {}), **extra)
    remote.push(path)


def candles(remote, wd, man, client, ds, sym, now):
    last = man.latest(ds, sym)
    if last is None:
        raise NotReady(f"{ds}:{sym} has no data yet - run backfill first")
    fetch = client.klines if ds == "klines_1m" else client.mark_klines
    cursor, raw = last + MIN_MS, []
    for _ in range(MAX_PAGES):
        page = fetch(sym, cursor)
        if not page:
            break
        raw += page
        cursor = int(page[-1][0]) + MIN_MS
        if len(page) < 1500:
            break
    closed = [a for a in raw if int(a[6]) <= now - 1000 and int(a[0]) > last]   # drop unclosed/old
    rows = archive.kline_rows(closed, ds, "api", now)
    by = {}
    for r in rows:
        by.setdefault(month_key(r["open_time"]), []).append(r)
    for mon, rs in by.items():
        _merge_into(remote, wd, man, ds, sym, fname(ds, sym, mon), rs)
    return len(rows)


def funding(remote, wd, man, client, sym, now):
    last = man.latest("funding", sym)
    data = client.funding(sym, last + 1 if last else None)
    rows = [{"funding_time": int(d["fundingTime"]), "rate": float(d["fundingRate"]),
             "mark_price": float(d["markPrice"]) if d.get("markPrice") else None,
             "source": "api", "ingested_at": now} for d in data]
    if rows:
        _merge_into(remote, wd, man, "funding", sym, fname("funding", sym), rows)
    return len(rows)


def metrics(remote, wd, man, client, sym, now):
    last = man.latest("metrics", sym)
    floor = now - 29 * 86_400_000                       # API only serves ~30 days
    start = max(last + 1, floor) if last else None
    data = [d for d in client.metrics(sym, start) if d["ts"] <= now - 300_000]   # no partial bucket
    by = {}
    for d in data:
        by.setdefault(month_key(d["ts"]), []).append({**d, "source": "api", "ingested_at": now})
    for mon, rs in by.items():
        _merge_into(remote, wd, man, "metrics", sym, fname("metrics", sym, mon), rs)
    return len(data)


def collect(remote, wd, client, symbols=SYMBOLS, log=print):
    now, errors = client.now(), []
    man = store.Manifest.load(remote, wd)
    for sym in symbols:
        jobs = [(f"{ds}:{sym}", lambda ds=ds: candles(remote, wd, man, client, ds, sym, now))
                for ds in ("klines_1m", "mark_1m")]
        jobs += [(f"funding:{sym}", lambda: funding(remote, wd, man, client, sym, now)),
                 (f"metrics:{sym}", lambda: metrics(remote, wd, man, client, sym, now))]
        for name, fn in jobs:
            try:
                log(f"ok   {name} +{fn()}")
            except Exception as e:
                errors.append(f"{name}: {e}")
                log(f"FAIL {name}: {e}")
    man.save(remote, wd)
    record_state(remote, wd, now, errors)
    return errors


def record_state(remote, wd, now, errors):
    st = state.load(remote, wd)
    st["last_run"] = now
    if errors:
        st["consecutive_failures"] = st.get("consecutive_failures", 0) + 1
        st["last_error"] = "; ".join(errors)[:500]
    else:
        st.update(last_ok=now, consecutive_failures=0, last_error=None)
    state.save(remote, wd, st)


def main():
    remote, wd, client = remote_mod.from_env(), os.environ.get("WORKDIR", "work"), Client()
    try:
        client.sync_time()
    except Exception as e:                                   # e.g. HTTP 451: record + fail loudly
        record_state(remote, wd, client.now(), [f"connectivity: {e}"])
        print("FAIL connectivity:", e)
        sys.exit(1)
    sys.exit(1 if collect(remote, wd, client) else 0)


if __name__ == "__main__":
    main()
