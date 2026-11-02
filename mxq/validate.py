import duckdb


def bad_ohlc(ds, tbl):
    """Rows with null/non-positive prices or an impossible high/low."""
    con = duckdb.connect()
    con.register("t", tbl)
    vol = " OR volume IS NULL OR volume < 0" if ds == "klines_1m" else ""
    return con.execute(
        "SELECT count(*) FROM t WHERE open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL "
        "OR NOT (low > 0 AND high >= greatest(open, close, low) AND low <= least(open, close, high))"
        + vol).fetchone()[0]


def continuity(tbl, key, step):
    """Duplicates and missing steps. Missing steps are recorded, not fatal (archives have real holes)."""
    con = duckdb.connect()
    con.register("t", tbl)
    n, d = con.execute(f"SELECT count(*), count(DISTINCT {key}) FROM t").fetchone()
    missing = con.execute(
        f"SELECT coalesce(sum(g / {step} - 1), 0) FROM (SELECT {key} - lag({key}) OVER (ORDER BY {key}) g "
        f"FROM (SELECT DISTINCT {key} FROM t)) WHERE g > {step}").fetchone()[0]
    return {"rows": n, "dups": n - d, "missing": int(missing)}


def spikes(tbl, pct=0.2):
    con = duckdb.connect()
    con.register("t", tbl)
    return con.execute(
        f"SELECT count(*) FROM (SELECT close, lag(close) OVER (ORDER BY open_time) p FROM t) "
        f"WHERE p > 0 AND abs(close / p - 1) > {pct}").fetchone()[0]
