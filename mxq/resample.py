import duckdb


def resample(files, tf_ms, tail, end_ms):
    """1m parquet files -> [[t,o,h,l,c,v],...] ascending. Only COMPLETE buckets that ended by end_ms."""
    if not files:
        return []
    lst = ",".join("'" + f.replace("'", "''") + "'" for f in files)
    n = tf_ms // 60_000
    q = (f"SELECT t, o, h, l, c, v FROM (SELECT t, arg_min(open, open_time) o, max(high) h, min(low) l, "
         f"arg_max(close, open_time) c, sum(volume) v, count(*) n "
         f"FROM (SELECT open_time - open_time % {tf_ms} AS t, * FROM read_parquet([{lst}])) "
         f"GROUP BY t HAVING count(*) = {n} AND t + {tf_ms} <= {end_ms} ORDER BY t DESC LIMIT {tail}) ORDER BY t")
    return [list(r) for r in duckdb.connect().execute(q).fetchall()]
