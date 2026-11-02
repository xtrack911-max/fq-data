import hashlib, json, os, time
import duckdb, pyarrow as pa, pyarrow.compute as pc, pyarrow.parquet as pq

F, I, S = pa.float64, pa.int64, pa.string
SCHEMAS = {
    "klines_1m": pa.schema([("open_time", I()), ("open", F()), ("high", F()), ("low", F()),
                            ("close", F()), ("volume", F()), ("close_time", I()),
                            ("quote_volume", F()), ("trades", I()), ("taker_buy_base", F()),
                            ("taker_buy_quote", F()), ("source", S()), ("ingested_at", I())]),
    "mark_1m": pa.schema([("open_time", I()), ("open", F()), ("high", F()), ("low", F()),
                          ("close", F()), ("close_time", I()), ("source", S()), ("ingested_at", I())]),
    "funding": pa.schema([("funding_time", I()), ("rate", F()), ("mark_price", F()),
                          ("source", S()), ("ingested_at", I())]),
    "metrics": pa.schema([("ts", I()), ("oi", F()), ("oi_value", F()), ("ls_top_acct", F()),
                          ("ls_top_pos", F()), ("ls_global", F()), ("taker_ls_vol", F()),
                          ("source", S()), ("ingested_at", I())]),
}
KEY = {"klines_1m": "open_time", "mark_1m": "open_time", "funding": "funding_time", "metrics": "ts"}
MONTHLY = ("klines_1m", "mark_1m", "metrics")


def fname(ds, sym, month=None):
    return f"{ds}_{sym}_{month}.parquet" if ds in MONTHLY else f"{ds}_{sym}.parquet"


def table(ds, rows):
    return pa.Table.from_pylist(rows, schema=SCHEMAS[ds])


def merge(ds, existing, new):
    """Union by key. Bulk rows beat api rows; otherwise the already-stored row wins."""
    key, con, parts = KEY[ds], duckdb.connect(), []
    if existing is not None and existing.num_rows:
        con.register("e", existing)
        parts.append("SELECT *, 1 AS ord FROM e")
    con.register("n", new)
    parts.append("SELECT *, 2 AS ord FROM n")
    sql = (f"SELECT * EXCLUDE (ord, pri) FROM (SELECT *, CASE WHEN source='bulk' THEN 0 ELSE 1 END AS pri "
           f"FROM ({' UNION ALL '.join(parts)})) "
           f"QUALIFY row_number() OVER (PARTITION BY {key} ORDER BY pri, ord) = 1 ORDER BY {key}")
    return con.sql(sql).to_arrow_table().cast(SCHEMAS[ds])


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def write(path, tbl):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    pq.write_table(tbl, path, compression="zstd")
    return sha256_file(path)


def read(path):
    return pq.read_table(path)


class Manifest:
    NAME = "manifest.json"

    def __init__(self, files=None):
        self.files = files or {}

    @classmethod
    def load(cls, remote, workdir):
        p = remote.fetch(cls.NAME, workdir)
        if not p:
            return cls()
        with open(p) as f:
            return cls(json.load(f)["files"])

    def save(self, remote, workdir):
        os.makedirs(workdir, exist_ok=True)
        p = os.path.join(workdir, self.NAME)
        with open(p, "w") as f:
            json.dump({"schema": 1, "files": self.files}, f, indent=1, sort_keys=True)
        remote.push(p)

    def record(self, name, ds, sym, tbl, sha, **extra):
        mm = pc.min_max(tbl[KEY[ds]]).as_py()
        self.files[name] = {"ds": ds, "sym": sym, "rows": tbl.num_rows, "min_ts": mm["min"],
                            "max_ts": mm["max"], "sha256": sha,
                            "built_at": int(time.time() * 1000), **extra}

    def names(self, ds, sym):
        return sorted(n for n, f in self.files.items()
                      if f.get("ds") == ds and f.get("sym") == sym and f.get("rows"))

    def latest(self, ds, sym):
        v = [self.files[n]["max_ts"] for n in self.names(ds, sym)]
        return max(v) if v else None

    def snapshot_id(self):
        items = sorted((n, f.get("sha256")) for n, f in self.files.items() if f.get("rows"))
        return hashlib.sha256(json.dumps(items).encode()).hexdigest()[:16]
