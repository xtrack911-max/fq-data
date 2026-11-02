import hashlib, json, os


def _h(prev, seq, ts, payload):
    s = json.dumps([prev, seq, ts, payload], sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode()).hexdigest()


def append(path, payload, ts):
    """Append-only, hash-chained JSONL: editing or deleting any past record breaks verify()."""
    prev, seq = "0" * 64, 0
    if os.path.exists(path):
        with open(path) as f:
            lines = [json.loads(x) for x in f if x.strip()]
        if lines:
            prev, seq = lines[-1]["hash"], lines[-1]["seq"] + 1
    rec = {"seq": seq, "ts": ts, "payload": payload, "prev": prev}
    rec["hash"] = _h(prev, seq, ts, payload)
    with open(path, "a") as f:
        f.write(json.dumps(rec, sort_keys=True) + "\n")
    return rec


def verify(path):
    prev, n = "0" * 64, 0
    with open(path) as f:
        for i, line in enumerate(x for x in f if x.strip()):
            r = json.loads(line)
            if r["seq"] != i or r["prev"] != prev or r["hash"] != _h(prev, i, r["ts"], r["payload"]):
                return False, i
            prev, n = r["hash"], n + 1
    return True, n
