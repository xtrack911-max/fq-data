import json, os

NAME = "state.json"


def load(remote, workdir):
    p = remote.fetch(NAME, workdir)
    if not p:
        return {"consecutive_failures": 0}
    with open(p) as f:
        return json.load(f)


def save(remote, workdir, st):
    os.makedirs(workdir, exist_ok=True)
    p = os.path.join(workdir, NAME)
    with open(p, "w") as f:
        json.dump(st, f, indent=1, sort_keys=True)
    remote.push(p)
