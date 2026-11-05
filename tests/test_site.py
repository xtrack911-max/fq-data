import json, os, shutil, subprocess
import pytest
from mxq import gate
from mxq.config import gate_config
from conftest import ms

HERE = os.path.dirname(__file__)
NOW = ms(2024, 2, 1, 12)
H = 3600_000


@pytest.fixture(scope="module")
def node_env():
    if not shutil.which("node"):
        pytest.skip("node not installed")
    env = dict(os.environ)
    if subprocess.run(["node", "-e", "require('jsdom')"], capture_output=True, env=env).returncode != 0:
        prefix = "/tmp/fq_jsdom"
        r = subprocess.run(["npm", "install", "--prefix", prefix, "jsdom"], capture_output=True, timeout=240)
        if r.returncode != 0:
            pytest.skip("jsdom unavailable (offline?)")
        env["NODE_PATH"] = os.path.join(prefix, "node_modules")
    return env


def status(gen=H, **kw):
    s = {"schema": 1, "generated_at": NOW - gen, "snapshot_id": "x",
         "collector": {"consecutive_failures": 0}, "series": {}}
    ages = {"klines_1m": 2 * H, "mark_1m": 2 * H, "funding": 72 * H, "metrics": 2 * H}
    ages.update(kw)
    for sym in ("BTCUSDT", "ETHUSDT"):
        for ds, a in ages.items():
            key = "last_close" if ds.endswith("_1m") else "last_ts"
            s["series"][f"{ds}:{sym}"] = {key: NOW - a, "recent_missing": 0}
    return s


def chart(sym="BTCUSDT"):
    bars = [[NOW - i * H, 1, 2, 0.5, 1.5, 3] for i in range(3)]
    return {"symbol": sym, "tf": {k: bars for k in ("1m", "5m", "15m", "1h", "4h", "1d")},
            "mark_last": 61234.5, "funding": [[NOW, 0.0001]], "oi": [[NOW, 8.5e9]]}


def files(cfg, st, charts=True):
    f = {"data/config.json": cfg, "data/status.json": st}
    if charts:
        f.update({"data/chart_BTCUSDT.json": chart(), "data/chart_ETHUSDT.json": chart("ETHUSDT")})
    return f


def test_dashboard_behaviour_in_a_real_dom(node_env, tmp_path):
    delayed, live = gate_config("delayed"), gate_config("live")
    scen = [
        {"name": "delayed ok", "now": NOW, "files": files(delayed, status())},
        {"name": "stale prices", "now": NOW, "files": files(delayed, status(klines_1m=120 * H))},
        {"name": "status missing", "now": NOW, "files": {}},
        {"name": "live ok", "now": NOW, "files": files(live, status(gen=60_000, klines_1m=5 * 60_000, mark_1m=5 * 60_000,
                                                                     funding=H, metrics=10 * 60_000))},
        {"name": "no chart", "now": NOW, "files": files(delayed, status(), charts=False)},
        {"name": "bad schema", "now": NOW, "files": files(delayed, {**status(), "schema": 2})},
        {"name": "delayed data under live rules", "now": NOW, "files": files(live, status())},
    ]
    p = tmp_path / "sc.json"
    p.write_text(json.dumps(scen))
    r = subprocess.run(["node", os.path.join(HERE, "dash_test.js"), str(p)], capture_output=True, text=True, env=node_env)
    assert r.returncode == 0, r.stderr
    out = {x["name"]: x for x in json.loads(r.stdout)}
    a = out["delayed ok"]
    assert a["badge"].startswith("DATA OK") and "DELAYED" in a["badge"] and a["cls"] == "ok"
    assert "live signals are off" in a["why"] and a["age"].startswith("Latest candle: 2024-02-01 10:00 UTC")
    assert a["mark"].startswith("61") and a["fund"] == "0.0100%" and a["oi"] == "8.50 B" and a["drawn"] == [3]
    b = out["stale prices"]
    assert b["badge"] == "SIGNALS SUSPENDED" and b["cls"] == "" and "klines_1m:BTCUSDT stale" in b["why"]
    c = out["status missing"]
    assert c["badge"] == "SIGNALS SUSPENDED" and c["why"] == "status unreachable" and c["age"] == ""
    assert out["live ok"]["badge"] == "LIVE DATA OK" and out["live ok"]["cls"] == "ok"
    d = out["no chart"]
    assert d["badge"].startswith("DATA OK") and d["note"] == "Chart data unavailable."
    assert out["bad schema"]["badge"] == "SIGNALS SUSPENDED"
    e = out["delayed data under live rules"]
    assert e["badge"] == "SIGNALS SUSPENDED" and "stale" in e["why"]
    # the page decides exactly like the Python gate
    assert gate.evaluate(status(klines_1m=120 * H), delayed, NOW)["signals_allowed"] is False
    assert gate.evaluate(status(), live, NOW)["signals_allowed"] is False
