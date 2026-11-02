import copy, json, os, shutil, subprocess
import pytest
from mxq import gate, ledger
from mxq.config import gate_config

CFG = gate_config()
NOW = 1_700_000_000_000


def good():
    s = {"schema": 1, "generated_at": NOW - 60_000,
         "collector": {"consecutive_failures": 0}, "series": {}}
    for sym in CFG["symbols"]:
        s["series"][f"klines_1m:{sym}"] = {"last_close": NOW - 60_000, "recent_missing": 0}
        s["series"][f"mark_1m:{sym}"] = {"last_close": NOW - 60_000, "recent_missing": 0}
        s["series"][f"funding:{sym}"] = {"last_ts": NOW - 3600_000}
        s["series"][f"metrics:{sym}"] = {"last_ts": NOW - 600_000}
    return s


def variant(fn):
    s = good()
    fn(s)
    return s


SCENARIOS = {
    "good": good(),
    "none": None,
    "bad schema": variant(lambda s: s.update(schema=2)),
    "status stale": variant(lambda s: s.update(generated_at=NOW - 3 * 3600_000)),
    "klines stale": variant(lambda s: s["series"]["klines_1m:BTCUSDT"].update(last_close=NOW - 3600_000)),
    "mark missing": variant(lambda s: s["series"].pop("mark_1m:ETHUSDT")),
    "gaps": variant(lambda s: s["series"]["klines_1m:ETHUSDT"].update(recent_missing=3)),
    "failures": variant(lambda s: s["collector"].update(consecutive_failures=2)),
    "one failure ok": variant(lambda s: s["collector"].update(consecutive_failures=1)),
    "funding stale": variant(lambda s: s["series"]["funding:BTCUSDT"].update(last_ts=NOW - 12 * 3600_000)),
    "null ts": variant(lambda s: s["series"]["metrics:BTCUSDT"].update(last_ts=None)),
    "klines 1s over limit": variant(lambda s: s["series"]["klines_1m:BTCUSDT"].update(last_close=NOW - 45 * 60000 - 1000)),
    "klines 1s under limit": variant(lambda s: s["series"]["klines_1m:BTCUSDT"].update(last_close=NOW - 45 * 60000 + 1000)),
    "metrics 1s over limit": variant(lambda s: s["series"]["metrics:ETHUSDT"].update(last_ts=NOW - 75 * 60000 - 1000)),
    "funding 1s under limit": variant(lambda s: s["series"]["funding:ETHUSDT"].update(last_ts=NOW - 600 * 60000 + 1000)),
    "status 1s over": variant(lambda s: s.update(generated_at=NOW - 45 * 60000 - 1000)),
    "everything": {"schema": 1, "generated_at": 0, "collector": {"consecutive_failures": 5}, "series": {}},
}


def test_gate_decisions():
    assert gate.evaluate(SCENARIOS["good"], CFG, NOW)["signals_allowed"] is True
    assert gate.evaluate(SCENARIOS["one failure ok"], CFG, NOW)["signals_allowed"] is True
    for name in ("none", "bad schema", "status stale", "klines stale", "mark missing", "gaps",
                 "failures", "funding stale", "null ts", "everything"):
        r = gate.evaluate(SCENARIOS[name], CFG, NOW)
        assert r["signals_allowed"] is False and r["reasons"], name
    b = gate.evaluate
    assert "klines_1m:BTCUSDT stale" in b(SCENARIOS["klines 1s over limit"], CFG, NOW)["reasons"]
    assert b(SCENARIOS["klines 1s under limit"], CFG, NOW)["signals_allowed"] is True
    assert "metrics:ETHUSDT stale" in b(SCENARIOS["metrics 1s over limit"], CFG, NOW)["reasons"]
    assert b(SCENARIOS["funding 1s under limit"], CFG, NOW)["signals_allowed"] is True
    assert "status stale" in b(SCENARIOS["status 1s over"], CFG, NOW)["reasons"]
    assert "klines_1m:BTCUSDT stale" in gate.evaluate(SCENARIOS["klines stale"], CFG, NOW)["reasons"]
    assert "klines_1m:ETHUSDT gaps: 3" in gate.evaluate(SCENARIOS["gaps"], CFG, NOW)["reasons"]
    assert gate.evaluate(SCENARIOS["good"], CFG, NOW + 2 * 3600_000)["signals_allowed"] is False  # time alone kills it


@pytest.mark.skipif(not shutil.which("node"), reason="node not installed")
def test_python_and_js_gates_agree(tmp_path):
    vec = [{"name": n, "status": s, "cfg": CFG, "now": NOW, "expected": gate.evaluate(s, CFG, NOW)}
           for n, s in SCENARIOS.items()]
    p = tmp_path / "v.json"
    p.write_text(json.dumps(vec))
    js = os.path.join(os.path.dirname(__file__), "gate_parity.js")
    r = subprocess.run(["node", js, str(p)], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


def test_ledger_chain_detects_tampering(tmp_path):
    p = str(tmp_path / "l.jsonl")
    for i in range(4):
        ledger.append(p, {"sig": i, "snapshot": "abc"}, 1000 + i)
    assert ledger.verify(p) == (True, 4)
    lines = open(p).read().splitlines()
    # edit a payload
    t = json.loads(lines[1]); t["payload"]["sig"] = 99
    open(p, "w").write("\n".join([lines[0], json.dumps(t, sort_keys=True)] + lines[2:]) + "\n")
    assert ledger.verify(p) == (False, 1)
    # delete a middle record
    open(p, "w").write("\n".join([lines[0], lines[2], lines[3]]) + "\n")
    assert ledger.verify(p)[0] is False
    # truncating the tail is detectable only against a remembered head: appends continue the chain
    open(p, "w").write("\n".join(lines[:3]) + "\n")
    assert ledger.verify(p) == (True, 3)
