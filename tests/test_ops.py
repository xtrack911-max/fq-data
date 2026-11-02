import hashlib, io, json, os, subprocess, sys, zipfile
import pytest
import probe
from mxq import remote as R
from conftest import kls, kl_csv, ms, seed_archive, run_backfill, FakeClient

ROOT = os.path.dirname(os.path.dirname(__file__))


class Resp:
    def __init__(self, status, body=b"", js=None):
        self.status_code, self.content, self._js = status, body, js
        self.text = body.decode("utf-8", "ignore")

    def json(self):
        return self._js


def zip_bytes(text):
    b = io.BytesIO()
    with zipfile.ZipFile(b, "w") as z:
        z.writestr("d.csv", text)
    return b.getvalue()


def healthy_get(url, params=None, timeout=0):
    if "fapi.binance.com" in url:
        if url.endswith("/time"):
            return Resp(200, js={"serverTime": 1})
        return Resp(200, js=[1, 2, 3])
    if url.endswith(".CHECKSUM"):
        return Resp(200, (hashlib.sha256(zip_bytes("a,b\n1,2\n")).hexdigest() + "  f.zip\n").encode())
    if "2021-12-15" in url:
        return Resp(404)
    return Resp(200, zip_bytes("a,b\n1,2\n"))


def test_probe_all_good_and_metrics_start_is_informational():
    res, live, arch = probe.run(get=healthy_get)
    assert live and arch
    assert any(n.startswith("metrics daily 2021-12-15") and not ok for _, n, ok, _ in res)
    assert any("sha256 OK" in d for _, _, ok, d in res if ok)


def test_probe_reports_451_clearly_and_separates_live_from_archive():
    def blocked(url, params=None, timeout=0):
        return Resp(451) if "fapi.binance.com" in url else healthy_get(url, params, timeout)
    res, live, arch = probe.run(get=blocked)
    assert not live and arch
    msgs = [d for g, _, ok, d in res if g == "live" and not ok]
    assert msgs and all("HTTP 451" in m and "another host" in m for m in msgs)


def test_probe_flags_checksum_mismatch():
    def corrupt(url, params=None, timeout=0):
        if url.endswith(".CHECKSUM"):
            return Resp(200, b"0" * 64 + b"  f.zip\n")
        return healthy_get(url, params, timeout)
    res, live, arch = probe.run(get=corrupt)
    assert any("MISMATCH" in d for _, _, _, d in res)


def test_release_remote_issues_expected_gh_commands(monkeypatch, tmp_path):
    calls = []

    def fake_run(cmd, capture_output, text):
        calls.append(cmd)
        class P:
            returncode = 1 if cmd[1:3] == ["release", "view"] else 0
            stderr = ""
        return P()
    monkeypatch.setattr(R.subprocess, "run", fake_run)
    r = R.ReleaseRemote("me/repo")
    assert calls[0][:4] == ["gh", "release", "view", "data"] and calls[1][:4] == ["gh", "release", "create", "data"]
    f = tmp_path / "a.parquet"
    f.write_bytes(b"x")
    r.push(str(f))
    assert calls[-1][:3] == ["gh", "release", "upload"] and "--clobber" in calls[-1] and calls[-1][-2:] == ["-R", "me/repo"]
    assert r.fetch("missing.parquet", str(tmp_path / "d")) is None          # file never appears -> None


def test_build_live_cli_runs_and_writes_alert_for_stale_data(world, tmp_path):
    seed_archive(world)
    run_backfill(world)
    import collect
    collect.collect(world.remote, world.wd, FakeClient(world.NOW), symbols=["BTCUSDT"], log=lambda *a: None)
    env = dict(os.environ, DATA_DIR=str(world.tmp / "remote"), WORKDIR=str(world.tmp / "cli_work"))
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    r = subprocess.run([sys.executable, os.path.join(ROOT, "build_live.py"), "--out", "live_out"],
                       cwd=run_dir, env=env, capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    status = json.load(open(run_dir / "live_out" / "status.json"))
    assert status["gate"]["signals_allowed"] is False                   # data from 2024 is stale "now"
    assert "SUSPENDED" in open(run_dir / "alert.txt").read()
    assert (run_dir / "live_out" / "config.json").exists()


def test_workflow_and_scripts_are_well_formed():
    import yaml
    cands = [os.path.join(ROOT, ".github", "workflows", "ops.yml"), os.path.join(ROOT, "ops.yml")]
    path = next((c for c in cands if os.path.exists(c)), None)
    if path is None:
        pytest.skip("ops.yml not present in this checkout")
    wf = yaml.safe_load(open(path))
    on = wf.get("on") or wf.get(True)
    assert on["schedule"][0]["cron"] == "*/15 * * * *"
    assert set(on["workflow_dispatch"]["inputs"]["action"]["options"]) == {"probe", "unpack", "test", "backfill", "collect"}
    assert wf["permissions"]["contents"] == "write"
    steps = [s.get("name") or s.get("uses") for s in wf["jobs"]["run"]["steps"]]
    assert {"unpack", "probe", "test", "backfill", "collect"} <= set(steps)
    assert subprocess.run(["bash", "-n", os.path.join(ROOT, "scripts", "collect.sh")]).returncode == 0
