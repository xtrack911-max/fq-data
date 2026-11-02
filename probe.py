"""Connectivity probe. Run FIRST. Tells you whether this host can reach Binance and the data archive."""
import hashlib, io, os, subprocess, sys, tempfile, time, zipfile
from datetime import datetime, timezone
import requests

FAPI, ARCH = "https://fapi.binance.com", "https://data.binance.vision/data/futures/um"


def pm(n):
    t = datetime.now(timezone.utc)
    y, m = t.year, t.month - n
    while m < 1:
        y, m = y - 1, m + 12
    return f"{y}-{m:02d}"


def classify(status):
    return {451: "BLOCKED: HTTP 451 (region/IP restricted) -> run the collector from another host",
            403: "FORBIDDEN (IP/WAF block)", 418: "BANNED (rate limit abuse)",
            429: "RATE LIMITED"}.get(status, f"HTTP {status}")


def run(get=requests.get, storage=False):
    res = []

    def chk(group, name, fn):
        try:
            res.append((group, name, True, fn()))
        except Exception as e:
            res.append((group, name, False, str(e)[:160]))

    def api(path, **p):
        r = get(FAPI + path, params=p, timeout=20)
        if r.status_code != 200:
            raise RuntimeError(classify(r.status_code))
        return r.json()

    def skew():
        t0 = time.time() * 1000
        st = api("/fapi/v1/time")["serverTime"]
        return f"clock offset vs Binance: {int(st - (t0 + time.time() * 1000) / 2)} ms"

    chk("live", "server time", skew)
    for name, path, extra in [("klines 1m", "/fapi/v1/klines", {"interval": "1m"}),
                              ("mark price klines", "/fapi/v1/markPriceKlines", {"interval": "1m"}),
                              ("funding rate", "/fapi/v1/fundingRate", {}),
                              ("open interest hist", "/futures/data/openInterestHist", {"period": "5m"}),
                              ("long/short ratio", "/futures/data/globalLongShortAccountRatio", {"period": "5m"})]:
        chk("live", name, lambda path=path, extra=extra: f"{len(api(path, symbol='BTCUSDT', limit=3, **extra))} rows")

    def arch(kind_path, show=True):
        def f():
            u = f"{ARCH}/{kind_path}"
            r = get(u, timeout=60)
            if r.status_code != 200:
                raise RuntimeError(f"zip {classify(r.status_code)}")
            c = get(u + ".CHECKSUM", timeout=30)
            if c.status_code != 200:
                raise RuntimeError(f"CHECKSUM file {classify(c.status_code)}")
            ok = hashlib.sha256(r.content).hexdigest() == c.text.split()[0].lower()
            z = zipfile.ZipFile(io.BytesIO(r.content))
            with z.open(z.namelist()[0]) as fh:
                head = [fh.readline().decode().strip() for _ in range(2)]
            return f"sha256 {'OK' if ok else 'MISMATCH'} | " + " || ".join(head)
        return f

    m = pm(2)
    chk("archive", "klines 1m monthly", arch(f"monthly/klines/BTCUSDT/1m/BTCUSDT-1m-{m}.zip"))
    chk("archive", "markPriceKlines 1m monthly", arch(f"monthly/markPriceKlines/BTCUSDT/1m/BTCUSDT-1m-{m}.zip"))
    chk("archive", "fundingRate monthly", arch(f"monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-{m}.zip"))
    for d in ("2021-12-15", "2022-01-15", f"{m}-15"):
        chk("archive", f"metrics daily {d}", arch(f"daily/metrics/BTCUSDT/BTCUSDT-metrics-{d}.zip"))
    if storage:
        def st():
            from mxq.remote import ReleaseRemote
            r = ReleaseRemote(os.environ["GITHUB_REPOSITORY"], tag="probe-test")
            with tempfile.TemporaryDirectory() as d:
                p = os.path.join(d, "probe.txt")
                open(p, "w").write("hello " + str(time.time()))
                r.push(p)
                os.makedirs(os.path.join(d, "dl"))
                q = r.fetch("probe.txt", os.path.join(d, "dl"))
                same = q and open(p).read() == open(q).read()
            subprocess.run(["gh", "release", "delete", "probe-test", "-y", "--cleanup-tag",
                            "-R", os.environ["GITHUB_REPOSITORY"]], capture_output=True)
            if not same:
                raise RuntimeError("roundtrip mismatch")
            return "release upload/download roundtrip OK"
        chk("storage", "GitHub Release roundtrip", st)
    live_ok = all(ok for g, n, ok, d in res if g == "live")
    arch_ok = all(ok for g, n, ok, d in res if g == "archive" and "2021-12-15" not in n)
    return res, live_ok, arch_ok


def main():
    res, live_ok, arch_ok = run(storage="--storage" in sys.argv)
    for g, n, ok, d in res:
        print(f"[{'PASS' if ok else 'FAIL'}] {g:7} {n:28} {d}")
    print("\nLIVE API :", "OK" if live_ok else "BLOCKED/FAILING -> collector must run on another host")
    print("ARCHIVE  :", "OK" if arch_ok else "FAILING -> backfill cannot run here")
    print("(metrics 2021-12-15 failing is informational: it only tells us where the archive starts)")
    sys.exit(0 if live_ok and arch_ok and all(ok for g, n, ok, d in res if g == "storage") else 1)


if __name__ == "__main__":
    main()
