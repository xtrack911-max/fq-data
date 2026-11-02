import json, time
import requests

_S = requests.Session()


class HttpError(RuntimeError):
    def __init__(self, status, url, body=""):
        super().__init__(f"HTTP {status} {url} {str(body)[:120]}")
        self.status = status


def get(url, params=None, timeout=30, retries=3):
    """Returns (status, bytes). Retries network errors and 429/5xx; never retries 4xx."""
    last = None
    for i in range(retries):
        try:
            r = _S.get(url, params=params, timeout=timeout)
        except requests.RequestException as e:
            last = e
            time.sleep(1.5 ** i)
            continue
        if r.status_code in (429, 500, 502, 503, 504):
            last = HttpError(r.status_code, url, r.text)
            time.sleep(2 ** i)
            continue
        return r.status_code, r.content
    raise last


def get_json(url, params=None):
    s, b = get(url, params)
    if s != 200:
        raise HttpError(s, url, b.decode("utf-8", "replace"))
    return json.loads(b)
