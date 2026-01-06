import json
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
import urllib.error
import urllib.parse
import urllib.request


def join_url(base_url: str, path: str) -> str:
    base = base_url.rstrip("/") + "/"
    return urllib.parse.urljoin(base, path.lstrip("/"))


class ApiError(RuntimeError):
    pass


@dataclass
class HttpJsonClient:
    base_url: str
    timeout_secs: float
    headers: Dict[str, str]

    def request_json(self, method: str, path: str, body: Optional[Any]) -> Tuple[int, Any]:
        url = join_url(self.base_url, path)
        data = None
        headers = {"Accept": "application/json", **self.headers}
        if body is not None:
            data = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(url=url, method=method, data=data, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_secs) as resp:
                raw = resp.read()
                if not raw:
                    return resp.status, None
                return resp.status, json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as e:
            raw = e.read()
            message = raw.decode("utf-8", errors="replace") if raw else str(e)
            raise ApiError(f"{method} {path} failed: HTTP {e.code}: {message}") from None
        except urllib.error.URLError as e:
            raise ApiError(f"{method} {path} failed: {e}") from None

    def request_text(
        self, method: str, path: str, body: Optional[Any] = None
    ) -> Tuple[int, str]:
        url = join_url(self.base_url, path)
        data = None
        headers = {"Accept": "text/plain", **self.headers}
        if body is not None:
            data = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(url=url, method=method, data=data, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_secs) as resp:
                raw = resp.read()
                text = raw.decode("utf-8", errors="replace") if raw else ""
                return resp.status, text
        except urllib.error.HTTPError as e:
            raw = e.read()
            message = raw.decode("utf-8", errors="replace") if raw else str(e)
            raise ApiError(f"{method} {path} failed: HTTP {e.code}: {message}") from None
        except urllib.error.URLError as e:
            raise ApiError(f"{method} {path} failed: {e}") from None
