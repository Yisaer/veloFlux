import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, List

from .http_client import ApiError, HttpJsonClient


@dataclass
class ManagerClient:
    http: HttpJsonClient

    @classmethod
    def new(cls, base_url: str, timeout_secs: float) -> "ManagerClient":
        return cls(
            http=HttpJsonClient(base_url=base_url, timeout_secs=timeout_secs, headers={})
        )

    def list_streams(self) -> List[Dict[str, Any]]:
        _, resp = self.http.request_json("GET", "/streams", None)
        return resp or []

    def describe_stream(self, name: str) -> Dict[str, Any]:
        _, resp = self.http.request_json(
            "GET", f"/streams/describe/{urllib.parse.quote(name)}", None
        )
        return resp or {}

    def list_functions(self) -> List[Dict[str, Any]]:
        _, resp = self.http.request_json("GET", "/functions", None)
        return resp or []

    def get_syntax_capabilities(self) -> Dict[str, Any]:
        _, resp = self.http.request_json("GET", "/capabilities/syntax", None)
        return resp or {}

    def create_stream(self, body: Dict[str, Any]) -> Dict[str, Any]:
        _, resp = self.http.request_json("POST", "/streams", body)
        return resp or {}

    def create_pipeline(self, body: Dict[str, Any]) -> Dict[str, Any]:
        _, resp = self.http.request_json("POST", "/pipelines", body)
        return resp or {}

    def delete_pipeline(self, pipeline_id: str) -> str:
        _, text = self.http.request_text(
            "DELETE", f"/pipelines/{urllib.parse.quote(pipeline_id)}", None
        )
        return text

    def explain_pipeline(self, pipeline_id: str) -> str:
        _, text = self.http.request_text(
            "GET", f"/pipelines/{urllib.parse.quote(pipeline_id)}/explain", None
        )
        return text


__all__ = ["ApiError", "ManagerClient"]
