import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .http_client import ApiError, HttpJsonClient


class LlmError(RuntimeError):
    pass


@dataclass
class ChatCompletionsClient:
    http: HttpJsonClient
    base_url: str

    @classmethod
    def new(cls, base_url: str, api_key: str, timeout_secs: float) -> "ChatCompletionsClient":
        headers = {"Authorization": f"Bearer {api_key}"}
        http = HttpJsonClient(
            base_url=base_url.rstrip("/"), timeout_secs=timeout_secs, headers=headers
        )
        return cls(http=http, base_url=base_url.rstrip("/"))

    def _v1_path(self, path: str) -> str:
        return path

    def complete_json(
        self,
        model: str,
        messages: List[Dict[str, str]],
        temperature: float = 0.0,
        response_format_json: bool = False,
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
        }
        if response_format_json:
            body["response_format"] = {"type": "json_object"}

        # Do not hardcode a version prefix (e.g. /v1). Different providers may expose
        # chat completions under /v1 or /beta. Users should include that in base_url.
        _, resp = self.http.request_json("POST", self._v1_path("/chat/completions"), body)
        try:
            content = resp["choices"][0]["message"]["content"]
        except Exception as e:
            raise LlmError(f"unexpected chat completion response shape: {resp}") from e
        if not isinstance(content, str) or not content.strip():
            raise LlmError(f"empty chat completion content: {resp}")
        return self.parse_json_content(content)

    @staticmethod
    def parse_json_content(text: str) -> Dict[str, Any]:
        raw = text.strip()
        if raw.startswith("```"):
            raw = raw.strip("`")
        try:
            return json.loads(raw)
        except Exception as e:
            raise LlmError(f"LLM did not return valid JSON: {text}") from e


__all__ = ["ApiError", "LlmError", "ChatCompletionsClient"]
