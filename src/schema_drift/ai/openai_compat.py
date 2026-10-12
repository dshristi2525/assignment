"""Client for any OpenAI-compatible /chat/completions endpoint.

Covers local models (Ollama, LM Studio, llama.cpp server) and hosted free tiers (Groq, OpenRouter, ...).
Standard library only; keys come from the environment, never from code.
"""
from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional

from .base import LLMError

_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}


class OpenAICompatClient:
    def __init__(self, base_url: str, model: str, api_key: Optional[str] = None,
                 timeout: float = 30.0, temperature: float = 0.0, max_tokens: int = 800) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.api_key = api_key
        self.timeout = timeout
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.name = f"openai_compat:{model}"

    def _opener(self) -> urllib.request.OpenerDirector:
        host = urllib.parse.urlparse(self.base_url).hostname or ""
        if host in _LOCAL_HOSTS:   # never send local-model traffic through a corporate/HTTP proxy
            return urllib.request.build_opener(urllib.request.ProxyHandler({}))
        return urllib.request.build_opener()

    def complete(self, system: str, user: str) -> str:
        body = json.dumps({
            "model": self.model,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
        }).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        request = urllib.request.Request(f"{self.base_url}/chat/completions", data=body, headers=headers)
        try:
            with self._opener().open(request, timeout=self.timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raise LLMError(f"HTTP {exc.code} from model endpoint") from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise LLMError(f"model endpoint unreachable: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise LLMError("model endpoint returned non-JSON") from exc
        try:
            content = payload["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise LLMError("unexpected response shape from model endpoint") from exc
        if not isinstance(content, str):
            raise LLMError("model returned no text content")
        return content
