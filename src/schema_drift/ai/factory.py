"""Choose an LLM client from configuration. Returns None when no model is configured."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Optional, Tuple

from ..util import ROOT
from .base import LLMClient
from .offline_stub import StubClient
from .openai_compat import OpenAICompatClient

# provider -> (default base_url, default model). Model ids change often: override with LLM_MODEL.
PRESETS: Dict[str, Tuple[Optional[str], Optional[str]]] = {
    "ollama": ("http://localhost:11434/v1", "llama3.2"),
    "groq": ("https://api.groq.com/openai/v1", None),
    "openrouter": ("https://openrouter.ai/api/v1", None),
    "openai_compat": (None, None),
}


class LLMConfigError(ValueError):
    pass


def load_env_file(path: Path = ROOT / ".env") -> None:
    """Minimal KEY=VALUE loader (no dependency). Never overrides variables already set."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.split("#", 1)[0].strip()
        if "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def make_client(provider: Optional[str] = None) -> Optional[LLMClient]:
    load_env_file()
    provider = (provider or os.getenv("LLM_PROVIDER") or "none").strip().lower()
    if provider in ("", "none", "off"):
        return None
    if provider == "stub" or provider.startswith("stub:"):            # offline demo double, not a real model
        try:
            return StubClient(provider.partition(":")[2] or "ok")
        except ValueError as exc:
            raise LLMConfigError(str(exc)) from exc
    if provider not in PRESETS:
        raise LLMConfigError(f"unknown LLM_PROVIDER {provider!r}; choose one of {['none', *PRESETS]}")
    default_url, default_model = PRESETS[provider]
    base_url = os.getenv("LLM_BASE_URL") or default_url
    model = os.getenv("LLM_MODEL") or default_model
    if not base_url or not model:
        raise LLMConfigError(f"provider {provider!r} needs LLM_BASE_URL and LLM_MODEL to be set")
    return OpenAICompatClient(base_url, model, os.getenv("LLM_API_KEY"),
                              timeout=float(os.getenv("LLM_TIMEOUT_SECONDS", "30")))
