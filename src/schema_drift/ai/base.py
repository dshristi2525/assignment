"""The only thing the rest of the pipeline knows about an LLM."""
from __future__ import annotations

from typing import Protocol, runtime_checkable


class LLMError(Exception):
    """Transport / provider failure (timeout, HTTP error, malformed provider payload)."""


@runtime_checkable
class LLMClient(Protocol):
    name: str

    def complete(self, system: str, user: str) -> str:
        """Return the model's raw text reply. Must raise LLMError on any failure."""
        ...
