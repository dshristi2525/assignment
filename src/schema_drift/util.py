from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

_EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def redact_value(value: object, max_len: int = 40) -> str:
    """Make a value safe to put in an alert or an LLM prompt: mask emails, truncate."""
    text = _EMAIL.sub("<email>", str(value))
    return text if len(text) <= max_len else text[: max_len - 3] + "..."
