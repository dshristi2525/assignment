"""Row-level validation against the contract."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional

from .contract import Contract, is_null


@dataclass(frozen=True)
class RowError:
    code: str
    column: Optional[str]
    detail: str


@dataclass
class RowOutcome:
    row_num: int
    values: Dict[str, Any]                      # parsed value per contract column (None when null/absent)
    extra: Dict[str, str]                       # values of columns that are not in the contract
    errors: List[RowError] = field(default_factory=list)


def validate_row(row_num: int, row: Dict[str, str], contract: Contract) -> RowOutcome:
    values: Dict[str, Any] = {}
    errors: List[RowError] = []
    for name, spec in contract.columns.items():
        raw = row.get(name)
        if is_null(raw):
            values[name] = None
            if spec.required:
                errors.append(RowError("NULL_REQUIRED", name, f"required column '{name}' is empty"))
            continue
        parsed, err = spec.validate(raw)
        values[name] = parsed
        if err:
            errors.append(RowError(err[0], name, f"{name}: {err[1]}"))
    extra = {k: v for k, v in row.items() if k not in contract.columns and not is_null(v)}
    return RowOutcome(row_num, values, extra, errors)


def _canon(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def row_fingerprint(outcome: RowOutcome) -> str:
    """Used only to tell an exact duplicate row from a conflicting one within the same file."""
    parts = [f"{k}={_canon(v)}" for k, v in sorted(outcome.values.items())]
    parts.append("extra=" + json.dumps(outcome.extra, sort_keys=True))
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()


def to_storage(value: Any) -> Any:
    """Parsed value -> something sqlite3 can bind."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, date):
        return value.isoformat()
    return value
