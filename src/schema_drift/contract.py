"""The data contract: what the pipeline expects from the source, and how values are checked."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

SUPPORTED_TYPES = ("integer", "decimal", "date", "string")
NULL_TOKENS = frozenset({"", "null", "none", "na", "n/a", "nan"})

_INT_RE = re.compile(r"[+-]?\d+")
_DEC_RE = re.compile(r"[+-]?\d+(\.\d+)?")
_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


class ContractError(ValueError):
    """The contract file itself is invalid (a configuration problem, not a data problem)."""


def is_null(raw: Any) -> bool:
    return raw is None or str(raw).strip().lower() in NULL_TOKENS


def parse_typed(raw: Any, type_: str) -> Any:
    """Strict parsing: '1,299.00', '05/10/2026' or 'Rs 12' are NOT silently coerced."""
    text = str(raw).strip()
    if type_ == "integer":
        if not _INT_RE.fullmatch(text):
            raise ValueError(f"not an integer: {text!r}")
        return int(text)
    if type_ == "decimal":
        if not _DEC_RE.fullmatch(text):
            raise ValueError(f"not a plain decimal number: {text!r}")
        return Decimal(text)
    if type_ == "date":
        if not _DATE_RE.fullmatch(text):
            raise ValueError(f"not an ISO date (YYYY-MM-DD): {text!r}")
        try:
            return datetime.strptime(text, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError(f"not a real calendar date: {text!r}") from exc
    if type_ == "string":
        return text
    raise ValueError(f"unsupported type {type_!r}")


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    type: str
    required: bool = False
    enum: Optional[Tuple[str, ...]] = None
    pattern: Optional[str] = None
    min: Optional[float] = None

    def type_ok(self, raw: Any) -> bool:
        try:
            parse_typed(raw, self.type)
            return True
        except ValueError:
            return False

    def validate(self, raw: Any) -> Tuple[Any, Optional[Tuple[str, str]]]:
        """Return (parsed_value, None) or (None, (reason_code, detail)). Nulls are handled by the caller."""
        try:
            parsed = parse_typed(raw, self.type)
        except ValueError as exc:
            return None, ("INVALID_TYPE", str(exc))
        if self.enum is not None and parsed not in self.enum:
            return None, ("INVALID_ENUM", f"{parsed!r} is not one of {list(self.enum)}")
        if self.pattern is not None and not re.fullmatch(self.pattern, str(parsed)):
            return None, ("INVALID_PATTERN", f"{parsed!r} does not match /{self.pattern}/")
        if self.min is not None and parsed < self.min:
            return None, ("BELOW_MIN", f"{parsed} is below the minimum {self.min}")
        return parsed, None

    def describe(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"type": self.type, "required": self.required}
        if self.enum is not None:
            out["enum"] = list(self.enum)
        return out


@dataclass(frozen=True)
class Thresholds:
    file_type_drift_rate: float = 0.5      # > this share of non-conforming values => whole file is quarantined
    value_drift_alert_rate: float = 0.05   # > this share of bad rows for one reason => WARNING drift event
    rename_min_conformance: float = 0.9    # a rename candidate's values must satisfy the missing column's spec
    rename_min_name_score: float = 0.5     # ...and its name must look similar


@dataclass
class Contract:
    dataset: str
    version: int
    primary_key: str
    columns: Dict[str, ColumnSpec]
    aliases: Dict[str, str] = field(default_factory=dict)
    thresholds: Thresholds = field(default_factory=Thresholds)

    # ---- construction / persistence -------------------------------------------------
    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "Contract":
        try:
            columns: Dict[str, ColumnSpec] = {}
            for name, spec in raw["columns"].items():
                ctype = spec["type"]
                if ctype not in SUPPORTED_TYPES:
                    raise ContractError(f"column {name!r}: unsupported type {ctype!r}")
                if "min" in spec and ctype not in ("integer", "decimal"):
                    raise ContractError(f"column {name!r}: 'min' only applies to numeric types")
                enum = tuple(spec["enum"]) if spec.get("enum") is not None else None
                columns[name] = ColumnSpec(name, ctype, bool(spec.get("required", False)),
                                           enum, spec.get("pattern"), spec.get("min"))
            thresholds = Thresholds(**raw.get("thresholds", {}))
            contract = cls(raw["dataset"], int(raw["contract_version"]), raw["primary_key"],
                           columns, dict(raw.get("aliases", {})), thresholds)
        except (KeyError, TypeError) as exc:
            raise ContractError(f"malformed contract: {exc!r}") from exc
        contract._validate()
        return contract

    @classmethod
    def load(cls, path: Path) -> "Contract":
        try:
            return cls.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))
        except json.JSONDecodeError as exc:
            raise ContractError(f"{path}: invalid JSON ({exc})") from exc

    def _validate(self) -> None:
        if self.primary_key not in self.columns or not self.columns[self.primary_key].required:
            raise ContractError("primary_key must be a required column of the contract")
        for t_name in ("file_type_drift_rate", "value_drift_alert_rate",
                       "rename_min_conformance", "rename_min_name_score"):
            if not 0 < getattr(self.thresholds, t_name) <= 1:
                raise ContractError(f"threshold {t_name} must be in (0, 1]")
        for incoming, canonical in self.aliases.items():
            if canonical not in self.columns:
                raise ContractError(f"alias {incoming!r} -> {canonical!r}: target is not a contract column")
            if incoming in self.columns:
                raise ContractError(f"alias {incoming!r} shadows a real contract column")

    def to_dict(self) -> Dict[str, Any]:
        columns: Dict[str, Any] = {}
        for name, spec in self.columns.items():
            entry: Dict[str, Any] = {"type": spec.type, "required": spec.required}
            if spec.enum is not None:
                entry["enum"] = list(spec.enum)
            if spec.pattern is not None:
                entry["pattern"] = spec.pattern
            if spec.min is not None:
                entry["min"] = spec.min
            columns[name] = entry
        return {
            "dataset": self.dataset,
            "contract_version": self.version,
            "primary_key": self.primary_key,
            "thresholds": self.thresholds.__dict__.copy(),
            "columns": columns,
            "aliases": dict(self.aliases),
        }

    def save(self, path: Path) -> None:
        Path(path).write_text(json.dumps(self.to_dict(), indent=2) + "\n", encoding="utf-8")

    def fingerprint(self) -> str:
        canonical = json.dumps(self.to_dict(), sort_keys=True)
        return hashlib.sha1(canonical.encode("utf-8")).hexdigest()[:12]

    # ---- evolution ------------------------------------------------------------------
    def with_alias(self, incoming: str, canonical: str) -> "Contract":
        """A human-approved rename. Returns a new contract with the version bumped."""
        if canonical not in self.columns:
            raise ContractError(f"{canonical!r} is not a contract column")
        if incoming in self.columns:
            raise ContractError(f"{incoming!r} is already a contract column")
        if self.aliases.get(incoming, canonical) != canonical:
            raise ContractError(f"{incoming!r} is already aliased to {self.aliases[incoming]!r}")
        new = Contract(self.dataset, self.version + 1, self.primary_key, dict(self.columns),
                       {**self.aliases, incoming: canonical}, self.thresholds)
        new._validate()
        return new

    @property
    def required_columns(self) -> Tuple[str, ...]:
        return tuple(n for n, s in self.columns.items() if s.required)
