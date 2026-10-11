"""Deterministic schema-drift detection. No AI is involved in any decision made here.

Drift classes and what the pipeline does about them
---------------------------------------------------
INFO      ALIAS_APPLIED             a human-approved rename was mapped onto the contract
WARNING   UNEXPECTED_COLUMN         new column not in the contract       -> load, keep values in extra_json
WARNING   MISSING_OPTIONAL_COLUMN   optional column vanished             -> load, never overwrite existing data
WARNING   RENAME_SUSPECTED          (optional column) probable rename    -> load, suggest alias
CRITICAL  MISSING_REQUIRED_COLUMN   required column vanished             -> quarantine the whole file
CRITICAL  RENAME_SUSPECTED          (required column) probable rename    -> quarantine the whole file, suggest alias
CRITICAL  TYPE_DRIFT                most values of a column changed type -> quarantine the whole file
CRITICAL  MALFORMED_HEADER          blank or duplicate column names      -> quarantine the whole file
"""
from __future__ import annotations

import difflib
import hashlib
import re
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, List, Set, Tuple

from .contract import ColumnSpec, Contract, is_null
from .models import CRITICAL, INFO, WARNING, DriftEvent, severity_rank
from .reader import header_problem
from .util import redact_value

SAMPLE_LIMIT = 500   # max values per column used to profile it (bounded memory / CPU on huge files)


def schema_fingerprint(columns: Iterable[str]) -> str:
    """Order-insensitive: reordering columns is not drift."""
    return hashlib.sha1("|".join(sorted(columns)).encode("utf-8")).hexdigest()[:12]


def name_score(a: str, b: str) -> float:
    """0..1 similarity between two column names (containment, token overlap, edit similarity)."""
    a, b = a.lower(), b.lower()
    if a == b:
        return 1.0
    if min(len(a), len(b)) >= 3 and (a in b or b in a):
        return 0.9
    tokens_a, tokens_b = set(re.split(r"\W+|_", a)) - {""}, set(re.split(r"\W+|_", b)) - {""}
    jaccard = len(tokens_a & tokens_b) / len(tokens_a | tokens_b) if tokens_a | tokens_b else 0.0
    return round(max(difflib.SequenceMatcher(None, a, b).ratio(), jaccard), 3)


def value_conformance(spec: ColumnSpec, values: List[str]) -> float:
    """Share of non-null values that satisfy the column's FULL spec (type, enum, pattern, min)."""
    values = [v for v in values if not is_null(v)][:SAMPLE_LIMIT]
    if not values:
        return 0.0
    return sum(1 for v in values if spec.validate(v)[1] is None) / len(values)


@dataclass
class SchemaDiff:
    header: List[str]
    alias_map: Dict[str, str]                       # incoming name -> canonical name (approved aliases)
    effective_header: List[str]
    unexpected: List[str]
    missing_required: List[str]
    missing_optional: List[str]
    candidate_matrix: Dict[str, Dict[str, float]]   # unexpected col -> missing contract col -> conformance
    events: List[DriftEvent] = field(default_factory=list)
    fingerprint: str = ""


def detect_schema_drift(header: List[str], contract: Contract,
                        values_of: Callable[[str], List[str]]) -> SchemaDiff:
    """Compare an incoming header with the contract. `values_of(col)` returns that column's raw values."""
    problem = header_problem(header)
    if problem:
        event = DriftEvent("MALFORMED_HEADER", CRITICAL, None, problem,
                           suggestion="Ask the producer to fix the header row; nothing was loaded.")
        return SchemaDiff(header, {}, header, [], [], [], {}, [event], schema_fingerprint(header))

    events: List[DriftEvent] = []
    alias_map = {h: contract.aliases[h] for h in header
                 if h in contract.aliases and contract.aliases[h] not in header}
    for incoming, canonical in alias_map.items():
        events.append(DriftEvent("ALIAS_APPLIED", INFO, canonical,
                                 f"Column '{incoming}' was mapped to '{canonical}' by an approved alias.",
                                 evidence={"incoming": incoming}))

    effective = [alias_map.get(h, h) for h in header]
    reverse = {v: k for k, v in alias_map.items()}                    # canonical -> incoming (for reading values)
    unexpected = [c for c in effective if c not in contract.columns]
    missing = [c for c in contract.columns if c not in effective]

    # --- rename inference: unexpected x missing, scored on name similarity AND value conformance ----------
    matrix: Dict[str, Dict[str, float]] = {}
    scored: List[Tuple[float, str, str, float, float]] = []
    for u in unexpected:
        vals = values_of(reverse.get(u, u))
        matrix[u] = {}
        for m in missing:
            conf = round(value_conformance(contract.columns[m], vals), 3)
            matrix[u][m] = conf
            ns = name_score(u, m)
            if conf >= contract.thresholds.rename_min_conformance and ns >= contract.thresholds.rename_min_name_score:
                scored.append((ns + conf, u, m, ns, conf))
    renames: Dict[str, Tuple[str, float, float]] = {}                 # missing col -> (incoming, name_score, conformance)
    taken: Set[str] = set()
    for _, u, m, ns, conf in sorted(scored, reverse=True):            # greedy one-to-one, best match first
        if u not in taken and m not in renames:
            renames[m] = (u, ns, conf)
            taken.add(u)

    for m in missing:
        required = contract.columns[m].required
        if m in renames:
            u, ns, conf = renames[m]
            events.append(DriftEvent(
                "RENAME_SUSPECTED", CRITICAL if required else WARNING, m,
                f"Column '{m}' is missing and new column '{u}' looks like its replacement "
                f"(name similarity {ns:.2f}, {conf:.0%} of values satisfy '{m}').",
                suggestion=f"Confirm with the producer, then: python main.py approve-alias {u} {m}",
                evidence={"incoming": u, "name_score": ns, "conformance": conf}))
        elif required:
            events.append(DriftEvent("MISSING_REQUIRED_COLUMN", CRITICAL, m,
                                     f"Required column '{m}' is absent from the file.",
                                     suggestion="Ask the producer to restore the column; nothing was loaded."))
        else:
            events.append(DriftEvent("MISSING_OPTIONAL_COLUMN", WARNING, m,
                                     f"Optional column '{m}' is absent; existing values are left untouched.",
                                     suggestion="Confirm the column was dropped intentionally."))
    for u in unexpected:
        if u not in taken:
            events.append(DriftEvent("UNEXPECTED_COLUMN", WARNING, u,
                                     f"New column '{u}' is not in the contract; its values are kept in extra_json.",
                                     suggestion="If the column is wanted, extend the contract and curated table."))

    events.sort(key=lambda e: (-severity_rank(e.severity), e.drift_type, e.column or ""))
    return SchemaDiff(header, alias_map, effective, unexpected,
                      [m for m in missing if contract.columns[m].required],
                      [m for m in missing if not contract.columns[m].required],
                      matrix, events, schema_fingerprint(header))


def detect_type_drift(rows: List[Tuple[int, Dict[str, str]]], contract: Contract,
                      present: Iterable[str]) -> List[DriftEvent]:
    """File-level check: did the *type* of a column change for (most of) the file?

    Row-level noise (a few bad values) is handled by row validation; this only fires when more than
    `file_type_drift_rate` of a column's values no longer parse, i.e. the producer changed the format.
    """
    events: List[DriftEvent] = []
    for name in present:
        spec = contract.columns.get(name)
        if spec is None:
            continue
        values = [r[name] for _, r in rows if name in r and not is_null(r[name])]
        if not values:
            continue
        bad = [v for v in values if not spec.type_ok(v)]
        rate = len(bad) / len(values)
        if rate > contract.thresholds.file_type_drift_rate:
            examples = list(dict.fromkeys(redact_value(v, 24) for v in bad))[:3]
            events.append(DriftEvent(
                "TYPE_DRIFT", CRITICAL, name,
                f"{len(bad)}/{len(values)} values ({rate:.0%}) of '{name}' no longer parse as {spec.type}; "
                f"examples: {examples}",
                suggestion="Producer changed the value format; agree a format or add a conversion rule.",
                evidence={"bad": len(bad), "total": len(values), "expected_type": spec.type, "examples": examples}))
    return events
