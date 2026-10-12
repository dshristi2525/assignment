"""AI-assisted incident explanation.

The model is ADVISORY and sits off the data path: loading and quarantining are decided by
deterministic code before the model is ever called. Whatever the model returns is treated as
untrusted input and validated; anything unusable degrades to a deterministic explanation.

Outcomes (Explanation.status)
  OK              reply parsed, schema-valid, grounded in the evidence, confidence >= threshold
  LOW_CONFIDENCE  valid but the model is unsure: text kept, mapping suggestion dropped, flagged for a human
  FALLBACK        no model configured, transport error, or reply rejected twice -> rule-based text
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from ..models import CRITICAL, Explanation, IncidentEvidence
from .base import LLMClient, LLMError
from .prompts import SYSTEM_PROMPT, build_user_prompt

MAX_TEXT = 1200
MAX_ACTIONS = 6
REQUIRED_KEYS = ("summary", "probable_cause", "impact", "recommended_actions",
                 "suggested_mapping", "columns_mentioned", "confidence")


class ExplanationRejected(Exception):
    """The model replied, but the reply cannot be trusted or used."""


# ---------------------------------------------------------------------------------------------
# Parsing + validation of the untrusted model reply
# ---------------------------------------------------------------------------------------------
def extract_json_object(text: str) -> Dict[str, Any]:
    start = text.find("{")
    if start < 0:
        raise ExplanationRejected("NO_JSON: reply contains no JSON object")
    try:
        obj, _ = json.JSONDecoder().raw_decode(text[start:])
    except json.JSONDecodeError as exc:
        raise ExplanationRejected(f"INVALID_JSON: {exc.msg}") from exc
    if not isinstance(obj, dict):
        raise ExplanationRejected("INVALID_JSON: top-level value is not an object")
    return obj


def _text(obj: Dict[str, Any], key: str) -> str:
    value = obj[key]
    if not isinstance(value, str) or not value.strip():
        raise ExplanationRejected(f"SCHEMA: '{key}' must be a non-empty string")
    return value.strip()[:MAX_TEXT]


def _str_list(obj: Dict[str, Any], key: str) -> List[str]:
    value = obj[key]
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise ExplanationRejected(f"SCHEMA: '{key}' must be an array of strings")
    return [v.strip()[:MAX_TEXT] for v in value if v.strip()]


def validate_reply(text: str, evidence: IncidentEvidence) -> Dict[str, Any]:
    obj = extract_json_object(text)
    missing = [k for k in REQUIRED_KEYS if k not in obj]
    if missing:
        raise ExplanationRejected(f"SCHEMA: missing keys {missing}")

    actions = _str_list(obj, "recommended_actions")[:MAX_ACTIONS]
    if not actions:
        raise ExplanationRejected("SCHEMA: 'recommended_actions' is empty")
    mentioned = _str_list(obj, "columns_mentioned")
    mapping = obj["suggested_mapping"]
    if not isinstance(mapping, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in mapping.items()):
        raise ExplanationRejected("SCHEMA: 'suggested_mapping' must be an object of string -> string")
    confidence = obj["confidence"]
    if isinstance(confidence, bool) or not isinstance(confidence, (int, float)) or not 0.0 <= confidence <= 1.0:
        raise ExplanationRejected("SCHEMA: 'confidence' must be a number between 0 and 1")

    # --- grounding: the model may only talk about columns that exist in the evidence ---------------
    unknown = sorted(c for c in mentioned if c not in evidence.known_columns())
    if unknown:
        raise ExplanationRejected(f"UNGROUNDED_COLUMNS: {unknown} do not appear in the evidence")
    targets = list(mapping.values())
    if len(set(targets)) != len(targets):
        raise ExplanationRejected("MAPPING_CONFLICT: several incoming columns mapped to one contract column")
    for incoming, canonical in mapping.items():
        if incoming not in evidence.candidate_matrix or canonical not in evidence.candidate_matrix[incoming]:
            raise ExplanationRejected(f"UNGROUNDED_MAPPING: {incoming!r} -> {canonical!r} is not a rename "
                                      "candidate (must be an unexpected incoming column and a missing contract column)")
        if evidence.candidate_matrix[incoming][canonical] < evidence.rename_min_conformance:
            raise ExplanationRejected(f"MAPPING_TYPE_MISMATCH: values of {incoming!r} do not satisfy the "
                                      f"contract for {canonical!r}")
    return {
        "summary": _text(obj, "summary"), "probable_cause": _text(obj, "probable_cause"),
        "impact": _text(obj, "impact"), "recommended_actions": actions,
        "suggested_mapping": dict(mapping), "columns_mentioned": mentioned, "confidence": float(confidence),
    }


# ---------------------------------------------------------------------------------------------
# Deterministic fallback (also what you get with no model configured)
# ---------------------------------------------------------------------------------------------
_CAUSES = {
    "RENAME_SUSPECTED": "The producer appears to have renamed column(s) without versioning the feed.",
    "MISSING_REQUIRED_COLUMN": "The producer dropped a column the contract requires.",
    "TYPE_DRIFT": "The producer changed the value format of a column for most of the file.",
    "MALFORMED_HEADER": "The header row is malformed (blank or duplicate column names).",
    "UNEXPECTED_COLUMN": "The producer added a column that the contract does not know about yet.",
    "MISSING_OPTIONAL_COLUMN": "The producer stopped sending an optional column.",
    "EMPTY_FILE": "The producer delivered an empty file.",
    "UNREADABLE_FILE": "The file could not be decoded or parsed.",
    "PARTIAL_TYPE_DRIFT": "Part of the feed switched value format for a column.",
    "NEW_ENUM_VALUES": "The producer started sending values outside the allowed list.",
    "NULL_SPIKE": "A required column is unexpectedly empty for many rows.",
}


def fallback_explanation(evidence: IncidentEvidence, reason: str, provider: str = "none",
                         attempts: int = 0) -> Explanation:
    events = evidence.events
    kinds = sorted({e["drift_type"] for e in events})
    cols = sorted({e["column"] for e in events if e.get("column")})
    counts = evidence.counts
    quarantined, read = counts.get("quarantined", 0), counts.get("rows_read", 0)

    causes: List[str] = []
    for e in sorted(events, key=lambda ev: ev["severity"] != CRITICAL):      # critical first, otherwise stable
        text = _CAUSES.get(e["drift_type"])
        if text and text not in causes:
            causes.append(text)
    cause = " ".join(causes[:2]) or "Source data deviates from the contract."
    if evidence.file_status in ("QUARANTINED_FILE", "REJECTED"):
        impact = f"The whole file was held back: {quarantined} of {read} rows are in quarantine and nothing was loaded."
    else:
        impact = (f"{counts.get('inserted', 0) + counts.get('updated', 0)} rows were loaded; "
                  f"{quarantined} of {read} rows were quarantined.")

    actions: List[str] = []
    mapping: Dict[str, str] = {}
    for e in events:
        if e.get("suggestion") and e["suggestion"] not in actions:
            actions.append(e["suggestion"])
        if e["drift_type"] == "RENAME_SUSPECTED":
            mapping[e["evidence"]["incoming"]] = e["column"]
    if quarantined:
        actions.append("Review the open rows in quarantine_records before deciding to replay or discard them.")

    return Explanation(
        summary=f"{evidence.file_name}: {evidence.file_status.replace('_', ' ').lower()} "
                f"({', '.join(kinds) or 'no drift events'}"
                f"{'; columns: ' + ', '.join(cols) if cols else ''}).",
        probable_cause=cause, impact=impact, recommended_actions=actions[:MAX_ACTIONS],
        suggested_mapping=mapping, columns_mentioned=cols, confidence=None,
        status="FALLBACK", provider=provider, fallback_reason=reason, attempts=attempts)


# ---------------------------------------------------------------------------------------------
class IncidentExplainer:
    def __init__(self, client: Optional[LLMClient], min_confidence: float = 0.6, max_attempts: int = 2) -> None:
        self.client = client
        self.min_confidence = min_confidence
        self.max_attempts = max(1, max_attempts)

    @property
    def provider(self) -> str:
        return self.client.name if self.client else "none"

    def explain(self, evidence: IncidentEvidence) -> Explanation:
        if self.client is None:
            return fallback_explanation(evidence, "NO_LLM_CONFIGURED")
        rejection = ""
        for attempt in range(1, self.max_attempts + 1):
            try:
                reply = self.client.complete(SYSTEM_PROMPT, build_user_prompt(evidence, rejection))
            except LLMError as exc:                      # transport problem: a retry will not help quickly
                return fallback_explanation(evidence, f"LLM_ERROR: {exc}", self.provider, attempt)
            except Exception as exc:                     # a buggy client must never break ingestion
                return fallback_explanation(evidence, f"LLM_CLIENT_CRASH: {type(exc).__name__}", self.provider, attempt)
            try:
                valid = validate_reply(reply, evidence)
            except ExplanationRejected as exc:
                rejection = str(exc)
                continue
            return self._finish(valid, attempt)
        return fallback_explanation(evidence, f"LLM_REPLY_REJECTED: {rejection}", self.provider, self.max_attempts)

    def _finish(self, valid: Dict[str, Any], attempts: int) -> Explanation:
        low = valid["confidence"] < self.min_confidence
        actions = valid["recommended_actions"]
        mapping = valid["suggested_mapping"]
        if low:
            mapping = {}                                  # never offer an actionable mapping the model is unsure about
            actions = ["LOW CONFIDENCE AI analysis: verify against the drift events before acting."] + actions
        return Explanation(
            summary=valid["summary"], probable_cause=valid["probable_cause"], impact=valid["impact"],
            recommended_actions=actions[:MAX_ACTIONS + 1], suggested_mapping=mapping,
            columns_mentioned=valid["columns_mentioned"], confidence=valid["confidence"],
            status="LOW_CONFIDENCE" if low else "OK", provider=self.provider, attempts=attempts)
