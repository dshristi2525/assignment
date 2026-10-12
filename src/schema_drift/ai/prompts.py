"""Prompt construction. The model sees metadata and a few redacted samples, never the full dataset."""
from __future__ import annotations

import json

from ..models import IncidentEvidence

SYSTEM_PROMPT = """You are a data-pipeline incident analyst.
A deterministic schema-drift detector has ALREADY decided what happened to the file and what was quarantined.
You only explain the incident to the on-call data engineer. You never decide whether data is loaded.

Rules:
- Use ONLY facts present in the evidence JSON. Never invent column names, counts, systems or people.
- Refer to columns only by the exact names that appear in the evidence, and list every column you mention in "columns_mentioned".
- "suggested_mapping" maps an INCOMING column to a CONTRACT column. Include a pair only when both its name similarity
  and its value_conformance in candidate_matrix support it; otherwise return {}.
- Never recommend loading quarantined data without human approval.
- If the evidence is ambiguous, say so and lower "confidence" (0.0 to 1.0).
- Reply with ONE JSON object and nothing else, with exactly these keys:
  summary (string), probable_cause (string), impact (string), recommended_actions (array of strings),
  suggested_mapping (object), columns_mentioned (array of strings), confidence (number)."""


def build_user_prompt(evidence: IncidentEvidence, rejection_reason: str = "") -> str:
    prompt = "Evidence for the incident:\n" + json.dumps(evidence.to_dict(), indent=2, default=str)
    if rejection_reason:
        prompt += (f"\n\nYour previous reply was rejected by an automatic validator: {rejection_reason}\n"
                   "Reply again with ONLY the corrected JSON object.")
    return prompt
