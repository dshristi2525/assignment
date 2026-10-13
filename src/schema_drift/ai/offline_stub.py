"""Offline STUB that stands in for a model so the AI code path can be demonstrated without a network.

It is NOT a language model: it re-phrases the evidence deterministically. Its purpose is to exercise the
real integration (prompt -> reply -> validation -> accept / retry / fallback), including failure modes:

    --llm stub              well-formed, grounded reply
    --llm stub:garbage      prose instead of JSON            -> retried, then rule-based fallback
    --llm stub:hallucinate  mentions a column that is not in the evidence -> rejected as ungrounded
    --llm stub:lowconf      valid but confidence 0.3         -> kept, flagged, mapping suggestion dropped
    --llm stub:down         provider outage                  -> immediate fallback
"""
from __future__ import annotations

import json
from typing import Any, Dict

from .base import LLMError

MODES = ("ok", "garbage", "hallucinate", "lowconf", "down")
_MARK = "Evidence for the incident:\n"


class StubClient:
    def __init__(self, mode: str = "ok") -> None:
        if mode not in MODES:
            raise ValueError(f"unknown stub mode {mode!r}; choose one of {MODES}")
        self.mode = mode
        self.name = f"stub:{mode}"

    def complete(self, system: str, user: str) -> str:
        if self.mode == "down":
            raise LLMError("simulated provider outage (connection refused)")
        if self.mode == "garbage":
            return "I'm sorry, as an AI I think the file might have some issues. Hope this helps!"
        start = user.index(_MARK) + len(_MARK)
        end = user.find("\n\nYour previous reply", start)
        evidence: Dict[str, Any] = json.JSONDecoder().raw_decode(user[start:end if end > 0 else None])[0]

        events = evidence["events"]
        mapping = {e["evidence"]["incoming"]: e["column"] for e in events if e["drift_type"] == "RENAME_SUSPECTED"}
        cols = sorted({e["column"] for e in events if e.get("column")} | set(mapping))
        counts = evidence["counts"]
        if self.mode == "hallucinate":
            cols.append("loyalty_tier")
        kinds = ", ".join(sorted({e["drift_type"] for e in events}))
        return json.dumps({
            "summary": f"{evidence['file_name']} drifted from the contract ({kinds}).",
            "probable_cause": ("The producer renamed column(s) without versioning the feed."
                               if mapping else "The producer changed the shape or format of the feed."),
            "impact": f"{counts['quarantined']} of {counts['rows_read']} rows are quarantined; "
                      f"{counts['inserted'] + counts['updated']} were loaded.",
            "recommended_actions": ["Confirm the change with the producer.",
                                    "If intended, approve the mapping/contract change and replay."],
            "suggested_mapping": mapping,
            "columns_mentioned": cols,
            "confidence": 0.3 if self.mode == "lowconf" else 0.9,
        })
