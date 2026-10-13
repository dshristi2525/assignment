from __future__ import annotations

import csv
import shutil
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from schema_drift.ai import IncidentExplainer
from schema_drift.ai.base import LLMError
from schema_drift.contract import Contract
from schema_drift.models import IncidentEvidence
from schema_drift.pipeline import Pipeline
from schema_drift.util import ROOT

BASE = ["order_id", "customer_id", "order_date", "amount", "status", "channel"]
RENAMED_HEADER = ["order_id", "customer_id", "order_date", "total_amount", "status", "channel"]


def good_row(i: int, **overrides: str) -> Dict[str, str]:
    row = {"order_id": str(i), "customer_id": f"C{i % 200 + 1:04d}", "order_date": "2026-10-01",
           "amount": f"{100 + i}.50", "status": "PLACED", "channel": "WEB"}
    row.update(overrides)
    return row


class RecordingSink:
    def __init__(self) -> None:
        self.alerts: List[Any] = []

    def send(self, alert) -> None:
        self.alerts.append(alert)


class ExplodingSink:
    def send(self, alert) -> None:
        raise OSError("webhook down")


class ScriptedClient:
    """Fake LLM: replays scripted replies (a str is returned, an Exception is raised) and records prompts."""
    name = "scripted"

    def __init__(self, *replies: Any) -> None:
        self.replies = list(replies)
        self.prompts: List[str] = []

    def complete(self, system: str, user: str) -> str:
        self.prompts.append(user)
        reply = self.replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        return reply


class PipelineTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.sink = RecordingSink()
        self.pipe = self.make_pipeline()

    def make_pipeline(self, explainer: Optional[IncidentExplainer] = None,
                      contract: Optional[Contract] = None, sinks: Optional[Sequence[Any]] = None) -> Pipeline:
        pipe = Pipeline(self.tmp / "t.db", contract or Contract.load(ROOT / "config" / "contract.json"),
                        explainer, sinks if sinks is not None else [self.sink])
        self.addCleanup(pipe.close)
        return pipe

    def write_csv(self, name: str, rows: List[Dict[str, str]], header: Optional[List[str]] = None) -> Path:
        header = header or BASE
        path = self.tmp / name
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=header, lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
        return path

    def write_bytes(self, name: str, data: bytes) -> Path:
        path = self.tmp / name
        path.write_bytes(data)
        return path

    def q(self, sql: str, *params: Any) -> List[Any]:
        return [tuple(r) for r in self.pipe.conn.execute(sql, params).fetchall()]

    def scalar(self, sql: str, *params: Any) -> Any:
        return self.q(sql, *params)[0][0]


def make_evidence(**overrides: Any) -> IncidentEvidence:
    """A day-4 style incident: amount->total_amount and status->order_status."""
    rename = lambda col, new: {  # noqa: E731
        "drift_type": "RENAME_SUSPECTED", "severity": "CRITICAL", "column": col,
        "detail": f"'{col}' missing, '{new}' looks like its replacement",
        "suggestion": f"approve-alias {new} {col}", "evidence": {"incoming": new, "name_score": 0.9, "conformance": 1.0}}
    base: Dict[str, Any] = dict(
        dataset="orders", file_name="orders_2026-10-04.csv", file_status="QUARANTINED_FILE", severity="CRITICAL",
        contract_columns={"order_id": {"type": "integer", "required": True},
                          "amount": {"type": "decimal", "required": True},
                          "status": {"type": "string", "required": True}},
        incoming_columns=["order_id", "total_amount", "order_status"],
        previous_columns=["order_id", "amount", "status"],
        events=[rename("amount", "total_amount"), rename("status", "order_status")],
        counts={"rows_read": 45, "inserted": 0, "updated": 0, "unchanged": 0, "quarantined": 45,
                "duplicates_dropped": 0},
        candidate_matrix={"total_amount": {"amount": 1.0, "status": 0.0},
                          "order_status": {"amount": 0.0, "status": 1.0}},
        sample_rows=[])
    base.update(overrides)
    return IncidentEvidence(**base)


def good_reply(**overrides: Any) -> str:
    import json
    reply: Dict[str, Any] = {
        "summary": "Two columns were renamed upstream.",
        "probable_cause": "The producer renamed amount and status without versioning the feed.",
        "impact": "All 45 rows are quarantined; nothing was loaded.",
        "recommended_actions": ["Confirm the renames with the producer.", "Approve the aliases and replay."],
        "suggested_mapping": {"total_amount": "amount", "order_status": "status"},
        "columns_mentioned": ["amount", "status", "total_amount", "order_status"],
        "confidence": 0.9,
    }
    reply.update(overrides)
    return json.dumps(reply)


__all__ = ["BASE", "RENAMED_HEADER", "good_row", "RecordingSink", "ExplodingSink", "ScriptedClient", "PipelineTestCase",
           "make_evidence", "good_reply", "LLMError"]
