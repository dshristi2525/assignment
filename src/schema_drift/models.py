"""Plain data classes shared across the pipeline. No behaviour that touches I/O lives here."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Set

INFO, WARNING, CRITICAL = "INFO", "WARNING", "CRITICAL"
_RANK = {INFO: 0, WARNING: 1, CRITICAL: 2}


def severity_rank(severity: str) -> int:
    return _RANK[severity]


def max_severity(events: Iterable["DriftEvent"]) -> Optional[str]:
    return max((e.severity for e in events), key=severity_rank, default=None)


@dataclass
class DriftEvent:
    drift_type: str
    severity: str
    column: Optional[str]
    detail: str
    suggestion: Optional[str] = None
    evidence: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class IncidentEvidence:
    """Everything the AI explainer is allowed to see. Metadata and a few redacted sample values only."""

    dataset: str
    file_name: str
    file_status: str
    severity: str
    contract_columns: Dict[str, Dict[str, Any]]
    incoming_columns: List[str]
    previous_columns: Optional[List[str]]
    events: List[Dict[str, Any]]
    counts: Dict[str, int]
    candidate_matrix: Dict[str, Dict[str, float]]  # unexpected incoming col -> missing contract col -> conformance
    sample_rows: List[Dict[str, str]]
    rename_min_conformance: float = 0.9

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def known_columns(self) -> Set[str]:
        known = set(self.contract_columns) | set(self.incoming_columns)
        if self.previous_columns:
            known |= set(self.previous_columns)
        return known


@dataclass
class Explanation:
    summary: str
    probable_cause: str
    impact: str
    recommended_actions: List[str]
    suggested_mapping: Dict[str, str]
    columns_mentioned: List[str]
    confidence: Optional[float]
    status: str  # OK | LOW_CONFIDENCE | FALLBACK
    provider: str
    fallback_reason: Optional[str] = None
    attempts: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class IngestResult:
    file_name: str
    status: str
    file_id: Optional[int] = None
    rows_read: int = 0
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    quarantined: int = 0
    duplicates_dropped: int = 0
    events: List[DriftEvent] = field(default_factory=list)
    incident_id: Optional[int] = None
    explanation: Optional[Explanation] = None
    skipped: bool = False
    message: str = ""

    @property
    def rows_loaded(self) -> int:
        return self.inserted + self.updated


@dataclass
class ReplayOutcome:
    file_name: str
    replayed: bool
    message: str
    result: Optional[IngestResult] = None
