"""Alert rendering and delivery. Sinks are pluggable; a failing sink never breaks ingestion."""
from __future__ import annotations

import json
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Protocol

from .models import Explanation


@dataclass
class Alert:
    incident_id: int
    severity: str
    title: str
    file_name: str
    file_status: str
    counts: Dict[str, int]
    events: List[Dict[str, Any]]
    explanation: Dict[str, Any]
    created_at: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class AlertSink(Protocol):
    def send(self, alert: Alert) -> None: ...


def _ai_banner(exp: Dict[str, Any]) -> str:
    bits = [f"status={exp['status']}", f"provider={exp['provider']}"]
    if exp.get("confidence") is not None:
        bits.append(f"confidence={exp['confidence']:.2f}")
    if exp.get("fallback_reason"):
        bits.append(f"reason={exp['fallback_reason']}")
    return ", ".join(bits)


def render_console(alert: Alert) -> str:
    c, exp = alert.counts, alert.explanation
    lines = [
        "=" * 78,
        f"ALERT [{alert.severity}] {alert.title}",
        f"  incident #{alert.incident_id}  file={alert.file_name}  status={alert.file_status}",
        f"  rows read={c.get('rows_read', 0)}  loaded={c.get('inserted', 0) + c.get('updated', 0)}  "
        f"quarantined={c.get('quarantined', 0)}  duplicates dropped={c.get('duplicates_dropped', 0)}",
        "  Drift detected:",
    ]
    for e in alert.events:
        if e["severity"] != "INFO":
            lines.append(f"    [{e['severity']}] {e['drift_type']}" + (f" ({e['column']})" if e.get("column") else "")
                         + f": {e['detail']}")
    lines += ["", f"  Incident explanation ({_ai_banner(exp)})",
              f"    Summary : {exp['summary']}", f"    Cause   : {exp['probable_cause']}",
              f"    Impact  : {exp['impact']}", "    Actions :"]
    lines += [f"      - {a}" for a in exp["recommended_actions"]]
    if exp["suggested_mapping"]:
        lines.append("    Suggested mapping (needs human approval): "
                     + ", ".join(f"{k} -> {v}" for k, v in exp["suggested_mapping"].items()))
    lines.append("=" * 78)
    return "\n".join(lines)


def render_markdown(alert: Alert) -> str:
    c, exp = alert.counts, alert.explanation
    out = [f"# [{alert.severity}] {alert.title}", "",
           f"- **Incident:** #{alert.incident_id}   **File:** `{alert.file_name}`   **Status:** {alert.file_status}",
           f"- **Rows:** read {c.get('rows_read', 0)}, loaded {c.get('inserted', 0) + c.get('updated', 0)}, "
           f"quarantined {c.get('quarantined', 0)}, duplicates dropped {c.get('duplicates_dropped', 0)}",
           f"- **Raised:** {alert.created_at}", "", "## Drift events", "",
           "| Severity | Type | Column | Detail |", "|---|---|---|---|"]
    for e in alert.events:
        if e["severity"] != "INFO":
            out.append(f"| {e['severity']} | {e['drift_type']} | {e.get('column') or '-'} | "
                       f"{e['detail'].replace('|', '/')} |")
    out += ["", f"## Incident explanation ({_ai_banner(exp)})", "",
            f"**Summary.** {exp['summary']}", "", f"**Probable cause.** {exp['probable_cause']}", "",
            f"**Impact.** {exp['impact']}", "", "**Recommended actions**", ""]
    out += [f"{i}. {a}" for i, a in enumerate(exp["recommended_actions"], 1)]
    if exp["suggested_mapping"]:
        out += ["", "**Suggested column mapping (requires human approval):** "
                + ", ".join(f"`{k}` -> `{v}`" for k, v in exp["suggested_mapping"].items())]
    return "\n".join(out) + "\n"


class ConsoleSink:
    def __init__(self, stream=None) -> None:
        self.stream = stream or sys.stdout

    def send(self, alert: Alert) -> None:
        print(render_console(alert), file=self.stream)


class FileSink:
    """Writes <out_dir>/<file>.alert.json and <file>.incident.md."""

    def __init__(self, out_dir: Path) -> None:
        self.out_dir = Path(out_dir)

    def send(self, alert: Alert) -> None:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        stem = re.sub(r"[^A-Za-z0-9._-]", "_", alert.file_name)
        (self.out_dir / f"{stem}.alert.json").write_text(
            json.dumps(alert.to_dict(), indent=2, default=str), encoding="utf-8")
        (self.out_dir / f"{stem}.incident.md").write_text(render_markdown(alert), encoding="utf-8")


def build_alert(incident_id: int, severity: str, title: str, file_name: str, file_status: str,
                counts: Dict[str, int], events: List[Dict[str, Any]], explanation: Explanation,
                created_at: str) -> Alert:
    return Alert(incident_id, severity, title, file_name, file_status, counts, events,
                 explanation.to_dict(), created_at)
