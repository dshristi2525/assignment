"""Ingestion orchestrator.

Flow for one file (see docs/architecture.md):

    read -> idempotency check -> analyze (schema diff + type profile)            [pure, deterministic]
         -> BEGIN
              raw layer  -> drift_events
              file-level critical drift?  yes: quarantine every row, load nothing
                                          no : validate rows -> dedupe -> stage -> upsert curated
                                               + quarantine bad rows + value-drift events
         -> COMMIT                                                               [atomic per file]
         -> incident: AI explanation (advisory) -> alert sinks                   [outside the transaction]

The data path never depends on the model or on alert delivery.
"""
from __future__ import annotations

import json
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from .alerts import AlertSink, build_alert
from .ai.explainer import IncidentExplainer, fallback_explanation
from .contract import Contract, ContractError, is_null
from .db import connect, init_schema, load_sql, physical_columns
from .drift import SAMPLE_LIMIT, SchemaDiff, detect_schema_drift, detect_type_drift, schema_fingerprint
from .models import (CRITICAL, WARNING, DriftEvent, IncidentEvidence, IngestResult, ReplayOutcome,
                     severity_rank)
from .reader import ParsedFile, ParsedRow, raw_row_from_storage, raw_row_to_storage, read_file
from .util import redact_value, utc_now
from .validate import row_fingerprint, to_storage, validate_row

_VALUE_DRIFT_EVENT = {
    "INVALID_TYPE": ("PARTIAL_TYPE_DRIFT", "Part of the feed changed value format for this column."),
    "INVALID_ENUM": ("NEW_ENUM_VALUES", "The producer sends values outside the allowed list."),
    "NULL_REQUIRED": ("NULL_SPIKE", "A required column is unexpectedly empty for many rows."),
    "INVALID_PATTERN": ("PATTERN_VIOLATIONS", "Values no longer match the expected pattern."),
    "BELOW_MIN": ("RANGE_VIOLATIONS", "Values fall outside the allowed range."),
}


@dataclass
class Analysis:
    parsed: ParsedFile
    diff: Optional[SchemaDiff]
    events: List[DriftEvent]
    canonical_rows: List[Tuple[int, Dict[str, str]]]      # rows with approved aliases applied
    critical: bool


def analyze(parsed: ParsedFile, contract: Contract) -> Analysis:
    """Pure function: decide what is wrong with a file without touching the database."""
    if parsed.error:
        code, message = parsed.error
        event = DriftEvent(code, CRITICAL, None, message, suggestion="Ask the producer to resend the file.")
        return Analysis(parsed, None, [event], [], True)

    good = [r for r in parsed.rows if r.fields is not None]

    def values_of(column: str) -> List[str]:
        out: List[str] = []
        for r in good:
            value = r.fields.get(column) if r.fields else None
            if value is not None and not is_null(value):
                out.append(value)
                if len(out) >= SAMPLE_LIMIT:
                    break
        return out

    diff = detect_schema_drift(parsed.header or [], contract, values_of)
    events = list(diff.events)
    canonical = [(r.row_num, {diff.alias_map.get(k, k): v for k, v in (r.fields or {}).items()}) for r in good]
    if not any(e.drift_type == "MALFORMED_HEADER" for e in events):
        events += detect_type_drift(canonical, contract, [c for c in diff.effective_header if c in contract.columns])
    return Analysis(parsed, diff, events, canonical, any(e.severity == CRITICAL for e in events))


class Pipeline:
    def __init__(self, db_path, contract: Contract, explainer: Optional[IncidentExplainer] = None,
                 sinks: Optional[Sequence[AlertSink]] = None) -> None:
        self.conn = connect(Path(db_path))
        init_schema(self.conn)
        self.explainer = explainer or IncidentExplainer(None)
        self.sinks = list(sinks or [])
        self.set_contract(contract)

    # ---- configuration -------------------------------------------------------------------
    def set_contract(self, contract: Contract) -> None:
        unknown = set(contract.columns) - physical_columns(self.conn, "curated_orders")
        if unknown:
            raise ContractError(f"contract columns {sorted(unknown)} have no column in curated_orders; "
                                "migrate the table before adding them to the contract")
        self.contract = contract

    def close(self) -> None:
        self.conn.close()

    # ---- public API ----------------------------------------------------------------------
    def ingest(self, path) -> IngestResult:
        path = Path(path)
        parsed = read_file(path)
        existing = self.conn.execute(
            "SELECT file_name, status FROM file_registry WHERE sha256 = ? AND status <> 'SUPERSEDED'",
            (parsed.sha256,)).fetchone()
        if existing:
            return IngestResult(path.name, existing["status"], skipped=True,
                                message=f"identical content already processed as '{existing['file_name']}' "
                                        f"(status {existing['status']}); skipped")
        return self._run(parsed, str(path), supersede_id=None)

    def replay(self) -> List[ReplayOutcome]:
        """Retry quarantined files from the RAW layer after the contract has changed (e.g. an approved alias)."""
        outcomes: List[ReplayOutcome] = []
        current = self.contract.fingerprint()
        rows = self.conn.execute(
            "SELECT file_id, file_name, source_path, sha256, header_json, contract_hash FROM file_registry "
            "WHERE status = 'QUARANTINED_FILE' ORDER BY file_id").fetchall()
        for row in rows:
            if row["contract_hash"] == current:
                outcomes.append(ReplayOutcome(row["file_name"], False,
                                              "contract unchanged since it was quarantined; nothing to retry"))
                continue
            parsed = self._parsed_from_raw(row)
            analysis = analyze(parsed, self.contract)
            if analysis.critical:
                reasons = ", ".join(sorted({e.drift_type + (f"({e.column})" if e.column else "")
                                            for e in analysis.events if e.severity == CRITICAL}))
                outcomes.append(ReplayOutcome(row["file_name"], False,
                                              f"still incompatible with the current contract: {reasons}; "
                                              "left in quarantine"))
                continue
            result = self._run(parsed, row["source_path"], supersede_id=row["file_id"])
            outcomes.append(ReplayOutcome(row["file_name"], True, "replayed from the raw layer", result))
        return outcomes

    # ---- internals -----------------------------------------------------------------------
    def _parsed_from_raw(self, registry_row) -> ParsedFile:
        rows = [raw_row_from_storage(r["row_num"], r["raw_kind"], r["raw_json"]) for r in self.conn.execute(
            "SELECT row_num, raw_kind, raw_json FROM raw_records WHERE file_id = ? ORDER BY row_num",
            (registry_row["file_id"],))]
        return ParsedFile(registry_row["file_name"], registry_row["sha256"],
                          json.loads(registry_row["header_json"]), rows)

    def _run(self, parsed: ParsedFile, source_path: Optional[str], supersede_id: Optional[int]) -> IngestResult:
        analysis = analyze(parsed, self.contract)
        result, evidence, severity, title = self._transact(parsed, analysis, source_path, supersede_id)
        if severity:
            self._raise_incident(result, evidence, severity, title)
        return result

    def _transact(self, parsed: ParsedFile, analysis: Analysis, source_path: Optional[str],
                  supersede_id: Optional[int]):
        c, contract, now = self.conn, self.contract, utc_now()
        events = list(analysis.events)
        result = IngestResult(parsed.file_name, "PROCESSING", rows_read=len(parsed.rows))
        c.execute("BEGIN IMMEDIATE")
        try:
            if supersede_id is not None:
                c.execute("UPDATE file_registry SET status = 'SUPERSEDED' WHERE file_id = ?", (supersede_id,))
                c.execute("UPDATE quarantine_records SET status = 'REPLAYED' WHERE file_id = ? AND status = 'OPEN'",
                          (supersede_id,))
            previous = self._last_good_columns()
            schema_version_id = self._register_schema_version(parsed, analysis, now)
            file_id = c.execute(
                "INSERT INTO file_registry (file_name, source_path, sha256, header_json, status, schema_version_id, "
                "contract_version, contract_hash, rows_read, processed_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (parsed.file_name, source_path, parsed.sha256,
                 json.dumps(parsed.header) if parsed.header is not None else None, "PROCESSING",
                 schema_version_id, contract.version, contract.fingerprint(), len(parsed.rows), now)).lastrowid
            result.file_id = file_id
            stored = [(r.row_num, *raw_row_to_storage(r)) for r in parsed.rows]
            c.executemany("INSERT INTO raw_records (file_id, row_num, raw_kind, raw_json) VALUES (?,?,?,?)",
                          [(file_id, rn, kind, payload) for rn, kind, payload in stored])
            payloads = {rn: payload for rn, _, payload in stored}

            if analysis.critical:
                status = "REJECTED" if parsed.error else "QUARANTINED_FILE"
                why = "; ".join(e.drift_type + (f"({e.column})" if e.column else "")
                                for e in events if e.severity == CRITICAL)
                c.executemany(
                    "INSERT INTO quarantine_records (file_id, row_num, reason_code, column_name, reason_detail, "
                    "raw_json, status, quarantined_at) VALUES (?,?,?,?,?,?, 'OPEN', ?)",
                    [(file_id, rn, "FILE_QUARANTINED", None, why, payload, now) for rn, payload in payloads.items()])
                result.quarantined = len(payloads)
            else:
                status = self._load_rows(file_id, parsed, analysis, events, result, payloads, now)

            c.executemany(
                "INSERT INTO drift_events (file_id, drift_type, severity, column_name, detail, suggestion, "
                "evidence_json, detected_at) VALUES (?,?,?,?,?,?,?,?)",
                [(file_id, e.drift_type, e.severity, e.column, e.detail, e.suggestion,
                  json.dumps(e.evidence, default=str), now) for e in events])
            c.execute("UPDATE file_registry SET status=?, rows_inserted=?, rows_updated=?, rows_unchanged=?, "
                      "rows_quarantined=?, rows_dup_dropped=? WHERE file_id=?",
                      (status, result.inserted, result.updated, result.unchanged, result.quarantined,
                       result.duplicates_dropped, file_id))
            c.execute("COMMIT")
        except BaseException:
            c.execute("ROLLBACK")
            raise

        result.status, result.events = status, events
        severity: Optional[str] = None
        if status in ("QUARANTINED_FILE", "REJECTED"):
            severity = CRITICAL
        elif status == "LOADED_WITH_WARNINGS":
            severity = WARNING
        evidence = self._build_evidence(parsed, analysis, events, result, severity or "INFO", previous) if severity else None
        kinds = ", ".join(sorted({e.drift_type for e in events if e.severity != "INFO"}))
        title = {"QUARANTINED_FILE": f"{parsed.file_name}: file quarantined ({kinds})",
                 "REJECTED": f"{parsed.file_name}: file rejected ({kinds})",
                 "LOADED_WITH_WARNINGS": f"{parsed.file_name}: loaded with schema warnings ({kinds})"}.get(status, "")
        return result, evidence, severity, title

    def _load_rows(self, file_id: int, parsed: ParsedFile, analysis: Analysis, events: List[DriftEvent],
                   result: IngestResult, payloads: Dict[int, str], now: str) -> str:
        c, contract = self.conn, self.contract
        pk, diff = contract.primary_key, analysis.diff
        quarantine: List[Tuple[int, str, Optional[str], str]] = []           # (row_num, code, column, detail)
        reasons: Counter = Counter()
        first_example: Dict[Tuple[str, Optional[str]], str] = {}

        for row in parsed.rows:                                               # rows that do not fit the header
            if row.fields is None:
                quarantine.append((row.row_num, "MALFORMED_ROW", None,
                                   f"expected {len(parsed.header or [])} fields, got {len(row.raw_fields)}"))
        valid = []
        for row_num, row in analysis.canonical_rows:
            outcome = validate_row(row_num, row, contract)
            if outcome.errors:
                first = outcome.errors[0]
                quarantine.append((row_num, first.code, first.column, "; ".join(e.detail for e in outcome.errors)))
                for err in outcome.errors:
                    reasons[(err.code, err.column)] += 1
                    first_example.setdefault((err.code, err.column), err.detail)
            else:
                valid.append(outcome)

        seen: Dict[object, str] = {}                                          # in-file de-duplication
        unique = []
        for outcome in valid:
            key, fingerprint = outcome.values[pk], row_fingerprint(outcome)
            if key not in seen:
                seen[key] = fingerprint
                unique.append(outcome)
            elif seen[key] == fingerprint:
                result.duplicates_dropped += 1                                # exact repeat: harmless, count it
            else:
                quarantine.append((outcome.row_num, "DUPLICATE_KEY_CONFLICT", pk,
                                   f"{pk}={key} repeats with different values; first occurrence kept"))

        total, threshold = max(len(parsed.rows), 1), contract.thresholds.value_drift_alert_rate
        for (code, column), count in sorted(reasons.items(), key=lambda kv: (kv[0][0], kv[0][1] or "")):
            if code in _VALUE_DRIFT_EVENT and count / total > threshold:
                drift_type, meaning = _VALUE_DRIFT_EVENT[code]
                events.append(DriftEvent(
                    drift_type, WARNING, column,
                    f"{count}/{total} rows ({count / total:.0%}) have a bad '{column}' ({code}); "
                    f"e.g. {redact_value(first_example[(code, column)], 90)}",
                    suggestion=f"{meaning} Review quarantined rows; agree a fix with the producer.",
                    evidence={"rows": count, "total": total, "reason": code}))
        malformed = sum(1 for _, code, _, _ in quarantine if code == "MALFORMED_ROW")
        if malformed / total > threshold:
            events.append(DriftEvent("MALFORMED_ROW_SPIKE", WARNING, None,
                                     f"{malformed}/{total} rows have the wrong number of fields.",
                                     suggestion="Check delimiters / quoting in the producer export.",
                                     evidence={"rows": malformed, "total": total}))

        c.execute("DELETE FROM stg_orders")
        c.executemany(
            "INSERT INTO stg_orders (order_id, customer_id, order_date, amount, status, channel, extra_json) "
            "VALUES (?,?,?,?,?,?,?)",
            [(to_storage(o.values["order_id"]), o.values["customer_id"], to_storage(o.values["order_date"]),
              to_storage(o.values["amount"]), o.values["status"], o.values.get("channel"),
              json.dumps(o.extra, sort_keys=True) if o.extra else None) for o in unique])
        params = {"file_id": file_id, "now": now,
                  "has_channel": int("channel" in (diff.effective_header if diff else [])),
                  "has_extra": int(bool(diff and diff.unexpected))}
        effect = c.execute(load_sql("count_load_effect.sql"), params).fetchone()
        c.execute(load_sql("load_curated.sql"), params)
        c.execute("DELETE FROM stg_orders")
        result.inserted, result.updated, result.unchanged = effect["inserted"], effect["updated"], effect["unchanged"]

        c.executemany(
            "INSERT INTO quarantine_records (file_id, row_num, reason_code, column_name, reason_detail, raw_json, "
            "status, quarantined_at) VALUES (?,?,?,?,?,?, 'OPEN', ?)",
            [(file_id, rn, code, col, detail, payloads[rn], now) for rn, code, col, detail in sorted(quarantine)])
        result.quarantined = len(quarantine)
        return "LOADED_WITH_WARNINGS" if any(severity_rank(e.severity) >= 1 for e in events) else "LOADED"

    def _register_schema_version(self, parsed: ParsedFile, analysis: Analysis, now: str) -> Optional[int]:
        if parsed.header is None:
            return None
        fingerprint = analysis.diff.fingerprint if analysis.diff else schema_fingerprint(parsed.header)
        row = self.conn.execute("SELECT schema_version_id FROM schema_versions WHERE fingerprint = ?",
                                (fingerprint,)).fetchone()
        if row:
            return row[0]
        return self.conn.execute(
            "INSERT INTO schema_versions (fingerprint, columns_json, first_seen_file, first_seen_at) VALUES (?,?,?,?)",
            (fingerprint, json.dumps(sorted(parsed.header)), parsed.file_name, now)).lastrowid

    def _last_good_columns(self) -> Optional[List[str]]:
        row = self.conn.execute(
            "SELECT header_json FROM file_registry WHERE status IN ('LOADED','LOADED_WITH_WARNINGS') "
            "ORDER BY file_id DESC LIMIT 1").fetchone()
        return json.loads(row["header_json"]) if row and row["header_json"] else None

    def _build_evidence(self, parsed: ParsedFile, analysis: Analysis, events: List[DriftEvent],
                        result: IngestResult, severity: str, previous: Optional[List[str]]) -> IncidentEvidence:
        samples: List[Dict[str, str]] = []
        if result.status in ("QUARANTINED_FILE", "REJECTED"):
            source = [r.fields for r in parsed.rows if r.fields is not None][:3]
        else:
            source = []
            for row in self.conn.execute(
                    "SELECT raw_json FROM quarantine_records WHERE file_id = ? AND status = 'OPEN' "
                    "ORDER BY row_num LIMIT 3", (result.file_id,)):
                data = json.loads(row["raw_json"])
                if isinstance(data, dict):
                    source.append(data)
        for fields in source:
            samples.append({k: redact_value(v) for k, v in fields.items()})
        return IncidentEvidence(
            dataset=self.contract.dataset, file_name=parsed.file_name, file_status=result.status, severity=severity,
            contract_columns={n: s.describe() for n, s in self.contract.columns.items()},
            incoming_columns=list(parsed.header or []), previous_columns=previous,
            events=[e.to_dict() for e in events if e.severity != "INFO"],
            counts={"rows_read": result.rows_read, "inserted": result.inserted, "updated": result.updated,
                    "unchanged": result.unchanged, "quarantined": result.quarantined,
                    "duplicates_dropped": result.duplicates_dropped},
            candidate_matrix=analysis.diff.candidate_matrix if analysis.diff else {},
            sample_rows=samples, rename_min_conformance=self.contract.thresholds.rename_min_conformance)

    def _raise_incident(self, result: IngestResult, evidence: IncidentEvidence, severity: str, title: str) -> None:
        try:
            explanation = self.explainer.explain(evidence)
        except Exception as exc:                       # the explainer must never be able to break ingestion
            explanation = fallback_explanation(evidence, f"EXPLAINER_CRASH: {type(exc).__name__}")
        now = utc_now()
        incident_id = self.conn.execute(
            "INSERT INTO incidents (file_id, severity, title, ai_status, ai_provider, ai_fallback_reason, "
            "explanation_json, created_at) VALUES (?,?,?,?,?,?,?,?)",
            (result.file_id, severity, title, explanation.status, explanation.provider, explanation.fallback_reason,
             json.dumps(explanation.to_dict()), now)).lastrowid
        result.incident_id, result.explanation = incident_id, explanation
        alert = build_alert(incident_id, severity, title, result.file_name, result.status, evidence.counts,
                            evidence.events, explanation, now)
        for sink in self.sinks:
            try:
                sink.send(alert)
            except Exception as exc:                   # alert delivery is best-effort; the incident row is the record
                print(f"[warn] alert sink {type(sink).__name__} failed: {exc}", file=sys.stderr)
