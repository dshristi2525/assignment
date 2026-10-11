"""Read a delimited file into a ParsedFile without ever raising on bad *data*.

Problems that make the whole file unusable (empty, undecodable, malformed header) are reported as
`error`; row-level problems (wrong number of fields) are kept as unparseable rows.
"""
from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .util import sha256_bytes


@dataclass
class ParsedRow:
    row_num: int                                  # line number in the source file
    fields: Optional[Dict[str, str]]              # None => row could not be mapped onto the header
    raw_fields: List[str] = field(default_factory=list)


@dataclass
class ParsedFile:
    file_name: str
    sha256: str
    header: Optional[List[str]]
    rows: List[ParsedRow]
    error: Optional[Tuple[str, str]] = None       # (drift_type, message): file cannot be processed at all


def header_problem(header: List[str]) -> Optional[str]:
    if any(h == "" for h in header):
        return "header contains an empty column name"
    dupes = sorted({h for h in header if header.count(h) > 1})
    if dupes:
        return f"header contains duplicate column names: {dupes}"
    return None


def read_file(path: Path) -> ParsedFile:
    path = Path(path)
    data = path.read_bytes()
    sha = sha256_bytes(data)
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        return ParsedFile(path.name, sha, None, [], ("UNREADABLE_FILE", f"file is not valid UTF-8 ({exc.reason})"))
    if not text.strip():
        return ParsedFile(path.name, sha, None, [], ("EMPTY_FILE", "file contains no header and no data"))

    reader = csv.reader(io.StringIO(text, newline=""))
    rows: List[ParsedRow] = []
    try:
        header = [h.strip() for h in next(reader)]
        bad_header = header_problem(header) is not None
        for raw in reader:
            if not raw:                     # blank line
                continue
            if bad_header or len(raw) != len(header):
                rows.append(ParsedRow(reader.line_num, None, raw))
            else:
                rows.append(ParsedRow(reader.line_num, dict(zip(header, (v.strip() for v in raw))), raw))
    except csv.Error as exc:
        return ParsedFile(path.name, sha, None, [], ("UNREADABLE_FILE", f"CSV parsing failed: {exc}"))
    return ParsedFile(path.name, sha, header, rows)


# ---- raw layer <-> ParsedFile (used by replay: the source file does not need to exist any more) ----
def raw_row_to_storage(row: ParsedRow) -> Tuple[str, str]:
    if row.fields is not None:
        return "record", json.dumps(row.fields, ensure_ascii=False)
    return "fields", json.dumps(row.raw_fields, ensure_ascii=False)


def raw_row_from_storage(row_num: int, kind: str, payload: str) -> ParsedRow:
    data: Any = json.loads(payload)
    if kind == "record":
        return ParsedRow(row_num, data, list(data.values()))
    return ParsedRow(row_num, None, data)
