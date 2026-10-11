from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Dict

from .util import ROOT

SQL_DIR = ROOT / "sql"


def connect(db_path: Path) -> sqlite3.Connection:
    db_path = Path(db_path)
    if str(db_path) != ":memory:":
        db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.isolation_level = None          # autocommit; the pipeline issues explicit BEGIN / COMMIT
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(load_sql("schema.sql"))


def load_sql(name: str) -> str:
    return (SQL_DIR / name).read_text(encoding="utf-8")


def named_queries(name: str = "analytics.sql") -> Dict[str, str]:
    """Split a SQL file on '-- name: <id>' markers."""
    queries: Dict[str, str] = {}
    current = None
    for line in load_sql(name).splitlines():
        match = re.match(r"--\s*name:\s*(\w+)", line)
        if match:
            current = match.group(1)
            queries[current] = ""
        elif current is not None:
            queries[current] += line + "\n"
    return {k: v.strip() for k, v in queries.items()}


def physical_columns(conn: sqlite3.Connection, table: str) -> set:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
