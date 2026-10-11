-- Schema Drift Handler: data model (SQLite)
--
--   RAW        file_registry, raw_records        every byte received, replayable
--   STAGING    stg_orders                        validated rows of the file being loaded
--   CURATED    curated_orders                    clean, de-duplicated business table
--   CONTROL    schema_versions, drift_events,    what changed, what was rejected,
--              quarantine_records, incidents     and what a human was told

CREATE TABLE IF NOT EXISTS schema_versions (
    schema_version_id INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint       TEXT    NOT NULL UNIQUE,      -- hash of the sorted column names
    columns_json      TEXT    NOT NULL,
    first_seen_file   TEXT    NOT NULL,
    first_seen_at     TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS file_registry (
    file_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    file_name         TEXT    NOT NULL,
    source_path       TEXT,
    sha256            TEXT    NOT NULL,
    header_json       TEXT,
    status            TEXT    NOT NULL CHECK (status IN
                        ('LOADED', 'LOADED_WITH_WARNINGS', 'QUARANTINED_FILE', 'REJECTED', 'SUPERSEDED')),
    schema_version_id INTEGER REFERENCES schema_versions (schema_version_id),
    contract_version  INTEGER,
    contract_hash     TEXT,
    rows_read         INTEGER NOT NULL DEFAULT 0,
    rows_inserted     INTEGER NOT NULL DEFAULT 0,
    rows_updated      INTEGER NOT NULL DEFAULT 0,
    rows_unchanged    INTEGER NOT NULL DEFAULT 0,
    rows_quarantined  INTEGER NOT NULL DEFAULT 0,
    rows_dup_dropped  INTEGER NOT NULL DEFAULT 0,
    processed_at      TEXT    NOT NULL
);

-- Idempotency guard enforced by the database: one *active* registry row per file content.
CREATE UNIQUE INDEX IF NOT EXISTS uq_file_registry_active_sha
    ON file_registry (sha256) WHERE status <> 'SUPERSEDED';

CREATE TABLE IF NOT EXISTS raw_records (
    file_id   INTEGER NOT NULL REFERENCES file_registry (file_id),
    row_num   INTEGER NOT NULL,                      -- line number in the source file
    raw_kind  TEXT    NOT NULL CHECK (raw_kind IN ('record', 'fields')),
    raw_json  TEXT    NOT NULL,                      -- 'record': {header: value}; 'fields': [values] (unparseable row)
    PRIMARY KEY (file_id, row_num)
);

CREATE TABLE IF NOT EXISTS drift_events (
    event_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id       INTEGER NOT NULL REFERENCES file_registry (file_id),
    drift_type    TEXT    NOT NULL,
    severity      TEXT    NOT NULL CHECK (severity IN ('INFO', 'WARNING', 'CRITICAL')),
    column_name   TEXT,
    detail        TEXT    NOT NULL,
    suggestion    TEXT,
    evidence_json TEXT,
    detected_at   TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS quarantine_records (
    quarantine_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id        INTEGER NOT NULL REFERENCES file_registry (file_id),
    row_num        INTEGER NOT NULL,
    reason_code    TEXT    NOT NULL,
    column_name    TEXT,
    reason_detail  TEXT,
    raw_json       TEXT    NOT NULL,
    status         TEXT    NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'REPLAYED')),
    quarantined_at TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_quarantine_file ON quarantine_records (file_id, status);

CREATE TABLE IF NOT EXISTS stg_orders (
    order_id    INTEGER PRIMARY KEY,
    customer_id TEXT    NOT NULL,
    order_date  TEXT    NOT NULL,
    amount      REAL    NOT NULL,
    status      TEXT    NOT NULL,
    channel     TEXT,
    extra_json  TEXT
);

CREATE TABLE IF NOT EXISTS curated_orders (
    order_id        INTEGER PRIMARY KEY,
    customer_id     TEXT    NOT NULL,
    order_date      TEXT    NOT NULL CHECK (order_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    amount          REAL    NOT NULL CHECK (amount >= 0),   -- production: DECIMAL / integer minor units
    status          TEXT    NOT NULL,
    channel         TEXT,
    extra_json      TEXT,                                   -- values of columns not (yet) in the contract
    source_file_id  INTEGER NOT NULL REFERENCES file_registry (file_id),
    first_loaded_at TEXT    NOT NULL,
    last_updated_at TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS incidents (
    incident_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id            INTEGER NOT NULL REFERENCES file_registry (file_id),
    severity           TEXT    NOT NULL CHECK (severity IN ('WARNING', 'CRITICAL')),
    title              TEXT    NOT NULL,
    ai_status          TEXT    NOT NULL CHECK (ai_status IN ('OK', 'LOW_CONFIDENCE', 'FALLBACK')),
    ai_provider        TEXT    NOT NULL,
    ai_fallback_reason TEXT,
    explanation_json   TEXT    NOT NULL,
    created_at         TEXT    NOT NULL
);
