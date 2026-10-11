-- Reporting queries used by `python main.py report`. Each block starts with "-- name: <id>".

-- name: file_status
SELECT file_id, file_name, status,
       rows_read, rows_inserted AS inserted, rows_updated AS updated,
       rows_unchanged AS unchanged, rows_dup_dropped AS dup_dropped, rows_quarantined AS quarantined
FROM file_registry
WHERE status <> 'SUPERSEDED'
ORDER BY file_id;

-- name: drift_events
SELECT f.file_name, d.severity, d.drift_type, COALESCE(d.column_name, '-') AS column_name
FROM drift_events AS d
JOIN file_registry AS f USING (file_id)
WHERE f.status <> 'SUPERSEDED'
ORDER BY f.file_id,
         CASE d.severity WHEN 'CRITICAL' THEN 0 WHEN 'WARNING' THEN 1 ELSE 2 END,
         d.drift_type;

-- name: quarantine_by_reason
SELECT q.reason_code, COUNT(*) AS open_records, COUNT(DISTINCT q.file_id) AS files
FROM quarantine_records AS q
WHERE q.status = 'OPEN'
GROUP BY q.reason_code
ORDER BY open_records DESC, q.reason_code;

-- name: schema_versions
SELECT schema_version_id AS version, fingerprint, first_seen_file, columns_json
FROM schema_versions
ORDER BY schema_version_id;

-- name: incidents
SELECT i.incident_id AS id, f.file_name, i.severity, i.ai_status, i.ai_provider, i.title
FROM incidents AS i
JOIN file_registry AS f USING (file_id)
ORDER BY i.incident_id;

-- name: curated_by_status
SELECT status, COUNT(*) AS orders, ROUND(SUM(amount), 2) AS total_amount
FROM curated_orders
GROUP BY status
ORDER BY orders DESC;

-- name: data_quality_scorecard
SELECT
    (SELECT COUNT(*) FROM curated_orders)                                        AS curated_rows,
    (SELECT COUNT(*) FROM quarantine_records WHERE status = 'OPEN')              AS open_quarantine,
    (SELECT COUNT(*) FROM file_registry WHERE status = 'QUARANTINED_FILE')       AS files_quarantined,
    (SELECT COUNT(*) FROM curated_orders WHERE extra_json IS NOT NULL)           AS rows_with_unpromoted_columns;
