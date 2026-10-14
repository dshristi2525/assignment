# Architecture

## Data flow

```mermaid
flowchart LR
    SRC["CSV files<br/>data/incoming"] --> RD["Reader<br/>sha256, tolerant parse"]
    RD --> IDEM{"Same content<br/>already processed?"}
    IDEM -- yes --> SKIP["Skip (idempotent)"]
    IDEM -- no --> AN["Analyze (pure)<br/>schema diff vs contract<br/>type profile"]
    CON[("config/contract.json<br/>+ approved aliases")] --> AN
    AN --> TX

    subgraph TX["One SQLite transaction per file"]
      RAW[("RAW<br/>raw_records<br/>file_registry")] --> DEC{"Critical drift?"}
      DEC -- yes --> QF["Quarantine whole file"]
      DEC -- no --> VAL["Validate rows<br/>dedupe"]
      VAL -- bad rows --> QR["Quarantine rows"]
      VAL -- good rows --> STG[("stg_orders")] --> CUR[("CURATED<br/>curated_orders<br/>idempotent upsert")]
      QF --> Q[("quarantine_records")]
      QR --> Q
      DEC --> DE[("drift_events<br/>schema_versions")]
    end

    TX -- after COMMIT --> EV["Evidence<br/>metadata + redacted samples"]
    EV --> AI["IncidentExplainer<br/>LLMClient interface"]
    AI -- "invalid / ungrounded / down" --> FB["Rule-based fallback"]
    AI -- "valid" --> OUT
    FB --> OUT["incidents table +<br/>Alert sinks: console, .md, .json"]
    Q -. "approve-alias → replay" .-> AN
```

## AI validation ladder

```mermaid
flowchart TD
    A["Model reply (untrusted text)"] --> B{"JSON object?"}
    B -- no --> R
    B -- yes --> C{"All keys / types valid?"}
    C -- no --> R
    C -- yes --> D{"Columns + mapping grounded in evidence?"}
    D -- no --> R
    D -- yes --> E{"confidence ≥ 0.6?"}
    E -- no --> L["LOW_CONFIDENCE: text kept, mapping dropped, human warning"]
    E -- yes --> OK["OK"]
    R{"attempt < 2?"} -- yes --> RETRY["Retry with rejection reason"] --> A
    R -- no --> FB["FALLBACK: rule-based text, reason stored"]
    T["Transport error / client crash"] --> FB
```

## Sequence for the breaking-change day

1. `orders_2026-10-04.csv` arrives with `total_amount`, `order_status`.
2. Reader hashes it; not seen before. `analyze()` finds required `amount`/`status` missing and rename candidates whose values satisfy the contract → `RENAME_SUSPECTED` (CRITICAL).
3. Transaction: raw rows stored, **all 45 rows quarantined** (`FILE_QUARANTINED`), drift events + new schema version recorded, file status `QUARANTINED_FILE`, COMMIT. Curated table untouched.
4. Evidence built → explainer → validated explanation → `incidents` row → console + `.incident.md` + `.alert.json`.
5. Human: `approve-alias total_amount amount` (contract v+1) → `replay` re-analyzes from RAW, loads 45 rows, marks the quarantined file `SUPERSEDED` and its rows `REPLAYED`.
