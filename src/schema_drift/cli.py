"""Command line interface:  python main.py <command> --help"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path
from typing import List, Optional, Sequence

from .ai import IncidentExplainer, LLMConfigError, make_client
from .alerts import ConsoleSink, FileSink
from .contract import Contract, ContractError
from .datagen import generate
from .db import connect, init_schema, named_queries
from .models import IngestResult
from .pipeline import Pipeline
from .util import ROOT

DEFAULT_DB = ROOT / "output" / "pipeline.db"
DEFAULT_CONTRACT = ROOT / "config" / "contract.json"
DEFAULT_INCOMING = ROOT / "data" / "incoming"
DEFAULT_ALERTS = ROOT / "output" / "alerts"


# ---- helpers --------------------------------------------------------------------------------
def format_table(rows: Sequence[Sequence[object]], headers: Sequence[str]) -> str:
    cells = [[str(c) for c in r] for r in rows]
    widths = [max(len(h), *(len(r[i]) for r in cells)) if cells else len(h) for i, h in enumerate(headers)]
    line = "  ".join("-" * w for w in widths)
    out = ["  ".join(h.ljust(w) for h, w in zip(headers, widths)), line]
    out += ["  ".join(c.ljust(w) for c, w in zip(r, widths)) for r in cells]
    return "\n".join(out)


def print_result(result: IngestResult) -> None:
    if result.skipped:
        print(f"  SKIPPED  {result.file_name}: {result.message}")
        return
    print(f"  {result.status:<21} {result.file_name}: read={result.rows_read} inserted={result.inserted} "
          f"updated={result.updated} unchanged={result.unchanged} quarantined={result.quarantined} "
          f"duplicates_dropped={result.duplicates_dropped}")
    for e in result.events:
        print(f"      [{e.severity}] {e.drift_type}" + (f" ({e.column})" if e.column else ""))


def build_pipeline(args, alerts_dir: Optional[Path] = None, contract: Optional[Contract] = None) -> Pipeline:
    contract = contract or Contract.load(Path(args.contract))
    explainer = IncidentExplainer(make_client(args.llm))
    sinks = [ConsoleSink(), FileSink(alerts_dir or Path(args.alerts_dir))]
    return Pipeline(Path(args.db), contract, explainer, sinks)


def print_report(db_path: Path) -> None:
    conn = connect(db_path)
    init_schema(conn)
    titles = {
        "file_status": "Files", "drift_events": "Drift events", "schema_versions": "Schema versions seen",
        "quarantine_by_reason": "Open quarantine by reason", "incidents": "Incidents",
        "curated_by_status": "Curated orders by status", "data_quality_scorecard": "Scorecard",
    }
    for name, sql in named_queries().items():
        cur = conn.execute(sql)
        rows = [tuple(r) for r in cur.fetchall()]
        print(f"\n## {titles.get(name, name)}")
        print(format_table(rows, [d[0] for d in cur.description]) if rows else "(none)")
    conn.close()


# ---- commands -------------------------------------------------------------------------------
def cmd_generate(args) -> int:
    paths = generate(Path(args.out), args.seed)
    print(f"Wrote {len(paths)} files to {args.out}")
    return 0


def cmd_ingest(args) -> int:
    paths: List[Path] = [Path(p) for p in args.paths]
    if args.dir:
        paths += sorted(Path(args.dir).glob("*.csv"))
    if not paths:
        print("nothing to ingest: pass file paths or --dir", file=sys.stderr)
        return 2
    pipeline = build_pipeline(args)
    failures = 0
    for path in paths:
        print(f"\n> ingesting {path.name}")
        try:
            print_result(pipeline.ingest(path))
        except Exception as exc:                      # one bad file must not stop the batch; rolled back, retryable
            failures += 1
            print(f"  ERROR {path.name}: {type(exc).__name__}: {exc} (rolled back; safe to re-run)", file=sys.stderr)
    pipeline.close()
    return 1 if failures else 0


def cmd_replay(args) -> int:
    pipeline = build_pipeline(args)
    outcomes = pipeline.replay()
    for o in outcomes:
        print(f"  {'REPLAYED' if o.replayed else 'UNCHANGED':<9} {o.file_name}: {o.message}")
        if o.result:
            print_result(o.result)
    if not outcomes:
        print("  no quarantined files")
    pipeline.close()
    return 0


def cmd_approve_alias(args) -> int:
    path = Path(args.contract)
    contract = Contract.load(path).with_alias(args.incoming, args.canonical)
    contract.save(path)
    print(f"Approved alias {args.incoming} -> {args.canonical}; contract is now v{contract.version}. "
          "Run `replay` to retry quarantined files.")
    return 0


def cmd_report(args) -> int:
    print_report(Path(args.db))
    return 0


def cmd_demo(args) -> int:
    work = Path(args.workdir)
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    incoming, contract_path = work / "incoming", work / "contract.json"
    shutil.copy(DEFAULT_CONTRACT, contract_path)
    files = generate(incoming, seed=42)

    def step(title: str) -> None:
        print(f"\n{'#' * 78}\n# {title}\n{'#' * 78}")
        if args.pause:
            input("  (press Enter to continue) ")

    args.db, args.alerts_dir = str(work / "pipeline.db"), str(work / "alerts")
    pipeline = build_pipeline(args, Path(args.alerts_dir), Contract.load(contract_path))
    captions = [
        "Day 1: baseline schema (2 bad rows + 1 exact duplicate: row-level noise, below alert threshold)",
        "Day 2: columns REORDERED (not drift) + upserts of existing orders + 1 conflicting duplicate key",
        "Day 3: ADDITIVE drift: new 'coupon_code', optional 'channel' dropped -> load with warnings",
        "Day 4: BREAKING drift: amount->total_amount, status->order_status -> quarantine the whole file",
        "Day 5: PARTIAL type drift: 30% of order_date in DD/MM/YYYY -> quarantine those rows only",
        "Day 6: TYPE drift: every amount is now 'Rs 1,299.00' -> quarantine the whole file",
    ]
    for path, caption in zip(files, captions):
        step(caption)
        print_result(pipeline.ingest(path))

    step("Re-run the whole batch: idempotency (nothing is loaded twice)")
    for path in files:
        print_result(pipeline.ingest(path))

    step("Producer confirms the day-4 renames. Approve aliases in the contract, then replay quarantined files")
    contract = Contract.load(contract_path)
    for incoming, canonical in (("total_amount", "amount"), ("order_status", "status")):
        contract = contract.with_alias(incoming, canonical)
        print(f"  approved alias {incoming} -> {canonical}")
    contract.save(contract_path)
    pipeline.set_contract(contract)
    for outcome in pipeline.replay():
        print(f"  {'REPLAYED' if outcome.replayed else 'UNCHANGED':<9} {outcome.file_name}: {outcome.message}")
        if outcome.result:
            print_result(outcome.result)

    step("Report")
    pipeline.close()
    print_report(Path(args.db))
    print(f"\nArtifacts: {work}/pipeline.db, {work}/alerts/*.incident.md, {work}/alerts/*.alert.json")
    return 0


# ---- entry point ----------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="schema_drift", description="Schema drift handling file-ingestion pipeline")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--db", default=str(DEFAULT_DB), help="SQLite database path")
    common.add_argument("--contract", default=str(DEFAULT_CONTRACT), help="data contract JSON")
    common.add_argument("--alerts-dir", default=str(DEFAULT_ALERTS), help="where alert files are written")
    common.add_argument("--llm", default=None,
                        help="none | ollama | groq | openrouter | openai_compat | stub[:garbage|hallucinate|lowconf|down] "
                             "(default: $LLM_PROVIDER or none)")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("generate-data", help="write the synthetic six-day feed")
    p.add_argument("--out", default=str(DEFAULT_INCOMING))
    p.add_argument("--seed", type=int, default=42)
    p.set_defaults(func=cmd_generate)

    p = sub.add_parser("ingest", parents=[common], help="ingest one or more CSV files")
    p.add_argument("paths", nargs="*")
    p.add_argument("--dir", help="ingest every *.csv in this directory (sorted by name)")
    p.set_defaults(func=cmd_ingest)

    p = sub.add_parser("replay", parents=[common], help="retry quarantined files after a contract change")
    p.set_defaults(func=cmd_replay)

    p = sub.add_parser("approve-alias", parents=[common], help="approve a column rename in the contract")
    p.add_argument("incoming")
    p.add_argument("canonical")
    p.set_defaults(func=cmd_approve_alias)

    p = sub.add_parser("report", parents=[common], help="print data-quality report")
    p.set_defaults(func=cmd_report)

    p = sub.add_parser("demo", parents=[common], help="run the full drift scenario end to end")
    p.add_argument("--workdir", default=str(ROOT / "output" / "demo"))
    p.add_argument("--pause", action="store_true", help="wait for Enter between steps (handy for screen recording)")
    p.set_defaults(func=cmd_demo)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except (ContractError, LLMConfigError) as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
