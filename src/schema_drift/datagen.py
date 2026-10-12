"""Deterministic synthetic data: an orders feed whose schema changes over six days.

    day 1  baseline schema                         + 2 bad rows, 1 exact duplicate row
    day 2  columns REORDERED (not drift)           + 5 status updates for existing orders, 1 conflicting duplicate
    day 3  ADDITIVE drift: new 'coupon_code', optional 'channel' dropped
    day 4  BREAKING drift: 'amount'->'total_amount', 'status'->'order_status'
    day 5  PARTIAL type drift: 30% of 'order_date' values switch to DD/MM/YYYY
    day 6  TYPE drift: every 'amount' becomes a formatted string like 'Rs 1,299.00'

Same seed => byte-identical files, so reviewers can regenerate exactly what is committed in data/incoming/.
"""
from __future__ import annotations

import csv
import random
from pathlib import Path
from typing import Dict, List

BASE_HEADER = ["order_id", "customer_id", "order_date", "amount", "status", "channel"]
STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]
CHANNELS = ["WEB", "APP", "STORE"]

Row = Dict[str, str]


def _order(rng: random.Random, order_id: int, day: int) -> Row:
    return {
        "order_id": str(order_id),
        "customer_id": f"C{rng.randint(1, 200):04d}",
        "order_date": f"2026-10-{day:02d}",
        "amount": f"{rng.uniform(199, 9999):.2f}",
        "status": rng.choices(STATUSES, weights=[4, 3, 5, 1, 1])[0],
        "channel": rng.choice(CHANNELS),
    }


def _write(path: Path, header: List[str], rows: List[Row]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=header, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def generate(out_dir: Path, seed: int = 42) -> List[Path]:
    rng = random.Random(seed)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    paths: List[Path] = []

    def emit(day: int, header: List[str], rows: List[Row]) -> None:
        path = out_dir / f"orders_2026-10-{day:02d}.csv"
        _write(path, header, rows)
        paths.append(path)

    # day 1 - baseline, with realistic noise (2/51 bad rows stays below the 5% alert threshold)
    day1 = [_order(rng, 1001 + i, 1) for i in range(50)]
    day1[6]["amount"] = ""                 # null in a required column
    day1[22]["status"] = "SHIPED"          # typo => invalid enum value
    day1.append(dict(day1[10]))            # exact duplicate row
    emit(1, BASE_HEADER, day1)

    # day 2 - reordered columns, upserts of existing orders, one conflicting duplicate key
    placed = [r for r in day1[:50] if r["status"] == "PLACED" and r["amount"] and r["status"] != "SHIPED"][:5]
    day2 = [{**r, "status": "SHIPPED", "order_date": "2026-10-02"} for r in placed]
    day2 += [_order(rng, 1051 + i, 2) for i in range(35)]
    day2.append({**day2[10], "amount": "1.00"})   # same order_id as an earlier row, different amount
    emit(2, ["status", "order_id", "amount", "customer_id", "channel", "order_date"], day2)

    # day 3 - additive drift
    header3 = ["order_id", "customer_id", "order_date", "amount", "status", "coupon_code"]
    day3 = []
    for i in range(40):
        row = _order(rng, 1086 + i, 3)
        row["coupon_code"] = rng.choice(["", "", "SAVE10", "FEST25", "NEWUSER"])
        day3.append({k: row[k] for k in header3})
    emit(3, header3, day3)

    # day 4 - breaking renames
    header4 = ["order_id", "customer_id", "order_date", "total_amount", "order_status", "channel"]
    day4 = []
    for i in range(45):
        row = _order(rng, 1126 + i, 4)
        day4.append({"order_id": row["order_id"], "customer_id": row["customer_id"], "order_date": row["order_date"],
                     "total_amount": row["amount"], "order_status": row["status"], "channel": row["channel"]})
    emit(4, header4, day4)

    # day 5 - partial type drift: 30% of dates switch format
    day5 = [_order(rng, 1171 + i, 5) for i in range(40)]
    for i in rng.sample(range(40), 12):
        day5[i]["order_date"] = "05/10/2026"
    emit(5, BASE_HEADER, day5)

    # day 6 - full type drift on amount
    day6 = []
    for i in range(30):
        row = _order(rng, 1211 + i, 6)
        row["amount"] = f"Rs {float(row['amount']):,.2f}"
        day6.append(row)
    emit(6, BASE_HEADER, day6)
    return paths
