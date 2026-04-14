#!/usr/bin/env python3
"""Generate or upsert one row into daily-profit-report.csv from runtime artifacts.

Inputs:
- ORDER_LOG_PATH JSONL (from append_order_record)
- POSITIONS_PATH JSON (from save_positions)

The script keeps a small state file to compute daily deltas from cumulative fields.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from typing import Dict, List

getcontext().prec = 28

CSV_FIELDS = [
    "date",
    "start_capital",
    "gross_pnl",
    "fees",
    "slippage_cost",
    "failure_cost",
    "net_pnl",
    "daily_net_return",
    "max_drawdown_to_date",
    "report_count",
    "filled_or_partial_count",
    "notes",
]


@dataclass
class Totals:
    realized_pnl: Decimal
    total_fees: Decimal


def parse_decimal(raw: object, field_name: str) -> Decimal:
    try:
        return Decimal(str(raw))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid decimal for {field_name}: {raw}") from exc


def load_positions_totals(positions_path: Path) -> Totals:
    if not positions_path.exists():
        raise FileNotFoundError(
            f"positions snapshot not found: {positions_path}. "
            "Run trading first or pass --positions-path."
        )
    payload = json.loads(positions_path.read_text(encoding="utf-8"))
    realized_pnl = parse_decimal(payload.get("realized_pnl", "0"), "realized_pnl")
    total_fees = parse_decimal(payload.get("total_fees", "0"), "total_fees")
    return Totals(realized_pnl=realized_pnl, total_fees=total_fees)


def count_order_reports_for_day(order_log_path: Path, day: str) -> tuple[int, int]:
    report_count = 0
    filled_or_partial_count = 0
    if not order_log_path.exists():
        return report_count, filled_or_partial_count

    with order_log_path.open("r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            occurred_at = str(item.get("occurred_at", ""))
            if not occurred_at.startswith(day):
                continue
            report_count += 1
            status = str(item.get("status", ""))
            if status in ("Filled", "PartiallyFilled"):
                filled_or_partial_count += 1

    return report_count, filled_or_partial_count


def load_state(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def save_state(path: Path, state: Dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=True, indent=2), encoding="utf-8")


def load_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        return list(reader)


def save_csv_rows(path: Path, rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def upsert_row(path: Path, row: Dict[str, str]) -> None:
    rows = load_csv_rows(path)
    replaced = False
    for idx, existing in enumerate(rows):
        if existing.get("date") == row["date"]:
            rows[idx] = row
            replaced = True
            break
    if not replaced:
        rows.append(row)
        rows.sort(key=lambda r: r.get("date", ""))
    save_csv_rows(path, rows)


def d_to_str(v: Decimal) -> str:
    return format(v, "f")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate one daily profit report row from order log + positions snapshot."
    )
    parser.add_argument(
        "--order-log-path",
        default="data/orders.jsonl",
        help="Path to ORDER_LOG_PATH JSONL (default: data/orders.jsonl).",
    )
    parser.add_argument(
        "--positions-path",
        default="data/positions.json",
        help="Path to POSITIONS_PATH JSON (default: data/positions.json).",
    )
    parser.add_argument(
        "--output-csv",
        default="data/daily-profit-report.csv",
        help="Output CSV path (default: data/daily-profit-report.csv).",
    )
    parser.add_argument(
        "--state-path",
        default="data/daily-report-state.json",
        help="State JSON path for delta computation (default: data/daily-report-state.json).",
    )
    parser.add_argument(
        "--date",
        default=date.today().isoformat(),
        help="Report date in YYYY-MM-DD (default: today).",
    )
    parser.add_argument(
        "--start-capital",
        default="100",
        help="Start capital for this day, e.g. 100 or 100.50 (default: 100).",
    )
    parser.add_argument(
        "--slippage-cost",
        default="0",
        help="Manual slippage cost for this day (default: 0).",
    )
    parser.add_argument(
        "--failure-cost",
        default="0",
        help="Manual failure/retry cost for this day (default: 0).",
    )
    parser.add_argument("--notes", default="", help="Optional notes column.")
    args = parser.parse_args()

    day = args.date
    start_capital = parse_decimal(args.start_capital, "start_capital")
    slippage_cost = parse_decimal(args.slippage_cost, "slippage_cost")
    failure_cost = parse_decimal(args.failure_cost, "failure_cost")
    if start_capital <= Decimal("0"):
        raise ValueError("start_capital must be > 0")

    order_log_path = Path(args.order_log_path)
    positions_path = Path(args.positions_path)
    output_csv = Path(args.output_csv)
    state_path = Path(args.state_path)

    try:
        totals = load_positions_totals(positions_path)
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
    state = load_state(state_path)
    prev_realized = parse_decimal(state.get("last_realized_pnl_total", "0"), "state.last_realized_pnl_total")
    prev_fees = parse_decimal(state.get("last_total_fees_total", "0"), "state.last_total_fees_total")
    prev_peak = parse_decimal(state.get("equity_peak", str(start_capital)), "state.equity_peak")

    realized_pnl_delta = totals.realized_pnl - prev_realized
    fees_delta = totals.total_fees - prev_fees
    gross_pnl = realized_pnl_delta + fees_delta
    net_pnl = realized_pnl_delta - slippage_cost - failure_cost
    daily_net_return = net_pnl / start_capital

    equity = start_capital + net_pnl
    equity_peak = max(prev_peak, equity)
    if equity_peak > Decimal("0"):
        max_drawdown_to_date = (equity_peak - equity) / equity_peak
    else:
        max_drawdown_to_date = Decimal("0")

    report_count, filled_or_partial_count = count_order_reports_for_day(order_log_path, day)

    row = {
        "date": day,
        "start_capital": d_to_str(start_capital),
        "gross_pnl": d_to_str(gross_pnl),
        "fees": d_to_str(fees_delta),
        "slippage_cost": d_to_str(slippage_cost),
        "failure_cost": d_to_str(failure_cost),
        "net_pnl": d_to_str(net_pnl),
        "daily_net_return": d_to_str(daily_net_return),
        "max_drawdown_to_date": d_to_str(max_drawdown_to_date),
        "report_count": str(report_count),
        "filled_or_partial_count": str(filled_or_partial_count),
        "notes": args.notes,
    }

    upsert_row(output_csv, row)

    state_update = {
        "last_date": day,
        "last_realized_pnl_total": d_to_str(totals.realized_pnl),
        "last_total_fees_total": d_to_str(totals.total_fees),
        "equity_peak": d_to_str(equity_peak),
    }
    save_state(state_path, state_update)

    print(f"updated report row for {day}: {output_csv}")
    print(f"state updated: {state_path}")


if __name__ == "__main__":
    main()
