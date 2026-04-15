from __future__ import annotations

import argparse
import csv
import math
from pathlib import Path


DEFAULT_INPUT = Path("reports/asset-discovery-lista-r3-stat-shortlist.csv")
DEFAULT_OUTPUT = Path("reports/asset-discovery-lista-r3-stat-elite-top49.csv")


def _float(row: dict[str, str], key: str) -> float:
    value = row.get(key, "")
    if value in ("", None):
        return 0.0
    if str(value).upper() == "INF":
        return math.inf
    return float(value)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Filtra a shortlist R3 para estrategias elite operacionais."
    )
    parser.add_argument("--input-csv", default=str(DEFAULT_INPUT))
    parser.add_argument("--output-csv", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--min-trades", type=int, default=200)
    parser.add_argument("--min-tickers", type=int, default=50)
    parser.add_argument("--min-take-profit-rate", type=float, default=65.0)
    parser.add_argument("--min-win-rate", type=float, default=70.0)
    parser.add_argument("--min-average-return", type=float, default=1.3)
    parser.add_argument("--min-profit-factor", type=float, default=2.5)
    parser.add_argument("--min-test-take-profit-rate", type=float, default=70.0)
    parser.add_argument("--min-test-average-return", type=float, default=2.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = Path(args.input_csv)
    output_path = Path(args.output_csv)

    with input_path.open(encoding="utf-8", newline="") as file_obj:
        rows = list(csv.DictReader(file_obj))

    if not rows:
        raise SystemExit(f"Shortlist vazia: {input_path}")

    elite = [
        row
        for row in rows
        if _float(row, "trades") >= args.min_trades
        and _float(row, "tickers") >= args.min_tickers
        and _float(row, "take_profit_rate_pct") >= args.min_take_profit_rate
        and _float(row, "profitable_rate_pct") >= args.min_win_rate
        and _float(row, "average_trade_return_pct") >= args.min_average_return
        and _float(row, "profit_factor") >= args.min_profit_factor
        and _float(row, "test_take_profit_rate_pct") >= args.min_test_take_profit_rate
        and _float(row, "test_average_trade_return_pct") >= args.min_test_average_return
    ]
    elite.sort(
        key=lambda row: (
            _float(row, "score"),
            _float(row, "test_average_trade_return_pct"),
            _float(row, "take_profit_rate_pct"),
        ),
        reverse=True,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(elite)

    print(f"elite={len(elite)} -> {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
