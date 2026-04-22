from __future__ import annotations

import argparse
import csv
import math
from collections import Counter
from pathlib import Path


DEFAULT_INPUT = Path("reports/cap5_t10_14-approved.csv")
DEFAULT_OUTPUT = Path("reports/cap5_t10_14-shortlist.csv")
DEFAULT_REPORT = Path("reports/cap5_t10_14-shortlist.md")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Filtra um approved.csv bruto de discovery R3 para shortlist operacional."
    )
    parser.add_argument("--input-csv", default=str(DEFAULT_INPUT))
    parser.add_argument("--output-csv", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--report-md", default=str(DEFAULT_REPORT))
    parser.add_argument("--top", type=int, default=100)
    parser.add_argument("--min-occurrences", type=int, default=300)
    parser.add_argument("--min-tickers", type=int, default=50)
    parser.add_argument("--min-success-rate", type=float, default=85.0)
    parser.add_argument("--min-take-profit-rate", type=float, default=85.0)
    parser.add_argument("--min-profit-factor", type=float, default=2.5)
    parser.add_argument("--min-average-trade-return-pct", type=float, default=0.45)
    parser.add_argument("--max-stop-loss-rate", type=float, default=12.0)
    parser.add_argument("--entry-rule", choices=("open", "close", "all"), default="all")
    parser.add_argument("--state-size", type=int, choices=(1, 2, 3, 0), default=0)
    parser.add_argument("--target-min", type=float, default=0.0)
    parser.add_argument("--target-max", type=float, default=99.0)
    parser.add_argument("--stop-max", type=float, default=99.0)
    return parser.parse_args()


def _float(row: dict[str, str], key: str) -> float:
    value = row.get(key, "")
    if value in ("", None):
        return 0.0
    if str(value).upper() == "INF":
        return math.inf
    return float(value)


def _score(row: dict[str, str]) -> float:
    pf = min(_float(row, "profit_factor"), 10.0)
    occurrences = max(_float(row, "total_occurrences"), 1.0)
    tickers = max(_float(row, "tickers_with_matches"), 1.0)
    return (
        _float(row, "take_profit_rate_pct") * 2.6
        + _float(row, "success_rate_pct") * 1.7
        + pf * 18.0
        + _float(row, "average_trade_return_pct") * 24.0
        - _float(row, "stop_loss_rate_pct") * 1.3
        + math.log10(occurrences) * 14.0
        + math.log10(tickers) * 10.0
    )


def _passes(row: dict[str, str], args: argparse.Namespace) -> bool:
    if args.entry_rule != "all" and row.get("entry_rule") != args.entry_rule:
        return False
    if args.state_size and int(_float(row, "state_size")) != args.state_size:
        return False
    target_pct = _float(row, "take_profit_pct")
    stop_pct = _float(row, "stop_loss_pct")
    return (
        _float(row, "total_occurrences") >= args.min_occurrences
        and _float(row, "tickers_with_matches") >= args.min_tickers
        and _float(row, "success_rate_pct") >= args.min_success_rate
        and _float(row, "take_profit_rate_pct") >= args.min_take_profit_rate
        and _float(row, "profit_factor") >= args.min_profit_factor
        and _float(row, "average_trade_return_pct") >= args.min_average_trade_return_pct
        and _float(row, "stop_loss_rate_pct") <= args.max_stop_loss_rate
        and target_pct >= args.target_min
        and target_pct <= args.target_max
        and stop_pct <= args.stop_max
    )


def _fmt(value: float, digits: int = 2) -> str:
    if math.isinf(value):
        return "INF"
    return f"{value:.{digits}f}"


def _write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    input_path = Path(args.input_csv)
    output_path = Path(args.output_csv)
    report_path = Path(args.report_md)

    with input_path.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        fieldnames = list(reader.fieldnames or [])
        rows = [row for row in reader]

    kept: list[dict[str, str]] = []
    for row in rows:
        if not _passes(row, args):
            continue
        row = dict(row)
        row["operational_score"] = _fmt(_score(row), 4)
        kept.append(row)

    kept.sort(
        key=lambda row: (
            float(row["operational_score"]),
            _float(row, "take_profit_rate_pct"),
            _float(row, "profit_factor"),
            _float(row, "average_trade_return_pct"),
            _float(row, "total_occurrences"),
        ),
        reverse=True,
    )
    if args.top > 0:
        kept = kept[: args.top]

    output_fields = ["operational_score", *fieldnames]
    _write_csv(output_path, kept, output_fields)

    entry_counter = Counter(row.get("entry_rule", "") for row in kept)
    target_counter = Counter(row.get("take_profit_pct", "") for row in kept)
    feature_counter = Counter(row.get("feature_keys", "") for row in kept)

    lines = [
        "# Shortlist Operacional",
        "",
        f"- Entrada: `{input_path}`",
        f"- Estrategias aprovadas no bruto: `{len(rows)}`",
        f"- Estrategias na shortlist: `{len(kept)}`",
        f"- Filtros: ocorrencias>={args.min_occurrences}, tickers>={args.min_tickers}, success>={args.min_success_rate:.1f}%, take_profit>={args.min_take_profit_rate:.1f}%, profit_factor>={args.min_profit_factor:.2f}, avg_trade>={args.min_average_trade_return_pct:.2f}%, stop_loss<={args.max_stop_loss_rate:.1f}%",
        "",
        "## Distribuicao",
        "",
    ]
    for key, value in sorted(entry_counter.items()):
        lines.append(f"- `{key}`: `{value}`")
    lines.append("")
    lines.append("## Targets")
    lines.append("")
    for key, value in sorted(target_counter.items(), key=lambda item: float(item[0] or 0.0)):
        lines.append(f"- `{key}%`: `{value}`")
    lines.append("")
    lines.append("## Top 20")
    lines.append("")
    for index, row in enumerate(kept[:20], start=1):
        lines.append(
            f"{index}. `{row['template_code']}` | `{row['entry_rule']}` | alvo `{row['take_profit_pct']}%` | stop `{row['stop_loss_pct']}%` | ocorrencias `{row['total_occurrences']}` | tickers `{row['tickers_with_matches']}` | tp `{row['take_profit_rate_pct']}%` | success `{row['success_rate_pct']}%` | pf `{row['profit_factor']}` | avg `{row['average_trade_return_pct']}%` | score `{row['operational_score']}`"
        )
        lines.append(f"   - `{row['feature_keys']}`")
        lines.append(f"   - {row['code']}")
    lines.append("")
    lines.append("## Feature Sets Mais Frequentes")
    lines.append("")
    for feature_keys, count in feature_counter.most_common(15):
        lines.append(f"- `{feature_keys}`: `{count}`")
    lines.append("")
    lines.append(f"- CSV: `{output_path}`")

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines), encoding="utf-8")

    print(f"approved_rows={len(rows)}")
    print(f"shortlist_rows={len(kept)}")
    print(f"output={output_path}")
    print(f"report={report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
