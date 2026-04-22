from __future__ import annotations

import argparse
import csv
import math
from pathlib import Path


DEFAULT_INPUT = Path("reports/asset-discovery-lista-r3-stat-final.csv")
DEFAULT_TICKERS_INPUT = Path("reports/asset-discovery-lista-r3-stat-final-tickers.csv")
DEFAULT_TRADES_INPUT = Path("reports/asset-discovery-lista-r3-stat-trades-approved.csv")
DEFAULT_OUTPUT = Path("reports/asset-discovery-lista-r3-stat-high-accuracy.csv")
DEFAULT_TICKERS_OUTPUT = Path("reports/asset-discovery-lista-r3-stat-high-accuracy-tickers.csv")
DEFAULT_ACTIONS_OUTPUT = Path("reports/asset-discovery-lista-r3-stat-high-accuracy-actions.csv")
DEFAULT_REPORT = Path("reports/asset-discovery-lista-r3-stat-high-accuracy.md")


def _float(row: dict[str, str], key: str) -> float:
    value = row.get(key, "")
    if value in ("", None):
        return 0.0
    if str(value).upper() == "INF":
        return math.inf
    return float(value)


def _fmt(value: float, digits: int = 2) -> str:
    if math.isinf(value):
        return "INF"
    return f"{value:.{digits}f}"


def _accuracy_score(row: dict[str, str]) -> float:
    test_trades = _float(row, "test_trades")
    test_pf = min(_float(row, "test_profit_factor"), 20.0)
    stop_rate = _float(row, "stop_loss_rate_pct")
    test_stop_rate = _float(row, "test_stop_loss_rate_pct")
    return (
        _float(row, "test_profitable_rate_pct") * 3.0
        + _float(row, "test_take_profit_rate_pct") * 2.5
        + _float(row, "profitable_rate_pct") * 1.4
        + _float(row, "take_profit_rate_pct") * 1.1
        + _float(row, "test_average_trade_return_pct") * 12.0
        + test_pf * 4.0
        + math.log10(max(test_trades, 1.0)) * 10.0
        - stop_rate * 0.8
        - test_stop_rate * 1.2
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Filtra estrategias R3 com foco em maior taxa de acerto, aceitando alvos menores."
    )
    parser.add_argument("--input-csv", default=str(DEFAULT_INPUT))
    parser.add_argument("--tickers-input-csv", default=str(DEFAULT_TICKERS_INPUT))
    parser.add_argument("--trades-input-csv", default=str(DEFAULT_TRADES_INPUT))
    parser.add_argument("--output-csv", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--tickers-output-csv", default=str(DEFAULT_TICKERS_OUTPUT))
    parser.add_argument("--actions-output-csv", default=str(DEFAULT_ACTIONS_OUTPUT))
    parser.add_argument("--report-md", default=str(DEFAULT_REPORT))
    parser.add_argument("--max-target-pct", type=float, default=3.0)
    parser.add_argument("--min-trades", type=int, default=200)
    parser.add_argument("--min-tickers", type=int, default=50)
    parser.add_argument("--min-win-rate", type=float, default=75.0)
    parser.add_argument("--min-take-profit-rate", type=float, default=70.0)
    parser.add_argument("--min-profit-factor", type=float, default=2.5)
    parser.add_argument("--min-average-return", type=float, default=1.0)
    parser.add_argument("--min-test-trades", type=int, default=25)
    parser.add_argument("--min-test-win-rate", type=float, default=85.0)
    parser.add_argument("--min-test-take-profit-rate", type=float, default=80.0)
    parser.add_argument("--min-test-profit-factor", type=float, default=4.0)
    parser.add_argument("--min-test-average-return", type=float, default=1.5)
    parser.add_argument("--top", type=int, default=30)
    parser.add_argument(
        "--include-atr",
        action="store_true",
        help="Inclui estrategias ATR. Por padrao, mantem apenas alvo percentual fixo.",
    )
    return parser.parse_args()


def _passes(row: dict[str, str], args: argparse.Namespace) -> bool:
    target_pct = _float(row, "target_pct")
    if target_pct <= 0.0 and not args.include_atr:
        return False
    if target_pct > args.max_target_pct:
        return False
    return (
        _float(row, "trades") >= args.min_trades
        and _float(row, "tickers") >= args.min_tickers
        and _float(row, "profitable_rate_pct") >= args.min_win_rate
        and _float(row, "take_profit_rate_pct") >= args.min_take_profit_rate
        and _float(row, "profit_factor") >= args.min_profit_factor
        and _float(row, "average_trade_return_pct") >= args.min_average_return
        and _float(row, "test_trades") >= args.min_test_trades
        and _float(row, "test_profitable_rate_pct") >= args.min_test_win_rate
        and _float(row, "test_take_profit_rate_pct") >= args.min_test_take_profit_rate
        and _float(row, "test_profit_factor") >= args.min_test_profit_factor
        and _float(row, "test_average_trade_return_pct") >= args.min_test_average_return
    )


def _write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_ticker_csv(
    input_path: Path,
    output_path: Path,
    strategy_codes: set[str],
) -> int:
    if not input_path.exists():
        return 0

    kept = 0
    with input_path.open(encoding="utf-8", newline="") as input_obj:
        reader = csv.DictReader(input_obj)
        if not reader.fieldnames:
            return 0
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8", newline="") as output_obj:
            writer = csv.DictWriter(output_obj, fieldnames=reader.fieldnames)
            writer.writeheader()
            for row in reader:
                if row.get("strategy_code") in strategy_codes:
                    writer.writerow(row)
                    kept += 1
    return kept


def _write_ticker_csv_from_trades(
    input_path: Path,
    output_path: Path,
    strategies: dict[str, dict[str, str]],
) -> int:
    if not input_path.exists():
        return 0

    metrics: dict[tuple[str, str], dict[str, float | str]] = {}
    first_last: dict[tuple[str, str], list[str]] = {}

    with input_path.open(encoding="utf-8", newline="") as input_obj:
        reader = csv.DictReader(input_obj)
        for row in reader:
            code = row.get("strategy_code", "")
            if code not in strategies:
                continue
            ticker = row.get("ticker", "")
            key = (code, ticker)
            trade_return = float(row.get("trade_return_pct") or 0.0)
            exit_reason = row.get("exit_reason", "")
            current = metrics.setdefault(
                key,
                {
                    "trades": 0.0,
                    "take_profit_count": 0.0,
                    "stop_loss_count": 0.0,
                    "time_cap_count": 0.0,
                    "profitable_count": 0.0,
                    "sum_return": 0.0,
                    "gross_profit": 0.0,
                    "gross_loss": 0.0,
                },
            )
            current["trades"] = float(current["trades"]) + 1.0
            current["sum_return"] = float(current["sum_return"]) + trade_return
            if exit_reason == "take_profit":
                current["take_profit_count"] = float(current["take_profit_count"]) + 1.0
            elif exit_reason.startswith("stop_loss"):
                current["stop_loss_count"] = float(current["stop_loss_count"]) + 1.0
            elif exit_reason == "time_cap":
                current["time_cap_count"] = float(current["time_cap_count"]) + 1.0
            if trade_return > 0:
                current["profitable_count"] = float(current["profitable_count"]) + 1.0
                current["gross_profit"] = float(current["gross_profit"]) + trade_return
            elif trade_return < 0:
                current["gross_loss"] = float(current["gross_loss"]) + abs(trade_return)

            trigger_date = row.get("trigger_date", "")
            dates = first_last.setdefault(key, [trigger_date, trigger_date])
            if trigger_date and (not dates[0] or trigger_date < dates[0]):
                dates[0] = trigger_date
            if trigger_date and (not dates[1] or trigger_date > dates[1]):
                dates[1] = trigger_date

    fieldnames = [
        "strategy_code",
        "strategy_label",
        "ticker",
        "target_pct",
        "stop_pct",
        "time_cap_days",
        "trades",
        "take_profit_count",
        "take_profit_rate_pct",
        "stop_loss_count",
        "stop_loss_rate_pct",
        "time_cap_count",
        "time_cap_rate_pct",
        "profitable_count",
        "profitable_rate_pct",
        "average_trade_return_pct",
        "net_trade_return_pct",
        "profit_factor",
        "first_trade_date",
        "last_trade_date",
    ]
    rows: list[dict[str, str]] = []
    for (code, ticker), metric in metrics.items():
        strategy = strategies[code]
        trades = float(metric["trades"])
        gross_loss = float(metric["gross_loss"])
        gross_profit = float(metric["gross_profit"])
        profit_factor = math.inf if gross_loss == 0.0 and gross_profit > 0.0 else gross_profit / gross_loss if gross_loss else 0.0
        dates = first_last.get((code, ticker), ["", ""])
        rows.append(
            {
                "strategy_code": code,
                "strategy_label": strategy.get("label", ""),
                "ticker": ticker,
                "target_pct": strategy.get("target_pct", ""),
                "stop_pct": strategy.get("stop_pct", ""),
                "time_cap_days": strategy.get("time_cap_days", ""),
                "trades": str(int(trades)),
                "take_profit_count": str(int(float(metric["take_profit_count"]))),
                "take_profit_rate_pct": _fmt(float(metric["take_profit_count"]) / trades * 100.0, 4),
                "stop_loss_count": str(int(float(metric["stop_loss_count"]))),
                "stop_loss_rate_pct": _fmt(float(metric["stop_loss_count"]) / trades * 100.0, 4),
                "time_cap_count": str(int(float(metric["time_cap_count"]))),
                "time_cap_rate_pct": _fmt(float(metric["time_cap_count"]) / trades * 100.0, 4),
                "profitable_count": str(int(float(metric["profitable_count"]))),
                "profitable_rate_pct": _fmt(float(metric["profitable_count"]) / trades * 100.0, 4),
                "average_trade_return_pct": _fmt(float(metric["sum_return"]) / trades, 4),
                "net_trade_return_pct": _fmt(float(metric["sum_return"]), 4),
                "profit_factor": _fmt(profit_factor, 4),
                "first_trade_date": dates[0],
                "last_trade_date": dates[1],
            }
        )
    rows.sort(
        key=lambda row: (
            float(row["profitable_rate_pct"]),
            float(row["take_profit_rate_pct"]),
            float(row["trades"]),
            float(row["average_trade_return_pct"]),
        ),
        reverse=True,
    )
    _write_csv(output_path, rows, fieldnames)
    return len(rows)


def _write_actions_csv(tickers_path: Path, output_path: Path) -> list[dict[str, str]]:
    if not tickers_path.exists():
        return []

    grouped: dict[str, list[dict[str, str]]] = {}
    with tickers_path.open(encoding="utf-8", newline="") as file_obj:
        for row in csv.DictReader(file_obj):
            grouped.setdefault(row["ticker"], []).append(row)

    rows: list[dict[str, str]] = []
    for ticker, items in grouped.items():
        total_trades = sum(_float(item, "trades") for item in items)
        strategies_count = len({item["strategy_code"] for item in items})
        avg_return = sum(_float(item, "average_trade_return_pct") for item in items) / len(items)
        avg_win = sum(_float(item, "profitable_rate_pct") for item in items) / len(items)
        avg_hit = sum(_float(item, "take_profit_rate_pct") for item in items) / len(items)
        avg_stop = sum(_float(item, "stop_loss_rate_pct") for item in items) / len(items)
        best_pf = max(_float(item, "profit_factor") for item in items)
        score = (
            strategies_count * 10.0
            + total_trades * 0.05
            + avg_win * 1.5
            + avg_hit * 1.2
            + avg_return * 20.0
            + min(best_pf, 20.0) * 2.0
            - avg_stop * 0.8
        )
        best = max(
            items,
            key=lambda item: (
                _float(item, "profitable_rate_pct"),
                _float(item, "take_profit_rate_pct"),
                _float(item, "trades"),
                _float(item, "average_trade_return_pct"),
            ),
        )
        rows.append(
            {
                "ticker": ticker,
                "strategies_count": str(strategies_count),
                "total_trades": str(int(total_trades)),
                "average_of_average_trade_return_pct": _fmt(avg_return, 4),
                "average_profitable_trade_rate_pct": _fmt(avg_win, 4),
                "average_take_profit_rate_pct": _fmt(avg_hit, 4),
                "average_stop_loss_rate_pct": _fmt(avg_stop, 4),
                "best_profit_factor": _fmt(best_pf, 4),
                "score": _fmt(score, 4),
                "best_strategy_code": best["strategy_code"],
                "best_strategy_label": best["strategy_label"],
            }
        )
    rows.sort(key=lambda row: _float(row, "score"), reverse=True)
    _write_csv(
        output_path,
        rows,
        [
            "ticker",
            "strategies_count",
            "total_trades",
            "average_of_average_trade_return_pct",
            "average_profitable_trade_rate_pct",
            "average_take_profit_rate_pct",
            "average_stop_loss_rate_pct",
            "best_profit_factor",
            "score",
            "best_strategy_code",
            "best_strategy_label",
        ],
    )
    return rows


def _write_report(
    path: Path,
    rows: list[dict[str, str]],
    args: argparse.Namespace,
    ticker_rows: int,
    action_rows: list[dict[str, str]],
) -> None:
    lines = [
        "# R3 High Accuracy",
        "",
        "Filtro voltado para maior taxa de acerto, usando somente estrategias percentuais ja mineradas.",
        "",
        "## Criterios",
        "",
        f"- alvo percentual: ate {_fmt(args.max_target_pct)}%",
        f"- trades globais: >= {args.min_trades}",
        f"- tickers globais: >= {args.min_tickers}",
        f"- win rate global: >= {_fmt(args.min_win_rate)}%",
        f"- alvo batido global: >= {_fmt(args.min_take_profit_rate)}%",
        f"- profit factor global: >= {_fmt(args.min_profit_factor)}",
        f"- trades teste 2026: >= {args.min_test_trades}",
        f"- win rate teste 2026: >= {_fmt(args.min_test_win_rate)}%",
        f"- alvo batido teste 2026: >= {_fmt(args.min_test_take_profit_rate)}%",
        f"- profit factor teste 2026: >= {_fmt(args.min_test_profit_factor)}",
        "",
        "## Resultado",
        "",
        f"- estrategias aprovadas: {len(rows)}",
        f"- linhas por ticker exportadas: {ticker_rows}",
        f"- acoes consolidadas: {len(action_rows)}",
        "",
        "## Top Estrategias",
        "",
        "| rank | code | alvo | stop | trades | tickers | win | alvo batido | test trades | test win | test alvo | test avg | test PF |",
        "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for rank, row in enumerate(rows[: args.top], 1):
        lines.append(
            "| "
            + " | ".join(
                [
                    str(rank),
                    row["code"],
                    _fmt(_float(row, "target_pct")),
                    _fmt(_float(row, "stop_pct")),
                    str(int(_float(row, "trades"))),
                    str(int(_float(row, "tickers"))),
                    _fmt(_float(row, "profitable_rate_pct")),
                    _fmt(_float(row, "take_profit_rate_pct")),
                    str(int(_float(row, "test_trades"))),
                    _fmt(_float(row, "test_profitable_rate_pct")),
                    _fmt(_float(row, "test_take_profit_rate_pct")),
                    _fmt(_float(row, "test_average_trade_return_pct")),
                    _fmt(_float(row, "test_profit_factor")),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Top Acoes",
            "",
            "| rank | ticker | estrategias | trades | win medio | alvo medio | stop medio | retorno medio | score |",
            "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for rank, row in enumerate(action_rows[: args.top], 1):
        lines.append(
            "| "
            + " | ".join(
                [
                    str(rank),
                    row["ticker"],
                    row["strategies_count"],
                    row["total_trades"],
                    _fmt(_float(row, "average_profitable_trade_rate_pct")),
                    _fmt(_float(row, "average_take_profit_rate_pct")),
                    _fmt(_float(row, "average_stop_loss_rate_pct")),
                    _fmt(_float(row, "average_of_average_trade_return_pct")),
                    _fmt(_float(row, "score")),
                ]
            )
            + " |"
        )
    lines.append("")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    args = parse_args()
    input_path = Path(args.input_csv)
    output_path = Path(args.output_csv)

    with input_path.open(encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        fieldnames = list(reader.fieldnames or [])
        rows = [row for row in reader if _passes(row, args)]

    rows.sort(
        key=lambda row: (
            _accuracy_score(row),
            _float(row, "test_profitable_rate_pct"),
            _float(row, "test_take_profit_rate_pct"),
            _float(row, "profitable_rate_pct"),
            _float(row, "trades"),
        ),
        reverse=True,
    )

    output_fieldnames = fieldnames + ["accuracy_score"] if "accuracy_score" not in fieldnames else fieldnames
    output_rows: list[dict[str, str]] = []
    for row in rows:
        out = dict(row)
        out["accuracy_score"] = _fmt(_accuracy_score(row), 4)
        output_rows.append(out)

    _write_csv(output_path, output_rows, output_fieldnames)

    ticker_rows = _write_ticker_csv_from_trades(
        input_path=Path(args.trades_input_csv),
        output_path=Path(args.tickers_output_csv),
        strategies={row["code"]: row for row in rows},
    )
    if ticker_rows == 0:
        ticker_rows = _write_ticker_csv(
            input_path=Path(args.tickers_input_csv),
            output_path=Path(args.tickers_output_csv),
            strategy_codes={row["code"] for row in rows},
        )
    action_rows = _write_actions_csv(Path(args.tickers_output_csv), Path(args.actions_output_csv))
    _write_report(Path(args.report_md), rows, args, ticker_rows, action_rows)

    print(f"high_accuracy={len(rows)} -> {output_path}")
    print(f"ticker_rows={ticker_rows} -> {args.tickers_output_csv}")
    print(f"actions={len(action_rows)} -> {args.actions_output_csv}")
    print(f"report -> {args.report_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
