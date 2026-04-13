from __future__ import annotations

import csv
import math
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"

APPROVED_PATH = REPORTS / "asset-discovery-lista-r2-free-approved.csv"
TICKERS_PATH = REPORTS / "asset-discovery-lista-r2-free-tickers-all.csv"

STRATEGIES_CSV = REPORTS / "asset-discovery-lista-r2-operational-strategies.csv"
TICKERS_CSV = REPORTS / "asset-discovery-lista-r2-operational-tickers.csv"
ACTIONS_CSV = REPORTS / "asset-discovery-lista-r2-operational-actions.csv"
REPORT_MD = REPORTS / "asset-discovery-lista-r2-operational-report.md"


GLOBAL_MIN_TRADES = 300
GLOBAL_MIN_TICKERS = 50
GLOBAL_MIN_WIN = 65.0
GLOBAL_MIN_AVG = 1.5
GLOBAL_MIN_PF = 2.5

TICKER_MIN_TRADES = 15
TICKER_MIN_WIN = 65.0
TICKER_MIN_AVG = 1.5
TICKER_MIN_PF = 2.5

MIN_QUALIFIED_TICKERS_PER_STRATEGY = 3


STRATEGY_FIELDS = [
    "code",
    "label",
    "family",
    "template_code",
    "template_label",
    "trade_direction",
    "entry_rule",
    "take_profit_pct",
    "stop_loss_pct",
    "time_cap_days",
    "state_size",
    "state_signature",
    "feature_keys",
    "tickers_with_matches",
    "total_occurrences",
    "success_rate_pct",
    "profitable_trade_rate_pct",
    "average_trade_return_pct",
    "net_trade_return_pct",
    "profit_factor",
    "qualified_tickers_count",
    "qualified_rows_count",
    "operational_score",
    "first_trade_date",
    "last_trade_date",
]

TICKER_FIELDS = [
    "strategy_code",
    "strategy_label",
    "family",
    "ticker",
    "total_trades",
    "successful_trades",
    "success_rate_pct",
    "profitable_trades",
    "profitable_trade_rate_pct",
    "average_trade_return_pct",
    "median_trade_return_pct",
    "net_trade_return_pct",
    "cumulative_return_pct",
    "profit_factor",
    "first_trade_date",
    "last_trade_date",
]


def _float(value: object) -> float:
    if value in ("", "None", None):
        return 0.0
    if str(value).upper() == "INF":
        return math.inf
    return float(value)


def _int(value: object) -> int:
    return int(float(value)) if value not in ("", None) else 0


def _format(value: float, digits: int = 4) -> str:
    if math.isinf(value):
        return "INF"
    return f"{value:.{digits}f}"


def _write_csv(path: Path, rows: list[dict[str, object]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _strategy_score(row: dict[str, object], qualified_tickers: int, qualified_rows: int) -> float:
    trades_component = min(math.log10(max(_int(row["total_occurrences"]), 1)), 3.0) * 4.0
    ticker_component = min(math.log10(max(_int(row["tickers_with_matches"]), 1)), 2.0) * 4.0
    return (
        (_float(row["average_trade_return_pct"]) * 25.0)
        + ((_float(row["profitable_trade_rate_pct"]) - 50.0) * 1.5)
        + (min(_float(row["profit_factor"]), 8.0) * 8.0)
        + trades_component
        + ticker_component
        + (qualified_tickers * 5.0)
        + (qualified_rows * 0.5)
    )


def _action_score(row: dict[str, object], strategies_count: int, total_trades: int) -> float:
    return (
        (_float(row["average_of_average_trade_return_pct"]) * 25.0)
        + ((_float(row["average_profitable_trade_rate_pct"]) - 50.0) * 1.5)
        + (min(_float(row["best_profit_factor"]), 12.0) * 5.0)
        + (strategies_count * 10.0)
        + min(total_trades, 120) * 0.15
    )


def main() -> None:
    approved: list[dict[str, object]] = []
    with APPROVED_PATH.open(encoding="utf-8", newline="") as file_obj:
        for row in csv.DictReader(file_obj):
            parsed: dict[str, object] = dict(row)
            for key in [
                "success_rate_pct",
                "profitable_trade_rate_pct",
                "average_trade_return_pct",
                "net_trade_return_pct",
                "profit_factor",
            ]:
                parsed[key] = _float(row[key])
            for key in ["tickers_with_matches", "total_occurrences", "state_size"]:
                parsed[key] = _int(row[key])
            approved.append(parsed)

    global_candidates = {
        str(row["code"]): row
        for row in approved
        if _int(row["total_occurrences"]) >= GLOBAL_MIN_TRADES
        and _int(row["tickers_with_matches"]) >= GLOBAL_MIN_TICKERS
        and _float(row["profitable_trade_rate_pct"]) >= GLOBAL_MIN_WIN
        and _float(row["average_trade_return_pct"]) >= GLOBAL_MIN_AVG
        and _float(row["profit_factor"]) >= GLOBAL_MIN_PF
    }

    ticker_rows_by_strategy: dict[str, list[dict[str, str]]] = {}
    with TICKERS_PATH.open(encoding="utf-8", newline="") as file_obj:
        for row in csv.DictReader(file_obj):
            strategy_code = row["strategy_code"]
            if strategy_code not in global_candidates:
                continue
            if _int(row["total_trades"]) < TICKER_MIN_TRADES:
                continue
            if _float(row["profitable_trade_rate_pct"]) < TICKER_MIN_WIN:
                continue
            if _float(row["average_trade_return_pct"]) < TICKER_MIN_AVG:
                continue
            if _float(row["profit_factor"]) < TICKER_MIN_PF:
                continue
            if _float(row["net_trade_return_pct"]) <= 0:
                continue
            ticker_rows_by_strategy.setdefault(strategy_code, []).append(row)

    selected_strategies: list[dict[str, object]] = []
    for strategy_code, row in global_candidates.items():
        strategy_ticker_rows = ticker_rows_by_strategy.get(strategy_code, [])
        qualified_tickers = len({item["ticker"] for item in strategy_ticker_rows})
        if qualified_tickers < MIN_QUALIFIED_TICKERS_PER_STRATEGY:
            continue
        output = {key: row.get(key, "") for key in STRATEGY_FIELDS if key not in {"qualified_tickers_count", "qualified_rows_count", "operational_score"}}
        output["qualified_tickers_count"] = qualified_tickers
        output["qualified_rows_count"] = len(strategy_ticker_rows)
        output["operational_score"] = _strategy_score(row, qualified_tickers, len(strategy_ticker_rows))
        selected_strategies.append(output)

    selected_strategies.sort(
        key=lambda row: (
            -float(row["operational_score"]),
            -_float(row["average_trade_return_pct"]),
            -_float(row["profitable_trade_rate_pct"]),
            -_int(row["total_occurrences"]),
        )
    )
    selected_codes = {str(row["code"]) for row in selected_strategies}

    selected_ticker_rows: list[dict[str, str]] = []
    for strategy_code in selected_codes:
        selected_ticker_rows.extend(ticker_rows_by_strategy.get(strategy_code, []))
    selected_ticker_rows.sort(
        key=lambda row: (
            row["strategy_code"],
            -_float(row["average_trade_return_pct"]),
            -_float(row["profitable_trade_rate_pct"]),
            -_float(row["profit_factor"]),
            row["ticker"],
        )
    )

    action_summary: dict[str, dict[str, object]] = {}
    for row in selected_ticker_rows:
        item = action_summary.setdefault(
            row["ticker"],
            {
                "ticker": row["ticker"],
                "strategies_count": 0,
                "total_trades": 0,
                "sum_net_trade_return_pct": 0.0,
                "sum_average_trade_return_pct": 0.0,
                "sum_profitable_trade_rate_pct": 0.0,
                "best_profit_factor": 0.0,
                "best_strategy_label": "",
            },
        )
        item["strategies_count"] = _int(item["strategies_count"]) + 1
        item["total_trades"] = _int(item["total_trades"]) + _int(row["total_trades"])
        item["sum_net_trade_return_pct"] = _float(item["sum_net_trade_return_pct"]) + _float(row["net_trade_return_pct"])
        item["sum_average_trade_return_pct"] = (
            _float(item["sum_average_trade_return_pct"]) + _float(row["average_trade_return_pct"])
        )
        item["sum_profitable_trade_rate_pct"] = (
            _float(item["sum_profitable_trade_rate_pct"]) + _float(row["profitable_trade_rate_pct"])
        )
        if _float(row["profit_factor"]) > _float(item["best_profit_factor"]):
            item["best_profit_factor"] = _float(row["profit_factor"])
            item["best_strategy_label"] = row["strategy_label"]

    action_rows: list[dict[str, object]] = []
    for item in action_summary.values():
        strategies_count = _int(item["strategies_count"])
        total_trades = _int(item["total_trades"])
        row = {
            "ticker": item["ticker"],
            "strategies_count": strategies_count,
            "total_trades": total_trades,
            "sum_net_trade_return_pct": _float(item["sum_net_trade_return_pct"]),
            "average_of_average_trade_return_pct": _float(item["sum_average_trade_return_pct"]) / strategies_count,
            "average_profitable_trade_rate_pct": _float(item["sum_profitable_trade_rate_pct"]) / strategies_count,
            "best_profit_factor": _float(item["best_profit_factor"]),
            "best_strategy_label": item["best_strategy_label"],
        }
        row["operational_score"] = _action_score(row, strategies_count, total_trades)
        action_rows.append(row)
    action_rows.sort(key=lambda row: (-_float(row["operational_score"]), row["ticker"]))

    strategy_output: list[dict[str, object]] = []
    for row in selected_strategies:
        item = dict(row)
        for key in [
            "success_rate_pct",
            "profitable_trade_rate_pct",
            "average_trade_return_pct",
            "net_trade_return_pct",
            "profit_factor",
            "operational_score",
        ]:
            item[key] = _format(_float(item[key]))
        strategy_output.append(item)

    action_fields = [
        "ticker",
        "strategies_count",
        "total_trades",
        "sum_net_trade_return_pct",
        "average_of_average_trade_return_pct",
        "average_profitable_trade_rate_pct",
        "best_profit_factor",
        "operational_score",
        "best_strategy_label",
    ]
    action_output: list[dict[str, object]] = []
    for row in action_rows:
        item = dict(row)
        for key in [
            "sum_net_trade_return_pct",
            "average_of_average_trade_return_pct",
            "average_profitable_trade_rate_pct",
            "best_profit_factor",
            "operational_score",
        ]:
            item[key] = _format(_float(item[key]))
        action_output.append(item)

    _write_csv(STRATEGIES_CSV, strategy_output, STRATEGY_FIELDS)
    _write_csv(TICKERS_CSV, selected_ticker_rows, TICKER_FIELDS)
    _write_csv(ACTIONS_CSV, action_output, action_fields)

    lines = [
        "# Rodada 2 - Filtro Operacional",
        "",
        "Objetivo: separar estrategias descobertas de estrategias operaveis.",
        "",
        "## Regra de corte",
        f"- Estrategia global: trades >= {GLOBAL_MIN_TRADES}, acoes >= {GLOBAL_MIN_TICKERS}, acerto verde >= {GLOBAL_MIN_WIN:.0f}%, media >= {GLOBAL_MIN_AVG:.1f}%, PF >= {GLOBAL_MIN_PF:.1f}.",
        f"- Acao dentro da estrategia: trades >= {TICKER_MIN_TRADES}, acerto verde >= {TICKER_MIN_WIN:.0f}%, media >= {TICKER_MIN_AVG:.1f}%, PF >= {TICKER_MIN_PF:.1f}, lucro liquido > 0.",
        f"- Estrategia so entra se tiver pelo menos {MIN_QUALIFIED_TICKERS_PER_STRATEGY} acoes qualificadas individualmente.",
        "",
        "## Resultado",
        f"- Estrategias globais fortes antes do corte por acao: `{len(global_candidates)}`",
        f"- Estrategias operacionais finais: `{len(selected_strategies)}`",
        f"- Linhas estrategia x acao operacionais: `{len(selected_ticker_rows)}`",
        f"- Acoes operacionais: `{len(action_rows)}`",
        "",
        "## Estrategias operacionais",
        "",
        "| # | Estrategia | Trades | Acoes | Acoes qualificadas | Acerto verde | Media | PF | Score |",
        "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for index, row in enumerate(selected_strategies, 1):
        lines.append(
            f"| {index} | {row['label']} | {row['total_occurrences']} | {row['tickers_with_matches']} | "
            f"{row['qualified_tickers_count']} | {_float(row['profitable_trade_rate_pct']):.2f}% | "
            f"{_float(row['average_trade_return_pct']):.2f}% | {_float(row['profit_factor']):.2f} | "
            f"{_float(row['operational_score']):.2f} |"
        )

    lines.extend(["", "## Acoes operacionais", ""])
    for index, row in enumerate(action_rows, 1):
        lines.append(
            f"{index}. `{row['ticker']}` em `{row['strategies_count']}` estrategias | trades `{row['total_trades']}` | "
            f"media `{_float(row['average_of_average_trade_return_pct']):.2f}%` | "
            f"acerto verde `{_float(row['average_profitable_trade_rate_pct']):.2f}%` | "
            f"PF max `{_float(row['best_profit_factor']):.2f}` | score `{_float(row['operational_score']):.2f}`"
        )

    lines.extend(["", "## Detalhe por estrategia", ""])
    for strategy in selected_strategies:
        lines.append(f"### {strategy['label']}")
        lines.append(
            f"- Backtest: trades `{strategy['total_occurrences']}`, acerto verde `{_float(strategy['profitable_trade_rate_pct']):.2f}%`, "
            f"media `{_float(strategy['average_trade_return_pct']):.2f}%`, PF `{_float(strategy['profit_factor']):.2f}`."
        )
        lines.append("")
        lines.append("| Acao | Trades | Acerto verde | Media | Net | PF | Periodo |")
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: | --- |")
        for row in ticker_rows_by_strategy.get(str(strategy["code"]), [])[:20]:
            lines.append(
                f"| {row['ticker']} | {row['total_trades']} | {_float(row['profitable_trade_rate_pct']):.2f}% | "
                f"{_float(row['average_trade_return_pct']):.2f}% | {_float(row['net_trade_return_pct']):.2f}% | "
                f"{_float(row['profit_factor']):.2f} | {row['first_trade_date']} a {row['last_trade_date']} |"
            )
        lines.append("")

    lines.extend(
        [
            "## Arquivos",
            f"- Estrategias: `{STRATEGIES_CSV.relative_to(ROOT)}`",
            f"- Estrategia x acao: `{TICKERS_CSV.relative_to(ROOT)}`",
            f"- Acoes consolidadas: `{ACTIONS_CSV.relative_to(ROOT)}`",
        ]
    )
    REPORT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(STRATEGIES_CSV)
    print(TICKERS_CSV)
    print(ACTIONS_CSV)
    print(REPORT_MD)
    print(f"global_candidates={len(global_candidates)}")
    print(f"operational_strategies={len(selected_strategies)}")
    print(f"operational_ticker_rows={len(selected_ticker_rows)}")
    print(f"operational_actions={len(action_rows)}")


if __name__ == "__main__":
    main()
