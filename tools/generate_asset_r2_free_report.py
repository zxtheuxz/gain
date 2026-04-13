from __future__ import annotations

import csv
import math
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"

APPROVED_PATH = REPORTS / "asset-discovery-lista-r2-free-approved.csv"
TICKERS_PATH = REPORTS / "asset-discovery-lista-r2-free-tickers-all.csv"

ROBUST_CSV = REPORTS / "asset-discovery-lista-r2-free-robust.csv"
AGGRESSIVE_CSV = REPORTS / "asset-discovery-lista-r2-free-aggressive.csv"
TOP10_CSV = REPORTS / "asset-discovery-lista-r2-free-top10-robust.csv"
TOP10_TICKERS_CSV = REPORTS / "asset-discovery-lista-r2-free-top10-tickers.csv"
TOP10_ACTIONS_CSV = REPORTS / "asset-discovery-lista-r2-free-top10-actions.csv"
REPORT_MD = REPORTS / "asset-discovery-lista-r2-free-report.md"


def _float(value: str) -> float:
    if value in ("", "None", None):
        return 0.0
    if str(value).upper() == "INF":
        return math.inf
    return float(value)


def _int(value: str) -> int:
    return int(float(value)) if value else 0


def _write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _strategy_sort_key(row: dict[str, object]) -> tuple[float, float, float, int]:
    return (
        float(row["average_trade_return_pct"]),
        float(row["profitable_trade_rate_pct"]),
        float(row["profit_factor"]),
        int(row["total_occurrences"]),
    )


def _format_float(value: object, digits: int = 4) -> str:
    value_float = float(value)
    if math.isinf(value_float):
        return "INF"
    return f"{value_float:.{digits}f}"


def main() -> None:
    strategy_fields = [
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
        "first_trade_date",
        "last_trade_date",
    ]
    ticker_fields = [
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

    approved_rows: list[dict[str, object]] = []
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
            for key in ["total_occurrences", "tickers_with_matches", "state_size"]:
                parsed[key] = _int(row[key])
            approved_rows.append(parsed)

    strict_rows = [
        row
        for row in approved_rows
        if float(row["profitable_trade_rate_pct"]) >= 60.0
        and float(row["average_trade_return_pct"]) >= 1.0
        and float(row["profit_factor"]) >= 2.0
        and int(row["total_occurrences"]) >= 80
        and int(row["tickers_with_matches"]) >= 8
    ]
    robust_rows = [
        row
        for row in strict_rows
        if int(row["total_occurrences"]) >= 300 and int(row["tickers_with_matches"]) >= 50
    ]
    aggressive_rows = [
        row
        for row in strict_rows
        if float(row["profitable_trade_rate_pct"]) >= 70.0
        and float(row["average_trade_return_pct"]) >= 1.5
        and float(row["profit_factor"]) >= 3.0
        and int(row["total_occurrences"]) >= 80
        and int(row["tickers_with_matches"]) >= 25
    ]

    robust_rows.sort(key=_strategy_sort_key, reverse=True)
    aggressive_rows.sort(key=_strategy_sort_key, reverse=True)
    top10_rows = robust_rows[:10]

    def _strategy_output(row: dict[str, object]) -> dict[str, object]:
        output = {key: row[key] for key in strategy_fields}
        for key in [
            "success_rate_pct",
            "profitable_trade_rate_pct",
            "average_trade_return_pct",
            "net_trade_return_pct",
            "profit_factor",
        ]:
            output[key] = _format_float(row[key])
        return output

    _write_csv(ROBUST_CSV, [_strategy_output(row) for row in robust_rows], strategy_fields)
    _write_csv(AGGRESSIVE_CSV, [_strategy_output(row) for row in aggressive_rows], strategy_fields)
    _write_csv(TOP10_CSV, [_strategy_output(row) for row in top10_rows], strategy_fields)

    top10_codes = {str(row["code"]) for row in top10_rows}
    ticker_rows: list[dict[str, str]] = []
    with TICKERS_PATH.open(encoding="utf-8", newline="") as file_obj:
        for row in csv.DictReader(file_obj):
            if row["strategy_code"] not in top10_codes:
                continue
            if _int(row["total_trades"]) < 15:
                continue
            if _float(row["profitable_trade_rate_pct"]) < 60.0:
                continue
            if _float(row["average_trade_return_pct"]) < 1.0:
                continue
            if _float(row["profit_factor"]) < 2.0:
                continue
            if _float(row["net_trade_return_pct"]) <= 0:
                continue
            ticker_rows.append(row)

    ticker_rows.sort(
        key=lambda row: (
            row["strategy_code"],
            -_float(row["average_trade_return_pct"]),
            -_float(row["profitable_trade_rate_pct"]),
            -_float(row["profit_factor"]),
        )
    )
    _write_csv(TOP10_TICKERS_CSV, ticker_rows, ticker_fields)

    action_summary: dict[str, dict[str, object]] = {}
    for row in ticker_rows:
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
        item["strategies_count"] = int(item["strategies_count"]) + 1
        item["total_trades"] = int(item["total_trades"]) + _int(row["total_trades"])
        item["sum_net_trade_return_pct"] = float(item["sum_net_trade_return_pct"]) + _float(row["net_trade_return_pct"])
        item["sum_average_trade_return_pct"] = (
            float(item["sum_average_trade_return_pct"]) + _float(row["average_trade_return_pct"])
        )
        item["sum_profitable_trade_rate_pct"] = (
            float(item["sum_profitable_trade_rate_pct"]) + _float(row["profitable_trade_rate_pct"])
        )
        profit_factor = _float(row["profit_factor"])
        if profit_factor > float(item["best_profit_factor"]):
            item["best_profit_factor"] = profit_factor
            item["best_strategy_label"] = row["strategy_label"]

    action_rows: list[dict[str, object]] = []
    for item in action_summary.values():
        strategies_count = int(item["strategies_count"])
        action_rows.append(
            {
                "ticker": item["ticker"],
                "strategies_count": strategies_count,
                "total_trades": item["total_trades"],
                "sum_net_trade_return_pct": _format_float(item["sum_net_trade_return_pct"]),
                "average_of_average_trade_return_pct": _format_float(
                    float(item["sum_average_trade_return_pct"]) / strategies_count
                ),
                "average_profitable_trade_rate_pct": _format_float(
                    float(item["sum_profitable_trade_rate_pct"]) / strategies_count
                ),
                "best_profit_factor": _format_float(item["best_profit_factor"]),
                "best_strategy_label": item["best_strategy_label"],
            }
        )
    action_rows.sort(
        key=lambda row: (
            -int(row["strategies_count"]),
            -float(row["sum_net_trade_return_pct"]),
            -int(row["total_trades"]),
            row["ticker"],
        )
    )
    _write_csv(
        TOP10_ACTIONS_CSV,
        action_rows,
        [
            "ticker",
            "strategies_count",
            "total_trades",
            "sum_net_trade_return_pct",
            "average_of_average_trade_return_pct",
            "average_profitable_trade_rate_pct",
            "best_profit_factor",
            "best_strategy_label",
        ],
    )

    ticker_rows_by_code: dict[str, list[dict[str, str]]] = {}
    for row in ticker_rows:
        ticker_rows_by_code.setdefault(row["strategy_code"], []).append(row)

    lines = [
        "# Rodada 2 Livre - Estrategias com Lista Liquida",
        "",
        "Universo: `lista.md` com acoes mais liquidas.",
        "Janela do backtest: `2025-04-10` ate `2026-04-10`.",
        "",
        "## O que mudou nesta rodada",
        "- Testamos apenas compra (`long`), porque as rodadas anteriores concentraram os melhores resultados em compra.",
        "- Entradas testadas: abertura e fechamento.",
        "- Saidas testadas: alvo/stop percentuais `4:2`, `5:2`, `6:2`, `6:3` e `8:4`, com cap de seguranca de 5 pregoes.",
        "- Padroes com ate 3 fatores combinados.",
        "- Novos fatores: posicao no range de 20D, sequencia de altas/quedas e quantidade de dias positivos nos ultimos 5 pregoes.",
        "",
        "## Filtro",
        "- Corte minimo geral: acerto verde >= 60%, media por trade >= 1%, PF >= 2, trades >= 80 e acoes >= 8.",
        "- Corte robusto: alem do corte minimo, trades >= 300 e acoes >= 50.",
        "- Corte agressivo: acerto verde >= 70%, media >= 1,5%, PF >= 3, trades >= 80 e acoes >= 25.",
        "- Corte por acao para o top 10: trades >= 15, acerto verde >= 60%, media >= 1%, PF >= 2 e lucro liquido > 0.",
        "",
        "## Resultado",
        f"- Estrategias aprovadas pela rodada bruta: `{len(approved_rows)}`",
        f"- Estrategias no corte minimo final: `{len(strict_rows)}`",
        f"- Estrategias robustas: `{len(robust_rows)}`",
        f"- Estrategias agressivas: `{len(aggressive_rows)}`",
        f"- Estrategias no top 10 robusto: `{len(top10_rows)}`",
        f"- Linhas estrategia x acao qualificadas no top 10: `{len(ticker_rows)}`",
        f"- Acoes qualificadas no top 10: `{len(action_rows)}`",
        "",
        "## Top 10 robusto",
        "",
        "| # | Estrategia | Trades | Acoes | Acerto verde | Media/trade | Net | PF |",
        "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for index, row in enumerate(top10_rows, 1):
        lines.append(
            f"| {index} | {row['label']} | {row['total_occurrences']} | {row['tickers_with_matches']} | "
            f"{float(row['profitable_trade_rate_pct']):.2f}% | {float(row['average_trade_return_pct']):.2f}% | "
            f"{float(row['net_trade_return_pct']):.2f}% | {float(row['profit_factor']):.2f} |"
        )

    lines.extend(["", "## Top acoes no top 10 robusto", ""])
    for index, row in enumerate(action_rows[:25], 1):
        lines.append(
            f"{index}. `{row['ticker']}` em `{row['strategies_count']}` estrategias | "
            f"trades `{row['total_trades']}` | media `{row['average_of_average_trade_return_pct']}%` | "
            f"acerto verde `{row['average_profitable_trade_rate_pct']}%` | net `{row['sum_net_trade_return_pct']}%`"
        )

    lines.extend(["", "## Detalhe por estrategia do top 10", ""])
    for row in top10_rows:
        lines.append(f"### {row['label']}")
        lines.append(
            f"- Backtest geral: trades `{row['total_occurrences']}`, acoes `{row['tickers_with_matches']}`, "
            f"acerto verde `{float(row['profitable_trade_rate_pct']):.2f}%`, "
            f"media `{float(row['average_trade_return_pct']):.2f}%`, PF `{float(row['profit_factor']):.2f}`."
        )
        lines.append(f"- Fatores: `{row['state_signature']}`")
        rows_for_strategy = ticker_rows_by_code.get(str(row["code"]), [])
        if not rows_for_strategy:
            lines.append("- Nenhuma acao passou no corte individual desta estrategia.")
            lines.append("")
            continue
        lines.append("")
        lines.append("| Acao | Trades | Acerto verde | Media | Net | PF | Periodo |")
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: | --- |")
        for ticker_row in rows_for_strategy[:20]:
            lines.append(
                f"| {ticker_row['ticker']} | {ticker_row['total_trades']} | "
                f"{_float(ticker_row['profitable_trade_rate_pct']):.2f}% | "
                f"{_float(ticker_row['average_trade_return_pct']):.2f}% | "
                f"{_float(ticker_row['net_trade_return_pct']):.2f}% | "
                f"{_float(ticker_row['profit_factor']):.2f} | "
                f"{ticker_row['first_trade_date']} a {ticker_row['last_trade_date']} |"
            )
        lines.append("")

    lines.extend(
        [
            "## Arquivos gerados",
            f"- Top 10 robusto: `{TOP10_CSV.relative_to(ROOT)}`",
            f"- Robustas completas: `{ROBUST_CSV.relative_to(ROOT)}`",
            f"- Agressivas completas: `{AGGRESSIVE_CSV.relative_to(ROOT)}`",
            f"- Acoes qualificadas do top 10: `{TOP10_TICKERS_CSV.relative_to(ROOT)}`",
            f"- Resumo por acao do top 10: `{TOP10_ACTIONS_CSV.relative_to(ROOT)}`",
        ]
    )

    REPORT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(ROBUST_CSV)
    print(AGGRESSIVE_CSV)
    print(TOP10_CSV)
    print(TOP10_TICKERS_CSV)
    print(TOP10_ACTIONS_CSV)
    print(REPORT_MD)
    print(f"strict={len(strict_rows)}")
    print(f"robust={len(robust_rows)}")
    print(f"aggressive={len(aggressive_rows)}")
    print(f"top10_ticker_rows={len(ticker_rows)}")
    print(f"top10_actions={len(action_rows)}")


if __name__ == "__main__":
    main()
