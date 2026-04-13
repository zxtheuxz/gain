from __future__ import annotations

import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"

APPROVED_PATH = REPORTS / "asset-discovery-lista-approved.csv"
TICKERS_PATH = REPORTS / "asset-discovery-lista-tickers-all.csv"

SHORT_CSV = REPORTS / "asset-discovery-lista-shortlist-min1pct.csv"
SHORT_MD = REPORTS / "asset-discovery-lista-shortlist-min1pct.md"
TICKER_CSV = REPORTS / "asset-discovery-lista-tickers-min1pct.csv"
OVERALL_CSV = REPORTS / "asset-discovery-lista-actions-overall-min1pct.csv"
REPORT_MD = REPORTS / "asset-discovery-lista-report-min1pct.md"


def _float(value: str) -> float:
    return float(value) if value not in ("", "None") else 0.0


def _int(value: str) -> int:
    return int(float(value)) if value else 0


def main() -> None:
    strategy_min_success = 58.0
    strategy_min_pf = 2.0
    strategy_min_avg = 1.0
    strategy_min_trades = 80
    strategy_min_tickers = 8

    ticker_min_trades = 15
    ticker_min_success = 55.0
    ticker_min_avg = 1.0
    ticker_min_pf = 1.5

    approved_rows: list[dict[str, object]] = []
    with APPROVED_PATH.open(encoding="utf-8", newline="") as file_obj:
        for row in csv.DictReader(file_obj):
            parsed = dict(row)
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

    shortlist = [
        row for row in approved_rows
        if row["success_rate_pct"] >= strategy_min_success
        and row["profit_factor"] >= strategy_min_pf
        and row["average_trade_return_pct"] >= strategy_min_avg
        and row["total_occurrences"] >= strategy_min_trades
        and row["tickers_with_matches"] >= strategy_min_tickers
    ]
    shortlist.sort(
        key=lambda row: (
            row["net_trade_return_pct"],
            row["success_rate_pct"],
            row["profit_factor"],
            row["average_trade_return_pct"],
        ),
        reverse=True,
    )

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
    with SHORT_CSV.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=strategy_fields)
        writer.writeheader()
        for row in shortlist:
            output = {key: row[key] for key in strategy_fields}
            for key in [
                "success_rate_pct",
                "profitable_trade_rate_pct",
                "average_trade_return_pct",
                "net_trade_return_pct",
                "profit_factor",
            ]:
                output[key] = f"{row[key]:.4f}"
            writer.writerow(output)

    shortlist_codes = {row["code"] for row in shortlist}
    ticker_rows: list[dict[str, str]] = []
    with TICKERS_PATH.open(encoding="utf-8", newline="") as file_obj:
        for row in csv.DictReader(file_obj):
            if row["strategy_code"] not in shortlist_codes:
                continue
            if _int(row["total_trades"]) < ticker_min_trades:
                continue
            if _float(row["success_rate_pct"]) < ticker_min_success:
                continue
            if _float(row["average_trade_return_pct"]) < ticker_min_avg:
                continue
            if _float(row["profit_factor"]) < ticker_min_pf:
                continue
            if _float(row["net_trade_return_pct"]) <= 0:
                continue
            ticker_rows.append(row)

    ticker_rows.sort(
        key=lambda row: (
            row["strategy_code"],
            -_float(row["net_trade_return_pct"]),
            -_float(row["average_trade_return_pct"]),
            -_float(row["profit_factor"]),
        )
    )
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
    with TICKER_CSV.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=ticker_fields)
        writer.writeheader()
        writer.writerows(ticker_rows)

    overall: dict[str, dict[str, object]] = {}
    for row in ticker_rows:
        item = overall.setdefault(
            row["ticker"],
            {
                "ticker": row["ticker"],
                "strategies": 0,
                "trades": 0,
                "sum_net": 0.0,
                "sum_avg": 0.0,
                "sum_success": 0.0,
                "best_pf": 0.0,
                "best_strategy": "",
            },
        )
        item["strategies"] = int(item["strategies"]) + 1
        item["trades"] = int(item["trades"]) + _int(row["total_trades"])
        item["sum_net"] = float(item["sum_net"]) + _float(row["net_trade_return_pct"])
        item["sum_avg"] = float(item["sum_avg"]) + _float(row["average_trade_return_pct"])
        item["sum_success"] = float(item["sum_success"]) + _float(row["success_rate_pct"])
        profit_factor = _float(row["profit_factor"])
        if profit_factor > float(item["best_pf"]):
            item["best_pf"] = profit_factor
            item["best_strategy"] = row["strategy_label"]

    overall_rows: list[dict[str, object]] = []
    for item in overall.values():
        count = int(item["strategies"])
        overall_rows.append(
            {
                "ticker": item["ticker"],
                "strategies_count": count,
                "total_trades": item["trades"],
                "sum_net_trade_return_pct": f"{float(item['sum_net']):.4f}",
                "average_of_average_trade_return_pct": f"{float(item['sum_avg']) / count:.4f}",
                "average_success_rate_pct": f"{float(item['sum_success']) / count:.4f}",
                "best_profit_factor": f"{float(item['best_pf']):.4f}",
                "best_strategy_label": item["best_strategy"],
            }
        )
    overall_rows.sort(
        key=lambda row: (
            -int(row["strategies_count"]),
            -float(row["sum_net_trade_return_pct"]),
            -int(row["total_trades"]),
            row["ticker"],
        )
    )
    with OVERALL_CSV.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(
            file_obj,
            fieldnames=[
                "ticker",
                "strategies_count",
                "total_trades",
                "sum_net_trade_return_pct",
                "average_of_average_trade_return_pct",
                "average_success_rate_pct",
                "best_profit_factor",
                "best_strategy_label",
            ],
        )
        writer.writeheader()
        writer.writerows(overall_rows)

    lines = [
        "# Lista Liquida - Shortlist Min 1%",
        "",
        "Universo: `lista.md`, acoes grandes/mais liquidas da B3.",
        "Janela: `2025-04-10` ate `2026-04-10`.",
        "",
        (
            "Filtro final: acerto >= 58%, PF >= 2,00, retorno medio >= 1,00%, "
            "minimo 80 trades e 8 acoes."
        ),
        "",
        f"- Estrategias aprovadas brutas: `{len(approved_rows)}`",
        f"- Estrategias finais no corte: `{len(shortlist)}`",
        f"- Linhas estrategia x acao no corte por ticker: `{len(ticker_rows)}`",
        f"- Acoes qualificadas: `{len(overall_rows)}`",
        "",
        "| Estrategia | Trades | Acoes | Acerto | Media | Net | PF |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in shortlist[:30]:
        lines.append(
            f"| {row['label']} | {row['total_occurrences']} | {row['tickers_with_matches']} | "
            f"{row['success_rate_pct']:.2f}% | {row['average_trade_return_pct']:.2f}% | "
            f"{row['net_trade_return_pct']:.2f}% | {row['profit_factor']:.2f} |"
        )
    SHORT_MD.write_text("\n".join(lines), encoding="utf-8")

    rows_by_code: dict[str, list[dict[str, str]]] = {}
    for row in ticker_rows:
        rows_by_code.setdefault(row["strategy_code"], []).append(row)

    report_lines = [
        "# Lista Liquida - Relatorio de Estrategias e Acoes",
        "",
        "Este relatorio usa apenas os ativos de `lista.md`, para evitar sinais em papeis sem book/oferta suficiente.",
        "",
        "## Resumo",
        f"- Estrategias finais: `{len(shortlist)}`",
        f"- Acoes qualificadas: `{len(overall_rows)}`",
        (
            "- Corte por estrategia: acerto >= 58%, PF >= 2,00, media >= 1,00%, "
            "trades >= 80, acoes >= 8"
        ),
        (
            "- Corte por acao: trades >= 15, acerto >= 55%, media >= 1,00%, "
            "PF >= 1,50, lucro liquido > 0"
        ),
        "",
        "## Acoes que mais aparecem",
    ]
    for index, row in enumerate(overall_rows[:20], 1):
        report_lines.append(
            f"{index}. `{row['ticker']}` em `{row['strategies_count']}` estrategias | trades `{row['total_trades']}` | "
            f"media `{row['average_of_average_trade_return_pct']}%` | acerto `{row['average_success_rate_pct']}%` | "
            f"net `{row['sum_net_trade_return_pct']}%`"
        )
    report_lines.append("")
    for strategy in shortlist:
        report_lines.append(f"## {strategy['label']}")
        report_lines.append(
            f"- Trades: `{strategy['total_occurrences']}` | Acoes: `{strategy['tickers_with_matches']}` | "
            f"Acerto: `{strategy['success_rate_pct']:.2f}%` | Media: `{strategy['average_trade_return_pct']:.2f}%` | "
            f"Net: `{strategy['net_trade_return_pct']:.2f}%` | PF: `{strategy['profit_factor']:.2f}`"
        )
        report_lines.append(f"- Estado: `{strategy['state_signature']}`")
        report_lines.append("")
        strategy_ticker_rows = rows_by_code.get(strategy["code"], [])
        if not strategy_ticker_rows:
            report_lines.append("- Nenhuma acao passou no corte individual desta estrategia.")
            report_lines.append("")
            continue
        report_lines.append("| Acao | Trades | Acerto | Media | Net | PF | Periodo |")
        report_lines.append("| --- | ---: | ---: | ---: | ---: | ---: | --- |")
        for row in strategy_ticker_rows[:15]:
            report_lines.append(
                f"| {row['ticker']} | {row['total_trades']} | {float(row['success_rate_pct']):.2f}% | "
                f"{float(row['average_trade_return_pct']):.2f}% | {float(row['net_trade_return_pct']):.2f}% | "
                f"{float(row['profit_factor']):.2f} | {row['first_trade_date']} a {row['last_trade_date']} |"
            )
        report_lines.append("")
    REPORT_MD.write_text("\n".join(report_lines), encoding="utf-8")

    print(SHORT_CSV)
    print(SHORT_MD)
    print(TICKER_CSV)
    print(OVERALL_CSV)
    print(REPORT_MD)
    print(f"short={len(shortlist)}")
    print(f"ticker_rows={len(ticker_rows)}")
    print(f"actions={len(overall_rows)}")


if __name__ == "__main__":
    main()
