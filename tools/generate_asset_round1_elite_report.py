from __future__ import annotations

import csv
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
SHORTLIST_CSV = REPORTS / "asset-discovery-round1-super-shortlist.csv"
TICKERS_CSV = REPORTS / "asset-discovery-round1-tickers-all.csv"

ELITE_ALL_CSV = REPORTS / "asset-discovery-round1-elite-tickers.csv"
ELITE_QUALIFIED_CSV = REPORTS / "asset-discovery-round1-elite-tickers-qualified.csv"
ELITE_OVERALL_CSV = REPORTS / "asset-discovery-round1-elite-actions-overall.csv"
ELITE_MD = REPORTS / "asset-discovery-round1-elite-report.md"


def _load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as file_obj:
        return list(csv.DictReader(file_obj))


def _parse_float(value: str) -> float:
    return float(value) if value not in ("", "None") else 0.0


def _parse_int(value: str) -> int:
    return int(float(value)) if value else 0


def _qualified(row: dict[str, str]) -> bool:
    return (
        _parse_int(row["total_trades"]) >= 15
        and _parse_float(row["success_rate_pct"]) >= 55.0
        and _parse_float(row["average_trade_return_pct"]) >= 1.00
        and _parse_float(row["net_trade_return_pct"]) > 0.0
        and _parse_float(row["profit_factor"]) >= 1.50
    )


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    shortlist_rows = _load_csv(SHORTLIST_CSV)
    elite_rows = [row for row in shortlist_rows if row["tier"] == "D_elite"]
    elite_by_code = {row["code"]: row for row in elite_rows}
    elite_codes = set(elite_by_code)

    ticker_rows = _load_csv(TICKERS_CSV)
    elite_ticker_rows = [row for row in ticker_rows if row["strategy_code"] in elite_codes]
    qualified_rows = [row for row in elite_ticker_rows if _qualified(row)]

    elite_ticker_rows_sorted = sorted(
        elite_ticker_rows,
        key=lambda row: (
            row["strategy_code"],
            -_parse_float(row["net_trade_return_pct"]),
            -_parse_float(row["profit_factor"]),
            -_parse_float(row["average_trade_return_pct"]),
            -_parse_int(row["total_trades"]),
            row["ticker"],
        ),
    )
    qualified_rows_sorted = sorted(
        qualified_rows,
        key=lambda row: (
            row["strategy_code"],
            -_parse_float(row["net_trade_return_pct"]),
            -_parse_float(row["profit_factor"]),
            -_parse_float(row["average_trade_return_pct"]),
            -_parse_int(row["total_trades"]),
            row["ticker"],
        ),
    )

    base_fields = [
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
    _write_csv(ELITE_ALL_CSV, base_fields, elite_ticker_rows_sorted)
    _write_csv(ELITE_QUALIFIED_CSV, base_fields, qualified_rows_sorted)

    overall: dict[str, dict[str, object]] = {}
    for row in qualified_rows_sorted:
        ticker = row["ticker"]
        item = overall.setdefault(
            ticker,
            {
                "ticker": ticker,
                "elite_strategies_count": 0,
                "total_trades": 0,
                "sum_net_trade_return_pct": 0.0,
                "average_of_average_trade_return_pct": 0.0,
                "average_success_rate_pct": 0.0,
                "best_profit_factor": 0.0,
                "best_strategy_label": "",
            },
        )
        item["elite_strategies_count"] = int(item["elite_strategies_count"]) + 1
        item["total_trades"] = int(item["total_trades"]) + _parse_int(row["total_trades"])
        item["sum_net_trade_return_pct"] = float(item["sum_net_trade_return_pct"]) + _parse_float(row["net_trade_return_pct"])
        item["average_of_average_trade_return_pct"] = float(item["average_of_average_trade_return_pct"]) + _parse_float(row["average_trade_return_pct"])
        item["average_success_rate_pct"] = float(item["average_success_rate_pct"]) + _parse_float(row["success_rate_pct"])
        pf = _parse_float(row["profit_factor"])
        if pf > float(item["best_profit_factor"]):
            item["best_profit_factor"] = pf
            item["best_strategy_label"] = row["strategy_label"]

    overall_rows: list[dict[str, object]] = []
    for ticker, item in overall.items():
        count = int(item["elite_strategies_count"])
        overall_rows.append(
            {
                "ticker": ticker,
                "elite_strategies_count": count,
                "total_trades": int(item["total_trades"]),
                "sum_net_trade_return_pct": f"{float(item['sum_net_trade_return_pct']):.4f}",
                "average_of_average_trade_return_pct": f"{float(item['average_of_average_trade_return_pct']) / count:.4f}",
                "average_success_rate_pct": f"{float(item['average_success_rate_pct']) / count:.4f}",
                "best_profit_factor": f"{float(item['best_profit_factor']):.4f}",
                "best_strategy_label": item["best_strategy_label"],
            }
        )

    overall_rows.sort(
        key=lambda row: (
            -int(row["elite_strategies_count"]),
            -float(row["sum_net_trade_return_pct"]),
            -int(row["total_trades"]),
            row["ticker"],
        )
    )
    _write_csv(
        ELITE_OVERALL_CSV,
        [
            "ticker",
            "elite_strategies_count",
            "total_trades",
            "sum_net_trade_return_pct",
            "average_of_average_trade_return_pct",
            "average_success_rate_pct",
            "best_profit_factor",
            "best_strategy_label",
        ],
        overall_rows,
    )

    qualified_by_code: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in qualified_rows_sorted:
        qualified_by_code[row["strategy_code"]].append(row)

    lines: list[str] = []
    lines.append("# Rodada 1 - Elite Por Acao")
    lines.append("")
    lines.append("Base: `2025-04-10` ate `2026-04-10`.")
    lines.append("")
    lines.append("## O que este relatorio mostra")
    lines.append("- As `16` estrategias do nivel `D_elite` da Rodada 1.")
    lines.append("- Todas as acoes encontradas nessas estrategias.")
    lines.append("- Um corte pratico de `acoes qualificadas` para leitura operacional.")
    lines.append("")
    lines.append("## Filtro das Acoes Qualificadas")
    lines.append("- Minimo de `15` trades por acao dentro da estrategia.")
    lines.append("- `Acerto >= 55%`.")
    lines.append("- `Retorno medio >= 1,00%` por trade.")
    lines.append("- `Profit Factor >= 1,50`.")
    lines.append("- `Lucro liquido > 0`.")
    lines.append("")
    lines.append("## Volume")
    lines.append(f"- Estrategias elite: `{len(elite_rows)}`")
    lines.append(f"- Linhas estrategia x acao: `{len(elite_ticker_rows_sorted)}`")
    lines.append(f"- Linhas qualificadas: `{len(qualified_rows_sorted)}`")
    lines.append(f"- Acoes com pelo menos uma estrategia elite qualificada: `{len(overall_rows)}`")
    lines.append("")
    lines.append("## Acoes Que Mais Se Repetem Entre As Elite")
    if overall_rows:
        for idx, row in enumerate(overall_rows[:15], 1):
            lines.append(
                f"{idx}. `{row['ticker']}` em `{row['elite_strategies_count']}` estrategias | trades `{row['total_trades']}` | soma net `{row['sum_net_trade_return_pct']}%` | media `{row['average_of_average_trade_return_pct']}%`"
            )
    else:
        lines.append("- Nenhuma acao passou no corte qualificado.")
    lines.append("")

    for elite in elite_rows:
        code = elite["code"]
        rows = qualified_by_code.get(code, [])
        lines.append(f"## {elite['label']}")
        lines.append(
            f"- Trades da estrategia: `{elite['total_occurrences']}` | Acoes: `{elite['tickers_with_matches']}` | Acerto: `{float(elite['success_rate_pct']):.2f}%` | Media: `{float(elite['average_trade_return_pct']):.2f}%` | Net: `{float(elite['net_trade_return_pct']):.2f}%` | PF: `{float(elite['profit_factor']):.2f}`"
        )
        lines.append(f"- Regra de estado: `{elite['state_signature']}`")
        lines.append("")
        if not rows:
            lines.append("- Nenhuma acao passou no corte qualificado desta estrategia.")
            lines.append("")
            continue
        lines.append("| Acao | Trades | Acerto | Media | Net | PF | Periodo |")
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: | --- |")
        for row in rows[:15]:
            lines.append(
                f"| {row['ticker']} | {row['total_trades']} | {float(row['success_rate_pct']):.2f}% | {float(row['average_trade_return_pct']):.2f}% | {float(row['net_trade_return_pct']):.2f}% | {float(row['profit_factor']):.2f} | {row['first_trade_date']} a {row['last_trade_date']} |"
            )
        lines.append("")

    ELITE_MD.write_text("\n".join(lines), encoding="utf-8")

    print(ELITE_ALL_CSV)
    print(ELITE_QUALIFIED_CSV)
    print(ELITE_OVERALL_CSV)
    print(ELITE_MD)
    print(f"elite_strategies={len(elite_rows)}")
    print(f"elite_rows={len(elite_ticker_rows_sorted)}")
    print(f"qualified_rows={len(qualified_rows_sorted)}")


if __name__ == "__main__":
    main()
