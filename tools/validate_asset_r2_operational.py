from __future__ import annotations

import csv
import math
import sys
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from b3_patterns.analysis import summarize_trades_by_ticker
from b3_patterns.asset_discovery_round1 import (  # noqa: E402
    AssetDiscoveryPatternSummary,
    build_asset_discovery_round1_templates,
    collect_asset_discovery_pattern_trades,
)
from b3_patterns.reporting import export_strategy_ticker_csv, export_strategy_trades_csv  # noqa: E402

REPORTS = ROOT / "reports"

STRATEGIES_CSV = REPORTS / "asset-discovery-lista-r2-operational-strategies.csv"
VALIDATION_STRATEGIES_CSV = REPORTS / "asset-discovery-lista-r2-operational-validation-strategies.csv"
VALIDATION_TICKERS_CSV = REPORTS / "asset-discovery-lista-r2-operational-validation-tickers.csv"
VALIDATION_TRADES_CSV = REPORTS / "asset-discovery-lista-r2-operational-validation-trades.csv"
VALIDATION_REPORT_MD = REPORTS / "asset-discovery-lista-r2-operational-validation.md"

DB_PATH = ROOT / "b3_history.db"
TICKERS_FILE = ROOT / "lista.md"
START_DATE = "2026-01-01"
END_DATE = "2026-04-10"


def _float(value: object) -> float:
    if value in ("", "None", None):
        return 0.0
    if str(value).upper() == "INF":
        return math.inf
    return float(value)


def _int(value: object) -> int:
    return int(float(value)) if value not in ("", None) else 0


def _pf(gross_profit: float, gross_loss: float) -> float:
    if gross_loss == 0:
        return math.inf if gross_profit > 0 else 0.0
    return gross_profit / gross_loss


def _format(value: float, digits: int = 4) -> str:
    if math.isinf(value):
        return "INF"
    return f"{value:.{digits}f}"


def _summary_from_row(row: dict[str, str]) -> AssetDiscoveryPatternSummary:
    return AssetDiscoveryPatternSummary(
        code=row["code"],
        label=row["label"],
        family=row["family"],
        template_code=row["template_code"],
        template_label=row["template_label"],
        trade_direction=row["trade_direction"],
        entry_rule=row["entry_rule"],
        take_profit_pct=_float(row["take_profit_pct"]),
        stop_loss_pct=_float(row["stop_loss_pct"]),
        time_cap_days=_int(row["time_cap_days"]),
        state_size=_int(row["state_size"]),
        state_signature=row["state_signature"],
        feature_keys=row["feature_keys"],
        min_trade_return_pct=0.0,
        tickers_with_matches=_int(row["tickers_with_matches"]),
        total_occurrences=_int(row["total_occurrences"]),
        successful_occurrences=0,
        success_rate_pct=_float(row["success_rate_pct"]),
        profitable_trades=0,
        profitable_trade_rate_pct=_float(row["profitable_trade_rate_pct"]),
        average_asset_move_pct=0.0,
        average_trade_return_pct=_float(row["average_trade_return_pct"]),
        net_trade_return_pct=_float(row["net_trade_return_pct"]),
        cumulative_return_pct=0.0,
        profit_factor=_float(row["profit_factor"]),
        first_trade_date=row["first_trade_date"],
        last_trade_date=row["last_trade_date"],
    )


def main() -> None:
    strategies: list[AssetDiscoveryPatternSummary] = []
    full_year_lookup: dict[str, dict[str, str]] = {}
    with STRATEGIES_CSV.open(encoding="utf-8", newline="") as file_obj:
        for row in csv.DictReader(file_obj):
            full_year_lookup[row["code"]] = row
            strategies.append(_summary_from_row(row))

    templates = build_asset_discovery_round1_templates(
        entry_rules=["open", "close"],
        trade_directions=["long"],
        target_stop_pairs=[(8.0, 4.0)],
        time_cap_days=5,
    )
    trades = collect_asset_discovery_pattern_trades(
        db_path=DB_PATH,
        tickers_file=TICKERS_FILE,
        start_date=START_DATE,
        end_date=END_DATE,
        approved_summaries=strategies,
        template_definitions=templates,
        max_pattern_size=3,
    )

    by_strategy: dict[str, list[object]] = defaultdict(list)
    for trade in trades:
        by_strategy[trade.strategy_code].append(trade)

    rows: list[dict[str, object]] = []
    for strategy in strategies:
        strategy_trades = by_strategy.get(strategy.code, [])
        gross_profit = sum(item.trade_return_pct for item in strategy_trades if item.trade_return_pct > 0)
        gross_loss = -sum(item.trade_return_pct for item in strategy_trades if item.trade_return_pct < 0)
        total = len(strategy_trades)
        profitable = sum(1 for item in strategy_trades if item.trade_return_pct > 0)
        avg = sum((item.trade_return_pct for item in strategy_trades), 0.0) / total if total else 0.0
        net = sum((item.trade_return_pct for item in strategy_trades), 0.0)
        tickers = len({item.ticker for item in strategy_trades})
        full = full_year_lookup[strategy.code]
        rows.append(
            {
                "code": strategy.code,
                "label": strategy.label,
                "validation_trades": total,
                "validation_tickers": tickers,
                "validation_profitable_trades": profitable,
                "validation_profitable_trade_rate_pct": _format((profitable / total) * 100.0 if total else 0.0),
                "validation_average_trade_return_pct": _format(avg),
                "validation_net_trade_return_pct": _format(net),
                "validation_profit_factor": _format(_pf(gross_profit, gross_loss)),
                "full_year_trades": full["total_occurrences"],
                "full_year_profitable_trade_rate_pct": full["profitable_trade_rate_pct"],
                "full_year_average_trade_return_pct": full["average_trade_return_pct"],
                "full_year_profit_factor": full["profit_factor"],
            }
        )
    rows.sort(
        key=lambda row: (
            -_float(row["validation_average_trade_return_pct"]),
            -_float(row["validation_profitable_trade_rate_pct"]),
            -_int(row["validation_trades"]),
        )
    )

    with VALIDATION_STRATEGIES_CSV.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(
            file_obj,
            fieldnames=[
                "code",
                "label",
                "validation_trades",
                "validation_tickers",
                "validation_profitable_trades",
                "validation_profitable_trade_rate_pct",
                "validation_average_trade_return_pct",
                "validation_net_trade_return_pct",
                "validation_profit_factor",
                "full_year_trades",
                "full_year_profitable_trade_rate_pct",
                "full_year_average_trade_return_pct",
                "full_year_profit_factor",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    ticker_summaries = summarize_trades_by_ticker(trades)
    export_strategy_ticker_csv(ticker_summaries, VALIDATION_TICKERS_CSV)
    export_strategy_trades_csv(trades, VALIDATION_TRADES_CSV)

    total_trades = len(trades)
    total_profitable = sum(1 for trade in trades if trade.trade_return_pct > 0)
    total_avg = sum((trade.trade_return_pct for trade in trades), 0.0) / total_trades if total_trades else 0.0
    total_net = sum((trade.trade_return_pct for trade in trades), 0.0)
    gross_profit = sum(trade.trade_return_pct for trade in trades if trade.trade_return_pct > 0)
    gross_loss = -sum(trade.trade_return_pct for trade in trades if trade.trade_return_pct < 0)

    lines = [
        "# Rodada 2 - Validacao Temporal do Filtro Operacional",
        "",
        f"Periodo validado: `{START_DATE}` ate `{END_DATE}`.",
        "",
        "Esta validacao mede o comportamento recente das estrategias operacionais finais. Ela nao substitui uma validacao fora da amostra perfeita, porque as estrategias foram escolhidas olhando a janela anual completa.",
        "",
        "## Consolidado",
        f"- Estrategias validadas: `{len(strategies)}`",
        f"- Trades no periodo: `{total_trades}`",
        f"- Acoes com trades: `{len({trade.ticker for trade in trades})}`",
        f"- Acerto verde: `{((total_profitable / total_trades) * 100.0 if total_trades else 0.0):.2f}%`",
        f"- Media por trade: `{total_avg:.2f}%`",
        f"- Net: `{total_net:.2f}%`",
        f"- PF: `{_format(_pf(gross_profit, gross_loss), 2)}`",
        "",
        "## Por estrategia",
        "",
        "| Estrategia | Trades validacao | Acoes | Acerto validacao | Media validacao | PF validacao | Acerto ano | Media ano | PF ano |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        lines.append(
            f"| {row['label']} | {row['validation_trades']} | {row['validation_tickers']} | "
            f"{row['validation_profitable_trade_rate_pct']}% | {row['validation_average_trade_return_pct']}% | "
            f"{row['validation_profit_factor']} | {row['full_year_profitable_trade_rate_pct']}% | "
            f"{row['full_year_average_trade_return_pct']}% | {row['full_year_profit_factor']} |"
        )
    lines.extend(
        [
            "",
            "## Arquivos",
            f"- Estrategias: `{VALIDATION_STRATEGIES_CSV.relative_to(ROOT)}`",
            f"- Tickers: `{VALIDATION_TICKERS_CSV.relative_to(ROOT)}`",
            f"- Trades: `{VALIDATION_TRADES_CSV.relative_to(ROOT)}`",
        ]
    )
    VALIDATION_REPORT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(VALIDATION_STRATEGIES_CSV)
    print(VALIDATION_TICKERS_CSV)
    print(VALIDATION_TRADES_CSV)
    print(VALIDATION_REPORT_MD)
    print(f"trades={total_trades}")
    print(f"win={((total_profitable / total_trades) * 100.0 if total_trades else 0.0):.4f}")
    print(f"avg={total_avg:.4f}")
    print(f"pf={_format(_pf(gross_profit, gross_loss))}")


if __name__ == "__main__":
    main()
