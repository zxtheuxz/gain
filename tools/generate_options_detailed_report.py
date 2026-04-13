from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from math import isinf
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from b3_patterns.analysis import split_strategy_results, summarize_trades_by_ticker
from b3_patterns.db import connect, initialize_database
from b3_patterns.options import backtest_option_strategies, build_option_strategy_definitions
from b3_patterns.reporting import export_strategy_ticker_csv, export_strategy_trades_csv


def _format_pct(value: float) -> str:
    return f"{value:.4f}"


def _format_metric(value: float | None) -> str:
    if value is None:
        return "n/a"
    if isinf(value):
        return "INF"
    return f"{value:.4f}"


def _resolve_dates(start_date: str | None, end_date: str | None) -> tuple[str, str]:
    end_day = date.fromisoformat(end_date) if end_date else date.today()
    start_day = date.fromisoformat(start_date) if start_date else end_day - timedelta(days=365)
    return start_day.isoformat(), end_day.isoformat()


def _load_database_stats(db_path: str | Path) -> tuple[int, int, int]:
    with connect(db_path) as connection:
        initialize_database(connection)
        spot_tickers = int(
            connection.execute(
                "SELECT COUNT(DISTINCT ticker) FROM spot_quote_history"
            ).fetchone()[0]
        )
        option_roots = int(
            connection.execute(
                "SELECT COUNT(DISTINCT underlying_root) FROM option_quote_history"
            ).fetchone()[0]
        )
        option_symbols = int(
            connection.execute(
                "SELECT COUNT(DISTINCT option_symbol) FROM option_quote_history"
            ).fetchone()[0]
        )
    return spot_tickers, option_roots, option_symbols


def _build_strategy_table_rows(
    summaries,
    status_map: dict[str, str],
    rejection_reasons: dict[str, list[str]],
) -> list[str]:
    lines = [
        "| Status | Codigo | Estrategia | Trades | Acoes | Acerto % | Win % | Media % | Mediana % | Net % | PF | Motivos |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for summary in summaries:
        reasons = "; ".join(rejection_reasons.get(summary.code, [])) or "-"
        lines.append(
            "| "
            + " | ".join(
                [
                    status_map.get(summary.code, "unknown"),
                    summary.code,
                    summary.label,
                    str(summary.total_occurrences),
                    str(summary.tickers_with_matches),
                    f"{summary.success_rate_pct:.2f}",
                    f"{summary.profitable_trade_rate_pct:.2f}",
                    _format_pct(summary.average_trade_return_pct),
                    _format_pct(summary.median_trade_return_pct),
                    _format_pct(summary.net_trade_return_pct),
                    _format_metric(summary.profit_factor),
                    reasons,
                ]
            )
            + " |"
        )
    return lines


def _build_ticker_table_rows(ticker_summaries) -> list[str]:
    lines = [
        "| Ticker | Trades | Acerto % | Win % | Media % | Mediana % | Net % | Comp % | PF | Primeiro | Ultimo |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    for item in ticker_summaries:
        lines.append(
            "| "
            + " | ".join(
                [
                    item.ticker,
                    str(item.total_trades),
                    f"{item.success_rate_pct:.2f}",
                    f"{item.profitable_trade_rate_pct:.2f}",
                    _format_pct(item.average_trade_return_pct),
                    _format_pct(item.median_trade_return_pct),
                    _format_pct(item.net_trade_return_pct),
                    _format_pct(item.cumulative_return_pct),
                    _format_metric(item.profit_factor),
                    item.first_trade_date,
                    item.last_trade_date,
                ]
            )
            + " |"
        )
    return lines


def _render_trade_line(label: str, trade) -> str:
    return (
        f"- {label}: {trade.ticker} | opcao {trade.instrument_symbol or '-'} | "
        f"entrada {trade.trigger_date} | saida {trade.exit_date} | "
        f"retorno {trade.trade_return_pct:.4f}% | trigger {trade.trigger_change_pct:.4f}% | "
        f"entry {trade.entry_price:.4f} | exit {trade.exit_price:.4f}"
    )


def generate_report(
    db_path: str | Path,
    tickers_file: str | Path,
    output_md: str | Path,
    output_trades_csv: str | Path,
    output_tickers_csv: str | Path,
    start_date: str | None = None,
    end_date: str | None = None,
    min_trade_return_pct: float = 0.0,
    round_trip_cost_pct: float = 0.0,
    min_success_rate: float = 55.0,
    min_profit_factor: float = 1.10,
    min_average_trade_return_pct: float = 0.0,
    min_trades: int = 10,
) -> Path:
    start_date, end_date = _resolve_dates(start_date, end_date)
    strategy_definitions = build_option_strategy_definitions(
        min_trade_return_pct=min_trade_return_pct,
        round_trip_cost_pct=round_trip_cost_pct,
    )
    summaries, trades = backtest_option_strategies(
        db_path=db_path,
        tickers_file=tickers_file,
        start_date=start_date,
        end_date=end_date,
        strategy_definitions=strategy_definitions,
    )
    approved, _, rejected, rejection_reasons = split_strategy_results(
        summaries=summaries,
        trades=trades,
        min_success_rate_pct=min_success_rate,
        min_profit_factor=min_profit_factor,
        min_average_trade_return_pct=min_average_trade_return_pct,
        min_trades=min_trades,
        require_positive_net=True,
    )

    export_strategy_trades_csv(trades, output_trades_csv)
    ticker_summaries = summarize_trades_by_ticker(trades)
    export_strategy_ticker_csv(ticker_summaries, output_tickers_csv)

    summary_by_code = {item.code: item for item in summaries}
    status_map = {item.code: "approved" for item in approved}
    status_map.update({item.code: "rejected" for item in rejected})

    tickers_by_strategy = defaultdict(list)
    for item in ticker_summaries:
        tickers_by_strategy[item.strategy_code].append(item)
    for items in tickers_by_strategy.values():
        items.sort(
            key=lambda item: (
                item.net_trade_return_pct,
                item.average_trade_return_pct,
                item.total_trades,
            ),
            reverse=True,
        )

    trades_by_strategy = defaultdict(list)
    for trade in trades:
        trades_by_strategy[trade.strategy_code].append(trade)

    profitable_trades = sum(1 for item in trades if item.trade_return_pct > 0)
    losing_trades = sum(1 for item in trades if item.trade_return_pct < 0)
    flat_trades = len(trades) - profitable_trades - losing_trades
    spot_tickers, option_roots, option_symbols = _load_database_stats(db_path)

    lines = [
        "# Relatorio Detalhado de Backtest de Opcoes ATM",
        "",
        f"- Gerado em: {datetime.now().astimezone().isoformat(timespec='seconds')}",
        f"- Banco: `{Path(db_path)}`",
        f"- Universo de acoes: `{Path(tickers_file)}`",
        f"- Janela do backtest: `{start_date}` ate `{end_date}`",
        f"- Tickers spot na base: `{spot_tickers}`",
        f"- Raizes de opcoes na base: `{option_roots}`",
        f"- Contratos de opcoes distintos na base: `{option_symbols}`",
        f"- Estrategias testadas: `{len(summaries)}`",
        f"- Estrategias aprovadas: `{len(approved)}`",
        f"- Estrategias reprovadas: `{len(rejected)}`",
        f"- Trades totais: `{len(trades)}`",
        f"- Trades com ganho: `{profitable_trades}`",
        f"- Trades com perda: `{losing_trades}`",
        f"- Trades zerados: `{flat_trades}`",
        "",
        "## Criterios de aprovacao",
        "",
        f"- Taxa de acerto minima: `{min_success_rate:.2f}%`",
        f"- Profit factor minimo: `{min_profit_factor:.2f}`",
        f"- Retorno medio minimo por trade: `{min_average_trade_return_pct:.2f}%`",
        f"- Numero minimo de trades: `{min_trades}`",
        f"- Lucro liquido obrigatoriamente positivo: `sim`",
        f"- Custo operacional aplicado: `{round_trip_cost_pct:.4f}%`",
        "",
        "## Observacoes de leitura",
        "",
        "- `Acerto %` usa o criterio de sucesso configurado para a estrategia.",
        "- `Win %` conta apenas trades com retorno estritamente positivo.",
        "- `Net %` e a soma simples dos retornos percentuais trade a trade.",
        "- `Comp %` e um composto sequencial teorico. Em opcoes ele pode colapsar para `-100%` quando ha perda total de premio em algum ponto, entao use mais `Net %`, `Media %` e `PF` para comparar setups.",
        "",
        "## Todas as estrategias",
        "",
    ]
    lines.extend(_build_strategy_table_rows(summaries, status_map, rejection_reasons))

    approved_sorted = sorted(
        approved,
        key=lambda item: (item.net_trade_return_pct, item.average_trade_return_pct),
        reverse=True,
    )
    rejected_sorted = sorted(
        rejected,
        key=lambda item: (item.net_trade_return_pct, item.average_trade_return_pct),
        reverse=True,
    )

    lines.extend(
        [
            "",
            "## Aprovadas",
            "",
        ]
    )
    lines.extend(_build_strategy_table_rows(approved_sorted, status_map, rejection_reasons))

    lines.extend(
        [
            "",
            "## Reprovadas",
            "",
        ]
    )
    lines.extend(_build_strategy_table_rows(rejected_sorted, status_map, rejection_reasons))

    lines.extend(
        [
            "",
            "## Detalhamento por estrategia",
            "",
        ]
    )

    for summary in summaries:
        strategy_trades = trades_by_strategy.get(summary.code, [])
        strategy_tickers = tickers_by_strategy.get(summary.code, [])
        best_trade = max(strategy_trades, key=lambda item: item.trade_return_pct)
        worst_trade = min(strategy_trades, key=lambda item: item.trade_return_pct)
        top_ticker = strategy_tickers[0] if strategy_tickers else None
        bottom_ticker = strategy_tickers[-1] if strategy_tickers else None
        reasons = rejection_reasons.get(summary.code, [])

        lines.extend(
            [
                f"### {summary.label}",
                "",
                f"- Codigo: `{summary.code}`",
                f"- Status: `{status_map.get(summary.code, 'unknown')}`",
                (
                    "- Motivos da reprovacao: `"
                    + "; ".join(reasons)
                    + "`"
                    if reasons
                    else "- Motivos da reprovacao: `-`"
                ),
                f"- Trades: `{summary.total_occurrences}`",
                f"- Acoes com ocorrencias: `{summary.tickers_with_matches}`",
                f"- Acerto %: `{summary.success_rate_pct:.2f}`",
                f"- Win %: `{summary.profitable_trade_rate_pct:.2f}`",
                f"- Media por trade %: `{summary.average_trade_return_pct:.4f}`",
                f"- Mediana por trade %: `{summary.median_trade_return_pct:.4f}`",
                f"- Net %: `{summary.net_trade_return_pct:.4f}`",
                f"- Comp %: `{summary.cumulative_return_pct:.4f}`",
                f"- Profit factor: `{_format_metric(summary.profit_factor)}`",
            ]
        )

        if top_ticker is not None:
            lines.append(
                f"- Melhor acao agregada: `{top_ticker.ticker}` | net `{top_ticker.net_trade_return_pct:.4f}%` | media `{top_ticker.average_trade_return_pct:.4f}%` | trades `{top_ticker.total_trades}`"
            )
        if bottom_ticker is not None:
            lines.append(
                f"- Pior acao agregada: `{bottom_ticker.ticker}` | net `{bottom_ticker.net_trade_return_pct:.4f}%` | media `{bottom_ticker.average_trade_return_pct:.4f}%` | trades `{bottom_ticker.total_trades}`"
            )

        lines.extend(
            [
                _render_trade_line("Melhor trade individual", best_trade),
                _render_trade_line("Pior trade individual", worst_trade),
                "",
                "#### Acoes desta estrategia",
                "",
            ]
        )
        lines.extend(_build_ticker_table_rows(strategy_tickers))
        lines.append("")

    output_path = Path(output_md)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")
    return output_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gera relatorio Markdown detalhado do backtest de opcoes ATM.",
    )
    parser.add_argument("--db-path", default="b3_history.db")
    parser.add_argument("--tickers-file", default="acoes-listadas-b3.csv")
    parser.add_argument(
        "--output-md",
        default="reports/options-relatorio-detalhado.md",
    )
    parser.add_argument(
        "--output-trades-csv",
        default="reports/options-trades-all.csv",
    )
    parser.add_argument(
        "--output-tickers-csv",
        default="reports/options-tickers-detailed-all.csv",
    )
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--min-trade-return-pct", type=float, default=0.0)
    parser.add_argument("--round-trip-cost-pct", type=float, default=0.0)
    parser.add_argument("--min-success-rate", type=float, default=55.0)
    parser.add_argument("--min-profit-factor", type=float, default=1.10)
    parser.add_argument("--min-average-trade-return-pct", type=float, default=0.0)
    parser.add_argument("--min-trades", type=int, default=10)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    output_path = generate_report(
        db_path=args.db_path,
        tickers_file=args.tickers_file,
        output_md=args.output_md,
        output_trades_csv=args.output_trades_csv,
        output_tickers_csv=args.output_tickers_csv,
        start_date=args.start_date,
        end_date=args.end_date,
        min_trade_return_pct=args.min_trade_return_pct,
        round_trip_cost_pct=args.round_trip_cost_pct,
        min_success_rate=args.min_success_rate,
        min_profit_factor=args.min_profit_factor,
        min_average_trade_return_pct=args.min_average_trade_return_pct,
        min_trades=args.min_trades,
    )
    print(f"Relatorio detalhado exportado para {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
