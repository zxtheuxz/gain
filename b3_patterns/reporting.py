from __future__ import annotations

import csv
from math import isinf
from pathlib import Path

from .models import (
    DiscoveryPatternSummary,
    DiscoveryRefinedSummary,
    PatternResult,
    StrategyRegistryEntry,
    StrategySummary,
    StrategyTickerSummary,
    StrategyTrade,
)


def _format_metric(value: float | None) -> str:
    if value is None:
        return "n/a"
    if isinf(value):
        return "INF"
    return f"{value:.2f}"


def render_console_report(results: list[PatternResult], top: int = 20) -> str:
    if not results:
        return "Nenhum padrao encontrado para os parametros informados."

    headers = (
        ("Ticker", 12),
        ("Ocorrencias", 12),
        ("Acertos", 10),
        ("Assertiv.(%)", 14),
        ("Media T+1(%)", 14),
        ("Mediana T+1(%)", 16),
    )
    header_line = " ".join(label.ljust(width) for label, width in headers)
    separator = "-" * len(header_line)
    lines = [header_line, separator]

    for item in results[:top]:
        lines.append(
            " ".join(
                [
                    item.ticker.ljust(12),
                    str(item.occurrences).rjust(12),
                    str(item.successful_occurrences).rjust(10),
                    f"{item.success_rate_pct:>14.2f}",
                    f"{item.average_next_day_return_pct:>14.2f}",
                    f"{item.median_next_day_return_pct:>16.2f}",
                ]
            )
        )

    return "\n".join(lines)


def export_csv(results: list[PatternResult], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(
            file_obj,
            fieldnames=[
                "ticker",
                "occurrences",
                "successful_occurrences",
                "success_rate_pct",
                "average_next_day_return_pct",
                "median_next_day_return_pct",
            ],
        )
        writer.writeheader()
        for item in results:
            writer.writerow(
                {
                    "ticker": item.ticker,
                    "occurrences": item.occurrences,
                    "successful_occurrences": item.successful_occurrences,
                    "success_rate_pct": f"{item.success_rate_pct:.4f}",
                    "average_next_day_return_pct": f"{item.average_next_day_return_pct:.4f}",
                    "median_next_day_return_pct": f"{item.median_next_day_return_pct:.4f}",
                }
            )

    return path


def render_strategy_report(results: list[StrategySummary], top: int = 20) -> str:
    if not results:
        return "Nenhuma estrategia encontrou ocorrencias com os parametros informados."

    headers = (
        ("Estrategia", 58),
        ("Trades", 8),
        ("Suc.(%)", 9),
        ("Win(%)", 9),
        ("Med BT(%)", 11),
        ("Net BT(%)", 11),
        ("Comp BT(%)", 12),
        ("PF", 8),
    )
    header_line = " ".join(label.ljust(width) for label, width in headers)
    separator = "-" * len(header_line)
    lines = [header_line, separator]

    for item in results[:top]:
        lines.append(
            " ".join(
                [
                    item.label.ljust(58),
                    str(item.total_occurrences).rjust(8),
                    f"{item.success_rate_pct:>9.2f}",
                    f"{item.profitable_trade_rate_pct:>9.2f}",
                    f"{item.average_trade_return_pct:>11.2f}",
                    f"{item.net_trade_return_pct:>11.2f}",
                    f"{item.cumulative_return_pct:>12.2f}",
                    _format_metric(item.profit_factor).rjust(8),
                ]
            )
        )

    return "\n".join(lines)


def render_discovery_report(results: list[DiscoveryPatternSummary], top: int = 20) -> str:
    if not results:
        return "Nenhum padrao probabilistico encontrou ocorrencias com os parametros informados."

    headers = (
        ("Padrao", 78),
        ("Trades", 8),
        ("Tickers", 8),
        ("Win(%)", 9),
        ("Med BT(%)", 11),
        ("Net BT(%)", 11),
        ("PF", 8),
    )
    header_line = " ".join(label.ljust(width) for label, width in headers)
    separator = "-" * len(header_line)
    lines = [header_line, separator]

    for item in results[:top]:
        lines.append(
            " ".join(
                [
                    item.label.ljust(78),
                    str(item.total_occurrences).rjust(8),
                    str(item.tickers_with_matches).rjust(8),
                    f"{item.profitable_trade_rate_pct:>9.2f}",
                    f"{item.average_trade_return_pct:>11.2f}",
                    f"{item.net_trade_return_pct:>11.2f}",
                    _format_metric(item.profit_factor).rjust(8),
                ]
            )
        )

    return "\n".join(lines)


def export_strategy_csv(results: list[StrategySummary], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(
            file_obj,
            fieldnames=[
                "code",
                "label",
                "family",
                "setup_kind",
                "trigger_direction",
                "threshold_pct",
                "trade_direction",
                "entry_rule",
                "exit_offset_days",
                "min_trade_return_pct",
                "tickers_with_matches",
                "total_occurrences",
                "successful_occurrences",
                "success_rate_pct",
                "average_asset_move_pct",
                "median_asset_move_pct",
                "profitable_trades",
                "profitable_trade_rate_pct",
                "average_trade_return_pct",
                "median_trade_return_pct",
                "net_trade_return_pct",
                "cumulative_return_pct",
                "profit_factor",
            ],
        )
        writer.writeheader()
        for item in results:
            writer.writerow(
                {
                    "code": item.code,
                    "label": item.label,
                    "family": item.family,
                    "setup_kind": item.setup_kind,
                    "trigger_direction": item.trigger_direction,
                    "threshold_pct": f"{item.threshold_pct:.4f}",
                    "trade_direction": item.trade_direction,
                    "entry_rule": item.entry_rule,
                    "exit_offset_days": item.exit_offset_days,
                    "min_trade_return_pct": f"{item.min_trade_return_pct:.4f}",
                    "tickers_with_matches": item.tickers_with_matches,
                    "total_occurrences": item.total_occurrences,
                    "successful_occurrences": item.successful_occurrences,
                    "success_rate_pct": f"{item.success_rate_pct:.4f}",
                    "average_asset_move_pct": f"{item.average_asset_move_pct:.4f}",
                    "median_asset_move_pct": f"{item.median_asset_move_pct:.4f}",
                    "profitable_trades": item.profitable_trades,
                    "profitable_trade_rate_pct": f"{item.profitable_trade_rate_pct:.4f}",
                    "average_trade_return_pct": f"{item.average_trade_return_pct:.4f}",
                    "median_trade_return_pct": f"{item.median_trade_return_pct:.4f}",
                    "net_trade_return_pct": f"{item.net_trade_return_pct:.4f}",
                    "cumulative_return_pct": f"{item.cumulative_return_pct:.4f}",
                    "profit_factor": (
                        "INF" if item.profit_factor is not None and isinf(item.profit_factor)
                        else "" if item.profit_factor is None
                        else f"{item.profit_factor:.4f}"
                    ),
                }
            )

    return path


def export_discovery_csv(
    results: list[DiscoveryPatternSummary],
    output_path: str | Path,
) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(
            file_obj,
            fieldnames=[
                "code",
                "label",
                "family",
                "template_code",
                "template_label",
                "option_side",
                "dte_target_days",
                "trade_direction",
                "entry_rule",
                "exit_offset_days",
                "state_size",
                "state_signature",
                "feature_keys",
                "min_trade_return_pct",
                "tickers_with_matches",
                "total_occurrences",
                "successful_occurrences",
                "success_rate_pct",
                "profitable_trades",
                "profitable_trade_rate_pct",
                "average_asset_move_pct",
                "average_trade_return_pct",
                "net_trade_return_pct",
                "cumulative_return_pct",
                "profit_factor",
                "first_trade_date",
                "last_trade_date",
            ],
        )
        writer.writeheader()
        for item in results:
            writer.writerow(
                {
                    "code": item.code,
                    "label": item.label,
                    "family": item.family,
                    "template_code": item.template_code,
                    "template_label": item.template_label,
                    "option_side": item.option_side,
                    "dte_target_days": item.dte_target_days,
                    "trade_direction": item.trade_direction,
                    "entry_rule": item.entry_rule,
                    "exit_offset_days": item.exit_offset_days,
                    "state_size": item.state_size,
                    "state_signature": item.state_signature,
                    "feature_keys": item.feature_keys,
                    "min_trade_return_pct": f"{item.min_trade_return_pct:.4f}",
                    "tickers_with_matches": item.tickers_with_matches,
                    "total_occurrences": item.total_occurrences,
                    "successful_occurrences": item.successful_occurrences,
                    "success_rate_pct": f"{item.success_rate_pct:.4f}",
                    "profitable_trades": item.profitable_trades,
                    "profitable_trade_rate_pct": f"{item.profitable_trade_rate_pct:.4f}",
                    "average_asset_move_pct": f"{item.average_asset_move_pct:.4f}",
                    "average_trade_return_pct": f"{item.average_trade_return_pct:.4f}",
                    "net_trade_return_pct": f"{item.net_trade_return_pct:.4f}",
                    "cumulative_return_pct": f"{item.cumulative_return_pct:.4f}",
                    "profit_factor": (
                        "INF" if item.profit_factor is not None and isinf(item.profit_factor)
                        else "" if item.profit_factor is None
                        else f"{item.profit_factor:.4f}"
                    ),
                    "first_trade_date": item.first_trade_date,
                    "last_trade_date": item.last_trade_date,
                }
            )

    return path


def export_strategy_trades_csv(results: list[StrategyTrade], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(
            file_obj,
            fieldnames=[
                "strategy_code",
                "strategy_label",
                "family",
                "ticker",
                "trigger_date",
                "exit_date",
                "direction",
                "trigger_change_pct",
                "entry_price",
                "exit_price",
                "asset_move_pct",
                "trade_return_pct",
                "is_profitable",
                "is_successful",
                "instrument_symbol",
                "contract_expiration",
                "dte_target_days",
                "exit_reason",
            ],
        )
        writer.writeheader()
        for item in results:
            writer.writerow(
                {
                    "strategy_code": item.strategy_code,
                    "strategy_label": item.strategy_label,
                    "family": item.family,
                    "ticker": item.ticker,
                    "trigger_date": item.trigger_date,
                    "exit_date": item.exit_date,
                    "direction": item.direction,
                    "trigger_change_pct": f"{item.trigger_change_pct:.4f}",
                    "entry_price": f"{item.entry_price:.4f}",
                    "exit_price": f"{item.exit_price:.4f}",
                    "asset_move_pct": f"{item.asset_move_pct:.4f}",
                    "trade_return_pct": f"{item.trade_return_pct:.4f}",
                    "is_profitable": item.is_profitable,
                    "is_successful": item.is_successful,
                    "instrument_symbol": item.instrument_symbol or "",
                    "contract_expiration": item.contract_expiration or "",
                    "dte_target_days": "" if item.dte_target_days is None else item.dte_target_days,
                    "exit_reason": item.exit_reason or "",
                }
            )

    return path


def export_strategy_ticker_csv(
    results: list[StrategyTickerSummary],
    output_path: str | Path,
) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(
            file_obj,
            fieldnames=[
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
            ],
        )
        writer.writeheader()
        for item in results:
            writer.writerow(
                {
                    "strategy_code": item.strategy_code,
                    "strategy_label": item.strategy_label,
                    "family": item.family,
                    "ticker": item.ticker,
                    "total_trades": item.total_trades,
                    "successful_trades": item.successful_trades,
                    "success_rate_pct": f"{item.success_rate_pct:.4f}",
                    "profitable_trades": item.profitable_trades,
                    "profitable_trade_rate_pct": f"{item.profitable_trade_rate_pct:.4f}",
                    "average_trade_return_pct": f"{item.average_trade_return_pct:.4f}",
                    "median_trade_return_pct": f"{item.median_trade_return_pct:.4f}",
                    "net_trade_return_pct": f"{item.net_trade_return_pct:.4f}",
                    "cumulative_return_pct": f"{item.cumulative_return_pct:.4f}",
                    "profit_factor": (
                        "INF" if item.profit_factor is not None and isinf(item.profit_factor)
                        else "" if item.profit_factor is None
                        else f"{item.profit_factor:.4f}"
                    ),
                    "first_trade_date": item.first_trade_date,
                    "last_trade_date": item.last_trade_date,
                }
            )

    return path


def export_strategy_registry_csv(
    results: list[StrategyRegistryEntry],
    output_path: str | Path,
) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(
            file_obj,
            fieldnames=[
                "code",
                "label",
                "family",
                "setup_kind",
                "trigger_direction",
                "threshold_pct",
                "trade_direction",
                "entry_rule",
                "exit_offset_days",
                "min_trade_return_pct",
                "status",
                "rejection_reasons",
                "tested_at",
                "total_occurrences",
                "success_rate_pct",
                "profitable_trade_rate_pct",
                "average_trade_return_pct",
                "net_trade_return_pct",
                "profit_factor",
            ],
        )
        writer.writeheader()
        for item in results:
            writer.writerow(
                {
                    "code": item.code,
                    "label": item.label,
                    "family": item.family,
                    "setup_kind": item.setup_kind,
                    "trigger_direction": item.trigger_direction,
                    "threshold_pct": f"{item.threshold_pct:.4f}",
                    "trade_direction": item.trade_direction,
                    "entry_rule": item.entry_rule,
                    "exit_offset_days": item.exit_offset_days,
                    "min_trade_return_pct": f"{item.min_trade_return_pct:.4f}",
                    "status": item.status,
                    "rejection_reasons": item.rejection_reasons,
                    "tested_at": item.tested_at,
                    "total_occurrences": item.total_occurrences,
                    "success_rate_pct": f"{item.success_rate_pct:.4f}",
                    "profitable_trade_rate_pct": f"{item.profitable_trade_rate_pct:.4f}",
                    "average_trade_return_pct": f"{item.average_trade_return_pct:.4f}",
                    "net_trade_return_pct": f"{item.net_trade_return_pct:.4f}",
                    "profit_factor": (
                        "INF" if item.profit_factor is not None and isinf(item.profit_factor)
                        else "" if item.profit_factor is None
                        else f"{item.profit_factor:.4f}"
                    ),
                }
            )

    return path


def export_strategy_registry_markdown(
    results: list[StrategyRegistryEntry],
    output_path: str | Path,
) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    approved = [item for item in results if item.status == "approved"]
    rejected = [item for item in results if item.status == "rejected"]

    lines = [
        "# Memoria de Estrategias Testadas",
        "",
        f"- Aprovadas: {len(approved)}",
        f"- Reprovadas: {len(rejected)}",
        "",
        "## Aprovadas",
        "",
        "| Codigo | Estrategia | Trades | Acerto % | Media/trade % | Net BT % | PF |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]

    for item in approved:
        lines.append(
            f"| {item.code} | {item.label} | {item.total_occurrences} | "
            f"{item.success_rate_pct:.2f} | {item.average_trade_return_pct:.4f} | "
            f"{item.net_trade_return_pct:.4f} | {_format_metric(item.profit_factor)} |"
        )

    lines.extend(
        [
            "",
            "## Reprovadas",
            "",
            "| Codigo | Estrategia | Trades | Acerto % | Media/trade % | Net BT % | Motivos |",
            "| --- | --- | ---: | ---: | ---: | ---: | --- |",
        ]
    )

    for item in rejected:
        lines.append(
            f"| {item.code} | {item.label} | {item.total_occurrences} | "
            f"{item.success_rate_pct:.2f} | {item.average_trade_return_pct:.4f} | "
            f"{item.net_trade_return_pct:.4f} | {item.rejection_reasons or '-'} |"
        )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def export_discovery_markdown(
    approved_results: list[DiscoveryPatternSummary],
    rejected_results: list[DiscoveryPatternSummary],
    features,
    output_path: str | Path,
    *,
    start_date: str,
    end_date: str,
) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        "# Discovery Probabilistico de Opcoes",
        "",
        f"- Janela: {start_date} ate {end_date}",
        f"- Features avaliadas: {len(features)}",
        f"- Padroes aprovados: {len(approved_results)}",
        f"- Padroes reprovados: {len(rejected_results)}",
        "",
        "## Biblioteca de Features",
        "",
        "| Chave | Feature | Pair trading |",
        "| --- | --- | --- |",
    ]

    for feature in features:
        lines.append(
            f"| {feature.key} | {feature.label} | {'sim' if feature.pairable else 'nao'} |"
        )

    lines.extend(
        [
            "",
            "## Aprovados",
            "",
            "| Padrao | Trades | Tickers | Win % | Media/trade % | Net % | PF |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )

    for item in approved_results:
        lines.append(
            f"| {item.label} | {item.total_occurrences} | {item.tickers_with_matches} | "
            f"{item.profitable_trade_rate_pct:.2f} | {item.average_trade_return_pct:.4f} | "
            f"{item.net_trade_return_pct:.4f} | {_format_metric(item.profit_factor)} |"
        )

    lines.extend(
        [
            "",
            "## Reprovados",
            "",
            "| Padrao | Trades | Tickers | Win % | Media/trade % | Net % | PF |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )

    for item in rejected_results:
        lines.append(
            f"| {item.label} | {item.total_occurrences} | {item.tickers_with_matches} | "
            f"{item.profitable_trade_rate_pct:.2f} | {item.average_trade_return_pct:.4f} | "
            f"{item.net_trade_return_pct:.4f} | {_format_metric(item.profit_factor)} |"
        )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def render_refined_discovery_report(
    results: list[DiscoveryRefinedSummary],
    top: int = 20,
) -> str:
    if not results:
        return "Nenhum padrao sobreviveu ao refinamento de robustez."

    headers = (
        ("Padrao", 78),
        ("Trades", 8),
        ("Val", 8),
        ("AvgUp", 8),
        ("PFUp", 8),
        ("ValPF", 8),
        ("ValNet", 11),
    )
    header_line = " ".join(label.ljust(width) for label, width in headers)
    separator = "-" * len(header_line)
    lines = [header_line, separator]

    for item in results[:top]:
        lines.append(
            " ".join(
                [
                    item.label.ljust(78),
                    str(item.total_occurrences).rjust(8),
                    str(item.validation_trades).rjust(8),
                    f"{item.average_trade_uplift_pct:>8.2f}",
                    f"{(item.profit_factor_uplift or 0.0):>8.2f}",
                    _format_metric(item.validation_profit_factor).rjust(8),
                    f"{item.validation_net_trade_return_pct:>11.2f}",
                ]
            )
        )

    return "\n".join(lines)


def export_refined_discovery_csv(
    results: list[DiscoveryRefinedSummary],
    output_path: str | Path,
) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(
            file_obj,
            fieldnames=[
                "code",
                "label",
                "family",
                "template_code",
                "template_label",
                "option_side",
                "dte_target_days",
                "trade_direction",
                "state_size",
                "state_signature",
                "feature_keys",
                "tickers_with_matches",
                "total_occurrences",
                "profitable_trade_rate_pct",
                "average_trade_return_pct",
                "net_trade_return_pct",
                "profit_factor",
                "baseline_average_trade_return_pct",
                "baseline_profit_factor",
                "average_trade_uplift_pct",
                "profit_factor_uplift",
                "train_trades",
                "train_average_trade_return_pct",
                "train_net_trade_return_pct",
                "train_profit_factor",
                "validation_trades",
                "validation_average_trade_return_pct",
                "validation_net_trade_return_pct",
                "validation_profit_factor",
                "active_months",
                "positive_months",
                "positive_month_ratio",
                "validation_active_months",
                "validation_positive_months",
                "validation_positive_month_ratio",
                "overlap_bucket",
                "robustness_score",
            ],
        )
        writer.writeheader()
        for item in results:
            writer.writerow(
                {
                    "code": item.code,
                    "label": item.label,
                    "family": item.family,
                    "template_code": item.template_code,
                    "template_label": item.template_label,
                    "option_side": item.option_side,
                    "dte_target_days": item.dte_target_days,
                    "trade_direction": item.trade_direction,
                    "state_size": item.state_size,
                    "state_signature": item.state_signature,
                    "feature_keys": item.feature_keys,
                    "tickers_with_matches": item.tickers_with_matches,
                    "total_occurrences": item.total_occurrences,
                    "profitable_trade_rate_pct": f"{item.profitable_trade_rate_pct:.4f}",
                    "average_trade_return_pct": f"{item.average_trade_return_pct:.4f}",
                    "net_trade_return_pct": f"{item.net_trade_return_pct:.4f}",
                    "profit_factor": (
                        "INF" if item.profit_factor is not None and isinf(item.profit_factor)
                        else "" if item.profit_factor is None
                        else f"{item.profit_factor:.4f}"
                    ),
                    "baseline_average_trade_return_pct": f"{item.baseline_average_trade_return_pct:.4f}",
                    "baseline_profit_factor": (
                        "INF" if item.baseline_profit_factor is not None and isinf(item.baseline_profit_factor)
                        else "" if item.baseline_profit_factor is None
                        else f"{item.baseline_profit_factor:.4f}"
                    ),
                    "average_trade_uplift_pct": f"{item.average_trade_uplift_pct:.4f}",
                    "profit_factor_uplift": (
                        "" if item.profit_factor_uplift is None else f"{item.profit_factor_uplift:.4f}"
                    ),
                    "train_trades": item.train_trades,
                    "train_average_trade_return_pct": f"{item.train_average_trade_return_pct:.4f}",
                    "train_net_trade_return_pct": f"{item.train_net_trade_return_pct:.4f}",
                    "train_profit_factor": (
                        "INF" if item.train_profit_factor is not None and isinf(item.train_profit_factor)
                        else "" if item.train_profit_factor is None
                        else f"{item.train_profit_factor:.4f}"
                    ),
                    "validation_trades": item.validation_trades,
                    "validation_average_trade_return_pct": f"{item.validation_average_trade_return_pct:.4f}",
                    "validation_net_trade_return_pct": f"{item.validation_net_trade_return_pct:.4f}",
                    "validation_profit_factor": (
                        "INF" if item.validation_profit_factor is not None and isinf(item.validation_profit_factor)
                        else "" if item.validation_profit_factor is None
                        else f"{item.validation_profit_factor:.4f}"
                    ),
                    "active_months": item.active_months,
                    "positive_months": item.positive_months,
                    "positive_month_ratio": f"{item.positive_month_ratio:.4f}",
                    "validation_active_months": item.validation_active_months,
                    "validation_positive_months": item.validation_positive_months,
                    "validation_positive_month_ratio": f"{item.validation_positive_month_ratio:.4f}",
                    "overlap_bucket": item.overlap_bucket,
                    "robustness_score": f"{item.robustness_score:.4f}",
                }
            )

    return path


def export_refined_discovery_markdown(
    results: list[DiscoveryRefinedSummary],
    output_path: str | Path,
    *,
    validation_start_date: str,
) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        "# Shortlist Refinada do Discovery",
        "",
        f"- Inicio da validacao: {validation_start_date}",
        f"- Padroes selecionados: {len(results)}",
        "",
        "| Padrao | Trades | Val trades | Avg uplift % | PF uplift | Val PF | Val net % |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]

    for item in results:
        lines.append(
            f"| {item.label} | {item.total_occurrences} | {item.validation_trades} | "
            f"{item.average_trade_uplift_pct:.4f} | {(item.profit_factor_uplift or 0.0):.4f} | "
            f"{_format_metric(item.validation_profit_factor)} | {item.validation_net_trade_return_pct:.4f} |"
        )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path
