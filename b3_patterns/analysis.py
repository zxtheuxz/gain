from __future__ import annotations

from collections import defaultdict
from math import prod
from pathlib import Path
from statistics import median

from .db import connect, initialize_database
from .models import (
    AdjustedBar,
    PatternResult,
    StrategyDefinition,
    StrategySummary,
    StrategyTickerSummary,
    StrategyTrade,
)


def _matches_threshold(
    change_pct: float,
    threshold_pct: float,
    mode: str = "auto",
) -> bool:
    if mode == "up":
        return change_pct >= threshold_pct
    if mode == "down":
        return change_pct <= threshold_pct
    if threshold_pct >= 0:
        return change_pct >= threshold_pct
    return change_pct <= threshold_pct


def _coerce_adjustment_factor(close_price: float, adj_close_price: float) -> float:
    if close_price == 0:
        return 1.0
    return adj_close_price / close_price


def _load_grouped_bars(db_path: str | Path) -> dict[str, list[AdjustedBar]]:
    grouped_bars: dict[str, list[AdjustedBar]] = defaultdict(list)

    with connect(db_path) as connection:
        initialize_database(connection)
        rows = connection.execute(
            """
            SELECT ticker, trade_date, open, high, low, close, adj_close
            FROM price_history
            ORDER BY ticker, trade_date
            """
        ).fetchall()

    for row in rows:
        adjustment_factor = _coerce_adjustment_factor(
            close_price=float(row["close"]),
            adj_close_price=float(row["adj_close"]),
        )
        grouped_bars[row["ticker"]].append(
            AdjustedBar(
                ticker=str(row["ticker"]),
                trade_date=str(row["trade_date"]),
                open=float(row["open"]) * adjustment_factor,
                high=float(row["high"]) * adjustment_factor,
                low=float(row["low"]) * adjustment_factor,
                close=float(row["adj_close"]),
            )
        )

    return dict(grouped_bars)


def _calculate_trade_return_pct(
    entry_price: float,
    exit_price: float,
    trade_direction: str,
) -> float:
    gross_return = ((exit_price / entry_price) - 1.0) * 100.0
    if trade_direction == "short":
        return -gross_return
    return gross_return


def _calculate_profit_factor(trade_returns: list[float]) -> float | None:
    gross_profit = sum(value for value in trade_returns if value > 0)
    gross_loss = -sum(value for value in trade_returns if value < 0)
    if gross_loss == 0:
        if gross_profit == 0:
            return None
        return float("inf")
    return gross_profit / gross_loss


def _build_close_change_strategy(
    threshold_pct: float,
    trigger_direction: str,
    trade_direction: str,
    min_trade_return_pct: float,
) -> StrategyDefinition:
    direction_label = "Queda" if trigger_direction == "down" else "Alta"
    action_label = "sobe" if trade_direction == "long" else "cai"
    suffix = "reversao" if trigger_direction != ("up" if trade_direction == "long" else "down") else "continuacao"
    display_level = f"{threshold_pct:.1f}".rstrip("0").rstrip(".")
    return StrategyDefinition(
        code=f"close_{'queda' if trigger_direction == 'down' else 'alta'}_{display_level}_{suffix}",
        label=(
            f"Fechamento: {direction_label} >= {display_level}%"
            f" -> {'compra' if trade_direction == 'long' else 'venda'} no close, T+1 {action_label}"
        ),
        family="close_to_close",
        setup_kind="close_change",
        trigger_direction=trigger_direction,
        threshold_pct=threshold_pct,
        trade_direction=trade_direction,
        entry_rule="close",
        exit_offset_days=1,
        min_trade_return_pct=min_trade_return_pct,
    )


def _build_intraday_touch_strategy(
    threshold_pct: float,
    trigger_direction: str,
    exit_offset_days: int,
    min_trade_return_pct: float,
) -> StrategyDefinition:
    direction_label = "Queda" if trigger_direction == "down" else "Alta"
    trade_direction = "long" if trigger_direction == "down" else "short"
    exit_label = "D0" if exit_offset_days == 0 else "D1"
    display_level = f"{threshold_pct:.1f}".rstrip("0").rstrip(".")
    return StrategyDefinition(
        code=(
            f"intraday_{'queda' if trigger_direction == 'down' else 'alta'}_"
            f"{display_level}_close_{exit_label.lower()}"
        ),
        label=(
            f"Intraday: {direction_label} >= {display_level}%"
            f" -> {'compra' if trade_direction == 'long' else 'venda'} no gatilho, sai close {exit_label}"
        ),
        family="intraday_touch",
        setup_kind="intraday_touch",
        trigger_direction=trigger_direction,
        threshold_pct=threshold_pct,
        trade_direction=trade_direction,
        entry_rule="threshold",
        exit_offset_days=exit_offset_days,
        min_trade_return_pct=min_trade_return_pct,
    )


def _build_gap_open_strategy(
    threshold_pct: float,
    trigger_direction: str,
    min_trade_return_pct: float,
) -> StrategyDefinition:
    direction_label = "Gap de baixa" if trigger_direction == "down" else "Gap de alta"
    trade_direction = "long" if trigger_direction == "down" else "short"
    display_level = f"{threshold_pct:.1f}".rstrip("0").rstrip(".")
    return StrategyDefinition(
        code=f"gap_{'baixa' if trigger_direction == 'down' else 'alta'}_{display_level}_close_d0",
        label=(
            f"Gap: {direction_label} >= {display_level}%"
            f" -> {'compra' if trade_direction == 'long' else 'venda'} na abertura, sai close D0"
        ),
        family="gap_open",
        setup_kind="gap_open",
        trigger_direction=trigger_direction,
        threshold_pct=threshold_pct,
        trade_direction=trade_direction,
        entry_rule="open",
        exit_offset_days=0,
        min_trade_return_pct=min_trade_return_pct,
    )


def build_strategy_definitions(
    threshold_levels: list[float] | None = None,
    min_trade_return_pct: float = 0.0,
) -> list[StrategyDefinition]:
    levels = threshold_levels or [1.0, 2.0, 3.0, 4.0, 5.0]
    normalized_levels = sorted({abs(level) for level in levels if level > 0})
    if not normalized_levels:
        raise ValueError("Informe ao menos um nivel positivo para a grade de estrategias.")

    strategies: list[StrategyDefinition] = []

    for level in normalized_levels:
        strategies.extend(
            [
                _build_close_change_strategy(level, "down", "long", min_trade_return_pct),
                _build_close_change_strategy(level, "down", "short", min_trade_return_pct),
                _build_close_change_strategy(level, "up", "long", min_trade_return_pct),
                _build_close_change_strategy(level, "up", "short", min_trade_return_pct),
                _build_intraday_touch_strategy(level, "down", 0, min_trade_return_pct),
                _build_intraday_touch_strategy(level, "down", 1, min_trade_return_pct),
                _build_intraday_touch_strategy(level, "up", 0, min_trade_return_pct),
                _build_intraday_touch_strategy(level, "up", 1, min_trade_return_pct),
                _build_gap_open_strategy(level, "down", min_trade_return_pct),
                _build_gap_open_strategy(level, "up", min_trade_return_pct),
            ]
        )

    return strategies


def _resolve_trigger_match(
    previous_bar: AdjustedBar,
    current_bar: AdjustedBar,
    strategy: StrategyDefinition,
) -> tuple[float, float] | None:
    previous_close = previous_bar.close
    threshold_ratio = strategy.threshold_pct / 100.0

    if strategy.setup_kind == "close_change":
        trigger_change_pct = ((current_bar.close / previous_close) - 1.0) * 100.0
        mode = "down" if strategy.trigger_direction == "down" else "up"
        if not _matches_threshold(
            trigger_change_pct,
            -strategy.threshold_pct if mode == "down" else strategy.threshold_pct,
            mode=mode,
        ):
            return None
        return trigger_change_pct, current_bar.close

    if strategy.setup_kind == "intraday_touch":
        if strategy.trigger_direction == "down":
            trigger_price = previous_close * (1.0 - threshold_ratio)
            if current_bar.low > trigger_price:
                return None
            return -strategy.threshold_pct, trigger_price

        trigger_price = previous_close * (1.0 + threshold_ratio)
        if current_bar.high < trigger_price:
            return None
        return strategy.threshold_pct, trigger_price

    if strategy.setup_kind == "gap_open":
        trigger_change_pct = ((current_bar.open / previous_close) - 1.0) * 100.0
        mode = "down" if strategy.trigger_direction == "down" else "up"
        if not _matches_threshold(
            trigger_change_pct,
            -strategy.threshold_pct if mode == "down" else strategy.threshold_pct,
            mode=mode,
        ):
            return None
        return trigger_change_pct, current_bar.open

    raise ValueError(f"Tipo de setup desconhecido: {strategy.setup_kind}")


def _evaluate_single_strategy(
    grouped_bars: dict[str, list[AdjustedBar]],
    strategy: StrategyDefinition,
) -> tuple[list[PatternResult], list[float], list[float], list[StrategyTrade]]:
    pattern_results: list[PatternResult] = []
    asset_moves: list[float] = []
    trade_returns: list[float] = []
    trades: list[StrategyTrade] = []

    for ticker, bars in grouped_bars.items():
        if len(bars) < 2 + strategy.exit_offset_days:
            continue

        ticker_asset_moves: list[float] = []
        ticker_trade_returns: list[float] = []
        successful_occurrences = 0

        for idx in range(1, len(bars)):
            exit_idx = idx + strategy.exit_offset_days
            if exit_idx >= len(bars):
                break

            previous_bar = bars[idx - 1]
            current_bar = bars[idx]
            exit_bar = bars[exit_idx]
            trigger_match = _resolve_trigger_match(previous_bar, current_bar, strategy)
            if trigger_match is None:
                continue

            trigger_change_pct, entry_price = trigger_match
            asset_move_pct = ((exit_bar.close / entry_price) - 1.0) * 100.0
            trade_return_pct = _calculate_trade_return_pct(
                entry_price=entry_price,
                exit_price=exit_bar.close,
                trade_direction=strategy.trade_direction,
            )
            is_successful = trade_return_pct >= strategy.min_trade_return_pct

            ticker_asset_moves.append(asset_move_pct)
            ticker_trade_returns.append(trade_return_pct)
            asset_moves.append(asset_move_pct)
            trade_returns.append(trade_return_pct)
            if is_successful:
                successful_occurrences += 1

            trades.append(
                StrategyTrade(
                    strategy_code=strategy.code,
                    strategy_label=strategy.label,
                    family=strategy.family,
                    ticker=ticker,
                    trigger_date=current_bar.trade_date,
                    exit_date=exit_bar.trade_date,
                    direction=strategy.trade_direction,
                    trigger_change_pct=trigger_change_pct,
                    entry_price=entry_price,
                    exit_price=exit_bar.close,
                    asset_move_pct=asset_move_pct,
                    trade_return_pct=trade_return_pct,
                    is_profitable=trade_return_pct > 0,
                    is_successful=is_successful,
                )
            )

        if not ticker_trade_returns:
            continue

        pattern_results.append(
            PatternResult(
                ticker=ticker,
                occurrences=len(ticker_trade_returns),
                successful_occurrences=successful_occurrences,
                success_rate_pct=(successful_occurrences / len(ticker_trade_returns)) * 100.0,
                average_next_day_return_pct=sum(ticker_trade_returns) / len(ticker_trade_returns),
                median_next_day_return_pct=median(ticker_trade_returns),
            )
        )

    ranked_patterns = sorted(
        pattern_results,
        key=lambda item: (
            item.success_rate_pct,
            item.average_next_day_return_pct,
            item.occurrences,
        ),
        reverse=True,
    )

    return ranked_patterns, asset_moves, trade_returns, trades


def analyze_patterns(
    db_path: str | Path,
    trigger_change_pct: float = -2.0,
    target_next_day_pct: float = 0.0,
    target_mode: str = "auto",
) -> list[PatternResult]:
    grouped_bars = _load_grouped_bars(db_path)
    trigger_direction = "down" if trigger_change_pct < 0 else "up"
    trade_direction = "short" if target_mode == "down" or (
        target_mode == "auto" and target_next_day_pct < 0
    ) else "long"
    strategy = StrategyDefinition(
        code="custom_close_analysis",
        label="Analise customizada close-to-close",
        family="close_to_close",
        setup_kind="close_change",
        trigger_direction=trigger_direction,
        threshold_pct=abs(trigger_change_pct),
        trade_direction=trade_direction,
        entry_rule="close",
        exit_offset_days=1,
        min_trade_return_pct=abs(target_next_day_pct),
    )
    ranked_patterns, _, _, _ = _evaluate_single_strategy(grouped_bars, strategy)
    return ranked_patterns


def backtest_strategy_grid(
    db_path: str | Path,
    threshold_levels: list[float] | None = None,
    min_trade_return_pct: float = 0.0,
    strategy_definitions: list[StrategyDefinition] | None = None,
) -> tuple[list[StrategySummary], list[StrategyTrade]]:
    grouped_bars = _load_grouped_bars(db_path)
    summaries: list[StrategySummary] = []
    all_trades: list[StrategyTrade] = []

    candidate_strategies = strategy_definitions or build_strategy_definitions(
        threshold_levels=threshold_levels,
        min_trade_return_pct=min_trade_return_pct,
    )

    for strategy in candidate_strategies:
        ticker_results, asset_moves, trade_returns, trades = _evaluate_single_strategy(
            grouped_bars=grouped_bars,
            strategy=strategy,
        )
        if not trade_returns:
            continue

        profitable_trades = sum(1 for value in trade_returns if value > 0)
        total_occurrences = len(trade_returns)
        successful_occurrences = sum(
            1 for value in trade_returns if value >= strategy.min_trade_return_pct
        )

        summaries.append(
            StrategySummary(
                code=strategy.code,
                label=strategy.label,
                family=strategy.family,
                setup_kind=strategy.setup_kind,
                trigger_direction=strategy.trigger_direction,
                threshold_pct=strategy.threshold_pct,
                trade_direction=strategy.trade_direction,
                entry_rule=strategy.entry_rule,
                exit_offset_days=strategy.exit_offset_days,
                min_trade_return_pct=strategy.min_trade_return_pct,
                tickers_with_matches=len(ticker_results),
                total_occurrences=total_occurrences,
                successful_occurrences=successful_occurrences,
                success_rate_pct=(successful_occurrences / total_occurrences) * 100.0,
                average_asset_move_pct=sum(asset_moves) / total_occurrences,
                median_asset_move_pct=median(asset_moves),
                profitable_trades=profitable_trades,
                profitable_trade_rate_pct=(profitable_trades / total_occurrences) * 100.0,
                average_trade_return_pct=sum(trade_returns) / total_occurrences,
                median_trade_return_pct=median(trade_returns),
                net_trade_return_pct=sum(trade_returns),
                cumulative_return_pct=(
                    prod(1.0 + (value / 100.0) for value in trade_returns) - 1.0
                ) * 100.0,
                profit_factor=_calculate_profit_factor(trade_returns),
            )
        )
        all_trades.extend(trades)

    sorted_summaries = sorted(
        summaries,
        key=lambda item: (
            item.net_trade_return_pct,
            item.profit_factor if item.profit_factor is not None else float("-inf"),
            item.success_rate_pct,
            item.average_trade_return_pct,
        ),
        reverse=True,
    )
    sorted_trades = sorted(
        all_trades,
        key=lambda item: (item.strategy_code, item.trigger_date, item.ticker),
    )
    return sorted_summaries, sorted_trades


def qualify_strategy_summary(
    summary: StrategySummary,
    min_success_rate_pct: float = 55.0,
    min_profit_factor: float = 1.0,
    min_average_trade_return_pct: float = 0.10,
    min_trades: int = 200,
    require_positive_net: bool = True,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if summary.success_rate_pct < min_success_rate_pct:
        reasons.append(f"success_rate<{min_success_rate_pct:.2f}")
    if summary.profit_factor is None or summary.profit_factor < min_profit_factor:
        reasons.append(f"profit_factor<{min_profit_factor:.2f}")
    if summary.average_trade_return_pct < min_average_trade_return_pct:
        reasons.append(f"average_trade_return<{min_average_trade_return_pct:.2f}")
    if summary.total_occurrences < min_trades:
        reasons.append(f"trades<{min_trades}")
    if require_positive_net and summary.net_trade_return_pct <= 0:
        reasons.append("net<=0")
    return not reasons, reasons


def split_strategy_results(
    summaries: list[StrategySummary],
    trades: list[StrategyTrade],
    min_success_rate_pct: float = 55.0,
    min_profit_factor: float = 1.0,
    min_average_trade_return_pct: float = 0.10,
    min_trades: int = 200,
    require_positive_net: bool = True,
) -> tuple[list[StrategySummary], list[StrategyTrade], list[StrategySummary], dict[str, list[str]]]:
    approved_summaries: list[StrategySummary] = []
    rejected_summaries: list[StrategySummary] = []
    rejection_reasons: dict[str, list[str]] = {}

    for summary in summaries:
        approved, reasons = qualify_strategy_summary(
            summary=summary,
            min_success_rate_pct=min_success_rate_pct,
            min_profit_factor=min_profit_factor,
            min_average_trade_return_pct=min_average_trade_return_pct,
            min_trades=min_trades,
            require_positive_net=require_positive_net,
        )
        if approved:
            approved_summaries.append(summary)
        else:
            rejected_summaries.append(summary)
            rejection_reasons[summary.code] = reasons

    approved_codes = {item.code for item in approved_summaries}
    approved_trades = [item for item in trades if item.strategy_code in approved_codes]
    return approved_summaries, approved_trades, rejected_summaries, rejection_reasons


def filter_strategy_results(
    summaries: list[StrategySummary],
    trades: list[StrategyTrade],
    min_success_rate_pct: float = 55.0,
    min_profit_factor: float = 1.0,
    min_average_trade_return_pct: float = 0.10,
    min_trades: int = 200,
    require_positive_net: bool = True,
) -> tuple[list[StrategySummary], list[StrategyTrade]]:
    qualified_summaries, qualified_trades, _, _ = split_strategy_results(
        summaries=summaries,
        trades=trades,
        min_success_rate_pct=min_success_rate_pct,
        min_profit_factor=min_profit_factor,
        min_average_trade_return_pct=min_average_trade_return_pct,
        min_trades=min_trades,
        require_positive_net=require_positive_net,
    )
    return qualified_summaries, qualified_trades


def summarize_trades_by_ticker(trades: list[StrategyTrade]) -> list[StrategyTickerSummary]:
    grouped_trades: dict[tuple[str, str], list[StrategyTrade]] = defaultdict(list)
    strategy_labels: dict[str, str] = {}
    strategy_families: dict[str, str] = {}

    for trade in trades:
        grouped_trades[(trade.strategy_code, trade.ticker)].append(trade)
        strategy_labels[trade.strategy_code] = trade.strategy_label
        strategy_families[trade.strategy_code] = trade.family

    summaries: list[StrategyTickerSummary] = []

    for (strategy_code, ticker), ticker_trades in grouped_trades.items():
        trade_returns = [item.trade_return_pct for item in ticker_trades]
        successful_trades = sum(1 for item in ticker_trades if item.is_successful)
        profitable_trades = sum(1 for item in ticker_trades if item.is_profitable)
        sorted_dates = sorted(item.trigger_date for item in ticker_trades)
        summaries.append(
            StrategyTickerSummary(
                strategy_code=strategy_code,
                strategy_label=strategy_labels[strategy_code],
                family=strategy_families[strategy_code],
                ticker=ticker,
                total_trades=len(ticker_trades),
                successful_trades=successful_trades,
                success_rate_pct=(successful_trades / len(ticker_trades)) * 100.0,
                profitable_trades=profitable_trades,
                profitable_trade_rate_pct=(profitable_trades / len(ticker_trades)) * 100.0,
                average_trade_return_pct=sum(trade_returns) / len(ticker_trades),
                median_trade_return_pct=median(trade_returns),
                net_trade_return_pct=sum(trade_returns),
                cumulative_return_pct=(
                    prod(1.0 + (value / 100.0) for value in trade_returns) - 1.0
                ) * 100.0,
                profit_factor=_calculate_profit_factor(trade_returns),
                first_trade_date=sorted_dates[0],
                last_trade_date=sorted_dates[-1],
            )
        )

    return sorted(
        summaries,
        key=lambda item: (
            item.net_trade_return_pct,
            item.average_trade_return_pct,
            item.total_trades,
        ),
        reverse=True,
    )


def filter_strategy_ticker_results(
    summaries: list[StrategyTickerSummary],
    min_success_rate_pct: float = 55.0,
    min_profit_factor: float = 1.0,
    min_average_trade_return_pct: float = 0.50,
    min_trades: int = 15,
    require_positive_net: bool = True,
) -> list[StrategyTickerSummary]:
    return [
        item
        for item in summaries
        if item.success_rate_pct >= min_success_rate_pct
        and (item.profit_factor is not None and item.profit_factor >= min_profit_factor)
        and item.average_trade_return_pct >= min_average_trade_return_pct
        and item.total_trades >= min_trades
        and (item.net_trade_return_pct > 0 if require_positive_net else True)
    ]
