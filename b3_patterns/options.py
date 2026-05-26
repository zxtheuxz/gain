from __future__ import annotations

from bisect import bisect_left
from collections import defaultdict
from datetime import date, datetime, timedelta
from math import prod
from pathlib import Path
from statistics import median

from .db import connect, initialize_database
from .models import (
    OptionQuoteBar,
    OptionStrategyDefinition,
    SpotQuoteBar,
    StrategySummary,
    StrategyTrade,
)
from .tickers import load_tickers


def _calculate_profit_factor(trade_returns: list[float]) -> float | None:
    gross_profit = sum(value for value in trade_returns if value > 0)
    gross_loss = -sum(value for value in trade_returns if value < 0)
    if gross_loss == 0:
        if gross_profit == 0:
            return None
        return float("inf")
    return gross_profit / gross_loss


def _coerce_date(raw_value: str) -> date:
    return datetime.strptime(raw_value, "%Y-%m-%d").date()


def build_option_strategy_definitions(
    min_trade_return_pct: float = 0.0,
    round_trip_cost_pct: float = 0.0,
) -> list[OptionStrategyDefinition]:
    strategies: list[OptionStrategyDefinition] = []

    for threshold_pct in (1.0, 2.0):
        for dte_target_days in (7, 15):
            strategies.append(
                OptionStrategyDefinition(
                    code=f"opt_gap_baixa_{int(threshold_pct)}_call_atm_{dte_target_days}dte_d0",
                    label=(
                        f"Opcao ATM: gap de baixa >= {int(threshold_pct)}%"
                        f" -> compra CALL ATM {dte_target_days} DTE, sai close D0"
                    ),
                    family="option_gap_reversal",
                    setup_kind="gap",
                    trigger_direction="down",
                    threshold_pct=threshold_pct,
                    option_side="call",
                    dte_target_days=dte_target_days,
                    entry_rule="open",
                    holding_days=0,
                    min_trade_return_pct=min_trade_return_pct,
                    round_trip_cost_pct=round_trip_cost_pct,
                )
            )
            strategies.append(
                OptionStrategyDefinition(
                    code=f"opt_gap_alta_{int(threshold_pct)}_put_atm_{dte_target_days}dte_d0",
                    label=(
                        f"Opcao ATM: gap de alta >= {int(threshold_pct)}%"
                        f" -> compra PUT ATM {dte_target_days} DTE, sai close D0"
                    ),
                    family="option_gap_reversal",
                    setup_kind="gap",
                    trigger_direction="up",
                    threshold_pct=threshold_pct,
                    option_side="put",
                    dte_target_days=dte_target_days,
                    entry_rule="open",
                    holding_days=0,
                    min_trade_return_pct=min_trade_return_pct,
                    round_trip_cost_pct=round_trip_cost_pct,
                )
            )

    for threshold_pct in (2.0, 3.0):
        for dte_target_days in (15, 30):
            strategies.append(
                OptionStrategyDefinition(
                    code=f"opt_close_queda_{int(threshold_pct)}_call_atm_{dte_target_days}dte_d1",
                    label=(
                        f"Opcao ATM: fechamento caiu >= {int(threshold_pct)}%"
                        f" -> compra CALL ATM {dte_target_days} DTE, sai close D1"
                    ),
                    family="option_close_reversal",
                    setup_kind="close_change",
                    trigger_direction="down",
                    threshold_pct=threshold_pct,
                    option_side="call",
                    dte_target_days=dte_target_days,
                    entry_rule="close",
                    holding_days=1,
                    min_trade_return_pct=min_trade_return_pct,
                    round_trip_cost_pct=round_trip_cost_pct,
                )
            )

    for lookback_days, threshold_pct, holding_days in (
        (2, 3.0, 7),
        (3, 5.0, 15),
        (5, 8.0, 30),
    ):
        strategies.append(
            OptionStrategyDefinition(
                code=(
                    f"opt_multi_{lookback_days}d_queda_{int(threshold_pct)}"
                    f"_call_atm_{holding_days}dte_h{holding_days}"
                ),
                label=(
                    f"Opcao ATM: queda acumulada {lookback_days}D >= {int(threshold_pct)}%"
                    f" -> compra CALL ATM {holding_days} DTE, sai D+{holding_days}"
                ),
                family="option_multi_day_reversal",
                setup_kind="cumulative_return",
                trigger_direction="down",
                threshold_pct=threshold_pct,
                option_side="call",
                dte_target_days=holding_days,
                entry_rule="close",
                holding_days=holding_days,
                min_trade_return_pct=min_trade_return_pct,
                round_trip_cost_pct=round_trip_cost_pct,
                lookback_days=lookback_days,
            )
        )
        strategies.append(
            OptionStrategyDefinition(
                code=(
                    f"opt_multi_{lookback_days}d_alta_{int(threshold_pct)}"
                    f"_put_atm_{holding_days}dte_h{holding_days}"
                ),
                label=(
                    f"Opcao ATM: alta acumulada {lookback_days}D >= {int(threshold_pct)}%"
                    f" -> compra PUT ATM {holding_days} DTE, sai D+{holding_days}"
                ),
                family="option_multi_day_reversal",
                setup_kind="cumulative_return",
                trigger_direction="up",
                threshold_pct=threshold_pct,
                option_side="put",
                dte_target_days=holding_days,
                entry_rule="close",
                holding_days=holding_days,
                min_trade_return_pct=min_trade_return_pct,
                round_trip_cost_pct=round_trip_cost_pct,
                lookback_days=lookback_days,
            )
        )

    for fast_ma_days, slow_ma_days, holding_days in (
        (5, 20, 7),
        (10, 20, 15),
        (20, 50, 30),
    ):
        strategies.append(
            OptionStrategyDefinition(
                code=(
                    f"opt_ma_{fast_ma_days}x{slow_ma_days}_bull"
                    f"_call_atm_{holding_days}dte_h{holding_days}"
                ),
                label=(
                    f"Opcao ATM: MM{fast_ma_days} cruza acima MM{slow_ma_days}"
                    f" -> compra CALL ATM {holding_days} DTE, sai D+{holding_days}"
                ),
                family="option_moving_average",
                setup_kind="moving_average_cross",
                trigger_direction="up",
                threshold_pct=0.0,
                option_side="call",
                dte_target_days=holding_days,
                entry_rule="close",
                holding_days=holding_days,
                min_trade_return_pct=min_trade_return_pct,
                round_trip_cost_pct=round_trip_cost_pct,
                fast_ma_days=fast_ma_days,
                slow_ma_days=slow_ma_days,
            )
        )
        strategies.append(
            OptionStrategyDefinition(
                code=(
                    f"opt_ma_{fast_ma_days}x{slow_ma_days}_bear"
                    f"_put_atm_{holding_days}dte_h{holding_days}"
                ),
                label=(
                    f"Opcao ATM: MM{fast_ma_days} cruza abaixo MM{slow_ma_days}"
                    f" -> compra PUT ATM {holding_days} DTE, sai D+{holding_days}"
                ),
                family="option_moving_average",
                setup_kind="moving_average_cross",
                trigger_direction="down",
                threshold_pct=0.0,
                option_side="put",
                dte_target_days=holding_days,
                entry_rule="close",
                holding_days=holding_days,
                min_trade_return_pct=min_trade_return_pct,
                round_trip_cost_pct=round_trip_cost_pct,
                fast_ma_days=fast_ma_days,
                slow_ma_days=slow_ma_days,
            )
        )

    for threshold_pct in (1.0, 2.0):
        for dte_target_days in (7, 15):
            for stop_loss_pct, take_profit_levels in (
                (5.0, (1.0, 2.0, 3.0, 4.0, 5.0, 6.0)),
                (3.0, (6.0, 8.0, 10.0, 12.0)),
                (5.0, (8.0, 10.0, 12.0, 15.0)),
            ):
                for take_profit_pct in take_profit_levels:
                    stop_label = int(stop_loss_pct)
                    target_label = int(take_profit_pct)
                    strategies.append(
                        OptionStrategyDefinition(
                            code=(
                                f"opt_gap_baixa_{int(threshold_pct)}_call_atm_{dte_target_days}dte"
                                f"_sl{stop_label}_tp{target_label}"
                            ),
                            label=(
                                f"Opcao ATM: gap de baixa >= {int(threshold_pct)}%"
                                f" -> compra CALL ATM {dte_target_days} DTE,"
                                f" alvo +{target_label}%, stop -{stop_label}%"
                            ),
                            family="option_gap_target_stop",
                            setup_kind="gap",
                            trigger_direction="down",
                            threshold_pct=threshold_pct,
                            option_side="call",
                            dte_target_days=dte_target_days,
                            entry_rule="open",
                            holding_days=dte_target_days,
                            min_trade_return_pct=min_trade_return_pct,
                            round_trip_cost_pct=round_trip_cost_pct,
                            exit_kind="target_stop",
                            take_profit_pct=take_profit_pct,
                            stop_loss_pct=stop_loss_pct,
                        )
                    )
                    strategies.append(
                        OptionStrategyDefinition(
                            code=(
                                f"opt_gap_alta_{int(threshold_pct)}_put_atm_{dte_target_days}dte"
                                f"_sl{stop_label}_tp{target_label}"
                            ),
                            label=(
                                f"Opcao ATM: gap de alta >= {int(threshold_pct)}%"
                                f" -> compra PUT ATM {dte_target_days} DTE,"
                                f" alvo +{target_label}%, stop -{stop_label}%"
                            ),
                            family="option_gap_target_stop",
                            setup_kind="gap",
                            trigger_direction="up",
                            threshold_pct=threshold_pct,
                            option_side="put",
                            dte_target_days=dte_target_days,
                            entry_rule="open",
                            holding_days=dte_target_days,
                            min_trade_return_pct=min_trade_return_pct,
                            round_trip_cost_pct=round_trip_cost_pct,
                            exit_kind="target_stop",
                            take_profit_pct=take_profit_pct,
                            stop_loss_pct=stop_loss_pct,
                        )
                    )

    return strategies


def _load_spot_bars(
    db_path: str | Path,
    allowed_tickers: set[str],
) -> dict[str, list[SpotQuoteBar]]:
    grouped_bars: dict[str, list[SpotQuoteBar]] = defaultdict(list)

    with connect(db_path) as connection:
        initialize_database(connection)
        rows = connection.execute(
            """
            SELECT ticker, trade_date, open, high, low, close, volume, trade_count
            FROM spot_quote_history
            ORDER BY ticker, trade_date
            """
        ).fetchall()

    for row in rows:
        ticker = str(row["ticker"]).upper()
        if ticker not in allowed_tickers:
            continue
        grouped_bars[ticker].append(
            SpotQuoteBar(
                ticker=ticker,
                trade_date=str(row["trade_date"]),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=int(row["volume"]),
                trade_count=int(row["trade_count"]),
            )
        )

    return dict(grouped_bars)


def _load_option_bars(db_path: str | Path) -> tuple[
    dict[str, dict[str, list[OptionQuoteBar]]],
    dict[tuple[str, str], OptionQuoteBar],
]:
    by_root_and_date: dict[str, dict[str, list[OptionQuoteBar]]] = defaultdict(
        lambda: defaultdict(list)
    )
    by_symbol_and_date: dict[tuple[str, str], OptionQuoteBar] = {}

    with connect(db_path) as connection:
        initialize_database(connection)
        rows = connection.execute(
            """
            SELECT
                option_symbol,
                trade_date,
                underlying_root,
                option_side,
                expiration_date,
                strike_price,
                open,
                high,
                low,
                close,
                volume,
                trade_count
            FROM option_quote_history
            ORDER BY underlying_root, trade_date, option_symbol
            """
        ).fetchall()

    for row in rows:
        bar = OptionQuoteBar(
            option_symbol=str(row["option_symbol"]),
            underlying_root=str(row["underlying_root"]),
            option_side=str(row["option_side"]),
            trade_date=str(row["trade_date"]),
            expiration_date=str(row["expiration_date"]),
            strike_price=float(row["strike_price"]),
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=int(row["volume"]),
            trade_count=int(row["trade_count"]),
        )
        by_root_and_date[bar.underlying_root][bar.trade_date].append(bar)
        by_symbol_and_date[(bar.option_symbol, bar.trade_date)] = bar

    return (
        {
            root: {trade_date: list(items) for trade_date, items in grouped.items()}
            for root, grouped in by_root_and_date.items()
        },
        by_symbol_and_date,
    )


def _build_preferred_root_mapping(spot_bars: dict[str, list[SpotQuoteBar]]) -> dict[str, str]:
    grouped_candidates: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for ticker, bars in spot_bars.items():
        root = ticker[:4]
        total_volume = sum(item.volume for item in bars)
        grouped_candidates[root].append((ticker, total_volume))

    preferred: dict[str, str] = {}
    for root, candidates in grouped_candidates.items():
        preferred[root] = sorted(candidates, key=lambda item: (-item[1], item[0]))[0][0]
    return preferred


def _build_prefix_sums(values: list[float]) -> list[float]:
    prefix_sums = [0.0]
    running_total = 0.0
    for value in values:
        running_total += value
        prefix_sums.append(running_total)
    return prefix_sums


def _calculate_sma(
    prefix_sums: list[float],
    end_idx: int,
    window_days: int,
) -> float | None:
    start_idx = end_idx - window_days + 1
    if start_idx < 0:
        return None
    return (prefix_sums[end_idx + 1] - prefix_sums[start_idx]) / window_days


def _find_exit_index(
    trade_dates: list[date],
    start_idx: int,
    holding_days: int,
) -> int | None:
    target_exit_day = trade_dates[start_idx] + timedelta(days=holding_days)
    exit_idx = bisect_left(trade_dates, target_exit_day, lo=start_idx)
    if exit_idx >= len(trade_dates):
        return None
    return exit_idx


def _resolve_underlying_signal(
    bars: list[SpotQuoteBar],
    close_prices: list[float],
    close_prefix_sums: list[float],
    idx: int,
    strategy: OptionStrategyDefinition,
) -> tuple[bool, float, float]:
    previous_bar = bars[idx - 1]
    current_bar = bars[idx]

    if strategy.setup_kind == "gap":
        trigger_change_pct = ((current_bar.open / previous_bar.close) - 1.0) * 100.0
        if strategy.trigger_direction == "down":
            return (
                trigger_change_pct <= -strategy.threshold_pct,
                trigger_change_pct,
                current_bar.open,
            )
        return (
            trigger_change_pct >= strategy.threshold_pct,
            trigger_change_pct,
            current_bar.open,
        )

    if strategy.setup_kind == "close_change":
        trigger_change_pct = ((current_bar.close / previous_bar.close) - 1.0) * 100.0
        if strategy.trigger_direction == "down":
            return (
                trigger_change_pct <= -strategy.threshold_pct,
                trigger_change_pct,
                current_bar.close,
            )
        return (
            trigger_change_pct >= strategy.threshold_pct,
            trigger_change_pct,
            current_bar.close,
        )

    if strategy.setup_kind == "cumulative_return":
        lookback_days = strategy.lookback_days or 1
        start_idx = idx - lookback_days
        if start_idx < 0:
            return False, 0.0, current_bar.close
        base_price = close_prices[start_idx]
        if base_price <= 0:
            return False, 0.0, current_bar.close
        trigger_change_pct = ((current_bar.close / base_price) - 1.0) * 100.0
        if strategy.trigger_direction == "down":
            return (
                trigger_change_pct <= -strategy.threshold_pct,
                trigger_change_pct,
                current_bar.close,
            )
        return (
            trigger_change_pct >= strategy.threshold_pct,
            trigger_change_pct,
            current_bar.close,
        )

    if strategy.setup_kind == "moving_average_cross":
        if idx <= 0 or not strategy.fast_ma_days or not strategy.slow_ma_days:
            return False, 0.0, current_bar.close
        previous_fast = _calculate_sma(close_prefix_sums, idx - 1, strategy.fast_ma_days)
        previous_slow = _calculate_sma(close_prefix_sums, idx - 1, strategy.slow_ma_days)
        current_fast = _calculate_sma(close_prefix_sums, idx, strategy.fast_ma_days)
        current_slow = _calculate_sma(close_prefix_sums, idx, strategy.slow_ma_days)
        if (
            previous_fast is None
            or previous_slow is None
            or current_fast is None
            or current_slow is None
            or current_slow == 0
        ):
            return False, 0.0, current_bar.close

        trigger_change_pct = ((current_fast / current_slow) - 1.0) * 100.0
        if strategy.trigger_direction == "up":
            return (
                previous_fast <= previous_slow and current_fast > current_slow,
                trigger_change_pct,
                current_bar.close,
            )
        return (
            previous_fast >= previous_slow and current_fast < current_slow,
            trigger_change_pct,
            current_bar.close,
        )

    raise ValueError(f"Tipo de setup de opcao desconhecido: {strategy.setup_kind}")


def _calculate_trade_return_pct(
    entry_price: float,
    exit_price: float,
    round_trip_cost_pct: float,
) -> float:
    if entry_price <= 0:
        return 0.0
    return ((exit_price / entry_price) - 1.0) * 100.0 - round_trip_cost_pct


def _select_atm_option(
    entry_date: str,
    underlying_root: str,
    spot_price: float,
    strategy: OptionStrategyDefinition,
    option_bars_by_root_and_date: dict[str, dict[str, list[OptionQuoteBar]]],
    exit_date: str,
) -> OptionQuoteBar | None:
    day_candidates = option_bars_by_root_and_date.get(underlying_root, {}).get(entry_date, [])
    if not day_candidates:
        return None

    entry_day = _coerce_date(entry_date)
    exit_day = _coerce_date(exit_date)
    eligible_candidates = []

    for bar in day_candidates:
        if bar.option_side != strategy.option_side:
            continue
        expiration_day = _coerce_date(bar.expiration_date)
        dte_days = (expiration_day - entry_day).days
        if dte_days < strategy.dte_target_days:
            continue
        if expiration_day < exit_day:
            continue
        entry_price = bar.open if strategy.entry_rule == "open" else bar.close
        if entry_price <= 0:
            continue
        eligible_candidates.append((bar, dte_days))

    if not eligible_candidates:
        return None

    eligible_candidates.sort(
        key=lambda item: (
            item[1] - strategy.dte_target_days,
            abs(item[0].strike_price - spot_price),
            -item[0].trade_count,
            -item[0].volume,
            item[0].option_symbol,
        )
    )
    return eligible_candidates[0][0]


def _resolve_target_stop_exit(
    *,
    bars: list[SpotQuoteBar],
    entry_idx: int,
    max_exit_idx: int,
    selected_option: OptionQuoteBar,
    option_bars_by_symbol_and_date: dict[tuple[str, str], OptionQuoteBar],
    strategy: OptionStrategyDefinition,
    entry_price: float,
) -> tuple[str, float, float, str] | None:
    target_multiplier = 1.0 + ((strategy.take_profit_pct or 0.0) / 100.0)
    stop_multiplier = 1.0 - ((strategy.stop_loss_pct or 0.0) / 100.0)
    target_price = entry_price * target_multiplier
    stop_price = entry_price * stop_multiplier
    monitoring_start_idx = entry_idx if strategy.entry_rule == "open" else entry_idx + 1
    last_observed_bar: OptionQuoteBar | None = None

    for day_idx in range(monitoring_start_idx, max_exit_idx + 1):
        option_bar = option_bars_by_symbol_and_date.get(
            (selected_option.option_symbol, bars[day_idx].trade_date)
        )
        if option_bar is None:
            continue
        last_observed_bar = option_bar

        hit_target = (
            strategy.take_profit_pct is not None and option_bar.high >= target_price
        )
        hit_stop = (
            strategy.stop_loss_pct is not None and option_bar.low <= stop_price
        )

        if hit_target and hit_stop:
            return option_bar.trade_date, stop_price, -float(strategy.stop_loss_pct or 0.0), "stop_loss_conflict"
        if hit_target:
            return option_bar.trade_date, target_price, float(strategy.take_profit_pct or 0.0), "take_profit"
        if hit_stop:
            return option_bar.trade_date, stop_price, -float(strategy.stop_loss_pct or 0.0), "stop_loss"

    if last_observed_bar is None:
        return None
    return (
        last_observed_bar.trade_date,
        last_observed_bar.close,
        ((last_observed_bar.close / entry_price) - 1.0) * 100.0,
        "time_stop",
    )


def backtest_option_strategies(
    db_path: str | Path,
    tickers_file: str | Path,
    start_date: str | None = None,
    end_date: str | None = None,
    strategy_definitions: list[OptionStrategyDefinition] | None = None,
) -> tuple[list[StrategySummary], list[StrategyTrade]]:
    tickers = {
        ticker.removesuffix(".SA").upper()
        for ticker in load_tickers(tickers_file)
    }
    spot_bars = _load_spot_bars(db_path, allowed_tickers=tickers)
    option_bars_by_root_and_date, option_bars_by_symbol_and_date = _load_option_bars(db_path)
    preferred_root_mapping = _build_preferred_root_mapping(spot_bars)
    effective_strategies = strategy_definitions or build_option_strategy_definitions()

    end_day = _coerce_date(end_date) if end_date else date.today()
    start_day = _coerce_date(start_date) if start_date else end_day - timedelta(days=365)

    summaries: list[StrategySummary] = []
    trades: list[StrategyTrade] = []

    for strategy in effective_strategies:
        strategy_trade_returns: list[float] = []
        strategy_asset_moves: list[float] = []
        tickers_with_matches: set[str] = set()

        for underlying_root, ticker in preferred_root_mapping.items():
            bars = spot_bars.get(ticker, [])
            if len(bars) < 2:
                continue

            trade_dates = [_coerce_date(item.trade_date) for item in bars]
            close_prices = [item.close for item in bars]
            close_prefix_sums = _build_prefix_sums(close_prices)

            for idx in range(1, len(bars)):
                current_bar = bars[idx]
                entry_day = trade_dates[idx]

                if entry_day < start_day or entry_day > (end_day - timedelta(days=strategy.holding_days)):
                    continue

                exit_idx = _find_exit_index(
                    trade_dates=trade_dates,
                    start_idx=idx,
                    holding_days=strategy.holding_days,
                )
                if exit_idx is None:
                    continue

                exit_bar = bars[exit_idx]
                exit_day = trade_dates[exit_idx]
                if exit_day > end_day:
                    continue

                matched, trigger_change_pct, spot_price = _resolve_underlying_signal(
                    bars=bars,
                    close_prices=close_prices,
                    close_prefix_sums=close_prefix_sums,
                    idx=idx,
                    strategy=strategy,
                )
                if not matched:
                    continue

                selected_option = _select_atm_option(
                    entry_date=current_bar.trade_date,
                    underlying_root=underlying_root,
                    spot_price=spot_price,
                    strategy=strategy,
                    option_bars_by_root_and_date=option_bars_by_root_and_date,
                    exit_date=exit_bar.trade_date,
                )
                if selected_option is None:
                    continue

                entry_price = (
                    selected_option.open
                    if strategy.entry_rule == "open"
                    else selected_option.close
                )
                if entry_price <= 0:
                    continue

                if strategy.exit_kind == "target_stop":
                    resolved_exit = _resolve_target_stop_exit(
                        bars=bars,
                        entry_idx=idx,
                        max_exit_idx=exit_idx,
                        selected_option=selected_option,
                        option_bars_by_symbol_and_date=option_bars_by_symbol_and_date,
                        strategy=strategy,
                        entry_price=entry_price,
                    )
                    if resolved_exit is None:
                        continue
                    resolved_exit_date, resolved_exit_price, option_gross_move_pct, exit_reason = resolved_exit
                    exit_price = resolved_exit_price
                    actual_exit_date = resolved_exit_date
                else:
                    exit_option_bar = option_bars_by_symbol_and_date.get(
                        (selected_option.option_symbol, exit_bar.trade_date)
                    )
                    if exit_option_bar is None or exit_option_bar.close <= 0:
                        continue
                    option_gross_move_pct = ((exit_option_bar.close / entry_price) - 1.0) * 100.0
                    exit_price = exit_option_bar.close
                    actual_exit_date = exit_bar.trade_date
                    exit_reason = "time_exit"

                trade_return_pct = _calculate_trade_return_pct(
                    entry_price=entry_price,
                    exit_price=exit_price,
                    round_trip_cost_pct=strategy.round_trip_cost_pct,
                )
                is_successful = trade_return_pct >= strategy.min_trade_return_pct

                strategy_trade_returns.append(trade_return_pct)
                strategy_asset_moves.append(option_gross_move_pct)
                tickers_with_matches.add(ticker)
                trades.append(
                    StrategyTrade(
                        strategy_code=strategy.code,
                        strategy_label=strategy.label,
                        family=strategy.family,
                        ticker=ticker,
                        trigger_date=current_bar.trade_date,
                        exit_date=actual_exit_date,
                        direction=f"long_{strategy.option_side}",
                        trigger_change_pct=trigger_change_pct,
                        entry_price=entry_price,
                        exit_price=exit_price,
                        asset_move_pct=option_gross_move_pct,
                        trade_return_pct=trade_return_pct,
                        is_profitable=trade_return_pct > 0,
                        is_successful=is_successful,
                        instrument_symbol=selected_option.option_symbol,
                        contract_expiration=selected_option.expiration_date,
                        dte_target_days=strategy.dte_target_days,
                        exit_reason=exit_reason,
                    )
                )

        if not strategy_trade_returns:
            continue

        profitable_trades = sum(1 for item in strategy_trade_returns if item > 0)
        successful_occurrences = sum(
            1 for item in strategy_trade_returns if item >= strategy.min_trade_return_pct
        )
        summaries.append(
            StrategySummary(
                code=strategy.code,
                label=strategy.label,
                family=strategy.family,
                setup_kind=strategy.setup_kind,
                trigger_direction=strategy.trigger_direction,
                threshold_pct=strategy.threshold_pct,
                trade_direction=f"long_{strategy.option_side}",
                entry_rule=strategy.entry_rule,
                exit_offset_days=strategy.holding_days,
                min_trade_return_pct=strategy.min_trade_return_pct,
                tickers_with_matches=len(tickers_with_matches),
                total_occurrences=len(strategy_trade_returns),
                successful_occurrences=successful_occurrences,
                success_rate_pct=(successful_occurrences / len(strategy_trade_returns)) * 100.0,
                average_asset_move_pct=sum(strategy_asset_moves) / len(strategy_asset_moves),
                median_asset_move_pct=median(strategy_asset_moves),
                profitable_trades=profitable_trades,
                profitable_trade_rate_pct=(profitable_trades / len(strategy_trade_returns)) * 100.0,
                average_trade_return_pct=sum(strategy_trade_returns) / len(strategy_trade_returns),
                median_trade_return_pct=median(strategy_trade_returns),
                net_trade_return_pct=sum(strategy_trade_returns),
                cumulative_return_pct=(
                    prod(1.0 + (item / 100.0) for item in strategy_trade_returns) - 1.0
                ) * 100.0,
                profit_factor=_calculate_profit_factor(strategy_trade_returns),
            )
        )

    sorted_summaries = sorted(
        summaries,
        key=lambda item: (
            item.net_trade_return_pct,
            item.profit_factor if item.profit_factor is not None else float("-inf"),
            item.average_trade_return_pct,
            item.success_rate_pct,
        ),
        reverse=True,
    )
    sorted_trades = sorted(
        trades,
        key=lambda item: (item.strategy_code, item.trigger_date, item.ticker),
    )
    return sorted_summaries, sorted_trades
