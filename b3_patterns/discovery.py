from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from math import exp, log, sqrt
from pathlib import Path

from .models import (
    DiscoveryPatternSummary,
    OptionDiscoveryTemplate,
    SpotQuoteBar,
    StrategyRegistryEntry,
    StrategyTrade,
)
from .options import (
    _build_preferred_root_mapping,
    _coerce_date,
    _load_option_bars,
    _load_spot_bars,
)
from .tickers import load_tickers


@dataclass(slots=True)
class DiscoveryFeature:
    key: str
    label: str
    pairable: bool


@dataclass(slots=True)
class _PatternAccumulator:
    code: str
    label: str
    family: str
    template_code: str
    template_label: str
    option_side: str
    dte_target_days: int
    trade_direction: str
    entry_rule: str
    exit_offset_days: int
    state_size: int
    state_signature: str
    feature_keys: str
    min_trade_return_pct: float
    tickers: set[str]
    total_occurrences: int = 0
    successful_occurrences: int = 0
    profitable_trades: int = 0
    sum_asset_move_pct: float = 0.0
    sum_trade_return_pct: float = 0.0
    gross_profit_pct: float = 0.0
    gross_loss_pct: float = 0.0
    log_cumulative_multiplier: float = 0.0
    zero_cumulative_multiplier: bool = False
    first_trade_date: str | None = None
    last_trade_date: str | None = None


FEATURE_LIBRARY: list[DiscoveryFeature] = [
    DiscoveryFeature("gap_pct", "Gap da abertura", True),
    DiscoveryFeature("gap_vs_range20", "Gap vs range medio 20D", True),
    DiscoveryFeature("prev_ret_1d", "Retorno de D-1", True),
    DiscoveryFeature("prev_ret_2d", "Retorno acumulado 2D", False),
    DiscoveryFeature("prev_ret_3d", "Retorno acumulado 3D", True),
    DiscoveryFeature("prev_ret_5d", "Retorno acumulado 5D", True),
    DiscoveryFeature("prev_ret_1d_z20", "Retorno D-1 em z-score 20D", False),
    DiscoveryFeature("close_vs_sma5", "Fechamento vs MM5", False),
    DiscoveryFeature("close_vs_sma20", "Fechamento vs MM20", True),
    DiscoveryFeature("sma5_vs_sma20", "MM5 vs MM20", False),
    DiscoveryFeature("sma20_slope_5d", "Inclinacao da MM20 em 5D", False),
    DiscoveryFeature("volume_ratio_20d", "Volume vs media 20D", True),
    DiscoveryFeature("range_ratio_20d", "Range vs media 20D", False),
    DiscoveryFeature("compression_3v20", "Compressao 3D vs 20D", True),
    DiscoveryFeature("vol_ratio_5v20", "Volatilidade 5D vs 20D", False),
    DiscoveryFeature("body_ratio_prev", "Corpo do candle anterior", False),
    DiscoveryFeature("close_location_prev", "Posicao do fechamento anterior", False),
    DiscoveryFeature("lower_wick_ratio_prev", "Pavio inferior anterior", False),
    DiscoveryFeature("upper_wick_ratio_prev", "Pavio superior anterior", False),
    DiscoveryFeature("range_position_20d", "Posicao no range de 20D", False),
]

FEATURE_BY_KEY = {item.key: item for item in FEATURE_LIBRARY}


def list_discovery_features() -> list[DiscoveryFeature]:
    return list(FEATURE_LIBRARY)


def build_option_discovery_templates(
    round_trip_cost_pct: float = 0.0,
    dte_targets: list[int] | None = None,
) -> list[OptionDiscoveryTemplate]:
    targets = dte_targets or [7, 15, 30]
    normalized_targets = sorted({int(item) for item in targets if int(item) > 0})
    templates: list[OptionDiscoveryTemplate] = []

    for dte_target_days in normalized_targets:
        templates.append(
            OptionDiscoveryTemplate(
                code=f"disc_call_atm_{dte_target_days}dte_d0",
                label=f"Discovery: CALL ATM {dte_target_days} DTE, open -> close D0",
                family="option_state_discovery",
                option_side="call",
                dte_target_days=dte_target_days,
                trade_direction="long_call",
                round_trip_cost_pct=round_trip_cost_pct,
            )
        )
        templates.append(
            OptionDiscoveryTemplate(
                code=f"disc_put_atm_{dte_target_days}dte_d0",
                label=f"Discovery: PUT ATM {dte_target_days} DTE, open -> close D0",
                family="option_state_discovery",
                option_side="put",
                dte_target_days=dte_target_days,
                trade_direction="long_put",
                round_trip_cost_pct=round_trip_cost_pct,
            )
        )

    return templates


def _calculate_profit_factor(gross_profit_pct: float, gross_loss_pct: float) -> float | None:
    if gross_loss_pct == 0:
        if gross_profit_pct == 0:
            return None
        return float("inf")
    return gross_profit_pct / gross_loss_pct


def _prefix_sums(values: list[float]) -> list[float]:
    totals = [0.0]
    running_total = 0.0
    for value in values:
        running_total += value
        totals.append(running_total)
    return totals


def _prefix_sums_sq(values: list[float]) -> list[float]:
    totals = [0.0]
    running_total = 0.0
    for value in values:
        running_total += value * value
        totals.append(running_total)
    return totals


def _window_average(prefix_sums: list[float], end_idx: int, window_days: int) -> float | None:
    start_idx = end_idx - window_days + 1
    if start_idx < 0:
        return None
    return (prefix_sums[end_idx + 1] - prefix_sums[start_idx]) / window_days


def _window_std(
    prefix_sums: list[float],
    prefix_sums_sq: list[float],
    end_idx: int,
    window_days: int,
) -> float | None:
    start_idx = end_idx - window_days + 1
    if start_idx < 0:
        return None
    total = prefix_sums[end_idx + 1] - prefix_sums[start_idx]
    total_sq = prefix_sums_sq[end_idx + 1] - prefix_sums_sq[start_idx]
    mean = total / window_days
    variance = max((total_sq / window_days) - (mean * mean), 0.0)
    return sqrt(variance)


def _bucket_signed_pct(value: float) -> tuple[str, str]:
    if value <= -6.0:
        return "shock_dn", "choque de baixa"
    if value <= -3.0:
        return "dn_3_6", "queda de 3% a 6%"
    if value <= -1.0:
        return "dn_1_3", "queda de 1% a 3%"
    if value < 1.0:
        return "flat", "neutro"
    if value < 3.0:
        return "up_1_3", "alta de 1% a 3%"
    if value < 6.0:
        return "up_3_6", "alta de 3% a 6%"
    return "shock_up", "choque de alta"


def _bucket_signed_distance(value: float) -> tuple[str, str]:
    if value <= -8.0:
        return "far_dn", "muito abaixo"
    if value <= -4.0:
        return "dn_4_8", "abaixo forte"
    if value <= -1.0:
        return "dn_1_4", "abaixo leve"
    if value < 1.0:
        return "near", "perto do equilibrio"
    if value < 4.0:
        return "up_1_4", "acima leve"
    if value < 8.0:
        return "up_4_8", "acima forte"
    return "far_up", "muito acima"


def _bucket_signed_relative(value: float) -> tuple[str, str]:
    if value <= -2.0:
        return "rel_shock_dn", "muito abaixo do normal"
    if value <= -1.0:
        return "rel_dn", "abaixo do normal"
    if value <= -0.3:
        return "rel_soft_dn", "levemente abaixo"
    if value < 0.3:
        return "rel_near", "perto do normal"
    if value < 1.0:
        return "rel_soft_up", "levemente acima"
    if value < 2.0:
        return "rel_up", "acima do normal"
    return "rel_shock_up", "muito acima do normal"


def _bucket_ratio(value: float) -> tuple[str, str]:
    if value < 0.7:
        return "very_low", "muito baixo"
    if value < 0.9:
        return "low", "baixo"
    if value < 1.1:
        return "normal", "normal"
    if value < 1.4:
        return "high", "alto"
    if value < 1.8:
        return "very_high", "muito alto"
    return "extreme", "extremo"


def _bucket_fraction(value: float) -> tuple[str, str]:
    if value < 0.15:
        return "very_low", "muito baixo"
    if value < 0.35:
        return "low", "baixo"
    if value < 0.65:
        return "mid", "medio"
    if value < 0.85:
        return "high", "alto"
    return "very_high", "muito alto"


def _bucket_close_position(value: float) -> tuple[str, str]:
    if value < 0.15:
        return "near_low", "fechou perto da minima"
    if value < 0.35:
        return "lower_band", "fechou na metade inferior"
    if value < 0.65:
        return "middle", "fechou no meio"
    if value < 0.85:
        return "upper_band", "fechou na metade superior"
    return "near_high", "fechou perto da maxima"


def _rolling_range_position(
    highs: list[float],
    lows: list[float],
    close_price: float,
    end_idx: int,
) -> float | None:
    start_idx = end_idx - 19
    if start_idx < 0:
        return None
    high_20d = max(highs[start_idx : end_idx + 1])
    low_20d = min(lows[start_idx : end_idx + 1])
    if high_20d == low_20d:
        return 0.5
    return (close_price - low_20d) / (high_20d - low_20d)


def _build_feature_states(
    bars: list[SpotQuoteBar],
    idx: int,
    close_prefix_sums: list[float],
    volume_prefix_sums: list[float],
    range_pct_prefix_sums: list[float],
    daily_return_prefix_sums: list[float],
    daily_return_prefix_sums_sq: list[float],
    highs: list[float],
    lows: list[float],
    closes: list[float],
    opens: list[float],
    volumes: list[float],
) -> dict[str, tuple[str, str]]:
    prev_idx = idx - 1
    if prev_idx < 1:
        return {}

    states: dict[str, tuple[str, str]] = {}
    current_open = opens[idx]
    previous_close = closes[prev_idx]
    if previous_close <= 0:
        return {}

    gap_pct = ((current_open / previous_close) - 1.0) * 100.0
    states["gap_pct"] = _bucket_signed_pct(gap_pct)

    daily_range = highs[prev_idx] - lows[prev_idx]
    prev_range_pct = (daily_range / previous_close) * 100.0 if previous_close > 0 else 0.0
    avg_range_20d = _window_average(range_pct_prefix_sums, prev_idx, 20)
    if avg_range_20d is not None and avg_range_20d > 0:
        gap_vs_range20 = gap_pct / avg_range_20d
        states["gap_vs_range20"] = _bucket_signed_relative(gap_vs_range20)
        states["range_ratio_20d"] = _bucket_ratio(prev_range_pct / avg_range_20d)

    def _ret_lookback(window_days: int) -> float | None:
        start_idx = prev_idx - window_days
        if start_idx < 0 or closes[start_idx] <= 0:
            return None
        return ((closes[prev_idx] / closes[start_idx]) - 1.0) * 100.0

    prev_ret_1d = _ret_lookback(1)
    prev_ret_2d = _ret_lookback(2)
    prev_ret_3d = _ret_lookback(3)
    prev_ret_5d = _ret_lookback(5)
    if prev_ret_1d is not None:
        states["prev_ret_1d"] = _bucket_signed_pct(prev_ret_1d)
    if prev_ret_2d is not None:
        states["prev_ret_2d"] = _bucket_signed_pct(prev_ret_2d)
    if prev_ret_3d is not None:
        states["prev_ret_3d"] = _bucket_signed_pct(prev_ret_3d)
    if prev_ret_5d is not None:
        states["prev_ret_5d"] = _bucket_signed_pct(prev_ret_5d)

    return_std_20d = _window_std(
        daily_return_prefix_sums,
        daily_return_prefix_sums_sq,
        prev_idx,
        20,
    )
    if prev_ret_1d is not None and return_std_20d is not None and return_std_20d > 0:
        states["prev_ret_1d_z20"] = _bucket_signed_relative(prev_ret_1d / return_std_20d)

    sma_5 = _window_average(close_prefix_sums, prev_idx, 5)
    sma_20 = _window_average(close_prefix_sums, prev_idx, 20)
    sma_20_prev5 = _window_average(close_prefix_sums, prev_idx - 5, 20)
    if sma_5 is not None and sma_5 > 0:
        states["close_vs_sma5"] = _bucket_signed_distance(((closes[prev_idx] / sma_5) - 1.0) * 100.0)
    if sma_20 is not None and sma_20 > 0:
        states["close_vs_sma20"] = _bucket_signed_distance(((closes[prev_idx] / sma_20) - 1.0) * 100.0)
    if sma_5 is not None and sma_20 is not None and sma_20 > 0:
        states["sma5_vs_sma20"] = _bucket_signed_distance(((sma_5 / sma_20) - 1.0) * 100.0)
    if sma_20 is not None and sma_20_prev5 is not None and sma_20_prev5 > 0:
        states["sma20_slope_5d"] = _bucket_signed_distance(((sma_20 / sma_20_prev5) - 1.0) * 100.0)

    avg_volume_20d = _window_average(volume_prefix_sums, prev_idx, 20)
    if avg_volume_20d is not None and avg_volume_20d > 0:
        states["volume_ratio_20d"] = _bucket_ratio(volumes[prev_idx] / avg_volume_20d)

    avg_range_3d = _window_average(range_pct_prefix_sums, prev_idx, 3)
    if avg_range_3d is not None and avg_range_20d is not None and avg_range_20d > 0:
        states["compression_3v20"] = _bucket_ratio(avg_range_3d / avg_range_20d)

    return_std_5d = _window_std(
        daily_return_prefix_sums,
        daily_return_prefix_sums_sq,
        prev_idx,
        5,
    )
    if return_std_5d is not None and return_std_20d is not None and return_std_20d > 0:
        states["vol_ratio_5v20"] = _bucket_ratio(return_std_5d / return_std_20d)

    if daily_range > 0:
        body_ratio = abs(closes[prev_idx] - opens[prev_idx]) / daily_range
        close_location = (closes[prev_idx] - lows[prev_idx]) / daily_range
        lower_wick = (min(opens[prev_idx], closes[prev_idx]) - lows[prev_idx]) / daily_range
        upper_wick = (highs[prev_idx] - max(opens[prev_idx], closes[prev_idx])) / daily_range
        states["body_ratio_prev"] = _bucket_fraction(body_ratio)
        states["close_location_prev"] = _bucket_close_position(close_location)
        states["lower_wick_ratio_prev"] = _bucket_fraction(lower_wick)
        states["upper_wick_ratio_prev"] = _bucket_fraction(upper_wick)

    range_position_20d = _rolling_range_position(
        highs=highs,
        lows=lows,
        close_price=closes[prev_idx],
        end_idx=prev_idx,
    )
    if range_position_20d is not None:
        states["range_position_20d"] = _bucket_close_position(range_position_20d)

    return states


def _select_template_option(
    *,
    trade_date: str,
    underlying_root: str,
    option_side: str,
    dte_target_days: int,
    spot_price: float,
    option_bars_by_root_and_date: dict[str, dict[str, list]],
) -> object | None:
    candidates = option_bars_by_root_and_date.get(underlying_root, {}).get(trade_date, [])
    if not candidates:
        return None

    entry_day = _coerce_date(trade_date)
    eligible_candidates: list[tuple[object, int]] = []
    for bar in candidates:
        if bar.option_side != option_side:
            continue
        expiration_day = _coerce_date(bar.expiration_date)
        dte_days = (expiration_day - entry_day).days
        if dte_days < dte_target_days:
            continue
        if bar.open <= 0 or bar.close <= 0:
            continue
        eligible_candidates.append((bar, dte_days))

    if not eligible_candidates:
        return None

    eligible_candidates.sort(
        key=lambda item: (
            item[1] - dte_target_days,
            abs(item[0].strike_price - spot_price),
            -item[0].trade_count,
            -item[0].volume,
            item[0].option_symbol,
        )
    )
    return eligible_candidates[0][0]


def _pattern_items(
    feature_states: dict[str, tuple[str, str]],
    max_pattern_size: int,
) -> list[tuple[tuple[str, str, str], ...]]:
    ordered_single_items = [
        (feature.key, *feature_states[feature.key])
        for feature in FEATURE_LIBRARY
        if feature.key in feature_states
    ]
    patterns = [((feature_key, code, label),) for feature_key, code, label in ordered_single_items]
    if max_pattern_size < 2:
        return patterns

    pairable_items = [
        (feature.key, *feature_states[feature.key])
        for feature in FEATURE_LIBRARY
        if feature.key in feature_states and feature.pairable
    ]
    for left_idx in range(len(pairable_items)):
        for right_idx in range(left_idx + 1, len(pairable_items)):
            patterns.append((pairable_items[left_idx], pairable_items[right_idx]))
    return patterns


def _build_pattern_identity(
    template: OptionDiscoveryTemplate,
    pattern_items: tuple[tuple[str, str, str], ...],
) -> tuple[str, str, str, str]:
    state_size = len(pattern_items)
    code_suffix = "__".join(f"{key}={code}" for key, code, _ in pattern_items)
    state_signature = "; ".join(
        f"{FEATURE_BY_KEY[key].label}: {value_label}" for key, _, value_label in pattern_items
    )
    feature_keys = ",".join(key for key, _, _ in pattern_items)
    label = f"{template.label} | {state_signature}"
    code = f"{template.code}__{state_size}f__{code_suffix}"
    return code, label, state_signature, feature_keys


def _iter_discovery_samples(
    *,
    db_path: str | Path,
    tickers_file: str | Path,
    start_date: str,
    end_date: str,
    template_definitions: list[OptionDiscoveryTemplate],
):
    tickers = {ticker.removesuffix(".SA").upper() for ticker in load_tickers(tickers_file)}
    spot_bars = _load_spot_bars(db_path, allowed_tickers=tickers)
    option_bars_by_root_and_date, _ = _load_option_bars(db_path)
    preferred_root_mapping = _build_preferred_root_mapping(spot_bars)

    start_day = _coerce_date(start_date)
    end_day = _coerce_date(end_date)

    for underlying_root, ticker in preferred_root_mapping.items():
        bars = spot_bars.get(ticker, [])
        if len(bars) < 25:
            continue

        opens = [item.open for item in bars]
        highs = [item.high for item in bars]
        lows = [item.low for item in bars]
        closes = [item.close for item in bars]
        volumes = [float(item.volume) for item in bars]
        range_pcts = [
            0.0 if item.close <= 0 else ((item.high - item.low) / item.close) * 100.0
            for item in bars
        ]
        daily_returns = [0.0]
        for bar_idx in range(1, len(bars)):
            previous_close = closes[bar_idx - 1]
            if previous_close <= 0:
                daily_returns.append(0.0)
                continue
            daily_returns.append(((closes[bar_idx] / previous_close) - 1.0) * 100.0)

        close_prefix_sums = _prefix_sums(closes)
        volume_prefix_sums = _prefix_sums(volumes)
        range_pct_prefix_sums = _prefix_sums(range_pcts)
        daily_return_prefix_sums = _prefix_sums(daily_returns)
        daily_return_prefix_sums_sq = _prefix_sums_sq(daily_returns)

        for idx in range(1, len(bars)):
            current_bar = bars[idx]
            trade_day = _coerce_date(current_bar.trade_date)
            if trade_day < start_day or trade_day > end_day or current_bar.open <= 0:
                continue

            feature_states = _build_feature_states(
                bars=bars,
                idx=idx,
                close_prefix_sums=close_prefix_sums,
                volume_prefix_sums=volume_prefix_sums,
                range_pct_prefix_sums=range_pct_prefix_sums,
                daily_return_prefix_sums=daily_return_prefix_sums,
                daily_return_prefix_sums_sq=daily_return_prefix_sums_sq,
                highs=highs,
                lows=lows,
                closes=closes,
                opens=opens,
                volumes=volumes,
            )
            if not feature_states:
                continue

            gap_pct = ((current_bar.open / closes[idx - 1]) - 1.0) * 100.0
            for template in template_definitions:
                selected_option = _select_template_option(
                    trade_date=current_bar.trade_date,
                    underlying_root=underlying_root,
                    option_side=template.option_side,
                    dte_target_days=template.dte_target_days,
                    spot_price=current_bar.open,
                    option_bars_by_root_and_date=option_bars_by_root_and_date,
                )
                if selected_option is None:
                    continue

                entry_price = selected_option.open
                exit_price = selected_option.close
                asset_move_pct = ((exit_price / entry_price) - 1.0) * 100.0
                trade_return_pct = asset_move_pct - template.round_trip_cost_pct
                yield {
                    "template": template,
                    "ticker": ticker,
                    "trade_date": current_bar.trade_date,
                    "gap_pct": gap_pct,
                    "feature_states": feature_states,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "asset_move_pct": asset_move_pct,
                    "trade_return_pct": trade_return_pct,
                    "instrument_symbol": selected_option.option_symbol,
                    "contract_expiration": selected_option.expiration_date,
                }


def _update_accumulator(
    accumulator: _PatternAccumulator,
    *,
    ticker: str,
    trade_date: str,
    asset_move_pct: float,
    trade_return_pct: float,
) -> None:
    accumulator.total_occurrences += 1
    accumulator.tickers.add(ticker)
    accumulator.sum_asset_move_pct += asset_move_pct
    accumulator.sum_trade_return_pct += trade_return_pct
    if trade_return_pct >= accumulator.min_trade_return_pct:
        accumulator.successful_occurrences += 1
    if trade_return_pct > 0:
        accumulator.profitable_trades += 1
        accumulator.gross_profit_pct += trade_return_pct
    elif trade_return_pct < 0:
        accumulator.gross_loss_pct += -trade_return_pct

    multiplier = 1.0 + (trade_return_pct / 100.0)
    if multiplier <= 0:
        accumulator.zero_cumulative_multiplier = True
    else:
        accumulator.log_cumulative_multiplier += log(multiplier)

    if accumulator.first_trade_date is None or trade_date < accumulator.first_trade_date:
        accumulator.first_trade_date = trade_date
    if accumulator.last_trade_date is None or trade_date > accumulator.last_trade_date:
        accumulator.last_trade_date = trade_date


def mine_option_discovery_patterns(
    *,
    db_path: str | Path,
    tickers_file: str | Path,
    start_date: str,
    end_date: str,
    min_trade_return_pct: float = 0.0,
    template_definitions: list[OptionDiscoveryTemplate] | None = None,
    max_pattern_size: int = 2,
    known_codes: set[str] | None = None,
) -> list[DiscoveryPatternSummary]:
    effective_templates = template_definitions or build_option_discovery_templates()
    ignored_codes = known_codes or set()
    accumulators: dict[str, _PatternAccumulator] = {}
    pattern_items_cache: dict[tuple[tuple[tuple[str, str, str], ...], int], list[tuple[tuple[str, str, str], ...]]] = {}
    identity_cache: dict[tuple[str, tuple[tuple[str, str, str], ...]], tuple[str, str, str, str]] = {}

    for sample in _iter_discovery_samples(
        db_path=db_path,
        tickers_file=tickers_file,
        start_date=start_date,
        end_date=end_date,
        template_definitions=effective_templates,
    ):
        feature_signature = tuple(
            (feature.key, *sample["feature_states"][feature.key])
            for feature in FEATURE_LIBRARY
            if feature.key in sample["feature_states"]
        )
        pattern_cache_key = (feature_signature, max_pattern_size)
        pattern_items_list = pattern_items_cache.get(pattern_cache_key)
        if pattern_items_list is None:
            pattern_items_list = _pattern_items(
                feature_states=sample["feature_states"],
                max_pattern_size=max_pattern_size,
            )
            pattern_items_cache[pattern_cache_key] = pattern_items_list

        for pattern_items in pattern_items_list:
            identity_cache_key = (sample["template"].code, pattern_items)
            identity = identity_cache.get(identity_cache_key)
            if identity is None:
                identity = _build_pattern_identity(
                    template=sample["template"],
                    pattern_items=pattern_items,
                )
                identity_cache[identity_cache_key] = identity
            code, label, state_signature, feature_keys = identity
            if code in ignored_codes:
                continue
            accumulator = accumulators.get(code)
            if accumulator is None:
                accumulator = _PatternAccumulator(
                    code=code,
                    label=label,
                    family=sample["template"].family,
                    template_code=sample["template"].code,
                    template_label=sample["template"].label,
                    option_side=sample["template"].option_side,
                    dte_target_days=sample["template"].dte_target_days,
                    trade_direction=sample["template"].trade_direction,
                    entry_rule=sample["template"].entry_rule,
                    exit_offset_days=sample["template"].holding_days,
                    state_size=len(pattern_items),
                    state_signature=state_signature,
                    feature_keys=feature_keys,
                    min_trade_return_pct=min_trade_return_pct,
                    tickers=set(),
                )
                accumulators[code] = accumulator
            _update_accumulator(
                accumulator,
                ticker=sample["ticker"],
                trade_date=sample["trade_date"],
                asset_move_pct=sample["asset_move_pct"],
                trade_return_pct=sample["trade_return_pct"],
            )

    summaries: list[DiscoveryPatternSummary] = []
    for accumulator in accumulators.values():
        profit_factor = _calculate_profit_factor(
            gross_profit_pct=accumulator.gross_profit_pct,
            gross_loss_pct=accumulator.gross_loss_pct,
        )
        cumulative_return_pct = (
            -100.0
            if accumulator.zero_cumulative_multiplier
            else (exp(accumulator.log_cumulative_multiplier) - 1.0) * 100.0
        )
        summaries.append(
            DiscoveryPatternSummary(
                code=accumulator.code,
                label=accumulator.label,
                family=accumulator.family,
                template_code=accumulator.template_code,
                template_label=accumulator.template_label,
                option_side=accumulator.option_side,
                dte_target_days=accumulator.dte_target_days,
                trade_direction=accumulator.trade_direction,
                entry_rule=accumulator.entry_rule,
                exit_offset_days=accumulator.exit_offset_days,
                state_size=accumulator.state_size,
                state_signature=accumulator.state_signature,
                feature_keys=accumulator.feature_keys,
                min_trade_return_pct=accumulator.min_trade_return_pct,
                tickers_with_matches=len(accumulator.tickers),
                total_occurrences=accumulator.total_occurrences,
                successful_occurrences=accumulator.successful_occurrences,
                success_rate_pct=(accumulator.successful_occurrences / accumulator.total_occurrences) * 100.0,
                profitable_trades=accumulator.profitable_trades,
                profitable_trade_rate_pct=(accumulator.profitable_trades / accumulator.total_occurrences) * 100.0,
                average_asset_move_pct=accumulator.sum_asset_move_pct / accumulator.total_occurrences,
                average_trade_return_pct=accumulator.sum_trade_return_pct / accumulator.total_occurrences,
                net_trade_return_pct=accumulator.sum_trade_return_pct,
                cumulative_return_pct=cumulative_return_pct,
                profit_factor=profit_factor,
                first_trade_date=accumulator.first_trade_date or "",
                last_trade_date=accumulator.last_trade_date or "",
            )
        )

    return sorted(
        summaries,
        key=lambda item: (
            item.net_trade_return_pct,
            item.profit_factor if item.profit_factor is not None else float("-inf"),
            item.average_trade_return_pct,
            item.total_occurrences,
        ),
        reverse=True,
    )


def qualify_discovery_summary(
    summary: DiscoveryPatternSummary,
    *,
    min_success_rate_pct: float = 0.0,
    min_profit_factor: float = 1.15,
    min_average_trade_return_pct: float = 0.0,
    min_trades: int = 50,
    min_tickers: int = 5,
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
    if summary.tickers_with_matches < min_tickers:
        reasons.append(f"tickers<{min_tickers}")
    if require_positive_net and summary.net_trade_return_pct <= 0:
        reasons.append("net<=0")
    return not reasons, reasons


def split_discovery_results(
    summaries: list[DiscoveryPatternSummary],
    *,
    min_success_rate_pct: float = 0.0,
    min_profit_factor: float = 1.15,
    min_average_trade_return_pct: float = 0.0,
    min_trades: int = 50,
    min_tickers: int = 5,
    require_positive_net: bool = True,
) -> tuple[list[DiscoveryPatternSummary], list[DiscoveryPatternSummary], dict[str, list[str]]]:
    approved: list[DiscoveryPatternSummary] = []
    rejected: list[DiscoveryPatternSummary] = []
    rejection_reasons: dict[str, list[str]] = {}

    for summary in summaries:
        is_approved, reasons = qualify_discovery_summary(
            summary=summary,
            min_success_rate_pct=min_success_rate_pct,
            min_profit_factor=min_profit_factor,
            min_average_trade_return_pct=min_average_trade_return_pct,
            min_trades=min_trades,
            min_tickers=min_tickers,
            require_positive_net=require_positive_net,
        )
        if is_approved:
            approved.append(summary)
        else:
            rejected.append(summary)
            rejection_reasons[summary.code] = reasons

    return approved, rejected, rejection_reasons


def build_discovery_registry_entries(
    approved_summaries: list[DiscoveryPatternSummary],
    rejected_summaries: list[DiscoveryPatternSummary],
    rejection_reasons: dict[str, list[str]],
) -> list[StrategyRegistryEntry]:
    from datetime import datetime

    evaluated_at = datetime.now().astimezone().isoformat(timespec="seconds")
    entries: list[StrategyRegistryEntry] = []

    for summary in approved_summaries:
        entries.append(
            StrategyRegistryEntry(
                code=summary.code,
                label=summary.label,
                family=summary.family,
                setup_kind=f"state_{summary.state_size}f",
                trigger_direction=summary.option_side,
                threshold_pct=float(summary.state_size),
                trade_direction=summary.trade_direction,
                entry_rule=summary.entry_rule,
                exit_offset_days=summary.exit_offset_days,
                min_trade_return_pct=summary.min_trade_return_pct,
                status="approved",
                rejection_reasons="",
                tested_at=evaluated_at,
                total_occurrences=summary.total_occurrences,
                success_rate_pct=summary.success_rate_pct,
                profitable_trade_rate_pct=summary.profitable_trade_rate_pct,
                average_trade_return_pct=summary.average_trade_return_pct,
                net_trade_return_pct=summary.net_trade_return_pct,
                profit_factor=summary.profit_factor,
            )
        )

    for summary in rejected_summaries:
        entries.append(
            StrategyRegistryEntry(
                code=summary.code,
                label=summary.label,
                family=summary.family,
                setup_kind=f"state_{summary.state_size}f",
                trigger_direction=summary.option_side,
                threshold_pct=float(summary.state_size),
                trade_direction=summary.trade_direction,
                entry_rule=summary.entry_rule,
                exit_offset_days=summary.exit_offset_days,
                min_trade_return_pct=summary.min_trade_return_pct,
                status="rejected",
                rejection_reasons=";".join(rejection_reasons.get(summary.code, [])),
                tested_at=evaluated_at,
                total_occurrences=summary.total_occurrences,
                success_rate_pct=summary.success_rate_pct,
                profitable_trade_rate_pct=summary.profitable_trade_rate_pct,
                average_trade_return_pct=summary.average_trade_return_pct,
                net_trade_return_pct=summary.net_trade_return_pct,
                profit_factor=summary.profit_factor,
            )
        )

    return sorted(entries, key=lambda item: (item.status, item.code))


def collect_discovery_pattern_trades(
    *,
    db_path: str | Path,
    tickers_file: str | Path,
    start_date: str,
    end_date: str,
    approved_summaries: list[DiscoveryPatternSummary],
    template_definitions: list[OptionDiscoveryTemplate] | None = None,
    max_pattern_size: int = 2,
) -> list[StrategyTrade]:
    effective_templates = template_definitions or build_option_discovery_templates()
    approved_by_code = {item.code: item for item in approved_summaries}
    if not approved_by_code:
        return []

    trades: list[StrategyTrade] = []
    pattern_items_cache: dict[tuple[tuple[tuple[str, str, str], ...], int], list[tuple[tuple[str, str, str], ...]]] = {}
    identity_cache: dict[tuple[str, tuple[tuple[str, str, str], ...]], str] = {}
    for sample in _iter_discovery_samples(
        db_path=db_path,
        tickers_file=tickers_file,
        start_date=start_date,
        end_date=end_date,
        template_definitions=effective_templates,
    ):
        feature_signature = tuple(
            (feature.key, *sample["feature_states"][feature.key])
            for feature in FEATURE_LIBRARY
            if feature.key in sample["feature_states"]
        )
        pattern_cache_key = (feature_signature, max_pattern_size)
        pattern_items_list = pattern_items_cache.get(pattern_cache_key)
        if pattern_items_list is None:
            pattern_items_list = _pattern_items(
                feature_states=sample["feature_states"],
                max_pattern_size=max_pattern_size,
            )
            pattern_items_cache[pattern_cache_key] = pattern_items_list

        for pattern_items in pattern_items_list:
            identity_cache_key = (sample["template"].code, pattern_items)
            code = identity_cache.get(identity_cache_key)
            if code is None:
                code, _, _, _ = _build_pattern_identity(
                    template=sample["template"],
                    pattern_items=pattern_items,
                )
                identity_cache[identity_cache_key] = code
            summary = approved_by_code.get(code)
            if summary is None:
                continue
            trades.append(
                StrategyTrade(
                    strategy_code=summary.code,
                    strategy_label=summary.label,
                    family=summary.family,
                    ticker=sample["ticker"],
                    trigger_date=sample["trade_date"],
                    exit_date=sample["trade_date"],
                    direction=summary.trade_direction,
                    trigger_change_pct=sample["gap_pct"],
                    entry_price=sample["entry_price"],
                    exit_price=sample["exit_price"],
                    asset_move_pct=sample["asset_move_pct"],
                    trade_return_pct=sample["trade_return_pct"],
                    is_profitable=sample["trade_return_pct"] > 0,
                    is_successful=sample["trade_return_pct"] >= summary.min_trade_return_pct,
                    instrument_symbol=sample["instrument_symbol"],
                    contract_expiration=sample["contract_expiration"],
                    dte_target_days=summary.dte_target_days,
                    exit_reason="time_exit",
                )
            )

    return sorted(trades, key=lambda item: (item.strategy_code, item.trigger_date, item.ticker))


def default_discovery_window(
    *,
    start_date: str | None,
    end_date: str | None,
) -> tuple[str, str]:
    end_day = _coerce_date(end_date) if end_date else date.today()
    start_day = _coerce_date(start_date) if start_date else end_day - timedelta(days=365)
    return start_day.isoformat(), end_day.isoformat()
