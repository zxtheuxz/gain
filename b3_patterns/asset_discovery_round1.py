from __future__ import annotations

import csv
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from itertools import combinations
from math import exp, log, sqrt
from pathlib import Path
from time import monotonic
from typing import Callable

from .analysis import filter_strategy_ticker_results, summarize_trades_by_ticker
from .models import SpotQuoteBar, StrategyRegistryEntry, StrategyTickerSummary, StrategyTrade
from .options import _load_spot_bars
from .tickers import load_tickers


@dataclass(slots=True, frozen=True)
class AssetDiscoveryFeature:
    key: str
    label: str
    pairable: bool = True


@dataclass(slots=True, frozen=True)
class AssetDiscoveryTemplate:
    code: str
    label: str
    family: str
    trade_direction: str
    entry_rule: str
    take_profit_pct: float
    stop_loss_pct: float
    time_cap_days: int
    round_trip_cost_pct: float = 0.0
    exit_mode: str = "percent"  # "percent" or "atr"
    atr_target_mult: float = 0.0
    atr_stop_mult: float = 0.0


@dataclass(slots=True)
class AssetDiscoveryPatternSummary:
    code: str
    label: str
    family: str
    template_code: str
    template_label: str
    trade_direction: str
    entry_rule: str
    take_profit_pct: float
    stop_loss_pct: float
    time_cap_days: int
    state_size: int
    state_signature: str
    feature_keys: str
    min_trade_return_pct: float
    tickers_with_matches: int
    total_occurrences: int
    successful_occurrences: int
    success_rate_pct: float
    profitable_trades: int
    profitable_trade_rate_pct: float
    take_profit_trades: int
    take_profit_rate_pct: float
    stop_loss_trades: int
    stop_loss_rate_pct: float
    time_cap_trades: int
    time_cap_rate_pct: float
    average_asset_move_pct: float
    average_trade_return_pct: float
    net_trade_return_pct: float
    cumulative_return_pct: float
    profit_factor: float | None
    first_trade_date: str
    last_trade_date: str
    exit_mode: str = "percent"
    atr_target_mult: float = 0.0
    atr_stop_mult: float = 0.0


@dataclass(slots=True)
class _PatternAccumulator:
    code: str
    label: str
    family: str
    template_code: str
    template_label: str
    trade_direction: str
    entry_rule: str
    take_profit_pct: float
    stop_loss_pct: float
    time_cap_days: int
    state_size: int
    state_signature: str
    feature_keys: str
    min_trade_return_pct: float
    tickers: set[str]
    total_occurrences: int = 0
    successful_occurrences: int = 0
    profitable_trades: int = 0
    take_profit_trades: int = 0
    stop_loss_trades: int = 0
    time_cap_trades: int = 0
    sum_asset_move_pct: float = 0.0
    sum_trade_return_pct: float = 0.0
    gross_profit_pct: float = 0.0
    gross_loss_pct: float = 0.0
    log_cumulative_multiplier: float = 0.0
    zero_cumulative_multiplier: bool = False
    first_trade_date: str | None = None
    last_trade_date: str | None = None
    exit_mode: str = "percent"
    atr_target_mult: float = 0.0
    atr_stop_mult: float = 0.0


@dataclass(slots=True)
class _TickerTradeAccumulator:
    strategy_code: str
    strategy_label: str
    family: str
    ticker: str
    total_trades: int = 0
    successful_trades: int = 0
    profitable_trades: int = 0
    sum_trade_return_pct: float = 0.0
    gross_profit_pct: float = 0.0
    gross_loss_pct: float = 0.0
    log_cumulative_multiplier: float = 0.0
    zero_cumulative_multiplier: bool = False
    first_trade_date: str | None = None
    last_trade_date: str | None = None

    def update(
        self,
        *,
        trade_return_pct: float,
        is_successful: bool,
        is_profitable: bool,
        trigger_date: str,
    ) -> None:
        self.total_trades += 1
        if is_successful:
            self.successful_trades += 1
        if is_profitable:
            self.profitable_trades += 1
            self.gross_profit_pct += trade_return_pct
        elif trade_return_pct < 0:
            self.gross_loss_pct += abs(trade_return_pct)
        self.sum_trade_return_pct += trade_return_pct
        multiplier = 1.0 + (trade_return_pct / 100.0)
        if multiplier <= 0:
            self.zero_cumulative_multiplier = True
        else:
            self.log_cumulative_multiplier += log(multiplier)
        if self.first_trade_date is None or trigger_date < self.first_trade_date:
            self.first_trade_date = trigger_date
        if self.last_trade_date is None or trigger_date > self.last_trade_date:
            self.last_trade_date = trigger_date


FEATURE_LIBRARY: list[AssetDiscoveryFeature] = [
    AssetDiscoveryFeature("gap_pct", "Gap da abertura"),
    AssetDiscoveryFeature("gap_vs_range20", "Gap vs range medio 20D"),
    AssetDiscoveryFeature("prev_ret_1d", "Retorno acumulado 1D"),
    AssetDiscoveryFeature("prev_ret_2d", "Retorno acumulado 2D"),
    AssetDiscoveryFeature("prev_ret_3d", "Retorno acumulado 3D"),
    AssetDiscoveryFeature("prev_ret_5d", "Retorno acumulado 5D"),
    AssetDiscoveryFeature("prev_ret_10d", "Retorno acumulado 10D"),
    AssetDiscoveryFeature("prev_ret_20d", "Retorno acumulado 20D"),
    AssetDiscoveryFeature("prev_ret_1d_z20", "Retorno 1D em z-score 20D", False),
    AssetDiscoveryFeature("prev_ret_3d_z20", "Retorno 3D em z-score 20D", False),
    AssetDiscoveryFeature("prev_ret_5d_z20", "Retorno 5D em z-score 20D", False),
    AssetDiscoveryFeature("close_vs_sma5", "Fechamento vs MM5", False),
    AssetDiscoveryFeature("close_vs_sma20", "Fechamento vs MM20"),
    AssetDiscoveryFeature("close_vs_sma50", "Fechamento vs MM50"),
    AssetDiscoveryFeature("sma5_vs_sma20", "MM5 vs MM20"),
    AssetDiscoveryFeature("sma20_vs_sma50", "MM20 vs MM50"),
    AssetDiscoveryFeature("sma20_slope_5d", "Inclinacao da MM20 em 5D"),
    AssetDiscoveryFeature("sma50_slope_10d", "Inclinacao da MM50 em 10D"),
    AssetDiscoveryFeature("atr14_pct", "ATR14 em % do preco", False),
    AssetDiscoveryFeature("gap_vs_atr14", "Gap vs ATR14"),
    AssetDiscoveryFeature("range_ratio_20d", "Range vs media 20D", False),
    AssetDiscoveryFeature("compression_3v20", "Compressao 3D vs 20D"),
    AssetDiscoveryFeature("vol_ratio_5v20", "Volatilidade 5D vs 20D", False),
    AssetDiscoveryFeature("volume_ratio_20d", "Volume vs media 20D"),
    AssetDiscoveryFeature("volume_ratio_5v20", "Volume 5D vs 20D", False),
    AssetDiscoveryFeature("dollar_volume_ratio_20d", "Volume financeiro vs media 20D", False),
    AssetDiscoveryFeature("intraday_return_prev", "Retorno intraday do candle de referencia"),
    AssetDiscoveryFeature("body_ratio_prev", "Corpo do candle de referencia", False),
    AssetDiscoveryFeature("close_location_prev", "Posicao do fechamento no candle", False),
    AssetDiscoveryFeature("lower_wick_ratio_prev", "Pavio inferior do candle", False),
    AssetDiscoveryFeature("upper_wick_ratio_prev", "Pavio superior do candle", False),
    AssetDiscoveryFeature("range_position_5d", "Posicao no range 5D"),
    AssetDiscoveryFeature("range_position_10d", "Posicao no range 10D"),
    AssetDiscoveryFeature("range_position_20d", "Posicao no range 20D"),
    AssetDiscoveryFeature("drawdown_5d", "Queda desde maxima 5D"),
    AssetDiscoveryFeature("drawdown_10d", "Queda desde maxima 10D"),
    AssetDiscoveryFeature("drawdown_20d", "Queda desde maxima 20D", False),
    AssetDiscoveryFeature("rebound_5d", "Distancia desde minima 5D", False),
    AssetDiscoveryFeature("rebound_10d", "Distancia desde minima 10D", False),
    AssetDiscoveryFeature("streak_direction", "Sequencia de altas/quedas"),
    AssetDiscoveryFeature("positive_days_5d", "Dias positivos nos ultimos 5 pregoes"),
    AssetDiscoveryFeature("positive_days_10d", "Dias positivos nos ultimos 10 pregoes"),
    # -- R3 novas features --
    AssetDiscoveryFeature("rsi_14", "RSI 14 periodos"),
    AssetDiscoveryFeature("bb_position_20", "Posicao nas Bandas de Bollinger 20D"),
    AssetDiscoveryFeature("close_vs_ema9", "Fechamento vs EMA9"),
    AssetDiscoveryFeature("close_vs_ema21", "Fechamento vs EMA21"),
    AssetDiscoveryFeature("day_of_week", "Dia da semana"),
    AssetDiscoveryFeature("atr_return_1d", "Retorno 1D normalizado por ATR"),
    AssetDiscoveryFeature("atr_return_5d", "Retorno 5D normalizado por ATR"),
    AssetDiscoveryFeature("volume_spike_1d", "Spike de volume vs maximo 5D"),
]

FEATURE_BY_KEY = {item.key: item for item in FEATURE_LIBRARY}


def list_asset_discovery_features() -> list[AssetDiscoveryFeature]:
    return list(FEATURE_LIBRARY)


def build_asset_discovery_round1_templates(
    *,
    entry_rules: list[str] | None = None,
    trade_directions: list[str] | None = None,
    target_stop_pairs: list[tuple[float, float]] | None = None,
    time_cap_days: int = 5,
    round_trip_cost_pct: float = 0.0,
) -> list[AssetDiscoveryTemplate]:
    effective_entry_rules = entry_rules or ["open", "close"]
    effective_trade_directions = trade_directions or ["long", "short"]
    invalid_directions = sorted(set(effective_trade_directions) - {"long", "short"})
    if invalid_directions:
        raise ValueError(f"Direcoes invalidas: {', '.join(invalid_directions)}")
    effective_pairs = target_stop_pairs or [
        (1.0, 1.0),
        (2.0, 1.0),
        (3.0, 1.5),
        (4.0, 2.0),
        (6.0, 3.0),
    ]

    templates: list[AssetDiscoveryTemplate] = []
    for entry_rule in effective_entry_rules:
        for trade_direction in effective_trade_directions:
            for take_profit_pct, stop_loss_pct in effective_pairs:
                tp_label = f"{take_profit_pct:.1f}".rstrip("0").rstrip(".")
                sl_label = f"{stop_loss_pct:.1f}".rstrip("0").rstrip(".")
                code = (
                    f"asset_{trade_direction}_{entry_rule}_"
                    f"tp{tp_label.replace('.', '_')}_sl{sl_label.replace('.', '_')}_cap{time_cap_days}"
                )
                label = (
                    f"Discovery Acao: {'compra' if trade_direction == 'long' else 'venda'} no {entry_rule}, "
                    f"alvo +{tp_label}% / stop -{sl_label}% / cap {time_cap_days}D"
                )
                templates.append(
                    AssetDiscoveryTemplate(
                        code=code,
                        label=label,
                        family="asset_state_discovery_round1",
                        trade_direction=trade_direction,
                        entry_rule=entry_rule,
                        take_profit_pct=take_profit_pct,
                        stop_loss_pct=stop_loss_pct,
                        time_cap_days=time_cap_days,
                        round_trip_cost_pct=round_trip_cost_pct,
                    )
                )

    return templates


def build_asset_discovery_atr_templates(
    *,
    entry_rules: list[str] | None = None,
    trade_directions: list[str] | None = None,
    atr_target_stop_pairs: list[tuple[float, float]] | None = None,
    time_cap_days_list: list[int] | None = None,
    round_trip_cost_pct: float = 0.0,
) -> list[AssetDiscoveryTemplate]:
    effective_entry_rules = entry_rules or ["open", "close"]
    effective_trade_directions = trade_directions or ["long"]
    effective_pairs = atr_target_stop_pairs or [
        (1.0, 1.0),
        (1.5, 1.0),
        (1.5, 1.5),
        (2.0, 1.0),
        (2.0, 1.5),
        (2.0, 2.0),
    ]
    effective_caps = time_cap_days_list or [5, 10]

    templates: list[AssetDiscoveryTemplate] = []
    for entry_rule in effective_entry_rules:
        for trade_direction in effective_trade_directions:
            for atr_tp, atr_sl in effective_pairs:
                for cap in effective_caps:
                    tp_label = f"{atr_tp:.1f}".rstrip("0").rstrip(".")
                    sl_label = f"{atr_sl:.1f}".rstrip("0").rstrip(".")
                    code = (
                        f"asset_{trade_direction}_{entry_rule}_"
                        f"atr_tp{tp_label.replace('.', '_')}_sl{sl_label.replace('.', '_')}_cap{cap}"
                    )
                    label = (
                        f"Discovery Acao: {'compra' if trade_direction == 'long' else 'venda'} no {entry_rule}, "
                        f"alvo +{tp_label}xATR / stop -{sl_label}xATR / cap {cap}D"
                    )
                    templates.append(
                        AssetDiscoveryTemplate(
                            code=code,
                            label=label,
                            family="asset_state_discovery_round1",
                            trade_direction=trade_direction,
                            entry_rule=entry_rule,
                            take_profit_pct=0.0,
                            stop_loss_pct=0.0,
                            time_cap_days=cap,
                            round_trip_cost_pct=round_trip_cost_pct,
                            exit_mode="atr",
                            atr_target_mult=atr_tp,
                            atr_stop_mult=atr_sl,
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
    if value <= -8.0:
        return "shock_dn", "choque de baixa"
    if value <= -5.0:
        return "dn_5_8", "queda de 5% a 8%"
    if value <= -3.0:
        return "dn_3_5", "queda de 3% a 5%"
    if value <= -1.0:
        return "dn_1_3", "queda de 1% a 3%"
    if value < 1.0:
        return "flat", "neutro"
    if value < 3.0:
        return "up_1_3", "alta de 1% a 3%"
    if value < 5.0:
        return "up_3_5", "alta de 3% a 5%"
    if value < 8.0:
        return "up_5_8", "alta de 5% a 8%"
    return "shock_up", "choque de alta"


def _bucket_signed_distance(value: float) -> tuple[str, str]:
    if value <= -10.0:
        return "far_dn", "muito abaixo"
    if value <= -5.0:
        return "dn_5_10", "abaixo forte"
    if value <= -1.5:
        return "dn_1_5", "abaixo leve"
    if value < 1.5:
        return "near", "perto do equilibrio"
    if value < 5.0:
        return "up_1_5", "acima leve"
    if value < 10.0:
        return "up_5_10", "acima forte"
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


def _bucket_streak(value: int) -> tuple[str, str]:
    if value <= -4:
        return "down_4p", "queda em 4+ pregoes seguidos"
    if value <= -2:
        return "down_2_3", "queda em 2 a 3 pregoes seguidos"
    if value == -1:
        return "down_1", "queda no ultimo pregao"
    if value == 0:
        return "flat", "sem sequencia clara"
    if value == 1:
        return "up_1", "alta no ultimo pregao"
    if value <= 3:
        return "up_2_3", "alta em 2 a 3 pregoes seguidos"
    return "up_4p", "alta em 4+ pregoes seguidos"


def _bucket_positive_days_5d(value: int) -> tuple[str, str]:
    if value <= 1:
        return "weak", "0 a 1 dia positivo em 5D"
    if value == 2:
        return "soft_weak", "2 dias positivos em 5D"
    if value == 3:
        return "soft_strong", "3 dias positivos em 5D"
    return "strong", "4 a 5 dias positivos em 5D"


def _direction_streak(closes: list[float], end_idx: int, max_days: int = 5) -> int:
    streak = 0
    start_idx = max(1, end_idx - max_days + 1)
    for idx in range(end_idx, start_idx - 1, -1):
        if closes[idx] > closes[idx - 1]:
            if streak < 0:
                break
            streak += 1
        elif closes[idx] < closes[idx - 1]:
            if streak > 0:
                break
            streak -= 1
        else:
            break
    return streak


def _positive_days(closes: list[float], end_idx: int, window_days: int) -> int | None:
    start_idx = end_idx - window_days + 1
    if start_idx < 1:
        return None
    return sum(1 for idx in range(start_idx, end_idx + 1) if closes[idx] > closes[idx - 1])


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


def _rolling_range_position_window(
    highs: list[float],
    lows: list[float],
    close_price: float,
    end_idx: int,
    window_days: int,
) -> float | None:
    start_idx = end_idx - window_days + 1
    if start_idx < 0:
        return None
    high_value = max(highs[start_idx : end_idx + 1])
    low_value = min(lows[start_idx : end_idx + 1])
    if high_value == low_value:
        return 0.5
    return (close_price - low_value) / (high_value - low_value)


def _drawdown_from_high(
    highs: list[float],
    close_price: float,
    end_idx: int,
    window_days: int,
) -> float | None:
    start_idx = end_idx - window_days + 1
    if start_idx < 0:
        return None
    high_value = max(highs[start_idx : end_idx + 1])
    if high_value <= 0:
        return None
    return ((close_price / high_value) - 1.0) * 100.0


def _rebound_from_low(
    lows: list[float],
    close_price: float,
    end_idx: int,
    window_days: int,
) -> float | None:
    start_idx = end_idx - window_days + 1
    if start_idx < 0:
        return None
    low_value = min(lows[start_idx : end_idx + 1])
    if low_value <= 0:
        return None
    return ((close_price / low_value) - 1.0) * 100.0


def _bucket_drawdown(value: float) -> tuple[str, str]:
    if value <= -15.0:
        return "dd_15p", "15%+ abaixo da maxima"
    if value <= -10.0:
        return "dd_10_15", "10% a 15% abaixo da maxima"
    if value <= -6.0:
        return "dd_6_10", "6% a 10% abaixo da maxima"
    if value <= -3.0:
        return "dd_3_6", "3% a 6% abaixo da maxima"
    if value <= -1.0:
        return "dd_1_3", "1% a 3% abaixo da maxima"
    return "near_high", "perto da maxima"


def _bucket_rebound(value: float) -> tuple[str, str]:
    if value < 1.0:
        return "near_low", "perto da minima"
    if value < 3.0:
        return "reb_1_3", "1% a 3% acima da minima"
    if value < 6.0:
        return "reb_3_6", "3% a 6% acima da minima"
    if value < 10.0:
        return "reb_6_10", "6% a 10% acima da minima"
    if value < 15.0:
        return "reb_10_15", "10% a 15% acima da minima"
    return "reb_15p", "15%+ acima da minima"


def _bucket_positive_days_10d(value: int) -> tuple[str, str]:
    if value <= 3:
        return "weak", "0 a 3 dias positivos em 10D"
    if value <= 5:
        return "neutral", "4 a 5 dias positivos em 10D"
    if value <= 7:
        return "strong", "6 a 7 dias positivos em 10D"
    return "very_strong", "8 a 10 dias positivos em 10D"


def _bucket_rsi(value: float) -> tuple[str, str]:
    if value < 20.0:
        return "oversold", "sobrevendido"
    if value < 35.0:
        return "low", "RSI baixo"
    if value < 50.0:
        return "neutral_low", "RSI neutro baixo"
    if value < 65.0:
        return "neutral_high", "RSI neutro alto"
    if value < 80.0:
        return "high", "RSI alto"
    return "overbought", "sobrecomprado"


def _bucket_day_of_week(value: int) -> tuple[str, str]:
    labels = {
        0: ("mon", "segunda-feira"),
        1: ("tue", "terca-feira"),
        2: ("wed", "quarta-feira"),
        3: ("thu", "quinta-feira"),
        4: ("fri", "sexta-feira"),
    }
    return labels.get(value, ("unknown", "desconhecido"))


def _ema_series(closes: list[float], period: int) -> list[float]:
    """Compute EMA using standard multiplier: 2/(period+1)."""
    ema_values: list[float] = [0.0] * len(closes)
    if len(closes) < period:
        return ema_values
    # seed with SMA of first `period` values
    sma = sum(closes[:period]) / period
    ema_values[period - 1] = sma
    multiplier = 2.0 / (period + 1)
    for idx in range(period, len(closes)):
        ema_values[idx] = (closes[idx] - ema_values[idx - 1]) * multiplier + ema_values[idx - 1]
    return ema_values


def _rsi_series(closes: list[float], period: int = 14) -> list[float]:
    """Compute RSI using Wilder smoothing."""
    rsi_values: list[float] = [50.0] * len(closes)
    if len(closes) < period + 1:
        return rsi_values
    gains: list[float] = []
    losses: list[float] = []
    for idx in range(1, len(closes)):
        change = closes[idx] - closes[idx - 1]
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))
    # initial averages (SMA of first `period` changes)
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    if avg_loss == 0:
        rsi_values[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi_values[period] = 100.0 - (100.0 / (1.0 + rs))
    # Wilder smoothing
    for idx in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[idx]) / period
        avg_loss = (avg_loss * (period - 1) + losses[idx]) / period
        if avg_loss == 0:
            rsi_values[idx + 1] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi_values[idx + 1] = 100.0 - (100.0 / (1.0 + rs))
    return rsi_values


def _bollinger_position(closes: list[float], close_prefix_sums: list[float], close_prefix_sums_sq: list[float], end_idx: int, period: int = 20) -> float | None:
    """Compute (close - lower_band) / (upper_band - lower_band)."""
    sma = _window_average(close_prefix_sums, end_idx, period)
    std = _window_std(close_prefix_sums, close_prefix_sums_sq, end_idx, period)
    if sma is None or std is None or std == 0:
        return None
    upper = sma + 2.0 * std
    lower = sma - 2.0 * std
    band_width = upper - lower
    if band_width <= 0:
        return 0.5
    return (closes[end_idx] - lower) / band_width


def _max_volume_window(volumes: list[float], end_idx: int, window_days: int) -> float | None:
    """Max volume in the trailing window (exclusive of end_idx)."""
    start_idx = end_idx - window_days
    if start_idx < 0:
        return None
    return max(volumes[start_idx:end_idx]) if end_idx > start_idx else None


def _atr_pct_series(highs: list[float], lows: list[float], closes: list[float]) -> list[float]:
    values = [0.0]
    for idx in range(1, len(closes)):
        previous_close = closes[idx - 1]
        if previous_close <= 0:
            values.append(0.0)
            continue
        true_range = max(
            highs[idx] - lows[idx],
            abs(highs[idx] - previous_close),
            abs(lows[idx] - previous_close),
        )
        values.append((true_range / previous_close) * 100.0)
    return values


def _ret_lookback(closes: list[float], end_idx: int, window_days: int) -> float | None:
    start_idx = end_idx - window_days
    if start_idx < 0 or closes[start_idx] <= 0:
        return None
    return ((closes[end_idx] / closes[start_idx]) - 1.0) * 100.0


def _build_feature_states(
    *,
    entry_rule: str,
    idx: int,
    opens: list[float],
    highs: list[float],
    lows: list[float],
    closes: list[float],
    volumes: list[float],
    close_prefix_sums: list[float],
    close_prefix_sums_sq: list[float] | None = None,
    volume_prefix_sums: list[float],
    dollar_volume_prefix_sums: list[float],
    range_pct_prefix_sums: list[float],
    daily_return_prefix_sums: list[float],
    daily_return_prefix_sums_sq: list[float],
    atr_pct_prefix_sums: list[float],
    entry_price: float,
    rsi_values: list[float] | None = None,
    ema9_values: list[float] | None = None,
    ema21_values: list[float] | None = None,
    trade_dates: list[str] | None = None,
) -> dict[str, tuple[str, str]]:
    if entry_rule == "open":
        anchor_idx = idx - 1
        gap_reference_close = closes[idx - 1] if idx >= 1 else 0.0
    else:
        anchor_idx = idx
        gap_reference_close = closes[idx - 1] if idx >= 1 else 0.0

    if anchor_idx < 1 or gap_reference_close <= 0:
        return {}

    states: dict[str, tuple[str, str]] = {}
    anchor_close = closes[anchor_idx]
    anchor_open = opens[anchor_idx]
    anchor_high = highs[anchor_idx]
    anchor_low = lows[anchor_idx]
    anchor_volume = volumes[anchor_idx]
    anchor_range = anchor_high - anchor_low
    anchor_range_pct = (anchor_range / anchor_close) * 100.0 if anchor_close > 0 else 0.0

    gap_pct = ((opens[idx] / gap_reference_close) - 1.0) * 100.0
    states["gap_pct"] = _bucket_signed_pct(gap_pct)

    avg_range_20d = _window_average(range_pct_prefix_sums, anchor_idx, 20)
    if avg_range_20d is not None and avg_range_20d > 0:
        states["gap_vs_range20"] = _bucket_signed_relative(gap_pct / avg_range_20d)
        states["range_ratio_20d"] = _bucket_ratio(anchor_range_pct / avg_range_20d)

    prev_ret_1d = _ret_lookback(closes, anchor_idx, 1)
    prev_ret_2d = _ret_lookback(closes, anchor_idx, 2)
    prev_ret_3d = _ret_lookback(closes, anchor_idx, 3)
    prev_ret_5d = _ret_lookback(closes, anchor_idx, 5)
    prev_ret_10d = _ret_lookback(closes, anchor_idx, 10)
    prev_ret_20d = _ret_lookback(closes, anchor_idx, 20)
    if prev_ret_1d is not None:
        states["prev_ret_1d"] = _bucket_signed_pct(prev_ret_1d)
    if prev_ret_2d is not None:
        states["prev_ret_2d"] = _bucket_signed_pct(prev_ret_2d)
    if prev_ret_3d is not None:
        states["prev_ret_3d"] = _bucket_signed_pct(prev_ret_3d)
    if prev_ret_5d is not None:
        states["prev_ret_5d"] = _bucket_signed_pct(prev_ret_5d)
    if prev_ret_10d is not None:
        states["prev_ret_10d"] = _bucket_signed_pct(prev_ret_10d)
    if prev_ret_20d is not None:
        states["prev_ret_20d"] = _bucket_signed_pct(prev_ret_20d)

    return_std_20d = _window_std(daily_return_prefix_sums, daily_return_prefix_sums_sq, anchor_idx, 20)
    if prev_ret_1d is not None and return_std_20d is not None and return_std_20d > 0:
        states["prev_ret_1d_z20"] = _bucket_signed_relative(prev_ret_1d / return_std_20d)
    if prev_ret_3d is not None and return_std_20d is not None and return_std_20d > 0:
        states["prev_ret_3d_z20"] = _bucket_signed_relative(prev_ret_3d / return_std_20d)
    if prev_ret_5d is not None and return_std_20d is not None and return_std_20d > 0:
        states["prev_ret_5d_z20"] = _bucket_signed_relative(prev_ret_5d / return_std_20d)

    sma_5 = _window_average(close_prefix_sums, anchor_idx, 5)
    sma_20 = _window_average(close_prefix_sums, anchor_idx, 20)
    sma_50 = _window_average(close_prefix_sums, anchor_idx, 50)
    sma_20_prev5 = _window_average(close_prefix_sums, anchor_idx - 5, 20) if anchor_idx >= 24 else None
    sma_50_prev10 = _window_average(close_prefix_sums, anchor_idx - 10, 50) if anchor_idx >= 59 else None
    if sma_5 is not None and sma_5 > 0:
        states["close_vs_sma5"] = _bucket_signed_distance(((anchor_close / sma_5) - 1.0) * 100.0)
    if sma_20 is not None and sma_20 > 0:
        states["close_vs_sma20"] = _bucket_signed_distance(((anchor_close / sma_20) - 1.0) * 100.0)
    if sma_50 is not None and sma_50 > 0:
        states["close_vs_sma50"] = _bucket_signed_distance(((anchor_close / sma_50) - 1.0) * 100.0)
    if sma_5 is not None and sma_20 is not None and sma_20 > 0:
        states["sma5_vs_sma20"] = _bucket_signed_distance(((sma_5 / sma_20) - 1.0) * 100.0)
    if sma_20 is not None and sma_50 is not None and sma_50 > 0:
        states["sma20_vs_sma50"] = _bucket_signed_distance(((sma_20 / sma_50) - 1.0) * 100.0)
    if sma_20 is not None and sma_20_prev5 is not None and sma_20_prev5 > 0:
        states["sma20_slope_5d"] = _bucket_signed_distance(((sma_20 / sma_20_prev5) - 1.0) * 100.0)
    if sma_50 is not None and sma_50_prev10 is not None and sma_50_prev10 > 0:
        states["sma50_slope_10d"] = _bucket_signed_distance(((sma_50 / sma_50_prev10) - 1.0) * 100.0)

    atr14_pct = _window_average(atr_pct_prefix_sums, anchor_idx, 14)
    if atr14_pct is not None and atr14_pct > 0:
        states["atr14_pct"] = _bucket_ratio(atr14_pct / max(anchor_range_pct, 0.0001))
        states["gap_vs_atr14"] = _bucket_signed_relative(gap_pct / atr14_pct)

    avg_range_3d = _window_average(range_pct_prefix_sums, anchor_idx, 3)
    if avg_range_3d is not None and avg_range_20d is not None and avg_range_20d > 0:
        states["compression_3v20"] = _bucket_ratio(avg_range_3d / avg_range_20d)

    return_std_5d = _window_std(daily_return_prefix_sums, daily_return_prefix_sums_sq, anchor_idx, 5)
    if return_std_5d is not None and return_std_20d is not None and return_std_20d > 0:
        states["vol_ratio_5v20"] = _bucket_ratio(return_std_5d / return_std_20d)

    avg_volume_20d = _window_average(volume_prefix_sums, anchor_idx, 20)
    avg_volume_5d = _window_average(volume_prefix_sums, anchor_idx, 5)
    if avg_volume_20d is not None and avg_volume_20d > 0:
        states["volume_ratio_20d"] = _bucket_ratio(anchor_volume / avg_volume_20d)
    if avg_volume_5d is not None and avg_volume_20d is not None and avg_volume_20d > 0:
        states["volume_ratio_5v20"] = _bucket_ratio(avg_volume_5d / avg_volume_20d)

    avg_dollar_volume_20d = _window_average(dollar_volume_prefix_sums, anchor_idx, 20)
    anchor_dollar_volume = anchor_close * anchor_volume
    if avg_dollar_volume_20d is not None and avg_dollar_volume_20d > 0:
        states["dollar_volume_ratio_20d"] = _bucket_ratio(anchor_dollar_volume / avg_dollar_volume_20d)

    if anchor_open > 0:
        states["intraday_return_prev"] = _bucket_signed_pct(((anchor_close / anchor_open) - 1.0) * 100.0)

    if anchor_range > 0:
        body_ratio = abs(anchor_close - anchor_open) / anchor_range
        close_location = (anchor_close - anchor_low) / anchor_range
        lower_wick = (min(anchor_open, anchor_close) - anchor_low) / anchor_range
        upper_wick = (anchor_high - max(anchor_open, anchor_close)) / anchor_range
        states["body_ratio_prev"] = _bucket_fraction(body_ratio)
        states["close_location_prev"] = _bucket_close_position(close_location)
        states["lower_wick_ratio_prev"] = _bucket_fraction(lower_wick)
        states["upper_wick_ratio_prev"] = _bucket_fraction(upper_wick)

    range_position_20d = _rolling_range_position(highs=highs, lows=lows, close_price=anchor_close, end_idx=anchor_idx)
    if range_position_20d is not None:
        states["range_position_20d"] = _bucket_close_position(range_position_20d)
    range_position_5d = _rolling_range_position_window(
        highs=highs,
        lows=lows,
        close_price=anchor_close,
        end_idx=anchor_idx,
        window_days=5,
    )
    if range_position_5d is not None:
        states["range_position_5d"] = _bucket_close_position(range_position_5d)
    range_position_10d = _rolling_range_position_window(
        highs=highs,
        lows=lows,
        close_price=anchor_close,
        end_idx=anchor_idx,
        window_days=10,
    )
    if range_position_10d is not None:
        states["range_position_10d"] = _bucket_close_position(range_position_10d)

    for window_days in (5, 10, 20):
        drawdown = _drawdown_from_high(highs=highs, close_price=anchor_close, end_idx=anchor_idx, window_days=window_days)
        if drawdown is not None:
            states[f"drawdown_{window_days}d"] = _bucket_drawdown(drawdown)
    for window_days in (5, 10):
        rebound = _rebound_from_low(lows=lows, close_price=anchor_close, end_idx=anchor_idx, window_days=window_days)
        if rebound is not None:
            states[f"rebound_{window_days}d"] = _bucket_rebound(rebound)

    states["streak_direction"] = _bucket_streak(_direction_streak(closes, anchor_idx, max_days=5))
    positive_days_5d = _positive_days(closes, anchor_idx, window_days=5)
    if positive_days_5d is not None:
        states["positive_days_5d"] = _bucket_positive_days_5d(positive_days_5d)
    positive_days_10d = _positive_days(closes, anchor_idx, window_days=10)
    if positive_days_10d is not None:
        states["positive_days_10d"] = _bucket_positive_days_10d(positive_days_10d)

    # -- R3 novas features --
    if rsi_values is not None and anchor_idx < len(rsi_values) and anchor_idx >= 14:
        states["rsi_14"] = _bucket_rsi(rsi_values[anchor_idx])

    if close_prefix_sums_sq is not None:
        bb_pos = _bollinger_position(closes, close_prefix_sums, close_prefix_sums_sq, anchor_idx, period=20)
        if bb_pos is not None:
            states["bb_position_20"] = _bucket_close_position(bb_pos)

    if ema9_values is not None and anchor_idx >= 8 and ema9_values[anchor_idx] > 0:
        states["close_vs_ema9"] = _bucket_signed_distance(((anchor_close / ema9_values[anchor_idx]) - 1.0) * 100.0)
    if ema21_values is not None and anchor_idx >= 20 and ema21_values[anchor_idx] > 0:
        states["close_vs_ema21"] = _bucket_signed_distance(((anchor_close / ema21_values[anchor_idx]) - 1.0) * 100.0)

    if trade_dates is not None and idx < len(trade_dates):
        try:
            day_of_week = date.fromisoformat(trade_dates[idx]).weekday()
            states["day_of_week"] = _bucket_day_of_week(day_of_week)
        except (ValueError, IndexError):
            pass

    if atr14_pct is not None and atr14_pct > 0:
        if prev_ret_1d is not None:
            states["atr_return_1d"] = _bucket_signed_relative(prev_ret_1d / atr14_pct)
        if prev_ret_5d is not None:
            states["atr_return_5d"] = _bucket_signed_relative(prev_ret_5d / atr14_pct)

    max_vol_5d = _max_volume_window(volumes, anchor_idx, window_days=5)
    if max_vol_5d is not None and max_vol_5d > 0:
        states["volume_spike_1d"] = _bucket_ratio(anchor_volume / max_vol_5d)

    return states


def _simulate_percent_exit(
    *,
    bars: list[SpotQuoteBar],
    entry_idx: int,
    entry_price: float,
    trade_direction: str,
    take_profit_pct: float,
    stop_loss_pct: float,
    time_cap_days: int,
    include_entry_bar: bool,
) -> tuple[int, float, str] | None:
    start_idx = entry_idx if include_entry_bar else entry_idx + 1
    end_idx = min(len(bars) - 1, entry_idx + time_cap_days)
    if start_idx > end_idx:
        return None

    if trade_direction == "long":
        target_price = entry_price * (1.0 + take_profit_pct / 100.0)
        stop_price = entry_price * (1.0 - stop_loss_pct / 100.0)
    else:
        target_price = entry_price * (1.0 - take_profit_pct / 100.0)
        stop_price = entry_price * (1.0 + stop_loss_pct / 100.0)

    for bar_idx in range(start_idx, end_idx + 1):
        bar = bars[bar_idx]
        if trade_direction == "long":
            target_hit = bar.high >= target_price
            stop_hit = bar.low <= stop_price
        else:
            target_hit = bar.low <= target_price
            stop_hit = bar.high >= stop_price

        if target_hit and stop_hit:
            return bar_idx, stop_price, "stop_loss_conflict"
        if stop_hit:
            return bar_idx, stop_price, "stop_loss"
        if target_hit:
            return bar_idx, target_price, "take_profit"

    final_bar = bars[end_idx]
    return end_idx, final_bar.close, "time_cap"


def _simulate_atr_exit(
    *,
    bars: list[SpotQuoteBar],
    entry_idx: int,
    entry_price: float,
    trade_direction: str,
    atr_target_mult: float,
    atr_stop_mult: float,
    time_cap_days: int,
    include_entry_bar: bool,
    atr_pct_prefix_sums: list[float],
) -> tuple[int, float, str] | None:
    """Exit using ATR-based dynamic target/stop levels."""
    atr14_pct = _window_average(atr_pct_prefix_sums, entry_idx, 14)
    if atr14_pct is None or atr14_pct <= 0 or entry_price <= 0:
        return None
    atr_value = entry_price * (atr14_pct / 100.0)
    target_offset = atr_value * atr_target_mult
    stop_offset = atr_value * atr_stop_mult

    if trade_direction == "long":
        target_price = entry_price + target_offset
        stop_price = entry_price - stop_offset
    else:
        target_price = entry_price - target_offset
        stop_price = entry_price + stop_offset

    start_idx = entry_idx if include_entry_bar else entry_idx + 1
    end_idx = min(len(bars) - 1, entry_idx + time_cap_days)
    if start_idx > end_idx:
        return None

    for bar_idx in range(start_idx, end_idx + 1):
        bar = bars[bar_idx]
        if trade_direction == "long":
            target_hit = bar.high >= target_price
            stop_hit = bar.low <= stop_price
        else:
            target_hit = bar.low <= target_price
            stop_hit = bar.high >= stop_price

        if target_hit and stop_hit:
            return bar_idx, stop_price, "stop_loss_conflict"
        if stop_hit:
            return bar_idx, stop_price, "stop_loss"
        if target_hit:
            return bar_idx, target_price, "take_profit"

    final_bar = bars[end_idx]
    return end_idx, final_bar.close, "time_cap"


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
    upper_size = min(max_pattern_size, len(pairable_items))
    for pattern_size in range(2, upper_size + 1):
        for pattern_group in combinations(pairable_items, pattern_size):
            patterns.append(tuple(pattern_group))
    return patterns


def _build_pattern_identity(
    template: AssetDiscoveryTemplate,
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


def _iter_asset_samples(
    *,
    db_path: str | Path,
    tickers_file: str | Path,
    start_date: str,
    end_date: str,
    template_definitions: list[AssetDiscoveryTemplate],
    progress_callback: Callable[[dict[str, object]], None] | None = None,
):
    tickers = {ticker.removesuffix(".SA").upper() for ticker in load_tickers(tickers_file)}
    spot_bars = _load_spot_bars(db_path, allowed_tickers=tickers)
    start_day = date.fromisoformat(start_date)
    end_day = date.fromisoformat(end_date)
    templates_by_entry_rule: dict[str, list[AssetDiscoveryTemplate]] = defaultdict(list)
    for template in template_definitions:
        templates_by_entry_rule[template.entry_rule].append(template)

    eligible_items = [(ticker, bars) for ticker, bars in spot_bars.items() if len(bars) >= 70]
    total_tickers = len(eligible_items)
    processed_tickers = 0
    total_samples = 0

    for ticker, bars in eligible_items:
        ticker_samples = 0

        opens = [item.open for item in bars]
        highs = [item.high for item in bars]
        lows = [item.low for item in bars]
        closes = [item.close for item in bars]
        volumes = [float(item.volume) for item in bars]
        dollar_volumes = [item.close * float(item.volume) for item in bars]
        range_pcts = [
            0.0 if item.close <= 0 else ((item.high - item.low) / item.close) * 100.0
            for item in bars
        ]
        daily_returns = [0.0]
        for bar_idx in range(1, len(bars)):
            previous_close = closes[bar_idx - 1]
            if previous_close <= 0:
                daily_returns.append(0.0)
            else:
                daily_returns.append(((closes[bar_idx] / previous_close) - 1.0) * 100.0)

        atr_pct_values = _atr_pct_series(highs, lows, closes)
        close_prefix_sums = _prefix_sums(closes)
        close_prefix_sums_sq = _prefix_sums_sq(closes)
        volume_prefix_sums = _prefix_sums(volumes)
        dollar_volume_prefix_sums = _prefix_sums(dollar_volumes)
        range_pct_prefix_sums = _prefix_sums(range_pcts)
        daily_return_prefix_sums = _prefix_sums(daily_returns)
        daily_return_prefix_sums_sq = _prefix_sums_sq(daily_returns)
        atr_pct_prefix_sums = _prefix_sums(atr_pct_values)
        rsi_values = _rsi_series(closes, period=14)
        ema9_values = _ema_series(closes, period=9)
        ema21_values = _ema_series(closes, period=21)
        trade_dates_list = [item.trade_date for item in bars]

        for idx in range(1, len(bars)):
            current_bar = bars[idx]
            trade_day = date.fromisoformat(current_bar.trade_date)
            if trade_day < start_day or trade_day > end_day:
                continue

            for entry_rule in ("open", "close"):
                entry_price = current_bar.open if entry_rule == "open" else current_bar.close
                if entry_price <= 0:
                    continue
                feature_states = _build_feature_states(
                    entry_rule=entry_rule,
                    idx=idx,
                    opens=opens,
                    highs=highs,
                    lows=lows,
                    closes=closes,
                    volumes=volumes,
                    close_prefix_sums=close_prefix_sums,
                    close_prefix_sums_sq=close_prefix_sums_sq,
                    volume_prefix_sums=volume_prefix_sums,
                    dollar_volume_prefix_sums=dollar_volume_prefix_sums,
                    range_pct_prefix_sums=range_pct_prefix_sums,
                    daily_return_prefix_sums=daily_return_prefix_sums,
                    daily_return_prefix_sums_sq=daily_return_prefix_sums_sq,
                    atr_pct_prefix_sums=atr_pct_prefix_sums,
                    entry_price=entry_price,
                    rsi_values=rsi_values,
                    ema9_values=ema9_values,
                    ema21_values=ema21_values,
                    trade_dates=trade_dates_list,
                )
                if not feature_states:
                    continue

                for template in templates_by_entry_rule.get(entry_rule, []):
                    if template.exit_mode == "atr":
                        exit_result = _simulate_atr_exit(
                            bars=bars,
                            entry_idx=idx,
                            entry_price=entry_price,
                            trade_direction=template.trade_direction,
                            atr_target_mult=template.atr_target_mult,
                            atr_stop_mult=template.atr_stop_mult,
                            time_cap_days=template.time_cap_days,
                            include_entry_bar=(entry_rule == "open"),
                            atr_pct_prefix_sums=atr_pct_prefix_sums,
                        )
                    else:
                        exit_result = _simulate_percent_exit(
                            bars=bars,
                            entry_idx=idx,
                            entry_price=entry_price,
                            trade_direction=template.trade_direction,
                            take_profit_pct=template.take_profit_pct,
                            stop_loss_pct=template.stop_loss_pct,
                            time_cap_days=template.time_cap_days,
                            include_entry_bar=(entry_rule == "open"),
                        )
                    if exit_result is None:
                        continue
                    exit_idx, exit_price, exit_reason = exit_result
                    if template.trade_direction == "long":
                        asset_move_pct = ((exit_price / entry_price) - 1.0) * 100.0
                    else:
                        asset_move_pct = -(((exit_price / entry_price) - 1.0) * 100.0)
                    trade_return_pct = asset_move_pct - template.round_trip_cost_pct
                    yield {
                        "template": template,
                        "ticker": ticker,
                        "trade_date": current_bar.trade_date,
                        "exit_date": bars[exit_idx].trade_date,
                        "feature_states": feature_states,
                        "entry_price": entry_price,
                        "exit_price": exit_price,
                        "asset_move_pct": asset_move_pct,
                        "trade_return_pct": trade_return_pct,
                        "exit_reason": exit_reason,
                        "trigger_change_pct": ((current_bar.open / closes[idx - 1]) - 1.0) * 100.0 if idx >= 1 and closes[idx - 1] > 0 else 0.0,
                    }
                    total_samples += 1
                    ticker_samples += 1

        processed_tickers += 1
        if progress_callback is not None:
            progress_callback(
                {
                    "processed_tickers": processed_tickers,
                    "total_tickers": total_tickers,
                    "ticker": ticker,
                    "ticker_samples": ticker_samples,
                    "samples": total_samples,
                }
            )


def mine_asset_discovery_patterns(
    *,
    db_path: str | Path,
    tickers_file: str | Path,
    start_date: str,
    end_date: str,
    min_trade_return_pct: float = 0.0,
    template_definitions: list[AssetDiscoveryTemplate] | None = None,
    max_pattern_size: int = 2,
    known_codes: set[str] | None = None,
    progress_callback: Callable[[dict[str, object]], None] | None = None,
) -> list[AssetDiscoveryPatternSummary]:
    effective_templates = template_definitions or build_asset_discovery_round1_templates()
    ignored_codes = known_codes or set()
    accumulators: dict[str, _PatternAccumulator] = {}
    pattern_items_cache: dict[tuple[tuple[tuple[str, str, str], ...], int], list[tuple[tuple[str, str, str], ...]]] = {}
    identity_cache: dict[tuple[str, tuple[tuple[str, str, str], ...]], tuple[str, str, str, str]] = {}
    started_at = monotonic()

    def _handle_iter_progress(progress: dict[str, object]) -> None:
        if progress_callback is None:
            return
        payload = dict(progress)
        payload["stage"] = "mine"
        payload["elapsed_seconds"] = monotonic() - started_at
        payload["patterns"] = len(accumulators)
        progress_callback(payload)

    for sample in _iter_asset_samples(
        db_path=db_path,
        tickers_file=tickers_file,
        start_date=start_date,
        end_date=end_date,
        template_definitions=effective_templates,
        progress_callback=_handle_iter_progress,
    ):
        feature_signature = tuple(
            (feature.key, *sample["feature_states"][feature.key])
            for feature in FEATURE_LIBRARY
            if feature.key in sample["feature_states"]
        )
        pattern_cache_key = (feature_signature, max_pattern_size)
        pattern_items_list = pattern_items_cache.get(pattern_cache_key)
        if pattern_items_list is None:
            pattern_items_list = _pattern_items(sample["feature_states"], max_pattern_size=max_pattern_size)
            pattern_items_cache[pattern_cache_key] = pattern_items_list

        for pattern_items in pattern_items_list:
            identity_cache_key = (sample["template"].code, pattern_items)
            identity = identity_cache.get(identity_cache_key)
            if identity is None:
                identity = _build_pattern_identity(sample["template"], pattern_items)
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
                    trade_direction=sample["template"].trade_direction,
                    entry_rule=sample["template"].entry_rule,
                    take_profit_pct=sample["template"].take_profit_pct,
                    stop_loss_pct=sample["template"].stop_loss_pct,
                    time_cap_days=sample["template"].time_cap_days,
                    state_size=len(pattern_items),
                    state_signature=state_signature,
                    feature_keys=feature_keys,
                    min_trade_return_pct=min_trade_return_pct,
                    tickers=set(),
                    exit_mode=sample["template"].exit_mode,
                    atr_target_mult=sample["template"].atr_target_mult,
                    atr_stop_mult=sample["template"].atr_stop_mult,
                )
                accumulators[code] = accumulator

            accumulator.total_occurrences += 1
            accumulator.tickers.add(sample["ticker"])
            accumulator.sum_asset_move_pct += sample["asset_move_pct"]
            accumulator.sum_trade_return_pct += sample["trade_return_pct"]
            if sample["trade_return_pct"] >= min_trade_return_pct:
                accumulator.successful_occurrences += 1
            if sample["trade_return_pct"] > 0:
                accumulator.profitable_trades += 1
                accumulator.gross_profit_pct += sample["trade_return_pct"]
            elif sample["trade_return_pct"] < 0:
                accumulator.gross_loss_pct += -sample["trade_return_pct"]
            exit_reason = str(sample["exit_reason"])
            if exit_reason == "take_profit":
                accumulator.take_profit_trades += 1
            elif exit_reason.startswith("stop_loss"):
                accumulator.stop_loss_trades += 1
            elif exit_reason == "time_cap":
                accumulator.time_cap_trades += 1
            multiplier = 1.0 + (sample["trade_return_pct"] / 100.0)
            if multiplier <= 0:
                accumulator.zero_cumulative_multiplier = True
            else:
                accumulator.log_cumulative_multiplier += log(multiplier)
            trade_date = sample["trade_date"]
            if accumulator.first_trade_date is None or trade_date < accumulator.first_trade_date:
                accumulator.first_trade_date = trade_date
            if accumulator.last_trade_date is None or trade_date > accumulator.last_trade_date:
                accumulator.last_trade_date = trade_date

    summaries: list[AssetDiscoveryPatternSummary] = []
    for accumulator in accumulators.values():
        profit_factor = _calculate_profit_factor(accumulator.gross_profit_pct, accumulator.gross_loss_pct)
        cumulative_return_pct = (
            -100.0 if accumulator.zero_cumulative_multiplier else (exp(accumulator.log_cumulative_multiplier) - 1.0) * 100.0
        )
        summaries.append(
            AssetDiscoveryPatternSummary(
                code=accumulator.code,
                label=accumulator.label,
                family=accumulator.family,
                template_code=accumulator.template_code,
                template_label=accumulator.template_label,
                trade_direction=accumulator.trade_direction,
                entry_rule=accumulator.entry_rule,
                take_profit_pct=accumulator.take_profit_pct,
                stop_loss_pct=accumulator.stop_loss_pct,
                time_cap_days=accumulator.time_cap_days,
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
                take_profit_trades=accumulator.take_profit_trades,
                take_profit_rate_pct=(accumulator.take_profit_trades / accumulator.total_occurrences) * 100.0,
                stop_loss_trades=accumulator.stop_loss_trades,
                stop_loss_rate_pct=(accumulator.stop_loss_trades / accumulator.total_occurrences) * 100.0,
                time_cap_trades=accumulator.time_cap_trades,
                time_cap_rate_pct=(accumulator.time_cap_trades / accumulator.total_occurrences) * 100.0,
                average_asset_move_pct=accumulator.sum_asset_move_pct / accumulator.total_occurrences,
                average_trade_return_pct=accumulator.sum_trade_return_pct / accumulator.total_occurrences,
                net_trade_return_pct=accumulator.sum_trade_return_pct,
                cumulative_return_pct=cumulative_return_pct,
                profit_factor=profit_factor,
                first_trade_date=accumulator.first_trade_date or "",
                last_trade_date=accumulator.last_trade_date or "",
                exit_mode=accumulator.exit_mode,
                atr_target_mult=accumulator.atr_target_mult,
                atr_stop_mult=accumulator.atr_stop_mult,
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


def _pattern_items_progressive(
    feature_states: dict[str, tuple[str, str]],
    promoted_pairs: set[frozenset[str]] | None,
) -> list[tuple[tuple[str, str, str], ...]]:
    """Generate 3-factor patterns only when at least one 2-factor subset was promoted."""
    ordered_single_items = [
        (feature.key, *feature_states[feature.key])
        for feature in FEATURE_LIBRARY
        if feature.key in feature_states
    ]
    # Always include 1-factor patterns
    patterns: list[tuple[tuple[str, str, str], ...]] = [
        ((feature_key, code, label),) for feature_key, code, label in ordered_single_items
    ]
    pairable_items = [
        (feature.key, *feature_states[feature.key])
        for feature in FEATURE_LIBRARY
        if feature.key in feature_states and feature.pairable
    ]
    # 2-factor patterns
    for pair in combinations(pairable_items, 2):
        patterns.append(tuple(pair))
    # 3-factor patterns, only if a 2-factor subset was promoted
    if promoted_pairs is not None and len(pairable_items) >= 3:
        for triple in combinations(pairable_items, 3):
            keys = [item[0] for item in triple]
            # Check if any 2-element subset of these 3 keys is in promoted_pairs
            has_promoted_subset = any(
                frozenset(pair) in promoted_pairs
                for pair in combinations(keys, 2)
            )
            if has_promoted_subset:
                patterns.append(tuple(triple))
    return patterns


def mine_asset_discovery_patterns_progressive(
    *,
    db_path: str | Path,
    tickers_file: str | Path,
    start_date: str,
    end_date: str,
    min_trade_return_pct: float = 0.0,
    template_definitions: list[AssetDiscoveryTemplate] | None = None,
    known_codes: set[str] | None = None,
    progress_callback: Callable[[dict[str, object]], None] | None = None,
    pre_filter_min_profitable_rate: float = 55.0,
    pre_filter_min_profit_factor: float = 1.5,
    pre_filter_min_trades: int = 50,
    max_promoted_pairs: int = 150,
    prune_every_tickers: int = 25,
    max_accumulators: int = 500_000,
) -> list[AssetDiscoveryPatternSummary]:
    """Two-phase mining: first find good 2-factor combos, then expand to 3 factors."""
    effective_templates = template_definitions or build_asset_discovery_round1_templates()
    ignored_codes = known_codes or set()

    # Phase 1: mine 2-factor patterns
    if progress_callback:
        progress_callback({"stage": "progressive_phase1", "message": "Mining 2-factor patterns..."})
    summaries_2f = mine_asset_discovery_patterns(
        db_path=db_path,
        tickers_file=tickers_file,
        start_date=start_date,
        end_date=end_date,
        min_trade_return_pct=min_trade_return_pct,
        template_definitions=effective_templates,
        max_pattern_size=2,
        known_codes=None,
        progress_callback=progress_callback,
    )

    # Phase 2: identify promoted 2-factor feature key pairs, ranked by quality
    pair_candidates: list[tuple[float, frozenset[str]]] = []
    for summary in summaries_2f:
        if summary.state_size != 2:
            continue
        if summary.total_occurrences < pre_filter_min_trades:
            continue
        if summary.profitable_trade_rate_pct < pre_filter_min_profitable_rate:
            continue
        if summary.profit_factor is None or summary.profit_factor < pre_filter_min_profit_factor:
            continue
        keys = frozenset(summary.feature_keys.split(","))
        score = summary.profitable_trade_rate_pct + (summary.profit_factor or 0) * 10.0
        pair_candidates.append((score, keys))

    # Deduplicate keeping the best score per key pair
    best_by_pair: dict[frozenset[str], float] = {}
    for score, keys in pair_candidates:
        if keys not in best_by_pair or score > best_by_pair[keys]:
            best_by_pair[keys] = score
    ranked_pairs = sorted(best_by_pair.items(), key=lambda item: item[1], reverse=True)
    promoted_pairs: set[frozenset[str]] = {keys for keys, _ in ranked_pairs[:max_promoted_pairs]}

    if progress_callback:
        progress_callback({
            "stage": "progressive_phase2",
            "message": (
                f"Promoted {len(promoted_pairs)} 2-factor pairs to 3-factor expansion"
                f" (candidates={len(best_by_pair)}, limit={max_promoted_pairs})"
            ),
        })

    if not promoted_pairs:
        return summaries_2f

    # Phase 3: mine 3-factor patterns using progressive expansion
    codes_2f = {s.code for s in summaries_2f}
    accumulators: dict[str, _PatternAccumulator] = {}
    identity_cache: dict[tuple[str, tuple[tuple[str, str, str], ...]], tuple[str, str, str, str]] = {}
    started_at = monotonic()
    _last_pruned_ticker = [0]
    _pruned_total = [0]

    def _prune_weak_accumulators(processed: int, total: int) -> None:
        """Remove accumulators that cannot reach final thresholds given progress so far."""
        if total <= 0 or processed < prune_every_tickers:
            return
        if processed - _last_pruned_ticker[0] < prune_every_tickers:
            return
        _last_pruned_ticker[0] = processed
        progress_ratio = processed / total
        # After seeing 30%+ of tickers, require minimum trades proportional to progress
        if progress_ratio < 0.3:
            return
        # Patterns need >=200 trades total (CLI default); if at 50% progress and only 5 trades, prune
        min_trades_at_progress = max(3, int(20 * progress_ratio))
        # Also prune patterns with very few tickers relative to progress
        min_tickers_at_progress = max(1, int(5 * progress_ratio))
        to_remove = [
            code for code, acc in accumulators.items()
            if acc.total_occurrences < min_trades_at_progress
            or len(acc.tickers) < min_tickers_at_progress
        ]
        for code in to_remove:
            del accumulators[code]
        _pruned_total[0] += len(to_remove)
        # Also trim identity_cache to prevent unbounded growth
        if len(identity_cache) > max_accumulators * 2:
            identity_cache.clear()

    def _enforce_accumulator_limit() -> None:
        """Hard limit on accumulator count to prevent OOM."""
        if len(accumulators) <= max_accumulators:
            return
        # Keep top accumulators by trade count (more trades = more likely to be useful)
        sorted_codes = sorted(
            accumulators.keys(),
            key=lambda c: accumulators[c].total_occurrences,
        )
        to_remove = sorted_codes[: len(accumulators) - max_accumulators]
        for code in to_remove:
            del accumulators[code]
        _pruned_total[0] += len(to_remove)
        identity_cache.clear()

    def _handle_iter_progress(progress: dict[str, object]) -> None:
        processed = int(progress.get("processed_tickers", 0))
        total = int(progress.get("total_tickers", 0))
        # Prune weak accumulators periodically
        _prune_weak_accumulators(processed, total)
        # Enforce hard limit
        if len(accumulators) > max_accumulators:
            _enforce_accumulator_limit()
        if progress_callback is None:
            return
        payload = dict(progress)
        payload["stage"] = "mine_3f"
        payload["elapsed_seconds"] = monotonic() - started_at
        payload["patterns_3f"] = len(accumulators)
        payload["promoted_pairs"] = len(promoted_pairs)
        payload["pruned"] = _pruned_total[0]
        progress_callback(payload)

    for sample in _iter_asset_samples(
        db_path=db_path,
        tickers_file=tickers_file,
        start_date=start_date,
        end_date=end_date,
        template_definitions=effective_templates,
        progress_callback=_handle_iter_progress,
    ):
        pattern_items_list = _pattern_items_progressive(sample["feature_states"], promoted_pairs)
        for pattern_items in pattern_items_list:
            if len(pattern_items) != 3:
                continue  # 1f and 2f already covered in summaries_2f
            identity_cache_key = (sample["template"].code, pattern_items)
            identity = identity_cache.get(identity_cache_key)
            if identity is None:
                identity = _build_pattern_identity(sample["template"], pattern_items)
                identity_cache[identity_cache_key] = identity
            code, label, state_signature, feature_keys = identity
            if code in ignored_codes or code in codes_2f:
                continue
            accumulator = accumulators.get(code)
            if accumulator is None:
                # Skip creating new accumulators if at hard limit
                if len(accumulators) >= max_accumulators:
                    continue
                accumulator = _PatternAccumulator(
                    code=code,
                    label=label,
                    family=sample["template"].family,
                    template_code=sample["template"].code,
                    template_label=sample["template"].label,
                    trade_direction=sample["template"].trade_direction,
                    entry_rule=sample["template"].entry_rule,
                    take_profit_pct=sample["template"].take_profit_pct,
                    stop_loss_pct=sample["template"].stop_loss_pct,
                    time_cap_days=sample["template"].time_cap_days,
                    state_size=len(pattern_items),
                    state_signature=state_signature,
                    feature_keys=feature_keys,
                    min_trade_return_pct=min_trade_return_pct,
                    tickers=set(),
                    exit_mode=sample["template"].exit_mode,
                    atr_target_mult=sample["template"].atr_target_mult,
                    atr_stop_mult=sample["template"].atr_stop_mult,
                )
                accumulators[code] = accumulator

            accumulator.total_occurrences += 1
            accumulator.tickers.add(sample["ticker"])
            accumulator.sum_asset_move_pct += sample["asset_move_pct"]
            accumulator.sum_trade_return_pct += sample["trade_return_pct"]
            if sample["trade_return_pct"] >= min_trade_return_pct:
                accumulator.successful_occurrences += 1
            if sample["trade_return_pct"] > 0:
                accumulator.profitable_trades += 1
                accumulator.gross_profit_pct += sample["trade_return_pct"]
            elif sample["trade_return_pct"] < 0:
                accumulator.gross_loss_pct += -sample["trade_return_pct"]
            exit_reason = str(sample["exit_reason"])
            if exit_reason == "take_profit":
                accumulator.take_profit_trades += 1
            elif exit_reason.startswith("stop_loss"):
                accumulator.stop_loss_trades += 1
            elif exit_reason == "time_cap":
                accumulator.time_cap_trades += 1
            multiplier = 1.0 + (sample["trade_return_pct"] / 100.0)
            if multiplier <= 0:
                accumulator.zero_cumulative_multiplier = True
            else:
                accumulator.log_cumulative_multiplier += log(multiplier)
            trade_date = sample["trade_date"]
            if accumulator.first_trade_date is None or trade_date < accumulator.first_trade_date:
                accumulator.first_trade_date = trade_date
            if accumulator.last_trade_date is None or trade_date > accumulator.last_trade_date:
                accumulator.last_trade_date = trade_date

    # Build 3-factor summaries
    summaries_3f: list[AssetDiscoveryPatternSummary] = []
    for accumulator in accumulators.values():
        profit_factor = _calculate_profit_factor(accumulator.gross_profit_pct, accumulator.gross_loss_pct)
        cumulative_return_pct = (
            -100.0 if accumulator.zero_cumulative_multiplier else (exp(accumulator.log_cumulative_multiplier) - 1.0) * 100.0
        )
        summaries_3f.append(
            AssetDiscoveryPatternSummary(
                code=accumulator.code,
                label=accumulator.label,
                family=accumulator.family,
                template_code=accumulator.template_code,
                template_label=accumulator.template_label,
                trade_direction=accumulator.trade_direction,
                entry_rule=accumulator.entry_rule,
                take_profit_pct=accumulator.take_profit_pct,
                stop_loss_pct=accumulator.stop_loss_pct,
                time_cap_days=accumulator.time_cap_days,
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
                take_profit_trades=accumulator.take_profit_trades,
                take_profit_rate_pct=(accumulator.take_profit_trades / accumulator.total_occurrences) * 100.0,
                stop_loss_trades=accumulator.stop_loss_trades,
                stop_loss_rate_pct=(accumulator.stop_loss_trades / accumulator.total_occurrences) * 100.0,
                time_cap_trades=accumulator.time_cap_trades,
                time_cap_rate_pct=(accumulator.time_cap_trades / accumulator.total_occurrences) * 100.0,
                average_asset_move_pct=accumulator.sum_asset_move_pct / accumulator.total_occurrences,
                average_trade_return_pct=accumulator.sum_trade_return_pct / accumulator.total_occurrences,
                net_trade_return_pct=accumulator.sum_trade_return_pct,
                cumulative_return_pct=cumulative_return_pct,
                profit_factor=profit_factor,
                first_trade_date=accumulator.first_trade_date or "",
                last_trade_date=accumulator.last_trade_date or "",
                exit_mode=accumulator.exit_mode,
                atr_target_mult=accumulator.atr_target_mult,
                atr_stop_mult=accumulator.atr_stop_mult,
            )
        )

    # Merge 2f + 3f results
    all_summaries = summaries_2f + summaries_3f
    return sorted(
        all_summaries,
        key=lambda item: (
            item.net_trade_return_pct,
            item.profit_factor if item.profit_factor is not None else float("-inf"),
            item.average_trade_return_pct,
            item.total_occurrences,
        ),
        reverse=True,
    )


def qualify_asset_discovery_summary(
    summary: AssetDiscoveryPatternSummary,
    *,
    min_success_rate_pct: float = 0.0,
    min_take_profit_rate_pct: float = 0.0,
    min_profit_factor: float = 1.10,
    min_average_trade_return_pct: float = 0.0,
    min_trades: int = 50,
    min_tickers: int = 5,
    require_positive_net: bool = True,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if summary.success_rate_pct < min_success_rate_pct:
        reasons.append(f"success_rate<{min_success_rate_pct:.2f}")
    if summary.take_profit_rate_pct < min_take_profit_rate_pct:
        reasons.append(f"take_profit_rate<{min_take_profit_rate_pct:.2f}")
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


def split_asset_discovery_results(
    summaries: list[AssetDiscoveryPatternSummary],
    *,
    min_success_rate_pct: float = 0.0,
    min_take_profit_rate_pct: float = 0.0,
    min_profit_factor: float = 1.10,
    min_average_trade_return_pct: float = 0.0,
    min_trades: int = 50,
    min_tickers: int = 5,
    require_positive_net: bool = True,
) -> tuple[list[AssetDiscoveryPatternSummary], list[AssetDiscoveryPatternSummary], dict[str, list[str]]]:
    approved: list[AssetDiscoveryPatternSummary] = []
    rejected: list[AssetDiscoveryPatternSummary] = []
    rejection_reasons: dict[str, list[str]] = {}
    for summary in summaries:
        is_approved, reasons = qualify_asset_discovery_summary(
            summary,
            min_success_rate_pct=min_success_rate_pct,
            min_take_profit_rate_pct=min_take_profit_rate_pct,
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


def build_asset_discovery_registry_entries(
    approved_summaries: list[AssetDiscoveryPatternSummary],
    rejected_summaries: list[AssetDiscoveryPatternSummary],
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
                trigger_direction=summary.trade_direction,
                threshold_pct=float(summary.state_size),
                trade_direction=summary.trade_direction,
                entry_rule=summary.entry_rule,
                exit_offset_days=summary.time_cap_days,
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
                trigger_direction=summary.trade_direction,
                threshold_pct=float(summary.state_size),
                trade_direction=summary.trade_direction,
                entry_rule=summary.entry_rule,
                exit_offset_days=summary.time_cap_days,
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


def collect_asset_discovery_pattern_trades(
    *,
    db_path: str | Path,
    tickers_file: str | Path,
    start_date: str,
    end_date: str,
    approved_summaries: list[AssetDiscoveryPatternSummary],
    template_definitions: list[AssetDiscoveryTemplate] | None = None,
    max_pattern_size: int = 2,
    progress_callback: Callable[[dict[str, object]], None] | None = None,
) -> list[StrategyTrade]:
    effective_templates = template_definitions or build_asset_discovery_round1_templates()
    approved_by_code = {item.code: item for item in approved_summaries}
    if not approved_by_code:
        return []

    trades: list[StrategyTrade] = []
    pattern_items_cache: dict[tuple[tuple[tuple[str, str, str], ...], int], list[tuple[tuple[str, str, str], ...]]] = {}
    identity_cache: dict[tuple[str, tuple[tuple[str, str, str], ...]], str] = {}
    started_at = monotonic()

    def _handle_iter_progress(progress: dict[str, object]) -> None:
        if progress_callback is None:
            return
        payload = dict(progress)
        payload["stage"] = "trades"
        payload["elapsed_seconds"] = monotonic() - started_at
        payload["approved_patterns"] = len(approved_by_code)
        payload["trades"] = len(trades)
        progress_callback(payload)

    for sample in _iter_asset_samples(
        db_path=db_path,
        tickers_file=tickers_file,
        start_date=start_date,
        end_date=end_date,
        template_definitions=effective_templates,
        progress_callback=_handle_iter_progress,
    ):
        feature_signature = tuple(
            (feature.key, *sample["feature_states"][feature.key])
            for feature in FEATURE_LIBRARY
            if feature.key in sample["feature_states"]
        )
        pattern_cache_key = (feature_signature, max_pattern_size)
        pattern_items_list = pattern_items_cache.get(pattern_cache_key)
        if pattern_items_list is None:
            pattern_items_list = _pattern_items(sample["feature_states"], max_pattern_size=max_pattern_size)
            pattern_items_cache[pattern_cache_key] = pattern_items_list

        for pattern_items in pattern_items_list:
            identity_cache_key = (sample["template"].code, pattern_items)
            code = identity_cache.get(identity_cache_key)
            if code is None:
                code, _, _, _ = _build_pattern_identity(sample["template"], pattern_items)
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
                    exit_date=sample["exit_date"],
                    direction=summary.trade_direction,
                    trigger_change_pct=sample["trigger_change_pct"],
                    entry_price=sample["entry_price"],
                    exit_price=sample["exit_price"],
                    asset_move_pct=sample["asset_move_pct"],
                    trade_return_pct=sample["trade_return_pct"],
                    is_profitable=sample["trade_return_pct"] > 0,
                    is_successful=sample["trade_return_pct"] >= summary.min_trade_return_pct,
                    exit_reason=sample["exit_reason"],
                )
            )
    return sorted(trades, key=lambda item: (item.strategy_code, item.trigger_date, item.ticker))


def collect_asset_discovery_pattern_trades_to_csv(
    *,
    db_path: str | Path,
    tickers_file: str | Path,
    start_date: str,
    end_date: str,
    approved_summaries: list[AssetDiscoveryPatternSummary],
    output_path: str | Path,
    template_definitions: list[AssetDiscoveryTemplate] | None = None,
    max_pattern_size: int = 2,
    progress_callback: Callable[[dict[str, object]], None] | None = None,
) -> list[StrategyTickerSummary]:
    effective_templates = template_definitions or build_asset_discovery_round1_templates()
    approved_by_code = {item.code: item for item in approved_summaries}
    if not approved_by_code:
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
        return []

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    ticker_accumulators: dict[tuple[str, str], _TickerTradeAccumulator] = {}
    pattern_items_cache: dict[tuple[tuple[tuple[str, str, str], ...], int], list[tuple[tuple[str, str, str], ...]]] = {}
    identity_cache: dict[tuple[str, tuple[tuple[str, str, str], ...]], str] = {}
    started_at = monotonic()
    total_trades = 0

    def _handle_iter_progress(progress: dict[str, object]) -> None:
        if progress_callback is None:
            return
        payload = dict(progress)
        payload["stage"] = "trades"
        payload["elapsed_seconds"] = monotonic() - started_at
        payload["approved_patterns"] = len(approved_by_code)
        payload["trades"] = total_trades
        progress_callback(payload)

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

        for sample in _iter_asset_samples(
            db_path=db_path,
            tickers_file=tickers_file,
            start_date=start_date,
            end_date=end_date,
            template_definitions=effective_templates,
            progress_callback=_handle_iter_progress,
        ):
            feature_signature = tuple(
                (feature.key, *sample["feature_states"][feature.key])
                for feature in FEATURE_LIBRARY
                if feature.key in sample["feature_states"]
            )
            pattern_cache_key = (feature_signature, max_pattern_size)
            pattern_items_list = pattern_items_cache.get(pattern_cache_key)
            if pattern_items_list is None:
                pattern_items_list = _pattern_items(sample["feature_states"], max_pattern_size=max_pattern_size)
                pattern_items_cache[pattern_cache_key] = pattern_items_list

            for pattern_items in pattern_items_list:
                identity_cache_key = (sample["template"].code, pattern_items)
                code = identity_cache.get(identity_cache_key)
                if code is None:
                    code, _, _, _ = _build_pattern_identity(sample["template"], pattern_items)
                    identity_cache[identity_cache_key] = code
                summary = approved_by_code.get(code)
                if summary is None:
                    continue

                trade_return_pct = sample["trade_return_pct"]
                is_profitable = trade_return_pct > 0
                is_successful = trade_return_pct >= summary.min_trade_return_pct
                writer.writerow(
                    {
                        "strategy_code": summary.code,
                        "strategy_label": summary.label,
                        "family": summary.family,
                        "ticker": sample["ticker"],
                        "trigger_date": sample["trade_date"],
                        "exit_date": sample["exit_date"],
                        "direction": summary.trade_direction,
                        "trigger_change_pct": f"{sample['trigger_change_pct']:.4f}",
                        "entry_price": f"{sample['entry_price']:.4f}",
                        "exit_price": f"{sample['exit_price']:.4f}",
                        "asset_move_pct": f"{sample['asset_move_pct']:.4f}",
                        "trade_return_pct": f"{trade_return_pct:.4f}",
                        "is_profitable": is_profitable,
                        "is_successful": is_successful,
                        "instrument_symbol": "",
                        "contract_expiration": "",
                        "dte_target_days": "",
                        "exit_reason": sample["exit_reason"] or "",
                    }
                )
                total_trades += 1
                if total_trades % 100000 == 0:
                    file_obj.flush()

                accumulator_key = (summary.code, sample["ticker"])
                accumulator = ticker_accumulators.get(accumulator_key)
                if accumulator is None:
                    accumulator = _TickerTradeAccumulator(
                        strategy_code=summary.code,
                        strategy_label=summary.label,
                        family=summary.family,
                        ticker=sample["ticker"],
                    )
                    ticker_accumulators[accumulator_key] = accumulator
                accumulator.update(
                    trade_return_pct=trade_return_pct,
                    is_successful=is_successful,
                    is_profitable=is_profitable,
                    trigger_date=sample["trade_date"],
                )

    summaries: list[StrategyTickerSummary] = []
    for accumulator in ticker_accumulators.values():
        total = accumulator.total_trades
        average_trade_return_pct = accumulator.sum_trade_return_pct / total if total else 0.0
        cumulative_return_pct = -100.0 if accumulator.zero_cumulative_multiplier else (exp(accumulator.log_cumulative_multiplier) - 1.0) * 100.0
        summaries.append(
            StrategyTickerSummary(
                strategy_code=accumulator.strategy_code,
                strategy_label=accumulator.strategy_label,
                family=accumulator.family,
                ticker=accumulator.ticker,
                total_trades=total,
                successful_trades=accumulator.successful_trades,
                success_rate_pct=(accumulator.successful_trades / total) * 100.0 if total else 0.0,
                profitable_trades=accumulator.profitable_trades,
                profitable_trade_rate_pct=(accumulator.profitable_trades / total) * 100.0 if total else 0.0,
                average_trade_return_pct=average_trade_return_pct,
                # Streaming mode avoids retaining every trade in memory; keep the field populated
                # with the average so downstream CSV/export code remains compatible.
                median_trade_return_pct=average_trade_return_pct,
                net_trade_return_pct=accumulator.sum_trade_return_pct,
                cumulative_return_pct=cumulative_return_pct,
                profit_factor=_calculate_profit_factor(
                    accumulator.gross_profit_pct,
                    accumulator.gross_loss_pct,
                ),
                first_trade_date=accumulator.first_trade_date or "",
                last_trade_date=accumulator.last_trade_date or "",
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


def render_asset_discovery_report(results: list[AssetDiscoveryPatternSummary], top: int = 20) -> str:
    if not results:
        return "Nenhum padrao quantitativo em acoes encontrou ocorrencias com os parametros informados."
    headers = (("Padrao", 90), ("Trades", 8), ("Tickers", 8), ("Win(%)", 9), ("Alvo(%)", 9), ("Med BT(%)", 11), ("Net BT(%)", 11), ("PF", 8))
    header_line = " ".join(label.ljust(width) for label, width in headers)
    separator = "-" * len(header_line)
    lines = [header_line, separator]
    for item in results[:top]:
        pf = "n/a" if item.profit_factor is None else "INF" if item.profit_factor == float("inf") else f"{item.profit_factor:.2f}"
        lines.append(
            " ".join(
                [
                    item.label.ljust(90),
                    str(item.total_occurrences).rjust(8),
                    str(item.tickers_with_matches).rjust(8),
                    f"{item.profitable_trade_rate_pct:>9.2f}",
                    f"{item.take_profit_rate_pct:>9.2f}",
                    f"{item.average_trade_return_pct:>11.2f}",
                    f"{item.net_trade_return_pct:>11.2f}",
                    pf.rjust(8),
                ]
            )
        )
    return "\n".join(lines)


def export_asset_discovery_csv(results: list[AssetDiscoveryPatternSummary], output_path: str | Path) -> Path:
    import csv
    from math import isinf

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(
            file_obj,
            fieldnames=[
                "code", "label", "family", "template_code", "template_label", "trade_direction", "entry_rule",
                "take_profit_pct", "stop_loss_pct", "time_cap_days", "state_size", "state_signature", "feature_keys",
                "min_trade_return_pct", "tickers_with_matches", "total_occurrences", "successful_occurrences",
                "success_rate_pct", "profitable_trades", "profitable_trade_rate_pct",
                "take_profit_trades", "take_profit_rate_pct", "stop_loss_trades", "stop_loss_rate_pct",
                "time_cap_trades", "time_cap_rate_pct", "average_asset_move_pct",
                "average_trade_return_pct", "net_trade_return_pct", "cumulative_return_pct", "profit_factor",
                "first_trade_date", "last_trade_date",
                "exit_mode", "atr_target_mult", "atr_stop_mult",
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
                    "trade_direction": item.trade_direction,
                    "entry_rule": item.entry_rule,
                    "take_profit_pct": f"{item.take_profit_pct:.4f}",
                    "stop_loss_pct": f"{item.stop_loss_pct:.4f}",
                    "time_cap_days": item.time_cap_days,
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
                    "take_profit_trades": item.take_profit_trades,
                    "take_profit_rate_pct": f"{item.take_profit_rate_pct:.4f}",
                    "stop_loss_trades": item.stop_loss_trades,
                    "stop_loss_rate_pct": f"{item.stop_loss_rate_pct:.4f}",
                    "time_cap_trades": item.time_cap_trades,
                    "time_cap_rate_pct": f"{item.time_cap_rate_pct:.4f}",
                    "average_asset_move_pct": f"{item.average_asset_move_pct:.4f}",
                    "average_trade_return_pct": f"{item.average_trade_return_pct:.4f}",
                    "net_trade_return_pct": f"{item.net_trade_return_pct:.4f}",
                    "cumulative_return_pct": f"{item.cumulative_return_pct:.4f}",
                    "profit_factor": "INF" if item.profit_factor is not None and isinf(item.profit_factor) else "" if item.profit_factor is None else f"{item.profit_factor:.4f}",
                    "first_trade_date": item.first_trade_date,
                    "last_trade_date": item.last_trade_date,
                    "exit_mode": item.exit_mode,
                    "atr_target_mult": f"{item.atr_target_mult:.4f}" if item.atr_target_mult else "",
                    "atr_stop_mult": f"{item.atr_stop_mult:.4f}" if item.atr_stop_mult else "",
                }
            )
    return path


def export_asset_discovery_markdown(
    approved_results: list[AssetDiscoveryPatternSummary],
    rejected_results: list[AssetDiscoveryPatternSummary],
    *,
    output_path: str | Path,
    start_date: str,
    end_date: str,
    features: list[AssetDiscoveryFeature],
    templates: list[AssetDiscoveryTemplate],
) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Discovery Quantitativo em Acoes - Rodada 1",
        "",
        f"- Janela: {start_date} ate {end_date}",
        f"- Features avaliadas: {len(features)}",
        f"- Templates avaliados: {len(templates)}",
        f"- Padroes aprovados: {len(approved_results)}",
        f"- Padroes reprovados: {len(rejected_results)}",
        "",
        "## Biblioteca de Features",
        "",
        "| Chave | Feature | Pairable |",
        "| --- | --- | --- |",
    ]
    for feature in features:
        lines.append(f"| {feature.key} | {feature.label} | {'sim' if feature.pairable else 'nao'} |")
    lines.extend(
        [
            "",
            "## Aprovados",
            "",
            "| Padrao | Trades | Tickers | Win % | Alvo % | Media/trade % | Net % | PF |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for item in approved_results[:100]:
        pf = "n/a" if item.profit_factor is None else "INF" if item.profit_factor == float("inf") else f"{item.profit_factor:.2f}"
        lines.append(f"| {item.label} | {item.total_occurrences} | {item.tickers_with_matches} | {item.profitable_trade_rate_pct:.2f} | {item.take_profit_rate_pct:.2f} | {item.average_trade_return_pct:.4f} | {item.net_trade_return_pct:.4f} | {pf} |")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def default_asset_discovery_window(*, start_date: str | None, end_date: str | None) -> tuple[str, str]:
    end_day = date.fromisoformat(end_date) if end_date else date.today()
    start_day = date.fromisoformat(start_date) if start_date else (end_day - timedelta(days=365))
    return start_day.isoformat(), end_day.isoformat()
