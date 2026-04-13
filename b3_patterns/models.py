from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class SpotQuoteBar:
    ticker: str
    trade_date: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    trade_count: int


@dataclass(slots=True)
class OptionQuoteBar:
    option_symbol: str
    underlying_root: str
    option_side: str
    trade_date: str
    expiration_date: str
    strike_price: float
    open: float
    high: float
    low: float
    close: float
    volume: int
    trade_count: int


@dataclass(slots=True)
class OptionStrategyDefinition:
    code: str
    label: str
    family: str
    setup_kind: str
    trigger_direction: str
    threshold_pct: float
    option_side: str
    dte_target_days: int
    entry_rule: str
    holding_days: int
    min_trade_return_pct: float
    round_trip_cost_pct: float
    lookback_days: int | None = None
    fast_ma_days: int | None = None
    slow_ma_days: int | None = None
    exit_kind: str = "time"
    take_profit_pct: float | None = None
    stop_loss_pct: float | None = None


@dataclass(slots=True, frozen=True)
class OptionDiscoveryTemplate:
    code: str
    label: str
    family: str
    option_side: str
    dte_target_days: int
    trade_direction: str
    entry_rule: str = "open"
    holding_days: int = 0
    round_trip_cost_pct: float = 0.0


@dataclass(slots=True)
class AdjustedBar:
    ticker: str
    trade_date: str
    open: float
    high: float
    low: float
    close: float


@dataclass(slots=True)
class PriceBar:
    ticker: str
    trade_date: str
    open: float
    high: float
    low: float
    close: float
    adj_close: float
    volume: int


@dataclass(slots=True)
class PatternResult:
    ticker: str
    occurrences: int
    successful_occurrences: int
    success_rate_pct: float
    average_next_day_return_pct: float
    median_next_day_return_pct: float


@dataclass(slots=True)
class StrategyDefinition:
    code: str
    label: str
    family: str
    setup_kind: str
    trigger_direction: str
    threshold_pct: float
    trade_direction: str
    entry_rule: str
    exit_offset_days: int
    min_trade_return_pct: float


@dataclass(slots=True)
class StrategySummary:
    code: str
    label: str
    family: str
    setup_kind: str
    trigger_direction: str
    threshold_pct: float
    trade_direction: str
    entry_rule: str
    exit_offset_days: int
    min_trade_return_pct: float
    tickers_with_matches: int
    total_occurrences: int
    successful_occurrences: int
    success_rate_pct: float
    average_asset_move_pct: float
    median_asset_move_pct: float
    profitable_trades: int
    profitable_trade_rate_pct: float
    average_trade_return_pct: float
    median_trade_return_pct: float
    net_trade_return_pct: float
    cumulative_return_pct: float
    profit_factor: float | None


@dataclass(slots=True)
class StrategyTrade:
    strategy_code: str
    strategy_label: str
    family: str
    ticker: str
    trigger_date: str
    exit_date: str
    direction: str
    trigger_change_pct: float
    entry_price: float
    exit_price: float
    asset_move_pct: float
    trade_return_pct: float
    is_profitable: bool
    is_successful: bool
    instrument_symbol: str | None = None
    contract_expiration: str | None = None
    dte_target_days: int | None = None
    exit_reason: str | None = None


@dataclass(slots=True)
class StrategyTickerSummary:
    strategy_code: str
    strategy_label: str
    family: str
    ticker: str
    total_trades: int
    successful_trades: int
    success_rate_pct: float
    profitable_trades: int
    profitable_trade_rate_pct: float
    average_trade_return_pct: float
    median_trade_return_pct: float
    net_trade_return_pct: float
    cumulative_return_pct: float
    profit_factor: float | None
    first_trade_date: str
    last_trade_date: str


@dataclass(slots=True)
class StrategyRegistryEntry:
    code: str
    label: str
    family: str
    setup_kind: str
    trigger_direction: str
    threshold_pct: float
    trade_direction: str
    entry_rule: str
    exit_offset_days: int
    min_trade_return_pct: float
    status: str
    rejection_reasons: str
    tested_at: str
    total_occurrences: int
    success_rate_pct: float
    profitable_trade_rate_pct: float
    average_trade_return_pct: float
    net_trade_return_pct: float
    profit_factor: float | None


@dataclass(slots=True)
class DiscoveryPatternSummary:
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
    tickers_with_matches: int
    total_occurrences: int
    successful_occurrences: int
    success_rate_pct: float
    profitable_trades: int
    profitable_trade_rate_pct: float
    average_asset_move_pct: float
    average_trade_return_pct: float
    net_trade_return_pct: float
    cumulative_return_pct: float
    profit_factor: float | None
    first_trade_date: str
    last_trade_date: str


@dataclass(slots=True)
class DiscoveryTemplateBaseline:
    template_code: str
    template_label: str
    option_side: str
    dte_target_days: int
    total_occurrences: int
    profitable_trades: int
    profitable_trade_rate_pct: float
    average_trade_return_pct: float
    net_trade_return_pct: float
    profit_factor: float | None
    first_trade_date: str
    last_trade_date: str


@dataclass(slots=True)
class DiscoveryRefinedSummary:
    code: str
    label: str
    family: str
    template_code: str
    template_label: str
    option_side: str
    dte_target_days: int
    trade_direction: str
    state_size: int
    state_signature: str
    feature_keys: str
    tickers_with_matches: int
    total_occurrences: int
    profitable_trade_rate_pct: float
    average_trade_return_pct: float
    net_trade_return_pct: float
    profit_factor: float | None
    baseline_average_trade_return_pct: float
    baseline_profit_factor: float | None
    average_trade_uplift_pct: float
    profit_factor_uplift: float | None
    train_trades: int
    train_average_trade_return_pct: float
    train_net_trade_return_pct: float
    train_profit_factor: float | None
    validation_trades: int
    validation_average_trade_return_pct: float
    validation_net_trade_return_pct: float
    validation_profit_factor: float | None
    active_months: int
    positive_months: int
    positive_month_ratio: float
    validation_active_months: int
    validation_positive_months: int
    validation_positive_month_ratio: float
    overlap_bucket: str
    robustness_score: float
