"""Shim de compatibilidade R7 → b3_patterns.asset_discovery_round1.

Reconstrucao apos o repo cleanup (commit 4349595) que removeu R7/ sem atualizar
os engines R10/R11. Expoe a API que r10_engine.py e r11_engine.py importam:

    Template, Outcome, _pf, _rate, _combo_code, _combo_label, _prepare_ticker_samples

Toda a logica pesada (features, simulacao de saida) e delegada a
b3_patterns.asset_discovery_round1 — apenas tradutor de tipos + walker por ticker.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from b3_patterns.asset_discovery_round1 import (  # noqa: E402
    FEATURE_BY_KEY,
    FEATURE_LIBRARY,
    _atr_pct_series,
    _build_feature_states,
    _ema_series,
    _prefix_sums,
    _prefix_sums_sq,
    _rsi_series,
    _simulate_percent_exit,
)


@dataclass(slots=True, frozen=True)
class Template:
    code: str
    label: str
    entry_rule: str          # "open" | "close"
    exit_mode: str           # "target_stop_swing" | "target_stop_same_day" |
                             # "fixed_close" | "fixed_next_open" | "fixed_next_close"
    target_pct: float        # so usado em target_stop_*
    stop_pct: float          # so usado em target_stop_*
    holding_days: int        # cap para target_stop_swing; 0 p/ same_day; ignorado em fixed_*
    success_return_pct: float
    strategy_prefix: str


@dataclass(slots=True)
class Outcome:
    trade_date: str
    success: bool
    profitable: bool
    take_profit: bool
    stop_loss: bool
    trade_return_pct: float


def _pf(gross_profit: float, gross_loss: float) -> float:
    if gross_loss <= 0:
        return float("inf") if gross_profit > 0 else 0.0
    return gross_profit / gross_loss


def _rate(success: int, trades: int) -> float:
    if trades <= 0:
        return 0.0
    return 100.0 * success / trades


def _combo_code(combo) -> str:
    return "__".join(f"{key}={code}" for key, code, _ in combo)


def _combo_label(combo) -> str:
    parts = []
    for key, _, value_label in combo:
        feature = FEATURE_BY_KEY.get(key)
        feature_label = feature.label if feature is not None else key
        parts.append(f"{feature_label}: {value_label}")
    return "; ".join(parts)


def _outcome_target_stop(
    template: Template,
    bars,
    idx: int,
    entry_price: float,
    entry_rule: str,
):
    result = _simulate_percent_exit(
        bars=bars,
        entry_idx=idx,
        entry_price=entry_price,
        trade_direction="long",
        take_profit_pct=template.target_pct,
        stop_loss_pct=template.stop_pct,
        time_cap_days=template.holding_days,
        include_entry_bar=(entry_rule == "open"),
    )
    if result is None:
        return None
    _, exit_price, reason = result
    ret_pct = ((exit_price / entry_price) - 1.0) * 100.0
    return Outcome(
        trade_date=bars[idx].trade_date,
        success=(ret_pct >= template.success_return_pct),
        profitable=(ret_pct > 0),
        take_profit=(reason == "take_profit"),
        stop_loss=(reason in ("stop_loss", "stop_loss_conflict")),
        trade_return_pct=ret_pct,
    )


def _outcome_fixed(template: Template, bars, idx: int, entry_price: float):
    if template.exit_mode == "fixed_close":
        exit_price = bars[idx].close
    elif template.exit_mode == "fixed_next_open":
        if idx + 1 >= len(bars):
            return None
        exit_price = bars[idx + 1].open
    elif template.exit_mode == "fixed_next_close":
        if idx + 1 >= len(bars):
            return None
        exit_price = bars[idx + 1].close
    else:
        return None
    if exit_price <= 0:
        return None
    ret_pct = ((exit_price / entry_price) - 1.0) * 100.0
    return Outcome(
        trade_date=bars[idx].trade_date,
        success=(ret_pct >= template.success_return_pct),
        profitable=(ret_pct > 0),
        take_profit=False,
        stop_loss=False,
        trade_return_pct=ret_pct,
    )


def _prepare_ticker_samples(
    ticker: str,
    bars,
    templates,
    mining_start,
    oos_end,
) -> dict[str, list]:
    """Para cada template, devolve list[(items, outcome)] sobre [mining_start, oos_end]."""
    start_day = mining_start if isinstance(mining_start, date) else date.fromisoformat(mining_start)
    end_day = oos_end if isinstance(oos_end, date) else date.fromisoformat(oos_end)

    if not bars:
        return {t.code: [] for t in templates}

    opens = [b.open for b in bars]
    highs = [b.high for b in bars]
    lows = [b.low for b in bars]
    closes = [b.close for b in bars]
    volumes = [float(b.volume) for b in bars]
    dollar_volumes = [closes[i] * volumes[i] for i in range(len(bars))]
    range_pcts = [
        0.0 if closes[i] <= 0 else ((highs[i] - lows[i]) / closes[i]) * 100.0
        for i in range(len(bars))
    ]
    daily_returns = [0.0]
    for i in range(1, len(bars)):
        prev = closes[i - 1]
        daily_returns.append(0.0 if prev <= 0 else ((closes[i] / prev) - 1.0) * 100.0)

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
    ema200_values = _ema_series(closes, period=200)
    trade_dates_list = [b.trade_date for b in bars]

    samples_by_template: dict[str, list] = {t.code: [] for t in templates}

    templates_by_entry: dict[str, list[Template]] = {"open": [], "close": []}
    for t in templates:
        templates_by_entry.setdefault(t.entry_rule, []).append(t)

    for idx in range(1, len(bars)):
        current = bars[idx]
        try:
            trade_day = date.fromisoformat(current.trade_date)
        except ValueError:
            continue
        if trade_day < start_day or trade_day > end_day:
            continue

        for entry_rule in ("open", "close"):
            templates_here = templates_by_entry.get(entry_rule)
            if not templates_here:
                continue
            entry_price = current.open if entry_rule == "open" else current.close
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
                ema200_values=ema200_values,
                trade_dates=trade_dates_list,
            )
            if not feature_states:
                continue

            items = tuple(
                (f.key, *feature_states[f.key])
                for f in FEATURE_LIBRARY
                if f.key in feature_states
            )

            for template in templates_here:
                if template.exit_mode in ("target_stop_swing", "target_stop_same_day"):
                    outcome = _outcome_target_stop(template, bars, idx, entry_price, entry_rule)
                elif template.exit_mode in ("fixed_close", "fixed_next_open", "fixed_next_close"):
                    outcome = _outcome_fixed(template, bars, idx, entry_price)
                else:
                    outcome = None
                if outcome is None:
                    continue
                samples_by_template[template.code].append((items, outcome))

    return samples_by_template
