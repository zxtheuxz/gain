from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import date
from math import isfinite
from pathlib import Path

from .asset_discovery_round1 import (
    FEATURE_BY_KEY,
    _atr_pct_series,
    _build_feature_states,
    _ema_series,
    _prefix_sums,
    _prefix_sums_sq,
    _rsi_series,
)
from .options import _load_spot_bars
from .tickers import load_tickers


JSON_INF_SENTINEL = 9999.0
OPERATIONAL_MIN_SIGNALS = 3


def _parse_float(value: str | None) -> float:
    if value in (None, "", "None"):
        return 0.0
    parsed_value = float(value)
    if not isfinite(parsed_value):
        return JSON_INF_SENTINEL
    return parsed_value


def _parse_int(value: str | None) -> int:
    if value in (None, ""):
        return 0
    return int(float(value))


def _load_csv_rows(path: str | Path) -> list[dict[str, str]]:
    csv_path = Path(path)
    with csv_path.open(encoding="utf-8", newline="") as file_obj:
        return list(csv.DictReader(file_obj))


def _parse_strategy_conditions(code: str, state_size: int) -> list[tuple[str, str]]:
    marker = f"__{state_size}f__"
    _, _, suffix = code.partition(marker)
    if not suffix:
        return []
    conditions: list[tuple[str, str]] = []
    for part in suffix.split("__"):
        key, _, bucket_code = part.partition("=")
        if key and bucket_code:
            conditions.append((key, bucket_code))
    return conditions


def _state_explanation(entry_rule: str, trade_direction: str, take_profit_pct: float, stop_loss_pct: float, time_cap_days: int) -> str:
    action = "Comprar" if trade_direction == "long" else "Vender"
    return (
        f"{action} no {entry_rule}, alvo de +{take_profit_pct:.1f}% "
        f"e stop de -{stop_loss_pct:.1f}% com limite maximo de {time_cap_days} dias."
    )


def build_asset_monitor_payload(
    *,
    db_path: str | Path,
    tickers_file: str | Path,
    strategies_csv: str | Path,
    ticker_stats_csv: str | Path | None = None,
    overall_actions_csv: str | Path | None = None,
    as_of_date: str | None = None,
    top_strategies: int | None = None,
) -> dict[str, object]:
    strategy_rows = _load_csv_rows(strategies_csv)
    total_strategies_available = len(strategy_rows)
    if top_strategies is not None and top_strategies > 0:
        strategy_rows = strategy_rows[:top_strategies]
    strategies = []
    strategies_by_entry_rule: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in strategy_rows:
        take_profit_pct = _parse_float(row.get("take_profit_pct") or row.get("target_pct"))
        stop_loss_pct = _parse_float(row.get("stop_loss_pct") or row.get("stop_pct"))
        strategy = {
            "code": row["code"],
            "label": row["label"],
            "trade_direction": row.get("trade_direction", "long"),
            "entry_rule": row["entry_rule"],
            "take_profit_pct": take_profit_pct,
            "stop_loss_pct": stop_loss_pct,
            "time_cap_days": _parse_int(row.get("time_cap_days")),
            "state_size": _parse_int(row["state_size"]),
            "state_signature": row["state_signature"],
            "feature_keys": row["feature_keys"],
            "tickers_with_matches": _parse_int(row.get("tickers_with_matches") or row.get("tickers")),
            "total_occurrences": _parse_int(row.get("total_occurrences") or row.get("trades")),
            "success_rate_pct": _parse_float(row.get("success_rate_pct") or row.get("profitable_rate_pct")),
            "profitable_trade_rate_pct": _parse_float(row.get("profitable_trade_rate_pct") or row.get("profitable_rate_pct")),
            "average_trade_return_pct": _parse_float(row["average_trade_return_pct"]),
            "net_trade_return_pct": _parse_float(row["net_trade_return_pct"]),
            "profit_factor": _parse_float(row["profit_factor"]),
            "conditions": _parse_strategy_conditions(row["code"], _parse_int(row["state_size"])),
        }
        strategy["how_to_operate"] = _state_explanation(
            strategy["entry_rule"],
            strategy["trade_direction"],
            strategy["take_profit_pct"],
            strategy["stop_loss_pct"],
            strategy["time_cap_days"],
        )
        strategies.append(strategy)
        strategies_by_entry_rule[strategy["entry_rule"]].append(strategy)

    ticker_stats_lookup: dict[tuple[str, str], dict[str, object]] = {}
    if ticker_stats_csv and Path(ticker_stats_csv).exists():
        for row in _load_csv_rows(ticker_stats_csv):
            ticker_stats_lookup[(row["strategy_code"], row["ticker"])] = {
                "ticker": row["ticker"],
                "total_trades": _parse_int(row.get("total_trades") or row.get("trades")),
                "success_rate_pct": _parse_float(row.get("success_rate_pct") or row.get("profitable_rate_pct")),
                "profitable_trade_rate_pct": _parse_float(row.get("profitable_trade_rate_pct") or row.get("profitable_rate_pct")),
                "take_profit_rate_pct": _parse_float(row.get("take_profit_rate_pct")),
                "average_trade_return_pct": _parse_float(row["average_trade_return_pct"]),
                "net_trade_return_pct": _parse_float(row["net_trade_return_pct"]),
                "profit_factor": _parse_float(row["profit_factor"]),
                "first_trade_date": row["first_trade_date"],
                "last_trade_date": row["last_trade_date"],
            }

    overall_lookup: dict[str, dict[str, object]] = {}
    if overall_actions_csv and Path(overall_actions_csv).exists():
        for row in _load_csv_rows(overall_actions_csv):
            average_success_rate = row.get("average_success_rate_pct") or row.get("average_profitable_trade_rate_pct")
            overall_lookup[row["ticker"]] = {
                "ticker": row["ticker"],
                "elite_strategies_count": _parse_int(row.get("elite_strategies_count") or row.get("strategies_count")),
                "total_trades": _parse_int(row["total_trades"]),
                "sum_net_trade_return_pct": _parse_float(row.get("sum_net_trade_return_pct")),
                "average_of_average_trade_return_pct": _parse_float(row["average_of_average_trade_return_pct"]),
                "average_success_rate_pct": _parse_float(average_success_rate),
                "average_profitable_trade_rate_pct": _parse_float(row.get("average_profitable_trade_rate_pct")),
                "best_profit_factor": _parse_float(row["best_profit_factor"]),
                "best_strategy_label": row["best_strategy_label"],
            }

    allowed_tickers = {ticker.removesuffix(".SA").upper() for ticker in load_tickers(tickers_file)}
    spot_bars = _load_spot_bars(db_path, allowed_tickers=allowed_tickers)

    as_of_day = date.fromisoformat(as_of_date) if as_of_date else None
    latest_trade_date = ""
    total_tickers_considered = 0
    signals: list[dict[str, object]] = []
    triggered_ticker_counter: Counter[str] = Counter()
    triggered_strategy_counter: Counter[str] = Counter()
    qualified_strategy_counter: Counter[str] = Counter()

    for ticker, bars in spot_bars.items():
        if len(bars) < 70:
            continue
        eligible_indices = [
            idx for idx, bar in enumerate(bars)
            if as_of_day is None or date.fromisoformat(bar.trade_date) <= as_of_day
        ]
        if not eligible_indices:
            continue
        idx = eligible_indices[-1]
        current_bar = bars[idx]
        latest_trade_date = max(latest_trade_date, current_bar.trade_date)
        if idx < 1:
            continue

        total_tickers_considered += 1

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

        for entry_rule, entry_price in (("open", current_bar.open), ("close", current_bar.close)):
            if entry_price <= 0 or entry_rule not in strategies_by_entry_rule:
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

            for strategy in strategies_by_entry_rule[entry_rule]:
                conditions = strategy["conditions"]
                if not conditions:
                    continue
                if not all(feature_states.get(key, ("", ""))[0] == bucket_code for key, bucket_code in conditions):
                    continue

                matched_states = [
                    {
                        "feature_key": key,
                        "feature_label": FEATURE_BY_KEY[key].label,
                        "bucket_code": feature_states[key][0],
                        "bucket_label": feature_states[key][1],
                    }
                    for key, _ in conditions
                    if key in feature_states
                ]
                ticker_stats = ticker_stats_lookup.get((strategy["code"], ticker))
                overall_ticker = overall_lookup.get(ticker)
                signal = {
                    "signal_id": f"{ticker}-{current_bar.trade_date}-{strategy['code']}",
                    "ticker": ticker,
                    "trade_date": current_bar.trade_date,
                    "entry_rule": entry_rule,
                    "trade_direction": strategy["trade_direction"],
                    "action_label": "Comprar" if strategy["trade_direction"] == "long" else "Vender",
                    "strategy_code": strategy["code"],
                    "strategy_label": strategy["label"],
                    "state_signature": strategy["state_signature"],
                    "how_to_operate": strategy["how_to_operate"],
                    "take_profit_pct": strategy["take_profit_pct"],
                    "stop_loss_pct": strategy["stop_loss_pct"],
                    "time_cap_days": strategy["time_cap_days"],
                    "strategy_metrics": {
                        "trades": strategy["total_occurrences"],
                        "tickers": strategy["tickers_with_matches"],
                        "success_rate_pct": strategy["success_rate_pct"],
                        "profitable_trade_rate_pct": strategy["profitable_trade_rate_pct"],
                        "average_trade_return_pct": strategy["average_trade_return_pct"],
                        "net_trade_return_pct": strategy["net_trade_return_pct"],
                        "profit_factor": strategy["profit_factor"],
                    },
                    "ticker_metrics": ticker_stats,
                    "overall_ticker_metrics": overall_ticker,
                    "matched_states": matched_states,
                    "price_snapshot": {
                        "open": current_bar.open,
                        "high": current_bar.high,
                        "low": current_bar.low,
                        "close": current_bar.close,
                        "volume": current_bar.volume,
                    },
                    "is_qualified_ticker": ticker_stats is not None,
                }
                signals.append(signal)
                triggered_ticker_counter[ticker] += 1
                triggered_strategy_counter[strategy["code"]] += 1
                if ticker_stats is not None:
                    qualified_strategy_counter[strategy["code"]] += 1

    signals.sort(
        key=lambda item: (
            not item["is_qualified_ticker"],
            -item["strategy_metrics"]["average_trade_return_pct"],
            -(item["ticker_metrics"] or {}).get("average_trade_return_pct", 0.0),
            item["ticker"],
            item["strategy_label"],
        )
    )

    operational_tickers = {
        ticker
        for ticker, count in triggered_ticker_counter.items()
        if count >= OPERATIONAL_MIN_SIGNALS
    }
    for signal in signals:
        ticker = str(signal["ticker"])
        signal["ticker_signal_count"] = triggered_ticker_counter[ticker]
        signal["is_operational_ticker"] = ticker in operational_tickers
    operational_strategy_counter: Counter[str] = Counter(
        str(signal["strategy_code"])
        for signal in signals
        if signal["is_operational_ticker"]
    )

    best_operational_by_ticker: dict[str, dict[str, object]] = {}
    for signal in signals:
        if not signal["is_operational_ticker"]:
            continue
        ticker = str(signal["ticker"])
        current = best_operational_by_ticker.get(ticker)
        signal_score = (
            float(signal["strategy_metrics"]["average_trade_return_pct"]) * 100.0
            + float(signal["strategy_metrics"]["profitable_trade_rate_pct"]) * 2.0
            + min(float(signal["strategy_metrics"]["profit_factor"]), 20.0) * 5.0
            + float(signal["strategy_metrics"]["trades"]) * 0.03
            + triggered_ticker_counter[ticker] * 25.0
        )
        if current is None or signal_score > float(current["operational_score"]):
            best_operational_by_ticker[ticker] = {
                "ticker": ticker,
                "signals": triggered_ticker_counter[ticker],
                "operational_score": signal_score,
                "best_signal_id": signal["signal_id"],
                "best_strategy_code": signal["strategy_code"],
                "best_strategy_label": signal["strategy_label"],
                "entry_rule": signal["entry_rule"],
                "action_label": signal["action_label"],
                "trade_date": signal["trade_date"],
                "strategy_metrics": signal["strategy_metrics"],
                "overall": overall_lookup.get(ticker),
            }
    top_operational_tickers = sorted(
        best_operational_by_ticker.values(),
        key=lambda item: (
            -int(item["signals"]),
            -float(item["operational_score"]),
            str(item["ticker"]),
        ),
    )

    strategies_payload = []
    for strategy in sorted(
        strategies,
        key=lambda item: (
            -triggered_strategy_counter[item["code"]],
            -item["average_trade_return_pct"],
            -item["net_trade_return_pct"],
        ),
    ):
        strategies_payload.append(
            {
                "code": strategy["code"],
                "label": strategy["label"],
                "entry_rule": strategy["entry_rule"],
                "trade_direction": strategy["trade_direction"],
                "take_profit_pct": strategy["take_profit_pct"],
                "stop_loss_pct": strategy["stop_loss_pct"],
                "time_cap_days": strategy["time_cap_days"],
                "state_signature": strategy["state_signature"],
                "how_to_operate": strategy["how_to_operate"],
                "metrics": {
                    "trades": strategy["total_occurrences"],
                    "tickers": strategy["tickers_with_matches"],
                    "success_rate_pct": strategy["success_rate_pct"],
                    "profitable_trade_rate_pct": strategy["profitable_trade_rate_pct"],
                    "average_trade_return_pct": strategy["average_trade_return_pct"],
                    "net_trade_return_pct": strategy["net_trade_return_pct"],
                    "profit_factor": strategy["profit_factor"],
                },
                "triggered_count": triggered_strategy_counter[strategy["code"]],
                "qualified_triggered_count": qualified_strategy_counter[strategy["code"]],
                "operational_triggered_count": operational_strategy_counter[strategy["code"]],
            }
        )

    payload = {
        "generated_at": date.today().isoformat(),
        "latest_trade_date": latest_trade_date,
        "monitor_kind": "asset_round1_elite_min1pct",
        "universe_size": total_tickers_considered,
        "strategies_available": total_strategies_available,
        "strategies_monitored": len(strategies),
        "signals_triggered": len(signals),
        "signals_triggered_qualified": sum(1 for signal in signals if signal["is_qualified_ticker"]),
        "operational_min_signals": OPERATIONAL_MIN_SIGNALS,
        "operational_tickers": len(operational_tickers),
        "signals_triggered_operational": sum(1 for signal in signals if signal["is_operational_ticker"]),
        "triggered_tickers": len(triggered_ticker_counter),
        "top_triggered_tickers": [
            {
                "ticker": ticker,
                "signals": count,
                "is_operational": ticker in operational_tickers,
                "overall": overall_lookup.get(ticker),
            }
            for ticker, count in triggered_ticker_counter.most_common(20)
        ],
        "top_operational_tickers": top_operational_tickers[:20],
        "strategies": strategies_payload,
        "signals": signals,
    }
    return payload


def export_asset_monitor_payload(
    *,
    db_path: str | Path,
    tickers_file: str | Path,
    strategies_csv: str | Path,
    ticker_stats_csv: str | Path | None = None,
    overall_actions_csv: str | Path | None = None,
    as_of_date: str | None = None,
    top_strategies: int | None = None,
    output_path: str | Path,
) -> Path:
    payload = build_asset_monitor_payload(
        db_path=db_path,
        tickers_file=tickers_file,
        strategies_csv=strategies_csv,
        ticker_stats_csv=ticker_stats_csv,
        overall_actions_csv=overall_actions_csv,
        as_of_date=as_of_date,
        top_strategies=top_strategies,
    )
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, allow_nan=False), encoding="utf-8")
    return path
