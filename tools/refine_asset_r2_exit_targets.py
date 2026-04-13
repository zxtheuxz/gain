from __future__ import annotations

import csv
import math
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from b3_patterns.asset_discovery_round1 import (  # noqa: E402
    _atr_pct_series,
    _build_feature_states,
    _prefix_sums,
    _prefix_sums_sq,
    _simulate_percent_exit,
)
from b3_patterns.options import _load_spot_bars  # noqa: E402
from b3_patterns.tickers import load_tickers  # noqa: E402


REPORTS = ROOT / "reports"
DB_PATH = ROOT / "b3_history.db"
TICKERS_FILE = ROOT / "lista.md"
BASE_STRATEGIES_CSV = REPORTS / "asset-discovery-lista-r2-operational-strategies.csv"

OUTPUT_STRATEGIES_CSV = REPORTS / "asset-discovery-lista-r2-exit-refined-strategies.csv"
OUTPUT_TICKERS_CSV = REPORTS / "asset-discovery-lista-r2-exit-refined-tickers.csv"
OUTPUT_FINAL_CSV = REPORTS / "asset-discovery-lista-r2-exit-refined-final.csv"
OUTPUT_FINAL_TICKERS_CSV = REPORTS / "asset-discovery-lista-r2-exit-refined-final-tickers.csv"
OUTPUT_FINAL_ACTIONS_CSV = REPORTS / "asset-discovery-lista-r2-exit-refined-final-actions.csv"
OUTPUT_REPORT_MD = REPORTS / "asset-discovery-lista-r2-exit-refined-report.md"

START_DATE = "2025-04-10"
END_DATE = "2026-04-10"
TIME_CAP_DAYS = 5

TARGET_STOP_PAIRS = [
    (1.0, 1.0),
    (1.0, 2.0),
    (1.0, 3.0),
    (1.0, 4.0),
    (2.0, 1.0),
    (2.0, 2.0),
    (2.0, 3.0),
    (2.0, 4.0),
    (3.0, 1.0),
    (3.0, 1.5),
    (3.0, 2.0),
    (3.0, 3.0),
    (3.0, 4.0),
    (4.0, 2.0),
    (4.0, 3.0),
    (4.0, 4.0),
    (5.0, 2.5),
    (5.0, 3.0),
    (5.0, 4.0),
    (6.0, 3.0),
    (6.0, 4.0),
    (7.0, 3.5),
    (7.0, 4.0),
    (8.0, 4.0),
]

FINAL_MIN_TRADES = 300
FINAL_MIN_TICKERS = 50
FINAL_MIN_TARGET_HIT_RATE = 50.0
FINAL_MIN_AVG_RETURN = 1.0
FINAL_MIN_PROFIT_FACTOR = 2.0
FINAL_MIN_TARGET_PCT = 2.0

FINAL_TICKER_MIN_TRADES = 15
FINAL_TICKER_MIN_TARGET_HIT_RATE = 50.0
FINAL_TICKER_MIN_AVG_RETURN = 1.0
FINAL_TICKER_MIN_PROFIT_FACTOR = 2.0


def _float(value: object) -> float:
    if value in ("", "None", None):
        return 0.0
    if str(value).upper() == "INF":
        return math.inf
    return float(value)


def _int(value: object) -> int:
    return int(float(value)) if value not in ("", None) else 0


def _format(value: float, digits: int = 4) -> str:
    if math.isinf(value):
        return "INF"
    return f"{value:.{digits}f}"


def _token(value: float) -> str:
    return f"{value:.1f}".rstrip("0").rstrip(".").replace(".", "_")


def _pair_label(value: float) -> str:
    return f"{value:.1f}".rstrip("0").rstrip(".")


def _profit_factor(gross_profit: float, gross_loss: float) -> float:
    if gross_loss == 0:
        return math.inf if gross_profit > 0 else 0.0
    return gross_profit / gross_loss


def _parse_conditions(code: str, state_size: int) -> tuple[str, tuple[tuple[str, str], ...]]:
    marker = f"__{state_size}f__"
    _, _, suffix = code.partition(marker)
    if not suffix:
        return "", ()
    conditions = []
    for part in suffix.split("__"):
        key, _, bucket = part.partition("=")
        if key and bucket:
            conditions.append((key, bucket))
    return suffix, tuple(conditions)


def _new_code(row: dict[str, str], suffix: str, target_pct: float, stop_pct: float) -> str:
    return (
        f"asset_{row['trade_direction']}_{row['entry_rule']}_"
        f"tp{_token(target_pct)}_sl{_token(stop_pct)}_cap{TIME_CAP_DAYS}"
        f"__{row['state_size']}f__{suffix}"
    )


def _new_label(row: dict[str, str], target_pct: float, stop_pct: float) -> str:
    entry = row["entry_rule"]
    direction_label = "compra" if row["trade_direction"] == "long" else "venda"
    return (
        f"Discovery Acao: {direction_label} no {entry}, alvo +{_pair_label(target_pct)}% / "
        f"stop -{_pair_label(stop_pct)}% / cap {TIME_CAP_DAYS}D | {row['state_signature']}"
    )


def _blank_acc() -> dict[str, object]:
    return {
        "trades": 0,
        "take_profit": 0,
        "stop_loss": 0,
        "time_cap": 0,
        "profitable": 0,
        "sum_return": 0.0,
        "gross_profit": 0.0,
        "gross_loss": 0.0,
        "tickers": set(),
        "first_trade_date": "",
        "last_trade_date": "",
    }


def _update_acc(acc: dict[str, object], *, ticker: str, trade_date: str, trade_return: float, exit_reason: str) -> None:
    acc["trades"] = int(acc["trades"]) + 1
    acc["sum_return"] = float(acc["sum_return"]) + trade_return
    if trade_return > 0:
        acc["profitable"] = int(acc["profitable"]) + 1
        acc["gross_profit"] = float(acc["gross_profit"]) + trade_return
    elif trade_return < 0:
        acc["gross_loss"] = float(acc["gross_loss"]) + abs(trade_return)

    if exit_reason == "take_profit":
        acc["take_profit"] = int(acc["take_profit"]) + 1
    elif exit_reason.startswith("stop_loss"):
        acc["stop_loss"] = int(acc["stop_loss"]) + 1
    elif exit_reason == "time_cap":
        acc["time_cap"] = int(acc["time_cap"]) + 1

    if "tickers" in acc:
        acc["tickers"].add(ticker)
    if not acc["first_trade_date"] or trade_date < str(acc["first_trade_date"]):
        acc["first_trade_date"] = trade_date
    if not acc["last_trade_date"] or trade_date > str(acc["last_trade_date"]):
        acc["last_trade_date"] = trade_date


def _metrics_from_acc(acc: dict[str, object]) -> dict[str, float | int]:
    trades = int(acc["trades"])
    take_profit = int(acc["take_profit"])
    stop_loss = int(acc["stop_loss"])
    time_cap = int(acc["time_cap"])
    profitable = int(acc["profitable"])
    gross_profit = float(acc["gross_profit"])
    gross_loss = float(acc["gross_loss"])
    return {
        "trades": trades,
        "take_profit_count": take_profit,
        "take_profit_rate_pct": (take_profit / trades) * 100.0 if trades else 0.0,
        "stop_loss_count": stop_loss,
        "stop_loss_rate_pct": (stop_loss / trades) * 100.0 if trades else 0.0,
        "time_cap_count": time_cap,
        "time_cap_rate_pct": (time_cap / trades) * 100.0 if trades else 0.0,
        "profitable_count": profitable,
        "profitable_rate_pct": (profitable / trades) * 100.0 if trades else 0.0,
        "average_trade_return_pct": float(acc["sum_return"]) / trades if trades else 0.0,
        "net_trade_return_pct": float(acc["sum_return"]),
        "profit_factor": _profit_factor(gross_profit, gross_loss),
    }


def _variant_score(row: dict[str, object]) -> float:
    return (
        float(row["take_profit_rate_pct"]) * 2.5
        + float(row["average_trade_return_pct"]) * 35.0
        + min(float(row["profit_factor"]), 8.0) * 8.0
        + float(row["target_pct"]) * 8.0
        - float(row["stop_loss_rate_pct"]) * 1.2
    )


def main() -> None:
    base_rows: list[dict[str, str]] = []
    with BASE_STRATEGIES_CSV.open(encoding="utf-8", newline="") as file_obj:
        for row in csv.DictReader(file_obj):
            suffix, conditions = _parse_conditions(row["code"], _int(row["state_size"]))
            row["state_suffix"] = suffix
            row["conditions"] = conditions
            base_rows.append(row)

    base_by_entry: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in base_rows:
        base_by_entry[row["entry_rule"]].append(row)

    strategy_acc: dict[tuple[str, float, float], dict[str, object]] = defaultdict(_blank_acc)
    ticker_acc: dict[tuple[str, float, float, str], dict[str, object]] = defaultdict(_blank_acc)

    start_day = date.fromisoformat(START_DATE)
    end_day = date.fromisoformat(END_DATE)
    allowed_tickers = {ticker.removesuffix(".SA").upper() for ticker in load_tickers(TICKERS_FILE)}
    spot_bars = _load_spot_bars(DB_PATH, allowed_tickers=allowed_tickers)

    for ticker, bars in spot_bars.items():
        if len(bars) < 70:
            continue

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
        for idx in range(1, len(bars)):
            previous_close = closes[idx - 1]
            daily_returns.append(((closes[idx] / previous_close) - 1.0) * 100.0 if previous_close > 0 else 0.0)

        atr_pct_values = _atr_pct_series(highs, lows, closes)
        close_prefix_sums = _prefix_sums(closes)
        volume_prefix_sums = _prefix_sums(volumes)
        dollar_volume_prefix_sums = _prefix_sums(dollar_volumes)
        range_pct_prefix_sums = _prefix_sums(range_pcts)
        daily_return_prefix_sums = _prefix_sums(daily_returns)
        daily_return_prefix_sums_sq = _prefix_sums_sq(daily_returns)
        atr_pct_prefix_sums = _prefix_sums(atr_pct_values)

        for idx in range(1, len(bars)):
            current_bar = bars[idx]
            trade_day = date.fromisoformat(current_bar.trade_date)
            if trade_day < start_day or trade_day > end_day:
                continue

            for entry_rule in ("open", "close"):
                if entry_rule not in base_by_entry:
                    continue
                entry_price = current_bar.open if entry_rule == "open" else current_bar.close
                if entry_price <= 0:
                    continue
                states = _build_feature_states(
                    entry_rule=entry_rule,
                    idx=idx,
                    opens=opens,
                    highs=highs,
                    lows=lows,
                    closes=closes,
                    volumes=volumes,
                    close_prefix_sums=close_prefix_sums,
                    volume_prefix_sums=volume_prefix_sums,
                    dollar_volume_prefix_sums=dollar_volume_prefix_sums,
                    range_pct_prefix_sums=range_pct_prefix_sums,
                    daily_return_prefix_sums=daily_return_prefix_sums,
                    daily_return_prefix_sums_sq=daily_return_prefix_sums_sq,
                    atr_pct_prefix_sums=atr_pct_prefix_sums,
                    entry_price=entry_price,
                )
                if not states:
                    continue

                matched_rows = [
                    row for row in base_by_entry[entry_rule]
                    if all(states.get(key, ("", ""))[0] == bucket for key, bucket in row["conditions"])
                ]
                if not matched_rows:
                    continue

                for target_pct, stop_pct in TARGET_STOP_PAIRS:
                    exit_result = _simulate_percent_exit(
                        bars=bars,
                        entry_idx=idx,
                        entry_price=entry_price,
                        trade_direction="long",
                        take_profit_pct=target_pct,
                        stop_loss_pct=stop_pct,
                        time_cap_days=TIME_CAP_DAYS,
                        include_entry_bar=(entry_rule == "open"),
                    )
                    if exit_result is None:
                        continue
                    _, exit_price, exit_reason = exit_result
                    trade_return = ((exit_price / entry_price) - 1.0) * 100.0
                    for row in matched_rows:
                        strategy_key = (row["code"], target_pct, stop_pct)
                        ticker_key = (row["code"], target_pct, stop_pct, ticker)
                        _update_acc(
                            strategy_acc[strategy_key],
                            ticker=ticker,
                            trade_date=current_bar.trade_date,
                            trade_return=trade_return,
                            exit_reason=exit_reason,
                        )
                        _update_acc(
                            ticker_acc[ticker_key],
                            ticker=ticker,
                            trade_date=current_bar.trade_date,
                            trade_return=trade_return,
                            exit_reason=exit_reason,
                        )

    base_by_code = {row["code"]: row for row in base_rows}
    strategy_rows: list[dict[str, object]] = []
    for (base_code, target_pct, stop_pct), acc in strategy_acc.items():
        base = base_by_code[base_code]
        metrics = _metrics_from_acc(acc)
        row = {
            "code": _new_code(base, base["state_suffix"], target_pct, stop_pct),
            "base_code": base_code,
            "label": _new_label(base, target_pct, stop_pct),
            "trade_direction": base["trade_direction"],
            "entry_rule": base["entry_rule"],
            "target_pct": target_pct,
            "stop_pct": stop_pct,
            "time_cap_days": TIME_CAP_DAYS,
            "state_size": base["state_size"],
            "state_signature": base["state_signature"],
            "feature_keys": base["feature_keys"],
            "tickers": len(acc["tickers"]),
            "first_trade_date": acc["first_trade_date"],
            "last_trade_date": acc["last_trade_date"],
            **metrics,
        }
        row["score"] = _variant_score(row)
        row["passes_final_filter"] = (
            int(row["trades"]) >= FINAL_MIN_TRADES
            and int(row["tickers"]) >= FINAL_MIN_TICKERS
            and float(row["target_pct"]) >= FINAL_MIN_TARGET_PCT
            and float(row["take_profit_rate_pct"]) >= FINAL_MIN_TARGET_HIT_RATE
            and float(row["average_trade_return_pct"]) >= FINAL_MIN_AVG_RETURN
            and float(row["profit_factor"]) >= FINAL_MIN_PROFIT_FACTOR
        )
        strategy_rows.append(row)

    strategy_rows.sort(
        key=lambda row: (
            -float(row["passes_final_filter"]),
            -float(row["take_profit_rate_pct"]),
            -float(row["target_pct"]),
            -float(row["average_trade_return_pct"]),
            -float(row["score"]),
        )
    )

    ticker_rows: list[dict[str, object]] = []
    for (base_code, target_pct, stop_pct, ticker), acc in ticker_acc.items():
        base = base_by_code[base_code]
        metrics = _metrics_from_acc(acc)
        row = {
            "strategy_code": _new_code(base, base["state_suffix"], target_pct, stop_pct),
            "base_strategy_code": base_code,
            "strategy_label": _new_label(base, target_pct, stop_pct),
            "ticker": ticker,
            "target_pct": target_pct,
            "stop_pct": stop_pct,
            "first_trade_date": acc["first_trade_date"],
            "last_trade_date": acc["last_trade_date"],
            **metrics,
        }
        row["passes_final_ticker_filter"] = (
            int(row["trades"]) >= FINAL_TICKER_MIN_TRADES
            and float(row["take_profit_rate_pct"]) >= FINAL_TICKER_MIN_TARGET_HIT_RATE
            and float(row["average_trade_return_pct"]) >= FINAL_TICKER_MIN_AVG_RETURN
            and float(row["profit_factor"]) >= FINAL_TICKER_MIN_PROFIT_FACTOR
        )
        ticker_rows.append(row)

    final_strategy_codes = {str(row["code"]) for row in strategy_rows if row["passes_final_filter"]}
    final_ticker_rows = [
        row for row in ticker_rows
        if row["strategy_code"] in final_strategy_codes and row["passes_final_ticker_filter"]
    ]

    action_acc: dict[str, dict[str, object]] = {}
    for row in final_ticker_rows:
        item = action_acc.setdefault(
            str(row["ticker"]),
            {
                "ticker": row["ticker"],
                "strategies_count": 0,
                "total_trades": 0,
                "sum_net_trade_return_pct": 0.0,
                "sum_average_trade_return_pct": 0.0,
                "sum_profitable_rate_pct": 0.0,
                "sum_take_profit_rate_pct": 0.0,
                "best_profit_factor": 0.0,
                "best_strategy_label": "",
            },
        )
        item["strategies_count"] = int(item["strategies_count"]) + 1
        item["total_trades"] = int(item["total_trades"]) + int(row["trades"])
        item["sum_net_trade_return_pct"] = float(item["sum_net_trade_return_pct"]) + float(row["net_trade_return_pct"])
        item["sum_average_trade_return_pct"] = (
            float(item["sum_average_trade_return_pct"]) + float(row["average_trade_return_pct"])
        )
        item["sum_profitable_rate_pct"] = float(item["sum_profitable_rate_pct"]) + float(row["profitable_rate_pct"])
        item["sum_take_profit_rate_pct"] = float(item["sum_take_profit_rate_pct"]) + float(row["take_profit_rate_pct"])
        if float(row["profit_factor"]) > float(item["best_profit_factor"]):
            item["best_profit_factor"] = float(row["profit_factor"])
            item["best_strategy_label"] = row["strategy_label"]

    action_rows: list[dict[str, object]] = []
    for item in action_acc.values():
        strategies_count = int(item["strategies_count"])
        action_rows.append(
            {
                "ticker": item["ticker"],
                "strategies_count": strategies_count,
                "total_trades": item["total_trades"],
                "sum_net_trade_return_pct": float(item["sum_net_trade_return_pct"]),
                "average_of_average_trade_return_pct": float(item["sum_average_trade_return_pct"]) / strategies_count,
                "average_success_rate_pct": float(item["sum_profitable_rate_pct"]) / strategies_count,
                "average_profitable_trade_rate_pct": float(item["sum_profitable_rate_pct"]) / strategies_count,
                "average_take_profit_rate_pct": float(item["sum_take_profit_rate_pct"]) / strategies_count,
                "best_profit_factor": float(item["best_profit_factor"]),
                "best_strategy_label": item["best_strategy_label"],
            }
        )
    action_rows.sort(
        key=lambda row: (
            -float(row["average_take_profit_rate_pct"]),
            -float(row["average_of_average_trade_return_pct"]),
            -int(row["strategies_count"]),
            row["ticker"],
        )
    )

    final_rows = [row for row in strategy_rows if row["passes_final_filter"]]
    final_rows.sort(key=lambda row: (-float(row["score"]), -float(row["take_profit_rate_pct"])))
    final_ticker_rows.sort(
        key=lambda row: (
            row["strategy_code"],
            -float(row["take_profit_rate_pct"]),
            -float(row["average_trade_return_pct"]),
            row["ticker"],
        )
    )

    strategy_fields = [
        "code", "base_code", "label", "trade_direction", "entry_rule", "target_pct", "stop_pct",
        "time_cap_days", "state_size", "state_signature", "feature_keys", "trades", "tickers",
        "take_profit_count", "take_profit_rate_pct", "stop_loss_count", "stop_loss_rate_pct",
        "time_cap_count", "time_cap_rate_pct", "profitable_count", "profitable_rate_pct",
        "average_trade_return_pct", "net_trade_return_pct", "profit_factor", "score",
        "passes_final_filter", "first_trade_date", "last_trade_date",
    ]

    def _serialize(row: dict[str, object], fields: list[str]) -> dict[str, object]:
        output = {key: row.get(key, "") for key in fields}
        for key in [
            "target_pct", "stop_pct", "take_profit_rate_pct", "stop_loss_rate_pct", "time_cap_rate_pct",
            "profitable_rate_pct", "average_trade_return_pct", "net_trade_return_pct", "profit_factor", "score",
        ]:
            if key in output:
                output[key] = _format(float(output[key]))
        return output

    with OUTPUT_STRATEGIES_CSV.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=strategy_fields)
        writer.writeheader()
        for row in strategy_rows:
            writer.writerow(_serialize(row, strategy_fields))

    with OUTPUT_FINAL_CSV.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=strategy_fields)
        writer.writeheader()
        for row in final_rows:
            writer.writerow(_serialize(row, strategy_fields))

    ticker_fields = [
        "strategy_code", "base_strategy_code", "strategy_label", "ticker", "target_pct", "stop_pct",
        "trades", "take_profit_count", "take_profit_rate_pct", "stop_loss_count", "stop_loss_rate_pct",
        "time_cap_count", "time_cap_rate_pct", "profitable_count", "profitable_rate_pct",
        "average_trade_return_pct", "net_trade_return_pct", "profit_factor", "passes_final_ticker_filter",
        "first_trade_date", "last_trade_date",
    ]
    with OUTPUT_TICKERS_CSV.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=ticker_fields)
        writer.writeheader()
        for row in ticker_rows:
            writer.writerow(_serialize(row, ticker_fields))

    with OUTPUT_FINAL_TICKERS_CSV.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=ticker_fields)
        writer.writeheader()
        for row in final_ticker_rows:
            writer.writerow(_serialize(row, ticker_fields))

    action_fields = [
        "ticker",
        "strategies_count",
        "total_trades",
        "sum_net_trade_return_pct",
        "average_of_average_trade_return_pct",
        "average_success_rate_pct",
        "average_profitable_trade_rate_pct",
        "average_take_profit_rate_pct",
        "best_profit_factor",
        "best_strategy_label",
    ]
    with OUTPUT_FINAL_ACTIONS_CSV.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=action_fields)
        writer.writeheader()
        for row in action_rows:
            output = dict(row)
            for key in [
                "sum_net_trade_return_pct",
                "average_of_average_trade_return_pct",
                "average_success_rate_pct",
                "average_profitable_trade_rate_pct",
                "average_take_profit_rate_pct",
                "best_profit_factor",
            ]:
                output[key] = _format(float(output[key]))
            writer.writerow(output)

    best_by_base: dict[str, dict[str, object]] = {}
    for row in strategy_rows:
        base_code = str(row["base_code"])
        current = best_by_base.get(base_code)
        if current is None or float(row["score"]) > float(current["score"]):
            best_by_base[base_code] = row

    lines = [
        "# Rodada 2 - Refinamento de Alvo/Stop",
        "",
        "Aqui mantemos os mesmos gatilhos das estrategias operacionais, mas testamos varios alvos e stops para descobrir qual alvo realmente bate com mais frequencia.",
        "",
        f"Janela: `{START_DATE}` ate `{END_DATE}`.",
        f"Filtro final sugerido: alvo >= `{FINAL_MIN_TARGET_PCT:.0f}%`, alvo batido >= `{FINAL_MIN_TARGET_HIT_RATE:.0f}%`, media >= `{FINAL_MIN_AVG_RETURN:.0f}%`, PF >= `{FINAL_MIN_PROFIT_FACTOR:.0f}`, trades >= `{FINAL_MIN_TRADES}`, acoes >= `{FINAL_MIN_TICKERS}`.",
        "",
        "## Resultado",
        f"- Variantes testadas: `{len(strategy_rows)}`",
        f"- Variantes finais que passaram no corte de alvo: `{len(final_rows)}`",
        f"- Linhas estrategia x acao finais: `{len(final_ticker_rows)}`",
        f"- Acoes finais: `{len(action_rows)}`",
        "",
        "## Melhores variantes finais",
        "",
        "| Estrategia refinada | Trades | Acoes | Alvo | Stop | Alvo batido | Stop | Prazo | Media | PF |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in final_rows[:30]:
        lines.append(
            f"| {row['label']} | {row['trades']} | {row['tickers']} | {float(row['target_pct']):.0f}% | "
            f"{float(row['stop_pct']):.0f}% | {float(row['take_profit_rate_pct']):.2f}% | "
            f"{float(row['stop_loss_rate_pct']):.2f}% | {float(row['time_cap_rate_pct']):.2f}% | "
            f"{float(row['average_trade_return_pct']):.2f}% | {float(row['profit_factor']):.2f} |"
        )

    lines.extend(["", "## Melhor alvo por estrategia base", ""])
    for index, row in enumerate(sorted(best_by_base.values(), key=lambda item: -float(item["score"])), 1):
        lines.append(
            f"{index}. alvo `{float(row['target_pct']):.0f}%` / stop `{float(row['stop_pct']):.0f}%` | "
            f"alvo batido `{float(row['take_profit_rate_pct']):.2f}%` | media `{float(row['average_trade_return_pct']):.2f}%` | "
            f"PF `{float(row['profit_factor']):.2f}` | {row['label']}"
        )

    lines.extend(["", "## Melhores acoes nas variantes finais", ""])
    for index, row in enumerate(final_ticker_rows[:40], 1):
        lines.append(
            f"{index}. `{row['ticker']}` | alvo `{float(row['target_pct']):.0f}%` / stop `{float(row['stop_pct']):.0f}%` | "
            f"alvo batido `{float(row['take_profit_rate_pct']):.2f}%` ({row['take_profit_count']}/{row['trades']}) | "
            f"media `{float(row['average_trade_return_pct']):.2f}%` | PF `{float(row['profit_factor']):.2f}` | {row['strategy_label']}"
        )

    lines.extend(
        [
            "",
            "## Arquivos",
            f"- Todas as variantes: `{OUTPUT_STRATEGIES_CSV.relative_to(ROOT)}`",
            f"- Variantes finais: `{OUTPUT_FINAL_CSV.relative_to(ROOT)}`",
            f"- Todas as acoes por variante: `{OUTPUT_TICKERS_CSV.relative_to(ROOT)}`",
            f"- Acoes finais: `{OUTPUT_FINAL_TICKERS_CSV.relative_to(ROOT)}`",
            f"- Acoes consolidadas finais: `{OUTPUT_FINAL_ACTIONS_CSV.relative_to(ROOT)}`",
        ]
    )
    OUTPUT_REPORT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(OUTPUT_STRATEGIES_CSV)
    print(OUTPUT_TICKERS_CSV)
    print(OUTPUT_FINAL_CSV)
    print(OUTPUT_FINAL_TICKERS_CSV)
    print(OUTPUT_FINAL_ACTIONS_CSV)
    print(OUTPUT_REPORT_MD)
    print(f"variants={len(strategy_rows)}")
    print(f"final_variants={len(final_rows)}")
    print(f"final_ticker_rows={len(final_ticker_rows)}")
    print(f"final_actions={len(action_rows)}")


if __name__ == "__main__":
    main()
