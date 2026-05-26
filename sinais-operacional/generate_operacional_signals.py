"""Gerador unificado de sinais diarios: swing (R10) + daytrade (R11).

Le os portfolios finais de swing e daytrade (mesmo schema, gerado por
build_r10_final.py / build_r11_final.py), avalia para cada ticker se algum
setup validado dispara no pregao `--as-of-date`, e emite um unico CSV com
os sinais marcados por `book` (swing|daytrade).

Reaproveita o motor de feature-states de generate_r10_signals.py /
b3_patterns.asset_discovery_round1 (portfolio-agnostico na pratica).

Uso:
  python3 sinais-operacional/generate_operacional_signals.py [--as-of-date YYYY-MM-DD]

Defaults: pega o r10-final.csv e r11-final.csv mais recentes em reports/.
Saida: reports/operacional-signals-<DATE>.csv (consumido por
sinais-operacional/track_operacional.py e telegram_send_operacional.py).
"""
from __future__ import annotations

import argparse
import csv
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from b3_patterns.asset_discovery_round1 import (  # noqa: E402
    FEATURE_BY_KEY,
    _atr_pct_series,
    _build_feature_states,
    _ema_series,
    _prefix_sums,
    _prefix_sums_sq,
    _rsi_series,
)
from b3_patterns.tickers import load_tickers  # noqa: E402


SIGNAL_COLUMNS = [
    "book",
    "ticker",
    "trade_date",
    "entry_rule",
    "trade_direction",
    "strategy_code",
    "take_profit_pct",
    "stop_loss_pct",
    "time_cap_days",
    "entry_price_reference",
    "success_rate_pct",
    "take_profit_rate_pct",
    "average_trade_return_pct",
    "profit_factor",
    "total_occurrences",
    "tickers_with_matches",
    "feature_keys",
    "matched_states",
    "wilson_ci95_lower_pct",
    "bootstrap_avg_return_lower_pct",
    "walk_forward_status",
    "v_trades",
    "v_win_pct",
    "o_trades",
    "o_win_pct",
    "tier",
]

_FACTOR_MARKER = re.compile(r"__(\d+)f__")

# Fallback Books v1 (presentes/versionados) quando os finais R10/R11 ainda
# nao foram trazidos. Assim que reports/r10-*/r10-final.csv (ou r11) aparecer,
# o glob passa a preferi-lo automaticamente (auto-upgrade).
_BOOKS_V1 = REPO_ROOT / "entrega-operacional-v1" / "reports" / "operational-books-v1"
SWING_FALLBACK = _BOOKS_V1 / "swing-combinado.csv"
DAYTRADE_FALLBACK = _BOOKS_V1 / "daytrade-combinado.csv"


def _find_latest(glob_pattern: str) -> Path | None:
    """Retorna o caminho mais recente que casa com o glob (por mtime)."""
    matches = sorted(
        REPO_ROOT.glob(glob_pattern),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return matches[0] if matches else None


def _resolve_swing_csv() -> Path | None:
    return _find_latest("reports/r10-*/r10-final.csv") or (SWING_FALLBACK if SWING_FALLBACK.exists() else None)


def _resolve_daytrade_csv() -> Path | None:
    return _find_latest("reports/r11-*/r11-final.csv") or (DAYTRADE_FALLBACK if DAYTRADE_FALLBACK.exists() else None)


def _parse_conditions(strategy_code: str) -> list[tuple[str, str]]:
    """Extrai pares (feature_key, bucket_code) do strategy_code.

    Formato: ...__<N>f__key1=bucket1__key2=bucket2...
    Detecta o marcador __Nf__ via regex (serve para R10 e R11, qualquer N).
    """
    marker = _FACTOR_MARKER.search(strategy_code)
    if not marker:
        return []
    suffix = strategy_code[marker.end():]
    conditions: list[tuple[str, str]] = []
    for part in suffix.split("__"):
        key, _, bucket_code = part.partition("=")
        if key and bucket_code:
            conditions.append((key, bucket_code))
    return conditions


def _classify_tier(row: dict[str, str]) -> str:
    """Tier S: OOS comprovado (>= 6 trades e 100% win).
    Tier A: validado mas OOS ainda nao disparou / < 6 trades / win alto.
    Tier B: OOS misto."""
    try:
        o_trades = int(float(row.get("o_trades") or 0))
        o_win = float(row.get("o_win_pct") or 0)
    except (TypeError, ValueError):
        o_trades = 0
        o_win = 0.0
    if o_trades >= 6 and o_win >= 100.0:
        return "S"
    if o_trades == 0:
        return "A"
    if o_win >= 90.0:
        return "A"
    return "B"


def _num(row: dict[str, str], key: str, default: float = 0.0) -> float:
    try:
        return float(row.get(key) or default)
    except (TypeError, ValueError):
        return default


def _pick(row: dict[str, str], *keys: str) -> str:
    """Primeiro valor nao-vazio entre as colunas candidatas (schemas distintos)."""
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return ""


def _pick_num(row: dict[str, str], *keys: str, default: float = 0.0) -> float:
    raw = _pick(row, *keys)
    try:
        return float(raw) if raw != "" else default
    except (TypeError, ValueError):
        return default


def _load_book(book_csv: Path, book: str) -> dict[str, list[dict[str, object]]]:
    """Retorna {ticker: [strategy_dict, ...]} para um portfolio final.

    Tolerante a dois schemas: R10/R11 final (holding_days, m_trades,
    m_win_pct, m_avg_return_pct, m_profit_factor, colunas v_*/o_*/wilson/
    bootstrap/walk_forward) e Books v1 swing/daytrade-combinado (time_cap_days,
    trades, profitable_rate_pct, average_trade_return_pct, profit_factor).
    """
    by_ticker: dict[str, list[dict[str, object]]] = defaultdict(list)
    with book_csv.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            ticker = row["ticker"].upper().removesuffix(".SA")
            conditions = _parse_conditions(row["strategy_code"])
            if not conditions:
                continue
            trades = int(_pick_num(row, "m_trades", "trades"))
            win_pct = _pick_num(row, "m_win_pct", "profitable_rate_pct")
            tp_rate = _pick_num(row, "take_profit_rate_pct")
            if not tp_rate and row.get("m_take_profit") and trades:
                tp_rate = int(_num(row, "m_take_profit")) / trades * 100.0
            by_ticker[ticker].append({
                "book": book,
                "strategy_code": row["strategy_code"],
                "entry_rule": _pick(row, "entry_rule") or "open",
                "trade_direction": _pick(row, "trade_direction") or "long",
                "target_pct": _pick_num(row, "target_pct"),
                "stop_pct": _pick_num(row, "stop_pct"),
                "holding_days": int(_pick_num(row, "holding_days", "time_cap_days")),
                "feature_keys": row.get("feature_keys", ""),
                "win_pct": win_pct,
                "take_profit_rate_pct": tp_rate,
                "trades": trades,
                "avg_return_pct": _pick_num(row, "m_avg_return_pct", "average_trade_return_pct"),
                "profit_factor": _pick(row, "m_profit_factor", "profit_factor"),
                "wilson_ci95_lower_pct": row.get("wilson_ci95_lower_pct", ""),
                "bootstrap_avg_return_lower_pct": row.get("bootstrap_avg_return_lower_pct", ""),
                "walk_forward_status": row.get("walk_forward_status", ""),
                "v_trades": row.get("v_trades", "0"),
                "v_win_pct": row.get("v_win_pct", "0"),
                "o_trades": row.get("o_trades", "0"),
                "o_win_pct": row.get("o_win_pct", "0"),
                "tier": _classify_tier(row),
                "conditions": conditions,
            })
    return by_ticker


def _load_price_bars(
    db_path: Path,
    allowed_tickers: set[str],
) -> dict[str, list[tuple[str, float, float, float, float, int]]]:
    grouped: dict[str, list[tuple[str, float, float, float, float, int]]] = defaultdict(list)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT ticker, trade_date, open, high, low, close, adj_close, volume
            FROM price_history
            ORDER BY ticker, trade_date
            """
        ).fetchall()
    for row in rows:
        ticker = str(row["ticker"]).upper().removesuffix(".SA")
        if ticker not in allowed_tickers:
            continue
        close = float(row["close"])
        adj_close = float(row["adj_close"])
        factor = adj_close / close if close > 0 else 1.0
        grouped[ticker].append((
            str(row["trade_date"]),
            float(row["open"]) * factor,
            float(row["high"]) * factor,
            float(row["low"]) * factor,
            adj_close,
            int(row["volume"]),
        ))
    return dict(grouped)


def _format_matched_states(
    conditions: list[tuple[str, str]],
    feature_states: dict[str, tuple[str, str]],
) -> str:
    parts = []
    for key, _ in conditions:
        if key not in feature_states:
            continue
        feature = FEATURE_BY_KEY.get(key)
        label = feature.label if feature else key
        _, bucket_label = feature_states[key]
        parts.append(f"{label}: {bucket_label}")
    return "; ".join(parts)


def main() -> int:
    parser = argparse.ArgumentParser(description="Gera sinais diarios swing (R10) + daytrade (R11).")
    parser.add_argument("--db-path", default="b3_history.db")
    parser.add_argument("--tickers-file", default="lista.md")
    parser.add_argument(
        "--swing-csv",
        default=None,
        help="r10-final.csv (default: mais recente reports/r10-*/r10-final.csv).",
    )
    parser.add_argument(
        "--daytrade-csv",
        default=None,
        help="r11-final.csv (default: mais recente reports/r11-*/r11-final.csv).",
    )
    parser.add_argument(
        "--as-of-date",
        help="Trade date a avaliar (YYYY-MM-DD). Default: mais recente em price_history.",
    )
    parser.add_argument("--output-csv", default=None)
    args = parser.parse_args()

    db_path = Path(args.db_path).resolve()
    tickers_file = Path(args.tickers_file).resolve()
    as_of_day = date.fromisoformat(args.as_of_date) if args.as_of_date else None

    swing_csv = Path(args.swing_csv).resolve() if args.swing_csv else _resolve_swing_csv()
    daytrade_csv = Path(args.daytrade_csv).resolve() if args.daytrade_csv else _resolve_daytrade_csv()

    strategies_by_ticker: dict[str, list[dict[str, object]]] = defaultdict(list)
    counts = {"swing": 0, "daytrade": 0}
    for book, book_csv in (("swing", swing_csv), ("daytrade", daytrade_csv)):
        if not book_csv or not book_csv.exists():
            print(f"AVISO: portfolio {book} nao encontrado ({book_csv}); pulando.", file=sys.stderr)
            continue
        loaded = _load_book(book_csv, book)
        for ticker, strats in loaded.items():
            strategies_by_ticker[ticker].extend(strats)
            counts[book] += len(strats)
        print(f"{book}: {counts[book]} setups carregados de {book_csv}")

    if not strategies_by_ticker:
        sys.exit("ERRO: nenhum portfolio carregado (swing/daytrade ausentes).")

    allowed_tickers = {t.removesuffix(".SA").upper() for t in load_tickers(tickers_file)}
    price_bars = _load_price_bars(db_path, allowed_tickers)

    signals: list[dict[str, object]] = []
    latest_trade_date = ""

    for ticker, bars in price_bars.items():
        if len(bars) < 70:
            continue
        if ticker not in strategies_by_ticker:
            continue
        eligible_indices = [
            idx for idx, bar in enumerate(bars)
            if as_of_day is None or date.fromisoformat(bar[0]) <= as_of_day
        ]
        if not eligible_indices:
            continue
        idx = eligible_indices[-1]
        current_bar = bars[idx]
        if as_of_day is not None and date.fromisoformat(current_bar[0]) != as_of_day:
            continue
        latest_trade_date = max(latest_trade_date, current_bar[0])
        if idx < 1:
            continue

        opens = [item[1] for item in bars]
        highs = [item[2] for item in bars]
        lows = [item[3] for item in bars]
        closes = [item[4] for item in bars]
        volumes = [float(item[5]) for item in bars]
        dollar_volumes = [item[4] * float(item[5]) for item in bars]
        range_pcts = [
            0.0 if item[4] <= 0 else ((item[2] - item[3]) / item[4]) * 100.0
            for item in bars
        ]
        daily_returns = [0.0]
        for bar_idx in range(1, len(bars)):
            previous_close = closes[bar_idx - 1]
            daily_returns.append(0.0 if previous_close <= 0 else ((closes[bar_idx] / previous_close) - 1.0) * 100.0)

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
        trade_dates_list = [item[0] for item in bars]

        for entry_rule, entry_price in (("open", current_bar[1]), ("close", current_bar[4])):
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

            for strategy in strategies_by_ticker[ticker]:
                if strategy["entry_rule"] != entry_rule:
                    continue
                conditions = strategy["conditions"]
                if not all(
                    feature_states.get(key, ("", ""))[0] == bucket_code
                    for key, bucket_code in conditions
                ):
                    continue
                signals.append({
                    "book": strategy["book"],
                    "ticker": ticker,
                    "trade_date": current_bar[0],
                    "entry_rule": entry_rule,
                    "trade_direction": strategy["trade_direction"],
                    "strategy_code": strategy["strategy_code"],
                    "take_profit_pct": f"{strategy['target_pct']:.4f}",
                    "stop_loss_pct": f"{strategy['stop_pct']:.4f}",
                    "time_cap_days": strategy["holding_days"],
                    "entry_price_reference": f"{entry_price:.4f}",
                    "success_rate_pct": f"{strategy['win_pct']:.4f}",
                    "take_profit_rate_pct": f"{strategy['take_profit_rate_pct']:.4f}",
                    "average_trade_return_pct": f"{strategy['avg_return_pct']:.4f}",
                    "profit_factor": strategy["profit_factor"],
                    "total_occurrences": strategy["trades"],
                    "tickers_with_matches": 1,
                    "feature_keys": strategy["feature_keys"],
                    "matched_states": _format_matched_states(conditions, feature_states),
                    "wilson_ci95_lower_pct": strategy["wilson_ci95_lower_pct"],
                    "bootstrap_avg_return_lower_pct": strategy["bootstrap_avg_return_lower_pct"],
                    "walk_forward_status": strategy["walk_forward_status"],
                    "v_trades": strategy["v_trades"],
                    "v_win_pct": strategy["v_win_pct"],
                    "o_trades": strategy["o_trades"],
                    "o_win_pct": strategy["o_win_pct"],
                    "tier": strategy["tier"],
                })

    book_rank = {"swing": 0, "daytrade": 1}
    tier_rank = {"S": 0, "A": 1, "B": 2}
    signals.sort(key=lambda s: (
        book_rank.get(s["book"], 9),
        tier_rank.get(s["tier"], 9),
        s["ticker"],
        -float(s["average_trade_return_pct"]),
        s["strategy_code"],
    ))

    if args.output_csv:
        output_path = Path(args.output_csv).resolve()
    else:
        suffix = args.as_of_date or latest_trade_date or date.today().isoformat()
        output_path = REPO_ROOT / "reports" / f"operacional-signals-{suffix}.csv"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SIGNAL_COLUMNS)
        writer.writeheader()
        for sig in signals:
            writer.writerow(sig)

    by_book = defaultdict(list)
    for s in signals:
        by_book[s["book"]].append(s)
    print(f"\nSinais para {latest_trade_date}: {len(signals)} total")
    for book in ("swing", "daytrade"):
        rows = by_book[book]
        if rows:
            tickers = sorted({s["ticker"] for s in rows})
            print(f"  {book}: {len(rows)} sinais em {len(tickers)} tickers -> {tickers}")
    print(f"CSV: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
