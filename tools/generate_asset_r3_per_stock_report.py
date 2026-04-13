"""Per-stock pattern analysis for R3 strategies.

Reads the same approved strategies + trades CSVs from the R3 stat pipeline,
groups trades by ticker, identifies which approved strategies work best for
each individual stock, and generates a focused per-stock report.
"""
from __future__ import annotations

import csv
import math
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"

# Input files (produced by generate_asset_r3_stat_report.py)
FINAL_STRATEGIES_CSV = REPORTS / "asset-discovery-lista-r3-stat-final.csv"
TRADES_CSV = REPORTS / "asset-discovery-lista-r3-stat-trades-approved.csv"

# Output files
PER_STOCK_CSV = REPORTS / "asset-discovery-lista-r3-per-stock-patterns.csv"
PER_STOCK_REPORT_MD = REPORTS / "asset-discovery-lista-r3-per-stock-patterns.md"

# Per-stock filter thresholds (more demanding than global)
PER_STOCK_MIN_TRADES = 20
PER_STOCK_MIN_TARGET_HIT_RATE = 65.0
PER_STOCK_MIN_PROFITABLE_RATE = 70.0
PER_STOCK_MIN_AVG_RETURN = 1.5
PER_STOCK_MIN_PROFIT_FACTOR = 2.5


@dataclass(slots=True)
class TickerStrategyMetrics:
    ticker: str = ""
    strategy_code: str = ""
    strategy_label: str = ""
    entry_rule: str = ""
    trade_direction: str = ""
    target_pct: float = 0.0
    stop_pct: float = 0.0
    time_cap_days: int = 0
    state_signature: str = ""
    feature_keys: str = ""
    trades: int = 0
    take_profit: int = 0
    stop_loss: int = 0
    time_cap: int = 0
    profitable: int = 0
    sum_return: float = 0.0
    sum_return_sq: float = 0.0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    first_trade_date: str = ""
    last_trade_date: str = ""
    returns_list: list[float] = field(default_factory=list)

    def update(self, trade_return: float, exit_reason: str, trigger_date: str) -> None:
        self.trades += 1
        self.sum_return += trade_return
        self.sum_return_sq += trade_return * trade_return
        self.returns_list.append(trade_return)
        if exit_reason == "take_profit":
            self.take_profit += 1
        elif exit_reason.startswith("stop_loss"):
            self.stop_loss += 1
        elif exit_reason == "time_cap":
            self.time_cap += 1
        if trade_return > 0:
            self.profitable += 1
            self.gross_profit += trade_return
        elif trade_return < 0:
            self.gross_loss += abs(trade_return)
        if not self.first_trade_date or trigger_date < self.first_trade_date:
            self.first_trade_date = trigger_date
        if not self.last_trade_date or trigger_date > self.last_trade_date:
            self.last_trade_date = trigger_date

    @property
    def take_profit_rate(self) -> float:
        return (self.take_profit / self.trades) * 100.0 if self.trades else 0.0

    @property
    def stop_loss_rate(self) -> float:
        return (self.stop_loss / self.trades) * 100.0 if self.trades else 0.0

    @property
    def time_cap_rate(self) -> float:
        return (self.time_cap / self.trades) * 100.0 if self.trades else 0.0

    @property
    def profitable_rate(self) -> float:
        return (self.profitable / self.trades) * 100.0 if self.trades else 0.0

    @property
    def average_return(self) -> float:
        return self.sum_return / self.trades if self.trades else 0.0

    @property
    def net_return(self) -> float:
        return self.sum_return

    @property
    def profit_factor(self) -> float:
        if self.gross_loss == 0:
            return math.inf if self.gross_profit > 0 else 0.0
        return self.gross_profit / self.gross_loss

    @property
    def return_std(self) -> float:
        if self.trades < 2:
            return 0.0
        mean = self.sum_return / self.trades
        variance = max((self.sum_return_sq / self.trades) - (mean * mean), 0.0)
        return math.sqrt(variance)

    @property
    def sharpe_ratio(self) -> float:
        std = self.return_std
        if std <= 0:
            return 0.0
        return self.average_return / std

    def passes_filter(self) -> bool:
        return (
            self.trades >= PER_STOCK_MIN_TRADES
            and self.take_profit_rate >= PER_STOCK_MIN_TARGET_HIT_RATE
            and self.profitable_rate >= PER_STOCK_MIN_PROFITABLE_RATE
            and self.average_return >= PER_STOCK_MIN_AVG_RETURN
            and self.profit_factor >= PER_STOCK_MIN_PROFIT_FACTOR
        )

    @property
    def score(self) -> float:
        return (
            self.take_profit_rate * 3.0
            + self.average_return * 25.0
            + min(self.profit_factor, 8.0) * 10.0
            + math.log(max(self.trades, 1)) * 5.0
            + self.sharpe_ratio * 15.0
        )


def _float(value: object) -> float:
    if value in ("", None):
        return 0.0
    s = str(value).upper()
    if s == "INF":
        return math.inf
    return float(value)


def _int(value: object) -> int:
    return int(float(value)) if value not in ("", None) else 0


def _fmt(value: float, digits: int = 4) -> str:
    if math.isinf(value):
        return "INF"
    return f"{value:.{digits}f}"


def main() -> None:
    # Load final strategies (already filtered by train/test + significance)
    final_codes: set[str] = set()
    strategy_info: dict[str, dict[str, object]] = {}
    if FINAL_STRATEGIES_CSV.exists():
        with FINAL_STRATEGIES_CSV.open(encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                final_codes.add(row["code"])
                strategy_info[row["code"]] = row

    if not final_codes:
        print("No final strategies found. Run generate_asset_r3_stat_report.py first.")
        return

    # Read trades and accumulate per (ticker, strategy) metrics
    per_stock: dict[tuple[str, str], TickerStrategyMetrics] = {}
    with TRADES_CSV.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            strategy_code = row["strategy_code"]
            if strategy_code not in final_codes:
                continue
            ticker = row["ticker"]
            key = (ticker, strategy_code)
            m = per_stock.get(key)
            if m is None:
                info = strategy_info.get(strategy_code, {})
                m = TickerStrategyMetrics(
                    ticker=ticker,
                    strategy_code=strategy_code,
                    strategy_label=row["strategy_label"],
                    entry_rule=str(info.get("entry_rule", "")),
                    trade_direction=str(info.get("trade_direction", "long")),
                    target_pct=_float(info.get("target_pct")),
                    stop_pct=_float(info.get("stop_pct")),
                    time_cap_days=_int(info.get("time_cap_days")),
                    state_signature=str(info.get("state_signature", "")),
                    feature_keys=str(info.get("feature_keys", "")),
                )
                per_stock[key] = m
            m.update(
                trade_return=_float(row["trade_return_pct"]),
                exit_reason=row["exit_reason"],
                trigger_date=row["trigger_date"],
            )

    # Filter per-stock metrics
    approved: list[TickerStrategyMetrics] = [m for m in per_stock.values() if m.passes_filter()]
    approved.sort(key=lambda m: (-m.score, m.ticker, m.strategy_code))

    # Group by ticker for the report
    by_ticker: dict[str, list[TickerStrategyMetrics]] = defaultdict(list)
    for m in approved:
        by_ticker[m.ticker].append(m)

    # Sort tickers by number of qualifying strategies (desc), then best avg return
    ticker_order = sorted(
        by_ticker.keys(),
        key=lambda t: (
            -len(by_ticker[t]),
            -max(m.average_return for m in by_ticker[t]),
            t,
        ),
    )

    # Write CSV
    csv_fields = [
        "ticker", "strategy_code", "strategy_label", "entry_rule", "trade_direction",
        "target_pct", "stop_pct", "time_cap_days", "state_signature", "feature_keys",
        "trades", "take_profit_count", "take_profit_rate_pct", "stop_loss_count", "stop_loss_rate_pct",
        "time_cap_count", "time_cap_rate_pct", "profitable_count", "profitable_rate_pct",
        "average_trade_return_pct", "net_trade_return_pct", "profit_factor", "sharpe_ratio", "score",
        "first_trade_date", "last_trade_date",
    ]
    csv_rows: list[dict[str, object]] = []
    for m in approved:
        csv_rows.append({
            "ticker": m.ticker,
            "strategy_code": m.strategy_code,
            "strategy_label": m.strategy_label,
            "entry_rule": m.entry_rule,
            "trade_direction": m.trade_direction,
            "target_pct": _fmt(m.target_pct),
            "stop_pct": _fmt(m.stop_pct),
            "time_cap_days": m.time_cap_days,
            "state_signature": m.state_signature,
            "feature_keys": m.feature_keys,
            "trades": m.trades,
            "take_profit_count": m.take_profit,
            "take_profit_rate_pct": _fmt(m.take_profit_rate),
            "stop_loss_count": m.stop_loss,
            "stop_loss_rate_pct": _fmt(m.stop_loss_rate),
            "time_cap_count": m.time_cap,
            "time_cap_rate_pct": _fmt(m.time_cap_rate),
            "profitable_count": m.profitable,
            "profitable_rate_pct": _fmt(m.profitable_rate),
            "average_trade_return_pct": _fmt(m.average_return),
            "net_trade_return_pct": _fmt(m.net_return),
            "profit_factor": _fmt(m.profit_factor),
            "sharpe_ratio": _fmt(m.sharpe_ratio),
            "score": _fmt(m.score),
            "first_trade_date": m.first_trade_date,
            "last_trade_date": m.last_trade_date,
        })

    PER_STOCK_CSV.parent.mkdir(parents=True, exist_ok=True)
    with PER_STOCK_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields)
        writer.writeheader()
        writer.writerows(csv_rows)

    # Write markdown report
    lines = [
        "# Rodada 3 - Padroes por Acao Individual",
        "",
        "Analise por acao: quais estrategias aprovadas (train/test + significancia) funcionam especialmente bem para cada acao individual.",
        "",
        "## Filtro per-stock",
        "",
        f"- Trades >= {PER_STOCK_MIN_TRADES}",
        f"- Alvo batido >= {PER_STOCK_MIN_TARGET_HIT_RATE:.0f}%",
        f"- Acerto verde >= {PER_STOCK_MIN_PROFITABLE_RATE:.0f}%",
        f"- Media >= {PER_STOCK_MIN_AVG_RETURN:.1f}%",
        f"- PF >= {PER_STOCK_MIN_PROFIT_FACTOR:.1f}",
        "",
        "## Resultado",
        "",
        f"- Total de combinacoes acao x estrategia analisadas: `{len(per_stock)}`.",
        f"- Combinacoes que passaram o filtro: `{len(approved)}`.",
        f"- Acoes com pelo menos 1 padrao aprovado: `{len(by_ticker)}`.",
        "",
    ]

    if not ticker_order:
        lines.append("Nenhum padrao por acao encontrado com os filtros atuais.")
    else:
        lines.append("## Padroes por acao")
        lines.append("")

        for rank, ticker in enumerate(ticker_order, 1):
            strategies = by_ticker[ticker]
            best = strategies[0]
            lines.append(f"### {rank}. {ticker} ({len(strategies)} estrategia{'s' if len(strategies) > 1 else ''})")
            lines.append("")
            lines.append(
                "| Estrategia | Trades | Alvo batido | Acerto verde | Media | PF | Sharpe |"
            )
            lines.append(
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"
            )
            for m in strategies:
                short_label = m.state_signature
                lines.append(
                    f"| {short_label} | {m.trades} | {m.take_profit_rate:.1f}% | "
                    f"{m.profitable_rate:.1f}% | {m.average_return:.2f}% | "
                    f"{_fmt(m.profit_factor, 2)} | {m.sharpe_ratio:.2f} |"
                )
            lines.append("")
            lines.append(
                f"Melhor: quando *{best.state_signature}*, "
                f"bate alvo {best.target_pct:.0f}% em {best.take_profit_rate:.0f}% das vezes "
                f"({best.trades} trades, media {best.average_return:.2f}%)."
            )
            lines.append("")

    lines.extend([
        "## Arquivos",
        "",
        f"- CSV completo: `{PER_STOCK_CSV.relative_to(ROOT)}`",
    ])

    PER_STOCK_REPORT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"per_stock_combinations={len(per_stock)}")
    print(f"per_stock_approved={len(approved)}")
    print(f"per_stock_tickers={len(by_ticker)}")
    print(PER_STOCK_REPORT_MD)


if __name__ == "__main__":
    main()
