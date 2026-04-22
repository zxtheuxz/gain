from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"

APPROVED_CSV = REPORTS / "asset-discovery-lista-r3-stat-approved.csv"
TRADES_CSV = REPORTS / "asset-discovery-lista-r3-stat-trades-approved.csv"

FINAL_STRATEGIES_CSV = REPORTS / "asset-discovery-lista-r3-stat-final.csv"
FINAL_TICKERS_CSV = REPORTS / "asset-discovery-lista-r3-stat-final-tickers.csv"
FINAL_ACTIONS_CSV = REPORTS / "asset-discovery-lista-r3-stat-actions.csv"
SHORTLIST_STRATEGIES_CSV = REPORTS / "asset-discovery-lista-r3-stat-shortlist.csv"
SHORTLIST_TICKERS_CSV = REPORTS / "asset-discovery-lista-r3-stat-shortlist-tickers.csv"
SHORTLIST_ACTIONS_CSV = REPORTS / "asset-discovery-lista-r3-stat-shortlist-actions.csv"
REPORT_MD = REPORTS / "asset-discovery-lista-r3-stat-report.md"

# -- Train/test split --
TRAIN_END_DATE = "2025-12-31"
TEST_START_DATE = "2026-01-01"

# Train period thresholds
TRAIN_MIN_TRADES = 15
TRAIN_MIN_TARGET_HIT_RATE = 55.0
TRAIN_MIN_PROFITABLE_RATE = 58.0
TRAIN_MIN_AVG_RETURN = 0.80
TRAIN_MIN_PF = 1.8

# Test period thresholds
TEST_MIN_TRADES = 10
TEST_MIN_TARGET_HIT_RATE = 50.0
TEST_MIN_PROFITABLE_RATE = 55.0
TEST_MIN_AVG_RETURN = 0.50
TEST_MIN_PF = 1.5

# Legacy validation alias (used for the validation column in output)
VALIDATION_START_DATE = TEST_START_DATE

GLOBAL_MIN_VALIDATION_TRADES = 25
GLOBAL_MIN_VALIDATION_TARGET_HIT_RATE = 50.0
GLOBAL_MIN_VALIDATION_PROFITABLE_RATE = 55.0
GLOBAL_MIN_VALIDATION_AVG_RETURN = 0.20

TICKER_MIN_TRADES = 12
TICKER_MIN_TARGET_HIT_RATE = 60.0
TICKER_MIN_PROFITABLE_RATE = 60.0
TICKER_MIN_AVG_RETURN = 1.0
TICKER_MIN_PROFIT_FACTOR = 2.0

SHORTLIST_MIN_TARGET_HIT_RATE = 60.0
SHORTLIST_MIN_WIN_RATE = 60.0
SHORTLIST_MIN_AVG_RETURN = 1.00
SHORTLIST_MIN_PROFIT_FACTOR = 2.0
SHORTLIST_LIMIT = 0  # 0 = sem limite, todas que passam no filtro de qualidade entram

# -- Statistical significance --
SIGNIFICANCE_LEVEL = 0.05


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gera relatorio estatistico R3 com train/test split e significancia."
    )
    parser.add_argument("--approved-csv", default=str(APPROVED_CSV))
    parser.add_argument("--trades-csv", default=str(TRADES_CSV))
    parser.add_argument("--final-strategies-csv", default=str(FINAL_STRATEGIES_CSV))
    parser.add_argument("--final-tickers-csv", default=str(FINAL_TICKERS_CSV))
    parser.add_argument("--final-actions-csv", default=str(FINAL_ACTIONS_CSV))
    parser.add_argument("--shortlist-strategies-csv", default=str(SHORTLIST_STRATEGIES_CSV))
    parser.add_argument("--shortlist-tickers-csv", default=str(SHORTLIST_TICKERS_CSV))
    parser.add_argument("--shortlist-actions-csv", default=str(SHORTLIST_ACTIONS_CSV))
    parser.add_argument("--report-md", default=str(REPORT_MD))
    return parser.parse_args()


@dataclass(slots=True)
class Metrics:
    trades: int = 0
    take_profit: int = 0
    stop_loss: int = 0
    time_cap: int = 0
    profitable: int = 0
    sum_return: float = 0.0
    sum_return_sq: float = 0.0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    returns_list: list[float] = field(default_factory=list)

    def update(self, trade_return: float, exit_reason: str) -> None:
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


def _normal_cdf(z: float) -> float:
    """Approximate standard normal CDF using Abramowitz & Stegun 26.2.17."""
    if z < -8.0:
        return 0.0
    if z > 8.0:
        return 1.0
    sign = 1.0 if z >= 0 else -1.0
    z = abs(z)
    t = 1.0 / (1.0 + 0.2316419 * z)
    d = 0.3989422804014327  # 1/sqrt(2*pi)
    p = d * math.exp(-0.5 * z * z) * (
        t * (0.319381530 + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))))
    )
    return 0.5 + sign * (0.5 - p)


def binomial_pvalue(successes: int, trials: int, p0: float = 0.5) -> float:
    """One-sided p-value for H0: success_rate <= p0 using normal approximation."""
    if trials < 5:
        return 1.0
    observed_rate = successes / trials
    std_err = math.sqrt(p0 * (1.0 - p0) / trials)
    if std_err <= 0:
        return 0.0 if observed_rate > p0 else 1.0
    z = (observed_rate - p0) / std_err
    return 1.0 - _normal_cdf(z)


def ttest_pvalue(mean: float, std: float, n: int) -> float:
    """One-sided p-value for H0: mean <= 0 using normal approximation to t-distribution."""
    if n < 5 or std <= 0:
        return 1.0
    t_stat = mean / (std / math.sqrt(n))
    return 1.0 - _normal_cdf(t_stat)


def _float(value: object) -> float:
    if value in ("", None):
        return 0.0
    if str(value).upper() == "INF":
        return math.inf
    return float(value)


def _int(value: object) -> int:
    return int(float(value)) if value not in ("", None) else 0


def _fmt(value: float, digits: int = 4) -> str:
    if math.isinf(value):
        return "INF"
    return f"{value:.{digits}f}"


def _metrics_fields(prefix: str = "") -> list[str]:
    return [
        f"{prefix}trades",
        f"{prefix}take_profit_count",
        f"{prefix}take_profit_rate_pct",
        f"{prefix}stop_loss_count",
        f"{prefix}stop_loss_rate_pct",
        f"{prefix}time_cap_count",
        f"{prefix}time_cap_rate_pct",
        f"{prefix}profitable_count",
        f"{prefix}profitable_rate_pct",
        f"{prefix}average_trade_return_pct",
        f"{prefix}net_trade_return_pct",
        f"{prefix}profit_factor",
    ]


def _metrics_row(metrics: Metrics, prefix: str = "") -> dict[str, object]:
    return {
        f"{prefix}trades": metrics.trades,
        f"{prefix}take_profit_count": metrics.take_profit,
        f"{prefix}take_profit_rate_pct": metrics.take_profit_rate,
        f"{prefix}stop_loss_count": metrics.stop_loss,
        f"{prefix}stop_loss_rate_pct": metrics.stop_loss_rate,
        f"{prefix}time_cap_count": metrics.time_cap,
        f"{prefix}time_cap_rate_pct": metrics.time_cap_rate,
        f"{prefix}profitable_count": metrics.profitable,
        f"{prefix}profitable_rate_pct": metrics.profitable_rate,
        f"{prefix}average_trade_return_pct": metrics.average_return,
        f"{prefix}net_trade_return_pct": metrics.sum_return,
        f"{prefix}profit_factor": metrics.profit_factor,
    }


def _write_csv(path: Path, rows: list[dict[str, object]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            output = {field: row.get(field, "") for field in fields}
            for key, value in list(output.items()):
                if key.endswith("_pct") or key in {"profit_factor", "validation_profit_factor", "score", "best_profit_factor"}:
                    if value != "":
                        output[key] = _fmt(float(value))
            writer.writerow(output)


def _strategy_score(row: dict[str, object]) -> float:
    trades = int(row.get("trades", 0))
    tickers = int(row.get("tickers", 0))
    sharpe = float(row.get("sharpe_ratio", 0))
    test_tp_rate = float(row.get("test_take_profit_rate_pct", row.get("validation_take_profit_rate_pct", 0)))
    test_avg = float(row.get("test_average_trade_return_pct", row.get("validation_average_trade_return_pct", 0)))
    validation_perf = test_tp_rate * 0.5 + test_avg * 20.0
    return (
        float(row["take_profit_rate_pct"]) * 3.0          # target hit (weight 3)
        + float(row["average_trade_return_pct"]) * 25.0    # avg return (weight 2)
        + min(float(row["profit_factor"]), 8.0) * 10.0     # PF (weight 2)
        + math.log(max(trades, 1)) * 5.0                   # trade count (weight 1)
        + min(tickers, 50) * 2.0                           # stock diversity (weight 1)
        + validation_perf * 3.0                            # regime stability (weight 3)
        + sharpe * 15.0                                    # consistency (weight 2)
    )


def _build_action_rows(ticker_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    action_acc: dict[str, dict[str, object]] = {}
    for row in ticker_rows:
        item = action_acc.setdefault(
            str(row["ticker"]),
            {
                "ticker": row["ticker"],
                "strategies_count": 0,
                "total_trades": 0,
                "sum_average_trade_return_pct": 0.0,
                "sum_profitable_rate_pct": 0.0,
                "sum_take_profit_rate_pct": 0.0,
                "best_profit_factor": 0.0,
                "best_strategy_label": "",
            },
        )
        item["strategies_count"] = int(item["strategies_count"]) + 1
        item["total_trades"] = int(item["total_trades"]) + int(row["trades"])
        item["sum_average_trade_return_pct"] = float(item["sum_average_trade_return_pct"]) + float(row["average_trade_return_pct"])
        item["sum_profitable_rate_pct"] = float(item["sum_profitable_rate_pct"]) + float(row["profitable_rate_pct"])
        item["sum_take_profit_rate_pct"] = float(item["sum_take_profit_rate_pct"]) + float(row["take_profit_rate_pct"])
        if float(row["profit_factor"]) > float(item["best_profit_factor"]):
            item["best_profit_factor"] = float(row["profit_factor"])
            item["best_strategy_label"] = row["strategy_label"]

    action_rows: list[dict[str, object]] = []
    for item in action_acc.values():
        strategies_count = int(item["strategies_count"])
        row = {
            "ticker": item["ticker"],
            "strategies_count": strategies_count,
            "total_trades": item["total_trades"],
            "average_of_average_trade_return_pct": float(item["sum_average_trade_return_pct"]) / strategies_count,
            "average_profitable_trade_rate_pct": float(item["sum_profitable_rate_pct"]) / strategies_count,
            "average_take_profit_rate_pct": float(item["sum_take_profit_rate_pct"]) / strategies_count,
            "best_profit_factor": item["best_profit_factor"],
            "best_strategy_label": item["best_strategy_label"],
        }
        row["score"] = (
            float(row["average_take_profit_rate_pct"]) * 2.0
            + float(row["average_of_average_trade_return_pct"]) * 30.0
            + min(float(row["best_profit_factor"]), 12.0) * 8.0
            + strategies_count * 5.0
        )
        action_rows.append(row)
    action_rows.sort(key=lambda row: (-float(row["score"]), row["ticker"]))
    return action_rows


def main() -> None:
    args = parse_args()
    approved_csv = Path(args.approved_csv)
    trades_csv = Path(args.trades_csv)
    final_strategies_csv = Path(args.final_strategies_csv)
    final_tickers_csv = Path(args.final_tickers_csv)
    final_actions_csv = Path(args.final_actions_csv)
    shortlist_strategies_csv = Path(args.shortlist_strategies_csv)
    shortlist_tickers_csv = Path(args.shortlist_tickers_csv)
    shortlist_actions_csv = Path(args.shortlist_actions_csv)
    report_md = Path(args.report_md)

    approved_rows: dict[str, dict[str, object]] = {}
    with approved_csv.open(encoding="utf-8", newline="") as file_obj:
        for row in csv.DictReader(file_obj):
            parsed: dict[str, object] = dict(row)
            for key in [
                "take_profit_pct",
                "stop_loss_pct",
                "take_profit_rate_pct",
                "stop_loss_rate_pct",
                "time_cap_rate_pct",
                "profitable_trade_rate_pct",
                "average_trade_return_pct",
                "net_trade_return_pct",
                "profit_factor",
            ]:
                parsed[key] = _float(row[key])
            for key in ["time_cap_days", "state_size", "tickers_with_matches", "total_occurrences"]:
                parsed[key] = _int(row[key])
            approved_rows[str(row["code"])] = parsed

    train_by_strategy: dict[str, Metrics] = defaultdict(Metrics)
    test_by_strategy: dict[str, Metrics] = defaultdict(Metrics)
    full_by_strategy: dict[str, Metrics] = defaultdict(Metrics)
    validation_by_strategy = test_by_strategy  # alias for backward compat
    ticker_metrics: dict[tuple[str, str], Metrics] = defaultdict(Metrics)
    ticker_dates: dict[tuple[str, str], dict[str, str]] = defaultdict(lambda: {"first_trade_date": "", "last_trade_date": ""})

    with trades_csv.open(encoding="utf-8", newline="") as file_obj:
        for row in csv.DictReader(file_obj):
            strategy_code = row["strategy_code"]
            trade_return = _float(row["trade_return_pct"])
            exit_reason = row["exit_reason"]
            trigger_date = row["trigger_date"]
            ticker = row["ticker"]
            full_by_strategy[strategy_code].update(trade_return, exit_reason)
            if trigger_date <= TRAIN_END_DATE:
                train_by_strategy[strategy_code].update(trade_return, exit_reason)
            if trigger_date >= TEST_START_DATE:
                test_by_strategy[strategy_code].update(trade_return, exit_reason)
            key = (strategy_code, ticker)
            ticker_metrics[key].update(trade_return, exit_reason)
            dates = ticker_dates[key]
            if not dates["first_trade_date"] or trigger_date < dates["first_trade_date"]:
                dates["first_trade_date"] = trigger_date
            if not dates["last_trade_date"] or trigger_date > dates["last_trade_date"]:
                dates["last_trade_date"] = trigger_date

    final_strategies: list[dict[str, object]] = []
    train_test_rejected = 0
    significance_rejected = 0
    for code, row in approved_rows.items():
        validation = validation_by_strategy[code]
        train = train_by_strategy[code]
        test = test_by_strategy[code]
        full = full_by_strategy[code]
        output = {
            "code": code,
            "label": row["label"],
            "family": row["family"],
            "trade_direction": row["trade_direction"],
            "entry_rule": row["entry_rule"],
            "target_pct": row["take_profit_pct"],
            "stop_pct": row["stop_loss_pct"],
            "time_cap_days": row["time_cap_days"],
            "state_size": row["state_size"],
            "state_signature": row["state_signature"],
            "feature_keys": row["feature_keys"],
            "tickers": row["tickers_with_matches"],
            "trades": row["total_occurrences"],
            "take_profit_count": row["take_profit_trades"],
            "take_profit_rate_pct": row["take_profit_rate_pct"],
            "stop_loss_count": row["stop_loss_trades"],
            "stop_loss_rate_pct": row["stop_loss_rate_pct"],
            "time_cap_count": row["time_cap_trades"],
            "time_cap_rate_pct": row["time_cap_rate_pct"],
            "profitable_count": row["profitable_trades"],
            "profitable_rate_pct": row["profitable_trade_rate_pct"],
            "average_trade_return_pct": row["average_trade_return_pct"],
            "net_trade_return_pct": row["net_trade_return_pct"],
            "profit_factor": row["profit_factor"],
            "first_trade_date": row["first_trade_date"],
            "last_trade_date": row["last_trade_date"],
            **_metrics_row(validation, prefix="validation_"),
            **_metrics_row(train, prefix="train_"),
            **_metrics_row(test, prefix="test_"),
        }
        # Sharpe ratio from full period
        output["sharpe_ratio"] = full.sharpe_ratio

        # Significance tests on full period
        binom_p = binomial_pvalue(full.profitable, full.trades, 0.5)
        ttest_p = ttest_pvalue(full.average_return, full.return_std, full.trades)
        output["binomial_pvalue"] = binom_p
        output["ttest_pvalue"] = ttest_p

        # Train/test split filter
        passes_train = (
            train.trades >= TRAIN_MIN_TRADES
            and train.take_profit_rate >= TRAIN_MIN_TARGET_HIT_RATE
            and train.profitable_rate >= TRAIN_MIN_PROFITABLE_RATE
            and train.average_return >= TRAIN_MIN_AVG_RETURN
            and train.profit_factor >= TRAIN_MIN_PF
        )
        passes_test = (
            test.trades >= TEST_MIN_TRADES
            and test.take_profit_rate >= TEST_MIN_TARGET_HIT_RATE
            and test.profitable_rate >= TEST_MIN_PROFITABLE_RATE
            and test.average_return >= TEST_MIN_AVG_RETURN
            and test.profit_factor >= TEST_MIN_PF
        )
        passes_train_test = passes_train and passes_test

        # Legacy validation filter (backward compat)
        output["passes_validation_filter"] = (
            int(output["validation_trades"]) >= GLOBAL_MIN_VALIDATION_TRADES
            and float(output["validation_take_profit_rate_pct"]) >= GLOBAL_MIN_VALIDATION_TARGET_HIT_RATE
            and float(output["validation_profitable_rate_pct"]) >= GLOBAL_MIN_VALIDATION_PROFITABLE_RATE
            and float(output["validation_average_trade_return_pct"]) >= GLOBAL_MIN_VALIDATION_AVG_RETURN
        )

        # Statistical significance filter
        passes_significance = binom_p < SIGNIFICANCE_LEVEL and ttest_p < SIGNIFICANCE_LEVEL

        if not passes_train_test:
            train_test_rejected += 1
            continue
        if not passes_significance:
            significance_rejected += 1
            continue

        output["score"] = _strategy_score(output)
        final_strategies.append(output)

    final_strategies.sort(
        key=lambda row: (
            -float(row["score"]),
            -float(row["take_profit_rate_pct"]),
            -float(row["average_trade_return_pct"]),
            -int(row["trades"]),
        )
    )
    final_codes = {str(row["code"]) for row in final_strategies}

    ticker_rows: list[dict[str, object]] = []
    for (strategy_code, ticker), metrics in ticker_metrics.items():
        if strategy_code not in final_codes:
            continue
        strategy = approved_rows[strategy_code]
        row = {
            "strategy_code": strategy_code,
            "strategy_label": strategy["label"],
            "ticker": ticker,
            "target_pct": strategy["take_profit_pct"],
            "stop_pct": strategy["stop_loss_pct"],
            "time_cap_days": strategy["time_cap_days"],
            **_metrics_row(metrics),
            **ticker_dates[(strategy_code, ticker)],
        }
        row["passes_ticker_filter"] = (
            int(row["trades"]) >= TICKER_MIN_TRADES
            and float(row["take_profit_rate_pct"]) >= TICKER_MIN_TARGET_HIT_RATE
            and float(row["profitable_rate_pct"]) >= TICKER_MIN_PROFITABLE_RATE
            and float(row["average_trade_return_pct"]) >= TICKER_MIN_AVG_RETURN
            and float(row["profit_factor"]) >= TICKER_MIN_PROFIT_FACTOR
        )
        if row["passes_ticker_filter"]:
            ticker_rows.append(row)

    ticker_rows.sort(
        key=lambda row: (
            row["strategy_code"],
            -float(row["take_profit_rate_pct"]),
            -float(row["average_trade_return_pct"]),
            row["ticker"],
        )
    )

    action_rows = _build_action_rows(ticker_rows)

    shortlist_strategies: list[dict[str, object]] = []
    seen_signatures: set[str] = set()
    for row in final_strategies:
        if float(row["take_profit_rate_pct"]) < SHORTLIST_MIN_TARGET_HIT_RATE:
            continue
        if float(row["profitable_rate_pct"]) < SHORTLIST_MIN_WIN_RATE:
            continue
        if float(row["average_trade_return_pct"]) < SHORTLIST_MIN_AVG_RETURN:
            continue
        if float(row["profit_factor"]) < SHORTLIST_MIN_PROFIT_FACTOR:
            continue
        signature = str(row["state_signature"])
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)
        shortlist_strategies.append(row)
        if SHORTLIST_LIMIT > 0 and len(shortlist_strategies) >= SHORTLIST_LIMIT:
            break

    shortlist_codes = {str(row["code"]) for row in shortlist_strategies}
    shortlist_ticker_rows = [row for row in ticker_rows if str(row["strategy_code"]) in shortlist_codes]
    shortlist_action_rows = _build_action_rows(shortlist_ticker_rows)

    strategy_fields = [
        "code", "label", "family", "trade_direction", "entry_rule", "target_pct", "stop_pct",
        "time_cap_days", "state_size", "state_signature", "feature_keys", "tickers",
        *_metrics_fields(), *_metrics_fields("validation_"),
        *_metrics_fields("train_"), *_metrics_fields("test_"),
        "sharpe_ratio", "binomial_pvalue", "ttest_pvalue",
        "score", "passes_validation_filter",
        "first_trade_date", "last_trade_date",
    ]
    ticker_fields = [
        "strategy_code", "strategy_label", "ticker", "target_pct", "stop_pct", "time_cap_days",
        *_metrics_fields(), "passes_ticker_filter", "first_trade_date", "last_trade_date",
    ]
    action_fields = [
        "ticker", "strategies_count", "total_trades", "average_of_average_trade_return_pct",
        "average_profitable_trade_rate_pct", "average_take_profit_rate_pct", "best_profit_factor",
        "score", "best_strategy_label",
    ]

    _write_csv(final_strategies_csv, final_strategies, strategy_fields)
    _write_csv(final_tickers_csv, ticker_rows, ticker_fields)
    _write_csv(final_actions_csv, action_rows, action_fields)
    _write_csv(shortlist_strategies_csv, shortlist_strategies, strategy_fields)
    _write_csv(shortlist_tickers_csv, shortlist_ticker_rows, ticker_fields)
    _write_csv(shortlist_actions_csv, shortlist_action_rows, action_fields)

    lines = [
        "# Rodada 3 - Relatorio Operacional (com train/test split e significancia)",
        "",
        "Esta rodada procurou padroes estatisticos em acoes liquidas da B3, usando OHLCV e fatores derivados: retorno acumulado, drawdown, medias moveis, volume, volatilidade, candle, posicao no range, RSI, Bollinger, EMA e dia da semana.",
        "",
        "## Corte final",
        "",
        "- A estrategia ja veio do discovery bruto com os filtros configurados na execucao original.",
        f"- **Train/test split**: treino ate `{TRAIN_END_DATE}`, teste desde `{TEST_START_DATE}`.",
        f"  - Treino: trades >= {TRAIN_MIN_TRADES}, alvo >= {TRAIN_MIN_TARGET_HIT_RATE:.0f}%, green >= {TRAIN_MIN_PROFITABLE_RATE:.0f}%, media >= {TRAIN_MIN_AVG_RETURN:.1f}%, PF >= {TRAIN_MIN_PF:.1f}.",
        f"  - Teste: trades >= {TEST_MIN_TRADES}, alvo >= {TEST_MIN_TARGET_HIT_RATE:.0f}%, green >= {TEST_MIN_PROFITABLE_RATE:.0f}%, media >= {TEST_MIN_AVG_RETURN:.1f}%, PF >= {TEST_MIN_PF:.1f}.",
        f"- **Significancia estatistica**: teste binomial (win > 50%) e t-test (media > 0) com p < {SIGNIFICANCE_LEVEL}.",
        f"- Por acao exigimos pelo menos {TICKER_MIN_TRADES} trades, alvo batido >= {TICKER_MIN_TARGET_HIT_RATE:.0f}%, media >= {TICKER_MIN_AVG_RETURN:.1f}% e PF >= {TICKER_MIN_PROFIT_FACTOR:.1f}.",
        "",
        "## Resultado",
        "",
        f"- Estrategias aprovadas no discovery bruto: `{len(approved_rows)}`.",
        f"- Rejeitadas por train/test split: `{train_test_rejected}`.",
        f"- Rejeitadas por significancia estatistica: `{significance_rejected}`.",
        f"- Estrategias finais: `{len(final_strategies)}`.",
        f"- Linhas estrategia x acao aprovadas: `{len(ticker_rows)}`.",
        f"- Acoes finais: `{len(action_rows)}`.",
        f"- Shortlist operacional sem repeticao de assinatura: `{len(shortlist_strategies)}` estrategias, `{len(shortlist_ticker_rows)}` linhas estrategia x acao, `{len(shortlist_action_rows)}` acoes.",
        "",
        "## Top estrategias",
        "",
        "| Estrategia | Trades | Acoes | Alvo | Stop | Cap | Alvo batido | Acerto verde | Media | PF | Valid alvo | Valid media |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in final_strategies[:30]:
        lines.append(
            f"| {row['label']} | {row['trades']} | {row['tickers']} | {float(row['target_pct']):.2f}% | "
            f"{float(row['stop_pct']):.2f}% | {row['time_cap_days']}D | {float(row['take_profit_rate_pct']):.2f}% | "
            f"{float(row['profitable_rate_pct']):.2f}% | {float(row['average_trade_return_pct']):.2f}% | "
            f"{_fmt(float(row['profit_factor']), 2)} | {float(row['validation_take_profit_rate_pct']):.2f}% | "
            f"{float(row['validation_average_trade_return_pct']):.2f}% |"
        )

    lines.extend(["", "## Top acoes consolidadas", ""])
    for index, row in enumerate(action_rows[:40], 1):
        lines.append(
            f"{index}. `{row['ticker']}` | estrategias `{row['strategies_count']}` | trades `{row['total_trades']}` | "
            f"alvo medio `{float(row['average_take_profit_rate_pct']):.2f}%` | acerto verde medio `{float(row['average_profitable_trade_rate_pct']):.2f}%` | "
            f"media `{float(row['average_of_average_trade_return_pct']):.2f}%` | PF melhor `{_fmt(float(row['best_profit_factor']), 2)}`"
        )

    lines.extend(["", "## Shortlist operacional", ""])
    for index, row in enumerate(shortlist_strategies, 1):
        lines.append(
            f"{index}. alvo `{float(row['target_pct']):.2f}%` / stop `{float(row['stop_pct']):.2f}%` / cap `{row['time_cap_days']}D` | "
            f"alvo batido `{float(row['take_profit_rate_pct']):.2f}%` | validacao alvo `{float(row['validation_take_profit_rate_pct']):.2f}%` | "
            f"media `{float(row['average_trade_return_pct']):.2f}%` | {row['label']}"
        )

    lines.extend(["", "## Arquivos", ""])
    lines.append(f"- Estrategias finais: `{final_strategies_csv}`")
    lines.append(f"- Acoes por estrategia: `{final_tickers_csv}`")
    lines.append(f"- Acoes consolidadas: `{final_actions_csv}`")
    lines.append(f"- Shortlist estrategias: `{shortlist_strategies_csv}`")
    lines.append(f"- Shortlist acoes por estrategia: `{shortlist_tickers_csv}`")
    lines.append(f"- Shortlist acoes consolidadas: `{shortlist_actions_csv}`")
    report_md.parent.mkdir(parents=True, exist_ok=True)
    report_md.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"final_strategies={len(final_strategies)}")
    print(f"final_tickers={len(ticker_rows)}")
    print(f"final_actions={len(action_rows)}")
    print(f"shortlist_strategies={len(shortlist_strategies)}")
    print(f"shortlist_tickers={len(shortlist_ticker_rows)}")
    print(f"shortlist_actions={len(shortlist_action_rows)}")
    print(report_md)


if __name__ == "__main__":
    main()
