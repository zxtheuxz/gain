from __future__ import annotations

import csv
import math
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"

STRATEGIES_CSV = REPORTS / "asset-discovery-lista-r2-operational-strategies.csv"
TRADES_CSV = REPORTS / "asset-discovery-lista-r2-free-trades-approved.csv"

OUTPUT_STRATEGIES_CSV = REPORTS / "asset-discovery-lista-r2-target-hit-strategies.csv"
OUTPUT_TICKERS_CSV = REPORTS / "asset-discovery-lista-r2-target-hit-tickers.csv"
OUTPUT_MD = REPORTS / "asset-discovery-lista-r2-target-hit-report.md"


MIN_STRATEGY_TARGET_HIT_RATE = 18.0
MIN_TICKER_TRADES = 15
MIN_TICKER_TARGET_HIT_RATE = 20.0


def _float(value: object) -> float:
    if value in ("", "None", None):
        return 0.0
    if str(value).upper() == "INF":
        return math.inf
    return float(value)


def _format(value: float, digits: int = 4) -> str:
    if math.isinf(value):
        return "INF"
    return f"{value:.{digits}f}"


def _pf(gross_profit: float, gross_loss: float) -> float:
    if gross_loss == 0:
        return math.inf if gross_profit > 0 else 0.0
    return gross_profit / gross_loss


def main() -> None:
    operational_rows: dict[str, dict[str, str]] = {}
    with STRATEGIES_CSV.open(encoding="utf-8", newline="") as file_obj:
        for row in csv.DictReader(file_obj):
            operational_rows[row["code"]] = row

    strategy_acc: dict[str, dict[str, object]] = defaultdict(
        lambda: {
            "trades": 0,
            "take_profit": 0,
            "stop_loss": 0,
            "time_cap": 0,
            "profitable": 0,
            "sum_return": 0.0,
            "gross_profit": 0.0,
            "gross_loss": 0.0,
            "tickers": set(),
        }
    )
    ticker_acc: dict[tuple[str, str], dict[str, object]] = defaultdict(
        lambda: {
            "trades": 0,
            "take_profit": 0,
            "stop_loss": 0,
            "time_cap": 0,
            "profitable": 0,
            "sum_return": 0.0,
            "gross_profit": 0.0,
            "gross_loss": 0.0,
            "first_trade_date": "",
            "last_trade_date": "",
        }
    )

    with TRADES_CSV.open(encoding="utf-8", newline="") as file_obj:
        for row in csv.DictReader(file_obj):
            strategy_code = row["strategy_code"]
            if strategy_code not in operational_rows:
                continue

            trade_return = _float(row["trade_return_pct"])
            exit_reason = row["exit_reason"]
            ticker = row["ticker"]

            for acc in (strategy_acc[strategy_code], ticker_acc[(strategy_code, ticker)]):
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

            strategy_acc[strategy_code]["tickers"].add(ticker)
            ticker_item = ticker_acc[(strategy_code, ticker)]
            trade_date = row["trigger_date"]
            if not ticker_item["first_trade_date"] or trade_date < str(ticker_item["first_trade_date"]):
                ticker_item["first_trade_date"] = trade_date
            if not ticker_item["last_trade_date"] or trade_date > str(ticker_item["last_trade_date"]):
                ticker_item["last_trade_date"] = trade_date

    strategy_rows: list[dict[str, object]] = []
    for strategy_code, base in operational_rows.items():
        acc = strategy_acc.get(strategy_code)
        if not acc:
            continue
        trades = int(acc["trades"])
        take_profit = int(acc["take_profit"])
        row = {
            "code": strategy_code,
            "label": base["label"],
            "entry_rule": base["entry_rule"],
            "take_profit_pct": base["take_profit_pct"],
            "stop_loss_pct": base["stop_loss_pct"],
            "time_cap_days": base["time_cap_days"],
            "state_signature": base["state_signature"],
            "trades": trades,
            "tickers": len(acc["tickers"]),
            "take_profit_count": take_profit,
            "take_profit_rate_pct": (take_profit / trades) * 100.0 if trades else 0.0,
            "stop_loss_count": int(acc["stop_loss"]),
            "stop_loss_rate_pct": (int(acc["stop_loss"]) / trades) * 100.0 if trades else 0.0,
            "time_cap_count": int(acc["time_cap"]),
            "time_cap_rate_pct": (int(acc["time_cap"]) / trades) * 100.0 if trades else 0.0,
            "profitable_rate_pct": (int(acc["profitable"]) / trades) * 100.0 if trades else 0.0,
            "average_trade_return_pct": float(acc["sum_return"]) / trades if trades else 0.0,
            "net_trade_return_pct": float(acc["sum_return"]),
            "profit_factor": _pf(float(acc["gross_profit"]), float(acc["gross_loss"])),
            "passes_target_hit_filter": ((take_profit / trades) * 100.0 if trades else 0.0) >= MIN_STRATEGY_TARGET_HIT_RATE,
        }
        strategy_rows.append(row)

    strategy_rows.sort(
        key=lambda row: (
            -float(row["take_profit_rate_pct"]),
            -float(row["average_trade_return_pct"]),
            -float(row["profitable_rate_pct"]),
            -int(row["trades"]),
        )
    )

    ticker_rows: list[dict[str, object]] = []
    for (strategy_code, ticker), acc in ticker_acc.items():
        strategy_row = operational_rows[strategy_code]
        trades = int(acc["trades"])
        take_profit = int(acc["take_profit"])
        take_profit_rate = (take_profit / trades) * 100.0 if trades else 0.0
        if trades < MIN_TICKER_TRADES or take_profit_rate < MIN_TICKER_TARGET_HIT_RATE:
            continue
        ticker_rows.append(
            {
                "strategy_code": strategy_code,
                "strategy_label": strategy_row["label"],
                "ticker": ticker,
                "trades": trades,
                "take_profit_count": take_profit,
                "take_profit_rate_pct": take_profit_rate,
                "stop_loss_count": int(acc["stop_loss"]),
                "stop_loss_rate_pct": (int(acc["stop_loss"]) / trades) * 100.0 if trades else 0.0,
                "time_cap_count": int(acc["time_cap"]),
                "time_cap_rate_pct": (int(acc["time_cap"]) / trades) * 100.0 if trades else 0.0,
                "profitable_rate_pct": (int(acc["profitable"]) / trades) * 100.0 if trades else 0.0,
                "average_trade_return_pct": float(acc["sum_return"]) / trades if trades else 0.0,
                "net_trade_return_pct": float(acc["sum_return"]),
                "profit_factor": _pf(float(acc["gross_profit"]), float(acc["gross_loss"])),
                "first_trade_date": acc["first_trade_date"],
                "last_trade_date": acc["last_trade_date"],
            }
        )
    ticker_rows.sort(
        key=lambda row: (
            -float(row["take_profit_rate_pct"]),
            -float(row["average_trade_return_pct"]),
            -float(row["profitable_rate_pct"]),
            row["ticker"],
        )
    )

    strategy_fields = [
        "code",
        "label",
        "entry_rule",
        "take_profit_pct",
        "stop_loss_pct",
        "time_cap_days",
        "state_signature",
        "trades",
        "tickers",
        "take_profit_count",
        "take_profit_rate_pct",
        "stop_loss_count",
        "stop_loss_rate_pct",
        "time_cap_count",
        "time_cap_rate_pct",
        "profitable_rate_pct",
        "average_trade_return_pct",
        "net_trade_return_pct",
        "profit_factor",
        "passes_target_hit_filter",
    ]
    with OUTPUT_STRATEGIES_CSV.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=strategy_fields)
        writer.writeheader()
        for row in strategy_rows:
            output = dict(row)
            for key in [
                "take_profit_rate_pct",
                "stop_loss_rate_pct",
                "time_cap_rate_pct",
                "profitable_rate_pct",
                "average_trade_return_pct",
                "net_trade_return_pct",
                "profit_factor",
            ]:
                output[key] = _format(float(output[key]))
            writer.writerow(output)

    ticker_fields = [
        "strategy_code",
        "strategy_label",
        "ticker",
        "trades",
        "take_profit_count",
        "take_profit_rate_pct",
        "stop_loss_count",
        "stop_loss_rate_pct",
        "time_cap_count",
        "time_cap_rate_pct",
        "profitable_rate_pct",
        "average_trade_return_pct",
        "net_trade_return_pct",
        "profit_factor",
        "first_trade_date",
        "last_trade_date",
    ]
    with OUTPUT_TICKERS_CSV.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=ticker_fields)
        writer.writeheader()
        for row in ticker_rows:
            output = dict(row)
            for key in [
                "take_profit_rate_pct",
                "stop_loss_rate_pct",
                "time_cap_rate_pct",
                "profitable_rate_pct",
                "average_trade_return_pct",
                "net_trade_return_pct",
                "profit_factor",
            ]:
                output[key] = _format(float(output[key]))
            writer.writerow(output)

    lines = [
        "# Rodada 2 - Filtro por Alvo Batido",
        "",
        "Este relatorio separa lucro por qualquer saida positiva de lucro por alvo cheio (`take_profit`).",
        "",
        f"- Filtro de estrategia: alvo batido em pelo menos `{MIN_STRATEGY_TARGET_HIT_RATE:.0f}%` dos trades.",
        f"- Filtro por acao: minimo `{MIN_TICKER_TRADES}` trades e alvo batido em pelo menos `{MIN_TICKER_TARGET_HIT_RATE:.0f}%` dos trades.",
        "",
        "## Estrategias operacionais por alvo batido",
        "",
        "| Estrategia | Trades | Alvo batido | Stop | Prazo | Acerto verde | Media | PF |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in strategy_rows:
        lines.append(
            f"| {row['label']} | {row['trades']} | {float(row['take_profit_rate_pct']):.2f}% "
            f"({row['take_profit_count']}) | {float(row['stop_loss_rate_pct']):.2f}% | "
            f"{float(row['time_cap_rate_pct']):.2f}% | {float(row['profitable_rate_pct']):.2f}% | "
            f"{float(row['average_trade_return_pct']):.2f}% | {float(row['profit_factor']):.2f} |"
        )

    lines.extend(["", "## Acoes com maior taxa de alvo batido", ""])
    for index, row in enumerate(ticker_rows[:30], 1):
        lines.append(
            f"{index}. `{row['ticker']}` | alvo `{float(row['take_profit_rate_pct']):.2f}%` "
            f"({row['take_profit_count']}/{row['trades']}) | media `{float(row['average_trade_return_pct']):.2f}%` | "
            f"acerto verde `{float(row['profitable_rate_pct']):.2f}%` | PF `{float(row['profit_factor']):.2f}` | {row['strategy_label']}"
        )

    lines.extend(
        [
            "",
            "## Leitura",
            "- Se a taxa de alvo batido for baixa, a estrategia pode ser boa para lucro medio, mas nao para esperar o alvo cheio.",
            "- Para isso, o proximo teste recomendado e reexecutar saidas menores, por exemplo `2:1`, `3:1`, `4:1.5`, `4:2`, e comparar alvo batido, media e PF no mesmo padrao.",
            "",
            "## Arquivos",
            f"- Estrategias: `{OUTPUT_STRATEGIES_CSV.relative_to(ROOT)}`",
            f"- Acoes: `{OUTPUT_TICKERS_CSV.relative_to(ROOT)}`",
        ]
    )
    OUTPUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(OUTPUT_STRATEGIES_CSV)
    print(OUTPUT_TICKERS_CSV)
    print(OUTPUT_MD)
    print(f"strategies={len(strategy_rows)}")
    print(f"strategies_pass_target_hit={sum(1 for row in strategy_rows if row['passes_target_hit_filter'])}")
    print(f"ticker_rows={len(ticker_rows)}")


if __name__ == "__main__":
    main()
