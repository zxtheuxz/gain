from __future__ import annotations

import argparse
import csv
import math
import re
from collections import defaultdict
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent


BOOKS = [
    {
        "name": "daytrade-principal",
        "title": "Day trade principal",
        "source": "1D",
        "targets": {2.0},
        "min_target_rate": 90.0,
        "min_test_target_rate": 90.0,
        "min_trades": 8,
        "description": "Compra no open, alvo 2%, saida no mesmo dia.",
    },
    {
        "name": "daytrade-cobertura-15",
        "title": "Day trade cobertura 1.5%",
        "source": "1D",
        "targets": {1.5},
        "min_target_rate": 90.0,
        "min_test_target_rate": 90.0,
        "min_trades": 8,
        "description": "Compra no open, alvo 1.5%, para aumentar cobertura de acoes.",
    },
    {
        "name": "daytrade-cobertura-10",
        "title": "Day trade cobertura 1%",
        "source": "1D",
        "targets": {1.0},
        "min_target_rate": 90.0,
        "min_test_target_rate": 90.0,
        "min_trades": 8,
        "description": "Compra no open, alvo 1%, fallback para papeis sem alvo maior.",
    },
    {
        "name": "swing-raro-7d-7",
        "title": "Swing raro 7D alvo 7%",
        "source": "7D",
        "targets": {7.0},
        "min_target_rate": 90.0,
        "min_test_target_rate": 90.0,
        "min_trades": 8,
        "description": "Setup raro, alvo 7%, prazo maximo 7 pregoes.",
    },
    {
        "name": "swing-principal-7d-5",
        "title": "Swing principal 7D alvo 5%",
        "source": "7D",
        "targets": {5.0},
        "min_target_rate": 90.0,
        "min_test_target_rate": 90.0,
        "min_trades": 8,
        "description": "Livro principal de swing: alvo 5%, prazo maximo 7 pregoes.",
    },
    {
        "name": "swing-cobertura-7d-4",
        "title": "Swing cobertura 7D alvo 4%",
        "source": "7D",
        "targets": {4.0},
        "min_target_rate": 90.0,
        "min_test_target_rate": 90.0,
        "min_trades": 8,
        "description": "Livro de cobertura de swing: alvo 4%, prazo maximo 7 pregoes.",
    },
    {
        "name": "swing-curto-5d-4-5",
        "title": "Swing curto 5D alvo 4%-5%",
        "source": "5D",
        "targets": {4.0, 5.0},
        "min_target_rate": 90.0,
        "min_test_target_rate": 90.0,
        "min_trades": 8,
        "description": "Alternativa mais curta: alvos 4%-5%, prazo maximo 5 pregoes.",
    },
]


SOURCE_DEFAULTS = {
    "1D": "reports/r5-sparse-v1-20m/cap-1d/r5-sparse-tickers.csv",
    "3D": "reports/r5-sparse-v1-20m/cap-3d/r5-sparse-tickers.csv",
    "5D": "reports/r5-sparse-v2-long-5d7d-2to10/cap-5d/r5-sparse-tickers.csv",
    "7D": "reports/r5-sparse-v2-long-5d7d-2to10/cap-7d/r5-sparse-tickers.csv",
}


OUTPUT_FIELDS = [
    "book",
    "priority",
    "cap",
    "ticker",
    "target_pct",
    "stop_pct",
    "time_cap_days",
    "trades",
    "active_months",
    "take_profit_rate_pct",
    "test_take_profit_rate_pct",
    "profitable_rate_pct",
    "test_profitable_rate_pct",
    "average_trade_return_pct",
    "test_average_trade_return_pct",
    "profit_factor",
    "test_profit_factor",
    "score",
    "strategy_code",
    "strategy_label",
    "entry_rule",
    "trade_direction",
    "state_signature",
    "feature_keys",
    "first_trade_date",
    "last_trade_date",
]


STRATEGY_FIELDS = [
    "book",
    "code",
    "label",
    "trade_direction",
    "entry_rule",
    "take_profit_pct",
    "stop_loss_pct",
    "time_cap_days",
    "state_size",
    "state_signature",
    "feature_keys",
    "tickers_with_matches",
    "total_occurrences",
    "success_rate_pct",
    "profitable_trade_rate_pct",
    "take_profit_rate_pct",
    "average_trade_return_pct",
    "net_trade_return_pct",
    "profit_factor",
]


TICKER_STATS_FIELDS = [
    "book",
    "strategy_code",
    "ticker",
    "trades",
    "total_trades",
    "active_months",
    "success_rate_pct",
    "profitable_rate_pct",
    "profitable_trade_rate_pct",
    "take_profit_rate_pct",
    "average_trade_return_pct",
    "net_trade_return_pct",
    "profit_factor",
    "first_trade_date",
    "last_trade_date",
]


def _float(value: str | None) -> float:
    if value in (None, ""):
        return 0.0
    if value.upper() == "INF":
        return math.inf
    return float(value)


def _int(value: str | None) -> int:
    if value in (None, ""):
        return 0
    return int(float(value))


def _display_float(value: float) -> str:
    if math.isinf(value):
        return "INF"
    return f"{value:.4f}"


def _load_rows(path: Path, cap: str) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as file_obj:
        rows = list(csv.DictReader(file_obj))
    for row in rows:
        row["_cap"] = cap
        row["_target"] = _float(row["target_pct"])
        row["_trades"] = _int(row["trades"])
        row["_months"] = _int(row["active_months"])
        row["_target_rate"] = _float(row["take_profit_rate_pct"])
        row["_test_target_rate"] = _float(row["test_take_profit_rate_pct"])
        row["_win_rate"] = _float(row["profitable_rate_pct"])
        row["_test_win_rate"] = _float(row["test_profitable_rate_pct"])
        row["_avg"] = _float(row["average_trade_return_pct"])
        row["_test_avg"] = _float(row["test_average_trade_return_pct"])
        row["_pf"] = _float(row["profit_factor"])
        row["_test_pf"] = _float(row["test_profit_factor"])
        row["_score"] = _float(row["score"])
    return rows


def _load_universe(path: Path) -> list[str]:
    seen = set()
    tickers = []
    for match in re.finditer(r"\b[A-Z]{4}\d{1,2}\b", path.read_text(encoding="utf-8")):
        ticker = match.group(0)
        if ticker not in seen:
            seen.add(ticker)
            tickers.append(ticker)
    return tickers


def _state_size(code: str) -> int:
    match = re.search(r"__(\d+)f__", code)
    if not match:
        return 0
    return int(match.group(1))


def _row_rank(row: dict[str, str]) -> tuple[float, int, int, float, float, float]:
    return (
        row["_target"],
        row["_trades"],
        row["_months"],
        row["_target_rate"],
        row["_test_target_rate"],
        row["_score"],
    )


def _select_book(rows: list[dict[str, str]], book: dict[str, object]) -> list[dict[str, str]]:
    candidates = [
        row
        for row in rows
        if row["trade_direction"] == "long"
        and row["_cap"] == book["source"]
        and row["_target"] in book["targets"]
        and row["_trades"] >= book["min_trades"]
        and row["_target_rate"] >= book["min_target_rate"]
        and row["_test_target_rate"] >= book["min_test_target_rate"]
    ]
    best_by_ticker: dict[str, dict[str, str]] = {}
    for row in candidates:
        current = best_by_ticker.get(row["ticker"])
        if current is None or _row_rank(row) > _row_rank(current):
            best_by_ticker[row["ticker"]] = row
    selected = sorted(best_by_ticker.values(), key=lambda row: (-row["_target"], -row["_trades"], row["ticker"]))
    for idx, row in enumerate(selected, start=1):
        row["book"] = str(book["name"])
        row["priority"] = str(idx)
    return selected


def _write_csv(path: Path, rows: list[dict[str, str]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _output_row(row: dict[str, str]) -> dict[str, str]:
    return {
        "book": row["book"],
        "priority": row["priority"],
        "cap": row["_cap"],
        "ticker": row["ticker"],
        "target_pct": row["target_pct"],
        "stop_pct": row["stop_pct"],
        "time_cap_days": row["time_cap_days"],
        "trades": row["trades"],
        "active_months": row["active_months"],
        "take_profit_rate_pct": row["take_profit_rate_pct"],
        "test_take_profit_rate_pct": row["test_take_profit_rate_pct"],
        "profitable_rate_pct": row["profitable_rate_pct"],
        "test_profitable_rate_pct": row["test_profitable_rate_pct"],
        "average_trade_return_pct": row["average_trade_return_pct"],
        "test_average_trade_return_pct": row["test_average_trade_return_pct"],
        "profit_factor": row["profit_factor"],
        "test_profit_factor": row["test_profit_factor"],
        "score": row["score"],
        "strategy_code": row["strategy_code"],
        "strategy_label": row["strategy_label"],
        "entry_rule": row["entry_rule"],
        "trade_direction": row["trade_direction"],
        "state_signature": row["state_signature"],
        "feature_keys": row["feature_keys"],
        "first_trade_date": row["first_trade_date"],
        "last_trade_date": row["last_trade_date"],
    }


def _aggregate_strategy_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    grouped: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[(row["book"], row["strategy_code"])].append(row)

    output = []
    for (book, code), items in grouped.items():
        trades = sum(item["_trades"] for item in items)
        if trades <= 0:
            continue
        weighted_target = sum(item["_target_rate"] * item["_trades"] for item in items) / trades
        weighted_win = sum(item["_win_rate"] * item["_trades"] for item in items) / trades
        weighted_avg = sum(item["_avg"] * item["_trades"] for item in items) / trades
        net = sum(item["_avg"] * item["_trades"] for item in items)
        finite_pfs = [item["_pf"] for item in items if math.isfinite(item["_pf"])]
        pf = max(finite_pfs) if finite_pfs else math.inf
        first = items[0]
        output.append(
            {
                "book": book,
                "code": code,
                "label": first["strategy_label"],
                "trade_direction": first["trade_direction"],
                "entry_rule": first["entry_rule"],
                "take_profit_pct": first["target_pct"],
                "stop_loss_pct": first["stop_pct"],
                "time_cap_days": first["time_cap_days"],
                "state_size": str(_state_size(code)),
                "state_signature": first["state_signature"],
                "feature_keys": first["feature_keys"],
                "tickers_with_matches": str(len({item["ticker"] for item in items})),
                "total_occurrences": str(trades),
                "success_rate_pct": f"{weighted_win:.4f}",
                "profitable_trade_rate_pct": f"{weighted_win:.4f}",
                "take_profit_rate_pct": f"{weighted_target:.4f}",
                "average_trade_return_pct": f"{weighted_avg:.4f}",
                "net_trade_return_pct": f"{net:.4f}",
                "profit_factor": _display_float(pf),
            }
        )
    output.sort(key=lambda row: (row["book"], -float(row["take_profit_pct"]), -int(row["total_occurrences"]), row["code"]))
    return output


def _ticker_stats_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    output = []
    for row in rows:
        net = row["_avg"] * row["_trades"]
        output.append(
            {
                "book": row["book"],
                "strategy_code": row["strategy_code"],
                "ticker": row["ticker"],
                "trades": row["trades"],
                "total_trades": row["trades"],
                "active_months": row["active_months"],
                "success_rate_pct": row["profitable_rate_pct"],
                "profitable_rate_pct": row["profitable_rate_pct"],
                "profitable_trade_rate_pct": row["profitable_rate_pct"],
                "take_profit_rate_pct": row["take_profit_rate_pct"],
                "average_trade_return_pct": row["average_trade_return_pct"],
                "net_trade_return_pct": f"{net:.4f}",
                "profit_factor": row["profit_factor"],
                "first_trade_date": row["first_trade_date"],
                "last_trade_date": row["last_trade_date"],
            }
        )
    return output


def _best_combined(rows: list[dict[str, str]], book_order: list[str]) -> list[dict[str, str]]:
    order = {name: idx for idx, name in enumerate(book_order)}
    best: dict[str, dict[str, str]] = {}
    for row in rows:
        current = best.get(row["ticker"])
        row_key = (-order[row["book"]], row["_target"], row["_trades"], row["_months"], row["_score"])
        if current is None:
            best[row["ticker"]] = row
            continue
        current_key = (-order[current["book"]], current["_target"], current["_trades"], current["_months"], current["_score"])
        if row_key > current_key:
            best[row["ticker"]] = row
    return sorted(best.values(), key=lambda row: (order[row["book"]], -row["_target"], row["ticker"]))


def _write_report(path: Path, universe: list[str], selections: dict[str, list[dict[str, str]]], combined_day: list[dict[str, str]], combined_swing: list[dict[str, str]]) -> None:
    covered_any = {row["ticker"] for rows in selections.values() for row in rows}
    lines = [
        "# Livros operacionais 1D-7D",
        "",
        "Objetivo: manter dois tipos de operacao para analise diaria sem carregar todos os sinais brutos.",
        "",
        "## Recomendacao de uso",
        "",
        "1. Day trade: priorizar `daytrade-principal` alvo 2%; usar `1.5%` e `1%` apenas para cobertura.",
        "2. Swing: priorizar `swing-raro-7d-7` quando aparecer, depois `swing-principal-7d-5`, depois `swing-cobertura-7d-4`.",
        "3. Usar no maximo uma linha por acao em cada livro combinado para deixar o painel analisavel.",
        "",
        "## Cobertura",
        "",
        f"- Universo em lista.md: `{len(universe)}` tickers detectados",
        f"- Acoes cobertas em algum livro: `{len(covered_any)}`",
        f"- Sem estrategia nos livros finais: `{', '.join([ticker for ticker in universe if ticker not in covered_any]) or 'nenhuma'}`",
        "",
        "## Livros",
        "",
        "| Livro | Acoes | Estrategias | Alvos | Trades mediana | Descricao |",
        "| --- | ---: | ---: | --- | ---: | --- |",
    ]
    for book in BOOKS:
        rows = selections[book["name"]]
        strategies = len({row["strategy_code"] for row in rows})
        targets = ", ".join(f"{target:g}%" for target in sorted({row["_target"] for row in rows}))
        trades = sorted(row["_trades"] for row in rows)
        median = trades[len(trades) // 2] if trades else 0
        lines.append(
            f"| `{book['name']}` | {len(rows)} | {strategies} | {targets or '-'} | {median} | {book['description']} |"
        )

    def add_top_section(title: str, rows: list[dict[str, str]], limit: int = 30) -> None:
        lines.extend(
            [
                "",
                f"## {title}",
                "",
                "| Rank | Livro | Ticker | Cap | Alvo | Trades | Meses | Alvo total | Alvo teste | Padrao |",
                "| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
            ]
        )
        for idx, row in enumerate(rows[:limit], start=1):
            label = row["strategy_label"].replace("|", "/")
            lines.append(
                f"| {idx} | `{row['book']}` | `{row['ticker']}` | {row['_cap']} | {row['_target']:g}% | "
                f"{row['_trades']} | {row['_months']} | {row['_target_rate']:.2f}% | {row['_test_target_rate']:.2f}% | {label} |"
            )

    day_sorted = sorted(combined_day, key=lambda row: (-row["_target"], -row["_trades"], row["ticker"]))
    swing_sorted = sorted(combined_swing, key=lambda row: (-row["_target"], -row["_trades"], row["ticker"]))
    add_top_section("Day trade combinado - melhor por acao", day_sorted)
    add_top_section("Swing combinado - melhor por acao", swing_sorted)

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build final operational strategy books.")
    parser.add_argument("--output-dir", default="reports/operational-books-v1")
    parser.add_argument("--tickers-file", default="lista.md")
    parser.add_argument("--source-1d", default=SOURCE_DEFAULTS["1D"])
    parser.add_argument("--source-3d", default=SOURCE_DEFAULTS["3D"])
    parser.add_argument("--source-5d", default=SOURCE_DEFAULTS["5D"])
    parser.add_argument("--source-7d", default=SOURCE_DEFAULTS["7D"])
    args = parser.parse_args()

    source_paths = {
        "1D": Path(args.source_1d),
        "3D": Path(args.source_3d),
        "5D": Path(args.source_5d),
        "7D": Path(args.source_7d),
    }
    all_rows: list[dict[str, str]] = []
    for cap, path in source_paths.items():
        if path.exists():
            all_rows.extend(_load_rows(path, cap))

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    universe = _load_universe(Path(args.tickers_file))

    selections: dict[str, list[dict[str, str]]] = {}
    all_selected: list[dict[str, str]] = []
    for book in BOOKS:
        selected = _select_book(all_rows, book)
        selections[book["name"]] = selected
        all_selected.extend(selected)
        _write_csv(output_dir / f"{book['name']}.csv", [_output_row(row) for row in selected], OUTPUT_FIELDS)

    combined_day = _best_combined(
        selections["daytrade-principal"] + selections["daytrade-cobertura-15"] + selections["daytrade-cobertura-10"],
        ["daytrade-principal", "daytrade-cobertura-15", "daytrade-cobertura-10"],
    )
    combined_swing = _best_combined(
        selections["swing-raro-7d-7"]
        + selections["swing-principal-7d-5"]
        + selections["swing-cobertura-7d-4"]
        + selections["swing-curto-5d-4-5"],
        ["swing-raro-7d-7", "swing-principal-7d-5", "swing-cobertura-7d-4", "swing-curto-5d-4-5"],
    )

    _write_csv(output_dir / "daytrade-combinado.csv", [_output_row(row) for row in combined_day], OUTPUT_FIELDS)
    _write_csv(output_dir / "swing-combinado.csv", [_output_row(row) for row in combined_swing], OUTPUT_FIELDS)
    _write_csv(output_dir / "operational-selected.csv", [_output_row(row) for row in all_selected], OUTPUT_FIELDS)
    _write_csv(output_dir / "monitor-strategies.csv", _aggregate_strategy_rows(all_selected), STRATEGY_FIELDS)
    _write_csv(output_dir / "monitor-ticker-stats.csv", _ticker_stats_rows(all_selected), TICKER_STATS_FIELDS)
    _write_report(output_dir / "operational-books.md", universe, selections, combined_day, combined_swing)

    print(f"output_dir={output_dir}")
    for book in BOOKS:
        rows = selections[book["name"]]
        print(f"{book['name']}: actions={len(rows)} strategies={len({row['strategy_code'] for row in rows})}")
    print(f"daytrade-combinado: actions={len(combined_day)}")
    print(f"swing-combinado: actions={len(combined_swing)}")
    print(f"monitor-strategies: {len(_aggregate_strategy_rows(all_selected))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
