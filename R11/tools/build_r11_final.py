"""R11 build final: copia do R10 build mas com prefix r11-*."""
from __future__ import annotations

import argparse
import csv
import math
import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))


SLIM_FIELDS = [
    "rank", "ticker", "strategy_code", "template_code", "template_label",
    "entry_rule", "exit_mode", "target_pct", "stop_pct", "holding_days",
    "state_size", "state_signature", "feature_keys",
    "m_trades", "m_win_pct", "m_avg_return_pct", "m_profit_factor",
    "m_take_profit", "m_stop_loss",
    "m1_trades", "m1_win_pct", "m1_avg_return_pct",
    "m2_trades", "m2_win_pct", "m2_avg_return_pct",
    "m3_trades", "m3_win_pct", "m3_avg_return_pct",
    "v_trades", "v_win_pct", "v_avg_return_pct", "v_profit_factor",
    "o_trades", "o_win_pct", "o_avg_return_pct", "o_profit_factor",
    "wilson_ci95_lower_pct", "binomial_p_mining", "binomial_p_val",
    "bootstrap_avg_return_lower_pct", "bootstrap_avg_return_upper_pct",
    "walk_forward_status",
    "fdr_q_value", "final_score",
    "first_trade_date", "last_trade_date",
]


def _float(v: object) -> float:
    if v in ("", None):
        return 0.0
    if str(v).upper() == "INF":
        return math.inf
    return float(v)


def _int(v: object) -> int:
    return int(float(v)) if v not in ("", None) else 0


def _strip_returns(row: dict[str, str]) -> dict[str, str]:
    return {k: v for k, v in row.items() if k not in ("m_returns", "v_returns", "o_returns")}


def _best_per_ticker(rows: list[dict[str, str]], top_per_ticker: int) -> list[dict[str, str]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for r in rows:
        grouped[r["ticker"]].append(r)
    out = []
    for ticker, items in grouped.items():
        items.sort(key=lambda r: -_float(r["final_score"]))
        for rank, item in enumerate(items[:top_per_ticker], start=1):
            slim = _strip_returns(item)
            slim["rank"] = str(rank)
            out.append(slim)
    out.sort(key=lambda r: (r["ticker"], int(r["rank"])))
    return out


def _write_csv(path: Path, rows: list[dict[str, str]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in fields})


def _write_report(
    path: Path,
    all_validated: list[dict[str, str]],
    best: list[dict[str, str]],
    gates: dict[str, str],
) -> None:
    lines = [
        "# R11 Portfolio Daytrade Validado (3 familias + Walk-Forward + Bootstrap)",
        "",
        "## Familias",
        "",
        "- **Familia 1 (intraday)**: target/stop em 1 pregao (alvos 1-2%, stops 0.5-2%)",
        "- **Familia 2 (mini-swing)**: target/stop em 2-3 pregoes (alvos 2-3%, stops 1-2.5%)",
        "- **Familia 3 (fixed)**: saida fixa, alvo livre (sucesso = retorno > 1%)",
        "",
        "## Janelas",
        "",
        f"- Mining: {gates['mining_start']} a {gates['mining_end']}",
        f"  - Sub-janela 1: <= {gates['mw1_end']}",
        f"  - Sub-janela 2: {gates['mw1_end']} a {gates['mw2_end']}",
        f"  - Sub-janela 3: {gates['mw2_end']} a {gates['mining_end']}",
        f"- Validacao: {gates['val_start']} a {gates['val_end']}",
        f"- Out-of-sample: {gates['oos_start']} a {gates['oos_end']}",
        "",
        "## Gates aplicados (calibrados pra daytrade)",
        "",
        f"- **Gate A (mining)**: trades >= {gates['min_mining_trades']}, "
        f"win >= {gates['min_mining_win']}%, PF >= {gates['min_mining_pf']}, "
        f"Wilson CI95 lower >= {gates['min_wilson']}%, binomial p < {gates['max_binom_m']}",
        f"- **Gate WF**: win >= {gates['wf_min_win']}% em cada sub-janela com >= {gates['wf_min_trades']} trades (min 2 de 3)",
        f"- **Gate B (val)**: trades >= {gates['min_val_trades']}, "
        f"win >= {gates['min_val_win']}%, avg return >= {gates['min_val_avg']}%, "
        f"binomial p < {gates['max_binom_v']}",
        f"- **Gate C (FDR)**: q-value < {gates['max_fdr_q']}",
        f"- **Gate Bootstrap**: CI95 lower bound do avg return mining > {gates['min_bootstrap_lower']}%",
        "",
        "## Resumo",
        "",
        f"- Setups que passaram TODOS os gates: **{len(all_validated)}**",
        f"- Tickers cobertos: {len({r['ticker'] for r in all_validated})}",
        f"- Final (top por ticker): {len(best)}",
        "",
        "## Top 30 (ranking final)",
        "",
        "| Rank | Ticker | Template | M trades | M win | WF | V trades | V win | OOS trades | OOS win | OOS avg | Wilson | Boot lower | Setup |",
        "|---:|---|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    top = sorted(all_validated, key=lambda r: -_float(r["final_score"]))[:30]
    for idx, r in enumerate(top, start=1):
        wf = r.get("walk_forward_status", "")
        wf_short = wf.split("(")[-1].rstrip(")") if "(" in wf else wf
        lines.append(
            f"| {idx} | `{r['ticker']}` | `{r['template_code']}` | "
            f"{r['m_trades']} | {_float(r['m_win_pct']):.1f}% | {wf_short} | "
            f"{r['v_trades']} | {_float(r['v_win_pct']):.1f}% | "
            f"{r['o_trades']} | {_float(r['o_win_pct']):.1f}% | "
            f"{_float(r['o_avg_return_pct']):.2f}% | "
            f"{_float(r['wilson_ci95_lower_pct']):.1f}% | "
            f"{_float(r['bootstrap_avg_return_lower_pct']):.2f}% | "
            f"{r['state_signature']} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_oos_report(path: Path, rows: list[dict[str, str]]) -> None:
    with_oos = [r for r in rows if _int(r["o_trades"]) > 0]
    oos_wins = sum(_int(r["o_success"]) for r in with_oos)
    oos_trades = sum(_int(r["o_trades"]) for r in with_oos)
    oos_returns = sum(_float(r["o_avg_return_pct"]) * _int(r["o_trades"]) for r in with_oos)
    win_pct = (oos_wins / oos_trades * 100.0) if oos_trades else 0.0
    avg_pct = (oos_returns / oos_trades) if oos_trades else 0.0

    lines = [
        "# R11 Out-of-Sample Performance (jan-mai 2026)",
        "",
        "Janela jamais usada durante mining/validacao. Mostra performance honesta.",
        "",
        f"- Setups validados: **{len(rows)}**",
        f"- Com >=1 trade no OOS: {len(with_oos)}",
        f"- Trades OOS totais: {oos_trades}",
        f"- **Win rate OOS agregado: {win_pct:.2f}%**",
        f"- **Avg return OOS agregado: {avg_pct:.2f}%**",
        "",
        "## Top 30 OOS",
        "",
        "| Ticker | Template | M win | WF | V win | OOS trades | OOS win | OOS avg | Boot lower |",
        "|---|---|---:|---|---:|---:|---:|---:|---:|",
    ]
    top = sorted(with_oos, key=lambda r: -_float(r["final_score"]))[:30]
    for r in top:
        wf = r.get("walk_forward_status", "")
        wf_short = wf.split("(")[-1].rstrip(")") if "(" in wf else wf
        lines.append(
            f"| `{r['ticker']}` | `{r['template_code']}` | "
            f"{_float(r['m_win_pct']):.1f}% | {wf_short} | "
            f"{_float(r['v_win_pct']):.1f}% | "
            f"{r['o_trades']} | {_float(r['o_win_pct']):.1f}% | "
            f"{_float(r['o_avg_return_pct']):.2f}% | "
            f"{_float(r['bootstrap_avg_return_lower_pct']):.2f}% |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--validated-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--top-per-ticker", type=int, default=3)
    parser.add_argument("--mining-start", default="2018-01-01")
    parser.add_argument("--mw1-end", default="2020-12-31")
    parser.add_argument("--mw2-end", default="2022-12-31")
    parser.add_argument("--mining-end", default="2024-12-31")
    parser.add_argument("--val-start", default="2025-01-01")
    parser.add_argument("--val-end", default="2025-12-31")
    parser.add_argument("--oos-start", default="2026-01-01")
    parser.add_argument("--oos-end", default="2026-05-22")
    parser.add_argument("--min-mining-trades", type=int, default=30)
    parser.add_argument("--min-mining-win", type=float, default=60.0)
    parser.add_argument("--min-mining-pf", type=float, default=1.8)
    parser.add_argument("--min-wilson", type=float, default=55.0)
    parser.add_argument("--max-binom-m", type=float, default=0.01)
    parser.add_argument("--wf-min-trades", type=int, default=3)
    parser.add_argument("--wf-min-win", type=float, default=60.0)
    parser.add_argument("--min-val-trades", type=int, default=10)
    parser.add_argument("--min-val-win", type=float, default=60.0)
    parser.add_argument("--min-val-avg", type=float, default=0.5)
    parser.add_argument("--max-binom-v", type=float, default=0.05)
    parser.add_argument("--max-fdr-q", type=float, default=0.05)
    parser.add_argument("--min-bootstrap-lower", type=float, default=0.0)
    args = parser.parse_args()

    validated_csv = Path(args.validated_csv)
    output_dir = Path(args.output_dir)

    rows = list(csv.DictReader(validated_csv.open()))
    print(f"R11 build | validated rows: {len(rows)}")

    slim = [_strip_returns(r) for r in rows]
    _write_csv(output_dir / "r11-validated-slim.csv", slim, SLIM_FIELDS[1:])

    best = _best_per_ticker(rows, args.top_per_ticker)
    _write_csv(output_dir / "r11-final.csv", best, SLIM_FIELDS)
    _write_csv(output_dir / "r11-signals-ready.csv", best, SLIM_FIELDS)

    gates = {
        "mining_start": args.mining_start,
        "mw1_end": args.mw1_end,
        "mw2_end": args.mw2_end,
        "mining_end": args.mining_end,
        "val_start": args.val_start,
        "val_end": args.val_end,
        "oos_start": args.oos_start,
        "oos_end": args.oos_end,
        "min_mining_trades": str(args.min_mining_trades),
        "min_mining_win": str(args.min_mining_win),
        "min_mining_pf": str(args.min_mining_pf),
        "min_wilson": str(args.min_wilson),
        "max_binom_m": str(args.max_binom_m),
        "wf_min_trades": str(args.wf_min_trades),
        "wf_min_win": str(args.wf_min_win),
        "min_val_trades": str(args.min_val_trades),
        "min_val_win": str(args.min_val_win),
        "min_val_avg": str(args.min_val_avg),
        "max_binom_v": str(args.max_binom_v),
        "max_fdr_q": str(args.max_fdr_q),
        "min_bootstrap_lower": str(args.min_bootstrap_lower),
    }
    _write_report(output_dir / "r11-report.md", rows, best, gates)
    _write_oos_report(output_dir / "r11-oos-report.md", rows)
    print(f"R11 build DONE | final csv: r11-final.csv ({len(best)} setups, {len({r['ticker'] for r in best})} tickers)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
