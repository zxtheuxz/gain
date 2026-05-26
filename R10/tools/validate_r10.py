"""R10 validation: Gate A (mining) + Gate WF (walk-forward 3 janelas) +
Gate B (val) + Gate Bootstrap (CI95 retorno > 0) + Gate C (FDR).

Reusa helpers de R9 (wilson_ci_lower, binomial_pvalue, FDR, bootstrap).
"""
from __future__ import annotations

import argparse
import csv
import math
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from R9.tools.validate_r9 import (  # noqa: E402
    wilson_ci_lower,
    binomial_pvalue,
    benjamini_hochberg_qvalues,
    _bootstrap_ci95_mean,
)


def _float(v: object) -> float:
    if v in ("", None):
        return 0.0
    if str(v).upper() == "INF":
        return math.inf
    return float(v)


def _int(v: object) -> int:
    return int(float(v)) if v not in ("", None) else 0


def _parse_returns(s: str) -> list[float]:
    if not s:
        return []
    return [float(x) for x in s.split(";") if x]


def _walk_forward_status(
    m1_trades: int, m1_win: float,
    m2_trades: int, m2_win: float,
    m3_trades: int, m3_win: float,
    *,
    min_trades_per_window: int,
    min_win_per_window: float,
) -> tuple[bool, str]:
    """Avalia walk-forward rigoroso.

    Regra:
    - Conta quantas janelas o ticker "existe" (trades >= min_trades_per_window)
    - Em todas as janelas em que existe, win >= min_win_per_window
    - Tem que existir em pelo menos 2 das 3 janelas para aprovar
    - Se existe em 3, todas tem que passar
    - Se existe em 2, ambas tem que passar
    - Se existe em 0-1, DESCARTADO

    Retorna (passou, status_str).
    """
    windows = [
        (1, m1_trades, m1_win),
        (2, m2_trades, m2_win),
        (3, m3_trades, m3_win),
    ]
    exists = [(i, t, w) for i, t, w in windows if t >= min_trades_per_window]
    if len(exists) < 2:
        return False, f"DESCARTADO (so {len(exists)} janelas com trades)"

    passes = [(i, t, w) for i, t, w in exists if w >= min_win_per_window]
    status = "/".join(
        f"{int(w)}%" if t >= min_trades_per_window else "—"
        for _, t, w in windows
    )

    if len(passes) != len(exists):
        return False, f"FALHOU walk-forward ({status})"
    return True, f"PASSOU {len(exists)}/{len(exists)} ({status})"


def validate(args: argparse.Namespace) -> int:
    input_csv = Path(args.input_csv)
    output_csv = Path(args.output_csv)

    rows = list(csv.DictReader(input_csv.open()))
    print(f"R10 validate | input rows (Gate A passed): {len(rows)}")

    # === Stats baratos (Wilson + binomial) para TODOS ===
    enriched = []
    pvals_mining = []
    for row in rows:
        m_trades = _int(row["m_trades"])
        m_success = _int(row["m_success"])
        v_trades = _int(row["v_trades"])
        v_success = _int(row["v_success"])

        wilson = wilson_ci_lower(m_success, m_trades) * 100.0
        binom_m = binomial_pvalue(m_success, m_trades, p0=0.5)
        binom_v = binomial_pvalue(v_success, v_trades, p0=0.5) if v_trades > 0 else 1.0

        enriched.append({
            **row,
            "wilson_ci95_lower_pct": f"{wilson:.4f}",
            "binomial_p_mining": f"{binom_m:.6g}",
            "binomial_p_val": f"{binom_v:.6g}",
            "bootstrap_avg_return_lower_pct": "",
            "bootstrap_avg_return_upper_pct": "",
            "walk_forward_status": "",
            "fdr_q_value": "",
        })
        pvals_mining.append(binom_m)

    # === FDR sobre p-values de mining ===
    qvals = benjamini_hochberg_qvalues(pvals_mining)
    for r, q in zip(enriched, qvals):
        r["fdr_q_value"] = f"{q:.6g}"

    initial = len(enriched)

    # Gate A.1: Wilson CI lower bound
    pass_wilson = [r for r in enriched if _float(r["wilson_ci95_lower_pct"]) >= args.min_wilson_ci_lower]
    print(f"  passa Wilson CI95 >= {args.min_wilson_ci_lower:.1f}%: {len(pass_wilson)} / {initial}")

    # Gate A.2: binomial p mining
    pass_binom_m = [r for r in pass_wilson if _float(r["binomial_p_mining"]) < args.max_binom_p_mining]
    print(f"  passa binomial p mining < {args.max_binom_p_mining}: {len(pass_binom_m)} / {len(pass_wilson)}")

    # Gate WF: walk-forward
    pass_wf = []
    for r in pass_binom_m:
        ok, status = _walk_forward_status(
            _int(r["m1_trades"]), _float(r["m1_win_pct"]),
            _int(r["m2_trades"]), _float(r["m2_win_pct"]),
            _int(r["m3_trades"]), _float(r["m3_win_pct"]),
            min_trades_per_window=args.wf_min_trades_per_window,
            min_win_per_window=args.wf_min_win_per_window,
        )
        r["walk_forward_status"] = status
        if ok:
            pass_wf.append(r)
    print(f"  passa walk-forward (win >= {args.wf_min_win_per_window}% em cada janela com >= {args.wf_min_trades_per_window} trades): {len(pass_wf)} / {len(pass_binom_m)}")

    # Gate B: val
    pass_val = []
    for r in pass_wf:
        v_trades = _int(r["v_trades"])
        v_win = _float(r["v_win_pct"])
        v_avg = _float(r["v_avg_return_pct"])
        v_p = _float(r["binomial_p_val"])
        if v_trades < args.min_val_trades:
            continue
        if v_win < args.min_val_win:
            continue
        if v_avg < args.min_val_avg:
            continue
        if v_p >= args.max_binom_p_val:
            continue
        pass_val.append(r)
    print(f"  passa Gate B (val_trades >= {args.min_val_trades}, win >= {args.min_val_win}%, avg >= {args.min_val_avg}%): {len(pass_val)} / {len(pass_wf)}")

    # Gate C: FDR
    pass_fdr = [r for r in pass_val if _float(r["fdr_q_value"]) < args.max_fdr_q]
    print(f"  passa FDR q < {args.max_fdr_q}: {len(pass_fdr)} / {len(pass_val)}")

    # Gate Bootstrap: CI95 lower bound do avg return > min
    if pass_fdr:
        print(f"  calculando bootstrap CI95 para {len(pass_fdr)} sobreviventes...")
        for r in pass_fdr:
            m_returns = _parse_returns(r.get("m_returns", ""))
            boot_lo, boot_hi = _bootstrap_ci95_mean(m_returns, iters=args.bootstrap_iters)
            r["bootstrap_avg_return_lower_pct"] = f"{boot_lo:.4f}"
            r["bootstrap_avg_return_upper_pct"] = f"{boot_hi:.4f}"

    pass_boot = [r for r in pass_fdr if _float(r["bootstrap_avg_return_lower_pct"]) > args.min_bootstrap_lower]
    print(f"  passa bootstrap CI95 lower > {args.min_bootstrap_lower}%: {len(pass_boot)} / {len(pass_fdr)}")

    # Score final
    for r in pass_boot:
        r["final_score"] = f"{(_float(r['v_win_pct']) * 4.0 + _float(r['wilson_ci95_lower_pct']) * 3.0 + _float(r['v_avg_return_pct']) * 10.0 + math.log1p(_int(r['v_trades'])) * 5.0 + _float(r['bootstrap_avg_return_lower_pct']) * 8.0):.4f}"

    pass_boot.sort(key=lambda r: -float(r["final_score"]))

    # Output
    out_fields = list(rows[0].keys()) if rows else []
    extra_fields = [
        "wilson_ci95_lower_pct",
        "binomial_p_mining",
        "binomial_p_val",
        "bootstrap_avg_return_lower_pct",
        "bootstrap_avg_return_upper_pct",
        "walk_forward_status",
        "fdr_q_value",
        "final_score",
    ]
    for f in extra_fields:
        if f not in out_fields:
            out_fields.append(f)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields)
        writer.writeheader()
        for r in pass_boot:
            writer.writerow({k: r.get(k, "") for k in out_fields})
    print(f"R10 validate DONE | validated rows: {len(pass_boot)} | csv={output_csv}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-csv", required=True)
    parser.add_argument("--output-csv", required=True)
    parser.add_argument("--min-wilson-ci-lower", type=float, default=70.0)
    parser.add_argument("--max-binom-p-mining", type=float, default=0.001)
    parser.add_argument("--max-binom-p-val", type=float, default=0.05)
    parser.add_argument("--wf-min-trades-per-window", type=int, default=3,
                        help="Minimo de trades para considerar a janela 'existente'.")
    parser.add_argument("--wf-min-win-per-window", type=float, default=70.0,
                        help="Minimo win rate (%%) em cada janela em que o ticker existe.")
    parser.add_argument("--min-val-trades", type=int, default=15)
    parser.add_argument("--min-val-win", type=float, default=75.0)
    parser.add_argument("--min-val-avg", type=float, default=4.0,
                        help="Minimo avg return (%%) na validacao (default 4 para swing).")
    parser.add_argument("--max-fdr-q", type=float, default=0.05)
    parser.add_argument("--min-bootstrap-lower", type=float, default=0.0,
                        help="Bootstrap CI95 lower bound do avg return mining (>%% indicado).")
    parser.add_argument("--bootstrap-iters", type=int, default=1000)
    args = parser.parse_args()
    return validate(args)


if __name__ == "__main__":
    raise SystemExit(main())
