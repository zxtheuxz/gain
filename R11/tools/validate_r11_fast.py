"""R11 validate FAST: mesma logica do validate_r11 mas com pandas.

Vetorizado: Wilson, binomial e walk-forward viram operacoes em colunas
do DataFrame, FDR em numpy array. Bootstrap so nos sobreviventes.

Throughput esperado: ~10M linhas em 3-5 min (vs 2h+ com csv.DictReader).
"""
from __future__ import annotations

import argparse
import math
import random
import sys
from pathlib import Path

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))


def wilson_ci_lower_vec(succ: np.ndarray, total: np.ndarray, z: float = 1.96) -> np.ndarray:
    """Wilson lower bound vetorizado. Retorna em fracao [0,1]."""
    out = np.zeros_like(succ, dtype=np.float64)
    valid = total > 0
    if not valid.any():
        return out
    s = succ[valid].astype(np.float64)
    t = total[valid].astype(np.float64)
    phat = s / t
    z2 = z * z
    denom = 1.0 + z2 / t
    centre = phat + z2 / (2.0 * t)
    margin = z * np.sqrt(phat * (1.0 - phat) / t + z2 / (4.0 * t * t))
    out[valid] = np.maximum(0.0, (centre - margin) / denom)
    return out


def binomial_pvalue_vec(succ: np.ndarray, total: np.ndarray, p0: float = 0.5) -> np.ndarray:
    """One-sided p-value normal approximation. Retorna 1.0 onde total < 5."""
    out = np.ones_like(succ, dtype=np.float64)
    valid = total >= 5
    if not valid.any():
        return out
    s = succ[valid].astype(np.float64)
    t = total[valid].astype(np.float64)
    observed = s / t
    std_err = math.sqrt(p0 * (1.0 - p0))
    z = (observed - p0) / (std_err / np.sqrt(t))
    # Survival function da normal padrao = 1 - cdf(z) = cdf(-z)
    out[valid] = _normal_sf_vec(z)
    return out


def _normal_sf_vec(z: np.ndarray) -> np.ndarray:
    """1 - CDF(z) via aproximacao racional (Abramowitz & Stegun 26.2.17)."""
    # Trata extremos
    z = np.asarray(z, dtype=np.float64)
    out = np.empty_like(z)
    very_low = z < -8.0
    very_high = z > 8.0
    mid = ~(very_low | very_high)
    out[very_low] = 1.0
    out[very_high] = 0.0
    if mid.any():
        zm = z[mid]
        sign = np.where(zm >= 0, 1.0, -1.0)
        za = np.abs(zm)
        t = 1.0 / (1.0 + 0.2316419 * za)
        d = 0.3989422804014327
        p = d * np.exp(-0.5 * za * za) * (
            t * (0.319381530 + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))))
        )
        # cdf(z) = 0.5 + sign * (0.5 - p)
        cdf = 0.5 + sign * (0.5 - p)
        out[mid] = 1.0 - cdf
    return out


def benjamini_hochberg_qvalues_np(pvals: np.ndarray) -> np.ndarray:
    """FDR Benjamini-Hochberg vetorizado."""
    n = len(pvals)
    if n == 0:
        return np.array([])
    order = np.argsort(pvals)
    sorted_p = pvals[order]
    ranks = np.arange(1, n + 1)
    q_sorted = sorted_p * n / ranks
    # monotonia: q[i] = min(q[i], q[i+1], ...)
    q_sorted = np.minimum.accumulate(q_sorted[::-1])[::-1]
    q_sorted = np.minimum(q_sorted, 1.0)
    out = np.empty_like(q_sorted)
    out[order] = q_sorted
    return out


def _bootstrap_ci95_mean(returns: list[float], iters: int = 1000, seed: int = 42) -> tuple[float, float]:
    """Bootstrap percentile CI95 para a media, Python puro."""
    if not returns:
        return 0.0, 0.0
    rnd = random.Random(seed)
    n = len(returns)
    means = []
    for _ in range(iters):
        sample_sum = 0.0
        for _ in range(n):
            sample_sum += returns[rnd.randint(0, n - 1)]
        means.append(sample_sum / n)
    means.sort()
    lower_idx = max(0, int(iters * 0.025))
    upper_idx = min(iters - 1, int(iters * 0.975))
    return means[lower_idx], means[upper_idx]


def _parse_returns(s) -> list[float]:
    if not isinstance(s, str) or not s:
        return []
    return [float(x) for x in s.split(";") if x]


def validate(args: argparse.Namespace) -> int:
    input_csv = Path(args.input_csv)
    output_csv = Path(args.output_csv)

    print(f"R11-fast validate | lendo {input_csv} ...", flush=True)
    df = pd.read_csv(input_csv, low_memory=False)
    print(f"R11-fast | input rows (Gate A passed): {len(df):,}", flush=True)

    # === Conversoes numericas ===
    int_cols = ["m_trades", "m_success", "v_trades", "v_success", "o_trades", "o_success",
                "m1_trades", "m1_success", "m2_trades", "m2_success", "m3_trades", "m3_success"]
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(np.int64)

    float_cols = ["m_win_pct", "v_win_pct", "o_win_pct", "v_avg_return_pct",
                  "m1_win_pct", "m2_win_pct", "m3_win_pct"]
    for c in float_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    initial = len(df)

    # === Stats baratos vetorizados ===
    m_trades = df["m_trades"].to_numpy()
    m_success = df["m_success"].to_numpy()
    v_trades = df["v_trades"].to_numpy()
    v_success = df["v_success"].to_numpy()

    wilson = wilson_ci_lower_vec(m_success, m_trades) * 100.0
    binom_m = binomial_pvalue_vec(m_success, m_trades, p0=0.5)
    binom_v = np.where(v_trades > 0, binomial_pvalue_vec(v_success, v_trades, p0=0.5), 1.0)

    df["wilson_ci95_lower_pct"] = wilson
    df["binomial_p_mining"] = binom_m
    df["binomial_p_val"] = binom_v
    df["bootstrap_avg_return_lower_pct"] = np.nan
    df["bootstrap_avg_return_upper_pct"] = np.nan
    df["walk_forward_status"] = ""
    df["fdr_q_value"] = np.nan

    # === FDR sobre p-values de mining ===
    qvals = benjamini_hochberg_qvalues_np(binom_m)
    df["fdr_q_value"] = qvals

    # Gate A.1: Wilson
    mask = df["wilson_ci95_lower_pct"].to_numpy() >= args.min_wilson_ci_lower
    df_after = df[mask].copy()
    print(f"  passa Wilson CI95 >= {args.min_wilson_ci_lower:.1f}%: {len(df_after):,} / {initial:,}", flush=True)

    # Gate A.2: binomial p mining
    mask = df_after["binomial_p_mining"].to_numpy() < args.max_binom_p_mining
    df_after = df_after[mask]
    print(f"  passa binomial p mining < {args.max_binom_p_mining}: {len(df_after):,}", flush=True)

    # Gate WF: walk-forward
    # Conta janelas validas (trades >= wf_min_trades_per_window)
    m1_t = df_after["m1_trades"].to_numpy()
    m2_t = df_after["m2_trades"].to_numpy()
    m3_t = df_after["m3_trades"].to_numpy()
    m1_w = df_after["m1_win_pct"].to_numpy()
    m2_w = df_after["m2_win_pct"].to_numpy()
    m3_w = df_after["m3_win_pct"].to_numpy()
    min_t = args.wf_min_trades_per_window
    min_w = args.wf_min_win_per_window

    valid_1 = m1_t >= min_t
    valid_2 = m2_t >= min_t
    valid_3 = m3_t >= min_t
    n_valid = valid_1.astype(int) + valid_2.astype(int) + valid_3.astype(int)
    # pass_1 = True se janela invalida OU win >= min
    pass_1 = (~valid_1) | (m1_w >= min_w)
    pass_2 = (~valid_2) | (m2_w >= min_w)
    pass_3 = (~valid_3) | (m3_w >= min_w)
    wf_ok = (n_valid >= 2) & pass_1 & pass_2 & pass_3

    # Status string em python (so pra quem passou pelos demais gates antes)
    status_arr = np.full(len(df_after), "", dtype=object)
    for i in range(len(df_after)):
        parts = []
        for valid, win, t in ((valid_1[i], m1_w[i], m1_t[i]),
                              (valid_2[i], m2_w[i], m2_t[i]),
                              (valid_3[i], m3_w[i], m3_t[i])):
            parts.append(f"{int(win)}%" if valid else "—")
        n_v = n_valid[i]
        if wf_ok[i]:
            status_arr[i] = f"PASSOU {n_v}/{n_v} ({'/'.join(parts)})"
        else:
            if n_v < 2:
                status_arr[i] = f"DESCARTADO (so {n_v} janelas)"
            else:
                status_arr[i] = f"FALHOU walk-forward ({'/'.join(parts)})"
    df_after = df_after.copy()
    df_after["walk_forward_status"] = status_arr
    df_after = df_after[wf_ok]
    print(f"  passa walk-forward (win >= {min_w}% em cada janela com >= {min_t} trades): {len(df_after):,}", flush=True)

    # Gate B: val
    mask = (
        (df_after["v_trades"].to_numpy() >= args.min_val_trades)
        & (df_after["v_win_pct"].to_numpy() >= args.min_val_win)
        & (df_after["v_avg_return_pct"].to_numpy() >= args.min_val_avg)
        & (df_after["binomial_p_val"].to_numpy() < args.max_binom_p_val)
    )
    df_after = df_after[mask]
    print(f"  passa Gate B (val_trades >= {args.min_val_trades}, win >= {args.min_val_win}%, avg >= {args.min_val_avg}%): {len(df_after):,}", flush=True)

    # Gate C: FDR
    mask = df_after["fdr_q_value"].to_numpy() < args.max_fdr_q
    df_after = df_after[mask]
    print(f"  passa FDR q < {args.max_fdr_q}: {len(df_after):,}", flush=True)

    # Pre-score (sem bootstrap ainda) para selecionar top-N por ticker para bootstrap
    if len(df_after):
        v_win_pre = df_after["v_win_pct"].to_numpy()
        v_avg_pre = df_after["v_avg_return_pct"].to_numpy()
        v_tr_pre = df_after["v_trades"].to_numpy()
        wlsn_pre = df_after["wilson_ci95_lower_pct"].to_numpy()
        pre_score = v_win_pre * 4.0 + wlsn_pre * 3.0 + v_avg_pre * 10.0 + np.log1p(v_tr_pre) * 5.0
        df_after = df_after.copy()
        df_after["pre_score"] = pre_score

        # Top-N por ticker para bootstrap (default 20)
        top_n = args.bootstrap_top_n_per_ticker
        df_after = (
            df_after.sort_values("pre_score", ascending=False)
            .groupby("ticker", as_index=False, group_keys=False)
            .head(top_n)
            .copy()
        )
        print(f"  selecionado top {top_n} por ticker para bootstrap: {len(df_after):,}", flush=True)

    # Bootstrap so nos sobreviventes selecionados
    if len(df_after):
        print(f"  calculando bootstrap CI95 para {len(df_after):,} sobreviventes...", flush=True)
        lo_arr = np.zeros(len(df_after))
        hi_arr = np.zeros(len(df_after))
        m_returns_list = df_after["m_returns"].tolist()
        for i, mr_str in enumerate(m_returns_list):
            mr = _parse_returns(mr_str)
            lo, hi = _bootstrap_ci95_mean(mr, iters=args.bootstrap_iters)
            lo_arr[i] = lo
            hi_arr[i] = hi
        df_after = df_after.copy()
        df_after["bootstrap_avg_return_lower_pct"] = lo_arr
        df_after["bootstrap_avg_return_upper_pct"] = hi_arr

    # Gate Bootstrap
    mask = df_after["bootstrap_avg_return_lower_pct"].to_numpy() > args.min_bootstrap_lower
    df_after = df_after[mask]
    print(f"  passa bootstrap CI95 lower > {args.min_bootstrap_lower}%: {len(df_after):,}", flush=True)

    # Score final
    if len(df_after):
        v_win = df_after["v_win_pct"].to_numpy()
        v_avg = df_after["v_avg_return_pct"].to_numpy()
        v_tr = df_after["v_trades"].to_numpy()
        wlsn = df_after["wilson_ci95_lower_pct"].to_numpy()
        boot = df_after["bootstrap_avg_return_lower_pct"].to_numpy()
        score = v_win * 4.0 + wlsn * 3.0 + v_avg * 10.0 + np.log1p(v_tr) * 5.0 + boot * 8.0
        df_after = df_after.copy()
        df_after["final_score"] = score
        df_after = df_after.sort_values("final_score", ascending=False)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df_after.to_csv(output_csv, index=False)
    print(f"R11-fast validate DONE | validated rows: {len(df_after):,} | csv={output_csv}", flush=True)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-csv", required=True)
    parser.add_argument("--output-csv", required=True)
    parser.add_argument("--min-wilson-ci-lower", type=float, default=55.0)
    parser.add_argument("--max-binom-p-mining", type=float, default=0.01)
    parser.add_argument("--max-binom-p-val", type=float, default=0.05)
    parser.add_argument("--wf-min-trades-per-window", type=int, default=3)
    parser.add_argument("--wf-min-win-per-window", type=float, default=60.0)
    parser.add_argument("--min-val-trades", type=int, default=10)
    parser.add_argument("--min-val-win", type=float, default=60.0)
    parser.add_argument("--min-val-avg", type=float, default=0.5)
    parser.add_argument("--max-fdr-q", type=float, default=0.05)
    parser.add_argument("--min-bootstrap-lower", type=float, default=0.0)
    parser.add_argument("--bootstrap-iters", type=int, default=1000)
    parser.add_argument("--bootstrap-top-n-per-ticker", type=int, default=20,
                        help="Bootstrap so nos top N candidatos por ticker (pre-score) para reduzir custo.")
    args = parser.parse_args()
    return validate(args)


if __name__ == "__main__":
    raise SystemExit(main())
