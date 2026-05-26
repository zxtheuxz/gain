"""Shim de compatibilidade R9 → implementacoes escalares Python puro.

Reconstrucao apos repo cleanup (commit 4349595). Expoe a API que
validate_r10.py importa:

    wilson_ci_lower, binomial_pvalue, benjamini_hochberg_qvalues, _bootstrap_ci95_mean

Logica identica ao validate_r11_fast.py mas escalar (sem numpy/pandas).
"""
from __future__ import annotations

import math
import random


def wilson_ci_lower(success: int, total: int, z: float = 1.96) -> float:
    """Wilson score confidence interval lower bound. Retorna fracao [0,1]."""
    if total <= 0:
        return 0.0
    phat = success / total
    z2 = z * z
    denom = 1.0 + z2 / total
    centre = phat + z2 / (2.0 * total)
    margin = z * math.sqrt(phat * (1.0 - phat) / total + z2 / (4.0 * total * total))
    return max(0.0, (centre - margin) / denom)


def _normal_sf(z: float) -> float:
    """1 - CDF normal padrao via aproximacao Abramowitz & Stegun 26.2.17."""
    if z > 8.0:
        return 0.0
    if z < -8.0:
        return 1.0
    sign = 1.0 if z >= 0 else -1.0
    za = abs(z)
    t = 1.0 / (1.0 + 0.2316419 * za)
    d = 0.3989422804014327
    p = d * math.exp(-0.5 * za * za) * (
        t * (0.319381530 + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))))
    )
    cdf = 0.5 + sign * (0.5 - p)
    return 1.0 - cdf


def binomial_pvalue(success: int, total: int, p0: float = 0.5) -> float:
    """One-sided p-value (H1: taxa > p0) via aproximacao normal."""
    if total < 5:
        return 1.0
    observed = success / total
    std_err = math.sqrt(p0 * (1.0 - p0))
    z = (observed - p0) / (std_err / math.sqrt(total))
    return _normal_sf(z)


def benjamini_hochberg_qvalues(pvalues: list[float]) -> list[float]:
    """FDR Benjamini-Hochberg. Retorna q-values na mesma ordem dos p-values."""
    n = len(pvalues)
    if n == 0:
        return []
    indexed = sorted(enumerate(pvalues), key=lambda x: x[1])
    q = [0.0] * n
    min_q = 1.0
    for rank, (orig_idx, pval) in enumerate(reversed(indexed), start=1):
        q_val = min(1.0, pval * n / (n - rank + 1))
        min_q = min(min_q, q_val)
        q[orig_idx] = min_q
    return q


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
