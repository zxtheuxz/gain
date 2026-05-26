"""R10 engine: discovery com walk-forward 3 janelas + janela mining por ticker.

Evolui R9:
- AccR10 com 3 sub-buckets de mining (m1/m2/m3 para walk-forward) alem de m/v/o
- Janela mining ajustada por ticker (usa o que cada ticker tem disponivel)
- So trilha swing
- Coleta retornos para bootstrap CI95 posterior

Nao modifica codigo R7/R9. Reusa Template, Outcome, _prepare_ticker_samples.
"""
from __future__ import annotations

import argparse
import csv
import math
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from itertools import combinations
from pathlib import Path
from time import monotonic

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from b3_patterns.db import connect, initialize_database  # noqa: E402
from b3_patterns.models import SpotQuoteBar  # noqa: E402
from b3_patterns.tickers import load_tickers  # noqa: E402
from R7.tools.run_r7a_asset_specific import (  # noqa: E402
    Outcome,
    Template,
    _combo_code,
    _combo_label,
    _pf,
    _prepare_ticker_samples,
    _rate,
)


def _load_price_bars(db_path, allowed_tickers: set[str]) -> dict[str, list[SpotQuoteBar]]:
    """Carrega bars de price_history (Yahoo, profundo) ajustados pelo fator adj_close/close."""
    from collections import defaultdict as _dd
    grouped: dict[str, list[SpotQuoteBar]] = _dd(list)
    with connect(db_path) as conn:
        initialize_database(conn)
        rows = conn.execute(
            """
            SELECT ticker, trade_date, open, high, low, close, adj_close, volume
            FROM price_history
            ORDER BY ticker, trade_date
            """
        ).fetchall()
    for row in rows:
        ticker_full = str(row["ticker"]).upper()
        ticker = ticker_full.removesuffix(".SA")
        if ticker not in allowed_tickers:
            continue
        close = float(row["close"])
        adj_close = float(row["adj_close"])
        factor = adj_close / close if close > 0 else 1.0
        grouped[ticker].append(
            SpotQuoteBar(
                ticker=ticker,
                trade_date=str(row["trade_date"]),
                open=float(row["open"]) * factor,
                high=float(row["high"]) * factor,
                low=float(row["low"]) * factor,
                close=adj_close,
                volume=int(row["volume"]),
                trade_count=0,
            )
        )
    return dict(grouped)


@dataclass(slots=True)
class AccR10:
    """Acumulador com 6 buckets: m1, m2, m3 (sub-windows de mining), m (mining full), v (val), o (oos)."""

    # MINING completo (agregado m1+m2+m3 + qualquer trade antes de mw1_start)
    m_trades: int = 0
    m_success: int = 0
    m_profitable: int = 0
    m_take_profit: int = 0
    m_stop_loss: int = 0
    m_gross_profit: float = 0.0
    m_gross_loss: float = 0.0
    m_net_return: float = 0.0
    m_returns: list[float] = field(default_factory=list)

    # MINING SUB-WINDOW 1 (ex.: 2018-2020)
    m1_trades: int = 0
    m1_success: int = 0
    m1_net_return: float = 0.0

    # MINING SUB-WINDOW 2 (ex.: 2021-2022)
    m2_trades: int = 0
    m2_success: int = 0
    m2_net_return: float = 0.0

    # MINING SUB-WINDOW 3 (ex.: 2023-2024)
    m3_trades: int = 0
    m3_success: int = 0
    m3_net_return: float = 0.0

    # VALIDATION
    v_trades: int = 0
    v_success: int = 0
    v_profitable: int = 0
    v_gross_profit: float = 0.0
    v_gross_loss: float = 0.0
    v_net_return: float = 0.0
    v_returns: list[float] = field(default_factory=list)

    # OUT-OF-SAMPLE (so reporta)
    o_trades: int = 0
    o_success: int = 0
    o_profitable: int = 0
    o_gross_profit: float = 0.0
    o_gross_loss: float = 0.0
    o_net_return: float = 0.0
    o_returns: list[float] = field(default_factory=list)

    first_trade_date: str = ""
    last_trade_date: str = ""

    def add(
        self,
        outcome: Outcome,
        mw1_end: date,
        mw2_end: date,
        mining_end: date,
        val_end: date,
    ) -> None:
        """Distribui trade no bucket correto. Janelas:
        - m1: trade_date <= mw1_end (e >= mw1_start implicito pela janela do engine)
        - m2: mw1_end < trade_date <= mw2_end
        - m3: mw2_end < trade_date <= mining_end
        - m:  trade_date <= mining_end (agregado, sempre acumula se for mining)
        - v:  mining_end < trade_date <= val_end
        - o:  trade_date > val_end
        """
        trade_day = date.fromisoformat(outcome.trade_date)
        if not self.first_trade_date:
            self.first_trade_date = outcome.trade_date
        self.last_trade_date = outcome.trade_date

        if trade_day <= mining_end:
            # full mining bucket
            self.m_trades += 1
            if outcome.success:
                self.m_success += 1
            if outcome.profitable:
                self.m_profitable += 1
                self.m_gross_profit += outcome.trade_return_pct
            elif outcome.trade_return_pct < 0:
                self.m_gross_loss += abs(outcome.trade_return_pct)
            if outcome.take_profit:
                self.m_take_profit += 1
            if outcome.stop_loss:
                self.m_stop_loss += 1
            self.m_net_return += outcome.trade_return_pct
            self.m_returns.append(outcome.trade_return_pct)

            # sub-window
            if trade_day <= mw1_end:
                self.m1_trades += 1
                if outcome.success:
                    self.m1_success += 1
                self.m1_net_return += outcome.trade_return_pct
            elif trade_day <= mw2_end:
                self.m2_trades += 1
                if outcome.success:
                    self.m2_success += 1
                self.m2_net_return += outcome.trade_return_pct
            else:
                self.m3_trades += 1
                if outcome.success:
                    self.m3_success += 1
                self.m3_net_return += outcome.trade_return_pct
        elif trade_day <= val_end:
            self.v_trades += 1
            if outcome.success:
                self.v_success += 1
            if outcome.profitable:
                self.v_profitable += 1
                self.v_gross_profit += outcome.trade_return_pct
            elif outcome.trade_return_pct < 0:
                self.v_gross_loss += abs(outcome.trade_return_pct)
            self.v_net_return += outcome.trade_return_pct
            self.v_returns.append(outcome.trade_return_pct)
        else:
            self.o_trades += 1
            if outcome.success:
                self.o_success += 1
            if outcome.profitable:
                self.o_profitable += 1
                self.o_gross_profit += outcome.trade_return_pct
            elif outcome.trade_return_pct < 0:
                self.o_gross_loss += abs(outcome.trade_return_pct)
            self.o_net_return += outcome.trade_return_pct
            self.o_returns.append(outcome.trade_return_pct)


def _accumulate_r10(
    samples,
    combos_iter,
    max_accumulators: int,
    mw1_end: date,
    mw2_end: date,
    mining_end: date,
    val_end: date,
) -> dict[tuple, AccR10]:
    accs: dict[tuple, AccR10] = {}
    for items, outcome in samples:
        for combo in combos_iter(items):
            acc = accs.get(combo)
            if acc is None:
                if len(accs) >= max_accumulators:
                    continue
                acc = AccR10()
                accs[combo] = acc
            acc.add(outcome, mw1_end, mw2_end, mining_end, val_end)
    return accs


def _promote_mining(
    accs: dict[tuple, AccR10],
    *,
    min_trades: int,
    min_success_rate: float,
    min_profit_factor: float,
    limit: int,
) -> set[tuple]:
    """Promove combos pela performance no MINING somente."""
    rows = []
    for combo, acc in accs.items():
        if acc.m_trades < min_trades:
            continue
        success_rate = _rate(acc.m_success, acc.m_trades)
        if success_rate < min_success_rate:
            continue
        pf = _pf(acc.m_gross_profit, acc.m_gross_loss)
        if pf < min_profit_factor:
            continue
        score = (
            success_rate * math.log1p(acc.m_trades)
            + min(pf, 20.0) * 5.0
            + (acc.m_net_return / max(acc.m_trades, 1))
        )
        rows.append((score, combo))
    rows.sort(reverse=True, key=lambda item: item[0])
    return {combo for _, combo in rows[:limit]}


def _gate_a_pass(
    acc: AccR10,
    *,
    min_trades: int,
    min_profit_factor: float,
    min_success_rate: float,
) -> bool:
    """Gate A bruto (sem stats). Stats sao calculados na fase de validacao."""
    if acc.m_trades < min_trades:
        return False
    pf = _pf(acc.m_gross_profit, acc.m_gross_loss)
    if pf < min_profit_factor:
        return False
    success_rate = _rate(acc.m_success, acc.m_trades)
    if success_rate < min_success_rate:
        return False
    return True


def _fmt_pct(value: float) -> str:
    return f"{value:.4f}"


def _fmt_float(value: float) -> str:
    if math.isinf(value):
        return "INF"
    return f"{value:.4f}"


def _fmt_returns(values: list[float]) -> str:
    return ";".join(f"{v:.6f}" for v in values)


def _row_for_acc(
    ticker: str,
    template: Template,
    combo: tuple,
    acc: AccR10,
) -> dict[str, str]:
    m_pf = _pf(acc.m_gross_profit, acc.m_gross_loss)
    v_pf = _pf(acc.v_gross_profit, acc.v_gross_loss)
    o_pf = _pf(acc.o_gross_profit, acc.o_gross_loss)
    m_avg = acc.m_net_return / acc.m_trades if acc.m_trades else 0.0
    v_avg = acc.v_net_return / acc.v_trades if acc.v_trades else 0.0
    o_avg = acc.o_net_return / acc.o_trades if acc.o_trades else 0.0
    m_win = _rate(acc.m_success, acc.m_trades)
    v_win = _rate(acc.v_success, acc.v_trades)
    o_win = _rate(acc.o_success, acc.o_trades)
    m1_win = _rate(acc.m1_success, acc.m1_trades)
    m2_win = _rate(acc.m2_success, acc.m2_trades)
    m3_win = _rate(acc.m3_success, acc.m3_trades)
    m1_avg = acc.m1_net_return / acc.m1_trades if acc.m1_trades else 0.0
    m2_avg = acc.m2_net_return / acc.m2_trades if acc.m2_trades else 0.0
    m3_avg = acc.m3_net_return / acc.m3_trades if acc.m3_trades else 0.0
    return {
        "ticker": ticker,
        "strategy_code": f"r10_swing_{ticker}_{template.code}__{len(combo)}f__{_combo_code(combo)}",
        "template_code": template.code,
        "template_label": template.label,
        "entry_rule": template.entry_rule,
        "exit_mode": template.exit_mode,
        "target_pct": _fmt_pct(template.target_pct),
        "stop_pct": _fmt_pct(template.stop_pct),
        "holding_days": str(template.holding_days),
        "state_size": str(len(combo)),
        "state_signature": _combo_label(combo),
        "feature_keys": ",".join(key for key, _, _ in combo),
        # Mining full
        "m_trades": str(acc.m_trades),
        "m_success": str(acc.m_success),
        "m_win_pct": _fmt_pct(m_win),
        "m_avg_return_pct": _fmt_pct(m_avg),
        "m_net_return_pct": _fmt_pct(acc.m_net_return),
        "m_take_profit": str(acc.m_take_profit),
        "m_stop_loss": str(acc.m_stop_loss),
        "m_profit_factor": _fmt_float(m_pf),
        "m_returns": _fmt_returns(acc.m_returns),
        # Walk-forward sub-windows
        "m1_trades": str(acc.m1_trades),
        "m1_success": str(acc.m1_success),
        "m1_win_pct": _fmt_pct(m1_win),
        "m1_avg_return_pct": _fmt_pct(m1_avg),
        "m2_trades": str(acc.m2_trades),
        "m2_success": str(acc.m2_success),
        "m2_win_pct": _fmt_pct(m2_win),
        "m2_avg_return_pct": _fmt_pct(m2_avg),
        "m3_trades": str(acc.m3_trades),
        "m3_success": str(acc.m3_success),
        "m3_win_pct": _fmt_pct(m3_win),
        "m3_avg_return_pct": _fmt_pct(m3_avg),
        # Val
        "v_trades": str(acc.v_trades),
        "v_success": str(acc.v_success),
        "v_win_pct": _fmt_pct(v_win),
        "v_avg_return_pct": _fmt_pct(v_avg),
        "v_profit_factor": _fmt_float(v_pf),
        "v_returns": _fmt_returns(acc.v_returns),
        # Oos
        "o_trades": str(acc.o_trades),
        "o_success": str(acc.o_success),
        "o_win_pct": _fmt_pct(o_win),
        "o_avg_return_pct": _fmt_pct(o_avg),
        "o_profit_factor": _fmt_float(o_pf),
        "o_returns": _fmt_returns(acc.o_returns),
        "first_trade_date": acc.first_trade_date,
        "last_trade_date": acc.last_trade_date,
    }


MINING_FIELDS = [
    "ticker", "strategy_code", "template_code", "template_label",
    "entry_rule", "exit_mode", "target_pct", "stop_pct", "holding_days",
    "state_size", "state_signature", "feature_keys",
    "m_trades", "m_success", "m_win_pct", "m_avg_return_pct",
    "m_net_return_pct", "m_take_profit", "m_stop_loss", "m_profit_factor", "m_returns",
    "m1_trades", "m1_success", "m1_win_pct", "m1_avg_return_pct",
    "m2_trades", "m2_success", "m2_win_pct", "m2_avg_return_pct",
    "m3_trades", "m3_success", "m3_win_pct", "m3_avg_return_pct",
    "v_trades", "v_success", "v_win_pct", "v_avg_return_pct",
    "v_profit_factor", "v_returns",
    "o_trades", "o_success", "o_win_pct", "o_avg_return_pct",
    "o_profit_factor", "o_returns",
    "first_trade_date", "last_trade_date",
]


def _templates_swing() -> list[Template]:
    """Templates da trilha swing (alvos grandes). Identico ao R9."""
    templates: list[Template] = []
    target_stop_pairs = [
        (4.0, 3.0),
        (4.0, 4.0),
        (6.0, 3.0),
        (6.0, 4.0),
        (8.0, 4.0),
        (8.0, 5.0),
        (10.0, 5.0),
    ]
    caps = [5, 10, 15, 20]
    entries = ["open", "close"]
    for entry in entries:
        for target, stop in target_stop_pairs:
            for cap in caps:
                tcode = str(target).replace(".", "_")
                scode = str(stop).replace(".", "_")
                templates.append(
                    Template(
                        code=f"{entry}_tp{tcode}_sl{scode}_cap{cap}",
                        label=f"{entry} -> alvo {target:g}% / stop {stop:g}% em ate {cap} pregoes",
                        entry_rule=entry,
                        exit_mode="target_stop_swing",
                        target_pct=target,
                        stop_pct=stop,
                        holding_days=cap,
                        success_return_pct=target,
                        strategy_prefix="r10swing",
                    )
                )
    return templates


def _mine_one_ticker(
    ticker: str,
    bars,
    templates: list[Template],
    *,
    ticker_mining_start: date,
    oos_end: date,
    mw1_end: date,
    mw2_end: date,
    mining_end: date,
    val_end: date,
    max_factors: int,
    max_promoted_2f: int,
    max_accumulators: int,
    gate_a_min_trades: int,
    gate_a_min_pf: float,
    gate_a_min_win: float,
) -> list[dict[str, str]]:
    """Minera 1 ticker e devolve linhas que passaram o Gate A.

    ticker_mining_start: pode ser diferente do mining_start global se o ticker
    so existir a partir de uma data posterior (IPO recente).
    """
    samples_by_template = _prepare_ticker_samples(
        ticker, bars, templates, ticker_mining_start, oos_end
    )
    out: list[dict[str, str]] = []
    for template in templates:
        samples = samples_by_template.get(template.code, [])
        if not samples:
            continue

        # 2f
        acc2 = _accumulate_r10(
            samples,
            lambda items: combinations(items, 2),
            max_accumulators,
            mw1_end,
            mw2_end,
            mining_end,
            val_end,
        )
        if max_factors <= 2:
            for combo, acc in acc2.items():
                if _gate_a_pass(
                    acc,
                    min_trades=gate_a_min_trades,
                    min_profit_factor=gate_a_min_pf,
                    min_success_rate=gate_a_min_win,
                ):
                    out.append(_row_for_acc(ticker, template, combo, acc))
            continue

        promoted2 = _promote_mining(
            acc2,
            min_trades=max(8, gate_a_min_trades // 3),
            min_success_rate=max(60.0, gate_a_min_win - 15.0),
            min_profit_factor=1.5,
            limit=max_promoted_2f,
        )

        def combos3(items):
            item_set = set(items)
            for pair in promoted2:
                if pair[0] in item_set and pair[1] in item_set:
                    last_key = pair[-1][0]
                    for item in items:
                        if item not in pair and item[0] > last_key:
                            yield tuple(sorted((*pair, item), key=lambda x: x[0]))

        acc3 = _accumulate_r10(
            samples, combos3, max_accumulators, mw1_end, mw2_end, mining_end, val_end
        )
        for combo, acc in acc2.items():
            if _gate_a_pass(
                acc,
                min_trades=gate_a_min_trades,
                min_profit_factor=gate_a_min_pf,
                min_success_rate=gate_a_min_win,
            ):
                out.append(_row_for_acc(ticker, template, combo, acc))
        for combo, acc in acc3.items():
            if _gate_a_pass(
                acc,
                min_trades=gate_a_min_trades,
                min_profit_factor=gate_a_min_pf,
                min_success_rate=gate_a_min_win,
            ):
                out.append(_row_for_acc(ticker, template, combo, acc))
    return out


def _write_csv(path: Path, rows: list[dict[str, str]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default="b3_history.db")
    parser.add_argument("--tickers-file", default="lista.md")
    parser.add_argument("--output-csv", required=True)
    parser.add_argument("--mining-start", default="2018-01-01",
                        help="Inicio do mining global. Tickers que comecaram depois usam sua propria data.")
    parser.add_argument("--mw1-end", default="2020-12-31",
                        help="Fim da sub-janela 1 (walk-forward).")
    parser.add_argument("--mw2-end", default="2022-12-31",
                        help="Fim da sub-janela 2 (walk-forward).")
    parser.add_argument("--mining-end", default="2024-12-31",
                        help="Fim do mining (= fim da sub-janela 3).")
    parser.add_argument("--val-end", default="2025-12-31")
    parser.add_argument("--oos-end", default="2026-05-22")
    parser.add_argument("--limit-tickers", type=int)
    parser.add_argument("--max-factors", type=int, default=3)
    parser.add_argument("--max-promoted-2f", type=int, default=1500)
    parser.add_argument("--max-accumulators", type=int, default=30_000_000)
    parser.add_argument("--gate-a-min-trades", type=int, default=30)
    parser.add_argument("--gate-a-min-pf", type=float, default=3.0)
    parser.add_argument("--gate-a-min-win", type=float, default=75.0)
    parser.add_argument("--min-mining-bars", type=int, default=220,
                        help="Minimo de bars dentro do periodo mining para o ticker ser elegivel.")
    parser.add_argument("--progress-every-tickers", type=int, default=1)
    args = parser.parse_args()

    started = monotonic()
    tickers_raw = {
        t.removesuffix(".SA").upper()
        for t in load_tickers(args.tickers_file, limit=args.limit_tickers)
    }
    spot_bars = _load_price_bars(args.db_path, tickers_raw)
    mining_start = date.fromisoformat(args.mining_start)
    mw1_end = date.fromisoformat(args.mw1_end)
    mw2_end = date.fromisoformat(args.mw2_end)
    mining_end = date.fromisoformat(args.mining_end)
    val_end = date.fromisoformat(args.val_end)
    oos_end = date.fromisoformat(args.oos_end)

    # Filtro de elegibilidade: ticker tem que ter >= min_mining_bars dentro do periodo mining
    eligible = []
    excluded = []
    for ticker, bars in spot_bars.items():
        mining_bars = [b for b in bars if mining_start <= date.fromisoformat(b.trade_date) <= mining_end]
        if len(mining_bars) < args.min_mining_bars:
            excluded.append((ticker, len(mining_bars), len(bars)))
            continue
        # ticker_mining_start: usa o que cada ticker tem disponivel
        first_in_mining = date.fromisoformat(mining_bars[0].trade_date)
        ticker_mining_start = max(mining_start, first_in_mining)
        eligible.append((ticker, bars, ticker_mining_start, len(mining_bars)))

    templates = _templates_swing()

    print(
        f"R10-swing | tickers_eligiveis={len(eligible)} (excluidos={len(excluded)}) | "
        f"templates={len(templates)} | mining={mining_start}..{mining_end} "
        f"(walk-forward: m1<={mw1_end}, m2<={mw2_end}, m3<={mining_end}) | "
        f"val<={val_end} | oos<={oos_end} | max_factors={args.max_factors}",
        flush=True,
    )
    if excluded:
        print(f"  Excluidos (< {args.min_mining_bars} bars no mining):")
        for t, mb, tb in excluded[:20]:
            print(f"    {t}: {mb} bars no mining (de {tb} totais)")

    all_rows: list[dict[str, str]] = []
    for ticker_idx, (ticker, bars, t_start, mining_bars_count) in enumerate(eligible, start=1):
        ticker_started = monotonic()
        rows = _mine_one_ticker(
            ticker,
            bars,
            templates,
            ticker_mining_start=t_start,
            oos_end=oos_end,
            mw1_end=mw1_end,
            mw2_end=mw2_end,
            mining_end=mining_end,
            val_end=val_end,
            max_factors=args.max_factors,
            max_promoted_2f=args.max_promoted_2f,
            max_accumulators=args.max_accumulators,
            gate_a_min_trades=args.gate_a_min_trades,
            gate_a_min_pf=args.gate_a_min_pf,
            gate_a_min_win=args.gate_a_min_win,
        )
        all_rows.extend(rows)
        if ticker_idx % args.progress_every_tickers == 0:
            elapsed = monotonic() - started
            print(
                f"[R10-swing] {ticker_idx}/{len(eligible)} "
                f"({ticker_idx / len(eligible) * 100:.1f}%) | "
                f"{elapsed / 60:.1f} min | {ticker} (start={t_start}, mining_bars={mining_bars_count}) | "
                f"gate_a_rows={len(rows)} | total={len(all_rows)} | "
                f"ticker_time={(monotonic() - ticker_started):.1f}s",
                flush=True,
            )

    _write_csv(Path(args.output_csv), all_rows, MINING_FIELDS)
    print(
        f"R10-swing DONE | gate_a_total={len(all_rows)} | "
        f"elapsed={(monotonic() - started) / 60:.1f} min | csv={args.output_csv}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
