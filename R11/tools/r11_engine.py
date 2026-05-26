"""R11 engine: discovery daytrade com 3 familias + walk-forward + bootstrap.

Delta sobre R10: substitui templates swing por 3 familias daytrade
(intraday tp/sl, mini-swing tp/sl, fixed exit alvo livre).

Reusa toda metodologia R10: AccR10 (6 buckets m1/m2/m3/m/v/o),
walk-forward por sub-janela, bootstrap a posteriori, janela mining por
ticker.
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import date
from itertools import combinations
from pathlib import Path
from time import monotonic

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from b3_patterns.tickers import load_tickers  # noqa: E402
from R7.tools.run_r7a_asset_specific import (  # noqa: E402
    Template,
    _combo_code,
    _combo_label,
    _pf,
    _prepare_ticker_samples,
    _rate,
)
from R10.tools.r10_engine import (  # noqa: E402
    AccR10,
    MINING_FIELDS,
    _accumulate_r10,
    _fmt_float,
    _fmt_pct,
    _fmt_returns,
    _gate_a_pass,
    _load_price_bars,
    _promote_mining,
    _write_csv,
)


def _templates_intraday() -> list[Template]:
    """Familia 1: target/stop intraday (cap 0 ou 1)."""
    templates: list[Template] = []
    target_stop_pairs = [
        (1.0, 0.5),
        (1.0, 1.0),
        (1.0, 1.5),
        (1.0, 2.0),
        (1.5, 1.0),
        (1.5, 1.5),
        (1.5, 2.0),
        (2.0, 1.0),
        (2.0, 1.5),
        (2.0, 2.0),
    ]
    for target, stop in target_stop_pairs:
        tcode = str(target).replace(".", "_")
        scode = str(stop).replace(".", "_")
        # cap 0: mesmo dia
        templates.append(Template(
            code=f"open_tp{tcode}_sl{scode}_same_day",
            label=f"open -> alvo {target:g}% / stop {stop:g}% no mesmo dia",
            entry_rule="open",
            exit_mode="target_stop_same_day",
            target_pct=target,
            stop_pct=stop,
            holding_days=0,
            success_return_pct=target,
            strategy_prefix="r11day",
        ))
        # cap 1: D+1
        for entry in ("open", "close"):
            templates.append(Template(
                code=f"{entry}_tp{tcode}_sl{scode}_cap1",
                label=f"{entry} -> alvo {target:g}% / stop {stop:g}% em ate 1 pregao",
                entry_rule=entry,
                exit_mode="target_stop_swing",
                target_pct=target,
                stop_pct=stop,
                holding_days=1,
                success_return_pct=target,
                strategy_prefix="r11day",
            ))
    return templates


def _templates_minisswing() -> list[Template]:
    """Familia 2: mini-swing tp/sl em 2-3 pregoes."""
    templates: list[Template] = []
    target_stop_pairs = [
        (2.0, 1.0),
        (2.0, 1.5),
        (2.0, 2.0),
        (2.5, 1.5),
        (2.5, 2.0),
        (2.5, 2.5),
        (3.0, 1.5),
        (3.0, 2.0),
        (3.0, 2.5),
    ]
    caps = [2, 3]
    entries = ["open", "close"]
    for entry in entries:
        for target, stop in target_stop_pairs:
            for cap in caps:
                tcode = str(target).replace(".", "_")
                scode = str(stop).replace(".", "_")
                templates.append(Template(
                    code=f"{entry}_tp{tcode}_sl{scode}_cap{cap}",
                    label=f"{entry} -> alvo {target:g}% / stop {stop:g}% em ate {cap} pregoes",
                    entry_rule=entry,
                    exit_mode="target_stop_swing",
                    target_pct=target,
                    stop_pct=stop,
                    holding_days=cap,
                    success_return_pct=target,
                    strategy_prefix="r11mini",
                ))
    return templates


def _templates_fixed() -> list[Template]:
    """Familia 3: saida fixa, alvo livre (sucesso = retorno > 1%)."""
    templates: list[Template] = []
    fixed = [
        ("open_close_same_day", "open", "fixed_close", 0, "open -> close mesmo dia"),
        ("open_next_open", "open", "fixed_next_open", 1, "open -> open D+1"),
        ("open_next_close", "open", "fixed_next_close", 1, "open -> close D+1"),
        ("close_next_open", "close", "fixed_next_open", 1, "close -> open D+1"),
        ("close_next_close", "close", "fixed_next_close", 1, "close -> close D+1"),
    ]
    for code, entry, mode, holding, label in fixed:
        templates.append(Template(
            code=code,
            label=label,
            entry_rule=entry,
            exit_mode=mode,
            target_pct=0.0,
            stop_pct=0.0,
            holding_days=holding,
            success_return_pct=1.0,  # alvo livre acima de 1%
            strategy_prefix="r11fix",
        ))
    return templates


def _all_templates() -> list[Template]:
    """Agrega as 3 familias R11."""
    return _templates_intraday() + _templates_minisswing() + _templates_fixed()


def _row_for_acc_r11(
    ticker: str,
    template: Template,
    combo: tuple,
    acc: AccR10,
) -> dict[str, str]:
    """Como _row_for_acc do R10 mas com prefix r11_day_<familia>."""
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
        "strategy_code": f"r11_day_{template.strategy_prefix}_{ticker}_{template.code}__{len(combo)}f__{_combo_code(combo)}",
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
        "m_trades": str(acc.m_trades),
        "m_success": str(acc.m_success),
        "m_win_pct": _fmt_pct(m_win),
        "m_avg_return_pct": _fmt_pct(m_avg),
        "m_net_return_pct": _fmt_pct(acc.m_net_return),
        "m_take_profit": str(acc.m_take_profit),
        "m_stop_loss": str(acc.m_stop_loss),
        "m_profit_factor": _fmt_float(m_pf),
        "m_returns": _fmt_returns(acc.m_returns),
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
        "v_trades": str(acc.v_trades),
        "v_success": str(acc.v_success),
        "v_win_pct": _fmt_pct(v_win),
        "v_avg_return_pct": _fmt_pct(v_avg),
        "v_profit_factor": _fmt_float(v_pf),
        "v_returns": _fmt_returns(acc.v_returns),
        "o_trades": str(acc.o_trades),
        "o_success": str(acc.o_success),
        "o_win_pct": _fmt_pct(o_win),
        "o_avg_return_pct": _fmt_pct(o_avg),
        "o_profit_factor": _fmt_float(o_pf),
        "o_returns": _fmt_returns(acc.o_returns),
        "first_trade_date": acc.first_trade_date,
        "last_trade_date": acc.last_trade_date,
    }


def _mine_one_ticker_r11(
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
    """Minera 1 ticker. Espelha _mine_one_ticker do R10 mas usa _row_for_acc_r11."""
    samples_by_template = _prepare_ticker_samples(
        ticker, bars, templates, ticker_mining_start, oos_end
    )
    out: list[dict[str, str]] = []
    for template in templates:
        samples = samples_by_template.get(template.code, [])
        if not samples:
            continue

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
                    out.append(_row_for_acc_r11(ticker, template, combo, acc))
            continue

        promoted2 = _promote_mining(
            acc2,
            min_trades=max(8, gate_a_min_trades // 3),
            min_success_rate=max(50.0, gate_a_min_win - 15.0),
            min_profit_factor=1.3,
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
                out.append(_row_for_acc_r11(ticker, template, combo, acc))
        for combo, acc in acc3.items():
            if _gate_a_pass(
                acc,
                min_trades=gate_a_min_trades,
                min_profit_factor=gate_a_min_pf,
                min_success_rate=gate_a_min_win,
            ):
                out.append(_row_for_acc_r11(ticker, template, combo, acc))
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default="b3_history.db")
    parser.add_argument("--tickers-file", default="lista.md")
    parser.add_argument("--output-csv", required=True)
    parser.add_argument("--mining-start", default="2018-01-01")
    parser.add_argument("--mw1-end", default="2020-12-31")
    parser.add_argument("--mw2-end", default="2022-12-31")
    parser.add_argument("--mining-end", default="2024-12-31")
    parser.add_argument("--val-end", default="2025-12-31")
    parser.add_argument("--oos-end", default="2026-05-22")
    parser.add_argument("--limit-tickers", type=int)
    parser.add_argument("--max-factors", type=int, default=3)
    parser.add_argument("--max-promoted-2f", type=int, default=1500)
    parser.add_argument("--max-accumulators", type=int, default=30_000_000)
    parser.add_argument("--gate-a-min-trades", type=int, default=30)
    parser.add_argument("--gate-a-min-pf", type=float, default=1.8)
    parser.add_argument("--gate-a-min-win", type=float, default=60.0)
    parser.add_argument("--min-mining-bars", type=int, default=220)
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

    eligible = []
    excluded = []
    for ticker, bars in spot_bars.items():
        mining_bars = [b for b in bars if mining_start <= date.fromisoformat(b.trade_date) <= mining_end]
        if len(mining_bars) < args.min_mining_bars:
            excluded.append((ticker, len(mining_bars), len(bars)))
            continue
        first_in_mining = date.fromisoformat(mining_bars[0].trade_date)
        ticker_mining_start = max(mining_start, first_in_mining)
        eligible.append((ticker, bars, ticker_mining_start, len(mining_bars)))

    templates = _all_templates()
    # contar templates por familia
    fam_intraday = sum(1 for t in templates if t.strategy_prefix == "r11day")
    fam_mini = sum(1 for t in templates if t.strategy_prefix == "r11mini")
    fam_fixed = sum(1 for t in templates if t.strategy_prefix == "r11fix")

    print(
        f"R11-daytrade | tickers_eligiveis={len(eligible)} (excluidos={len(excluded)}) | "
        f"templates={len(templates)} (intraday={fam_intraday}, mini={fam_mini}, fixed={fam_fixed}) | "
        f"mining={mining_start}..{mining_end} (walk-forward: m1<={mw1_end}, m2<={mw2_end}, m3<={mining_end}) | "
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
        rows = _mine_one_ticker_r11(
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
                f"[R11-day] {ticker_idx}/{len(eligible)} "
                f"({ticker_idx / len(eligible) * 100:.1f}%) | "
                f"{elapsed / 60:.1f} min | {ticker} (start={t_start}, mining_bars={mining_bars_count}) | "
                f"gate_a_rows={len(rows)} | total={len(all_rows)} | "
                f"ticker_time={(monotonic() - ticker_started):.1f}s",
                flush=True,
            )

    _write_csv(Path(args.output_csv), all_rows, MINING_FIELDS)
    print(
        f"R11-day DONE | gate_a_total={len(all_rows)} | "
        f"elapsed={(monotonic() - started) / 60:.1f} min | csv={args.output_csv}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
