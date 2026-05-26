"""Ledger + acompanhamento de sinais operacionais (swing + daytrade).

Mantem `reports/operacional-ledger.csv`: registra cada sinal emitido e, a
cada execucao, resolve as posicoes cuja saida ja pode ser computada com as
barras disponiveis no DB.

Modelo de entrada (fiel ao uso ao vivo):
- O sinal e detectado no pregao-base (`signal_date`, ultimo close avaliado).
- A ENTRADA acontece no PROXIMO pregao disponivel (robusto a feriado: a
  primeira barra com data > signal_date). entry_rule=open -> entra no open;
  entry_rule=close -> entra no close.
- A saida espelha `_simulate_percent_exit` de asset_discovery_round1:
  janela [entry_idx (ou +1 se entrada no close) .. entry_idx + cap_days],
  long: target=entry*(1+tp%), stop=entry*(1-sl%); em barra ambigua o STOP
  vence (conservador); senao sai no close do ultimo dia (time_cap).

Uso:
  python3 sinais-operacional/track_operacional.py [--as-of-date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from b3_patterns.tickers import load_tickers  # noqa: E402
from generate_operacional_signals import _load_price_bars  # noqa: E402

LEDGER_PATH = REPO_ROOT / "reports" / "operacional-ledger.csv"

LEDGER_COLUMNS = [
    "signal_id",
    "book",
    "ticker",
    "signal_date",
    "entry_date",
    "entry_rule",
    "trade_direction",
    "target_pct",
    "stop_pct",
    "cap_days",
    "strategy_code",
    "status",          # pending | open | closed
    "entry_price",
    "exit_date",
    "exit_price",
    "pnl_pct",
    "exit_reason",     # take_profit | stop_loss | stop_loss_conflict | time_cap
    "days_held",
    "last_price",
    "last_update",
]


def _signal_id(book: str, ticker: str, signal_date: str, strategy_code: str) -> str:
    return f"{book}|{ticker}|{signal_date}|{strategy_code}"


def _load_ledger() -> dict[str, dict[str, str]]:
    if not LEDGER_PATH.exists():
        return {}
    with LEDGER_PATH.open(encoding="utf-8", newline="") as f:
        return {row["signal_id"]: row for row in csv.DictReader(f)}


def _save_ledger(ledger: dict[str, dict[str, str]]) -> None:
    LEDGER_PATH.parent.mkdir(parents=True, exist_ok=True)
    rows = sorted(ledger.values(), key=lambda r: (r["signal_date"], r["book"], r["ticker"]))
    with LEDGER_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=LEDGER_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in LEDGER_COLUMNS})


def _ingest_signals(ledger: dict[str, dict[str, str]], signals_csv: Path) -> int:
    """Adiciona sinais novos como `pending` (idempotente por signal_id)."""
    added = 0
    with signals_csv.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            sid = _signal_id(row["book"], row["ticker"], row["trade_date"], row["strategy_code"])
            if sid in ledger:
                continue
            ledger[sid] = {
                "signal_id": sid,
                "book": row["book"],
                "ticker": row["ticker"],
                "signal_date": row["trade_date"],
                "entry_date": "",
                "entry_rule": row.get("entry_rule", "open"),
                "trade_direction": row.get("trade_direction", "long"),
                "target_pct": row["take_profit_pct"],
                "stop_pct": row["stop_loss_pct"],
                "cap_days": str(row["time_cap_days"]),
                "strategy_code": row["strategy_code"],
                "status": "pending",
                "entry_price": "",
                "exit_date": "",
                "exit_price": "",
                "pnl_pct": "",
                "exit_reason": "",
                "days_held": "",
                "last_price": "",
                "last_update": "",
            }
            added += 1
    return added


def _close(row: dict[str, str], exit_date: str, exit_price: float, entry_price: float,
           direction: str, reason: str) -> None:
    if direction == "long":
        pnl = (exit_price / entry_price - 1.0) * 100.0
    else:
        pnl = (entry_price / exit_price - 1.0) * 100.0
    row["status"] = "closed"
    row["exit_date"] = exit_date
    row["exit_price"] = f"{exit_price:.4f}"
    row["pnl_pct"] = f"{pnl:.4f}"
    row["exit_reason"] = reason


def _resolve(row: dict[str, str], bars: list[tuple[str, float, float, float, float, int]]) -> None:
    """Atualiza uma linha do ledger (pending/open) com as barras disponiveis."""
    signal_date = row["signal_date"]
    entry_idx = next((i for i, b in enumerate(bars) if b[0] > signal_date), None)
    if entry_idx is None:
        return  # proximo pregao ainda nao ocorreu

    entry_rule = row["entry_rule"]
    direction = row.get("trade_direction", "long")
    entry_price = bars[entry_idx][1] if entry_rule == "open" else bars[entry_idx][4]
    if entry_price <= 0:
        return

    cap_days = int(float(row["cap_days"]))
    tp = float(row["target_pct"])
    sl = float(row["stop_pct"])
    if direction == "long":
        target_price = entry_price * (1.0 + tp / 100.0)
        stop_price = entry_price * (1.0 - sl / 100.0)
    else:
        target_price = entry_price * (1.0 - tp / 100.0)
        stop_price = entry_price * (1.0 + sl / 100.0)

    last_idx = len(bars) - 1
    start_idx = entry_idx if entry_rule == "open" else entry_idx + 1
    end_idx = entry_idx + cap_days
    eval_end = min(end_idx, last_idx)

    row["entry_date"] = bars[entry_idx][0]
    row["entry_price"] = f"{entry_price:.4f}"
    row["days_held"] = str(max(0, last_idx - entry_idx + 1))
    row["last_price"] = f"{bars[last_idx][4]:.4f}"
    row["status"] = "open"

    for bar_idx in range(start_idx, eval_end + 1):
        _, _o, high, low, _close_px, _v = bars[bar_idx]
        if direction == "long":
            target_hit = high >= target_price
            stop_hit = low <= stop_price
        else:
            target_hit = low <= target_price
            stop_hit = high >= stop_price
        if target_hit and stop_hit:
            _close(row, bars[bar_idx][0], stop_price, entry_price, direction, "stop_loss_conflict")
            return
        if stop_hit:
            _close(row, bars[bar_idx][0], stop_price, entry_price, direction, "stop_loss")
            return
        if target_hit:
            _close(row, bars[bar_idx][0], target_price, entry_price, direction, "take_profit")
            return

    if end_idx <= last_idx:
        # cap inteiro decorrido sem alvo/stop -> sai no close do ultimo dia permitido
        _close(row, bars[end_idx][0], bars[end_idx][4], entry_price, direction, "time_cap")
    # senao: ainda em andamento (status=open)


def main() -> int:
    parser = argparse.ArgumentParser(description="Atualiza o ledger operacional (swing+daytrade).")
    parser.add_argument("--db-path", default="b3_history.db")
    parser.add_argument("--tickers-file", default="lista.md")
    parser.add_argument("--as-of-date", help="Data dos sinais a ingerir (YYYY-MM-DD). Default: mais recente.")
    parser.add_argument("--signals-csv", default=None,
                        help="CSV de sinais. Default: reports/operacional-signals-<DATE>.csv.")
    args = parser.parse_args()

    db_path = Path(args.db_path).resolve()
    tickers_file = Path(args.tickers_file).resolve()

    if args.signals_csv:
        signals_csv = Path(args.signals_csv).resolve()
    elif args.as_of_date:
        signals_csv = REPO_ROOT / "reports" / f"operacional-signals-{args.as_of_date}.csv"
    else:
        candidates = sorted((REPO_ROOT / "reports").glob("operacional-signals-*.csv"))
        if not candidates:
            sys.exit("ERRO: nenhum CSV operacional-signals-*.csv em reports/.")
        signals_csv = candidates[-1]

    ledger = _load_ledger()
    added = 0
    if signals_csv.exists():
        added = _ingest_signals(ledger, signals_csv)
    else:
        print(f"AVISO: {signals_csv} nao existe; apenas atualizando posicoes.", file=sys.stderr)

    allowed_tickers = {t.removesuffix(".SA").upper() for t in load_tickers(tickers_file)}
    price_bars = _load_price_bars(db_path, allowed_tickers)

    resolved_closed = 0
    still_open = 0
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    for row in ledger.values():
        if row["status"] == "closed":
            continue
        bars = price_bars.get(row["ticker"].upper())
        if not bars:
            continue
        _resolve(row, bars)
        row["last_update"] = now_iso
        if row["status"] == "closed":
            resolved_closed += 1
        elif row["status"] == "open":
            still_open += 1

    _save_ledger(ledger)

    total = len(ledger)
    pending = sum(1 for r in ledger.values() if r["status"] == "pending")
    open_n = sum(1 for r in ledger.values() if r["status"] == "open")
    closed_n = sum(1 for r in ledger.values() if r["status"] == "closed")
    print(f"Ledger: {total} sinais (novos: {added}) | pending: {pending}, open: {open_n}, closed: {closed_n}")
    print(f"  Resolvidos nesta execucao -> fechados: {resolved_closed}, em andamento: {still_open}")
    print(f"CSV: {LEDGER_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
