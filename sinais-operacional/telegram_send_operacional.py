"""Envia o resumo operacional diario (swing + daytrade) para o Telegram.

Le:
  - reports/operacional-signals-<DATE>.csv  -> sinais de HOJE (SWING/DAYTRADE)
  - reports/operacional-ledger.csv          -> RESULTADO DT (saidas no ultimo
                                               pregao) e SWING EM ANDAMENTO.

Secrets (bot_token + chat_id), procurados nesta ordem:
  1. sinais-operacional/telegram-secrets.json  (local da pasta, portavel)
  2. .claude/telegram-secrets.json             (compatibilidade)

Uso:
  python3 sinais-operacional/telegram_send_operacional.py [YYYY-MM-DD] [--dry-run]
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
SECRETS_CANDIDATES = [
    HERE / "telegram-secrets.json",
    REPO_ROOT / ".claude" / "telegram-secrets.json",
]
REPORTS = REPO_ROOT / "reports"
LEDGER_PATH = REPORTS / "operacional-ledger.csv"


def _load_secrets() -> dict:
    path = next((p for p in SECRETS_CANDIDATES if p.exists()), None)
    if path is None:
        locais = " ou ".join(str(p) for p in SECRETS_CANDIDATES)
        sys.exit(f"ERRO: secrets nao encontrado ({locais}). Configure bot_token + chat_id.")
    secrets = json.loads(path.read_text())
    if not secrets.get("bot_token") or not secrets.get("chat_id"):
        sys.exit("ERRO: bot_token e chat_id sao obrigatorios em telegram-secrets.json.")
    return secrets


def _resolve_signals_csv(as_of_date: str | None) -> Path | None:
    if as_of_date:
        path = REPORTS / f"operacional-signals-{as_of_date}.csv"
        return path if path.exists() else None
    candidates = sorted(REPORTS.glob("operacional-signals-*.csv"))
    return candidates[-1] if candidates else None


def _next_pregao(trade_date_str: str) -> str:
    current = date.fromisoformat(trade_date_str)
    nxt = current + timedelta(days=1)
    while nxt.weekday() >= 5:
        nxt += timedelta(days=1)
    return nxt.isoformat()


def _fmt_pf(raw) -> str:
    try:
        v = float(raw)
        return "INF" if v > 999 else f"{v:.1f}"
    except (TypeError, ValueError):
        return str(raw)[:4] or "-"


def _best_per_ticker(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    by_ticker: dict[str, dict[str, str]] = {}
    for r in rows:
        cur = by_ticker.get(r["ticker"])
        if cur is None or _pf_val(r) > _pf_val(cur):
            by_ticker[r["ticker"]] = r
    return [by_ticker[t] for t in sorted(by_ticker)]


def _pf_val(row: dict[str, str]) -> float:
    try:
        return float(row.get("profit_factor") or 0)
    except (TypeError, ValueError):
        return 9999.0


def _signal_table(rows: list[dict[str, str]]) -> list[str]:
    lines = ["```", f"{'Ticker':<7}{'Alvo':<7}{'Stop':<7}{'Dias':<5}{'PF':<6}{'Taxa':<6}"]
    for r in _best_per_ticker(rows):
        alvo = f"+{float(r['take_profit_pct']):.1f}%"
        stop = f"-{float(r['stop_loss_pct']):.1f}%"
        dias = f"{int(float(r['time_cap_days']))}d"
        pf = _fmt_pf(r.get("profit_factor"))
        try:
            taxa = f"{float(r['success_rate_pct']):.0f}%"
        except (TypeError, ValueError):
            taxa = "-"
        lines.append(f"{r['ticker']:<7}{alvo:<7}{stop:<7}{dias:<5}{pf:<6}{taxa:<6}")
    lines.append("```")
    return lines


def _outcome_label(reason: str) -> str:
    return {
        "take_profit": "TARGET",
        "stop_loss": "STOP",
        "stop_loss_conflict": "STOP",
        "time_cap": "CLOSE",
    }.get(reason, reason or "-")


def _outcome_icon(reason: str) -> str:
    if reason == "take_profit":
        return "TARGET"
    if reason in ("stop_loss", "stop_loss_conflict"):
        return "STOP"
    return "CLOSE"


def _load_ledger_rows() -> list[dict[str, str]]:
    if not LEDGER_PATH.exists():
        return []
    with LEDGER_PATH.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _format_message(signals_csv: Path | None, base_date: str) -> str:
    signal_rows: list[dict[str, str]] = []
    if signals_csv and signals_csv.exists():
        signal_rows = list(csv.DictReader(signals_csv.open()))

    swing = [r for r in signal_rows if r["book"] == "swing"]
    daytrade = [r for r in signal_rows if r["book"] == "daytrade"]
    qualified = len({r["ticker"] for r in signal_rows})
    next_pregao = _next_pregao(base_date)

    lines = [
        f"🤖 *Sinais para {next_pregao}*",
        f"_Base: fechamento {base_date} | {qualified} tickers qualificados_",
        "",
    ]

    if swing:
        lines.append(f"🕒 *SWING* ({len({r['ticker'] for r in swing})} tickers)")
        lines += _signal_table(swing)
        lines.append("• Entrada: open | Sai no close do ultimo dia permitido")
        lines.append("• Stop por OCO na entrada efetiva | Skip se gap up > +2%")
        lines.append("")

    if daytrade:
        lines.append(f"⚡ *DAYTRADE* ({len({r['ticker'] for r in daytrade})} tickers)")
        lines += _signal_table(daytrade)
        lines.append("• Entrada: open | Alvo/stop no dia (cap curto)")
        lines.append("")

    if not swing and not daytrade:
        lines.append("_Sem novos sinais para o proximo pregao._")
        lines.append("")

    # RESULTADO DT: daytrades do pregao de SAIDA mais recente disponivel (<= base)
    ledger = _load_ledger_rows()
    dt_all = [
        r for r in ledger
        if r["book"] == "daytrade" and r["status"] == "closed"
        and r.get("exit_date") and r["exit_date"] <= base_date
    ]
    dt_date = max((r["exit_date"] for r in dt_all), default="")
    dt_closed = [r for r in dt_all if r["exit_date"] == dt_date]
    if dt_closed:
        lines.append(f"📊 *RESULTADO DT {dt_date}*")
        lines.append("```")
        lines.append(f"{'Ticker':<7}{'Entry':<8}{'PnL':<8}{'Resultado':<10}")
        for r in sorted(dt_closed, key=lambda x: x["ticker"]):
            try:
                entry = f"{float(r['entry_price']):.2f}"
            except (TypeError, ValueError):
                entry = "-"
            try:
                pnl = f"{float(r['pnl_pct']):+.1f}%"
            except (TypeError, ValueError):
                pnl = "-"
            lines.append(f"{r['ticker']:<7}{entry:<8}{pnl:<8}{_outcome_icon(r['exit_reason']):<10}")
        lines.append("```")
        lines.append("")

    # SWING EM ANDAMENTO: swings ainda abertos
    swing_open = [r for r in ledger if r["book"] == "swing" and r["status"] == "open"]
    if swing_open:
        lines.append(f"🟡 *SWING EM ANDAMENTO* ({len(swing_open)})")
        lines.append("```")
        lines.append(f"{'Ticker':<7}{'D/Cap':<7}{'PnL':<8}{'Last':<8}")
        for r in sorted(swing_open, key=lambda x: (x["ticker"], x["entry_date"])):
            dcap = f"{r.get('days_held','?')}/{int(float(r['cap_days']))}"
            try:
                entry = float(r["entry_price"])
                last = float(r["last_price"])
                pnl = f"{(last/entry-1)*100:+.1f}%"
                last_s = f"{last:.2f}"
            except (TypeError, ValueError, ZeroDivisionError):
                pnl = "-"
                last_s = "-"
            lines.append(f"{r['ticker']:<7}{dcap:<7}{pnl:<8}{last_s:<8}")
        lines.append("```")

    return "\n".join(lines).rstrip()


def _send(token: str, chat_id, text: str) -> dict:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": "true",
    }).encode("utf-8")
    request = urllib.request.Request(url, data=payload, method="POST")
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Envia resumo operacional (swing+daytrade) ao Telegram.")
    parser.add_argument("as_of_date", nargs="?", help="Data base YYYY-MM-DD (default: CSV mais recente).")
    parser.add_argument("--dry-run", action="store_true", help="Imprime a mensagem sem enviar.")
    args = parser.parse_args()

    signals_csv = _resolve_signals_csv(args.as_of_date)
    if args.as_of_date:
        base_date = args.as_of_date
    elif signals_csv:
        base_date = signals_csv.stem.replace("operacional-signals-", "")
    else:
        sys.exit("ERRO: informe a data base ou gere um CSV operacional-signals-*.csv antes.")

    message = _format_message(signals_csv, base_date)

    if args.dry_run:
        print(message)
        return 0

    secrets = _load_secrets()
    response = _send(secrets["bot_token"], secrets["chat_id"], message)
    if response.get("ok"):
        print(f"OK: enviado (message_id={response['result']['message_id']}).")
        return 0
    print(f"FALHOU: {response}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
