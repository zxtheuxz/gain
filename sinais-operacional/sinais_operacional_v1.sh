#!/usr/bin/env bash
# Pipeline diario de sinais operacionais (swing R10 + daytrade R11; fallback
# Books v1 enquanto os finais R10/R11 nao estiverem em reports/).
#
# Roda via cron seg-sab 09:00 BRT (12:00 UTC):
#   0 12 * * 1-6 /home/codeuser/gain/sinais-operacional/sinais_operacional_v1.sh >> /home/codeuser/gain/reports/cron-sinais.log 2>&1
#
# Passos: sync Yahoo -> AS_OF (ultimo pregao) -> gera sinais -> atualiza ledger
#         -> envia Telegram.
#
# Flags de ambiente:
#   SINAIS_SKIP_SYNC=1      pula o sync (usa o DB como esta)
#   SINAIS_SKIP_TELEGRAM=1  pula o envio Telegram
#   PYTHON_BIN=...          interpretador (default .venv/bin/python, senao python3)
#   SYNC_WINDOW_DAYS=...    janela do sync (default 2200, preserva ~8 anos)
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"
cd "$REPO_ROOT"

if [ -n "${PYTHON_BIN:-}" ]; then
  PY="$PYTHON_BIN"
elif [ -x ".venv/bin/python" ]; then
  PY=".venv/bin/python"
else
  PY="python3"
fi

TICKERS_FILE="${TICKERS_FILE:-lista.md}"
SYNC_WINDOW_DAYS="${SYNC_WINDOW_DAYS:-2200}"
STAMP="$(date -u '+%Y-%m-%d %H:%M:%SZ')"
echo "==================== $STAMP ===================="
echo "[sinais-operacional] python=$PY tickers=$TICKERS_FILE"

# [1/4] sync Yahoo (janela grande para NAO truncar o historico de 8 anos)
if [ "${SINAIS_SKIP_SYNC:-0}" = "1" ]; then
  echo "[1/4] sync PULADO (SINAIS_SKIP_SYNC=1)"
else
  echo "[1/4] sync Yahoo (--window-days $SYNC_WINDOW_DAYS) ..."
  "$PY" -m b3_patterns sync --tickers-file "$TICKERS_FILE" --window-days "$SYNC_WINDOW_DAYS" \
    || echo "[1/4] AVISO: sync falhou; seguindo com o DB atual."
fi

# [2/4] descobre o ultimo pregao COMPLETO (ignora barra provisoria do dia
# corrente: o cron roda pre-mercado, mas se o Yahoo devolver um close
# intraday de hoje, nao queremos operar em cima dele).
AS_OF="$("$PY" - <<'PY'
import sqlite3
from datetime import date
today = date.today().isoformat()
row = sqlite3.connect("b3_history.db").execute(
    "SELECT MAX(trade_date) FROM price_history WHERE trade_date < ?", (today,)
).fetchone()
print(row[0] or "")
PY
)"
if [ -z "$AS_OF" ]; then
  echo "[2/4] ERRO: b3_history.db sem dados (MAX(trade_date) vazio)."
  exit 1
fi
echo "[2/4] AS_OF = $AS_OF"

# [3/4] gera sinais e atualiza o ledger
echo "[3/4] gerando sinais ..."
"$PY" "$HERE/generate_operacional_signals.py" --as-of-date "$AS_OF" || { echo "[3/4] ERRO ao gerar sinais."; exit 1; }
echo "[3/4] atualizando ledger ..."
"$PY" "$HERE/track_operacional.py" --as-of-date "$AS_OF" || { echo "[3/4] ERRO ao atualizar ledger."; exit 1; }

# [4/4] envia Telegram (secrets: sinais-operacional/telegram-secrets.json ou .claude/)
echo "[4/4] Telegram ..."
if [ "${SINAIS_SKIP_TELEGRAM:-0}" = "1" ]; then
  echo "  PULADO (SINAIS_SKIP_TELEGRAM=1). Previa:"
  "$PY" "$HERE/telegram_send_operacional.py" "$AS_OF" --dry-run
elif [ -f "$HERE/telegram-secrets.json" ] || [ -f ".claude/telegram-secrets.json" ]; then
  echo "  enviando ..."
  "$PY" "$HERE/telegram_send_operacional.py" "$AS_OF" || echo "  AVISO: envio Telegram falhou."
else
  echo "  PULADO (secrets ausente). Previa:"
  "$PY" "$HERE/telegram_send_operacional.py" "$AS_OF" --dry-run
fi

echo "[sinais-operacional] concluido para AS_OF=$AS_OF"
