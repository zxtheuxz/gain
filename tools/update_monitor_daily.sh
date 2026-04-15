#!/usr/bin/env bash
set -euo pipefail

cd /home/gain/gain

: "${B3_YEARS:=2025 2026}"
: "${ELITE_CSV:=reports/asset-discovery-lista-r3-stat-elite-top49.csv}"
: "${MONITOR_JSON:=monitor-web/public/data/asset-monitor.json}"

.venv/bin/python -m b3_patterns options-sync \
  --tickers-file lista.md \
  --years ${B3_YEARS}

.venv/bin/python -m b3_patterns asset-monitor-export \
  --db-path b3_history.db \
  --tickers-file lista.md \
  --strategies-csv "${ELITE_CSV}" \
  --top-strategies 0 \
  --output-json "${MONITOR_JSON}"

.venv/bin/python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path("monitor-web/public/data/asset-monitor.json").read_text())
print("generated_at", payload.get("generated_at"))
print("latest_trade_date", payload.get("latest_trade_date"))
print("strategies_monitored", payload.get("strategies_monitored"))
print("signals_triggered", payload.get("signals_triggered"))
print("operational_min_signals", payload.get("operational_min_signals"))
print("operational_tickers", payload.get("operational_tickers"))
print("signals_triggered_operational", payload.get("signals_triggered_operational"))
print("top_operational_tickers", [
    (item["ticker"], item["signals"])
    for item in payload.get("top_operational_tickers", [])
])
PY
