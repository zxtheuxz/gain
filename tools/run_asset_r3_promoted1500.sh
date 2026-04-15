#!/usr/bin/env bash
set -euo pipefail

cd /home/gain/gain
mkdir -p reports

.venv/bin/python -m b3_patterns asset-discover-r3 \
  --db-path b3_history.db \
  --tickers-file lista.md \
  --start-date "${R3_START_DATE:-2025-01-01}" \
  --end-date "${R3_END_DATE:-2026-04-13}" \
  --entry-rules open close \
  --trade-directions long short \
  --target-stop-pairs 3:3 3:4 4:4 \
  --atr-target-stop-pairs 1.5:1 2:1.5 \
  --atr-time-cap-days 5 10 \
  --time-cap-days 10 \
  --max-promoted-pairs "${R3_MAX_PROMOTED_PAIRS:-1500}" \
  --max-accumulators "${R3_MAX_ACCUMULATORS:-12000000}" \
  --prune-every-tickers 50 \
  --progress-every-tickers 10 \
  --include-known \
  2>&1 | tee "reports/r3-full-${R3_START_DATE:-2025-01-01}-${R3_END_DATE:-2026-04-13}-promoted${R3_MAX_PROMOTED_PAIRS:-1500}.log"
