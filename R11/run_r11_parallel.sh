#!/usr/bin/env bash
# R11 paralelo: daytrade portfolio com 3 familias + walk-forward + bootstrap.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON="${PYTHON:-.venv/bin/python}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%d-%H%M%S)}"
OUT_DIR="${OUT_DIR:-reports/r11-${RUN_ID}}"
LOG_PATH="$OUT_DIR/r11.log"

# Janelas (mesmas do R10)
MINING_START="${MINING_START:-2018-01-01}"
MW1_END="${MW1_END:-2020-12-31}"
MW2_END="${MW2_END:-2022-12-31}"
MINING_END="${MINING_END:-2024-12-31}"
VAL_START="${VAL_START:-2025-01-01}"
VAL_END="${VAL_END:-2025-12-31}"
OOS_START="${OOS_START:-2026-01-01}"
OOS_END="${OOS_END:-2026-05-25}"

# Gates A (calibrados pra daytrade)
MIN_MINING_TRADES="${MIN_MINING_TRADES:-30}"
MIN_MINING_WIN="${MIN_MINING_WIN:-60}"
MIN_MINING_PF="${MIN_MINING_PF:-1.8}"
MIN_WILSON="${MIN_WILSON:-55}"
MAX_BINOM_M="${MAX_BINOM_M:-0.01}"

# Gate Walk-forward (calibrado)
WF_MIN_TRADES="${WF_MIN_TRADES:-3}"
WF_MIN_WIN="${WF_MIN_WIN:-60}"

# Gates B
MIN_VAL_TRADES="${MIN_VAL_TRADES:-10}"
MIN_VAL_WIN="${MIN_VAL_WIN:-60}"
MIN_VAL_AVG="${MIN_VAL_AVG:-0.5}"
MAX_BINOM_V="${MAX_BINOM_V:-0.05}"

# Gate C
MAX_FDR_Q="${MAX_FDR_Q:-0.05}"

# Gate Bootstrap
MIN_BOOTSTRAP_LOWER="${MIN_BOOTSTRAP_LOWER:-0.0}"

# Engine
MAX_FACTORS="${MAX_FACTORS:-3}"
MAX_PROMOTED_2F="${MAX_PROMOTED_2F:-1500}"
MAX_ACCUMULATORS="${MAX_ACCUMULATORS:-30000000}"
MIN_MINING_BARS="${MIN_MINING_BARS:-220}"
TICKERS_FILE="${TICKERS_FILE:-lista.md}"

# Paralelizacao
WORKERS="${WORKERS:-24}"

mkdir -p "$OUT_DIR"
TMP_DIR="$OUT_DIR/_tmp"
mkdir -p "$TMP_DIR"

echo "==========================================" | tee "$LOG_PATH"
echo "R11 PARALELO - Daytrade (3 familias + Walk-Forward + Bootstrap)" | tee -a "$LOG_PATH"
echo "==========================================" | tee -a "$LOG_PATH"
echo "run_id     = $RUN_ID" | tee -a "$LOG_PATH"
echo "out_dir    = $OUT_DIR" | tee -a "$LOG_PATH"
echo "vCPU disp. = $(nproc), workers=$WORKERS" | tee -a "$LOG_PATH"
echo "mining     = $MINING_START a $MINING_END (walk-forward: m1<=$MW1_END, m2<=$MW2_END, m3<=$MINING_END)" | tee -a "$LOG_PATH"
echo "val        = $VAL_START a $VAL_END" | tee -a "$LOG_PATH"
echo "oos        = $OOS_START a $OOS_END" | tee -a "$LOG_PATH"
echo "gates_A    = trades>=$MIN_MINING_TRADES, win>=$MIN_MINING_WIN%, PF>=$MIN_MINING_PF, Wilson>=$MIN_WILSON%, binom<$MAX_BINOM_M" | tee -a "$LOG_PATH"
echo "gate_WF    = win>=$WF_MIN_WIN% em cada janela com >=$WF_MIN_TRADES trades" | tee -a "$LOG_PATH"
echo "gates_B    = val_trades>=$MIN_VAL_TRADES, val_win>=$MIN_VAL_WIN%, val_avg>=$MIN_VAL_AVG%, binom<$MAX_BINOM_V" | tee -a "$LOG_PATH"
echo "gate_C     = FDR q < $MAX_FDR_Q" | tee -a "$LOG_PATH"
echo "gate_Boot  = bootstrap CI95 lower > $MIN_BOOTSTRAP_LOWER%" | tee -a "$LOG_PATH"
echo "engine     = max_factors=$MAX_FACTORS, max_promoted_2f=$MAX_PROMOTED_2F, max_accs=$MAX_ACCUMULATORS, min_mining_bars=$MIN_MINING_BARS" | tee -a "$LOG_PATH"
echo "" | tee -a "$LOG_PATH"

split_tickers() {
  local n="$1"
  local prefix="$2"
  "$PYTHON" -c "
from b3_patterns.tickers import load_tickers
tickers = [t.removesuffix('.SA') for t in load_tickers('$TICKERS_FILE')]
n = $n
batches = [[] for _ in range(n)]
for i, t in enumerate(tickers):
    batches[i % n].append(t)
for i, batch in enumerate(batches):
    with open(f'$prefix-{i}.txt', 'w') as f:
        for t in batch:
            f.write(t + '\n')
print(f'split {len(tickers)} tickers em {n} batches')
"
}

echo "==========================================" | tee -a "$LOG_PATH"
echo "MINING ($WORKERS workers)" | tee -a "$LOG_PATH"
echo "==========================================" | tee -a "$LOG_PATH"

batches_prefix="$TMP_DIR/day-batch"
split_tickers "$WORKERS" "$batches_prefix" | tee -a "$LOG_PATH"

started=$(date +%s)
echo "[mining] disparando $WORKERS processos paralelos..." | tee -a "$LOG_PATH"

pids=()
for i in $(seq 0 $((WORKERS - 1))); do
  batch_file="$batches_prefix-$i.txt"
  out_csv="$TMP_DIR/r11-mining-$i.csv"
  log_file="$TMP_DIR/worker-$i.log"
  "$PYTHON" R11/tools/r11_engine.py \
    --db-path b3_history.db \
    --tickers-file "$batch_file" \
    --output-csv "$out_csv" \
    --mining-start "$MINING_START" \
    --mw1-end "$MW1_END" \
    --mw2-end "$MW2_END" \
    --mining-end "$MINING_END" \
    --val-end "$VAL_END" \
    --oos-end "$OOS_END" \
    --max-factors "$MAX_FACTORS" \
    --max-promoted-2f "$MAX_PROMOTED_2F" \
    --max-accumulators "$MAX_ACCUMULATORS" \
    --gate-a-min-trades "$MIN_MINING_TRADES" \
    --gate-a-min-pf "$MIN_MINING_PF" \
    --gate-a-min-win "$MIN_MINING_WIN" \
    --min-mining-bars "$MIN_MINING_BARS" \
    --progress-every-tickers 1 \
    > "$log_file" 2>&1 &
  pids+=($!)
  echo "[mining] worker $i (PID $!) batch=$batch_file" | tee -a "$LOG_PATH"
done

failed=0
for pid in "${pids[@]}"; do
  if ! wait "$pid"; then
    echo "[mining] worker PID $pid falhou" | tee -a "$LOG_PATH"
    failed=$((failed + 1))
  fi
done
elapsed=$(($(date +%s) - started))
echo "[mining] todos workers terminaram em ${elapsed}s (falhas=$failed)" | tee -a "$LOG_PATH"

if [[ $failed -gt 0 ]]; then
  echo "ABORTANDO - logs em $TMP_DIR/worker-*.log" | tee -a "$LOG_PATH"
  exit 1
fi

final_csv="$OUT_DIR/r11-mining.csv"
first=1
for i in $(seq 0 $((WORKERS - 1))); do
  part="$TMP_DIR/r11-mining-$i.csv"
  [[ ! -f "$part" ]] && continue
  if [[ $first -eq 1 ]]; then
    cat "$part" > "$final_csv"
    first=0
  else
    tail -n +2 "$part" >> "$final_csv"
  fi
done
total_rows=$(($(wc -l < "$final_csv") - 1))
echo "[mining] concatenado: $final_csv ($total_rows linhas)" | tee -a "$LOG_PATH"

echo "==========================================" | tee -a "$LOG_PATH"
echo "VALIDATE" | tee -a "$LOG_PATH"
echo "==========================================" | tee -a "$LOG_PATH"
"$PYTHON" R11/tools/validate_r11.py \
  --input-csv "$final_csv" \
  --output-csv "$OUT_DIR/r11-validated.csv" \
  --min-wilson-ci-lower "$MIN_WILSON" \
  --max-binom-p-mining "$MAX_BINOM_M" \
  --max-binom-p-val "$MAX_BINOM_V" \
  --wf-min-trades-per-window "$WF_MIN_TRADES" \
  --wf-min-win-per-window "$WF_MIN_WIN" \
  --min-val-trades "$MIN_VAL_TRADES" \
  --min-val-win "$MIN_VAL_WIN" \
  --min-val-avg "$MIN_VAL_AVG" \
  --max-fdr-q "$MAX_FDR_Q" \
  --min-bootstrap-lower "$MIN_BOOTSTRAP_LOWER" \
  2>&1 | tee -a "$LOG_PATH"

echo "==========================================" | tee -a "$LOG_PATH"
echo "BUILD FINAL" | tee -a "$LOG_PATH"
echo "==========================================" | tee -a "$LOG_PATH"
"$PYTHON" R11/tools/build_r11_final.py \
  --validated-csv "$OUT_DIR/r11-validated.csv" \
  --output-dir "$OUT_DIR" \
  --mining-start "$MINING_START" \
  --mw1-end "$MW1_END" \
  --mw2-end "$MW2_END" \
  --mining-end "$MINING_END" \
  --val-start "$VAL_START" \
  --val-end "$VAL_END" \
  --oos-start "$OOS_START" \
  --oos-end "$OOS_END" \
  --min-mining-trades "$MIN_MINING_TRADES" \
  --min-mining-win "$MIN_MINING_WIN" \
  --min-mining-pf "$MIN_MINING_PF" \
  --min-wilson "$MIN_WILSON" \
  --max-binom-m "$MAX_BINOM_M" \
  --wf-min-trades "$WF_MIN_TRADES" \
  --wf-min-win "$WF_MIN_WIN" \
  --min-val-trades "$MIN_VAL_TRADES" \
  --min-val-win "$MIN_VAL_WIN" \
  --min-val-avg "$MIN_VAL_AVG" \
  --max-binom-v "$MAX_BINOM_V" \
  --max-fdr-q "$MAX_FDR_Q" \
  --min-bootstrap-lower "$MIN_BOOTSTRAP_LOWER" \
  2>&1 | tee -a "$LOG_PATH"

echo "" | tee -a "$LOG_PATH"
echo "limpando intermediarios em $TMP_DIR..." | tee -a "$LOG_PATH"
rm -rf "$TMP_DIR"

echo "" | tee -a "$LOG_PATH"
echo "DONE - R11 finalizado" | tee -a "$LOG_PATH"
ls -lh "$OUT_DIR" | tee -a "$LOG_PATH"
