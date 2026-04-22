#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON="${PYTHON:-.venv/bin/python}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%d-%H%M%S)}"
OUT_DIR="${OUT_DIR:-reports/r3-fine-targets-${RUN_ID}}"

# Default grid:
# - Targets are in tenths because strategy codes currently keep one decimal place.
# - 3.0 was already tested in the previous R3 run, so default stops at 2.9.
#   Use INCLUDE_THREE=1 to include 3.0 as well.
read -r -a CHUNKS <<< "${CHUNKS:-10:14 15:19 20:24 25:29}"
if [[ "${INCLUDE_THREE:-0}" == "1" ]]; then
  CHUNKS+=("30:30")
fi

read -r -a CAPS <<< "${CAPS:-5 10}"
read -r -a STOPS <<< "${STOPS:-2.0 3.0 4.0}"
MAX_TARGET_PCT="${MAX_TARGET_PCT:-$([[ "${INCLUDE_THREE:-0}" == "1" ]] && echo "3.0" || echo "2.9")}"

MAX_PROMOTED_PAIRS="${MAX_PROMOTED_PAIRS:-2500}"
MAX_ACCUMULATORS="${MAX_ACCUMULATORS:-25000000}"
PRUNE_EVERY_TICKERS="${PRUNE_EVERY_TICKERS:-25}"
MIN_TRADES="${MIN_TRADES:-120}"
MIN_TICKERS="${MIN_TICKERS:-20}"
EXPORT_REJECTED="${EXPORT_REJECTED:-0}"
EXPORT_REGISTRY="${EXPORT_REGISTRY:-0}"
EXPORT_TICKER_ALL="${EXPORT_TICKER_ALL:-0}"

mkdir -p "$OUT_DIR"

echo "R3 fine targets"
echo "out_dir=$OUT_DIR"
echo "chunks=${CHUNKS[*]}"
echo "caps=${CAPS[*]}"
echo "stops=${STOPS[*]}"
echo "max_promoted_pairs=$MAX_PROMOTED_PAIRS"
echo "max_accumulators=$MAX_ACCUMULATORS"
echo "prune_every_tickers=$PRUNE_EVERY_TICKERS"
echo "export_rejected=$EXPORT_REJECTED"
echo "export_registry=$EXPORT_REGISTRY"
echo "export_ticker_all=$EXPORT_TICKER_ALL"
echo

for cap in "${CAPS[@]}"; do
  for chunk in "${CHUNKS[@]}"; do
    start="${chunk%%:*}"
    end="${chunk##*:}"
    run_name="cap${cap}_t${start}_${end}"
    log_path="$OUT_DIR/${run_name}.log"

    TARGET_STOP_PAIRS=()
    for target_tenths in $(seq "$start" "$end"); do
      target="$((target_tenths / 10)).$((target_tenths % 10))"
      for stop in "${STOPS[@]}"; do
        TARGET_STOP_PAIRS+=("${target}:${stop}")
      done
    done

    echo "== $run_name =="
    echo "target_stop_pairs=${TARGET_STOP_PAIRS[*]}"
    echo "log=$log_path"

    if [[ "${DRY_RUN:-0}" == "1" ]]; then
      continue
    fi

    REJECTED_ARGS=()
    if [[ "$EXPORT_REJECTED" == "1" ]]; then
      REJECTED_ARGS=(--rejected-csv "$OUT_DIR/${run_name}-rejected.csv")
    fi

    REGISTRY_ARGS=()
    if [[ "$EXPORT_REGISTRY" == "1" ]]; then
      REGISTRY_ARGS=(
        --registry-csv "$OUT_DIR/${run_name}-registry.csv"
        --registry-md "$OUT_DIR/${run_name}-memory.md"
      )
    fi

    TICKER_SUMMARY_ARGS=()
    if [[ "$EXPORT_TICKER_ALL" == "1" ]]; then
      TICKER_SUMMARY_ARGS=(--ticker-summary-csv "$OUT_DIR/${run_name}-tickers-all.csv")
    fi

    "$PYTHON" -m b3_patterns asset-discover-r3 \
      --tickers-file lista.md \
      --entry-rules open close \
      --trade-directions long \
      --target-stop-pairs "${TARGET_STOP_PAIRS[@]}" \
      --atr-target-stop-pairs 99:99 \
      --atr-time-cap-days "$cap" \
      --time-cap-days "$cap" \
      --include-known \
      --pre-filter-min-profitable-rate 68 \
      --pre-filter-min-profit-factor 1.35 \
      --pre-filter-min-trades 40 \
      --min-success-rate 70 \
      --min-take-profit-rate 70 \
      --min-profit-factor 1.70 \
      --min-average-trade-return-pct 0.15 \
      --min-trades "$MIN_TRADES" \
      --min-tickers "$MIN_TICKERS" \
      --max-promoted-pairs "$MAX_PROMOTED_PAIRS" \
      --max-accumulators "$MAX_ACCUMULATORS" \
      --prune-every-tickers "$PRUNE_EVERY_TICKERS" \
      --progress-every-tickers 10 \
      --output-csv "$OUT_DIR/${run_name}-approved.csv" \
      "${REJECTED_ARGS[@]}" \
      --trades-csv "$OUT_DIR/${run_name}-trades.csv" \
      "${TICKER_SUMMARY_ARGS[@]}" \
      --ticker-qualified-csv "$OUT_DIR/${run_name}-tickers-qualified.csv" \
      "${REGISTRY_ARGS[@]}" \
      --summary-md "$OUT_DIR/${run_name}-summary.md" \
      2>&1 | tee "$log_path"
  done
done

if [[ "${DRY_RUN:-0}" == "1" ]]; then
  echo
  echo "DRY_RUN=1: commands were not executed."
  exit 0
fi

MERGED_APPROVED="$OUT_DIR/asset-r3-fine-targets-approved.csv"
MERGED_TRADES="$OUT_DIR/asset-r3-fine-targets-trades.csv"

"$PYTHON" tools/merge_csv_by_header.py \
  --output "$MERGED_APPROVED" \
  "$OUT_DIR"/cap*_t*_*-approved.csv

"$PYTHON" tools/merge_csv_by_header.py \
  --output "$MERGED_TRADES" \
  "$OUT_DIR"/cap*_t*_*-trades.csv

"$PYTHON" tools/generate_asset_r3_stat_report.py \
  --approved-csv "$MERGED_APPROVED" \
  --trades-csv "$MERGED_TRADES" \
  --final-strategies-csv "$OUT_DIR/asset-r3-fine-targets-final.csv" \
  --final-tickers-csv "$OUT_DIR/asset-r3-fine-targets-final-tickers.csv" \
  --final-actions-csv "$OUT_DIR/asset-r3-fine-targets-actions.csv" \
  --shortlist-strategies-csv "$OUT_DIR/asset-r3-fine-targets-shortlist.csv" \
  --shortlist-tickers-csv "$OUT_DIR/asset-r3-fine-targets-shortlist-tickers.csv" \
  --shortlist-actions-csv "$OUT_DIR/asset-r3-fine-targets-shortlist-actions.csv" \
  --report-md "$OUT_DIR/asset-r3-fine-targets-stat-report.md"

"$PYTHON" tools/filter_asset_r3_high_accuracy.py \
  --input-csv "$OUT_DIR/asset-r3-fine-targets-final.csv" \
  --tickers-input-csv "$OUT_DIR/asset-r3-fine-targets-final-tickers.csv" \
  --trades-input-csv "$MERGED_TRADES" \
  --output-csv "$OUT_DIR/asset-r3-fine-targets-high-accuracy.csv" \
  --tickers-output-csv "$OUT_DIR/asset-r3-fine-targets-high-accuracy-tickers.csv" \
  --actions-output-csv "$OUT_DIR/asset-r3-fine-targets-high-accuracy-actions.csv" \
  --report-md "$OUT_DIR/asset-r3-fine-targets-high-accuracy.md" \
  --max-target-pct "$MAX_TARGET_PCT" \
  --min-trades 150 \
  --min-tickers 30 \
  --min-win-rate 82 \
  --min-take-profit-rate 78 \
  --min-profit-factor 1.80 \
  --min-average-return 0.15 \
  --min-test-trades 25 \
  --min-test-win-rate 85 \
  --min-test-take-profit-rate 80 \
  --min-test-profit-factor 2.00 \
  --min-test-average-return 0.10

echo
echo "DONE"
echo "out_dir=$OUT_DIR"
echo "high_accuracy=$OUT_DIR/asset-r3-fine-targets-high-accuracy.csv"
echo "actions=$OUT_DIR/asset-r3-fine-targets-high-accuracy-actions.csv"
echo "report=$OUT_DIR/asset-r3-fine-targets-high-accuracy.md"
