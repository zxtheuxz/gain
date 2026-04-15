#!/usr/bin/env bash
set -euo pipefail

cd /home/gain/gain

: "${SNAPSHOT_DIR:=snapshots/r3-elite-2026-04-13}"

mkdir -p reports monitor-web/public/data

cp "${SNAPSHOT_DIR}/asset-discovery-lista-r3-stat-elite-top49.csv" \
  reports/asset-discovery-lista-r3-stat-elite-top49.csv

cp "${SNAPSHOT_DIR}/asset-discovery-lista-r3-stat-report.md" \
  reports/asset-discovery-lista-r3-stat-report.md

cp "${SNAPSHOT_DIR}/asset-monitor.json" \
  monitor-web/public/data/asset-monitor.json

echo "Snapshot instalado de ${SNAPSHOT_DIR}"
echo "Dashboard JSON: monitor-web/public/data/asset-monitor.json"
echo "Elite CSV: reports/asset-discovery-lista-r3-stat-elite-top49.csv"
