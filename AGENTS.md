# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Quantitative pattern mining toolkit for B3 (Brazilian stock exchange). Discovers statistical patterns in price/volume data, backtests strategies with percentage and ATR-based exits, and monitors live signals via a React dashboard. Inspired by Renaissance Technologies' approach of combining many weak statistical signals.

## Commands

```bash
# Install dependencies
python -m pip install -r requirements.txt

# Run all tests
python -m pytest tests/ -v

# Run a single test file
python -m pytest tests/test_asset_discovery_round1.py -v

# Run a single test
python -m pytest tests/test_analysis.py::AnalysisTest::test_analyze_patterns -v

# Sync price history from Yahoo Finance
python -m b3_patterns sync --tickers-file lista.md

# Import B3 COTAHIST official data
python -m b3_patterns options-sync --tickers-file lista.md --years 2025 2026

# Full analysis pipeline
python -m b3_patterns run --output-csv reports/ranking.csv

# Asset discovery - Round 1 (percentage exits, 2-factor)
python -m b3_patterns asset-discover-round1 --entry-rules open close --target-stop-pairs 1:1 2:1 3:1.5 4:2 6:3 --time-cap-days 5

# Asset discovery - R3 (ATR exits, 50 features, progressive 2f→3f)
python -m b3_patterns asset-discover-r3

# R3 stat report (train/test split + statistical significance)
python tools/generate_asset_r3_stat_report.py

# Export monitor JSON for React dashboard
python -m b3_patterns asset-monitor-export

# Start web dashboard
cd monitor-web && npm install && npm run dev
```

### Fresh Clone / VPS Bootstrap

Important: Git does **not** contain the full local working folder. The repository intentionally excludes generated/heavy/local artifacts:

- `b3_history.db` (local SQLite database, ~559 MB on the original machine)
- `data/` (downloaded COTAHIST ZIP/TXT files)
- `reports/` (generated CSV/Markdown/log outputs)
- `.claude/`, `.pytest_cache/`, `.tmp-tests/`, `__pycache__/`
- `monitor-web/node_modules/` and `monitor-web/dist/`

After `git clone`, the VPS will have the source code, docs, tests, frontend source, and lockfiles, but it will **not** have a ready `b3_history.db`. Recreate it on the VPS from external sources:

```bash
# Clone private GitHub repo (requires GitHub auth/token if prompted)
git clone https://github.com/zxtheuxz/gain.git
cd gain

# Python environment
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

# Create/populate b3_history.db with Yahoo Finance price history
python -m b3_patterns sync --tickers-file lista.md

# Optional but required for options/COTAHIST workflows:
# downloads COTAHIST into data/cotahist/ and imports spot/options into the same SQLite DB
python -m b3_patterns options-sync --tickers-file lista.md --years 2025 2026
```

If the exact original database is required byte-for-byte, do **not** expect Git to provide it. Copy `b3_history.db` separately, or use Git LFS/object storage. The normal VPS path is to regenerate it with `sync` and `options-sync`.

Stock universe rule: always use `lista.md` as the operational stock universe for this project. Do not use `acoes-listadas-b3.csv` for sync, options, discovery, backtests, or monitor exports unless the user explicitly asks for that broader file. The CLI default is `lista.md`, but pass `--tickers-file lista.md` in manual commands when clarity matters.

When the user says **"Estou na VPS"**, assume the goal is to continue deployment from a fresh or partially prepared VPS. First inspect the VPS state, then continue:

```bash
pwd
git status --short --branch
python --version || python3 --version
test -f b3_history.db && ls -lh b3_history.db || echo "b3_history.db missing"
```

If `b3_history.db` is missing, recreate it with the sync commands above before running discovery/backtests/monitor exports. Do not assume local artifacts from `E:\gain` were cloned.

### VPS Deployment (Ubuntu, 8 vCPU, 124 GB RAM)

For heavy R3 discovery with full 3-factor expansion:

```bash
# Full run with generous memory limits (124 GB available)
python -m b3_patterns asset-discover-r3 \
  --entry-rules open close \
  --trade-directions long short \
  --target-stop-pairs 3:3 3:4 4:4 \
  --atr-target-stop-pairs 1.5:1 2:1.5 \
  --atr-time-cap-days 5 10 \
  --time-cap-days 10 \
  --max-promoted-pairs 500 \
  --max-accumulators 5000000 \
  --prune-every-tickers 50 \
  --progress-every-tickers 10 \
  2>&1 | tee reports/r3-full-run.log

# Conservative run (limit memory to ~30 GB)
python -m b3_patterns asset-discover-r3 \
  --max-promoted-pairs 150 \
  --max-accumulators 500000 \
  2>&1 | tee reports/r3-conservative.log
```

**Memory scaling reference** (from actual runs):
- 2f phase: ~200 MB, ~2 min for 81 tickers
- 3f phase (561 promoted pairs): 3.7M accumulators → ~4 GB, 40 min
- Trades collection: ~300 bytes/trade, 50M trades → ~15 GB
- `--max-promoted-pairs` is the primary memory control knob

## Architecture

### Data Flow

```
Yahoo Finance / B3 COTAHIST
  → ingestion.py / cotahist.py
    → SQLite (b3_history.db) with WAL mode
      → analysis.py        (trigger-based pattern matching)
      → options.py          (ATM options backtesting, 136 strategies)
      → discovery.py        (probabilistic state mining for options, 20 features)
      → asset_discovery_round1.py  (asset-first discovery, 50 features, 1f/2f/3f)
        → reporting.py      (CSV, Markdown, console output)
        → asset_monitor.py  (JSON export for React dashboard)
          → monitor-web/    (React + Vite signal dashboard)
```

### Key Modules

- **`asset_discovery_round1.py`** — The most complex module. Contains the 50-feature library (`FEATURE_LIBRARY`), template builders for percent/ATR exits, the progressive 2f→3f mining engine, pattern accumulation, and trade collection. Memory-intensive for 3-factor combinations.

- **`models.py`** — All dataclasses: `PriceBar`, `SpotQuoteBar`, `StrategyTrade`, `StrategySummary`, `AssetDiscoveryTemplate`, `AssetDiscoveryPatternSummary`, `StrategyRegistryEntry`, etc. All use `slots=True`.

- **`cli.py`** — Argparse CLI with 11 subcommands. Each `_run_*` function orchestrates a full pipeline (load → mine → filter → export). Entry point: `python -m b3_patterns <command>`.

- **`registry.py`** — CSV-based persistent memory of tested strategies (approved/rejected). Prevents redundant re-evaluation across runs via `get_known_strategy_codes()`.

- **`reporting.py`** — Unified export layer for CSV, Markdown, and console. Used by all pipelines.

### Database Schema (SQLite, WAL)

Four tables: `price_history` (Yahoo OHLCV), `spot_quote_history` (B3 spot), `option_quote_history` (B3 options), `sync_metadata` (state tracking). All indexed on `(ticker, trade_date)`.

### Discovery Pipeline Stages (R3)

1. **Build templates**: entry rules × directions × target/stop pairs × ATR variants
2. **Mine 2-factor patterns**: iterate all tickers × days × templates, accumulate per-pattern metrics
3. **Promote pairs**: filter 2f results by profitable rate + profit factor → promoted pairs
4. **Mine 3-factor patterns**: only expand triples where at least one 2f subset was promoted
5. **Split approved/rejected**: filter by min trades, win rate, target hit rate, profit factor, avg return, min tickers
6. **Collect trades**: re-iterate samples, emit individual trade records for approved patterns
7. **Export**: CSVs, Markdown reports, registry updates

### Monitor Dashboard

React 18 + Vite app in `monitor-web/`. Reads `public/data/asset-monitor.json` (generated by `asset-monitor-export`). Features: ticker search, qualified-only filter, entry rule filter, KPI cards, strategy badges. Dev server runs on `0.0.0.0:4173`.

## Conventions

- All prices are adjusted close. Raw OHLCV is multiplied by `adj_close/close` factor.
- Feature states are discretized into named bins (e.g., `gap_pct=strong_gap_down`). Pattern codes encode template + state combination.
- Profit factor = gross_profit / gross_loss. `None` when no loss, `inf` when profit but no loss.
- Exit modes: `percent` (fixed take-profit/stop-loss %), `atr` (ATR-multiple based).
- Exit reason values: `take_profit`, `stop_loss`, `stop_loss_trailing`, `time_cap`.
- Ticker normalization: `.SA` suffix for Yahoo Finance compatibility.
- Stock universe: always use `lista.md` unless the user explicitly asks for a different ticker file.
- Ticker files may be Markdown/TXT lists or CSV files with a `Ticker` column.
- Registry prevents re-testing known codes unless `--include-known` is passed.

## Testing

Tests use `unittest.TestCase` with `pytest` runner. Workspace isolation via `.tmp-tests/` temp directories with `addCleanup`. Tests create in-memory SQLite DBs with synthetic `PriceBar` data. No external API calls in tests.

Key test files map 1:1 to modules:
- `test_analysis.py` — patterns, strategies, registry
- `test_asset_discovery_round1.py` — features, exits, ATR, statistical significance (largest: 17 tests)
- `test_discovery.py` — options discovery + refinement
- `test_options.py` — options backtesting
- `test_tickers.py` — ticker loading/normalization

## Dependencies

Only two runtime dependencies: `yfinance` and `pandas` (used only for Yahoo Finance download). All analysis, backtesting, and statistics are pure Python with no numpy/scipy — uses hand-rolled normal CDF, binomial p-value, t-test approximation.

## Post-Processing Reports

The `tools/` directory contains standalone Python scripts that read approved CSVs and produce derived reports:
- `generate_asset_r3_stat_report.py` — Train/test split validation with statistical significance (binomial + t-test). Reads `r3-stat-approved.csv` + `r3-stat-trades-approved.csv`, produces final/shortlist CSVs and report Markdown.
- Other tools handle R1 elite reports, R2 operational filters, per-stock analysis, and exit target refinement.

These scripts are run manually after discovery completes, not as part of the CLI.
