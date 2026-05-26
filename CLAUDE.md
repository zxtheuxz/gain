# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Quantitative pattern mining toolkit for B3 (Brazilian stock exchange). Discovers statistical patterns in price/volume data, backtests strategies with walk-forward + bootstrap validation, and delivers live signals via Telegram. Inspired by Renaissance Technologies' approach of combining many weak statistical signals.

**Portfolios ativos:** R10 (swing) + R11 (daytrade) — rodados com 7 anos de historico, walk-forward em 3 sub-janelas, FDR e bootstrap.

**Pipeline de sinais:** `sinais-operacional/` — cron diario que entrega swing + daytrade no Telegram.

---

## Dois tipos de VPS

Este projeto usa **dois tipos de VPS com funcoes distintas**. Antes de qualquer acao em VPS, identifique qual e:

### VPS Discovery (rodar R10/R11)
- **Funcao:** rodar as rodadas de mineracao R10 e R11 para gerar novos portfolios
- **Requisito:** 64+ GB RAM (cada worker usa ~30 GB no pico)
- **Resultado:** `reports/r10-<ID>/r10-final.csv` + `reports/r11-<ID>/r11-final.csv`
- **Nao precisa** de credenciais Telegram, cron ou deploy continuo
- Ver secao **VPS Discovery Bootstrap** abaixo

### VPS Sinais (pipeline Telegram diario)
- **Funcao:** rodar o cron diario que gera sinais e envia no Telegram
- **Requisito:** qualquer VPS com 2+ GB RAM (so roda yfinance sync + avalia features)
- **Resultado:** mensagem diaria no Telegram com sinais swing + daytrade
- **Precisa de:** credenciais Telegram (`telegram-secrets.json`), cron configurado
- Ver secao **VPS Sinais Bootstrap** abaixo

> Se voce diz **"Estou na VPS"**, pergunte primeiro: e a VPS de Discovery ou a VPS de Sinais?

---

## Commands

```bash
# Instalar dependencias
python -m pip install -r requirements.txt

# Rodar todos os testes
python -m pytest tests/ -v

# Sync de historico de precos (Yahoo Finance)
python -m b3_patterns sync --tickers-file lista.md

# Sync com historico estendido 8 anos (obrigatorio antes de R10/R11)
python -m b3_patterns sync --tickers-file lista.md --window-days 2200

# Importar dados COTAHIST da B3 (spot + opcoes)
python -m b3_patterns options-sync --tickers-file lista.md --years 2025 2026
```

---

## VPS Discovery Bootstrap (R10/R11)

Para gerar novos portfolios R10/R11 do zero numa VPS nova:

```bash
# 1. Clonar o repositorio
git clone https://github.com/zxtheuxz/gain.git
cd gain

# 2. Ambiente Python
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# 3. Gerar b3_history.db com 8 anos de historico (~10-20 min)
.venv/bin/python -m b3_patterns sync --tickers-file lista.md --window-days 2200

# 4. Rodar R10 (swing) em tmux — ~30-40 min com 24 workers
chmod +x R10/run_r10_parallel.sh
tmux new-session -d -s r10 "bash R10/run_r10_parallel.sh"
tmux attach -t r10     # Ctrl+B D para desanexar sem matar

# 5. Rodar R11 (daytrade) em tmux — ~30-60 min com 24 workers
chmod +x R11/run_r11_parallel.sh
tmux new-session -d -s r11 "bash R11/run_r11_parallel.sh"
tmux attach -t r11
```

**Saidas finais:**
- `reports/r10-<RUN_ID>/r10-final.csv` — portfolio swing (top 3 por ticker)
- `reports/r11-<RUN_ID>/r11-final.csv` — portfolio daytrade (top 3 por ticker)
- `*-validated.csv` — todos os setups que passaram os gates
- `*-report.md` — top 30 humano com walk-forward
- `*-oos-report.md` — performance honesta no OOS (nunca usado para filtrar)

**Escolha de modo por RAM e vCPU:**

| RAM disponivel | vCPUs | WORKERS | Tempo R10 | Tempo R11 |
|---|---|---|---|---|
| < 64 GB | qualquer | — | Nao rode; RAM insuficiente | — |
| 64-127 GB | qualquer | 8 | ~120 min | ~150 min |
| 128 GB | 8 | **8** | ~90-120 min | ~90-150 min |
| 128 GB | 16+ | 16 | ~60-80 min | ~60-90 min |
| 192+ GB | 24+ | 24 | ~30-40 min | ~30-60 min |

**VPS atual: 8 vCPUs / 128 GB** — usar `WORKERS=8`, rodar R10 e R11 **sequencialmente** (nao ao mesmo tempo).

```bash
# R10 primeiro
WORKERS=8 bash R10/run_r10_parallel.sh

# Depois, R11
WORKERS=8 bash R11/run_r11_parallel.sh
```

Regra geral: `WORKERS` = numero de vCPUs. Mais workers que vCPUs so adiciona overhead de context-switch sem ganho real.

**Validate R11 grande (>10M linhas):** usar `R11/tools/validate_r11_fast.py`
(pandas vetorizado) em vez de `validate_r11.py` (csv puro). Escala 10-100x melhor.

**Antes de rodar, leia `R10/README.md` ou `R11/README.md`.**

---

## VPS Sinais Bootstrap (pipeline Telegram)

Para colocar o cron de sinais diarios em funcionamento numa VPS nova:

```bash
# 1. Clonar o repositorio
git clone https://github.com/zxtheuxz/gain.git
cd gain

# 2. Dependencias Python (no sistema ou venv)
python3 -m pip install -r requirements.txt

# 3. Banco de dados de precos
python3 -m b3_patterns sync --tickers-file lista.md --window-days 2200

# 4. Credenciais do Telegram (NUNCA commitar — ja esta no .gitignore)
cp sinais-operacional/telegram-secrets.example.json sinais-operacional/telegram-secrets.json
# edite bot_token e chat_id com seus valores reais

# 5. Permissao de execucao
chmod +x sinais-operacional/sinais_operacional_v1.sh

# 6. Configurar cron (seg-sab 09:00 BRT = 12:00 UTC)
( crontab -l 2>/dev/null; \
  echo '0 12 * * 1-6 /home/codeuser/gain/sinais-operacional/sinais_operacional_v1.sh >> /home/codeuser/gain/reports/cron-sinais.log 2>&1' ) | crontab -

# 7. Teste ponta a ponta
SINAIS_SKIP_SYNC=1 bash sinais-operacional/sinais_operacional_v1.sh
```

**Portfolio ativo agora:** Books v1 (`entrega-operacional-v1/reports/operational-books-v1/`)
- `swing-combinado.csv` (47 setups swing)
- `daytrade-combinado.csv` (77 setups daytrade)

**Auto-upgrade para R10/R11:** assim que `reports/r10-*/r10-final.csv` e
`reports/r11-*/r11-final.csv` existirem no servidor, o pipeline passa a usa-los
automaticamente — sem mexer em codigo. Basta copiar os CSVs gerados pela VPS Discovery.

```bash
# Copiar CSVs da VPS Discovery para a VPS Sinais
scp user@vps-discovery:~/gain/reports/r10-*/r10-final.csv reports/r10-novo/
scp user@vps-discovery:~/gain/reports/r11-*/r11-final.csv reports/r11-novo/
```

**Detalhes e troubleshooting:** `sinais-operacional/README.md`

---

## Portfolios operacionais

### R10 — SWING (alvos 4-10%, cap 5-20 pregoes)
- 199 setups em 71 tickers
- OOS jan-mai/2026: 75.80% win em 40k trades, +2.16% avg
- 50 setups Tier S (OOS comprovado)
- Codigo: `R10/` (engine, validate, build, run_r10_parallel.sh)

### R11 — DAYTRADE (alvos 1-3%, cap 1-3 pregoes)
- 231 setups em 77 tickers
- OOS jan-mai/2026: **97.86% win** em 16k trades, +1.43% avg
- 138 setups Tier S (OOS comprovado)
- Codigo: `R11/` (engine, validate, build, run_r11_parallel.sh)
- Padrao dominante: entrar no close quando acao caiu 1-3%, alvo +1% no D+1

### Metodologia compartilhada

Tres janelas temporais com walk-forward:
- **Mining:** 2018-01-01 a 2024-12-31 (~7 anos, 3 sub-janelas: 2018-20, 2021-22, 2023-24)
- **Val:** 2025-01-01 a 2025-12-31 (~12 meses, filtro estatistico)
- **OOS puro:** 2026-01-01 em diante (so reporta — nunca usado para filtrar)

Gates obrigatorios (todos devem passar):
- **Gate A:** trades, win%, profit factor, Wilson CI95 lower, binomial p
- **Gate WF:** walk-forward em 3 sub-janelas (win minimo em cada com >= 3 trades)
- **Gate B:** validacao 2025 (trades, win%, avg return)
- **Gate C:** FDR Benjamini-Hochberg q-value < 0.05
- **Gate Bootstrap:** CI95 lower bound do avg return > 0%

Thresholds calibrados por rodada:
- R10 (swing): win >= 75%, PF >= 3, val_trades >= 15, val_avg >= 4%
- R11 (daytrade): win >= 63%, PF >= 2.2, val_trades >= 10, val_avg >= 0.5%

---

## Historico das rodadas

| Rodada | Foco | Validacao | Status |
|---|---|---|---|
| R3 a R9 | varios | parcial / train-test simples | **removidos do repo** |
| **R10** | **swing 7 anos** | **walk-forward + bootstrap + FDR** | **atual (swing)** |
| **R11** | **daytrade 7 anos** | **walk-forward + bootstrap + FDR** | **atual (daytrade)** |

---

## Architecture

### Data Flow

```
Yahoo Finance
  → b3_patterns/ingestion.py
    → SQLite (b3_history.db, WAL mode)
      → R10/tools/r10_engine.py   (mining swing)
      → R11/tools/r11_engine.py   (mining daytrade)
        → reports/r10-<ID>/r10-final.csv
        → reports/r11-<ID>/r11-final.csv
          → sinais-operacional/generate_operacional_signals.py
            → reports/operacional-signals-<DATE>.csv
              → sinais-operacional/telegram_send_operacional.py
                → Telegram
```

### Modulos principais (`b3_patterns/`)

- **`asset_discovery_round1.py`** — motor de mineracao: 50 features, template builders, engine 2f→3f, acumulacao de padroes, coleta de trades. Usado pelos engines R10 e R11.
- **`models.py`** — dataclasses com `slots=True`: `PriceBar`, `StrategyTrade`, `StrategySummary`, etc.
- **`cli.py`** — CLI argparse com subcomandos. Entry point: `python -m b3_patterns <comando>`.
- **`ingestion.py`** — sync Yahoo Finance → SQLite.
- **`tickers.py`** — carrega e normaliza listas de tickers (`.SA` suffix).

### Database Schema (SQLite, WAL)

Tres tabelas principais: `price_history` (Yahoo OHLCV), `spot_quote_history` (B3 spot), `option_quote_history` (B3 opcoes). Todas indexadas em `(ticker, trade_date)`.

---

## Conventions

- Todos os precos sao adjusted close. OHLCV bruto multiplicado pelo fator `adj_close/close`.
- Feature states discretizados em bins nomeados (ex: `gap_pct=strong_gap_down`).
- Profit factor = gross_profit / gross_loss. `None` quando sem loss, `inf` quando lucro sem loss.
- Ticker normalization: sufixo `.SA` para compatibilidade Yahoo Finance.
- **Universo de acoes:** sempre usar `lista.md`. Nao usar `acoes-listadas-b3.csv` para sync, discovery, backtests ou exports — a menos que o usuario peca explicitamente.

---

## Testing

Testes usam `unittest.TestCase` com runner `pytest`. Isolamento via `.tmp-tests/` com `addCleanup`. SQLite in-memory com dados sinteticos. Sem chamadas externas.

```bash
python -m pytest tests/ -v
```

Arquivos principais:
- `test_asset_discovery_round1.py` — features, exits, ATR, significancia estatistica (17 testes)
- `test_analysis.py` — patterns, strategies, registry
- `test_tickers.py` — carregamento e normalizacao de tickers

---

## Dependencies

Apenas duas dependencias runtime: `yfinance` e `pandas` (somente para download Yahoo Finance). Toda a analise, backtesting e estatisticas sao Python puro — CDF normal, binomial p-value e t-test implementados manualmente, sem numpy/scipy.
