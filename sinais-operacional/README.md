# Sinais Operacionais — Swing + Daytrade no Telegram

Pipeline diario que avalia os books validados (swing + daytrade) no ultimo
pregao e entrega um resumo no Telegram: novos sinais, resultado dos daytrades
de ontem e swings em andamento. Roda sozinho via cron (seg-sab 09:00 BRT).

## Conteudo da pasta

| Arquivo | Funcao |
|---|---|
| `sinais_operacional_v1.sh` | Orquestrador chamado pelo cron: sync -> sinais -> ledger -> Telegram |
| `generate_operacional_signals.py` | Gera os sinais do dia (swing + daytrade) em `reports/operacional-signals-<DATE>.csv` |
| `track_operacional.py` | Mantem o ledger (`reports/operacional-ledger.csv`): entradas, saidas, posicoes abertas |
| `telegram_send_operacional.py` | Formata e envia a mensagem ao Telegram (`--dry-run` para so imprimir) |
| `telegram-secrets.example.json` | Modelo das credenciais (copie para `telegram-secrets.json`) |

## A mensagem (4 secoes)

- 🕒 **SWING** — novos sinais swing para o proximo pregao (Alvo/Stop/Dias/PF/Taxa).
- ⚡ **DAYTRADE** — novos sinais daytrade (quando houver).
- 📊 **RESULTADO DT** — daytrades que sairam no pregao mais recente (✅TARGET / 🛑STOP / ⏹️CLOSE).
- 🟡 **SWING EM ANDAMENTO** — swings ainda abertos (D/Cap, PnL, Last).

`RESULTADO DT` e `SWING EM ANDAMENTO` comecam vazios e se preenchem conforme
operacoes REAIS acontecem. Nao faca backfill sintetico do ledger.

## Portfolios (auto-upgrade R10/R11)

- Hoje opera **Books v1**: `entrega-operacional-v1/reports/operational-books-v1/swing-combinado.csv` (47 setups) + `daytrade-combinado.csv` (77 setups).
- Assim que existirem `reports/r10-*/r10-final.csv` (swing) e `reports/r11-*/r11-final.csv` (daytrade), o gerador **passa a usa-los automaticamente** — e so colocar os CSVs nesses caminhos, sem mexer em codigo. O loader e tolerante aos dois schemas (Books v1 e R10/R11).

## Deploy numa VPS nova (a partir de um fresh clone)

Pre-requisitos no repo: `b3_patterns/`, `lista.md`, `entrega-operacional-v1/` (ja versionados). Faltam apenas: dependencias Python, `b3_history.db` e as credenciais.

```bash
cd /caminho/para/gain

# 1) Dependencias (pandas + yfinance). Use UMA das opcoes:
python3 -m pip install -r requirements.txt          # no python do sistema
# ou venv: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

# 2) Banco de dados (b3_history.db NAO vem no git):
bash github-artifacts/restore_artifacts.sh          # restaura do backup, se existir
# ou recria do zero (historico ~8 anos):
python3 -m b3_patterns sync --tickers-file lista.md --window-days 2200

# 3) Credenciais do Telegram (NUNCA commitar; ja esta no .gitignore da pasta):
cp sinais-operacional/telegram-secrets.example.json sinais-operacional/telegram-secrets.json
# edite bot_token e chat_id

# 4) Permissao de execucao:
chmod +x sinais-operacional/sinais_operacional_v1.sh

# 5) Cron (seg-sab 09:00 BRT = 12:00 UTC). Ajuste o caminho absoluto:
( crontab -l 2>/dev/null; \
  echo '0 12 * * 1-6 /caminho/para/gain/sinais-operacional/sinais_operacional_v1.sh >> /caminho/para/gain/reports/cron-sinais.log 2>&1' ) | crontab -

# 6) Teste ponta a ponta (envia de verdade):
SINAIS_SKIP_SYNC=1 bash sinais-operacional/sinais_operacional_v1.sh
```

> O script usa a **data UTC** para ignorar a barra do dia corrente. Como o cron
> roda 12:00 UTC (= 09:00 BRT, pre-mercado), o ultimo pregao completo e sempre
> uma data anterior — evita operar em cima de close provisorio intraday.

## Execucao manual / debug

```bash
bash sinais-operacional/sinais_operacional_v1.sh                  # tudo (sync + envia)
SINAIS_SKIP_SYNC=1 bash sinais-operacional/sinais_operacional_v1.sh   # sem baixar dados
SINAIS_SKIP_TELEGRAM=1 bash sinais-operacional/sinais_operacional_v1.sh  # sem enviar (so previa)
python3 sinais-operacional/telegram_send_operacional.py --dry-run     # so a previa da mensagem
tail -f reports/cron-sinais.log                                  # acompanhar o cron
```

Flags de ambiente: `SINAIS_SKIP_SYNC`, `SINAIS_SKIP_TELEGRAM`, `PYTHON_BIN`,
`SYNC_WINDOW_DAYS` (default 2200), `TICKERS_FILE` (default `lista.md`).

## Detalhes tecnicos

- **Entrada** = proximo pregao apos o sinal (robusto a feriado: primeira barra com data > signal_date). `entry_rule=open` entra no open; `close` entra no close.
- **Saida** espelha `_simulate_percent_exit` de `b3_patterns/asset_discovery_round1.py`: janela `[entry .. entry+cap]`, long `target=entry*(1+tp%)` / `stop=entry*(1-sl%)`, STOP vence em barra ambigua (conservador), senao sai no close do ultimo dia (`time_cap`).
- **Ledger** `reports/operacional-ledger.csv` (chave `book|ticker|signal_date|strategy_code`; status `pending`/`open`/`closed`). Idempotente.
- Precos ajustados (`adj_close`). Estrategias long-only.

## Arquivos gerados (gitignored, em `reports/`)

`operacional-signals-<DATE>.csv`, `operacional-ledger.csv`, `cron-sinais.log`.
