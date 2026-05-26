# Gain — Mineração de Padrões B3

Toolkit de mineração quantitativa para a B3. Descobre padrões estatísticos em dados de preço/volume, valida com walk-forward + bootstrap em 7 anos de histórico, e entrega sinais diários via Telegram.

## Portfolios ativos

| Portfolio | Tipo | OOS 2026 | Setups |
|---|---|---|---|
| **R10** | Swing (alvos 4-10%, cap 5-20 pregões) | 75.80% win, +2.16% avg | 199 em 71 tickers |
| **R11** | Daytrade (alvos 1-3%, cap 1-3 pregões) | 97.86% win, +1.43% avg | 231 em 77 tickers |

Enquanto R10/R11 não são gerados, o pipeline opera com **Books v1** (`entrega-operacional-v1/`) como fallback automático.

---

## Dois tipos de VPS

### VPS Discovery — rodar R10/R11

> Requisito: **64+ GB RAM**

```bash
git clone https://github.com/zxtheuxz/gain.git
cd gain

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Gerar banco de preços com 8 anos de histórico (~10-20 min)
python -m b3_patterns sync --tickers-file lista.md --window-days 2200

# R10 (swing) — ~30-40 min
chmod +x R10/run_r10_parallel.sh
tmux new-session -d -s r10 "bash R10/run_r10_parallel.sh"
tmux attach -t r10   # Ctrl+B D para desanexar

# R11 (daytrade) — ~30-60 min
chmod +x R11/run_r11_parallel.sh
tmux new-session -d -s r11 "bash R11/run_r11_parallel.sh"
tmux attach -t r11

# Commitar os CSVs gerados
git add reports/
git commit -m "Add R10/R11 final portfolios"
git push origin main
```

Saídas em `reports/r10-<ID>/r10-final.csv` e `reports/r11-<ID>/r11-final.csv`.

### VPS Sinais — pipeline Telegram diário

> Requisito: qualquer VPS com 2+ GB RAM

```bash
git clone https://github.com/zxtheuxz/gain.git
cd gain

pip install -r requirements.txt
python3 -m b3_patterns sync --tickers-file lista.md --window-days 2200

# Credenciais Telegram
cp sinais-operacional/telegram-secrets.example.json sinais-operacional/telegram-secrets.json
# edite bot_token e chat_id

# Cron seg-sab 09:00 BRT (12:00 UTC)
chmod +x sinais-operacional/sinais_operacional_v1.sh
( crontab -l 2>/dev/null; \
  echo '0 12 * * 1-6 /home/codeuser/gain/sinais-operacional/sinais_operacional_v1.sh >> /home/codeuser/gain/reports/cron-sinais.log 2>&1' ) | crontab -

# Teste
SINAIS_SKIP_SYNC=1 bash sinais-operacional/sinais_operacional_v1.sh
```

Assim que `reports/r10-*/r10-final.csv` e `reports/r11-*/r11-final.csv` existirem (via `git pull` após a VPS Discovery terminar), o pipeline faz upgrade automático sem nenhuma mudança de código.

---

## Estrutura do repositório

```
R10/                        ← engine swing (mining, validate, build)
R11/                        ← engine daytrade (3 famílias de templates)
b3_patterns/                ← biblioteca core (ingestion, features, engine)
sinais-operacional/         ← pipeline Telegram diário
entrega-operacional-v1/     ← Books v1 (fallback swing + daytrade)
tests/                      ← testes unitários
lista.md                    ← universo operacional de tickers
requirements.txt
```

## Dependências

```bash
pip install -r requirements.txt
```

Apenas `yfinance` e `pandas`. Todo o motor de análise, backtesting e estatísticas é Python puro (sem numpy/scipy).

## Testes

```bash
python -m pytest tests/ -v
```
