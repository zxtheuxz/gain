# VPS Rebuild and R3 Operations

Use este guia para recriar a operacao em outra maquina. O Git guarda codigo, scripts, documentacao e snapshots pequenos de resultado. Ele nao guarda banco, COTAHIST bruto, relatorios gigantes, `.venv`, `node_modules` ou build do frontend.

Se a nova maquina nao tiver RAM/CPU para rodar o R3 pesado, use o snapshot versionado em `snapshots/r3-elite-2026-04-13/`. Ele contem o resultado top49 e o JSON pronto para o dashboard.

## 1. Clone e ambiente Python

```bash
git clone https://github.com/zxtheuxz/gain.git
cd gain

sudo apt update
sudo apt install -y python3.12-venv tmux

python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m pip install pytest
```

Valide:

```bash
.venv/bin/python -m pytest tests/ -q
```

## 2. Usar apenas o resultado pronto em maquina menor

Para maquinas que so precisam ver o resultado e abrir o dashboard:

```bash
tools/install_r3_snapshot.sh
```

Isso copia:

- `snapshots/r3-elite-2026-04-13/asset-monitor.json` para `monitor-web/public/data/asset-monitor.json`
- `snapshots/r3-elite-2026-04-13/asset-discovery-lista-r3-stat-elite-top49.csv` para `reports/`
- `snapshots/r3-elite-2026-04-13/asset-discovery-lista-r3-stat-report.md` para `reports/`

Depois rode o frontend:

```bash
cd monitor-web
npm install
npm run dev -- --host 0.0.0.0 --port 4173
```

Esse caminho nao exige `b3_history.db` e nao roda R3.

## 3. Recriar banco local quando a maquina for operacional

Sempre use `lista.md` como universo operacional.

```bash
.venv/bin/python -m b3_patterns sync \
  --tickers-file lista.md \
  --window-days 1500 \
  --max-workers 8

.venv/bin/python -m b3_patterns options-sync \
  --tickers-file lista.md \
  --years 2025 2026
```

Cheque a base:

```bash
.venv/bin/python - <<'PY'
import sqlite3
conn = sqlite3.connect("b3_history.db")
cur = conn.cursor()
for table, distinct_col in [
    ("price_history", "ticker"),
    ("spot_quote_history", "ticker"),
    ("option_quote_history", "underlying_root"),
]:
    cur.execute(
        f"select count(*), min(trade_date), max(trade_date), count(distinct {distinct_col}) from {table}"
    )
    print(table, cur.fetchone())
PY
```

## 4. Rodar R3 forte com tmux somente em maquina grande

Use esta etapa apenas em VPS com muita RAM. A rodada de origem usou dezenas de GiB de RAM e varias horas de CPU single-core.

Nunca rode discovery pesado fora do `tmux`.

```bash
tmux new -s r3
```

Dentro do `tmux`:

```bash
cd /home/gain/gain
tools/run_asset_r3_promoted1500.sh
```

Sair sem matar:

```text
Ctrl+b
d
```

Voltar:

```bash
tmux attach -t r3
```

Acompanhar de outro terminal:

```bash
tail -f /home/gain/gain/reports/r3-full-2025-01-01-2026-04-13-promoted1500.log
```

## 5. Pos-processar e filtrar elite

Depois que o R3 terminar:

```bash
cd /home/gain/gain

.venv/bin/python tools/generate_asset_r3_stat_report.py
.venv/bin/python tools/filter_asset_r3_elite.py
```

O filtro elite padrao exige:

- trades >= 200
- tickers >= 50
- alvo batido >= 65%
- acerto verde >= 70%
- retorno medio >= 1.3%
- profit factor >= 2.5
- alvo no teste >= 70%
- retorno medio no teste >= 2.0%

Saida padrao:

```text
reports/asset-discovery-lista-r3-stat-elite-top49.csv
```

## 6. Exportar dashboard

```bash
tools/update_monitor_daily.sh
```

Ou manualmente:

```bash
.venv/bin/python -m b3_patterns asset-monitor-export \
  --db-path b3_history.db \
  --tickers-file lista.md \
  --strategies-csv reports/asset-discovery-lista-r3-stat-elite-top49.csv \
  --top-strategies 0 \
  --output-json monitor-web/public/data/asset-monitor.json
```

Regra operacional no dashboard:

- Acionamento: estrategia elite apareceu no ticker.
- Confluencia operacional: ticker com pelo menos 3 estrategias elite acionadas no mesmo pregao.

## 7. Frontend

```bash
cd /home/gain/gain/monitor-web
npm install
npm run build
npm run dev -- --host 0.0.0.0 --port 4173
```

## 8. Cron diario do monitor

Opcional. Edite o crontab:

```bash
crontab -e
```

Exemplo para rodar em dias uteis as 21:00 UTC:

```cron
0 21 * * 1-5 /home/gain/gain/tools/update_monitor_daily.sh >> /home/gain/gain/reports/cron-monitor.log 2>&1
```

Nao coloque R3 pesado no cron. Rode R3 manualmente em `tmux`.

## 9. Limpeza de arquivos grandes

Depois de gerar final, shortlist, elite e dashboard, estes arquivos podem ser removidos para recuperar espaco:

```bash
rm -f reports/asset-discovery-lista-r3-stat-rejected.csv
rm -f reports/asset-discovery-lista-r3-stat-registry.csv
rm -f reports/asset-discovery-lista-r3-stat-memory.md
```

Cheque:

```bash
df -h /home/gain/gain
du -sh reports data b3_history.db
```

## 10. Arquivos que nao entram no Git

Esperado ficarem locais:

- `b3_history.db`
- `data/`
- `reports/`
- `.venv/`
- `.pytest_cache/`
- `.tmp-tests/`
- `monitor-web/node_modules/`
- `monitor-web/dist/`
