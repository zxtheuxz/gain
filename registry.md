# Registry - VPS R3 Discovery

Data do registro: 2026-04-14

Este arquivo resume o que foi feito na VPS para preparar dados, rodar o discovery R3, filtrar as melhores estrategias e atualizar o dashboard.

## Ambiente

- Caminho do projeto: `/home/gain/gain`
- Branch Git: `main...origin/main`
- Alteracao local de codigo/config: `.gitignore` foi atualizado para ignorar `.venv/`
- Ambiente Python: `.venv/`
- Banco SQLite local: `b3_history.db`
- Universo operacional: `lista.md`
- Dashboard: `monitor-web/`

## Preparacao da VPS

1. O clone ja existia em `/home/gain/gain`.
2. Python disponivel: `python3 3.12.3`.
3. `python` nao existia como alias no sistema, entao foi usado `.venv/bin/python`.
4. A criacao inicial da `.venv` falhou por falta de `python3.12-venv`.
5. Foi instalado:

```bash
sudo apt install -y python3.12-venv
```

6. Foi criada a `.venv`.
7. Foram instaladas dependencias do projeto:

```bash
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt
```

8. `pytest` nao estava no `requirements.txt`, entao foi instalado para validacao:

```bash
.venv/bin/python -m pip install pytest
```

9. Testes passaram:

```text
35 passed
```

## Dados importados

### Yahoo Finance

Foi rodado sync Yahoo com `lista.md` e janela maior:

```bash
.venv/bin/python -m b3_patterns sync --tickers-file lista.md --window-days 1500 --max-workers 8
```

Resultado em `price_history`:

- Linhas: `114741`
- Data inicial: `2014-08-13`
- Data final: `2026-04-13`
- Tickers: `83`

### COTAHIST B3

O R3 usa `spot_quote_history`, nao `price_history`. A primeira tentativa de R3 retornou zero ocorrencias porque `spot_quote_history` estava vazia.

Foi importado COTAHIST para 2025 e 2026:

```bash
.venv/bin/python -m b3_patterns options-sync --tickers-file lista.md --years 2025 2026
```

Estado final do COTAHIST:

- `spot_quote_history`: `24704` linhas, de `2025-01-02` ate `2026-04-13`, `83` tickers
- `option_quote_history`: importado para `77` raizes de opcoes
- Ultimo pregao disponivel no COTAHIST: `2026-04-13`
- Ainda nao havia candle COTAHIST de `2026-04-14` no momento deste registro

## Rodada R3 inicial - 500 pares promovidos / 5M acumuladores

Comando usado:

```bash
.venv/bin/python -m b3_patterns asset-discover-r3 \
  --db-path b3_history.db \
  --tickers-file lista.md \
  --start-date 2025-01-01 \
  --end-date 2026-04-10 \
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
  --include-known \
  2>&1 | tee reports/r3-full-2025-2026.log
```

Resultado:

- Padroes brutos: `5692934`
- Aprovados no discovery: `3504`
- Trades coletados: `1066417`
- Pos-processamento:
  - Estrategias finais: `2820`
  - Shortlist: `1568`
  - Acoes finais: `75`

Filtro elite aplicado sobre a shortlist:

- `trades >= 200`
- `tickers >= 50`
- `take_profit_rate_pct >= 65`
- `profitable_rate_pct >= 70`
- `average_trade_return_pct >= 1.3`
- `profit_factor >= 2.5`
- `test_take_profit_rate_pct >= 70`
- `test_average_trade_return_pct >= 2.0`

Resultado do filtro elite inicial:

- Estrategias elite: `21`

## Rodada R3 mais forte - 1500 pares / 12M acumuladores

A primeira tentativa desta rodada foi iniciada fora de `tmux` e parou quando a sessao/terminal caiu. Ela parou em:

```text
[R3:mine_3f] 30/82 (36.6%)
```

Depois foi criada sessao `tmux`:

```bash
tmux new -s r3
```

Comando rodado dentro do `tmux`:

```bash
cd /home/gain/gain
mkdir -p reports

.venv/bin/python -m b3_patterns asset-discover-r3 \
  --db-path b3_history.db \
  --tickers-file lista.md \
  --start-date 2025-01-01 \
  --end-date 2026-04-13 \
  --entry-rules open close \
  --trade-directions long short \
  --target-stop-pairs 3:3 3:4 4:4 \
  --atr-target-stop-pairs 1.5:1 2:1.5 \
  --atr-time-cap-days 5 10 \
  --time-cap-days 10 \
  --max-promoted-pairs 1500 \
  --max-accumulators 12000000 \
  --prune-every-tickers 50 \
  --progress-every-tickers 10 \
  --include-known \
  2>&1 | tee reports/r3-full-2025-2026-promoted1500.log
```

Resultado:

- Features: `50`
- Templates: `28`
  - Percentuais: `12`
  - ATR: `16`
- Janela: `2025-01-01` ate `2026-04-13`
- Tickers elegiveis: `82`
- Samples: `670100`
- Pares promovidos para 3f: `561`
- Limite de acumuladores 3f: `12000000`
- Poda observada: `815103`
- Trades coletados: `2937457`
- Padroes brutos: `12693172`
- Aprovados no discovery: `9720`
- Reprovados: `12683452`
- Tamanhos de padrao: `[1, 2, 3]`

Arquivos principais gerados:

- `reports/r3-full-2025-2026-promoted1500.log`
- `reports/asset-discovery-lista-r3-stat-approved.csv`
- `reports/asset-discovery-lista-r3-stat-trades-approved.csv`
- `reports/asset-discovery-lista-r3-stat-tickers-all.csv`
- `reports/asset-discovery-lista-r3-stat-tickers-qualified.csv`
- `reports/asset-discovery-lista-r3-stat-summary.md`

## Pos-processamento estatistico

Comando:

```bash
.venv/bin/python tools/generate_asset_r3_stat_report.py
```

Resultado da rodada mais forte:

- Estrategias finais: `7776`
- Linhas estrategia x acao finais: `11311`
- Acoes finais: `76`
- Shortlist: `3936`
- Linhas estrategia x acao na shortlist: `6128`
- Acoes na shortlist: `75`
- Relatorio: `reports/asset-discovery-lista-r3-stat-report.md`

Contagens dos CSVs uteis:

```text
9721    reports/asset-discovery-lista-r3-stat-approved.csv
2937458 reports/asset-discovery-lista-r3-stat-trades-approved.csv
7777    reports/asset-discovery-lista-r3-stat-final.csv
3937    reports/asset-discovery-lista-r3-stat-shortlist.csv
50      reports/asset-discovery-lista-r3-stat-elite-top49.csv
675164  reports/asset-discovery-lista-r3-stat-tickers-all.csv
9729    reports/asset-discovery-lista-r3-stat-tickers-qualified.csv
```

Observacao: os numeros acima incluem a linha de cabecalho.

## Elite atual

Foi aplicado o mesmo filtro elite forte sobre a nova shortlist.

Resultado:

- Estrategias elite: `49`
- Arquivo: `reports/asset-discovery-lista-r3-stat-elite-top49.csv`

Observacao: o arquivo elite foi renomeado de `top21` para `top49` apos a rodada mais forte encontrar `49` estrategias elite.

## Dashboard

O dashboard foi exportado usando o CSV elite atual:

```bash
.venv/bin/python -m b3_patterns asset-monitor-export \
  --db-path b3_history.db \
  --tickers-file lista.md \
  --strategies-csv reports/asset-discovery-lista-r3-stat-elite-top49.csv \
  --top-strategies 0 \
  --output-json monitor-web/public/data/asset-monitor.json
```

Status do JSON:

- Arquivo: `monitor-web/public/data/asset-monitor.json`
- Gerado em: `2026-04-14`
- Ultimo pregao disponivel: `2026-04-13`
- Estrategias disponiveis: `49`
- Estrategias monitoradas: `49`
- Sinais brutos acionados: `44`
- Regra operacional atual: confluencia de pelo menos `3` estrategias elite no mesmo ticker
- Tickers operacionais por confluencia: `5`
- Sinais dentro de tickers operacionais: `22`
- Sinais com filtro individual ticker x estrategia: `0`
- Tickers acionados: `22`

Top tickers acionados no ultimo pregao disponivel:

- `LREN3`: `6`
- `CURY3`: `5`
- `VAMO3`: `5`
- `BBSE3`: `3`
- `MGLU3`: `3`
- `BBDC4`: `2`
- `CSAN3`: `2`
- `IRBR3`: `2`
- `MRVE3`: `2`
- `VALE3`: `2`
- Outros com 1 sinal: `ALOS3`, `CPFE3`, `CYRE3`, `EQTL3`, `IGTI11`, `ISAE4`, `KLBN11`, `RADL3`, `RDOR3`, `SMFT3`

Interpretacao operacional atual:

- Houve `44` acionamentos das estrategias elite no ultimo pregao disponivel.
- Para reduzir excesso de sinais, a regra operacional passou a exigir confluencia: o ticker precisa ter pelo menos `3` estrategias elite acionadas no mesmo pregao.
- Com essa regra, em `2026-04-13` ficaram `5` tickers operacionais: `LREN3`, `CURY3`, `VAMO3`, `MGLU3`, `BBSE3`.
- O filtro individual ticker x estrategia continua existindo no JSON como informacao secundaria, mas nao bloqueia mais a leitura operacional principal.

## Arquivos removidos

Foram removidos arquivos gigantes recriaveis para recuperar espaco em disco:

- `reports/asset-discovery-lista-r3-stat-rejected.csv`
- `reports/asset-discovery-lista-r3-stat-registry.csv`
- `reports/asset-discovery-lista-r3-stat-memory.md`

Motivo:

- `rejected.csv` tinha cerca de `9.1G`
- `registry.csv` tinha cerca de `6.4G`
- `memory.md` tinha cerca de `5.1G`
- Apos a rodada, o disco chegou a ficar critico com apenas cerca de `2.4G` livres
- Esses arquivos nao eram necessarios para o dashboard nem para o filtro elite final

Arquivos mantidos:

- Aprovados
- Trades aprovados
- Relatorios final/shortlist
- Elite
- Resumos por acao
- Logs
- JSON do dashboard

## Snapshot versionado para outras maquinas

Como a VPS de 128 GiB e temporaria e outras maquinas podem nao suportar rodar a mineracao pesada, foi criado um snapshot pequeno e versionavel com os resultados essenciais:

- `snapshots/r3-elite-2026-04-13/asset-monitor.json`
- `snapshots/r3-elite-2026-04-13/asset-discovery-lista-r3-stat-elite-top49.csv`
- `snapshots/r3-elite-2026-04-13/asset-discovery-lista-r3-stat-report.md`
- `snapshots/r3-elite-2026-04-13/operational-tickers-2026-04-13.csv`
- `snapshots/r3-elite-2026-04-13/README.md`

Para instalar esse resultado em outra maquina depois do clone:

```bash
tools/install_r3_snapshot.sh
```

Isso permite abrir o dashboard com o resultado top49 sem baixar COTAHIST, sem recriar `b3_history.db` e sem rodar R3.

## Espaco em disco apos limpeza

Estado apos limpeza:

- Disco `/dev/root`: `29G`
- Usado: `5.2G`
- Livre: `23G`
- Uso: `19%`
- Pasta `reports/`: `1.5G`
- Pasta `data/`: `116M`
- Banco `b3_history.db`: `479M`
- JSON do dashboard: `180K`

## Estado tmux

A sessao `tmux` foi usada para a rodada final:

```bash
tmux new -s r3
tmux attach -t r3
```

No momento do registro, a rodada ja tinha terminado, mas a sessao ainda existia:

```text
r3: 1 windows
```

Para entrar:

```bash
tmux attach -t r3
```

Para sair sem matar processos:

```text
Ctrl+b
d
```

## Comandos uteis

Ver status do dashboard JSON:

```bash
cd /home/gain/gain
.venv/bin/python - <<'PY'
import json
from pathlib import Path
data = json.loads(Path('monitor-web/public/data/asset-monitor.json').read_text())
for key in [
    'generated_at',
    'latest_trade_date',
    'strategies_available',
    'strategies_monitored',
    'signals_triggered',
    'signals_triggered_qualified',
    'triggered_tickers',
]:
    print(key, data.get(key))
print('top_triggered_tickers', [(x['ticker'], x['signals']) for x in data.get('top_triggered_tickers', [])])
PY
```

Rodar dashboard:

```bash
cd /home/gain/gain/monitor-web
npm run dev -- --host 0.0.0.0 --port 4173
```

Checar disco:

```bash
df -h /home/gain/gain
du -sh reports data b3_history.db
```

Checar processos R3:

```bash
ps -ef | rg "asset-discover-r3|r3-full"
```

Checar log da rodada:

```bash
tail -f /home/gain/gain/reports/r3-full-2025-2026-promoted1500.log
```

## Observacoes tecnicas

- O R3 usa dados de `spot_quote_history`, preenchidos pelo COTAHIST.
- O sync Yahoo preenche `price_history`, mas isso sozinho nao alimenta o R3.
- O ultimo COTAHIST importado nesta sessao chegou ate `2026-04-13`.
- Em 2026-04-14, ainda nao havia candle COTAHIST de 2026-04-14.
- A rodada mais forte usou cerca de dezenas de GB de RAM durante `mine_3f`, mas coube bem na VPS de 125 GiB.
- O gargalo observado foi CPU single-core: o processo ficava perto de `99.9%` de CPU, nao usava multiplos cores de forma ampla.
- Para rodadas futuras, usar sempre `tmux`.

## Proximos passos sugeridos

1. Avaliar manualmente as `49` estrategias elite atuais.
2. Se quiser ser mais seletivo, criar um novo filtro "super elite" com exigencias maiores e aceitar menos estrategias.
3. Atualizar COTAHIST diariamente antes de exportar o monitor.
4. Usar a confluencia operacional como filtro principal: considerar primeiro tickers com pelo menos `3` estrategias elite acionadas no mesmo pregao.
5. Futuramente adicionar proxies de agressao e volume profile aproximado diario como novos fatores.

## Atualizacao da regra de acionamento

A regra de "qualificado" por ticker x estrategia foi considerada restritiva demais para a leitura operacional principal. Ela exigia que o ticker especifico tivesse historico proprio forte dentro da mesma estrategia, o que reduziu os sinais qualificados para `0` mesmo quando estrategias elite acionavam.

Nova regra implementada:

- `Acionamento`: qualquer estrategia elite top49 que aparece no ticker no ultimo pregao disponivel.
- `Confluencia operacional`: ticker com pelo menos `3` acionamentos de estrategias elite no mesmo pregao.
- O dashboard agora destaca "Acoes com confluencia" em vez de "Acoes qualificadas".
- O filtro do radar detalhado agora e "Somente com confluencia".

Campos adicionados ao JSON:

- `operational_min_signals`
- `operational_tickers`
- `signals_triggered_operational`
- `top_operational_tickers`
- `signal.is_operational_ticker`
- `signal.ticker_signal_count`
- `strategy.operational_triggered_count`
