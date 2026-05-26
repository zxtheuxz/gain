# Entrega operacional v1 - day trade e swing

Esta pasta empacota os arquivos finais da rodada feita na VPS em 2026-05-19 para trocar o monitor antigo por dois livros operacionais:

- Day trade: entrada no open e saida no mesmo dia.
- Swing curto: entrada no open, alvos maiores e prazo maximo de 5 a 7 pregoes.

O objetivo desta entrega e permitir copiar apenas o necessario para outra VPS que ja tenha `gain`, `b3_history.db`, monitor e cron configurados.

## Contexto da rodada

A busca foi ajustada para priorizar precisao, nao quantidade de acionamentos. A unidade operacional escolhida foi:

`estrategia + acao`

Ou seja, uma estrategia so entra para uma acao se aquela acao tambem passou no historico individual.

Foram analisados resultados de:

- `1D`: day trade, sai no mesmo dia.
- `3D`: intermediario, usado para comparacao.
- `5D`: swing curto.
- `7D`: swing principal para alvos maiores.

O universo usado foi sempre `lista.md`.

## Arquivos nesta pasta

```text
reports/operational-books-v1/operational-books.md
reports/operational-books-v1/daytrade-combinado.csv
reports/operational-books-v1/swing-combinado.csv
reports/operational-books-v1/monitor-strategies.csv
reports/operational-books-v1/monitor-ticker-stats.csv
reports/operational-books-v1/asset-monitor-operational.json
tools/build_operational_books.py
```

### Arquivos principais

- `operational-books.md`: relatorio humano. Explica os livros, cobertura, melhores acoes e melhores setups.
- `daytrade-combinado.csv`: melhor sinal day trade por acao, priorizando alvo 2%, depois 1.5%, depois 1%.
- `swing-combinado.csv`: melhor sinal swing por acao, priorizando 7D alvo 7%, depois 7D alvo 5%, depois 7D alvo 4%, depois 5D alvo 4%-5%.
- `monitor-strategies.csv`: estrategias que o monitor deve acompanhar.
- `monitor-ticker-stats.csv`: pares `estrategia + acao` aprovados. Este arquivo e essencial para filtrar sinais realmente qualificados.
- `asset-monitor-operational.json`: exemplo de JSON gerado com os livros novos.
- `tools/build_operational_books.py`: script para regenerar estes arquivos se os CSVs sparse forem atualizados.

## Cobertura final

Na geracao feita em 2026-05-19:

- Universo detectado em `lista.md`: 82 tickers.
- Cobertos em algum livro final: 77 acoes.
- Sem estrategia final: `EMBJ3`, `AXIA7`, `SUZB3`, `AXIA6`, `RENT4`, `CYRE4`.

Por livro:

- `daytrade-principal`: 17 acoes, alvo 2%, cap 1D.
- `daytrade-cobertura-15`: 68 acoes, alvo 1.5%, cap 1D.
- `daytrade-cobertura-10`: 66 acoes, alvo 1%, cap 1D.
- `daytrade-combinado`: 77 acoes.
- `swing-raro-7d-7`: 2 acoes, alvo 7%, cap 7D.
- `swing-principal-7d-5`: 15 acoes, alvo 5%, cap 7D.
- `swing-cobertura-7d-4`: 47 acoes, alvo 4%, cap 7D.
- `swing-curto-5d-4-5`: 20 acoes, alvo 4%-5%, cap 5D.
- `swing-combinado`: 47 acoes.

## Recomendacao operacional

Usar duas abas ou dois blocos no monitor:

1. Day trade
   - Prioridade 1: `daytrade-principal`, alvo 2%, cap 1D.
   - Prioridade 2: `daytrade-cobertura-15`, alvo 1.5%, cap 1D.
   - Prioridade 3: `daytrade-cobertura-10`, alvo 1%, cap 1D.

2. Swing
   - Prioridade 1: `swing-raro-7d-7`, alvo 7%, cap 7D.
   - Prioridade 2: `swing-principal-7d-5`, alvo 5%, cap 7D.
   - Prioridade 3: `swing-cobertura-7d-4`, alvo 4%, cap 7D.
   - Prioridade 4: `swing-curto-5d-4-5`, alvo 4%-5%, cap 5D.

Para analise diaria, nao use todos os sinais brutos. Use os combinados:

- `daytrade-combinado.csv`
- `swing-combinado.csv`

Eles ja limitam para no maximo uma melhor linha por acao dentro de cada tipo.

## Como copiar para outra VPS

No servidor de origem:

```bash
scp -r entrega-operacional-v1 usuario@IP_DA_VPS:/home/userid/gain/
```

Na VPS de destino, dentro do repo `gain`:

```bash
cp -r entrega-operacional-v1/reports/operational-books-v1 reports/
cp entrega-operacional-v1/tools/build_operational_books.py tools/
```

Se o caminho da outra VPS for diferente, ajuste `/home/userid/gain`.

## Como gerar o JSON do monitor

Na VPS de destino:

```bash
.venv/bin/python -m b3_patterns asset-monitor-export \
  --db-path b3_history.db \
  --tickers-file lista.md \
  --strategies-csv reports/operational-books-v1/monitor-strategies.csv \
  --ticker-stats-csv reports/operational-books-v1/monitor-ticker-stats.csv \
  --top-strategies 0 \
  --output-json monitor-web/public/data/asset-monitor.json
```

Para testar sem sobrescrever o JSON do monitor atual:

```bash
.venv/bin/python -m b3_patterns asset-monitor-export \
  --db-path b3_history.db \
  --tickers-file lista.md \
  --strategies-csv reports/operational-books-v1/monitor-strategies.csv \
  --ticker-stats-csv reports/operational-books-v1/monitor-ticker-stats.csv \
  --top-strategies 0 \
  --output-json reports/operational-books-v1/asset-monitor-operational.json
```

Depois confira:

```bash
.venv/bin/python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path("monitor-web/public/data/asset-monitor.json").read_text())
print("generated_at", payload.get("generated_at"))
print("latest_trade_date", payload.get("latest_trade_date"))
print("strategies_monitored", payload.get("strategies_monitored"))
print("signals_triggered", payload.get("signals_triggered"))
print("signals_triggered_qualified", payload.get("signals_triggered_qualified"))
print("triggered_tickers", payload.get("triggered_tickers"))
print("operational_tickers", payload.get("operational_tickers"))
PY
```

## Como adaptar o cron

O cron antigo provavelmente chama algum script como `tools/update_monitor_daily.sh` ou `tools/sinais_proximo_pregao.sh`.

O ponto essencial e trocar os CSVs antigos por:

```bash
--strategies-csv reports/operational-books-v1/monitor-strategies.csv
--ticker-stats-csv reports/operational-books-v1/monitor-ticker-stats.csv
```

Exemplo de bloco para cron:

```bash
cd /home/userid/gain

.venv/bin/python -m b3_patterns sync --tickers-file lista.md

.venv/bin/python -m b3_patterns asset-monitor-export \
  --db-path b3_history.db \
  --tickers-file lista.md \
  --strategies-csv reports/operational-books-v1/monitor-strategies.csv \
  --ticker-stats-csv reports/operational-books-v1/monitor-ticker-stats.csv \
  --top-strategies 0 \
  --output-json monitor-web/public/data/asset-monitor.json
```

## Telegram

Ja existe integracao antiga com Telegram:

- `tools/telegram_send_signals.py`
- `tools/sinais_proximo_pregao.sh`
- secrets em `.claude/telegram-secrets.json`

Formato esperado do secrets:

```json
{
  "bot_token": "SEU_TOKEN",
  "chat_id": "SEU_CHAT_ID"
}
```

Nao copie secrets entre VPS se nao precisar. Cada VPS pode ter seu proprio `.claude/telegram-secrets.json`.

Importante: o Telegram antigo foi feito para o fluxo `operational-high-precision-41`. Para usar estes livros novos, o proximo passo e adaptar o `.sh` ou o script de Telegram para ler o JSON gerado com `monitor-strategies.csv` e separar a mensagem em:

- Day trade
- Swing

Tambem e importante enviar apenas sinais qualificados por `monitor-ticker-stats.csv`, isto e, sinais em que o par `estrategia + acao` foi aprovado.

## Como regenerar os livros

Se os CSVs sparse existirem na VPS, rode:

```bash
.venv/bin/python tools/build_operational_books.py
```

Por padrao, ele procura:

```text
reports/r5-sparse-v1-20m/cap-1d/r5-sparse-tickers.csv
reports/r5-sparse-v1-20m/cap-3d/r5-sparse-tickers.csv
reports/r5-sparse-v2-long-5d7d-2to10/cap-5d/r5-sparse-tickers.csv
reports/r5-sparse-v2-long-5d7d-2to10/cap-7d/r5-sparse-tickers.csv
```

Se esses arquivos nao existirem na outra VPS, nao tem problema para operar o monitor: os arquivos ja prontos em `reports/operational-books-v1/` sao suficientes.

## Como interpretar os campos principais

- `ticker`: acao.
- `book`: livro operacional.
- `target_pct`: alvo.
- `stop_pct`: stop.
- `time_cap_days`: prazo maximo.
- `trades`: quantidade historica daquele par `estrategia + acao`.
- `active_months`: meses em que houve acionamento.
- `take_profit_rate_pct`: percentual total em que bateu alvo.
- `test_take_profit_rate_pct`: percentual no periodo de teste em que bateu alvo.
- `average_trade_return_pct`: retorno medio historico.
- `profit_factor`: fator de lucro.
- `strategy_label`: explicacao humana do setup.
- `state_signature`: estados tecnicos que precisam estar presentes para acionar.

## Observacao importante

Estes arquivos organizam backtests e sinais estatisticos. Eles nao garantem resultado futuro. O uso operacional recomendado e tratar os sinais como candidatos diarios, com gestao de risco e verificacao manual antes de executar.
