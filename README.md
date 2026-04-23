# Sistema de Mineracao de Padroes B3

Implementacao inicial do projeto descrito em `decisao-stack-app-mobile.md`.

## O que esta pronto

- Carga de tickers a partir do universo operacional em `lista.md`
- Normalizacao automatica para o sufixo `.SA`
- Sincronizacao do historico local em SQLite (`b3_history.db`)
- Uso de `Adj Close` para a analise de gatilhos e desfechos
- Ranking por assertividade e retorno medio no console
- Exportacao opcional do ranking em CSV
- Grade de estrategias para reversao e continuacao em quedas e altas
- Backtest simplificado com log de operacoes e PnL por estrategia
- Estrategias intradiarias e de gap usando OHLC diario ajustado
- Importacao do COTAHIST da B3 para backtest de opcoes ATM
- Backtest de opcoes com memoria separada para aprovadas e reprovadas
- Discovery probabilistico de estados de mercado para opcoes ATM D0

## Estrutura

- `b3_patterns/cli.py`: CLI principal
- `b3_patterns/ingestion.py`: download do Yahoo Finance
- `b3_patterns/db.py`: schema e persistencia SQLite
- `b3_patterns/analysis.py`: motor de busca de padroes
- `b3_patterns/reporting.py`: saida em console e CSV
- `b3_patterns/cotahist.py`: importacao do historico oficial COTAHIST
- `b3_patterns/options.py`: motor de backtest de opcoes ATM
- `b3_patterns/discovery.py`: mineracao probabilistica de estados de mercado
- `tests/`: testes unitarios do nucleo local

## Instalacao

```bash
python -m pip install -r requirements.txt
```

## Uso

Sincronizar o banco local:

```bash
python -m b3_patterns sync --tickers-file lista.md
```

Rodar a analise apenas com dados locais:

```bash
python -m b3_patterns analyze --trigger-change-pct -2.0 --target-next-day-pct 0.5 --top 15
```

Varrer uma grade de estrategias:

```bash
python -m b3_patterns strategies --levels 1 2 3 4 5 --output-csv reports/strategies.csv --trades-csv reports/strategy-trades.csv --ticker-summary-csv reports/strategy-tickers.csv --ticker-qualified-csv reports/strategy-tickers-qualified.csv
```

Importar o COTAHIST da B3 para opcoes:

```bash
python -m b3_patterns options-sync --tickers-file lista.md --years 2025 2026
```

Rodar o backtest de opcoes ATM no ultimo ano:

```bash
python -m b3_patterns options-backtest
```

Minerar padroes probabilisticos para opcoes ATM D0:

```bash
python -m b3_patterns options-discover --dte-targets 7 15
```

Refinar a shortlist com baseline, estabilidade e validacao:

```bash
python -m b3_patterns options-discover-refine
```

Rodar a Rodada 1 do discovery quantitativo em acoes, com saidas percentuais:

```bash
python -m b3_patterns asset-discover-round1 --entry-rules open close --target-stop-pairs 1:1 2:1 3:1.5 4:2 6:3 --time-cap-days 5 --max-pattern-size 2
```

Exportar o JSON consumido pelo monitor React + Vite:

```bash
python -m b3_patterns asset-monitor-export
```

Por padrao, o monitor usa `lista.md` e acompanha as `10` primeiras estrategias da shortlist liquida. Para acompanhar todas:

```bash
python -m b3_patterns asset-monitor-export --top-strategies 0
```

Subir o dashboard web do monitor:

```bash
cd monitor-web
npm install
npm run dev
```

## Campanha VPS: 41 estrategias de alta precisao

Esta VPS foi usada para a parte pesada da campanha R3 com alvos fracionados entre `1.0%` e `1.4%`, usando bastante RAM para mineracao `2f -> 3f`. O objetivo final nao foi manter o bruto inteiro, e sim extrair uma cesta enxuta com foco em **alta taxa de acerto**.

### O que saiu desta campanha

- Base operacional final: [reports/operational-high-precision-41.csv](reports/operational-high-precision-41.csv)
- Resumo legivel da cesta: [reports/operational-high-precision-41.md](reports/operational-high-precision-41.md)
- Trades reconstruidos de abril/2026 para essa cesta: [reports/operational-high-precision-41-apr2026-trades.csv](reports/operational-high-precision-41-apr2026-trades.csv)
- Sinais encontrados em `2026-04-22`: [reports/operational-high-precision-41-signals-2026-04-22.csv](reports/operational-high-precision-41-signals-2026-04-22.csv)

### Como a cesta de 41 foi filtrada

O ponto de partida foi o bloco bruto aprovado em:

- [reports/cap5_t10_14-approved.csv](reports/cap5_t10_14-approved.csv)

Depois disso, o recorte de alta precisao ficou em:

- `occurrences >= 200`
- `tickers >= 50`
- `success >= 90%`
- `take profit >= 90%`
- `stop loss rate <= 8%`
- `profit factor >= 2.0`
- `avg trade >= 0.45%`

Arquivos intermediarios importantes:

- Filtro de alta precisao: [reports/cap5_t10_14-high-precision.csv](reports/cap5_t10_14-high-precision.csv)
- Resumo do filtro: [reports/cap5_t10_14-high-precision.md](reports/cap5_t10_14-high-precision.md)
- Script de corte do bloco bruto: [tools/filter_asset_r3_block_approved.py](tools/filter_asset_r3_block_approved.py)

### Como continuar em outra maquina

1. Clonar o repositorio e instalar o ambiente Python.
2. Recriar ou copiar o `b3_history.db`.
3. Sincronizar o banco com o universo operacional:

```bash
python -m b3_patterns sync --tickers-file lista.md
```

4. Trabalhar diretamente com a cesta final:

- [reports/operational-high-precision-41.csv](reports/operational-high-precision-41.csv)
- [reports/operational-high-precision-41.md](reports/operational-high-precision-41.md)

### Observacao importante sobre sinais atuais

O monitor legado em `asset_monitor.py` ainda le `spot_quote_history`. Se o banco tiver sido atualizado apenas com `sync`, a tabela mais fresca sera `price_history`. Para sinais atuais da cesta de 41, use os arquivos desta campanha ou recalcule em cima de `price_history`.

### Onde olhar para entender mais

- Runner pesado da campanha: [tools/run_asset_r3_fine_targets_tmux.sh](tools/run_asset_r3_fine_targets_tmux.sh)
- Coleta de trades em streaming: [b3_patterns/asset_discovery_round1.py](b3_patterns/asset_discovery_round1.py)
- Orquestracao R3: [b3_patterns/cli.py](b3_patterns/cli.py)
- Pos-processamento estatistico: [tools/generate_asset_r3_stat_report.py](tools/generate_asset_r3_stat_report.py)
- Filtro de alta acuracia: [tools/filter_asset_r3_high_accuracy.py](tools/filter_asset_r3_high_accuracy.py)
- Merge de CSVs por cabecalho: [tools/merge_csv_by_header.py](tools/merge_csv_by_header.py)

Manter o JSON do dashboard sendo reexportado em loop local:

```powershell
powershell -ExecutionPolicy Bypass -File tools\watch_asset_monitor.ps1 -IntervalSeconds 60 -TopStrategies 10
```

Executar o fluxo completo e exportar CSV:

```bash
python -m b3_patterns run --output-csv reports/ranking.csv
```

## Observacoes

- O comando `analyze` nao consulta a API. Ele le somente o SQLite local.
- O comando `strategies` testa dez setups por nivel: fechamento para reversao/continuacao, intradiario com saida no mesmo dia, intradiario com saida no fechamento seguinte e gaps de abertura.
- O backtest assume entrada no fechamento do Dia T e saida no fechamento do Dia T+1, usando posicao comprada para estrategias de alta esperada e vendida para estrategias de queda esperada.
- Nos setups intradiarios, a entrada e assumida no preco de gatilho teorico baseado no fechamento ajustado anterior. Nos gaps, a entrada e a abertura ajustada do dia.
- O resultado de `strategies` ja sai filtrado por padrao para remover estrategia sem qualidade financeira: acerto minimo de 55 por cento, profit factor minimo de 1, retorno medio minimo de 0,10 por cento e ao menos 200 trades.
- O relatorio por acao consolidado usa os trades das estrategias aprovadas e pode exportar tanto o bruto quanto o filtrado por qualidade no nivel do ticker.
- O comando `strategies` agora mantem memoria persistente em `reports/strategy-registry.csv` e `reports/strategy-memory.md`, marcando cada estrategia como `approved` ou `rejected`.
- Em execucoes futuras, estrategias ja registradas sao puladas por padrao para evitar repetir aprovadas e reprovadas. Use `--include-known` apenas se quiser reavaliar algo ja testado.
- As estrategias reprovadas podem ser exportadas em `reports/strategies-rejected.csv`.
- O fluxo de opcoes usa os arquivos oficiais COTAHIST da B3, gravando spot em `spot_quote_history` e opcoes em `option_quote_history` no mesmo SQLite local.
- O comando `options-backtest` agora testa 136 estrategias de compra de CALL e PUT ATM, cobrindo gatilhos de gap, fechamento, acumulado de `2D/3D/5D`, cruzamentos de medias moveis `5x20`, `10x20` e `20x50`, alem de grades com `take profit` de `1%` a `15%` e `stop loss` fixo de `3%` e `5%`.
- Nos setups com horizonte maior que `D0`, a saida e tratada por calendario: o motor busca o primeiro pregao disponivel em `D+N`, alinhando melhor o hold de `7`, `15` e `30` dias com o `DTE` da opcao.
- Nos setups de `take profit` e `stop loss`, a saida ocorre no primeiro toque do `high/low` diario da opcao. Se alvo e stop forem tocados no mesmo dia, o motor assume o `stop` primeiro, por conservadorismo.
- A memoria das estrategias de opcoes fica separada em `reports/options-strategy-registry.csv` e `reports/options-strategy-memory.md`.
- Os resultados de opcoes sao exportados por padrao em `reports/options-strategies-approved.csv`, `reports/options-strategies-rejected.csv`, `reports/options-trades-approved.csv`, `reports/options-tickers-all.csv` e `reports/options-tickers-approved.csv`.
- O comando `options-discover` testa um universo de 20 features de estado de mercado e minera combinacoes de 1 e 2 fatores para `CALL` e `PUT` ATM com entrada na abertura e saida no fechamento do mesmo dia.
- O discovery usa memoria separada em `reports/options-discovery-registry.csv` e `reports/options-discovery-memory.md`, para nao repetir padroes ja avaliados.
- O resumo executivo do lote atual sai em `reports/options-discovery-summary.md`.
- O comando `options-discover-refine` compara cada padrao com o baseline do template, exige validacao fora da amostra e remove duplicatas com alta sobreposicao.
- O comando `asset-discover-round1` inaugura o discovery `asset-first`: os padroes nascem da acao e a saida principal e por percentual, nao por dias fixos.
- A Rodada 1 trabalha com 28 fatores de preco, medias, volatilidade, range, volume e estrutura de candle, combinando `1` e `2` fatores por padrao.
- As entradas da Rodada 1 podem ser `open` e `close`, e as saidas usam pares `take profit / stop loss` com `time cap` apenas como limite de seguranca.
- Os resultados da Rodada 1 ficam separados em `reports/asset-discovery-round1-approved.csv`, `reports/asset-discovery-round1-rejected.csv`, `reports/asset-discovery-round1-trades-approved.csv`, `reports/asset-discovery-round1-tickers-all.csv`, `reports/asset-discovery-round1-tickers-qualified.csv`, `reports/asset-discovery-round1-registry.csv` e `reports/asset-discovery-round1-memory.md`.
- O comando `asset-monitor-export` le a shortlist final da rodada com `lista.md` e gera `monitor-web/public/data/asset-monitor.json`, arquivo consumido pelo dashboard React + Vite. Por padrao ele monitora as `10` primeiras estrategias finais; use `--top-strategies 0` para incluir todas.
- O app em `monitor-web/` e um monitor diario: ele mostra os sinais disparados no ultimo pregao disponivel da base local, com destaque para tickers que tambem sustentam o padrao no corte por acao.
- O script `tools/watch_asset_monitor.ps1` reexporta o JSON periodicamente para o app atualizar sozinho. Ele nao baixa cotacao em tempo real; ele so reflete o estado atual do banco local.
- O comando `run` sincroniza somente quando o banco estiver ausente, sem metadata ou desatualizado em relacao ao dia atual.
- A janela padrao persiste os ultimos 90 pregoes por ativo, cobrindo mais de 3 meses de historico.
