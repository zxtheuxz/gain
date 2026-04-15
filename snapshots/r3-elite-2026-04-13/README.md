# R3 Elite Snapshot - 2026-04-13

Snapshot pequeno e versionavel dos resultados da VPS de 128 GiB.

Este snapshot existe para maquinas que nao conseguem rodar a mineracao R3 pesada. Ele permite abrir o dashboard e consultar os resultados sem `b3_history.db`, sem `data/` e sem `reports/` completos.

## Conteudo

- `asset-monitor.json`: payload pronto para o dashboard React.
- `asset-discovery-lista-r3-stat-elite-top49.csv`: 49 estrategias elite.
- `asset-discovery-lista-r3-stat-report.md`: relatorio estatistico da rodada.
- `operational-tickers-2026-04-13.csv`: tickers com confluencia operacional no ultimo pregao do snapshot.

## Rodada de origem

- Janela: `2025-01-01` ate `2026-04-13`
- Universo: `lista.md`
- Templates: 28
- Features: 50
- `--max-promoted-pairs 1500`
- `--max-accumulators 12000000`
- Padroes brutos: `12693172`
- Aprovados no discovery: `9720`
- Trades coletados: `2937457`
- Estrategias finais: `7776`
- Shortlist: `3936`
- Elite: `49`

## Regra operacional do snapshot

- Acionamento: estrategia elite apareceu no ticker.
- Confluencia operacional: ticker com pelo menos `3` estrategias elite acionadas no mesmo pregao.

No ultimo pregao disponivel (`2026-04-13`):

- Acionamentos totais: `44`
- Tickers acionados: `22`
- Tickers com confluencia: `5`

Tickers com confluencia:

- `LREN3`: 6 sinais
- `CURY3`: 5 sinais
- `VAMO3`: 5 sinais
- `MGLU3`: 3 sinais
- `BBSE3`: 3 sinais

## Instalar snapshot em outra maquina

Na raiz do repo:

```bash
tools/install_r3_snapshot.sh
```

Depois:

```bash
cd monitor-web
npm install
npm run dev -- --host 0.0.0.0 --port 4173
```

