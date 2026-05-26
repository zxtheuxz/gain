# R10 - Discovery com Walk-Forward + Bootstrap + Janela por Ticker

R10 e a evolucao do R9 com 3 melhorias estatisticas adicionais identificadas
apos a auditoria do R9:

## Lessoes do R9 que motivaram o R10

1. **"100% win in 100 trades" e suspeito** — VAMO3 #1 teve 86/86 wins em
   mining + 14/14 em val, mas regrediu para 4/6 (66.7%) no OOS. Sem
   filtro adicional, esses ruidosos passam.
2. **Daytrade puro nao tem edge defensavel** — confirmado no R9 (2 setups
   so, ambos quebraram no OOS).
3. **Tickers com pouco historico distorcem comparacao** — VAMO3 (desde
   2021) entrou em pe de igualdade com tickers de 8 anos.

## O que R10 muda vs R9

| Item | R9 | R10 |
|---|---|---|
| Trilhas | fast + swing | **so swing** (fast removido) |
| Validacao temporal | mining/val/oos (3 janelas) | + **walk-forward em 3 sub-janelas de mining** |
| Min val_trades | 10 | **15** |
| Gate bootstrap | calculado, nao usado | **gate obrigatorio: CI95 lower > 0%** |
| Janela mining | global fixa | **ajustada por ticker** (IPO recente usa o que tem) |
| Min mining bars | 220 totais | **220 dentro da janela mining** |

## Janelas temporais

```
2018-01-01 -- M1 -- 2020-12-31 -- M2 -- 2022-12-31 -- M3 -- 2024-12-31  (Mining)
                                                                  2025-01-01 -- VAL -- 2025-12-31
                                                                                          2026-01-01 -- OOS PURO -- 2026-05-22
```

- **M1 (3 anos):** 2018-2020
- **M2 (2 anos):** 2021-2022
- **M3 (2 anos):** 2023-2024
- **VAL (12 meses):** 2025 (filtro estatistico)
- **OOS PURO (5 meses):** 2026 jan-mai (jamais tocado, so reporta)

## Gates (TODOS obrigatorios)

### Gate A - Descoberta (mining full)

| Criterio | Valor |
|---|---|
| Min trades | 30 |
| Profit factor | >= 3 |
| Win rate | >= 75% |
| Wilson CI95 lower bound | >= 70% |
| Binomial p-value | < 0.001 |

### Gate WF - Walk-Forward (nova exigencia)

Para cada sub-janela (M1, M2, M3) em que o ticker tenha >= 3 trades:
- Win rate >= 70% **na propria sub-janela**

Regra de "existencia":
- Tickers com historia completa (8 anos): tem que passar nas 3 janelas
- Tickers com IPO em 2021+: tem que passar nas 2 que cobrem sua existencia
- Tickers que existem em apenas 1 janela: **descartados** (sem capacidade
  de walk-forward)

### Gate B - Validacao (val 2025)

| Criterio | Valor |
|---|---|
| Min trades | **15** (era 10) |
| Win rate | >= 75% |
| Avg return | >= 4% |
| Binomial p-value | < 0.05 |

### Gate C - Correcao multiplos testes

Benjamini-Hochberg FDR q-value < 0.05.

### Gate Bootstrap - Robustez do retorno (nova exigencia)

Bootstrap percentile CI95 lower bound do **avg return mining** > 0%.

Garante que o retorno medio do setup e estatisticamente positivo com
95% de confianca - nao depende so de win rate alto.

### OOS - Informativo, nao filtra

Performance em 2026-01-01 a 2026-05-22 reportada para cada setup
aprovado, mas nao usada como filtro.

## Como rodar

```bash
chmod +x R10/run_r10_parallel.sh
tmux new-session -d -s r10 "bash R10/run_r10_parallel.sh"
tmux attach -t r10   # ctrl+b d para destacar
```

Tempo esperado: ~30-45 min em 32 vCPU (24 workers + bootstrap).

## Outputs em `reports/r10-<RUN_ID>/`

```
r10.log                       # log completo
r10-mining.csv                # candidatos brutos pos-Gate A (todos os fatores)
r10-validated.csv             # passou Gates A + WF + B + C + Bootstrap
r10-validated-slim.csv        # mesmo, sem colunas de retornos brutos
r10-final.csv                 # top 3 por ticker (portfolio operacional)
r10-signals-ready.csv         # idem
r10-report.md                 # top 30 humano com WF + bootstrap
r10-oos-report.md             # performance honesta no OOS
```

Cada linha do `r10-final.csv` traz alem das colunas R9:

- `m1_trades`, `m1_win_pct`, `m1_avg_return_pct` (sub-janela 1)
- `m2_trades`, `m2_win_pct`, `m2_avg_return_pct` (sub-janela 2)
- `m3_trades`, `m3_win_pct`, `m3_avg_return_pct` (sub-janela 3)
- `walk_forward_status` (e.g., "PASSOU 3/3 (95%/91%/88%)")
- `bootstrap_avg_return_lower_pct` (CI95 lower bound do retorno mining)
- `bootstrap_avg_return_upper_pct` (CI95 upper bound)

## Expectativa realista

| Metrica | R9 | R10 esperado |
|---|---:|---:|
| Mining candidatos brutos | 1.91M | ~1-2M |
| Validados (todos os gates) | 33k | **5-15k** (walk-forward elimina muitos) |
| Setups finais (top 3 por ticker) | 231 | **50-150** |
| Tickers cobertos | 77 | **40-65** |

Menos setups, cada um defensavel em 3 regimes diferentes de mercado.

## Variaveis de ambiente

| Variavel | Default | Descricao |
|---|---|---|
| `RUN_ID` | timestamp UTC | Sufixo da pasta de saida |
| `OUT_DIR` | `reports/r10-<RUN_ID>` | Pasta de saida |
| `WORKERS` | 24 | Processos paralelos |
| `MINING_START` | 2018-01-01 | Inicio do mining |
| `MW1_END` | 2020-12-31 | Fim da sub-janela 1 |
| `MW2_END` | 2022-12-31 | Fim da sub-janela 2 |
| `MINING_END` | 2024-12-31 | Fim do mining (= fim da m3) |
| `VAL_END` | 2025-12-31 | Fim da validacao |
| `OOS_END` | 2026-05-22 | Fim do OOS |
| `MIN_VAL_TRADES` | 15 | Min trades na validacao (era 10 no R9) |
| `WF_MIN_WIN` | 70 | Min win rate em cada sub-janela do walk-forward |
| `MIN_BOOTSTRAP_LOWER` | 0.0 | CI95 lower bound do avg return tem que ser > este valor |
| `MAX_FACTORS` | 3 | Maximo de fatores (3 evita overfit do 4) |
| `MAX_ACCUMULATORS` | 30000000 | Limite de combos em memoria por worker |
| `MIN_MINING_BARS` | 220 | Min bars dentro da janela mining para ticker entrar |

Sobreescrever exemplo:

```bash
# Walk-forward mais permissivo (win 65% em vez de 70%)
WF_MIN_WIN=65 bash R10/run_r10_parallel.sh

# Bootstrap mais rigoroso (avg return tem que ter CI lower > 1%)
MIN_BOOTSTRAP_LOWER=1.0 bash R10/run_r10_parallel.sh
```

## Limitacoes

- Nao garante resultado futuro. Mercado muda, regimes mudam.
- Walk-forward reduz overfitting mas nao elimina completamente.
- Recomendado: paper trading 1-3 meses antes de risco real.
