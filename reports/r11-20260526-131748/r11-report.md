# R11 Portfolio Daytrade Validado (3 familias + Walk-Forward + Bootstrap)

## Familias

- **Familia 1 (intraday)**: target/stop em 1 pregao (alvos 1-2%, stops 0.5-2%)
- **Familia 2 (mini-swing)**: target/stop em 2-3 pregoes (alvos 2-3%, stops 1-2.5%)
- **Familia 3 (fixed)**: saida fixa, alvo livre (sucesso = retorno > 1%)

## Janelas

- Mining: 2018-01-01 a 2024-12-31
  - Sub-janela 1: <= 2020-12-31
  - Sub-janela 2: 2020-12-31 a 2022-12-31
  - Sub-janela 3: 2022-12-31 a 2024-12-31
- Validacao: 2025-01-01 a 2025-12-31
- Out-of-sample: 2026-01-01 a 2026-05-25

## Gates aplicados (calibrados pra daytrade)

- **Gate A (mining)**: trades >= 30, win >= 60.0%, PF >= 1.8, Wilson CI95 lower >= 55.0%, binomial p < 0.01
- **Gate WF**: win >= 60.0% em cada sub-janela com >= 3 trades (min 2 de 3)
- **Gate B (val)**: trades >= 10, win >= 60.0%, avg return >= 0.5%, binomial p < 0.05
- **Gate C (FDR)**: q-value < 0.05
- **Gate Bootstrap**: CI95 lower bound do avg return mining > 0.0%

## Resumo

- Setups que passaram TODOS os gates: **14173**
- Tickers cobertos: 77
- Final (top por ticker): 230

## Top 30 (ranking final)

| Rank | Ticker | Template | M trades | M win | WF | V trades | V win | OOS trades | OOS win | OOS avg | Wilson | Boot lower | Setup |
|---:|---|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | `BBSE3` | `open_tp1_0_sl2_0_cap1` | 32 | 100.0% | 100%/100%/100% | 10 | 100.0% | 0 | 0.0% | 0.00% | 89.3% | 1.00% | Fechamento vs MM50: acima forte; Dia da semana: quarta-feira; Retorno acumulado 10D: alta de 1% a 3% |
| 2 | `BRAV3` | `close_tp3_0_sl2_5_cap3` | 30 | 93.3% | 100%/100%/83% | 10 | 100.0% | 2 | 100.0% | 3.00% | 78.7% | 2.08% | Fechamento vs EMA9: acima forte; Queda desde maxima 20D: 3% a 6% abaixo da maxima; Distancia desde minima 5D: 15%+ acima da minima |
| 3 | `RDOR3` | `close_tp2_0_sl2_0_cap3` | 36 | 94.4% | —/100%/90% | 10 | 100.0% | 2 | 100.0% | 2.00% | 81.9% | 1.44% | Posicao nas Bandas de Bollinger 20D: fechou perto da maxima; Retorno 3D em z-score 20D: acima do normal; Distancia desde minima 10D: 10% a 15% acima da minima |
| 4 | `CSMG3` | `open_tp2_0_sl2_0_cap3` | 62 | 90.3% | —/92%/88% | 10 | 100.0% | 0 | 0.0% | 0.00% | 80.5% | 1.29% | Mes do ano (trimestre): primeiro trimestre; MM20 vs MM50: abaixo leve; Volatilidade 5D vs 20D: muito baixo |
| 5 | `CXSE3` | `open_tp1_0_sl2_0_cap1` | 44 | 95.5% | —/100%/94% | 10 | 100.0% | 10 | 100.0% | 1.00% | 84.9% | 0.66% | Posicao nas Bandas de Bollinger 20D: fechou perto da maxima; Compressao 3D vs 20D: alto; MM20 vs MM50: perto do equilibrio |
| 6 | `CSMG3` | `open_tp2_0_sl2_0_cap3` | 44 | 90.9% | —/92%/87% | 10 | 100.0% | 0 | 0.0% | 0.00% | 78.8% | 1.27% | Compressao 3D vs 20D: baixo; Mes do ano (trimestre): primeiro trimestre; MM20 vs MM50: abaixo leve |
| 7 | `BEEF3` | `close_tp2_0_sl2_0_cap3` | 30 | 93.3% | 100%/—/75% | 10 | 100.0% | 2 | 0.0% | -2.00% | 78.7% | 1.33% | Fechamento vs EMA200: acima forte; Fechamento vs MM200: muito acima; Dias positivos nos ultimos 5 pregoes: 4 a 5 dias positivos em 5D |
| 8 | `BBDC3` | `open_tp1_0_sl2_0_cap1` | 40 | 95.0% | 100%/94%/— | 10 | 100.0% | 0 | 0.0% | 0.00% | 83.5% | 0.62% | Fechamento vs EMA200: acima leve; Fechamento vs MM50: acima leve; MM20 vs MM50: acima leve |
| 9 | `CXSE3` | `open_tp1_0_sl2_0_cap1` | 40 | 95.0% | —/85%/100% | 10 | 100.0% | 4 | 100.0% | 1.00% | 83.5% | 0.62% | Posicao do fechamento no candle: fechou no meio; Fechamento vs EMA9: acima leve; MM20 vs MM50: perto do equilibrio |
| 10 | `PSSA3` | `close_tp1_0_sl2_0_cap1` | 40 | 95.0% | 100%/100%/85% | 10 | 100.0% | 0 | 0.0% | 0.00% | 83.5% | 0.62% | Fechamento vs EMA21: abaixo leve; Compressao 3D vs 20D: alto; Pavio superior do candle: baixo |
| 11 | `CSMG3` | `open_tp2_0_sl2_0_cap3` | 42 | 90.5% | —/100%/75% | 12 | 100.0% | 0 | 0.0% | 0.00% | 77.9% | 1.24% | Mes do ano (trimestre): primeiro trimestre; Retorno 5D em z-score 20D: muito acima do normal; MM20 vs MM50: abaixo leve |
| 12 | `CMIN3` | `close_tp3_0_sl2_5_cap3` | 34 | 88.2% | —/80%/91% | 10 | 100.0% | 2 | 0.0% | -2.50% | 73.4% | 1.71% | Posicao no range 10D: fechou na metade superior; Volume vs media 20D: normal; Volume 5D vs 20D: normal |
| 13 | `KLBN11` | `close_tp1_0_sl2_0_cap1` | 36 | 94.4% | 85%/100%/100% | 12 | 100.0% | 4 | 100.0% | 1.00% | 81.9% | 0.89% | Posicao do fechamento no candle: fechou perto da minima; Retorno acumulado 1D: neutro; Retorno 3D em z-score 20D: abaixo do normal |
| 14 | `PETR3` | `open_tp1_0_sl2_0_cap1` | 36 | 94.4% | 100%/100%/85% | 10 | 100.0% | 0 | 0.0% | 0.00% | 81.9% | 0.94% | Fechamento vs EMA21: perto do equilibrio; Retorno 5D em z-score 20D: muito acima do normal; Posicao no range 10D: fechou na metade superior |
| 15 | `SANB11` | `open_tp1_0_sl2_0_cap1` | 38 | 94.7% | 100%/90%/100% | 10 | 100.0% | 0 | 0.0% | 0.00% | 82.7% | 0.61% | Fechamento vs MM200: acima leve; Mes do ano (trimestre): terceiro trimestre; Posicao no range 5D: fechou no meio |
| 16 | `SBSP3` | `open_tp1_0_sl2_0_cap1` | 38 | 94.7% | 100%/100%/93% | 10 | 100.0% | 2 | 100.0% | 1.00% | 82.7% | 0.61% | Fechamento vs EMA200: acima forte; Posicao no range 10D: fechou no meio; Pavio superior do candle: muito baixo |
| 17 | `BBDC3` | `open_tp2_0_sl2_0_cap3` | 40 | 90.0% | 100%/88%/— | 10 | 100.0% | 0 | 0.0% | 0.00% | 76.9% | 1.43% | Fechamento vs EMA200: acima leve; Fechamento vs MM50: acima leve; MM20 vs MM50: acima leve |
| 18 | `ABEV3` | `open_tp1_0_sl2_0_cap1` | 36 | 94.4% | 87%/100%/100% | 10 | 100.0% | 4 | 50.0% | 0.36% | 81.9% | 0.74% | Dias positivos nos ultimos 5 pregoes: 0 a 1 dia positivo em 5D; RSI 14 periodos: RSI baixo; MM20 vs MM50: perto do equilibrio |
| 19 | `PETR3` | `open_tp1_0_sl2_0_cap1` | 36 | 94.4% | 100%/80%/100% | 12 | 100.0% | 0 | 0.0% | 0.00% | 81.9% | 0.58% | Fechamento vs EMA9: acima leve; Retorno acumulado 2D: neutro; MM20 vs MM50: abaixo leve |
| 20 | `ABEV3` | `open_tp1_0_sl2_0_cap1` | 50 | 92.0% | 88%/87%/100% | 10 | 100.0% | 0 | 0.0% | 0.00% | 81.2% | 0.52% | Queda desde maxima 10D: 3% a 6% abaixo da maxima; Gap vs ATR14: levemente abaixo; MM5 vs MM20: perto do equilibrio |
| 21 | `KLBN11` | `close_tp2_0_sl2_0_cap3` | 36 | 88.9% | 75%/92%/— | 10 | 100.0% | 0 | 0.0% | 0.00% | 74.7% | 1.68% | Fechamento vs EMA200: abaixo forte; Queda desde maxima 20D: 1% a 3% abaixo da maxima; MM20 vs MM50: abaixo leve |
| 22 | `BBAS3` | `open_tp1_0_sl2_0_cap1` | 32 | 93.8% | 100%/100%/85% | 10 | 100.0% | 0 | 0.0% | 0.00% | 79.9% | 0.93% | Dia da semana: sexta-feira; Retorno acumulado 2D: alta de 1% a 3%; Retorno acumulado 3D: neutro |
| 23 | `KLBN11` | `close_tp1_0_sl2_0_cap1` | 48 | 91.7% | 100%/100%/80% | 12 | 100.0% | 4 | 50.0% | -0.11% | 80.4% | 0.57% | Posicao do fechamento no candle: fechou perto da minima; Retorno 1D em z-score 20D: levemente abaixo; Range vs media 20D: muito baixo |
| 24 | `ABEV3` | `open_tp1_0_sl2_0_cap1` | 48 | 91.7% | 100%/100%/80% | 10 | 100.0% | 2 | 0.0% | -1.18% | 80.4% | 0.58% | Gap do candle de referencia preenchido: gap de baixa preenchido; Retorno acumulado 20D: queda de 3% a 5%; Inclinacao da MM20 em 5D: perto do equilibrio |
| 25 | `MULT3` | `open_tp1_0_sl2_0_cap1` | 32 | 93.8% | 100%/—/83% | 12 | 100.0% | 2 | 100.0% | 1.00% | 79.9% | 0.70% | Gap vs ATR14: levemente abaixo; Posicao no range 10D: fechou na metade superior; MM5 vs MM20: acima leve |
| 26 | `RAIL3` | `open_tp3_0_sl2_0_cap3` | 30 | 86.7% | 75%/90%/— | 12 | 100.0% | 2 | 100.0% | 3.00% | 70.3% | 1.67% | Fechamento vs EMA200: muito abaixo; Fechamento vs EMA21: perto do equilibrio; Spike de volume vs maximo 5D: muito baixo |
| 27 | `CMIG4` | `close_tp2_0_sl1_5_cap3` | 36 | 88.9% | 100%/100%/71% | 12 | 100.0% | 4 | 100.0% | 2.00% | 74.7% | 1.22% | Dia do mes (bucket): fim do mes; Volume financeiro vs media 20D: muito baixo; Dias positivos nos ultimos 10 pregoes: 6 a 7 dias positivos em 10D |
| 28 | `GGBR4` | `open_tp2_0_sl2_0_cap2` | 36 | 88.9% | 60%/100%/100% | 14 | 100.0% | 0 | 0.0% | 0.00% | 74.7% | 1.11% | Fechamento vs EMA200: acima forte; Retorno acumulado 3D: neutro; Distancia desde minima 5D: 1% a 3% acima da minima |
| 29 | `GGBR4` | `open_tp2_0_sl2_0_cap3` | 36 | 88.9% | 60%/100%/100% | 14 | 100.0% | 0 | 0.0% | 0.00% | 74.7% | 1.11% | Fechamento vs EMA200: acima forte; Retorno acumulado 3D: neutro; Distancia desde minima 5D: 1% a 3% acima da minima |
| 30 | `BBSE3` | `open_tp1_0_sl1_5_cap1` | 32 | 93.8% | 100%/66%/100% | 10 | 100.0% | 0 | 0.0% | 0.00% | 79.9% | 0.61% | Fechamento vs MM50: acima forte; Dia da semana: quarta-feira; Retorno acumulado 10D: alta de 1% a 3% |
