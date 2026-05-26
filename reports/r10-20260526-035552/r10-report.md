# R10 Portfolio Validado (Walk-Forward + Bootstrap)

## Janelas

- Mining: 2018-01-01 a 2024-12-31
  - Sub-janela 1: <= 2020-12-31
  - Sub-janela 2: 2020-12-31 a 2022-12-31
  - Sub-janela 3: 2022-12-31 a 2024-12-31
- Validacao: 2025-01-01 a 2025-12-31
- Out-of-sample (informativo): 2026-01-01 a 2026-05-25

## Gates aplicados (todos obrigatorios)

- **Gate A (mining):** trades >= 30, win >= 75.0%, PF >= 3.0, Wilson CI95 lower >= 70.0%, binomial p < 0.001
- **Gate WF (walk-forward):** win >= 70.0% em CADA sub-janela com >= 3 trades; tem que existir em >= 2 das 3 janelas
- **Gate B (val):** trades >= 15, win >= 75.0%, avg return >= 4.0%, binomial p < 0.05
- **Gate C (FDR):** Benjamini-Hochberg q-value < 0.05
- **Gate Bootstrap:** CI95 lower bound do avg return mining > 0.0%

## Resumo

- Setups que passaram TODOS os gates: **1863**
- Tickers cobertos: 58
- Final (top por ticker): 163

## Top 30 (ranking final)

| Rank | Ticker | Template | M trades | M win | WF (m1/m2/m3) | V trades | V win | OOS trades | OOS win | OOS avg | Wilson | Boot lower | FDR q | Setup |
|---:|---|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | `AXIA6` | `open_tp8_0_sl5_0_cap20` | 34 | 94.1% | 100%/100%/75% | 18 | 100.0% | 0 | 0.0% | 0.00% | 80.9% | 7.70% | 1.3e-06 | Fechamento vs MM200: perto do equilibrio; RSI 14 periodos: RSI neutro alto; MM5 vs MM20: acima leve |
| 2 | `PSSA3` | `close_tp6_0_sl4_0_cap20` | 40 | 100.0% | 100%/—/100% | 20 | 100.0% | 8 | 100.0% | 6.00% | 91.2% | 6.00% | 8.8e-09 | Fechamento vs EMA200: perto do equilibrio; Dia do mes (bucket): segunda metade; Queda desde maxima 20D: 6% a 10% abaixo da maxima |
| 3 | `AXIA6` | `open_tp8_0_sl4_0_cap20` | 30 | 93.3% | 100%/—/90% | 22 | 100.0% | 0 | 0.0% | 0.00% | 78.7% | 7.66% | 5.8e-06 | Fechamento vs MM200: perto do equilibrio; Fechamento vs MM50: acima leve; RSI 14 periodos: RSI neutro alto |
| 4 | `AXIA6` | `open_tp8_0_sl5_0_cap20` | 30 | 93.3% | 100%/—/90% | 22 | 100.0% | 0 | 0.0% | 0.00% | 78.7% | 7.66% | 5.8e-06 | Fechamento vs MM200: perto do equilibrio; Fechamento vs MM50: acima leve; RSI 14 periodos: RSI neutro alto |
| 5 | `AXIA6` | `open_tp6_0_sl4_0_cap20` | 30 | 100.0% | 100%/—/100% | 22 | 100.0% | 0 | 0.0% | 0.00% | 88.6% | 6.00% | 3.3e-07 | Fechamento vs MM200: perto do equilibrio; Fechamento vs MM50: acima leve; RSI 14 periodos: RSI neutro alto |
| 6 | `AXIA6` | `close_tp6_0_sl4_0_cap20` | 30 | 100.0% | 100%/—/100% | 22 | 100.0% | 0 | 0.0% | 0.00% | 88.6% | 6.00% | 3.3e-07 | Fechamento vs MM200: perto do equilibrio; Fechamento vs MM50: acima leve; RSI 14 periodos: RSI neutro alto |
| 7 | `CMIN3` | `open_tp8_0_sl4_0_cap20` | 34 | 94.1% | —/100%/93% | 16 | 100.0% | 0 | 0.0% | 0.00% | 80.9% | 6.24% | 1.3e-06 | Fechamento vs MM200: perto do equilibrio; Fechamento vs MM5: perto do equilibrio; Volatilidade 5D vs 20D: muito baixo |
| 8 | `CMIN3` | `open_tp8_0_sl5_0_cap20` | 34 | 94.1% | —/100%/93% | 16 | 100.0% | 0 | 0.0% | 0.00% | 80.9% | 6.09% | 1.3e-06 | Fechamento vs MM200: perto do equilibrio; Fechamento vs MM5: perto do equilibrio; Volatilidade 5D vs 20D: muito baixo |
| 9 | `AXIA3` | `open_tp8_0_sl5_0_cap20` | 30 | 93.3% | 80%/100%/— | 18 | 100.0% | 0 | 0.0% | 0.00% | 78.7% | 5.83% | 5.8e-06 | Fechamento vs MM200: acima leve; Mes do ano (trimestre): primeiro trimestre; MM5 vs MM20: acima leve |
| 10 | `PETR3` | `open_tp8_0_sl5_0_cap20` | 32 | 87.5% | 100%/80%/83% | 18 | 100.0% | 0 | 0.0% | 0.00% | 71.9% | 7.71% | 3.5e-05 | Fechamento vs EMA200: abaixo leve; Gap da abertura: neutro; MM20 vs MM50: abaixo leve |
| 11 | `MBRF3` | `close_tp10_0_sl5_0_cap15` | 44 | 90.9% | 90%/91%/— | 26 | 92.3% | 0 | 0.0% | 0.00% | 78.8% | 7.27% | 4.2e-07 | Mes do ano (trimestre): quarto trimestre; MM20 vs MM50: muito abaixo; Inclinacao da MM50 em 20D: muito abaixo |
| 12 | `MBRF3` | `close_tp10_0_sl5_0_cap20` | 44 | 90.9% | 90%/91%/— | 26 | 92.3% | 0 | 0.0% | 0.00% | 78.8% | 7.27% | 4.2e-07 | Mes do ano (trimestre): quarto trimestre; MM20 vs MM50: muito abaixo; Inclinacao da MM50 em 20D: muito abaixo |
| 13 | `CXSE3` | `open_tp4_0_sl4_0_cap20` | 52 | 100.0% | —/100%/100% | 22 | 100.0% | 0 | 0.0% | 0.00% | 93.1% | 4.00% | 1e-10 | Fechamento vs EMA21: perto do equilibrio; Mes do ano (trimestre): quarto trimestre; Distancia desde minima 5D: perto da minima |
| 14 | `AXIA6` | `close_tp8_0_sl4_0_cap20` | 30 | 86.7% | 100%/—/80% | 22 | 100.0% | 0 | 0.0% | 0.00% | 70.3% | 7.37% | 7.3e-05 | Fechamento vs MM200: perto do equilibrio; Fechamento vs MM50: acima leve; RSI 14 periodos: RSI neutro alto |
| 15 | `AXIA6` | `close_tp8_0_sl5_0_cap20` | 30 | 86.7% | 100%/—/80% | 22 | 100.0% | 0 | 0.0% | 0.00% | 70.3% | 7.37% | 7.3e-05 | Fechamento vs MM200: perto do equilibrio; Fechamento vs MM50: acima leve; RSI 14 periodos: RSI neutro alto |
| 16 | `ITSA4` | `close_tp4_0_sl4_0_cap20` | 46 | 100.0% | 100%/100%/100% | 20 | 100.0% | 0 | 0.0% | 0.00% | 92.3% | 4.00% | 9.2e-10 | Fechamento vs MM200: perto do equilibrio; RSI 14 periodos: RSI neutro alto; Spike de volume vs maximo 5D: muito baixo |
| 17 | `PSSA3` | `open_tp6_0_sl4_0_cap20` | 40 | 95.0% | 87%/—/100% | 20 | 100.0% | 10 | 100.0% | 6.00% | 83.5% | 4.75% | 1.4e-07 | Fechamento vs EMA200: perto do equilibrio; Dia do mes (bucket): segunda metade; Queda desde maxima 20D: 6% a 10% abaixo da maxima |
| 18 | `ITSA4` | `close_tp4_0_sl4_0_cap20` | 46 | 100.0% | —/100%/100% | 16 | 100.0% | 0 | 0.0% | 0.00% | 92.3% | 4.00% | 9.2e-10 | Fechamento vs MM200: perto do equilibrio; Queda desde maxima 20D: 1% a 3% abaixo da maxima; RSI 14 periodos: RSI neutro alto |
| 19 | `CXSE3` | `close_tp4_0_sl4_0_cap20` | 44 | 100.0% | —/100%/100% | 16 | 100.0% | 4 | 0.0% | 1.35% | 92.0% | 4.00% | 2e-09 | Dia do mes (bucket): primeira metade; MM20 vs MM50: perto do equilibrio; Volume 5D vs 20D: normal |
| 20 | `PSSA3` | `close_tp4_0_sl4_0_cap15` | 40 | 100.0% | 100%/—/100% | 20 | 100.0% | 8 | 100.0% | 4.00% | 91.2% | 4.00% | 8.8e-09 | Fechamento vs EMA200: perto do equilibrio; Dia do mes (bucket): segunda metade; Queda desde maxima 20D: 6% a 10% abaixo da maxima |
| 21 | `GGBR4` | `close_tp4_0_sl4_0_cap15` | 40 | 100.0% | 100%/100%/— | 18 | 100.0% | 0 | 0.0% | 0.00% | 91.2% | 4.00% | 8.8e-09 | Mes do ano (trimestre): quarto trimestre; Inclinacao da MM50 em 20D: acima forte; Volume vs media 20D: muito baixo |
| 22 | `GGBR4` | `close_tp4_0_sl4_0_cap20` | 40 | 100.0% | 100%/100%/— | 18 | 100.0% | 0 | 0.0% | 0.00% | 91.2% | 4.00% | 8.8e-09 | Mes do ano (trimestre): quarto trimestre; Inclinacao da MM50 em 20D: acima forte; Volume vs media 20D: muito baixo |
| 23 | `ITSA4` | `close_tp4_0_sl4_0_cap20` | 36 | 100.0% | 100%/100%/100% | 20 | 100.0% | 0 | 0.0% | 0.00% | 90.4% | 4.00% | 3.7e-08 | Fechamento vs MM200: perto do equilibrio; Compressao 3D vs 20D: normal; RSI 14 periodos: RSI neutro alto |
| 24 | `GGBR4` | `close_tp4_0_sl4_0_cap15` | 36 | 100.0% | 100%/100%/— | 20 | 100.0% | 0 | 0.0% | 0.00% | 90.4% | 4.00% | 3.7e-08 | Mes do ano (trimestre): quarto trimestre; Dias positivos nos ultimos 5 pregoes: 2 dias positivos em 5D; Inclinacao da MM50 em 20D: acima forte |
| 25 | `GGBR4` | `close_tp4_0_sl4_0_cap20` | 36 | 100.0% | 100%/100%/— | 20 | 100.0% | 0 | 0.0% | 0.00% | 90.4% | 4.00% | 3.7e-08 | Mes do ano (trimestre): quarto trimestre; Dias positivos nos ultimos 5 pregoes: 2 dias positivos em 5D; Inclinacao da MM50 em 20D: acima forte |
| 26 | `GGBR4` | `close_tp6_0_sl4_0_cap20` | 36 | 94.4% | 90%/100%/— | 20 | 100.0% | 0 | 0.0% | 0.00% | 81.9% | 4.61% | 5.8e-07 | Mes do ano (trimestre): quarto trimestre; Dias positivos nos ultimos 5 pregoes: 2 dias positivos em 5D; Inclinacao da MM50 em 20D: acima forte |
| 27 | `GOAU4` | `open_tp6_0_sl4_0_cap20` | 36 | 94.4% | 90%/100%/100% | 20 | 100.0% | 0 | 0.0% | 0.00% | 81.9% | 4.61% | 5.8e-07 | Fechamento vs MM200: muito abaixo; Queda desde maxima 10D: 3% a 6% abaixo da maxima; Retorno acumulado 1D: neutro |
| 28 | `MBRF3` | `close_tp4_0_sl4_0_cap10` | 36 | 100.0% | 100%/—/100% | 16 | 100.0% | 0 | 0.0% | 0.00% | 90.4% | 4.00% | 3.7e-08 | Mes do ano (trimestre): quarto trimestre; Distancia desde minima 10D: 15%+ acima da minima; MM5 vs MM20: acima forte |
| 29 | `MBRF3` | `close_tp4_0_sl4_0_cap15` | 36 | 100.0% | 100%/—/100% | 16 | 100.0% | 0 | 0.0% | 0.00% | 90.4% | 4.00% | 3.7e-08 | Mes do ano (trimestre): quarto trimestre; Distancia desde minima 10D: 15%+ acima da minima; MM5 vs MM20: acima forte |
| 30 | `MBRF3` | `close_tp4_0_sl4_0_cap20` | 36 | 100.0% | 100%/—/100% | 16 | 100.0% | 0 | 0.0% | 0.00% | 90.4% | 4.00% | 3.7e-08 | Mes do ano (trimestre): quarto trimestre; Distancia desde minima 10D: 15%+ acima da minima; MM5 vs MM20: acima forte |
