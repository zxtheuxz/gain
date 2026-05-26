# Livros operacionais 1D-7D

Objetivo: manter dois tipos de operacao para analise diaria sem carregar todos os sinais brutos.

## Recomendacao de uso

1. Day trade: priorizar `daytrade-principal` alvo 2%; usar `1.5%` e `1%` apenas para cobertura.
2. Swing: priorizar `swing-raro-7d-7` quando aparecer, depois `swing-principal-7d-5`, depois `swing-cobertura-7d-4`.
3. Usar no maximo uma linha por acao em cada livro combinado para deixar o painel analisavel.

## Cobertura

- Universo em lista.md: `82` tickers detectados
- Acoes cobertas em algum livro: `77`
- Sem estrategia nos livros finais: `EMBJ3, AXIA7, SUZB3, AXIA6, RENT4, CYRE4`

## Livros

| Livro | Acoes | Estrategias | Alvos | Trades mediana | Descricao |
| --- | ---: | ---: | --- | ---: | --- |
| `daytrade-principal` | 17 | 17 | 2% | 11 | Compra no open, alvo 2%, saida no mesmo dia. |
| `daytrade-cobertura-15` | 68 | 68 | 1.5% | 12 | Compra no open, alvo 1.5%, para aumentar cobertura de acoes. |
| `daytrade-cobertura-10` | 66 | 65 | 1% | 13 | Compra no open, alvo 1%, fallback para papeis sem alvo maior. |
| `swing-raro-7d-7` | 2 | 2 | 7% | 10 | Setup raro, alvo 7%, prazo maximo 7 pregoes. |
| `swing-principal-7d-5` | 15 | 14 | 5% | 12 | Livro principal de swing: alvo 5%, prazo maximo 7 pregoes. |
| `swing-cobertura-7d-4` | 47 | 45 | 4% | 15 | Livro de cobertura de swing: alvo 4%, prazo maximo 7 pregoes. |
| `swing-curto-5d-4-5` | 20 | 20 | 4%, 5% | 11 | Alternativa mais curta: alvos 4%-5%, prazo maximo 5 pregoes. |

## Day trade combinado - melhor por acao

| Rank | Livro | Ticker | Cap | Alvo | Trades | Meses | Alvo total | Alvo teste | Padrao |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | `daytrade-principal` | `AZZA3` | 1D | 2% | 14 | 6 | 92.86% | 100.00% | Discovery Acao: compra no open, alvo +2% / stop -4% / cap 1D / Retorno acumulado 1D: alta de 1% a 3%; MM20 vs MM50: abaixo forte; Inclinacao da MM20 em 5D: abaixo leve |
| 2 | `daytrade-principal` | `BRKM5` | 1D | 2% | 14 | 7 | 92.86% | 100.00% | Discovery Acao: compra no open, alvo +2% / stop -4% / cap 1D / Fechamento vs MM50: muito abaixo; Dia da semana: terca-feira |
| 3 | `daytrade-principal` | `HAPV3` | 1D | 2% | 14 | 5 | 92.86% | 100.00% | Discovery Acao: compra no open, alvo +2% / stop -4% / cap 1D / Retorno acumulado 10D: choque de baixa; MM20 vs MM50: muito abaixo; Dias positivos nos ultimos 5 pregoes: 0 a 1 dia positivo em 5D |
| 4 | `daytrade-principal` | `COGN3` | 1D | 2% | 13 | 9 | 92.31% | 100.00% | Discovery Acao: compra no open, alvo +2% / stop -4% / cap 1D / Retorno acumulado 5D: alta de 3% a 5%; Queda desde maxima 5D: 1% a 3% abaixo da maxima; Retorno 5D normalizado por ATR: levemente acima |
| 5 | `daytrade-principal` | `MGLU3` | 1D | 2% | 13 | 5 | 92.31% | 100.00% | Discovery Acao: compra no open, alvo +2% / stop -4% / cap 1D / MM5 vs MM20: abaixo forte; RSI 14 periodos: RSI neutro baixo; Dia da semana: terca-feira |
| 6 | `daytrade-principal` | `ASAI3` | 1D | 2% | 12 | 6 | 91.67% | 100.00% | Discovery Acao: compra no open, alvo +2% / stop -4% / cap 1D / Posicao no range 20D: fechou perto da maxima; RSI 14 periodos: RSI neutro alto; Fechamento vs EMA21: acima forte |
| 7 | `daytrade-principal` | `CSMG3` | 1D | 2% | 12 | 6 | 100.00% | 100.00% | Discovery Acao: compra no open, alvo +2% / stop -4% / cap 1D / Retorno acumulado 20D: choque de alta; Compressao 3D vs 20D: muito alto; Dias positivos nos ultimos 5 pregoes: 4 a 5 dias positivos em 5D |
| 8 | `daytrade-principal` | `RENT3` | 1D | 2% | 11 | 5 | 100.00% | 100.00% | Discovery Acao: compra no open, alvo +2% / stop -4% / cap 1D / Fechamento vs MM20: acima forte; Dias positivos nos ultimos 10 pregoes: 4 a 5 dias positivos em 10D; Fechamento vs EMA21: acima forte |
| 9 | `daytrade-principal` | `VAMO3` | 1D | 2% | 11 | 6 | 90.91% | 100.00% | Discovery Acao: compra no open, alvo +2% / stop -4% / cap 1D / Retorno acumulado 2D: alta de 5% a 8%; Dias positivos nos ultimos 10 pregoes: 4 a 5 dias positivos em 10D |
| 10 | `daytrade-principal` | `BEEF3` | 1D | 2% | 10 | 4 | 90.00% | 100.00% | Discovery Acao: compra no open, alvo +2% / stop -4% / cap 1D / Inclinacao da MM50 em 10D: abaixo forte; Posicao no range 20D: fechou no meio; Queda desde maxima 5D: 1% a 3% abaixo da maxima |
| 11 | `daytrade-principal` | `CEAB3` | 1D | 2% | 10 | 5 | 90.00% | 100.00% | Discovery Acao: compra no open, alvo +2% / stop -4% / cap 1D / Gap da abertura: alta de 1% a 3%; Posicao nas Bandas de Bollinger 20D: fechou no meio; Fechamento vs EMA9: acima leve |
| 12 | `daytrade-principal` | `MBRF3` | 1D | 2% | 10 | 4 | 90.00% | 100.00% | Discovery Acao: compra no open, alvo +2% / stop -4% / cap 1D / Gap da abertura: neutro; Retorno acumulado 3D: choque de alta; Posicao no range 5D: fechou perto da maxima |
| 13 | `daytrade-principal` | `USIM5` | 1D | 2% | 10 | 5 | 90.00% | 100.00% | Discovery Acao: compra no open, alvo +2% / stop -4% / cap 1D / Retorno acumulado 10D: choque de alta; RSI 14 periodos: RSI neutro alto; Posicao nas Bandas de Bollinger 20D: fechou perto da maxima |
| 14 | `daytrade-principal` | `B3SA3` | 1D | 2% | 8 | 5 | 100.00% | 100.00% | Discovery Acao: compra no open, alvo +2% / stop -4% / cap 1D / Retorno acumulado 1D: alta de 3% a 5%; Posicao no range 5D: fechou perto da maxima; Dias positivos nos ultimos 10 pregoes: 6 a 7 dias positivos em 10D |
| 15 | `daytrade-principal` | `CSAN3` | 1D | 2% | 8 | 4 | 100.00% | 100.00% | Discovery Acao: compra no open, alvo +2% / stop -4% / cap 1D / Inclinacao da MM50 em 10D: abaixo leve; Compressao 3D vs 20D: muito baixo; Queda desde maxima 10D: 10% a 15% abaixo da maxima |
| 16 | `daytrade-principal` | `CYRE3` | 1D | 2% | 8 | 6 | 100.00% | 100.00% | Discovery Acao: compra no open, alvo +2% / stop -4% / cap 1D / Posicao no range 5D: fechou na metade inferior; Queda desde maxima 5D: 6% a 10% abaixo da maxima; Retorno 5D normalizado por ATR: levemente abaixo |
| 17 | `daytrade-principal` | `YDUQ3` | 1D | 2% | 8 | 4 | 100.00% | 100.00% | Discovery Acao: compra no open, alvo +2% / stop -4% / cap 1D / Inclinacao da MM20 em 5D: abaixo leve; Retorno intraday do candle de referencia: alta de 1% a 3%; Queda desde maxima 5D: 1% a 3% abaixo da maxima |
| 18 | `daytrade-cobertura-15` | `RAIL3` | 1D | 1.5% | 20 | 11 | 95.00% | 100.00% | Discovery Acao: compra no open, alvo +1.5% / stop -3% / cap 1D / Gap da abertura: neutro; Retorno intraday do candle de referencia: neutro; Dia da semana: terca-feira |
| 19 | `daytrade-cobertura-15` | `HYPE3` | 1D | 1.5% | 15 | 8 | 93.33% | 100.00% | Discovery Acao: compra no open, alvo +1.5% / stop -3% / cap 1D / Gap vs range medio 20D: perto do normal; Inclinacao da MM20 em 5D: perto do equilibrio; Dia da semana: terca-feira |
| 20 | `daytrade-cobertura-15` | `PRIO3` | 1D | 1.5% | 15 | 5 | 93.33% | 100.00% | Discovery Acao: compra no open, alvo +1.5% / stop -3% / cap 1D / Fechamento vs MM50: acima forte; Compressao 3D vs 20D: alto |
| 21 | `daytrade-cobertura-15` | `GGBR4` | 1D | 1.5% | 14 | 6 | 92.86% | 90.91% | Discovery Acao: compra no open, alvo +1.5% / stop -3% / cap 1D / Retorno acumulado 1D: neutro; Retorno acumulado 3D: neutro; Retorno acumulado 20D: choque de alta |
| 22 | `daytrade-cobertura-15` | `POMO4` | 1D | 1.5% | 14 | 7 | 92.86% | 100.00% | Discovery Acao: compra no open, alvo +1.5% / stop -3% / cap 1D / Fechamento vs MM20: acima leve; Compressao 3D vs 20D: baixo; Sequencia de altas/quedas: alta em 2 a 3 pregoes seguidos |
| 23 | `daytrade-cobertura-15` | `ENEV3` | 1D | 1.5% | 13 | 10 | 92.31% | 100.00% | Discovery Acao: compra no open, alvo +1.5% / stop -3% / cap 1D / Retorno acumulado 1D: neutro; Gap vs ATR14: perto do normal; Dia da semana: terca-feira |
| 24 | `daytrade-cobertura-15` | `IGTI11` | 1D | 1.5% | 13 | 8 | 92.31% | 100.00% | Discovery Acao: compra no open, alvo +1.5% / stop -3% / cap 1D / Queda desde maxima 10D: 1% a 3% abaixo da maxima; Retorno 1D normalizado por ATR: perto do normal; Retorno 5D normalizado por ATR: acima do normal |
| 25 | `daytrade-cobertura-15` | `IRBR3` | 1D | 1.5% | 13 | 7 | 100.00% | 100.00% | Discovery Acao: compra no open, alvo +1.5% / stop -3% / cap 1D / MM20 vs MM50: abaixo leve; Dia da semana: terca-feira |
| 26 | `daytrade-cobertura-15` | `AURE3` | 1D | 1.5% | 12 | 8 | 91.67% | 100.00% | Discovery Acao: compra no open, alvo +1.5% / stop -3% / cap 1D / Gap da abertura: neutro; Posicao no range 10D: fechou no meio; Dia da semana: terca-feira |
| 27 | `daytrade-cobertura-15` | `BBAS3` | 1D | 1.5% | 12 | 5 | 100.00% | 100.00% | Discovery Acao: compra no open, alvo +1.5% / stop -3% / cap 1D / Inclinacao da MM50 em 10D: perto do equilibrio; Dias positivos nos ultimos 10 pregoes: 6 a 7 dias positivos em 10D; Retorno 5D normalizado por ATR: muito acima do normal |
| 28 | `daytrade-cobertura-15` | `BPAC11` | 1D | 1.5% | 12 | 6 | 91.67% | 100.00% | Discovery Acao: compra no open, alvo +1.5% / stop -3% / cap 1D / Volume vs media 20D: baixo; Dias positivos nos ultimos 5 pregoes: 4 a 5 dias positivos em 5D; Spike de volume vs maximo 5D: muito baixo |
| 29 | `daytrade-cobertura-15` | `CSNA3` | 1D | 1.5% | 12 | 8 | 91.67% | 100.00% | Discovery Acao: compra no open, alvo +1.5% / stop -3% / cap 1D / Posicao no range 10D: fechou no meio; Dia da semana: terca-feira; Spike de volume vs maximo 5D: muito baixo |
| 30 | `daytrade-cobertura-15` | `MULT3` | 1D | 1.5% | 12 | 4 | 91.67% | 100.00% | Discovery Acao: compra no open, alvo +1.5% / stop -3% / cap 1D / MM20 vs MM50: abaixo leve; Retorno 5D normalizado por ATR: levemente acima |

## Swing combinado - melhor por acao

| Rank | Livro | Ticker | Cap | Alvo | Trades | Meses | Alvo total | Alvo teste | Padrao |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | `swing-raro-7d-7` | `BEEF3` | 7D | 7% | 10 | 4 | 90.00% | 100.00% | Discovery Acao: compra no open, alvo +7% / stop -10% / cap 7D / Inclinacao da MM50 em 10D: abaixo forte; Posicao no range 20D: fechou no meio; Queda desde maxima 5D: 1% a 3% abaixo da maxima |
| 2 | `swing-raro-7d-7` | `VAMO3` | 7D | 7% | 9 | 5 | 100.00% | 100.00% | Discovery Acao: compra no open, alvo +7% / stop -10% / cap 7D / Dias positivos nos ultimos 5 pregoes: 3 dias positivos em 5D; RSI 14 periodos: RSI neutro alto; Fechamento vs EMA9: acima forte |
| 3 | `swing-principal-7d-5` | `MGLU3` | 7D | 5% | 21 | 8 | 90.48% | 100.00% | Discovery Acao: compra no open, alvo +5% / stop -10% / cap 7D / Inclinacao da MM20 em 5D: abaixo leve; Posicao no range 20D: fechou no meio; RSI 14 periodos: RSI neutro baixo |
| 4 | `swing-principal-7d-5` | `AZZA3` | 7D | 5% | 15 | 7 | 93.33% | 100.00% | Discovery Acao: compra no open, alvo +5% / stop -10% / cap 7D / Fechamento vs MM50: muito abaixo; MM20 vs MM50: abaixo forte; Volume vs media 20D: muito baixo |
| 5 | `swing-principal-7d-5` | `BRKM5` | 7D | 5% | 15 | 5 | 100.00% | 100.00% | Discovery Acao: compra no open, alvo +5% / stop -10% / cap 7D / Fechamento vs MM50: muito abaixo; RSI 14 periodos: RSI neutro baixo; Retorno 5D normalizado por ATR: abaixo do normal |
| 6 | `swing-principal-7d-5` | `CSNA3` | 7D | 5% | 15 | 6 | 100.00% | 100.00% | Discovery Acao: compra no open, alvo +5% / stop -10% / cap 7D / MM20 vs MM50: abaixo forte; RSI 14 periodos: RSI neutro baixo; Spike de volume vs maximo 5D: muito baixo |
| 7 | `swing-principal-7d-5` | `ASAI3` | 7D | 5% | 13 | 4 | 100.00% | 100.00% | Discovery Acao: compra no open, alvo +5% / stop -10% / cap 7D / Retorno acumulado 10D: choque de alta; MM20 vs MM50: abaixo leve; RSI 14 periodos: RSI neutro alto |
| 8 | `swing-principal-7d-5` | `CEAB3` | 7D | 5% | 13 | 5 | 92.31% | 100.00% | Discovery Acao: compra no open, alvo +5% / stop -10% / cap 7D / Retorno acumulado 20D: choque de baixa; Posicao no range 20D: fechou no meio |
| 9 | `swing-principal-7d-5` | `MRVE3` | 7D | 5% | 12 | 6 | 100.00% | 100.00% | Discovery Acao: compra no open, alvo +5% / stop -10% / cap 7D / Retorno acumulado 10D: choque de alta; Inclinacao da MM20 em 5D: perto do equilibrio; RSI 14 periodos: RSI neutro alto |
| 10 | `swing-principal-7d-5` | `DIRR3` | 7D | 5% | 11 | 5 | 90.91% | 100.00% | Discovery Acao: compra no open, alvo +5% / stop -10% / cap 7D / MM20 vs MM50: muito abaixo; Posicao no range 5D: fechou no meio; Dias positivos nos ultimos 5 pregoes: 3 dias positivos em 5D |
| 11 | `swing-principal-7d-5` | `HAPV3` | 7D | 5% | 11 | 6 | 90.91% | 100.00% | Discovery Acao: compra no open, alvo +5% / stop -10% / cap 7D / Inclinacao da MM20 em 5D: abaixo leve; Fechamento vs EMA9: acima leve |
| 12 | `swing-principal-7d-5` | `NATU3` | 7D | 5% | 11 | 5 | 90.91% | 100.00% | Discovery Acao: compra no open, alvo +5% / stop -10% / cap 7D / Retorno acumulado 5D: alta de 3% a 5%; Retorno 5D normalizado por ATR: levemente acima |
| 13 | `swing-principal-7d-5` | `CSAN3` | 7D | 5% | 10 | 5 | 90.00% | 100.00% | Discovery Acao: compra no open, alvo +5% / stop -10% / cap 7D / Retorno intraday do candle de referencia: alta de 3% a 5%; Sequencia de altas/quedas: alta no ultimo pregao; Dias positivos nos ultimos 5 pregoes: 2 dias positivos em 5D |
| 14 | `swing-principal-7d-5` | `CURY3` | 7D | 5% | 9 | 4 | 100.00% | 100.00% | Discovery Acao: compra no open, alvo +5% / stop -10% / cap 7D / Retorno acumulado 10D: choque de alta; MM20 vs MM50: abaixo leve; RSI 14 periodos: RSI neutro alto |
| 15 | `swing-principal-7d-5` | `RENT3` | 7D | 5% | 9 | 4 | 100.00% | 100.00% | Discovery Acao: compra no open, alvo +5% / stop -10% / cap 7D / Inclinacao da MM20 em 5D: abaixo leve; RSI 14 periodos: RSI neutro alto |
| 16 | `swing-cobertura-7d-4` | `COGN3` | 7D | 4% | 21 | 10 | 90.48% | 100.00% | Discovery Acao: compra no open, alvo +4% / stop -8% / cap 7D / Posicao no range 5D: fechou na metade superior; Queda desde maxima 5D: 1% a 3% abaixo da maxima; Dias positivos nos ultimos 10 pregoes: 4 a 5 dias positivos em 10D |
| 17 | `swing-cobertura-7d-4` | `CSMG3` | 7D | 4% | 20 | 5 | 100.00% | 100.00% | Discovery Acao: compra no open, alvo +4% / stop -8% / cap 7D / Inclinacao da MM20 em 5D: perto do equilibrio; Inclinacao da MM50 em 10D: acima leve; Fechamento vs EMA9: acima leve |
| 18 | `swing-cobertura-7d-4` | `RDOR3` | 7D | 4% | 19 | 4 | 94.74% | 100.00% | Discovery Acao: compra no open, alvo +4% / stop -8% / cap 7D / Fechamento vs MM50: abaixo forte; Inclinacao da MM20 em 5D: abaixo leve; Compressao 3D vs 20D: baixo |
| 19 | `swing-cobertura-7d-4` | `VBBR3` | 7D | 4% | 19 | 6 | 94.74% | 100.00% | Discovery Acao: compra no open, alvo +4% / stop -8% / cap 7D / MM20 vs MM50: perto do equilibrio; Inclinacao da MM50 em 10D: acima leve; Posicao no range 5D: fechou no meio |
| 20 | `swing-cobertura-7d-4` | `MOTV3` | 7D | 4% | 18 | 5 | 100.00% | 100.00% | Discovery Acao: compra no open, alvo +4% / stop -8% / cap 7D / MM20 vs MM50: abaixo leve; Inclinacao da MM50 em 10D: perto do equilibrio; Queda desde maxima 10D: 3% a 6% abaixo da maxima |
| 21 | `swing-cobertura-7d-4` | `IGTI11` | 7D | 4% | 17 | 5 | 94.12% | 100.00% | Discovery Acao: compra no open, alvo +4% / stop -8% / cap 7D / MM20 vs MM50: perto do equilibrio; Inclinacao da MM50 em 10D: acima leve; Queda desde maxima 5D: 1% a 3% abaixo da maxima |
| 22 | `swing-cobertura-7d-4` | `LREN3` | 7D | 4% | 17 | 6 | 94.12% | 100.00% | Discovery Acao: compra no open, alvo +4% / stop -8% / cap 7D / Fechamento vs MM50: abaixo forte; Posicao no range 10D: fechou no meio |
| 23 | `swing-cobertura-7d-4` | `USIM5` | 7D | 4% | 17 | 6 | 100.00% | 100.00% | Discovery Acao: compra no open, alvo +4% / stop -8% / cap 7D / Fechamento vs MM20: acima leve; Posicao no range 20D: fechou no meio; Retorno 1D normalizado por ATR: perto do normal |
| 24 | `swing-cobertura-7d-4` | `SMFT3` | 7D | 4% | 16 | 5 | 93.75% | 91.67% | Discovery Acao: compra no open, alvo +4% / stop -8% / cap 7D / MM20 vs MM50: abaixo forte; RSI 14 periodos: RSI baixo |
| 25 | `swing-cobertura-7d-4` | `B3SA3` | 7D | 4% | 15 | 5 | 93.33% | 100.00% | Discovery Acao: compra no open, alvo +4% / stop -8% / cap 7D / Retorno acumulado 10D: choque de alta; Posicao no range 10D: fechou na metade superior |
| 26 | `swing-cobertura-7d-4` | `VIVA3` | 7D | 4% | 15 | 8 | 93.33% | 100.00% | Discovery Acao: compra no open, alvo +4% / stop -8% / cap 7D / Compressao 3D vs 20D: baixo; Posicao no range 5D: fechou na metade superior; Dias positivos nos ultimos 10 pregoes: 4 a 5 dias positivos em 10D |
| 27 | `swing-cobertura-7d-4` | `MULT3` | 7D | 4% | 14 | 5 | 92.86% | 100.00% | Discovery Acao: compra no open, alvo +4% / stop -8% / cap 7D / Fechamento vs MM50: perto do equilibrio; Posicao no range 20D: fechou na metade superior |
| 28 | `swing-cobertura-7d-4` | `SBSP3` | 7D | 4% | 14 | 4 | 92.86% | 100.00% | Discovery Acao: compra no open, alvo +4% / stop -8% / cap 7D / MM20 vs MM50: abaixo leve; Inclinacao da MM50 em 10D: perto do equilibrio; Compressao 3D vs 20D: normal |
| 29 | `swing-cobertura-7d-4` | `UGPA3` | 7D | 4% | 14 | 5 | 92.86% | 100.00% | Discovery Acao: compra no open, alvo +4% / stop -8% / cap 7D / Retorno acumulado 10D: choque de alta; Dias positivos nos ultimos 5 pregoes: 3 dias positivos em 5D |
| 30 | `swing-cobertura-7d-4` | `HYPE3` | 7D | 4% | 13 | 8 | 100.00% | 100.00% | Discovery Acao: compra no open, alvo +4% / stop -8% / cap 7D / Retorno intraday do candle de referencia: alta de 1% a 3%; Retorno 1D normalizado por ATR: perto do normal; Spike de volume vs maximo 5D: muito baixo |
