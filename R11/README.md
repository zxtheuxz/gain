# R11 - Daytrade portfolio (3 familias + Walk-Forward + Bootstrap)

R11 e o portfolio operacional de **daytrade** (curto prazo), complementar
ao R10 (swing). Usa a mesma metodologia rigorosa do R10 (walk-forward +
bootstrap + janela por ticker) mas com **gates calibrados pra natureza
do daytrade** (win rate menor, R/R compensa).

## Diferenca chave vs R10

| | R10 (swing) | R11 (daytrade) |
|---|---|---|
| Foco | alvos 4-10% em 5-20 pregoes | alvos 1-3% em 1-3 pregoes + saida fixa |
| Min win rate | 75% | 60% |
| Min avg return val | 4% | 0.5% |
| Min val_trades | 15 | 10 |
| Min PF | 3.0 | 1.8 |
| Wilson CI lower | >= 70% | >= 55% |
| Walk-forward | obrigatorio | obrigatorio (mesma logica) |
| Bootstrap CI > 0 | obrigatorio | obrigatorio |

R11 aceita **win rate menor** (60% vs 75%) porque daytrade tem ruido
inerente maior. Mas exige **R/R favoravel** (PF >= 1.8) e **robustez
estatistica** (walk-forward + bootstrap) iguais ao R10.

## 3 familias de templates

### Familia 1 - Intraday (1 pregao)
- Alvos: 1.0, 1.5, 2.0%
- Stops: 0.5, 1.0, 1.5, 2.0%
- Cap: 0 (mesmo dia) ou 1 (D+1)

### Familia 2 - Mini-swing (2-3 pregoes)
- Alvos: 2.0, 2.5, 3.0%
- Stops: 1.0, 1.5, 2.0, 2.5%
- Cap: 2 ou 3 pregoes

### Familia 3 - Saida fixa, alvo livre
- Sem TP/SL. Saida em horario predeterminado
- Sucesso = retorno realizado > 1%
- Modalidades:
  - `open_close_same_day`: open -> close mesmo dia
  - `open_next_open`: open -> open D+1
  - `open_next_close`: open -> close D+1
  - `close_next_open`: close -> open D+1
  - `close_next_close`: close -> close D+1

## Como rodar

```bash
chmod +x R11/run_r11_parallel.sh
tmux new-session -d -s r11 "bash R11/run_r11_parallel.sh"
tmux attach -t r11   # ctrl+b d para destacar
```

Tempo esperado: ~15-30 min (24 workers).

## Outputs em `reports/r11-<RUN_ID>/`

```
r11.log
r11-mining.csv               # candidatos brutos
r11-validated.csv            # passou todos os gates
r11-validated-slim.csv       # sem retornos brutos
r11-final.csv                # top 3 por ticker (50-150 esperados)
r11-signals-ready.csv        # idem
r11-report.md                # top 30 humano
r11-oos-report.md            # OOS honesto
```

## Como operar R10 + R11 juntos

R10 e R11 sao independentes mas **complementares**. Mesmo ticker pode
estar nos dois portfolios mas com setups diferentes (R10 = swing,
R11 = daytrade). Nao ha conflito porque as features de entrada e o
horizonte sao diferentes.

Sugestao de capital: 60-70% R10 (swing, edge mais forte) + 30-40% R11
(daytrade, mais operacoes mas R/R menor).

## Limitacoes

- Daytrade tem ruido maior - aceitar variabilidade
- Win rate "verdadeiro" provavelmente entre 60-75%, nao mais
- Recomenda paper trading 1-3 meses antes de risco real
- Sizing menor por trade (50% do swing) ate confirmar perfil ao vivo
