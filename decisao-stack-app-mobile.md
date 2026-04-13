# 📑 Documento de Especificação: Sistema de Mineração de Padrões B3

## 1. Visão Geral do Projeto
O objetivo deste projeto é construir uma ferramenta de **Análise Quantitativa** para identificar padrões de comportamento repetitivos nos ativos da B3 (Bolsa Brasileira). O sistema deve buscar correlações estatísticas baseadas em movimentações históricas, permitindo validar se eventos específicos de preço no "Dia T" resultam em movimentos previsíveis no "Dia T+1".

## 2. Requisitos de Negócio
* **Identificação de Oportunidades:** Encontrar padrões do tipo: *"Sempre que a ação cai X%, ela recupera Y% no pregão seguinte"*.
* **Eficiência Operacional:** Evitar requisições excessivas a APIs externas através de um banco de dados local.
* **Escalabilidade:** Capacidade de analisar múltiplos tickers simultaneamente.

---

## 3. Arquitetura Técnica Sugerida

### 3.1. Stack Tecnológica
* **Linguagem:** Python 3.x (Padrão para análise de dados).
* **Interface de Dados:** Biblioteca `yfinance` para integração com Yahoo Finance.
* **Banco de Dados:** **SQLite** (Base local persistente).
* **Processamento:** Pandas/NumPy para cálculos vetoriais.

### 3.2. Estrutura de Dados (Database Schema)
O banco de dados SQLite deve conter uma tabela principal para armazenar o histórico **OHLC** (Open, High, Low, Close):

| Campo | Tipo | Descrição |
| :--- | :--- | :--- |
| `ticker` | TEXT | Símbolo da ação (ex: PETR4.SA) |
| `date` | DATETIME | Data do pregão |
| `open` | REAL | Preço de abertura |
| `high` | REAL | Máxima do dia |
| `low` | REAL | Mínima do dia |
| `close` | REAL | Preço de fechamento |
| `volume` | INTEGER | Volume financeiro/quantidade |

---

## 4. Fluxo de Implementação (Roadmap para Devs)

### Etapa 1: Módulo de Ingestão e Persistência
1.  O sistema deve verificar se o banco de dados `b3_history.db` existe.
2.  Caso não exista (ou esteja desatualizado), deve realizar o download dos dados via API.
3.  **Frequência:** O sistema deve buscar os últimos 30 dias de cada ativo (janela de análise).
4.  **Tratamento:** Salvar os dados no SQLite para garantir que os testes subsequentes sejam realizados instantaneamente, sem depender de internet.

### Etapa 2: Motor de Busca de Padrões (Logic Engine)
O algoritmo deve percorrer a série histórica realizando as seguintes operações:
1.  **Cálculo de Variação:** Calcular a variação percentual entre o fechamento de ontem e o fechamento de hoje.
2.  **Identificação de Gatilhos:** Localizar datas onde a variação atingiu o parâmetro definido (ex: queda $\ge 2\%$).
3.  **Análise de Desfecho:** Verificar o comportamento do preço no dia útil imediatamente posterior ao gatilho.

### Etapa 3: Relatório de Saída (Output)
O sistema deve gerar um ranking consolidado informando:
* **Ativo analisado.**
* **Número total de ocorrências** do padrão nos últimos 30 dias.
* **Taxa de Assertividade (%)**: Quantas vezes o movimento esperado de fato aconteceu.
* **Lucratividade Média:** O retorno médio por operação caso o padrão fosse seguido.

---

## 5. Regras de Ouro (Business Rules)
* **Ajuste de Dividendos:** É obrigatório utilizar o campo `Adj Close` (Fechamento Ajustado) para evitar sinais falsos causados por proventos ou grupamentos.
* **Sufixo de Ticker:** Garantir a inclusão automática do sufixo `.SA` para consultas na B3.
* **Independência de Dados:** O módulo de análise **não deve** chamar a API. Ele deve ler exclusivamente do SQLite após a sincronização inicial.

---

## 6. Critérios de Aceite
1.  Download completo dos dados de uma lista pré-definida de tickers.
2.  Gravação bem-sucedida no SQLite.
3.  Exibição de um relatório via console ou CSV com os padrões mais lucrativos encontrados.

---
**Documento elaborado para a equipe de desenvolvimento.** **Data:** 08/04/2026