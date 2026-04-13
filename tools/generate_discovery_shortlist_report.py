from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from datetime import datetime
from math import isinf
from pathlib import Path


def _format_pct(value: float) -> str:
    return f"{value:.4f}"


def _format_metric(value: float | None) -> str:
    if value is None:
        return "n/a"
    if isinf(value):
        return "INF"
    return f"{value:.4f}"


def _format_ratio(value: float) -> str:
    return f"{value * 100:.2f}%"


def _escape_md(text: object) -> str:
    return str(text).replace("|", "\\|")


def _humanize_bucket(value: str) -> str:
    mapping = {
        "queda de 1% a 3%": "em queda entre 1% e 3%",
        "queda de 3% a 6%": "em queda entre 3% e 6%",
        "alta de 1% a 3%": "em alta entre 1% e 3%",
        "alta de 3% a 6%": "em alta entre 3% e 6%",
        "choque de baixa": "em uma queda muito forte fora do normal",
        "choque de alta": "em uma alta muito forte fora do normal",
        "perto do equilibrio": "perto da media de 20 dias",
        "acima forte": "bem acima da media de 20 dias",
        "abaixo forte": "bem abaixo da media de 20 dias",
        "muito acima": "muito acima da media de 20 dias",
        "muito abaixo": "muito abaixo da media de 20 dias",
        "extremo": "muito acima do normal",
        "normal": "perto do normal",
        "alto": "mais comprimida do que o normal",
        "muito alto": "muito mais comprimida do que o normal",
        "baixo": "mais solta do que o normal",
        "neutro": "pequeno ou sem direcao forte",
        "levemente acima": "um pouco maior do que o normal",
        "levemente abaixo": "um pouco menor do que o normal",
    }
    return mapping.get(value, value)


def _plain_state_part(part: str) -> str:
    cleaned = part.strip()
    if not cleaned:
        return ""

    if ": " not in cleaned:
        return cleaned

    label, raw_value = cleaned.split(": ", 1)
    value = _humanize_bucket(raw_value.strip())

    templates = {
        "Retorno acumulado 3D": f"nos ultimos 3 pregoes, o papel ficou {value}",
        "Retorno acumulado 5D": f"nos ultimos 5 pregoes, o papel ficou {value}",
        "Retorno de D-1": f"no pregao anterior, o papel ficou {value}",
        "Fechamento vs MM20": f"no fechamento anterior, o preco estava {value}",
        "Volume vs media 20D": f"o volume estava {value} em relacao a media de 20 dias",
        "Gap da abertura": f"na abertura, o gap foi {value}",
        "Gap vs range medio 20D": f"na abertura, o tamanho do gap foi {value} em relacao ao comportamento normal do ativo",
        "Compressao 3D vs 20D": f"a oscilacao recente estava {value} em relacao aos ultimos 20 dias",
    }
    return templates.get(label, cleaned)


def _plain_state_signature(signature: str) -> str:
    parts = [_plain_state_part(part) for part in signature.split(";")]
    parts = [part for part in parts if part]
    if not parts:
        return signature
    sentence = "; ".join(parts)
    return sentence[:1].upper() + sentence[1:] + "."


def _parse_profit_factor(raw_value: str) -> float | None:
    cleaned = (raw_value or "").strip()
    if not cleaned:
        return None
    if cleaned == "INF":
        return float("inf")
    return float(cleaned)


def _load_shortlist(path: str | Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with Path(path).open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        for row in reader:
            rows.append(
                {
                    "code": row["code"],
                    "label": row["label"],
                    "family": row["family"],
                    "template_code": row["template_code"],
                    "template_label": row["template_label"],
                    "option_side": row["option_side"],
                    "dte_target_days": int(row["dte_target_days"]),
                    "trade_direction": row["trade_direction"],
                    "state_size": int(row["state_size"]),
                    "state_signature": row["state_signature"],
                    "feature_keys": row["feature_keys"],
                    "tickers_with_matches": int(row["tickers_with_matches"]),
                    "total_occurrences": int(row["total_occurrences"]),
                    "profitable_trade_rate_pct": float(row["profitable_trade_rate_pct"]),
                    "average_trade_return_pct": float(row["average_trade_return_pct"]),
                    "net_trade_return_pct": float(row["net_trade_return_pct"]),
                    "profit_factor": _parse_profit_factor(row["profit_factor"]),
                    "baseline_average_trade_return_pct": float(row["baseline_average_trade_return_pct"]),
                    "baseline_profit_factor": _parse_profit_factor(row["baseline_profit_factor"]),
                    "average_trade_uplift_pct": float(row["average_trade_uplift_pct"]),
                    "profit_factor_uplift": float(row["profit_factor_uplift"]),
                    "train_trades": int(row["train_trades"]),
                    "train_average_trade_return_pct": float(row["train_average_trade_return_pct"]),
                    "train_net_trade_return_pct": float(row["train_net_trade_return_pct"]),
                    "train_profit_factor": _parse_profit_factor(row["train_profit_factor"]),
                    "validation_trades": int(row["validation_trades"]),
                    "validation_average_trade_return_pct": float(row["validation_average_trade_return_pct"]),
                    "validation_net_trade_return_pct": float(row["validation_net_trade_return_pct"]),
                    "validation_profit_factor": _parse_profit_factor(row["validation_profit_factor"]),
                    "active_months": int(row["active_months"]),
                    "positive_months": int(row["positive_months"]),
                    "positive_month_ratio": float(row["positive_month_ratio"]),
                    "validation_active_months": int(row["validation_active_months"]),
                    "validation_positive_months": int(row["validation_positive_months"]),
                    "validation_positive_month_ratio": float(row["validation_positive_month_ratio"]),
                    "overlap_bucket": row["overlap_bucket"],
                    "robustness_score": float(row["robustness_score"]),
                }
            )
    return rows


def _load_ticker_rows(path: str | Path, min_trades: int) -> dict[str, list[dict[str, object]]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    with Path(path).open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        for row in reader:
            total_trades = int(row["total_trades"])
            if total_trades < min_trades:
                continue
            grouped[row["strategy_code"]].append(
                {
                    "ticker": row["ticker"],
                    "total_trades": total_trades,
                    "success_rate_pct": float(row["success_rate_pct"]),
                    "profitable_trade_rate_pct": float(row["profitable_trade_rate_pct"]),
                    "average_trade_return_pct": float(row["average_trade_return_pct"]),
                    "median_trade_return_pct": float(row["median_trade_return_pct"]),
                    "net_trade_return_pct": float(row["net_trade_return_pct"]),
                    "cumulative_return_pct": float(row["cumulative_return_pct"]),
                    "profit_factor": _parse_profit_factor(row["profit_factor"]),
                    "first_trade_date": row["first_trade_date"],
                    "last_trade_date": row["last_trade_date"],
                }
            )

    for rows in grouped.values():
        rows.sort(
            key=lambda item: (
                item["net_trade_return_pct"],
                item["average_trade_return_pct"],
                item["total_trades"],
            ),
            reverse=True,
        )
    return grouped


def _aggregate_tickers(tickers_by_strategy: dict[str, list[dict[str, object]]]) -> list[dict[str, object]]:
    aggregated: dict[str, dict[str, object]] = {}
    for rows in tickers_by_strategy.values():
        for row in rows:
            ticker = str(row["ticker"])
            item = aggregated.setdefault(
                ticker,
                {
                    "ticker": ticker,
                    "strategy_count": 0,
                    "total_trades": 0,
                    "sum_net_trade_return_pct": 0.0,
                    "average_trade_returns": [],
                    "best_net_trade_return_pct": float("-inf"),
                },
            )
            item["strategy_count"] = int(item["strategy_count"]) + 1
            item["total_trades"] = int(item["total_trades"]) + int(row["total_trades"])
            item["sum_net_trade_return_pct"] = float(item["sum_net_trade_return_pct"]) + float(
                row["net_trade_return_pct"]
            )
            average_returns = item["average_trade_returns"]
            assert isinstance(average_returns, list)
            average_returns.append(float(row["average_trade_return_pct"]))
            item["best_net_trade_return_pct"] = max(
                float(item["best_net_trade_return_pct"]),
                float(row["net_trade_return_pct"]),
            )

    result: list[dict[str, object]] = []
    for item in aggregated.values():
        average_returns = item["average_trade_returns"]
        assert isinstance(average_returns, list)
        result.append(
            {
                "ticker": item["ticker"],
                "strategy_count": item["strategy_count"],
                "total_trades": item["total_trades"],
                "sum_net_trade_return_pct": item["sum_net_trade_return_pct"],
                "average_trade_return_pct": sum(average_returns) / len(average_returns),
                "best_net_trade_return_pct": item["best_net_trade_return_pct"],
            }
        )

    result.sort(
        key=lambda row: (
            float(row["sum_net_trade_return_pct"]),
            int(row["strategy_count"]),
            int(row["total_trades"]),
        ),
        reverse=True,
    )
    return result


def _playbook(item: dict[str, object]) -> dict[str, str]:
    option_side = str(item["option_side"]).upper()
    dte_target_days = int(item["dte_target_days"])
    contract = f"{option_side} ATM com vencimento alvo de {dte_target_days} dias"
    action = "comprar CALL ATM" if option_side == "CALL" else "comprar PUT ATM"
    directional_read = "movimento de alta" if option_side == "CALL" else "movimento de baixa"
    return {
        "contract": contract,
        "action": action,
        "directional_read": directional_read,
        "entry": "na abertura do pregao",
        "exit": "no fechamento do mesmo dia",
        "duration": "intraday, sem carregar overnight",
    }


def _strategy_one_liner(item: dict[str, object]) -> str:
    option_side = str(item["option_side"]).upper()
    dte = int(item["dte_target_days"])
    action = "comprar CALL ATM" if option_side == "CALL" else "comprar PUT ATM"
    return (
        f"Se o papel entrar neste contexto, a regra e {action} de {dte} dias na abertura "
        f"e zerar no fechamento do mesmo dia."
    )


def _strategy_example(item: dict[str, object], rows: list[dict[str, object]]) -> str:
    ticker = str(rows[0]["ticker"]) if rows else "uma acao da sua lista"
    option_side = str(item["option_side"]).upper()
    dte = int(item["dte_target_days"])
    action = "CALL ATM" if option_side == "CALL" else "PUT ATM"
    return (
        f"Exemplo ilustrativo: imagine que {ticker} abriu hoje ja encaixada neste contexto. "
        f"Nesse caso, a leitura da estrategia seria comprar {action} com bucket de {dte} dias "
        f"na abertura e encerrar a operacao no fechamento do mesmo pregao."
    )


def _top_tickers_summary(rows: list[dict[str, object]], top: int = 5) -> str:
    if not rows:
        return "Nenhuma acao qualificada no relatorio por acao."
    return "; ".join(
        f"{item['ticker']} (resultado {item['net_trade_return_pct']:.2f}%, media {item['average_trade_return_pct']:.2f}%, trades {item['total_trades']})"
        for item in rows[:top]
    )


def _training_validation_comment(item: dict[str, object]) -> str:
    train_avg = float(item["train_average_trade_return_pct"])
    validation_avg = float(item["validation_average_trade_return_pct"])
    if validation_avg > train_avg:
        return "A validacao ficou mais forte do que o treino, o que e um sinal positivo."
    if validation_avg < train_avg:
        return "A validacao ficou abaixo do treino, mas ainda manteve resultado positivo dentro do corte final."
    return "Treino e validacao ficaram no mesmo patamar medio."


def _monthly_stability_comment(item: dict[str, object]) -> str:
    return (
        f"No periodo completo, a estrategia ficou positiva em {item['positive_months']} de {item['active_months']} meses "
        f"({_format_ratio(float(item['positive_month_ratio']))}). "
        f"Na validacao, ficou positiva em {item['validation_positive_months']} de {item['validation_active_months']} meses "
        f"({_format_ratio(float(item['validation_positive_month_ratio']))})."
    )


def _build_ticker_table_rows(rows: list[dict[str, object]]) -> list[str]:
    lines = [
        "| Acao | Trades | Acerto % | Trades positivos % | Media por trade % | Mediana % | Resultado total % | PF | Primeiro trade | Ultimo trade |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    for item in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    _escape_md(item["ticker"]),
                    str(item["total_trades"]),
                    f"{item['success_rate_pct']:.2f}",
                    f"{item['profitable_trade_rate_pct']:.2f}",
                    _format_pct(float(item["average_trade_return_pct"])),
                    _format_pct(float(item["median_trade_return_pct"])),
                    _format_pct(float(item["net_trade_return_pct"])),
                    _format_metric(item["profit_factor"]),
                    str(item["first_trade_date"]),
                    str(item["last_trade_date"]),
                ]
            )
            + " |"
        )
    return lines


def generate_report(
    *,
    shortlist_csv: str | Path,
    ticker_summary_csv: str | Path,
    output_md: str | Path,
    start_date: str,
    end_date: str,
    validation_start_date: str,
    top_tickers_per_strategy: int | None = None,
    ticker_min_trades: int = 15,
    tested_patterns: int = 5505,
    raw_approved_patterns: int = 3029,
    refined_patterns: int = 406,
) -> Path:
    shortlist = _load_shortlist(shortlist_csv)
    tickers_by_strategy = _load_ticker_rows(ticker_summary_csv, min_trades=ticker_min_trades)
    aggregate_tickers = _aggregate_tickers(tickers_by_strategy)

    recurring_tickers = Counter()
    total_ticker_rows = 0
    for strategy in shortlist:
        strategy_rows = tickers_by_strategy.get(str(strategy["code"]), [])
        total_ticker_rows += len(strategy_rows)
        for ticker_row in strategy_rows:
            recurring_tickers[str(ticker_row["ticker"])] += 1

    best_strategy = shortlist[0] if shortlist else None
    total_strategies = len(shortlist)

    lines = [
        "# Relatorio Explicado de Estrategias de Opcoes",
        "",
        "## Objetivo Deste Documento",
        "",
        "Este arquivo foi escrito para uma pessoa que vai ver o projeto pela primeira vez e precisa entender, em uma leitura unica, o que foi feito, como as estrategias nasceram, o que o backtest validou, como operar e em quais acoes cada leitura se sustentou melhor.",
        "",
        f"- Gerado em: `{datetime.now().astimezone().isoformat(timespec='seconds')}`",
        f"- Janela total analisada: `{start_date}` ate `{end_date}`",
        f"- Janela de treino: `{start_date}` ate `2025-12-31`",
        f"- Janela de validacao fora da amostra: `{validation_start_date}` ate `{end_date}`",
        f"- Estrategias finais neste relatorio: `{total_strategies}`",
        f"- Linhas de backtest por acao incluidas aqui: `{total_ticker_rows}`",
        f"- Filtro minimo por acao neste relatorio: `{ticker_min_trades}` trades",
        "",
        "## Resposta Curta",
        "",
        "- Todas as estrategias deste arquivo operam opcoes `ATM`.",
        "- A entrada acontece `na abertura do pregao`.",
        "- A saida acontece `no fechamento do mesmo dia`.",
        "- O ganho ou perda do backtest foi medido no `premio da opcao`, nao no preco da acao.",
        "- Este arquivo nao mostra palpites. Ele mostra apenas as leituras que sobreviveram ao nosso funil estatistico.",
    ]

    if best_strategy is not None:
        lines.extend(
            [
                f"- A estrategia numero 1 do ranking atual e `{best_strategy['label']}`.",
                f"- Resultado consolidado dela: media `{best_strategy['average_trade_return_pct']:.4f}%`, resultado total `{best_strategy['net_trade_return_pct']:.4f}%`, `PF {_format_metric(best_strategy['profit_factor'])}`.",
                f"- Resultado na validacao: `{best_strategy['validation_trades']}` trades, media `{best_strategy['validation_average_trade_return_pct']:.4f}%`, resultado total `{best_strategy['validation_net_trade_return_pct']:.4f}%`, `PF {_format_metric(best_strategy['validation_profit_factor'])}`.",
            ]
        )

    lines.extend(
        [
            "",
            "## O Que E Uma Estrategia Neste Projeto",
            "",
            "Aqui, estrategia nao quer dizer desenho bonito no grafico. Estrategia quer dizer um contexto objetivo de mercado que apareceu muitas vezes no passado e que, quando apareceu, favoreceu um tipo especifico de operacao em opcao.",
            "",
            "A estrutura de todas as 27 estrategias deste arquivo e sempre a mesma:",
            "",
            "1. A acao entra em um contexto bem definido.",
            "2. A estrategia diz se a operacao e de `CALL` ou `PUT`.",
            "3. A entrada acontece na abertura.",
            "4. A saida acontece no fechamento do mesmo dia.",
            "",
            "O que muda de uma estrategia para outra e apenas o contexto que precisa aparecer no papel.",
            "",
            "## Se Voce Ler Apenas 5 Minutos",
            "",
            "1. Todas as estrategias aqui sao `intraday`: entra na abertura e sai no fechamento.",
            "2. O contrato testado e sempre `ATM`, com bucket de `7 DTE` ou `15 DTE`.",
            "3. O que decide a qualidade da estrategia e `resultado financeiro`, nao apenas taxa de acerto.",
            "4. Cada estrategia tem uma tabela mostrando em quais `acoes` ela funcionou melhor no backtest.",
            "5. Para aplicar, o caminho correto e: escolher a estrategia -> escolher a acao da tabela -> confirmar o estado do dia -> executar a opcao certa -> zerar no fechamento.",
            "",
            "## Como Este Arquivo Deve Ser Lido",
            "",
            "1. Leia `Funil do estudo` para entender de onde vieram as estrategias.",
            "2. Leia `Regra operacional fixa` para saber entrada, contrato e saida.",
            "3. Leia `Top 10 para comecar` para ver as ideias mais fortes primeiro.",
            "4. Escolha uma estrategia e va para a secao detalhada dela.",
            "5. Dentro da estrategia, use primeiro `Acoes para priorizar` e depois a `Tabela completa por acao`.",
            "",
            "## Glossario Rapido",
            "",
            "- `CALL`: opcao usada quando a ideia e capturar alta do premio em um movimento favoravel.",
            "- `PUT`: opcao usada quando a ideia e capturar baixa do mercado via valorizacao da put.",
            "- `ATM`: strike mais proximo do preco atual da acao.",
            "- `7 DTE` e `15 DTE`: prazo alvo do contrato em dias corridos ate o vencimento.",
            "- `Media por trade %`: ganho ou perda medio por operacao.",
            "- `Resultado total %`: soma simples dos retornos de todos os trades.",
            "- `PF`: profit factor. Acima de 1 significa que o total ganho foi maior do que o total perdido.",
            "- `Uplift`: melhora do padrao em relacao ao template base do mesmo tipo de opcao.",
            "- `Acerto %`: taxa de trades que ficaram acima do criterio de sucesso interno do estudo.",
            "- `Trades positivos %`: taxa de trades que efetivamente fecharam positivos.",
            "",
            "## Funil Do Estudo",
            "",
            f"1. Comecamos com `{tested_patterns}` padroes de comportamento do mercado.",
            f"2. Desses, `{raw_approved_patterns}` ficaram positivos no primeiro corte bruto.",
            f"3. Depois aplicamos baseline, treino, validacao e estabilidade mensal. Restaram `{refined_patterns}`.",
            f"4. Por fim, removemos padroes muito parecidos entre si e ficamos com `{total_strategies}` estrategias finais neste relatorio.",
            "",
            "Traduzindo para linguagem simples: primeiro o sistema procurou milhares de padroes; depois eliminou o que parecia bom so no papel; no fim sobraram apenas as leituras que mantiveram resultado financeiro e robustez.",
            "",
            "## Regra Operacional Fixa Deste Relatorio",
            "",
            "- Tipo de operacao: `intraday com opcoes`.",
            "- Entrada: `na abertura do pregao`.",
            "- Saida: `sempre no fechamento do mesmo dia`.",
            "- Contrato: `ATM`, do lado correto (`CALL` ou `PUT`), com bucket de `7 DTE` ou `15 DTE`.",
            "- O relatorio nao foi feito para carregar overnight.",
            "- O backtest foi medido no premio da opcao.",
            "",
            "## Como Aplicar Na Pratica",
            "",
            "1. Escolha uma estrategia entre as melhores do ranking.",
            "2. Veja na secao dessa estrategia quais acoes mais sustentaram o resultado.",
            "3. No dia do trade, confirme se a acao realmente entrou no contexto descrito.",
            "4. Se entrou, escolha a opcao `ATM` do lado correto (`CALL` ou `PUT`) com o bucket certo (`7 DTE` ou `15 DTE`).",
            "5. Entre na abertura apenas se houver liquidez minima aceitavel.",
            "6. Zere no fechamento do mesmo dia, sem carregar overnight.",
            "",
            "## Quando Nao Operar",
            "",
            "1. Quando a acao nao estiver claramente dentro do contexto descrito na estrategia.",
            "2. Quando a opcao `ATM` estiver sem liquidez ou com spread ruim demais.",
            "3. Quando voce nao puder zerar a operacao no fechamento.",
            "4. Quando estiver tentando adaptar a estrategia para outro prazo ou outro tipo de contrato sem novo backtest.",
            "",
            "## Exemplo Geral Antes Das 27 Estrategias",
            "",
            "Exemplo simples: se uma estrategia disser que o papel caiu forte nos ultimos 3 dias, ficou perto da media de 20 e a operacao correta e `comprar PUT ATM 7 DTE`, isso quer dizer o seguinte:",
            "",
            "1. Voce olha a acao antes da abertura e confirma se esse contexto realmente apareceu.",
            "2. Na abertura, compra uma `PUT ATM` com vencimento alvo de `7 dias`.",
            "3. Nao carrega overnight.",
            "4. Zera no fechamento do mesmo dia.",
            "",
            "Ou seja: o relatorio sempre responde quatro perguntas basicas. O que observar na acao, qual opcao comprar, quando entrar e quando sair.",
            "",
            "## Como Escolher A Acao Dentro De Cada Estrategia",
            "",
            "1. Priorize acoes que aparecem no topo da tabela da propria estrategia.",
            "2. De preferencia para papeis com mais `trades`, `resultado total` forte e `PF` acima de 1.",
            "3. Se duas acoes estiverem parecidas, prefira a que aparece em mais estrategias do relatorio.",
            "4. Nao use uma acao fora da tabela apenas porque o grafico parece parecido. O criterio aqui e historico validado.",
            f"5. Neste relatorio, so aparecem acoes com pelo menos `{ticker_min_trades}` trades dentro da estrategia.",
            "",
            "## Como Ler O Backtest Sem Se Confundir",
            "",
            "- `Trades`: quantas vezes aquela situacao apareceu no historico.",
            "- `Trades positivos %`: quantas dessas operacoes terminaram no azul.",
            "- `Media por trade %`: ganho ou perda medio por operacao.",
            "- `Resultado total %`: soma simples de todos os retornos daquela linha.",
            "- `PF`: quanto o conjunto ganhou para cada 1 de perda. Acima de 1 ja e melhor do que perder dinheiro.",
            "",
            "Ponto importante: em opcoes, uma estrategia pode ter poucos trades positivos e ainda assim ganhar dinheiro, porque os ganhos vencedores podem ser muito maiores que as perdas. Por isso `PF`, `Media por trade %` e `Resultado total %` valem mais do que olhar apenas acerto.",
            "",
            "## Observacoes Importantes",
            "",
            "- Este documento resume um estudo estatistico. Nao e garantia de resultado futuro.",
            "- Custos reais, spread e execucao manual podem piorar o resultado do backtest.",
            "- Em opcoes, `PF`, `Resultado total %` e `Media por trade %` sao mais importantes do que olhar apenas taxa de acerto.",
            "",
            "## Top 10 Para Comecar",
            "",
            "| # | Operacao | Contexto em portugues simples | Trades | Media por trade % | PF | Val PF | Resultado total % |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )

    for position, item in enumerate(shortlist[:10], start=1):
        lines.append(
            f"| {position} | {item['option_side']} ATM {item['dte_target_days']} DTE | "
            f"{_escape_md(_plain_state_signature(str(item['state_signature'])))} | "
            f"{item['total_occurrences']} | {item['average_trade_return_pct']:.4f} | "
            f"{_format_metric(item['profit_factor'])} | {_format_metric(item['validation_profit_factor'])} | "
            f"{item['net_trade_return_pct']:.4f} |"
        )

    lines.extend(
        [
            "",
            "## Ranking Geral Das Estrategias",
            "",
            "| Estrategia | Trades | Acoes | Media por trade % | Resultado total % | PF | Avg uplift % | PF uplift | Val trades | Val PF | Val resultado total % |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )

    for item in shortlist:
        lines.append(
            f"| {_escape_md(item['label'])} | {item['total_occurrences']} | {item['tickers_with_matches']} | "
            f"{item['average_trade_return_pct']:.4f} | {item['net_trade_return_pct']:.4f} | "
            f"{_format_metric(item['profit_factor'])} | {item['average_trade_uplift_pct']:.4f} | "
            f"{item['profit_factor_uplift']:.4f} | {item['validation_trades']} | "
            f"{_format_metric(item['validation_profit_factor'])} | {item['validation_net_trade_return_pct']:.4f} |"
        )

    lines.extend(
        [
            "",
            "## Acoes Mais Recorrentes Entre As Estrategias",
            "",
            "Estas sao as acoes que mais vezes apareceram nas tabelas qualificadas das estrategias finais. Elas nao sao obrigatoriamente as unicas boas, mas merecem atencao especial porque se repetem em varias leituras.",
            "",
            "| Acao | Quantas estrategias aparece |",
            "| --- | ---: |",
        ]
    )

    for ticker, count in recurring_tickers.most_common(25):
        lines.append(f"| {_escape_md(ticker)} | {count} |")

    lines.extend(
        [
            "",
            "## Acoes Que Mais Carregaram Resultado No Conjunto",
            "",
            "A tabela abaixo soma a presenca das acoes em todas as estrategias da shortlist. Ela ajuda a responder quais papeis aparecem bastante e, ao mesmo tempo, entregaram mais resultado acumulado dentro do estudo.",
            "",
            "| Acao | Estrategias onde aparece | Trades somados | Resultado total somado % | Media das medias % | Melhor resultado isolado % |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )

    for item in aggregate_tickers[:20]:
        lines.append(
            f"| {_escape_md(item['ticker'])} | {item['strategy_count']} | {item['total_trades']} | "
            f"{float(item['sum_net_trade_return_pct']):.4f} | {float(item['average_trade_return_pct']):.4f} | "
            f"{float(item['best_net_trade_return_pct']):.4f} |"
        )

    lines.extend(
        [
            "",
            "## Estrategias Explicadas Uma a Uma",
            "",
        ]
    )

    for position, item in enumerate(shortlist, start=1):
        strategy_rows = tickers_by_strategy.get(str(item["code"]), [])
        playbook = _playbook(item)
        visible_rows = strategy_rows if top_tickers_per_strategy is None else strategy_rows[:top_tickers_per_strategy]

        lines.extend(
            [
                f"### {position}. {item['label']}",
                "",
                "#### Resumo Rapido",
                "",
                f"Quando o papel entra neste contexto, o estudo mostrou vantagem estatistica para `{playbook['action']}`. A entrada sempre acontece `{playbook['entry']}` e a saida sempre acontece `{playbook['exit']}`.",
                "",
                f"- Em uma frase: {_strategy_one_liner(item)}",
                "",
                "#### O Que Precisa Acontecer No Papel",
                "",
                f"- Leitura simples do contexto: {_plain_state_signature(str(item['state_signature']))}",
                f"- Assinatura tecnica do estado: `{item['state_signature']}`",
                "",
                "#### Exemplo Ilustrativo",
                "",
                f"- {_strategy_example(item, strategy_rows)}",
                "",
                "#### Como Operar",
                "",
                f"- Tipo de contrato: `{playbook['contract']}`.",
                f"- Quando entrar: `{playbook['entry']}`, depois de confirmar que a acao se encaixou no contexto acima.",
                f"- Quando sair: `{playbook['exit']}`.",
                f"- Duracao da operacao: `{playbook['duration']}`.",
                f"- Leitura direcional: foco em `{playbook['directional_read']}` via valorizacao do premio da opcao.",
                f"- Regra resumida: se a acao entrar neste contexto, a ideia e `{playbook['action']}` e zerar `{playbook['exit']}`.",
                "",
                "#### Passo A Passo Operacional",
                "",
                "1. Escolha uma acao da tabela desta estrategia.",
                "2. Confirme se ela realmente entrou no contexto descrito.",
                f"3. Na abertura, execute `{playbook['action']}`.",
                "4. Acompanhe a operacao ao longo do dia apenas para execucao.",
                f"5. No fechamento, saia da posicao. A regra desta estrategia nao e carregar para o dia seguinte.",
                "",
                "#### O Que O Backtest Mostrou",
                "",
                f"- Acoes diferentes com ocorrencia: `{item['tickers_with_matches']}`",
                f"- Trades totais: `{item['total_occurrences']}`",
                f"- Trades positivos: `{item['profitable_trade_rate_pct']:.2f}%`",
                f"- Media por trade: `{item['average_trade_return_pct']:.4f}%`",
                f"- Resultado total somado no backtest: `{item['net_trade_return_pct']:.4f}%`",
                f"- Profit factor: `{_format_metric(item['profit_factor'])}`",
                "",
                "#### Por Que Ela Entrou Na Shortlist",
                "",
                f"- Template base: `{item['template_label']}`",
                f"- Media por trade do template base: `{item['baseline_average_trade_return_pct']:.4f}%`",
                f"- PF do template base: `{_format_metric(item['baseline_profit_factor'])}`",
                f"- Melhora em media por trade contra o template base: `{item['average_trade_uplift_pct']:.4f}%`",
                f"- Melhora em PF contra o template base: `{item['profit_factor_uplift']:.4f}`",
                "",
                "#### Treino e Validacao",
                "",
                f"- Treino: `{item['train_trades']}` trades | media `{item['train_average_trade_return_pct']:.4f}%` | resultado total `{item['train_net_trade_return_pct']:.4f}%` | PF `{_format_metric(item['train_profit_factor'])}`",
                f"- Validacao: `{item['validation_trades']}` trades | media `{item['validation_average_trade_return_pct']:.4f}%` | resultado total `{item['validation_net_trade_return_pct']:.4f}%` | PF `{_format_metric(item['validation_profit_factor'])}`",
                f"- Leitura dessa comparacao: {_training_validation_comment(item)}",
                "",
                "#### Estabilidade",
                "",
                f"- {_monthly_stability_comment(item)}",
                f"- Bucket de sobreposicao: `{item['overlap_bucket']}`",
                f"- Score de robustez: `{item['robustness_score']:.4f}`",
                "",
                "#### Acoes Para Priorizar",
                "",
                f"- Acoes que mais sustentaram esta estrategia: {_top_tickers_summary(strategy_rows, top=5)}",
                "- Leitura pratica: comece pelas primeiras linhas da tabela abaixo, porque elas combinam melhor historico com resultado dentro desta estrategia.",
                "",
                "#### Tabela Completa Por Acao",
                "",
                f"A tabela abaixo mostra as acoes que passaram no filtro por acao dentro desta estrategia. Aqui voce enxerga quais papeis realmente puxaram o resultado e em quais datas esse comportamento apareceu. Neste relatorio, o corte minimo por acao e de `{ticker_min_trades}` trades.",
                "",
            ]
        )

        if visible_rows:
            lines.extend(_build_ticker_table_rows(visible_rows))
        else:
            lines.append("- Nenhuma acao qualificada no CSV informado.")

        lines.extend(
            [
                "",
                "#### Dados Tecnicos Do Estudo",
                "",
                f"- Codigo interno: `{item['code']}`",
                f"- Estado com {item['state_size']} fatores: `{item['feature_keys']}`",
                "",
            ]
        )

    lines.extend(
        [
            "## Fechamento",
            "",
            "Se uma pessoa ler apenas este arquivo, ela deve sair com quatro respostas claras:",
            "",
            "1. O que foi testado: opcoes ATM intraday, com entrada na abertura e saida no fechamento.",
            "2. Quais estrategias sobreviveram ao funil de discovery e validacao.",
            "3. Em quais acoes cada estrategia se sustentou melhor.",
            "4. Como aplicar o setup na pratica, sem precisar adivinhar entrada, contrato ou saida.",
            "",
            "Para analise complementar ou auditoria, os CSVs continuam sendo a base detalhada do projeto.",
            "",
        ]
    )

    output_path = Path(output_md)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")
    return output_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gera relatorio Markdown didatico da super-shortlist refinada de discovery.",
    )
    parser.add_argument(
        "--shortlist-csv",
        default="reports/options-discovery-super-shortlist.csv",
    )
    parser.add_argument(
        "--ticker-summary-csv",
        default="reports/options-discovery-refined-tickers-qualified.csv",
    )
    parser.add_argument(
        "--output-md",
        default="reports/options-discovery-super-shortlist-relatorio.md",
    )
    parser.add_argument(
        "--start-date",
        default="2025-04-09",
    )
    parser.add_argument(
        "--end-date",
        default="2026-04-09",
    )
    parser.add_argument(
        "--validation-start-date",
        default="2026-01-01",
    )
    parser.add_argument(
        "--top-tickers-per-strategy",
        type=int,
        default=None,
    )
    parser.add_argument(
        "--ticker-min-trades",
        type=int,
        default=15,
    )
    parser.add_argument(
        "--tested-patterns",
        type=int,
        default=5505,
    )
    parser.add_argument(
        "--raw-approved-patterns",
        type=int,
        default=3029,
    )
    parser.add_argument(
        "--refined-patterns",
        type=int,
        default=406,
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    output_path = generate_report(
        shortlist_csv=args.shortlist_csv,
        ticker_summary_csv=args.ticker_summary_csv,
        output_md=args.output_md,
        start_date=args.start_date,
        end_date=args.end_date,
        validation_start_date=args.validation_start_date,
        top_tickers_per_strategy=args.top_tickers_per_strategy,
        ticker_min_trades=args.ticker_min_trades,
        tested_patterns=args.tested_patterns,
        raw_approved_patterns=args.raw_approved_patterns,
        refined_patterns=args.refined_patterns,
    )
    print(f"Relatorio da shortlist exportado para {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
