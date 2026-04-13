from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path

from .analysis import (
    analyze_patterns,
    backtest_strategy_grid,
    build_strategy_definitions,
    filter_strategy_ticker_results,
    split_strategy_results,
    summarize_trades_by_ticker,
)
from .asset_monitor import export_asset_monitor_payload
from .asset_discovery_round1 import (
    build_asset_discovery_atr_templates,
    build_asset_discovery_registry_entries,
    build_asset_discovery_round1_templates,
    collect_asset_discovery_pattern_trades,
    default_asset_discovery_window,
    export_asset_discovery_csv,
    export_asset_discovery_markdown,
    list_asset_discovery_features,
    mine_asset_discovery_patterns,
    mine_asset_discovery_patterns_progressive,
    render_asset_discovery_report,
    split_asset_discovery_results,
)
from .cotahist import sync_cotahist_history
from .discovery import (
    build_discovery_registry_entries,
    build_option_discovery_templates,
    collect_discovery_pattern_trades,
    default_discovery_window,
    list_discovery_features,
    mine_option_discovery_patterns,
    split_discovery_results,
)
from .discovery_refinement import (
    load_discovery_summaries,
    refine_discovery_shortlist,
    summarize_template_baselines,
)
from .ingestion import is_sync_stale, sync_history
from .options import backtest_option_strategies, build_option_strategy_definitions
from .registry import (
    build_registry_entries,
    get_known_strategy_codes,
    load_registry_entries,
    merge_registry_entries,
)
from .reporting import (
    export_csv,
    export_discovery_csv,
    export_discovery_markdown,
    export_refined_discovery_csv,
    export_refined_discovery_markdown,
    export_strategy_registry_csv,
    export_strategy_registry_markdown,
    export_strategy_csv,
    export_strategy_ticker_csv,
    export_strategy_trades_csv,
    render_refined_discovery_report,
    render_discovery_report,
    render_console_report,
    render_strategy_report,
)
from .tickers import load_tickers


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="b3_patterns",
        description="Minerador de padroes de precos da B3 usando SQLite local.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    sync_parser = subparsers.add_parser(
        "sync",
        help="Baixa cotacoes da Yahoo Finance e atualiza o banco local.",
    )
    _add_db_argument(sync_parser)
    _add_sync_arguments(sync_parser)

    analyze_parser = subparsers.add_parser(
        "analyze",
        help="Roda a analise somente com dados locais do SQLite.",
    )
    _add_db_argument(analyze_parser)
    _add_analysis_arguments(analyze_parser)

    strategies_parser = subparsers.add_parser(
        "strategies",
        help="Varre varias estrategias de queda e alta somente com dados locais.",
    )
    _add_db_argument(strategies_parser)
    _add_strategy_arguments(strategies_parser)

    options_sync_parser = subparsers.add_parser(
        "options-sync",
        help="Baixa e importa COTAHIST da B3 para backtest de opcoes.",
    )
    _add_db_argument(options_sync_parser)
    _add_options_sync_arguments(options_sync_parser)

    options_backtest_parser = subparsers.add_parser(
        "options-backtest",
        help="Roda o backtest de opcoes ATM com memoria propria.",
    )
    _add_db_argument(options_backtest_parser)
    _add_options_backtest_arguments(options_backtest_parser)

    options_discover_parser = subparsers.add_parser(
        "options-discover",
        help="Mineracao probabilistica de estados de mercado para opcoes ATM D0.",
    )
    _add_db_argument(options_discover_parser)
    _add_options_discovery_arguments(options_discover_parser)

    options_discover_refine_parser = subparsers.add_parser(
        "options-discover-refine",
        help="Refina o discovery probabilistico com baseline, estabilidade e validacao.",
    )
    _add_db_argument(options_discover_refine_parser)
    _add_options_discovery_refine_arguments(options_discover_refine_parser)

    asset_discovery_round1_parser = subparsers.add_parser(
        "asset-discover-round1",
        help="Rodada 1 do discovery quantitativo em acoes com saidas percentuais.",
    )
    _add_db_argument(asset_discovery_round1_parser)
    _add_asset_discovery_round1_arguments(asset_discovery_round1_parser)

    asset_discovery_r3_parser = subparsers.add_parser(
        "asset-discover-r3",
        help="Rodada 3: discovery pesado com 50 features, ATR exits, mineracao progressiva 2f->3f.",
    )
    _add_db_argument(asset_discovery_r3_parser)
    _add_asset_discovery_r3_arguments(asset_discovery_r3_parser)

    asset_monitor_export_parser = subparsers.add_parser(
        "asset-monitor-export",
        help="Exporta os sinais atuais das estrategias finais da Rodada 1 para o dashboard web.",
    )
    _add_db_argument(asset_monitor_export_parser)
    _add_asset_monitor_export_arguments(asset_monitor_export_parser)

    run_parser = subparsers.add_parser(
        "run",
        help="Sincroniza se necessario e gera o ranking consolidado.",
    )
    _add_db_argument(run_parser)
    _add_sync_arguments(run_parser)
    _add_analysis_arguments(run_parser)
    run_parser.add_argument(
        "--force-sync",
        action="store_true",
        help="Forca sincronizacao antes da analise, mesmo com banco atualizado.",
    )

    return parser


def _add_db_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--db-path",
        default="b3_history.db",
        help="Caminho do banco SQLite local.",
    )


def _add_sync_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--tickers-file",
        default="acoes-listadas-b3.csv",
        help="CSV com a coluna Ticker.",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=90,
        help="Quantidade de pregoes a persistir por ativo.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=8,
        help="Quantidade de downloads paralelos.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limita a quantidade de tickers carregados do CSV.",
    )


def _add_analysis_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--trigger-change-pct",
        type=float,
        default=-2.0,
        help="Gatilho do Dia T em percentual. Ex.: -2.0 para queda de 2 por cento.",
    )
    parser.add_argument(
        "--target-next-day-pct",
        type=float,
        default=0.0,
        help="Retorno esperado para o Dia T+1 em percentual.",
    )
    parser.add_argument(
        "--target-mode",
        choices=["auto", "up", "down"],
        default="auto",
        help="Direcao esperada para o Dia T+1. 'auto' usa o sinal do percentual informado.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=20,
        help="Quantidade de ativos exibidos no ranking do console.",
    )
    parser.add_argument(
        "--output-csv",
        default=None,
        help="Caminho opcional para exportar o ranking em CSV.",
    )


def _add_strategy_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--levels",
        nargs="+",
        type=float,
        default=[1.0, 2.0, 3.0, 4.0, 5.0],
        help="Niveis percentuais a testar. Ex.: --levels 1 2 3",
    )
    parser.add_argument(
        "--min-trade-return-pct",
        type=float,
        default=0.0,
        help="Retorno minimo por trade para contar a estrategia como acerto.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=20,
        help="Quantidade de estrategias exibidas no console.",
    )
    parser.add_argument(
        "--min-success-rate",
        type=float,
        default=55.0,
        help="Filtro minimo de taxa de acerto para manter a estrategia.",
    )
    parser.add_argument(
        "--min-profit-factor",
        type=float,
        default=1.0,
        help="Filtro minimo de profit factor para manter a estrategia.",
    )
    parser.add_argument(
        "--min-average-trade-return-pct",
        type=float,
        default=0.10,
        help="Filtro minimo de retorno medio por trade para manter a estrategia.",
    )
    parser.add_argument(
        "--min-trades",
        type=int,
        default=200,
        help="Filtro minimo de operacoes para manter a estrategia.",
    )
    parser.add_argument(
        "--allow-negative-net",
        action="store_true",
        help="Mantem estrategias com lucro liquido negativo no resultado.",
    )
    parser.add_argument(
        "--include-known",
        action="store_true",
        help="Reavalia estrategias que ja estao registradas na memoria.",
    )
    parser.add_argument(
        "--output-csv",
        default=None,
        help="Caminho opcional para exportar o ranking de estrategias em CSV.",
    )
    parser.add_argument(
        "--rejected-csv",
        default="reports/strategies-rejected.csv",
        help="Caminho para exportar o ranking das estrategias reprovadas.",
    )
    parser.add_argument(
        "--trades-csv",
        default=None,
        help="Caminho opcional para exportar o log de operacoes do backtest.",
    )
    parser.add_argument(
        "--ticker-summary-csv",
        default=None,
        help="Caminho opcional para exportar o consolidado por estrategia e acao.",
    )
    parser.add_argument(
        "--ticker-qualified-csv",
        default=None,
        help="Caminho opcional para exportar apenas as acoes aprovadas por estrategia.",
    )
    parser.add_argument(
        "--ticker-min-success-rate",
        type=float,
        default=55.0,
        help="Filtro minimo de taxa de acerto no relatorio por acao.",
    )
    parser.add_argument(
        "--ticker-min-profit-factor",
        type=float,
        default=1.0,
        help="Filtro minimo de profit factor no relatorio por acao.",
    )
    parser.add_argument(
        "--ticker-min-average-trade-return-pct",
        type=float,
        default=0.50,
        help="Filtro minimo de retorno medio por trade no relatorio por acao.",
    )
    parser.add_argument(
        "--ticker-min-trades",
        type=int,
        default=15,
        help="Filtro minimo de trades no relatorio por acao.",
    )
    parser.add_argument(
        "--registry-csv",
        default="reports/strategy-registry.csv",
        help="Caminho da memoria persistente com estrategias ja testadas.",
    )
    parser.add_argument(
        "--registry-md",
        default="reports/strategy-memory.md",
        help="Resumo em Markdown da memoria de estrategias testadas.",
    )


def _add_options_sync_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--tickers-file",
        default="acoes-listadas-b3.csv",
        help="CSV com o universo de acoes para mapear opcoes.",
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=None,
        help="Anos do COTAHIST a importar. Ex.: --years 2025 2026",
    )
    parser.add_argument(
        "--cotahist-files",
        nargs="+",
        default=None,
        help="Arquivos COTAHIST locais (.zip ou .txt).",
    )
    parser.add_argument(
        "--data-dir",
        default="data/cotahist",
        help="Diretorio para armazenar os arquivos baixados.",
    )
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="Nao baixa arquivos faltantes. Usa apenas os arquivos informados.",
    )


def _add_options_backtest_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--tickers-file",
        default="acoes-listadas-b3.csv",
        help="CSV com o universo de acoes para mapear opcoes.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Data inicial do backtest no formato YYYY-MM-DD. Padrao: 1 ano atras.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Data final do backtest no formato YYYY-MM-DD. Padrao: hoje.",
    )
    parser.add_argument(
        "--min-trade-return-pct",
        type=float,
        default=0.0,
        help="Retorno minimo por trade para contar a estrategia como acerto.",
    )
    parser.add_argument(
        "--round-trip-cost-pct",
        type=float,
        default=0.0,
        help="Custo percentual total por operacao de opcao.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=20,
        help="Quantidade de estrategias exibidas no console.",
    )
    parser.add_argument(
        "--min-success-rate",
        type=float,
        default=55.0,
        help="Filtro minimo de taxa de acerto para manter a estrategia.",
    )
    parser.add_argument(
        "--min-profit-factor",
        type=float,
        default=1.10,
        help="Filtro minimo de profit factor para manter a estrategia.",
    )
    parser.add_argument(
        "--min-average-trade-return-pct",
        type=float,
        default=0.0,
        help="Filtro minimo de retorno medio por trade para manter a estrategia.",
    )
    parser.add_argument(
        "--min-trades",
        type=int,
        default=10,
        help="Filtro minimo de operacoes para manter a estrategia.",
    )
    parser.add_argument(
        "--include-known",
        action="store_true",
        help="Reavalia estrategias de opcoes ja registradas na memoria.",
    )
    parser.add_argument(
        "--output-csv",
        default="reports/options-strategies-approved.csv",
        help="CSV das estrategias de opcoes aprovadas.",
    )
    parser.add_argument(
        "--rejected-csv",
        default="reports/options-strategies-rejected.csv",
        help="CSV das estrategias de opcoes reprovadas.",
    )
    parser.add_argument(
        "--trades-csv",
        default="reports/options-trades-approved.csv",
        help="CSV do log de trades aprovados de opcoes.",
    )
    parser.add_argument(
        "--ticker-summary-csv",
        default="reports/options-tickers-all.csv",
        help="Consolidado por estrategia e acao para opcoes.",
    )
    parser.add_argument(
        "--ticker-qualified-csv",
        default="reports/options-tickers-approved.csv",
        help="Consolidado filtrado por estrategia e acao para opcoes.",
    )
    parser.add_argument(
        "--ticker-min-success-rate",
        type=float,
        default=55.0,
        help="Filtro minimo de taxa de acerto no relatorio por acao.",
    )
    parser.add_argument(
        "--ticker-min-profit-factor",
        type=float,
        default=1.10,
        help="Filtro minimo de profit factor no relatorio por acao.",
    )
    parser.add_argument(
        "--ticker-min-average-trade-return-pct",
        type=float,
        default=0.0,
        help="Filtro minimo de retorno medio por trade no relatorio por acao.",
    )
    parser.add_argument(
        "--ticker-min-trades",
        type=int,
        default=5,
        help="Filtro minimo de trades no relatorio por acao.",
    )
    parser.add_argument(
        "--registry-csv",
        default="reports/options-strategy-registry.csv",
        help="Memoria persistente das estrategias de opcoes testadas.",
    )
    parser.add_argument(
        "--registry-md",
        default="reports/options-strategy-memory.md",
        help="Resumo em Markdown da memoria de estrategias de opcoes.",
    )


def _add_options_discovery_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--tickers-file",
        default="acoes-listadas-b3.csv",
        help="CSV com o universo de acoes para mapear opcoes.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Data inicial do discovery no formato YYYY-MM-DD. Padrao: 1 ano atras.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Data final do discovery no formato YYYY-MM-DD. Padrao: hoje.",
    )
    parser.add_argument(
        "--dte-targets",
        nargs="+",
        type=int,
        default=[7, 15, 30],
        help="Buckets de vencimento ATM a testar. Ex.: --dte-targets 7 15 30",
    )
    parser.add_argument(
        "--min-trade-return-pct",
        type=float,
        default=0.0,
        help="Retorno minimo por trade para contar o padrao como acerto.",
    )
    parser.add_argument(
        "--round-trip-cost-pct",
        type=float,
        default=0.0,
        help="Custo percentual total por operacao de opcao.",
    )
    parser.add_argument(
        "--max-pattern-size",
        type=int,
        default=2,
        choices=[1, 2],
        help="Quantidade maxima de fatores por padrao descoberto.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=20,
        help="Quantidade de padroes exibidos no console.",
    )
    parser.add_argument(
        "--progress-every-tickers",
        type=int,
        default=25,
        help="Mostra progresso simples a cada N acoes processadas. Use 0 para desligar.",
    )
    parser.add_argument(
        "--min-success-rate",
        type=float,
        default=0.0,
        help="Filtro minimo de taxa de acerto para manter o padrao.",
    )
    parser.add_argument(
        "--min-profit-factor",
        type=float,
        default=1.15,
        help="Filtro minimo de profit factor para manter o padrao.",
    )
    parser.add_argument(
        "--min-average-trade-return-pct",
        type=float,
        default=0.0,
        help="Filtro minimo de retorno medio por trade para manter o padrao.",
    )
    parser.add_argument(
        "--min-trades",
        type=int,
        default=50,
        help="Filtro minimo de operacoes para manter o padrao.",
    )
    parser.add_argument(
        "--min-tickers",
        type=int,
        default=5,
        help="Filtro minimo de acoes diferentes para manter o padrao.",
    )
    parser.add_argument(
        "--allow-negative-net",
        action="store_true",
        help="Mantem padroes com lucro liquido negativo no resultado.",
    )
    parser.add_argument(
        "--include-known",
        action="store_true",
        help="Reavalia padroes probabilisticos ja registrados na memoria.",
    )
    parser.add_argument(
        "--output-csv",
        default="reports/options-discovery-approved.csv",
        help="CSV dos padroes aprovados.",
    )
    parser.add_argument(
        "--rejected-csv",
        default="reports/options-discovery-rejected.csv",
        help="CSV dos padroes reprovados.",
    )
    parser.add_argument(
        "--trades-csv",
        default="reports/options-discovery-trades-approved.csv",
        help="CSV do log de trades dos padroes aprovados.",
    )
    parser.add_argument(
        "--ticker-summary-csv",
        default="reports/options-discovery-tickers-all.csv",
        help="Consolidado por padrao e acao.",
    )
    parser.add_argument(
        "--ticker-qualified-csv",
        default="reports/options-discovery-tickers-qualified.csv",
        help="Consolidado filtrado por padrao e acao.",
    )
    parser.add_argument(
        "--ticker-min-success-rate",
        type=float,
        default=0.0,
        help="Filtro minimo de taxa de acerto no relatorio por acao.",
    )
    parser.add_argument(
        "--ticker-min-profit-factor",
        type=float,
        default=1.15,
        help="Filtro minimo de profit factor no relatorio por acao.",
    )
    parser.add_argument(
        "--ticker-min-average-trade-return-pct",
        type=float,
        default=0.0,
        help="Filtro minimo de retorno medio por trade no relatorio por acao.",
    )
    parser.add_argument(
        "--ticker-min-trades",
        type=int,
        default=5,
        help="Filtro minimo de trades no relatorio por acao.",
    )
    parser.add_argument(
        "--registry-csv",
        default="reports/options-discovery-registry.csv",
        help="Memoria persistente dos padroes probabilisticos testados.",
    )
    parser.add_argument(
        "--registry-md",
        default="reports/options-discovery-memory.md",
        help="Resumo em Markdown da memoria dos padroes probabilisticos.",
    )
    parser.add_argument(
        "--summary-md",
        default="reports/options-discovery-summary.md",
        help="Resumo executivo em Markdown do lote atual.",
    )


def _add_options_discovery_refine_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--tickers-file",
        default="acoes-listadas-b3.csv",
        help="CSV com o universo de acoes para mapear opcoes.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Data inicial da janela base no formato YYYY-MM-DD. Padrao: 1 ano atras.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Data final da janela base no formato YYYY-MM-DD. Padrao: hoje.",
    )
    parser.add_argument(
        "--validation-start-date",
        default="2026-01-01",
        help="Data de inicio da janela de validacao fora da amostra.",
    )
    parser.add_argument(
        "--dte-targets",
        nargs="+",
        type=int,
        default=[7, 15],
        help="Buckets de vencimento usados no baseline. Ex.: --dte-targets 7 15",
    )
    parser.add_argument(
        "--round-trip-cost-pct",
        type=float,
        default=0.0,
        help="Custo percentual total por operacao de opcao.",
    )
    parser.add_argument(
        "--input-csv",
        default="reports/options-discovery-approved.csv",
        help="CSV bruto de aprovados do discovery.",
    )
    parser.add_argument(
        "--trades-csv",
        default="reports/options-discovery-trades-approved.csv",
        help="CSV bruto de trades aprovados do discovery.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=20,
        help="Quantidade de padroes exibidos no console.",
    )
    parser.add_argument(
        "--min-state-size",
        type=int,
        default=2,
        help="Quantidade minima de fatores no padrao.",
    )
    parser.add_argument(
        "--min-avg-uplift-pct",
        type=float,
        default=1.0,
        help="Melhora minima de retorno medio sobre o baseline do template.",
    )
    parser.add_argument(
        "--min-pf-uplift",
        type=float,
        default=0.20,
        help="Melhora minima de profit factor sobre o baseline do template.",
    )
    parser.add_argument(
        "--min-total-trades",
        type=int,
        default=300,
        help="Quantidade minima total de trades do padrao.",
    )
    parser.add_argument(
        "--min-tickers",
        type=int,
        default=30,
        help="Quantidade minima de acoes distintas no padrao.",
    )
    parser.add_argument(
        "--min-train-trades",
        type=int,
        default=150,
        help="Quantidade minima de trades na janela de treino.",
    )
    parser.add_argument(
        "--min-validation-trades",
        type=int,
        default=40,
        help="Quantidade minima de trades na janela de validacao.",
    )
    parser.add_argument(
        "--min-train-profit-factor",
        type=float,
        default=1.10,
        help="Profit factor minimo na janela de treino.",
    )
    parser.add_argument(
        "--min-validation-profit-factor",
        type=float,
        default=1.10,
        help="Profit factor minimo na janela de validacao.",
    )
    parser.add_argument(
        "--min-train-average-trade-return-pct",
        type=float,
        default=0.0,
        help="Retorno medio minimo na janela de treino.",
    )
    parser.add_argument(
        "--min-validation-average-trade-return-pct",
        type=float,
        default=0.0,
        help="Retorno medio minimo na janela de validacao.",
    )
    parser.add_argument(
        "--min-active-months",
        type=int,
        default=6,
        help="Meses ativos minimos para estabilidade global.",
    )
    parser.add_argument(
        "--min-positive-month-ratio",
        type=float,
        default=0.55,
        help="Proporcao minima de meses positivos na janela total.",
    )
    parser.add_argument(
        "--min-validation-active-months",
        type=int,
        default=2,
        help="Meses ativos minimos na validacao.",
    )
    parser.add_argument(
        "--min-validation-positive-month-ratio",
        type=float,
        default=0.50,
        help="Proporcao minima de meses positivos na validacao.",
    )
    parser.add_argument(
        "--overlap-threshold",
        type=float,
        default=0.85,
        help="Sobreposicao maxima permitida para manter dois padroes no shortlist.",
    )
    parser.add_argument(
        "--output-csv",
        default="reports/options-discovery-refined.csv",
        help="CSV final da shortlist refinada.",
    )
    parser.add_argument(
        "--summary-md",
        default="reports/options-discovery-refined.md",
        help="Resumo Markdown da shortlist refinada.",
    )
    parser.add_argument(
        "--shortlist-trades-csv",
        default="reports/options-discovery-refined-trades.csv",
        help="CSV do log de trades da shortlist refinada.",
    )
    parser.add_argument(
        "--ticker-summary-csv",
        default="reports/options-discovery-refined-tickers.csv",
        help="Consolidado por padrao e acao da shortlist refinada.",
    )
    parser.add_argument(
        "--ticker-qualified-csv",
        default="reports/options-discovery-refined-tickers-qualified.csv",
        help="Consolidado filtrado por acao da shortlist refinada.",
    )
    parser.add_argument(
        "--ticker-min-success-rate",
        type=float,
        default=0.0,
        help="Filtro minimo de taxa de acerto no relatorio por acao.",
    )
    parser.add_argument(
        "--ticker-min-profit-factor",
        type=float,
        default=1.10,
        help="Filtro minimo de profit factor no relatorio por acao.",
    )
    parser.add_argument(
        "--ticker-min-average-trade-return-pct",
        type=float,
        default=0.0,
        help="Filtro minimo de retorno medio por trade no relatorio por acao.",
    )
    parser.add_argument(
        "--ticker-min-trades",
        type=int,
        default=5,
        help="Filtro minimo de trades no relatorio por acao.",
    )


def _add_asset_discovery_round1_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--tickers-file",
        default="acoes-listadas-b3.csv",
        help="CSV com o universo de acoes da rodada 1.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Data inicial da janela base no formato YYYY-MM-DD. Padrao: 1 ano atras.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Data final da janela base no formato YYYY-MM-DD. Padrao: hoje.",
    )
    parser.add_argument(
        "--entry-rules",
        nargs="+",
        default=["open", "close"],
        help="Regras de entrada a avaliar. Ex.: --entry-rules open close",
    )
    parser.add_argument(
        "--trade-directions",
        nargs="+",
        choices=["long", "short"],
        default=["long", "short"],
        help="Direcoes a avaliar. Use long para compra e short para venda.",
    )
    parser.add_argument(
        "--target-stop-pairs",
        nargs="+",
        default=["1:1", "2:1", "3:1.5", "4:2", "6:3"],
        help="Pares alvo:stop em percentual. Ex.: --target-stop-pairs 2:1 4:2",
    )
    parser.add_argument(
        "--time-cap-days",
        type=int,
        default=5,
        help="Cap maximo em dias para encerrar operacoes que nao baterem TP/SL.",
    )
    parser.add_argument(
        "--round-trip-cost-pct",
        type=float,
        default=0.0,
        help="Custo percentual total por operacao na acao.",
    )
    parser.add_argument(
        "--max-pattern-size",
        type=int,
        default=2,
        help="Quantidade maxima de fatores por padrao.",
    )
    parser.add_argument(
        "--min-trade-return-pct",
        type=float,
        default=0.0,
        help="Retorno minimo por trade para contar acerto do padrao.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=20,
        help="Quantidade de padroes exibidos no console.",
    )
    parser.add_argument(
        "--progress-every-tickers",
        type=int,
        default=25,
        help="Mostra progresso simples a cada N acoes processadas. Use 0 para desligar.",
    )
    parser.add_argument(
        "--min-success-rate",
        type=float,
        default=0.0,
        help="Filtro minimo de taxa de acerto para manter o padrao.",
    )
    parser.add_argument(
        "--min-take-profit-rate",
        type=float,
        default=0.0,
        help="Filtro minimo de taxa de alvo batido para manter o padrao.",
    )
    parser.add_argument(
        "--min-profit-factor",
        type=float,
        default=1.10,
        help="Filtro minimo de profit factor para manter o padrao.",
    )
    parser.add_argument(
        "--min-average-trade-return-pct",
        type=float,
        default=0.0,
        help="Filtro minimo de retorno medio por trade para manter o padrao.",
    )
    parser.add_argument(
        "--min-trades",
        type=int,
        default=80,
        help="Quantidade minima de ocorrencias para manter o padrao.",
    )
    parser.add_argument(
        "--min-tickers",
        type=int,
        default=8,
        help="Quantidade minima de acoes distintas para manter o padrao.",
    )
    parser.add_argument(
        "--allow-negative-net",
        action="store_true",
        help="Mantem padroes com lucro liquido negativo no resultado.",
    )
    parser.add_argument(
        "--include-known",
        action="store_true",
        help="Reavalia padroes da Rodada 1 ja presentes na memoria.",
    )
    parser.add_argument(
        "--output-csv",
        default="reports/asset-discovery-round1-approved.csv",
        help="CSV dos padroes aprovados da Rodada 1.",
    )
    parser.add_argument(
        "--rejected-csv",
        default="reports/asset-discovery-round1-rejected.csv",
        help="CSV dos padroes reprovados da Rodada 1.",
    )
    parser.add_argument(
        "--trades-csv",
        default="reports/asset-discovery-round1-trades-approved.csv",
        help="CSV do log de trades aprovados da Rodada 1.",
    )
    parser.add_argument(
        "--ticker-summary-csv",
        default="reports/asset-discovery-round1-tickers-all.csv",
        help="Consolidado por padrao e acao da Rodada 1.",
    )
    parser.add_argument(
        "--ticker-qualified-csv",
        default="reports/asset-discovery-round1-tickers-qualified.csv",
        help="Consolidado filtrado por padrao e acao da Rodada 1.",
    )
    parser.add_argument(
        "--ticker-min-success-rate",
        type=float,
        default=0.0,
        help="Filtro minimo de taxa de acerto no relatorio por acao.",
    )
    parser.add_argument(
        "--ticker-min-profit-factor",
        type=float,
        default=1.10,
        help="Filtro minimo de profit factor no relatorio por acao.",
    )
    parser.add_argument(
        "--ticker-min-average-trade-return-pct",
        type=float,
        default=0.0,
        help="Filtro minimo de retorno medio por trade no relatorio por acao.",
    )
    parser.add_argument(
        "--ticker-min-trades",
        type=int,
        default=15,
        help="Filtro minimo de trades no relatorio por acao.",
    )
    parser.add_argument(
        "--registry-csv",
        default="reports/asset-discovery-round1-registry.csv",
        help="Memoria persistente dos padroes ja testados na Rodada 1.",
    )
    parser.add_argument(
        "--registry-md",
        default="reports/asset-discovery-round1-memory.md",
        help="Resumo em Markdown da memoria dos padroes testados.",
    )
    parser.add_argument(
        "--summary-md",
        default="reports/asset-discovery-round1-summary.md",
        help="Resumo Markdown do discovery da Rodada 1.",
    )


def _add_asset_discovery_r3_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--tickers-file",
        default="acoes-listadas-b3.csv",
        help="CSV com o universo de acoes.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Data inicial no formato YYYY-MM-DD. Padrao: 1 ano atras.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Data final no formato YYYY-MM-DD. Padrao: hoje.",
    )
    parser.add_argument(
        "--entry-rules",
        nargs="+",
        default=["open", "close"],
        help="Regras de entrada. Ex.: --entry-rules open close",
    )
    parser.add_argument(
        "--trade-directions",
        nargs="+",
        choices=["long", "short"],
        default=["long", "short"],
        help="Direcoes a avaliar.",
    )
    parser.add_argument(
        "--target-stop-pairs",
        nargs="+",
        default=["3:3", "3:4", "4:4"],
        help="Pares alvo:stop percentuais. Ex.: --target-stop-pairs 3:3 4:4",
    )
    parser.add_argument(
        "--atr-target-stop-pairs",
        nargs="+",
        default=["1.5:1", "2:1.5"],
        help="Pares alvo:stop ATR multiplos. Ex.: --atr-target-stop-pairs 1.5:1 2:1.5",
    )
    parser.add_argument(
        "--time-cap-days",
        type=int,
        default=10,
        help="Cap maximo em dias.",
    )
    parser.add_argument(
        "--atr-time-cap-days",
        nargs="+",
        type=int,
        default=[5, 10],
        help="Caps em dias para templates ATR. Ex.: --atr-time-cap-days 5 10",
    )
    parser.add_argument(
        "--progressive",
        action="store_true",
        default=True,
        help="Usa mineracao progressiva 2f->3f (padrao: ativado).",
    )
    parser.add_argument(
        "--no-progressive",
        dest="progressive",
        action="store_false",
        help="Desativa mineracao progressiva; usa max-pattern-size direto.",
    )
    parser.add_argument(
        "--max-pattern-size",
        type=int,
        default=2,
        help="Tamanho maximo do padrao (2f no progressivo, pode subir).",
    )
    parser.add_argument(
        "--pre-filter-min-profitable-rate",
        type=float,
        default=55.0,
        help="Pre-filtro para 2f->3f: taxa de acerto minima.",
    )
    parser.add_argument(
        "--pre-filter-min-profit-factor",
        type=float,
        default=1.5,
        help="Pre-filtro para 2f->3f: profit factor minimo.",
    )
    parser.add_argument(
        "--pre-filter-min-trades",
        type=int,
        default=50,
        help="Pre-filtro para 2f->3f: trades minimos.",
    )
    parser.add_argument(
        "--max-promoted-pairs",
        type=int,
        default=150,
        help="Limite de pares 2f promovidos para expansao 3f. Reduz memoria.",
    )
    parser.add_argument(
        "--max-accumulators",
        type=int,
        default=500_000,
        help="Limite maximo de acumuladores 3f em memoria.",
    )
    parser.add_argument(
        "--prune-every-tickers",
        type=int,
        default=25,
        help="Intervalo de tickers para pruning de acumuladores fracos.",
    )
    parser.add_argument(
        "--min-success-rate",
        type=float,
        default=60.0,
        help="Filtro global: taxa de acerto minima.",
    )
    parser.add_argument(
        "--min-take-profit-rate",
        type=float,
        default=60.0,
        help="Filtro global: taxa de alvo batido minima.",
    )
    parser.add_argument(
        "--min-profit-factor",
        type=float,
        default=2.0,
        help="Filtro global: profit factor minimo.",
    )
    parser.add_argument(
        "--min-average-trade-return-pct",
        type=float,
        default=1.0,
        help="Filtro global: retorno medio minimo.",
    )
    parser.add_argument(
        "--min-trades",
        type=int,
        default=200,
        help="Filtro global: quantidade minima de trades.",
    )
    parser.add_argument(
        "--min-tickers",
        type=int,
        default=20,
        help="Filtro global: quantidade minima de acoes.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=30,
        help="Quantidade de padroes exibidos no console.",
    )
    parser.add_argument(
        "--progress-every-tickers",
        type=int,
        default=25,
        help="Mostra progresso a cada N acoes.",
    )
    parser.add_argument(
        "--include-known",
        action="store_true",
        help="Reavalia padroes ja testados.",
    )
    parser.add_argument(
        "--output-csv",
        default="reports/asset-discovery-lista-r3-stat-approved.csv",
        help="CSV dos padroes aprovados.",
    )
    parser.add_argument(
        "--rejected-csv",
        default="reports/asset-discovery-lista-r3-stat-rejected.csv",
        help="CSV dos padroes reprovados.",
    )
    parser.add_argument(
        "--trades-csv",
        default="reports/asset-discovery-lista-r3-stat-trades-approved.csv",
        help="CSV do log de trades.",
    )
    parser.add_argument(
        "--ticker-summary-csv",
        default="reports/asset-discovery-lista-r3-stat-tickers-all.csv",
        help="Consolidado por padrao e acao.",
    )
    parser.add_argument(
        "--ticker-qualified-csv",
        default="reports/asset-discovery-lista-r3-stat-tickers-qualified.csv",
        help="Consolidado filtrado por padrao e acao.",
    )
    parser.add_argument(
        "--ticker-min-trades",
        type=int,
        default=15,
        help="Filtro de trades minimos no relatorio por acao.",
    )
    parser.add_argument(
        "--ticker-min-success-rate",
        type=float,
        default=0.0,
        help="Filtro minimo de taxa de acerto no relatorio por acao.",
    )
    parser.add_argument(
        "--ticker-min-profit-factor",
        type=float,
        default=1.10,
        help="Filtro minimo de profit factor no relatorio por acao.",
    )
    parser.add_argument(
        "--ticker-min-average-trade-return-pct",
        type=float,
        default=0.0,
        help="Filtro minimo de retorno medio no relatorio por acao.",
    )
    parser.add_argument(
        "--registry-csv",
        default="reports/asset-discovery-lista-r3-stat-registry.csv",
        help="Memoria dos padroes.",
    )
    parser.add_argument(
        "--registry-md",
        default="reports/asset-discovery-lista-r3-stat-memory.md",
        help="Resumo Markdown da memoria.",
    )
    parser.add_argument(
        "--summary-md",
        default="reports/asset-discovery-lista-r3-stat-summary.md",
        help="Resumo Markdown do discovery R3.",
    )


def _add_asset_monitor_export_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--tickers-file",
        default="lista.md",
        help="CSV com o universo de acoes monitoradas.",
    )
    parser.add_argument(
        "--strategies-csv",
        default="reports/asset-discovery-lista-r3-stat-shortlist.csv",
        help="CSV com as estrategias finais que o monitor deve acompanhar.",
    )
    parser.add_argument(
        "--ticker-stats-csv",
        default="reports/asset-discovery-lista-r3-stat-shortlist-tickers.csv",
        help="CSV com os tickers qualificados por estrategia.",
    )
    parser.add_argument(
        "--overall-actions-csv",
        default="reports/asset-discovery-lista-r3-stat-shortlist-actions.csv",
        help="CSV com o consolidado geral das acoes mais fortes nas estrategias elite.",
    )
    parser.add_argument(
        "--as-of-date",
        default=None,
        help="Data de referencia no formato YYYY-MM-DD. Padrao: ultimo pregao disponivel.",
    )
    parser.add_argument(
        "--top-strategies",
        type=int,
        default=0,
        help="Quantidade de estrategias do CSV final a monitorar. Use 0 para monitorar todas.",
    )
    parser.add_argument(
        "--output-json",
        default="monitor-web/public/data/asset-monitor.json",
        help="Arquivo JSON consumido pelo app React + Vite.",
    )


def _run_sync(args: argparse.Namespace) -> int:
    tickers = load_tickers(args.tickers_file, limit=args.limit)
    summary = sync_history(
        tickers=tickers,
        db_path=args.db_path,
        window_days=args.window_days,
        max_workers=args.max_workers,
    )
    print(
        f"Sincronizacao concluida: {summary.synced_tickers}/{summary.processed_tickers} tickers gravados em {summary.database_path}."
    )
    if summary.failed_tickers:
        print("Falhas encontradas:")
        for failure in summary.failed_tickers:
            print(f" - {failure}")
    return 0 if summary.synced_tickers else 1


def _run_analyze(args: argparse.Namespace) -> int:
    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(
            f"Banco SQLite nao encontrado em {db_path}. Rode o comando `sync` antes."
        )

    results = analyze_patterns(
        db_path=db_path,
        trigger_change_pct=args.trigger_change_pct,
        target_next_day_pct=args.target_next_day_pct,
        target_mode=args.target_mode,
    )
    print(render_console_report(results, top=args.top))

    if args.output_csv:
        exported_path = export_csv(results, args.output_csv)
        print(f"CSV exportado para {exported_path}")

    return 0


def _run_strategies(args: argparse.Namespace) -> int:
    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(
            f"Banco SQLite nao encontrado em {db_path}. Rode o comando `sync` antes."
        )

    existing_registry_entries = load_registry_entries(args.registry_csv)
    strategy_definitions = build_strategy_definitions(
        threshold_levels=args.levels,
        min_trade_return_pct=args.min_trade_return_pct,
    )
    if not args.include_known:
        known_codes = get_known_strategy_codes(existing_registry_entries)
        strategy_definitions = [
            item for item in strategy_definitions if item.code not in known_codes
        ]

    if not strategy_definitions:
        print(
            f"Nenhuma estrategia nova para testar. {len(existing_registry_entries)} estrategias ja estao registradas em {args.registry_csv}."
        )
        if args.registry_md:
            exported_registry_md = export_strategy_registry_markdown(
                existing_registry_entries,
                args.registry_md,
            )
            print(f"Memoria Markdown atualizada em {exported_registry_md}")
        return 0

    results, trades = backtest_strategy_grid(
        db_path=db_path,
        threshold_levels=args.levels,
        min_trade_return_pct=args.min_trade_return_pct,
        strategy_definitions=strategy_definitions,
    )
    results, trades, rejected_results, rejection_reasons = split_strategy_results(
        summaries=results,
        trades=trades,
        min_success_rate_pct=args.min_success_rate,
        min_profit_factor=args.min_profit_factor,
        min_average_trade_return_pct=args.min_average_trade_return_pct,
        min_trades=args.min_trades,
        require_positive_net=not args.allow_negative_net,
    )
    new_registry_entries = build_registry_entries(
        approved_summaries=results,
        rejected_summaries=rejected_results,
        rejection_reasons=rejection_reasons,
    )
    merged_registry_entries = merge_registry_entries(
        existing_entries=existing_registry_entries,
        new_entries=new_registry_entries,
    )
    ticker_summaries = summarize_trades_by_ticker(trades)
    qualified_ticker_summaries = filter_strategy_ticker_results(
        summaries=ticker_summaries,
        min_success_rate_pct=args.ticker_min_success_rate,
        min_profit_factor=args.ticker_min_profit_factor,
        min_average_trade_return_pct=args.ticker_min_average_trade_return_pct,
        min_trades=args.ticker_min_trades,
        require_positive_net=not args.allow_negative_net,
    )
    print(render_strategy_report(results, top=args.top))
    print(
        f"Testadas nesta rodada: {len(strategy_definitions)} | aprovadas: {len(results)} | reprovadas: {len(rejected_results)}"
    )

    if args.output_csv:
        exported_path = export_strategy_csv(results, args.output_csv)
        print(f"CSV exportado para {exported_path}")
    if args.rejected_csv:
        exported_rejected_path = export_strategy_csv(rejected_results, args.rejected_csv)
        print(f"CSV de reprovadas exportado para {exported_rejected_path}")
    if args.trades_csv:
        exported_trades_path = export_strategy_trades_csv(trades, args.trades_csv)
        print(f"Log de trades exportado para {exported_trades_path}")
    if args.ticker_summary_csv:
        exported_ticker_summary_path = export_strategy_ticker_csv(
            ticker_summaries,
            args.ticker_summary_csv,
        )
        print(f"Resumo por acao exportado para {exported_ticker_summary_path}")
    if args.ticker_qualified_csv:
        exported_ticker_qualified_path = export_strategy_ticker_csv(
            qualified_ticker_summaries,
            args.ticker_qualified_csv,
        )
        print(f"Resumo por acao qualificado exportado para {exported_ticker_qualified_path}")
    if args.registry_csv:
        exported_registry_csv = export_strategy_registry_csv(
            merged_registry_entries,
            args.registry_csv,
        )
        print(f"Memoria CSV exportada para {exported_registry_csv}")
    if args.registry_md:
        exported_registry_md = export_strategy_registry_markdown(
            merged_registry_entries,
            args.registry_md,
        )
        print(f"Memoria Markdown exportada para {exported_registry_md}")

    return 0


def _run_options_sync(args: argparse.Namespace) -> int:
    summary = sync_cotahist_history(
        db_path=args.db_path,
        tickers_file=args.tickers_file,
        years=args.years,
        cotahist_files=args.cotahist_files,
        download_missing=not args.no_download,
        data_dir=args.data_dir,
    )
    print(
        f"COTAHIST importado: anos {summary.processed_years}, "
        f"{summary.spot_tickers} tickers spot e {summary.option_roots} raizes de opcoes em {summary.database_path}."
    )
    return 0


def _run_options_backtest(args: argparse.Namespace) -> int:
    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(
            f"Banco SQLite nao encontrado em {db_path}. Rode `options-sync` antes."
        )

    existing_registry_entries = load_registry_entries(args.registry_csv)
    strategy_definitions = build_option_strategy_definitions(
        min_trade_return_pct=args.min_trade_return_pct,
        round_trip_cost_pct=args.round_trip_cost_pct,
    )
    if not args.include_known:
        known_codes = get_known_strategy_codes(existing_registry_entries)
        strategy_definitions = [
            item for item in strategy_definitions if item.code not in known_codes
        ]

    if not strategy_definitions:
        print(
            f"Nenhuma estrategia nova de opcoes para testar. {len(existing_registry_entries)} estrategias ja estao registradas em {args.registry_csv}."
        )
        if args.registry_md:
            exported_registry_md = export_strategy_registry_markdown(
                existing_registry_entries,
                args.registry_md,
            )
            print(f"Memoria Markdown atualizada em {exported_registry_md}")
        return 0

    end_date = args.end_date or date.today().isoformat()
    start_date = args.start_date or (date.today() - timedelta(days=365)).isoformat()
    results, trades = backtest_option_strategies(
        db_path=db_path,
        tickers_file=args.tickers_file,
        start_date=start_date,
        end_date=end_date,
        strategy_definitions=strategy_definitions,
    )
    approved_results, approved_trades, rejected_results, rejection_reasons = split_strategy_results(
        summaries=results,
        trades=trades,
        min_success_rate_pct=args.min_success_rate,
        min_profit_factor=args.min_profit_factor,
        min_average_trade_return_pct=args.min_average_trade_return_pct,
        min_trades=args.min_trades,
        require_positive_net=True,
    )
    new_registry_entries = build_registry_entries(
        approved_summaries=approved_results,
        rejected_summaries=rejected_results,
        rejection_reasons=rejection_reasons,
    )
    merged_registry_entries = merge_registry_entries(
        existing_entries=existing_registry_entries,
        new_entries=new_registry_entries,
    )
    ticker_summaries = summarize_trades_by_ticker(approved_trades)
    qualified_ticker_summaries = filter_strategy_ticker_results(
        summaries=ticker_summaries,
        min_success_rate_pct=args.ticker_min_success_rate,
        min_profit_factor=args.ticker_min_profit_factor,
        min_average_trade_return_pct=args.ticker_min_average_trade_return_pct,
        min_trades=args.ticker_min_trades,
        require_positive_net=True,
    )

    print(render_strategy_report(approved_results, top=args.top))
    print(
        f"Janela: {start_date} ate {end_date} | testadas: {len(strategy_definitions)} | aprovadas: {len(approved_results)} | reprovadas: {len(rejected_results)}"
    )

    if args.output_csv:
        exported_path = export_strategy_csv(approved_results, args.output_csv)
        print(f"CSV exportado para {exported_path}")
    if args.rejected_csv:
        exported_rejected_path = export_strategy_csv(rejected_results, args.rejected_csv)
        print(f"CSV de reprovadas exportado para {exported_rejected_path}")
    if args.trades_csv:
        exported_trades_path = export_strategy_trades_csv(approved_trades, args.trades_csv)
        print(f"Log de trades exportado para {exported_trades_path}")
    if args.ticker_summary_csv:
        exported_ticker_summary_path = export_strategy_ticker_csv(
            ticker_summaries,
            args.ticker_summary_csv,
        )
        print(f"Resumo por acao exportado para {exported_ticker_summary_path}")
    if args.ticker_qualified_csv:
        exported_ticker_qualified_path = export_strategy_ticker_csv(
            qualified_ticker_summaries,
            args.ticker_qualified_csv,
        )
        print(f"Resumo por acao qualificado exportado para {exported_ticker_qualified_path}")
    if args.registry_csv:
        exported_registry_csv = export_strategy_registry_csv(
            merged_registry_entries,
            args.registry_csv,
        )
        print(f"Memoria CSV exportada para {exported_registry_csv}")
    if args.registry_md:
        exported_registry_md = export_strategy_registry_markdown(
            merged_registry_entries,
            args.registry_md,
        )
        print(f"Memoria Markdown exportada para {exported_registry_md}")

    return 0


def _run_options_discovery(args: argparse.Namespace) -> int:
    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(
            f"Banco SQLite nao encontrado em {db_path}. Rode `options-sync` antes."
        )

    existing_registry_entries = load_registry_entries(args.registry_csv)
    templates = build_option_discovery_templates(
        round_trip_cost_pct=args.round_trip_cost_pct,
        dte_targets=args.dte_targets,
    )
    known_codes = set()
    if not args.include_known:
        known_codes = get_known_strategy_codes(existing_registry_entries)

    start_date, end_date = default_discovery_window(
        start_date=args.start_date,
        end_date=args.end_date,
    )
    summaries = mine_option_discovery_patterns(
        db_path=db_path,
        tickers_file=args.tickers_file,
        start_date=start_date,
        end_date=end_date,
        min_trade_return_pct=args.min_trade_return_pct,
        template_definitions=templates,
        max_pattern_size=args.max_pattern_size,
        known_codes=known_codes,
    )

    if not summaries:
        print(
            f"Nenhum padrao novo para testar. {len(existing_registry_entries)} padroes ja estao registrados em {args.registry_csv}."
        )
        if args.registry_md:
            exported_registry_md = export_strategy_registry_markdown(
                existing_registry_entries,
                args.registry_md,
            )
            print(f"Memoria Markdown atualizada em {exported_registry_md}")
        return 0

    approved_results, rejected_results, rejection_reasons = split_discovery_results(
        summaries=summaries,
        min_success_rate_pct=args.min_success_rate,
        min_profit_factor=args.min_profit_factor,
        min_average_trade_return_pct=args.min_average_trade_return_pct,
        min_trades=args.min_trades,
        min_tickers=args.min_tickers,
        require_positive_net=not args.allow_negative_net,
    )
    approved_trades = collect_discovery_pattern_trades(
        db_path=db_path,
        tickers_file=args.tickers_file,
        start_date=start_date,
        end_date=end_date,
        approved_summaries=approved_results,
        template_definitions=templates,
        max_pattern_size=args.max_pattern_size,
    )
    new_registry_entries = build_discovery_registry_entries(
        approved_summaries=approved_results,
        rejected_summaries=rejected_results,
        rejection_reasons=rejection_reasons,
    )
    merged_registry_entries = merge_registry_entries(
        existing_entries=existing_registry_entries,
        new_entries=new_registry_entries,
    )
    ticker_summaries = summarize_trades_by_ticker(approved_trades)
    qualified_ticker_summaries = filter_strategy_ticker_results(
        summaries=ticker_summaries,
        min_success_rate_pct=args.ticker_min_success_rate,
        min_profit_factor=args.ticker_min_profit_factor,
        min_average_trade_return_pct=args.ticker_min_average_trade_return_pct,
        min_trades=args.ticker_min_trades,
        require_positive_net=not args.allow_negative_net,
    )

    print(render_discovery_report(approved_results, top=args.top))
    print(
        f"Janela: {start_date} ate {end_date} | testados: {len(summaries)} | aprovados: {len(approved_results)} | reprovados: {len(rejected_results)}"
    )

    if args.output_csv:
        exported_path = export_discovery_csv(approved_results, args.output_csv)
        print(f"CSV exportado para {exported_path}")
    if args.rejected_csv:
        exported_rejected_path = export_discovery_csv(rejected_results, args.rejected_csv)
        print(f"CSV de reprovadas exportado para {exported_rejected_path}")
    if args.trades_csv:
        exported_trades_path = export_strategy_trades_csv(approved_trades, args.trades_csv)
        print(f"Log de trades exportado para {exported_trades_path}")
    if args.ticker_summary_csv:
        exported_ticker_summary_path = export_strategy_ticker_csv(
            ticker_summaries,
            args.ticker_summary_csv,
        )
        print(f"Resumo por acao exportado para {exported_ticker_summary_path}")
    if args.ticker_qualified_csv:
        exported_ticker_qualified_path = export_strategy_ticker_csv(
            qualified_ticker_summaries,
            args.ticker_qualified_csv,
        )
        print(f"Resumo por acao qualificado exportado para {exported_ticker_qualified_path}")
    if args.registry_csv:
        exported_registry_csv = export_strategy_registry_csv(
            merged_registry_entries,
            args.registry_csv,
        )
        print(f"Memoria CSV exportada para {exported_registry_csv}")
    if args.registry_md:
        exported_registry_md = export_strategy_registry_markdown(
            merged_registry_entries,
            args.registry_md,
        )
        print(f"Memoria Markdown exportada para {exported_registry_md}")
    if args.summary_md:
        exported_summary_md = export_discovery_markdown(
            approved_results=approved_results,
            rejected_results=rejected_results,
            features=list_discovery_features(),
            output_path=args.summary_md,
            start_date=start_date,
            end_date=end_date,
        )
        print(f"Resumo Markdown exportado para {exported_summary_md}")

    return 0


def _run_options_discovery_refine(args: argparse.Namespace) -> int:
    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(
            f"Banco SQLite nao encontrado em {db_path}. Rode `options-sync` antes."
        )

    summaries = load_discovery_summaries(args.input_csv)
    if not summaries:
        raise FileNotFoundError(
            f"Nenhum padrao carregado em {args.input_csv}. Rode `options-discover` antes."
        )

    start_date, end_date = default_discovery_window(
        start_date=args.start_date,
        end_date=args.end_date,
    )
    baselines = summarize_template_baselines(
        db_path=db_path,
        tickers_file=args.tickers_file,
        start_date=start_date,
        end_date=end_date,
        dte_targets=args.dte_targets,
        round_trip_cost_pct=args.round_trip_cost_pct,
    )
    refined_results, rejection_reasons, refined_trades = refine_discovery_shortlist(
        summaries=summaries,
        trades_csv_path=args.trades_csv,
        baselines=baselines,
        validation_start_date=args.validation_start_date,
        min_state_size=args.min_state_size,
        min_avg_uplift_pct=args.min_avg_uplift_pct,
        min_pf_uplift=args.min_pf_uplift,
        min_total_trades=args.min_total_trades,
        min_tickers=args.min_tickers,
        min_train_trades=args.min_train_trades,
        min_validation_trades=args.min_validation_trades,
        min_train_profit_factor=args.min_train_profit_factor,
        min_validation_profit_factor=args.min_validation_profit_factor,
        min_train_average_trade_return_pct=args.min_train_average_trade_return_pct,
        min_validation_average_trade_return_pct=args.min_validation_average_trade_return_pct,
        min_active_months=args.min_active_months,
        min_positive_month_ratio=args.min_positive_month_ratio,
        min_validation_active_months=args.min_validation_active_months,
        min_validation_positive_month_ratio=args.min_validation_positive_month_ratio,
        overlap_threshold=args.overlap_threshold,
    )

    ticker_summaries = summarize_trades_by_ticker(refined_trades)
    qualified_ticker_summaries = filter_strategy_ticker_results(
        summaries=ticker_summaries,
        min_success_rate_pct=args.ticker_min_success_rate,
        min_profit_factor=args.ticker_min_profit_factor,
        min_average_trade_return_pct=args.ticker_min_average_trade_return_pct,
        min_trades=args.ticker_min_trades,
        require_positive_net=True,
    )

    print(render_refined_discovery_report(refined_results, top=args.top))
    print(
        f"Janela: {start_date} ate {end_date} | validacao desde {args.validation_start_date} | shortlist: {len(refined_results)} | reprovados no refinamento: {len(rejection_reasons)}"
    )

    if args.output_csv:
        exported_path = export_refined_discovery_csv(refined_results, args.output_csv)
        print(f"CSV exportado para {exported_path}")
    if args.summary_md:
        exported_summary_md = export_refined_discovery_markdown(
            refined_results,
            args.summary_md,
            validation_start_date=args.validation_start_date,
        )
        print(f"Resumo Markdown exportado para {exported_summary_md}")
    if args.shortlist_trades_csv:
        exported_trades_path = export_strategy_trades_csv(
            refined_trades,
            args.shortlist_trades_csv,
        )
        print(f"Log de trades exportado para {exported_trades_path}")
    if args.ticker_summary_csv:
        exported_ticker_summary_path = export_strategy_ticker_csv(
            ticker_summaries,
            args.ticker_summary_csv,
        )
        print(f"Resumo por acao exportado para {exported_ticker_summary_path}")
    if args.ticker_qualified_csv:
        exported_ticker_qualified_path = export_strategy_ticker_csv(
            qualified_ticker_summaries,
            args.ticker_qualified_csv,
        )
        print(f"Resumo por acao qualificado exportado para {exported_ticker_qualified_path}")

    return 0


def _parse_target_stop_pairs(values: list[str]) -> list[tuple[float, float]]:
    pairs: list[tuple[float, float]] = []
    for raw_value in values:
        if ":" not in raw_value:
            raise ValueError(f"Par alvo:stop invalido: {raw_value}. Use o formato 2:1")
        left_raw, right_raw = raw_value.split(":", 1)
        take_profit_pct = float(left_raw)
        stop_loss_pct = float(right_raw)
        if take_profit_pct <= 0 or stop_loss_pct <= 0:
            raise ValueError(f"Par alvo:stop invalido: {raw_value}. Valores devem ser positivos.")
        pairs.append((take_profit_pct, stop_loss_pct))
    if not pairs:
        raise ValueError("Informe ao menos um par alvo:stop.")
    return pairs


def _run_asset_discovery_round1(args: argparse.Namespace) -> int:
    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(
            f"Banco SQLite nao encontrado em {db_path}. Rode `options-sync` antes para usar a base spot da B3."
        )

    existing_registry_entries = load_registry_entries(args.registry_csv)
    known_codes = set() if args.include_known else get_known_strategy_codes(existing_registry_entries)
    start_date, end_date = default_asset_discovery_window(
        start_date=args.start_date,
        end_date=args.end_date,
    )
    target_stop_pairs = _parse_target_stop_pairs(args.target_stop_pairs)
    templates = build_asset_discovery_round1_templates(
        entry_rules=args.entry_rules,
        trade_directions=args.trade_directions,
        target_stop_pairs=target_stop_pairs,
        time_cap_days=args.time_cap_days,
        round_trip_cost_pct=args.round_trip_cost_pct,
    )

    def _format_elapsed(seconds: float) -> str:
        total_seconds = int(seconds)
        hours, remainder = divmod(total_seconds, 3600)
        minutes, secs = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"

    def _print_progress(progress: dict[str, object]) -> None:
        every = max(0, int(args.progress_every_tickers))
        processed = int(progress.get("processed_tickers", 0))
        total = int(progress.get("total_tickers", 0))
        if total <= 0:
            return
        if every > 0 and processed % every != 0 and processed != total:
            return
        stage = str(progress.get("stage", "stage"))
        pct = (processed / total) * 100.0
        elapsed = _format_elapsed(float(progress.get("elapsed_seconds", 0.0)))
        ticker = str(progress.get("ticker", ""))
        samples = int(progress.get("samples", 0))
        extras: list[str] = []
        if stage == "mine":
            extras.append(f"padroes={int(progress.get('patterns', 0))}")
        if stage == "trades":
            extras.append(f"trades={int(progress.get('trades', 0))}")
            extras.append(f"aprovados={int(progress.get('approved_patterns', 0))}")
        extra_text = " | " + " | ".join(extras) if extras else ""
        print(
            f"[progresso:{stage}] {processed}/{total} acoes ({pct:.1f}%) | elapsed {elapsed} | ultimo={ticker} | samples={samples}{extra_text}",
            flush=True,
        )

    summaries = mine_asset_discovery_patterns(
        db_path=db_path,
        tickers_file=args.tickers_file,
        start_date=start_date,
        end_date=end_date,
        min_trade_return_pct=args.min_trade_return_pct,
        template_definitions=templates,
        max_pattern_size=args.max_pattern_size,
        known_codes=known_codes,
        progress_callback=_print_progress,
    )
    approved_results, rejected_results, rejection_reasons = split_asset_discovery_results(
        summaries,
        min_success_rate_pct=args.min_success_rate,
        min_take_profit_rate_pct=args.min_take_profit_rate,
        min_profit_factor=args.min_profit_factor,
        min_average_trade_return_pct=args.min_average_trade_return_pct,
        min_trades=args.min_trades,
        min_tickers=args.min_tickers,
        require_positive_net=not args.allow_negative_net,
    )
    approved_trades = collect_asset_discovery_pattern_trades(
        db_path=db_path,
        tickers_file=args.tickers_file,
        start_date=start_date,
        end_date=end_date,
        approved_summaries=approved_results,
        template_definitions=templates,
        max_pattern_size=args.max_pattern_size,
        progress_callback=_print_progress,
    )
    new_registry_entries = build_asset_discovery_registry_entries(
        approved_results,
        rejected_results,
        rejection_reasons,
    )
    merged_registry_entries = merge_registry_entries(existing_registry_entries, new_registry_entries)
    ticker_summaries = summarize_trades_by_ticker(approved_trades)
    qualified_ticker_summaries = filter_strategy_ticker_results(
        summaries=ticker_summaries,
        min_success_rate_pct=args.ticker_min_success_rate,
        min_profit_factor=args.ticker_min_profit_factor,
        min_average_trade_return_pct=args.ticker_min_average_trade_return_pct,
        min_trades=args.ticker_min_trades,
        require_positive_net=True,
    )

    print(render_asset_discovery_report(approved_results, top=args.top))
    print(
        f"Janela: {start_date} ate {end_date} | templates: {len(templates)} | features: {len(list_asset_discovery_features())} | aprovados: {len(approved_results)} | reprovados: {len(rejected_results)}"
    )

    if args.output_csv:
        exported_path = export_asset_discovery_csv(approved_results, args.output_csv)
        print(f"CSV exportado para {exported_path}")
    if args.rejected_csv:
        exported_rejected_path = export_asset_discovery_csv(rejected_results, args.rejected_csv)
        print(f"CSV de reprovados exportado para {exported_rejected_path}")
    if args.trades_csv:
        exported_trades_path = export_strategy_trades_csv(approved_trades, args.trades_csv)
        print(f"Log de trades exportado para {exported_trades_path}")
    if args.ticker_summary_csv:
        exported_ticker_summary_path = export_strategy_ticker_csv(
            ticker_summaries,
            args.ticker_summary_csv,
        )
        print(f"Resumo por acao exportado para {exported_ticker_summary_path}")
    if args.ticker_qualified_csv:
        exported_ticker_qualified_path = export_strategy_ticker_csv(
            qualified_ticker_summaries,
            args.ticker_qualified_csv,
        )
        print(f"Resumo por acao qualificado exportado para {exported_ticker_qualified_path}")
    if args.registry_csv:
        exported_registry_csv = export_strategy_registry_csv(
            merged_registry_entries,
            args.registry_csv,
        )
        print(f"Memoria CSV atualizada em {exported_registry_csv}")
    if args.registry_md:
        exported_registry_md = export_strategy_registry_markdown(
            merged_registry_entries,
            args.registry_md,
        )
        print(f"Memoria Markdown atualizada em {exported_registry_md}")
    if args.summary_md:
        exported_summary_md = export_asset_discovery_markdown(
            approved_results,
            rejected_results,
            output_path=args.summary_md,
            start_date=start_date,
            end_date=end_date,
            features=list_asset_discovery_features(),
            templates=templates,
        )
        print(f"Resumo Markdown exportado para {exported_summary_md}")

    return 0


def _run_asset_discovery_r3(args: argparse.Namespace) -> int:
    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(
            f"Banco SQLite nao encontrado em {db_path}. Rode `options-sync` antes."
        )

    existing_registry_entries = load_registry_entries(args.registry_csv)
    known_codes = set() if args.include_known else get_known_strategy_codes(existing_registry_entries)
    start_date, end_date = default_asset_discovery_window(
        start_date=args.start_date,
        end_date=args.end_date,
    )

    # Build percent-based templates
    target_stop_pairs = _parse_target_stop_pairs(args.target_stop_pairs)
    percent_templates = build_asset_discovery_round1_templates(
        entry_rules=args.entry_rules,
        trade_directions=args.trade_directions,
        target_stop_pairs=target_stop_pairs,
        time_cap_days=args.time_cap_days,
    )

    # Build ATR-based templates
    atr_target_stop_pairs = _parse_target_stop_pairs(args.atr_target_stop_pairs)
    atr_templates = build_asset_discovery_atr_templates(
        entry_rules=args.entry_rules,
        trade_directions=args.trade_directions,
        atr_target_stop_pairs=atr_target_stop_pairs,
        time_cap_days_list=args.atr_time_cap_days,
    )

    all_templates = percent_templates + atr_templates
    print(
        f"R3 Discovery | features: {len(list_asset_discovery_features())} | "
        f"templates: {len(all_templates)} (percent: {len(percent_templates)}, ATR: {len(atr_templates)}) | "
        f"janela: {start_date} ate {end_date}"
    )

    def _format_elapsed(seconds: float) -> str:
        total_seconds = int(seconds)
        hours, remainder = divmod(total_seconds, 3600)
        minutes, secs = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"

    def _print_progress(progress: dict[str, object]) -> None:
        every = max(0, int(args.progress_every_tickers))
        processed = int(progress.get("processed_tickers", 0))
        total = int(progress.get("total_tickers", 0))
        if total <= 0:
            return
        if every > 0 and processed % every != 0 and processed != total:
            return
        stage = str(progress.get("stage", "stage"))
        pct = (processed / total) * 100.0
        elapsed = _format_elapsed(float(progress.get("elapsed_seconds", 0.0)))
        ticker = str(progress.get("ticker", ""))
        samples = int(progress.get("samples", 0))
        extras: list[str] = []
        if "patterns" in progress:
            extras.append(f"padroes={int(progress['patterns'])}")
        if "patterns_3f" in progress:
            extras.append(f"3f={int(progress['patterns_3f'])}")
        if "promoted_pairs" in progress:
            extras.append(f"promoted={int(progress['promoted_pairs'])}")
        if "pruned" in progress:
            extras.append(f"pruned={int(progress['pruned'])}")
        if "trades" in progress:
            extras.append(f"trades={int(progress['trades'])}")
        extra_text = " | " + " | ".join(extras) if extras else ""
        print(
            f"[R3:{stage}] {processed}/{total} ({pct:.1f}%) | {elapsed} | {ticker} | samples={samples}{extra_text}",
            flush=True,
        )

    if args.progressive:
        summaries = mine_asset_discovery_patterns_progressive(
            db_path=db_path,
            tickers_file=args.tickers_file,
            start_date=start_date,
            end_date=end_date,
            template_definitions=all_templates,
            known_codes=known_codes,
            progress_callback=_print_progress,
            pre_filter_min_profitable_rate=args.pre_filter_min_profitable_rate,
            pre_filter_min_profit_factor=args.pre_filter_min_profit_factor,
            pre_filter_min_trades=args.pre_filter_min_trades,
            max_promoted_pairs=args.max_promoted_pairs,
            prune_every_tickers=args.prune_every_tickers,
            max_accumulators=args.max_accumulators,
        )
    else:
        summaries = mine_asset_discovery_patterns(
            db_path=db_path,
            tickers_file=args.tickers_file,
            start_date=start_date,
            end_date=end_date,
            template_definitions=all_templates,
            max_pattern_size=args.max_pattern_size,
            known_codes=known_codes,
            progress_callback=_print_progress,
        )

    approved_results, rejected_results, rejection_reasons = split_asset_discovery_results(
        summaries,
        min_success_rate_pct=args.min_success_rate,
        min_take_profit_rate_pct=args.min_take_profit_rate,
        min_profit_factor=args.min_profit_factor,
        min_average_trade_return_pct=args.min_average_trade_return_pct,
        min_trades=args.min_trades,
        min_tickers=args.min_tickers,
        require_positive_net=True,
    )

    approved_trades = collect_asset_discovery_pattern_trades(
        db_path=db_path,
        tickers_file=args.tickers_file,
        start_date=start_date,
        end_date=end_date,
        approved_summaries=approved_results,
        template_definitions=all_templates,
        max_pattern_size=3,
        progress_callback=_print_progress,
    )

    new_registry_entries = build_asset_discovery_registry_entries(
        approved_results,
        rejected_results,
        rejection_reasons,
    )
    merged_registry_entries = merge_registry_entries(existing_registry_entries, new_registry_entries)
    ticker_summaries = summarize_trades_by_ticker(approved_trades)
    qualified_ticker_summaries = filter_strategy_ticker_results(
        summaries=ticker_summaries,
        min_success_rate_pct=args.ticker_min_success_rate,
        min_profit_factor=args.ticker_min_profit_factor,
        min_average_trade_return_pct=args.ticker_min_average_trade_return_pct,
        min_trades=args.ticker_min_trades,
        require_positive_net=True,
    )

    print(render_asset_discovery_report(approved_results, top=args.top))
    sizes = {s.state_size for s in summaries}
    print(
        f"R3 resultado | padroes brutos: {len(summaries)} | aprovados: {len(approved_results)} | "
        f"reprovados: {len(rejected_results)} | tamanhos: {sorted(sizes)}"
    )

    if args.output_csv:
        export_asset_discovery_csv(approved_results, args.output_csv)
        print(f"CSV aprovados: {args.output_csv}")
    if args.rejected_csv:
        export_asset_discovery_csv(rejected_results, args.rejected_csv)
        print(f"CSV reprovados: {args.rejected_csv}")
    if args.trades_csv:
        export_strategy_trades_csv(approved_trades, args.trades_csv)
        print(f"Log de trades: {args.trades_csv}")
    if args.ticker_summary_csv:
        export_strategy_ticker_csv(ticker_summaries, args.ticker_summary_csv)
        print(f"Resumo por acao: {args.ticker_summary_csv}")
    if args.ticker_qualified_csv:
        export_strategy_ticker_csv(qualified_ticker_summaries, args.ticker_qualified_csv)
        print(f"Resumo qualificado por acao: {args.ticker_qualified_csv}")
    if args.registry_csv:
        export_strategy_registry_csv(merged_registry_entries, args.registry_csv)
        print(f"Registro: {args.registry_csv}")
    if args.registry_md:
        export_strategy_registry_markdown(merged_registry_entries, args.registry_md)
        print(f"Registro MD: {args.registry_md}")
    if args.summary_md:
        export_asset_discovery_markdown(
            approved_results,
            rejected_results,
            output_path=args.summary_md,
            start_date=start_date,
            end_date=end_date,
            features=list_asset_discovery_features(),
            templates=all_templates,
        )
        print(f"Resumo MD: {args.summary_md}")

    return 0


def _run_asset_monitor_export(args: argparse.Namespace) -> int:
    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(
            f"Banco SQLite nao encontrado em {db_path}. Rode `options-sync` antes de exportar sinais."
        )

    exported_path = export_asset_monitor_payload(
        db_path=db_path,
        tickers_file=args.tickers_file,
        strategies_csv=args.strategies_csv,
        ticker_stats_csv=args.ticker_stats_csv,
        overall_actions_csv=args.overall_actions_csv,
        as_of_date=args.as_of_date,
        top_strategies=None if args.top_strategies == 0 else args.top_strategies,
        output_path=args.output_json,
    )
    print(f"JSON do monitor exportado para {exported_path}")
    return 0


def _run_all(args: argparse.Namespace) -> int:
    if args.force_sync or is_sync_stale(args.db_path):
        sync_code = _run_sync(args)
        if sync_code != 0:
            return sync_code
    else:
        print(f"Banco local em dia: {args.db_path}")

    return _run_analyze(args)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "sync":
        return _run_sync(args)
    if args.command == "analyze":
        return _run_analyze(args)
    if args.command == "strategies":
        return _run_strategies(args)
    if args.command == "options-sync":
        return _run_options_sync(args)
    if args.command == "options-backtest":
        return _run_options_backtest(args)
    if args.command == "options-discover":
        return _run_options_discovery(args)
    if args.command == "options-discover-refine":
        return _run_options_discovery_refine(args)
    if args.command == "asset-discover-round1":
        return _run_asset_discovery_round1(args)
    if args.command == "asset-discover-r3":
        return _run_asset_discovery_r3(args)
    if args.command == "asset-monitor-export":
        return _run_asset_monitor_export(args)
    if args.command == "run":
        return _run_all(args)

    parser.error("Comando invalido.")
    return 2
