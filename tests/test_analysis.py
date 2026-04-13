from __future__ import annotations

import unittest
from pathlib import Path
from shutil import rmtree

from b3_patterns.analysis import (
    analyze_patterns,
    backtest_strategy_grid,
    filter_strategy_results,
    filter_strategy_ticker_results,
    split_strategy_results,
    summarize_trades_by_ticker,
)
from b3_patterns.db import connect, initialize_database, replace_ticker_history
from b3_patterns.models import PriceBar
from b3_patterns.registry import (
    build_registry_entries,
    get_known_strategy_codes,
    load_registry_entries,
    merge_registry_entries,
)


def _workspace_db_path(name: str) -> Path:
    temp_root = Path.cwd() / ".tmp-tests"
    temp_dir = temp_root / name
    rmtree(temp_dir, ignore_errors=True)
    temp_dir.mkdir(parents=True, exist_ok=True)
    return temp_dir / "history.db"


class AnalysisTestCase(unittest.TestCase):
    def test_analyze_patterns_ranks_tickers_by_success(self) -> None:
        db_path = _workspace_db_path("analysis")
        self.addCleanup(lambda: rmtree(db_path.parent, ignore_errors=True))

        with connect(db_path) as connection:
            initialize_database(connection)
            replace_ticker_history(
                connection,
                "GOOD3.SA",
                [
                    PriceBar("GOOD3.SA", "2026-01-01", 10, 10, 10, 10, 10, 1_000),
                    PriceBar("GOOD3.SA", "2026-01-02", 9.7, 9.7, 9.7, 9.7, 9.7, 1_000),
                    PriceBar("GOOD3.SA", "2026-01-03", 10.1, 10.1, 10.1, 10.1, 10.1, 1_000),
                    PriceBar("GOOD3.SA", "2026-01-04", 9.7, 9.7, 9.7, 9.7, 9.7, 1_000),
                    PriceBar("GOOD3.SA", "2026-01-05", 10.0, 10.0, 10.0, 10.0, 10.0, 1_000),
                ],
            )
            replace_ticker_history(
                connection,
                "BAD3.SA",
                [
                    PriceBar("BAD3.SA", "2026-01-01", 10, 10, 10, 10, 10, 1_000),
                    PriceBar("BAD3.SA", "2026-01-02", 9.7, 9.7, 9.7, 9.7, 9.7, 1_000),
                    PriceBar("BAD3.SA", "2026-01-03", 9.6, 9.6, 9.6, 9.6, 9.6, 1_000),
                    PriceBar("BAD3.SA", "2026-01-04", 9.3, 9.3, 9.3, 9.3, 9.3, 1_000),
                    PriceBar("BAD3.SA", "2026-01-05", 9.1, 9.1, 9.1, 9.1, 9.1, 1_000),
                ],
            )

        results = analyze_patterns(
            db_path=db_path,
            trigger_change_pct=-2.0,
            target_next_day_pct=0.0,
        )

        self.assertEqual([item.ticker for item in results], ["GOOD3.SA", "BAD3.SA"])
        self.assertEqual(results[0].occurrences, 2)
        self.assertEqual(results[0].successful_occurrences, 2)
        self.assertEqual(results[1].successful_occurrences, 0)

    def test_analyze_patterns_supports_downside_target_mode(self) -> None:
        db_path = _workspace_db_path("analysis-down")
        self.addCleanup(lambda: rmtree(db_path.parent, ignore_errors=True))

        with connect(db_path) as connection:
            initialize_database(connection)
            replace_ticker_history(
                connection,
                "BAD3.SA",
                [
                    PriceBar("BAD3.SA", "2026-01-01", 10, 10, 10, 10, 10, 1_000),
                    PriceBar("BAD3.SA", "2026-01-02", 9.7, 9.7, 9.7, 9.7, 9.7, 1_000),
                    PriceBar("BAD3.SA", "2026-01-03", 9.6, 9.6, 9.6, 9.6, 9.6, 1_000),
                    PriceBar("BAD3.SA", "2026-01-04", 9.3, 9.3, 9.3, 9.3, 9.3, 1_000),
                    PriceBar("BAD3.SA", "2026-01-05", 9.1, 9.1, 9.1, 9.1, 9.1, 1_000),
                ],
            )

        results = analyze_patterns(
            db_path=db_path,
            trigger_change_pct=-2.0,
            target_next_day_pct=0.0,
            target_mode="down",
        )

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].ticker, "BAD3.SA")
        self.assertEqual(results[0].successful_occurrences, 2)

    def test_backtest_strategy_grid_covers_close_intraday_and_gap_families(self) -> None:
        db_path = _workspace_db_path("strategy-grid")
        self.addCleanup(lambda: rmtree(db_path.parent, ignore_errors=True))

        with connect(db_path) as connection:
            initialize_database(connection)
            replace_ticker_history(
                connection,
                "INTRA3.SA",
                [
                    PriceBar("INTRA3.SA", "2026-01-01", 100, 100, 100, 100, 100, 1_000),
                    PriceBar("INTRA3.SA", "2026-01-02", 98, 101, 94, 99, 99, 1_000),
                    PriceBar("INTRA3.SA", "2026-01-03", 101, 106, 100, 104, 104, 1_000),
                    PriceBar("INTRA3.SA", "2026-01-04", 102, 103, 97, 98, 98, 1_000),
                    PriceBar("INTRA3.SA", "2026-01-05", 99, 100, 98, 100, 100, 1_000),
                ],
            )

        summaries, trades = backtest_strategy_grid(
            db_path=db_path,
            threshold_levels=[2.0],
            min_trade_return_pct=0.0,
        )

        self.assertEqual(len(summaries), 10)
        self.assertEqual(
            {item.code for item in summaries},
            {
                "close_queda_2_reversao",
                "close_queda_2_continuacao",
                "close_alta_2_continuacao",
                "close_alta_2_reversao",
                "intraday_queda_2_close_d0",
                "intraday_queda_2_close_d1",
                "intraday_alta_2_close_d0",
                "intraday_alta_2_close_d1",
                "gap_baixa_2_close_d0",
                "gap_alta_2_close_d0",
            },
        )

        intraday_down_d1 = next(
            item for item in summaries if item.code == "intraday_queda_2_close_d1"
        )
        self.assertEqual(intraday_down_d1.total_occurrences, 2)
        self.assertAlmostEqual(intraday_down_d1.net_trade_return_pct, 4.2389, places=3)
        self.assertEqual(intraday_down_d1.profitable_trades, 1)

        gap_down_d0 = next(item for item in summaries if item.code == "gap_baixa_2_close_d0")
        self.assertEqual(gap_down_d0.total_occurrences, 1)
        self.assertAlmostEqual(gap_down_d0.average_trade_return_pct, 1.0204, places=3)

        close_up_reversal = next(
            item for item in summaries if item.code == "close_alta_2_reversao"
        )
        self.assertAlmostEqual(close_up_reversal.net_trade_return_pct, 5.7692, places=3)
        self.assertEqual(
            len([item for item in trades if item.strategy_code == "close_alta_2_reversao"]),
            1,
        )

    def test_filter_strategy_results_keeps_only_financially_valid_strategies(self) -> None:
        db_path = _workspace_db_path("strategy-filter")
        self.addCleanup(lambda: rmtree(db_path.parent, ignore_errors=True))

        with connect(db_path) as connection:
            initialize_database(connection)
            replace_ticker_history(
                connection,
                "INTRA3.SA",
                [
                    PriceBar("INTRA3.SA", "2026-01-01", 100, 100, 100, 100, 100, 1_000),
                    PriceBar("INTRA3.SA", "2026-01-02", 98, 101, 94, 99, 99, 1_000),
                    PriceBar("INTRA3.SA", "2026-01-03", 101, 106, 100, 104, 104, 1_000),
                    PriceBar("INTRA3.SA", "2026-01-04", 102, 103, 97, 98, 98, 1_000),
                    PriceBar("INTRA3.SA", "2026-01-05", 99, 100, 98, 100, 100, 1_000),
                ],
            )

        summaries, trades = backtest_strategy_grid(
            db_path=db_path,
            threshold_levels=[2.0],
            min_trade_return_pct=0.0,
        )
        filtered_summaries, filtered_trades = filter_strategy_results(
            summaries=summaries,
            trades=trades,
            min_success_rate_pct=50.0,
            min_profit_factor=1.0,
            min_average_trade_return_pct=0.5,
            min_trades=1,
            require_positive_net=True,
        )

        self.assertEqual(
            {item.code for item in filtered_summaries},
            {
                "close_alta_2_reversao",
                "close_queda_2_reversao",
                "gap_baixa_2_close_d0",
                "intraday_alta_2_close_d1",
                "intraday_queda_2_close_d1",
            },
        )
        self.assertEqual(
            {item.strategy_code for item in filtered_trades},
            {item.code for item in filtered_summaries},
        )

    def test_summarize_trades_by_ticker_builds_filtered_report(self) -> None:
        db_path = _workspace_db_path("ticker-summary")
        self.addCleanup(lambda: rmtree(db_path.parent, ignore_errors=True))

        with connect(db_path) as connection:
            initialize_database(connection)
            replace_ticker_history(
                connection,
                "INTRA3.SA",
                [
                    PriceBar("INTRA3.SA", "2026-01-01", 100, 100, 100, 100, 100, 1_000),
                    PriceBar("INTRA3.SA", "2026-01-02", 98, 101, 94, 99, 99, 1_000),
                    PriceBar("INTRA3.SA", "2026-01-03", 101, 106, 100, 104, 104, 1_000),
                    PriceBar("INTRA3.SA", "2026-01-04", 102, 103, 97, 98, 98, 1_000),
                    PriceBar("INTRA3.SA", "2026-01-05", 99, 100, 98, 100, 100, 1_000),
                ],
            )

        summaries, trades = backtest_strategy_grid(
            db_path=db_path,
            threshold_levels=[2.0],
            min_trade_return_pct=0.0,
        )
        filtered_summaries, filtered_trades = filter_strategy_results(
            summaries=summaries,
            trades=trades,
            min_success_rate_pct=50.0,
            min_profit_factor=1.0,
            min_average_trade_return_pct=0.5,
            min_trades=1,
            require_positive_net=True,
        )
        ticker_summaries = summarize_trades_by_ticker(filtered_trades)
        qualified_ticker_summaries = filter_strategy_ticker_results(
            summaries=ticker_summaries,
            min_success_rate_pct=50.0,
            min_profit_factor=1.0,
            min_average_trade_return_pct=0.5,
            min_trades=1,
            require_positive_net=True,
        )

        self.assertEqual(len(ticker_summaries), 5)
        self.assertEqual(len(qualified_ticker_summaries), 5)
        top_ticker = qualified_ticker_summaries[0]
        self.assertEqual(top_ticker.strategy_code, "close_alta_2_reversao")
        self.assertEqual(top_ticker.ticker, "INTRA3.SA")
        self.assertGreater(top_ticker.net_trade_return_pct, 0.0)

    def test_registry_entries_store_approved_and_rejected_strategies(self) -> None:
        db_path = _workspace_db_path("strategy-registry")
        self.addCleanup(lambda: rmtree(db_path.parent, ignore_errors=True))
        registry_path = db_path.parent / "registry.csv"

        with connect(db_path) as connection:
            initialize_database(connection)
            replace_ticker_history(
                connection,
                "INTRA3.SA",
                [
                    PriceBar("INTRA3.SA", "2026-01-01", 100, 100, 100, 100, 100, 1_000),
                    PriceBar("INTRA3.SA", "2026-01-02", 98, 101, 94, 99, 99, 1_000),
                    PriceBar("INTRA3.SA", "2026-01-03", 101, 106, 100, 104, 104, 1_000),
                    PriceBar("INTRA3.SA", "2026-01-04", 102, 103, 97, 98, 98, 1_000),
                    PriceBar("INTRA3.SA", "2026-01-05", 99, 100, 98, 100, 100, 1_000),
                ],
            )

        summaries, trades = backtest_strategy_grid(
            db_path=db_path,
            threshold_levels=[2.0],
            min_trade_return_pct=0.0,
        )
        approved, _, rejected, rejection_reasons = split_strategy_results(
            summaries=summaries,
            trades=trades,
            min_success_rate_pct=50.0,
            min_profit_factor=1.0,
            min_average_trade_return_pct=0.5,
            min_trades=1,
            require_positive_net=True,
        )
        registry_entries = build_registry_entries(
            approved_summaries=approved,
            rejected_summaries=rejected,
            rejection_reasons=rejection_reasons,
        )
        merged_entries = merge_registry_entries([], registry_entries)

        from b3_patterns.reporting import export_strategy_registry_csv

        export_strategy_registry_csv(merged_entries, registry_path)
        loaded_entries = load_registry_entries(registry_path)

        self.assertEqual(len(loaded_entries), 10)
        self.assertEqual(len(get_known_strategy_codes(loaded_entries)), 10)
        self.assertEqual(len([item for item in loaded_entries if item.status == "approved"]), 5)
        self.assertEqual(len([item for item in loaded_entries if item.status == "rejected"]), 5)
        rejected_codes = {item.code for item in loaded_entries if item.status == "rejected"}
        self.assertIn("close_alta_2_continuacao", rejected_codes)


if __name__ == "__main__":
    unittest.main()
