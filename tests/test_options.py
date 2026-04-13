from __future__ import annotations

import unittest
from pathlib import Path
from shutil import rmtree

from b3_patterns.analysis import split_strategy_results
from b3_patterns.db import (
    connect,
    initialize_database,
    replace_option_history,
    replace_spot_history,
)
from b3_patterns.models import OptionQuoteBar, OptionStrategyDefinition, SpotQuoteBar
from b3_patterns.options import (
    backtest_option_strategies,
    build_option_strategy_definitions,
)
from b3_patterns.registry import build_registry_entries
from b3_patterns.reporting import (
    export_strategy_registry_csv,
    export_strategy_registry_markdown,
)


def _workspace_test_dir(name: str) -> Path:
    temp_root = Path.cwd() / ".tmp-tests"
    temp_dir = temp_root / name
    rmtree(temp_dir, ignore_errors=True)
    temp_dir.mkdir(parents=True, exist_ok=True)
    return temp_dir


class OptionsBacktestTestCase(unittest.TestCase):
    def test_build_option_strategy_definitions_returns_136_variants(self) -> None:
        strategies = build_option_strategy_definitions()
        self.assertEqual(len(strategies), 136)
        self.assertEqual(
            {item.dte_target_days for item in strategies},
            {7, 15, 30},
        )
        self.assertEqual(
            {item.setup_kind for item in strategies},
            {"gap", "close_change", "cumulative_return", "moving_average_cross"},
        )
        self.assertIn("option_gap_target_stop", {item.family for item in strategies})
        self.assertIn(
            "opt_gap_baixa_1_call_atm_7dte_sl3_tp12",
            {item.code for item in strategies},
        )
        self.assertIn(
            "opt_gap_alta_2_put_atm_15dte_sl5_tp15",
            {item.code for item in strategies},
        )

    def test_backtest_option_strategies_uses_option_premium_gain_loss(self) -> None:
        temp_dir = _workspace_test_dir("options-backtest")
        self.addCleanup(lambda: rmtree(temp_dir, ignore_errors=True))
        db_path = temp_dir / "history.db"
        tickers_path = temp_dir / "tickers.csv"
        tickers_path.write_text("Ticker,Nome\nAAAA3,Teste\n", encoding="utf-8")

        with connect(db_path) as connection:
            initialize_database(connection)
            replace_spot_history(
                connection,
                "AAAA3",
                [
                    SpotQuoteBar("AAAA3", "2026-03-01", 100, 101, 99, 100, 1_000_000, 100),
                    SpotQuoteBar("AAAA3", "2026-03-02", 98, 101, 97, 99, 1_100_000, 110),
                    SpotQuoteBar("AAAA3", "2026-03-03", 100, 101, 94, 95, 1_200_000, 120),
                    SpotQuoteBar("AAAA3", "2026-03-04", 96, 99, 95, 98, 1_150_000, 115),
                ],
            )
            replace_option_history(
                connection,
                "AAAA",
                [
                    OptionQuoteBar("AAAAC7", "AAAA", "call", "2026-03-02", "2026-03-09", 98, 1.00, 1.60, 0.90, 1.40, 10_000, 50),
                    OptionQuoteBar("AAAAC15", "AAAA", "call", "2026-03-02", "2026-03-17", 98, 2.00, 2.50, 1.90, 2.30, 12_000, 60),
                    OptionQuoteBar("AAAAP7", "AAAA", "put", "2026-03-02", "2026-03-09", 98, 1.20, 1.30, 0.80, 0.90, 9_000, 40),
                    OptionQuoteBar("AAAAC15B", "AAAA", "call", "2026-03-03", "2026-03-18", 95, 2.10, 2.30, 2.00, 2.20, 11_000, 55),
                    OptionQuoteBar("AAAAC15B", "AAAA", "call", "2026-03-04", "2026-03-18", 95, 2.60, 2.90, 2.50, 2.80, 10_500, 52),
                    OptionQuoteBar("AAAAC30", "AAAA", "call", "2026-03-03", "2026-04-03", 95, 2.90, 3.10, 2.80, 3.00, 8_500, 45),
                    OptionQuoteBar("AAAAC30", "AAAA", "call", "2026-03-04", "2026-04-03", 95, 3.30, 3.60, 3.20, 3.50, 8_200, 44),
                ],
            )

        summaries, trades = backtest_option_strategies(
            db_path=db_path,
            tickers_file=tickers_path,
            start_date="2026-03-01",
            end_date="2026-03-04",
        )

        summary_map = {item.code: item for item in summaries}
        self.assertIn("opt_gap_baixa_2_call_atm_7dte_d0", summary_map)
        self.assertIn("opt_close_queda_3_call_atm_15dte_d1", summary_map)
        self.assertAlmostEqual(
            summary_map["opt_gap_baixa_2_call_atm_7dte_d0"].average_trade_return_pct,
            40.0,
            places=4,
        )
        self.assertAlmostEqual(
            summary_map["opt_close_queda_3_call_atm_15dte_d1"].net_trade_return_pct,
            27.2727,
            places=3,
        )

        trade_map = {item.strategy_code: item for item in trades}
        self.assertEqual(trade_map["opt_gap_baixa_2_call_atm_7dte_d0"].instrument_symbol, "AAAAC7")
        self.assertEqual(trade_map["opt_close_queda_3_call_atm_15dte_d1"].instrument_symbol, "AAAAC15B")

    def test_backtest_option_strategies_supports_cumulative_return_hold_by_calendar(self) -> None:
        temp_dir = _workspace_test_dir("options-cumulative")
        self.addCleanup(lambda: rmtree(temp_dir, ignore_errors=True))
        db_path = temp_dir / "history.db"
        tickers_path = temp_dir / "tickers.csv"
        tickers_path.write_text("Ticker,Nome\nAAAA3,Teste\n", encoding="utf-8")

        with connect(db_path) as connection:
            initialize_database(connection)
            replace_spot_history(
                connection,
                "AAAA3",
                [
                    SpotQuoteBar("AAAA3", "2026-03-01", 100, 101, 99, 100, 1_000_000, 100),
                    SpotQuoteBar("AAAA3", "2026-03-02", 99, 100, 97, 98, 1_100_000, 105),
                    SpotQuoteBar("AAAA3", "2026-03-03", 97, 98, 94, 95, 1_200_000, 110),
                    SpotQuoteBar("AAAA3", "2026-03-10", 98, 100, 97, 99, 1_300_000, 120),
                ],
            )
            replace_option_history(
                connection,
                "AAAA",
                [
                    OptionQuoteBar("AAAAC7C", "AAAA", "call", "2026-03-03", "2026-03-17", 95, 1.80, 2.10, 1.70, 2.00, 10_000, 50),
                    OptionQuoteBar("AAAAC7C", "AAAA", "call", "2026-03-10", "2026-03-17", 95, 2.40, 2.70, 2.30, 2.60, 12_000, 55),
                ],
            )

        summaries, trades = backtest_option_strategies(
            db_path=db_path,
            tickers_file=tickers_path,
            start_date="2026-03-01",
            end_date="2026-03-10",
            strategy_definitions=[
                OptionStrategyDefinition(
                    code="opt_test_cumulative",
                    label="Teste cumulative",
                    family="option_multi_day_reversal",
                    setup_kind="cumulative_return",
                    trigger_direction="down",
                    threshold_pct=3.0,
                    option_side="call",
                    dte_target_days=7,
                    entry_rule="close",
                    holding_days=7,
                    min_trade_return_pct=0.0,
                    round_trip_cost_pct=0.0,
                    lookback_days=2,
                )
            ],
        )

        self.assertEqual(len(summaries), 1)
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].exit_date, "2026-03-10")
        self.assertAlmostEqual(trades[0].trade_return_pct, 30.0, places=4)

    def test_backtest_option_strategies_supports_moving_average_cross(self) -> None:
        temp_dir = _workspace_test_dir("options-ma-cross")
        self.addCleanup(lambda: rmtree(temp_dir, ignore_errors=True))
        db_path = temp_dir / "history.db"
        tickers_path = temp_dir / "tickers.csv"
        tickers_path.write_text("Ticker,Nome\nAAAA3,Teste\n", encoding="utf-8")

        with connect(db_path) as connection:
            initialize_database(connection)
            replace_spot_history(
                connection,
                "AAAA3",
                [
                    SpotQuoteBar("AAAA3", "2026-03-01", 10, 10, 9, 10, 100_000, 10),
                    SpotQuoteBar("AAAA3", "2026-03-02", 9, 9, 8, 9, 110_000, 11),
                    SpotQuoteBar("AAAA3", "2026-03-03", 11, 11, 10, 11, 120_000, 12),
                    SpotQuoteBar("AAAA3", "2026-03-04", 12, 12, 11, 12, 130_000, 13),
                    SpotQuoteBar("AAAA3", "2026-03-05", 13, 13, 12, 13, 140_000, 14),
                ],
            )
            replace_option_history(
                connection,
                "AAAA",
                [
                    OptionQuoteBar("AAAACMA", "AAAA", "call", "2026-03-04", "2026-03-17", 12, 1.80, 2.10, 1.70, 2.00, 9_000, 40),
                    OptionQuoteBar("AAAACMA", "AAAA", "call", "2026-03-05", "2026-03-17", 12, 2.20, 2.60, 2.10, 2.50, 9_500, 42),
                ],
            )

        summaries, trades = backtest_option_strategies(
            db_path=db_path,
            tickers_file=tickers_path,
            start_date="2026-03-01",
            end_date="2026-03-05",
            strategy_definitions=[
                OptionStrategyDefinition(
                    code="opt_test_ma_cross",
                    label="Teste MA cross",
                    family="option_moving_average",
                    setup_kind="moving_average_cross",
                    trigger_direction="up",
                    threshold_pct=0.0,
                    option_side="call",
                    dte_target_days=7,
                    entry_rule="close",
                    holding_days=1,
                    min_trade_return_pct=0.0,
                    round_trip_cost_pct=0.0,
                    fast_ma_days=2,
                    slow_ma_days=3,
                )
            ],
        )

        self.assertEqual(len(summaries), 1)
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].trigger_date, "2026-03-04")
        self.assertAlmostEqual(trades[0].trade_return_pct, 25.0, places=4)

    def test_backtest_option_strategies_supports_target_stop_exit(self) -> None:
        temp_dir = _workspace_test_dir("options-target-stop")
        self.addCleanup(lambda: rmtree(temp_dir, ignore_errors=True))
        db_path = temp_dir / "history.db"
        tickers_path = temp_dir / "tickers.csv"
        tickers_path.write_text("Ticker,Nome\nAAAA3,Teste\n", encoding="utf-8")

        with connect(db_path) as connection:
            initialize_database(connection)
            replace_spot_history(
                connection,
                "AAAA3",
                [
                    SpotQuoteBar("AAAA3", "2026-03-01", 100, 101, 99, 100, 1_000_000, 100),
                    SpotQuoteBar("AAAA3", "2026-03-02", 98, 100, 97, 99, 1_000_000, 100),
                    SpotQuoteBar("AAAA3", "2026-03-09", 99, 100, 98, 99, 1_000_000, 100),
                ],
            )
            replace_option_history(
                connection,
                "AAAA",
                [
                    OptionQuoteBar("AAAATP", "AAAA", "call", "2026-03-02", "2026-03-17", 99, 1.00, 1.03, 0.98, 1.01, 8_000, 40),
                    OptionQuoteBar("AAAATP", "AAAA", "call", "2026-03-09", "2026-03-17", 99, 1.02, 1.04, 1.00, 1.03, 7_000, 38),
                ],
            )

        summaries, trades = backtest_option_strategies(
            db_path=db_path,
            tickers_file=tickers_path,
            start_date="2026-03-01",
            end_date="2026-03-09",
            strategy_definitions=[
                OptionStrategyDefinition(
                    code="opt_test_target_stop",
                    label="Teste target stop",
                    family="option_gap_target_stop",
                    setup_kind="gap",
                    trigger_direction="down",
                    threshold_pct=1.0,
                    option_side="call",
                    dte_target_days=7,
                    entry_rule="open",
                    holding_days=7,
                    min_trade_return_pct=0.0,
                    round_trip_cost_pct=0.0,
                    exit_kind="target_stop",
                    take_profit_pct=1.0,
                    stop_loss_pct=5.0,
                )
            ],
        )

        self.assertEqual(len(summaries), 1)
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].exit_reason, "take_profit")
        self.assertEqual(trades[0].exit_date, "2026-03-02")
        self.assertAlmostEqual(trades[0].trade_return_pct, 1.0, places=4)

    def test_option_registry_can_store_approved_and_rejected(self) -> None:
        temp_dir = _workspace_test_dir("options-registry")
        self.addCleanup(lambda: rmtree(temp_dir, ignore_errors=True))
        db_path = temp_dir / "history.db"
        registry_path = temp_dir / "registry.csv"
        registry_md_path = temp_dir / "registry.md"
        tickers_path = temp_dir / "tickers.csv"
        tickers_path.write_text("Ticker,Nome\nAAAA3,Teste\n", encoding="utf-8")

        with connect(db_path) as connection:
            initialize_database(connection)
            replace_spot_history(
                connection,
                "AAAA3",
                [
                    SpotQuoteBar("AAAA3", "2026-03-01", 100, 101, 99, 100, 1_000_000, 100),
                    SpotQuoteBar("AAAA3", "2026-03-02", 98, 101, 97, 99, 1_100_000, 110),
                    SpotQuoteBar("AAAA3", "2026-03-03", 100, 101, 94, 95, 1_200_000, 120),
                    SpotQuoteBar("AAAA3", "2026-03-04", 96, 99, 95, 98, 1_150_000, 115),
                ],
            )
            replace_option_history(
                connection,
                "AAAA",
                [
                    OptionQuoteBar("AAAAC7", "AAAA", "call", "2026-03-02", "2026-03-09", 98, 1.00, 1.60, 0.90, 1.40, 10_000, 50),
                    OptionQuoteBar("AAAAC15", "AAAA", "call", "2026-03-02", "2026-03-17", 98, 2.00, 2.50, 1.90, 2.30, 12_000, 60),
                    OptionQuoteBar("AAAAP7", "AAAA", "put", "2026-03-02", "2026-03-09", 98, 1.20, 1.30, 0.80, 0.90, 9_000, 40),
                    OptionQuoteBar("AAAAC15B", "AAAA", "call", "2026-03-03", "2026-03-18", 95, 2.10, 2.30, 2.00, 2.20, 11_000, 55),
                    OptionQuoteBar("AAAAC15B", "AAAA", "call", "2026-03-04", "2026-03-18", 95, 2.60, 2.90, 2.50, 2.80, 10_500, 52),
                ],
            )

        summaries, trades = backtest_option_strategies(
            db_path=db_path,
            tickers_file=tickers_path,
            start_date="2026-03-01",
            end_date="2026-03-04",
        )
        approved, _, rejected, rejection_reasons = split_strategy_results(
            summaries=summaries,
            trades=trades,
            min_success_rate_pct=50.0,
            min_profit_factor=1.0,
            min_average_trade_return_pct=0.0,
            min_trades=1,
            require_positive_net=True,
        )
        entries = build_registry_entries(approved, rejected, rejection_reasons)
        export_strategy_registry_csv(entries, registry_path)
        export_strategy_registry_markdown(entries, registry_md_path)

        registry_text = registry_path.read_text(encoding="utf-8")
        registry_md_text = registry_md_path.read_text(encoding="utf-8")
        self.assertIn("approved", registry_text)
        self.assertIn("status", registry_text)
        self.assertIn("## Aprovadas", registry_md_text)
        self.assertIn("## Reprovadas", registry_md_text)


if __name__ == "__main__":
    unittest.main()
