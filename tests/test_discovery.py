from __future__ import annotations

import csv
import unittest
from datetime import date, timedelta
from pathlib import Path
from shutil import rmtree

from b3_patterns.db import connect, initialize_database, replace_option_history, replace_spot_history
from b3_patterns.discovery import (
    build_discovery_registry_entries,
    build_option_discovery_templates,
    collect_discovery_pattern_trades,
    list_discovery_features,
    mine_option_discovery_patterns,
    split_discovery_results,
)
from b3_patterns.discovery_refinement import refine_discovery_shortlist
from b3_patterns.models import (
    DiscoveryTemplateBaseline,
    OptionQuoteBar,
    SpotQuoteBar,
    StrategyTrade,
)
from b3_patterns.discovery_refinement import load_discovery_summaries
from b3_patterns.registry import merge_registry_entries
from b3_patterns.reporting import export_discovery_csv, export_strategy_trades_csv


def _workspace_test_dir(name: str) -> Path:
    temp_root = Path.cwd() / ".tmp-tests"
    temp_dir = temp_root / name
    rmtree(temp_dir, ignore_errors=True)
    temp_dir.mkdir(parents=True, exist_ok=True)
    return temp_dir


class DiscoveryBacktestTestCase(unittest.TestCase):
    def test_build_option_discovery_templates_and_features(self) -> None:
        templates = build_option_discovery_templates()
        self.assertEqual(len(templates), 6)
        self.assertEqual({item.dte_target_days for item in templates}, {7, 15, 30})
        self.assertEqual({item.option_side for item in templates}, {"call", "put"})
        self.assertEqual(len(list_discovery_features()), 20)

    def test_mine_option_discovery_patterns_finds_profitable_gap_cluster(self) -> None:
        temp_dir = _workspace_test_dir("options-discovery")
        self.addCleanup(lambda: rmtree(temp_dir, ignore_errors=True))
        db_path = temp_dir / "history.db"
        tickers_path = temp_dir / "tickers.csv"
        export_path = temp_dir / "approved.csv"
        tickers_path.write_text("Ticker,Nome\nAAAA3,Teste\n", encoding="utf-8")

        start_day = date(2026, 1, 1)
        spot_bars: list[SpotQuoteBar] = []
        option_bars: list[OptionQuoteBar] = []
        previous_close = 100.0

        for day_offset in range(35):
            trade_day = start_day + timedelta(days=day_offset)
            trade_date = trade_day.isoformat()
            if day_offset < 20:
                open_price = previous_close * 1.002
                close_price = open_price * 1.001
            else:
                open_price = previous_close * 0.98
                close_price = open_price * 1.01
            high_price = max(open_price, close_price) * 1.01
            low_price = min(open_price, close_price) * 0.99
            spot_bars.append(
                SpotQuoteBar(
                    "AAAA3",
                    trade_date,
                    round(open_price, 4),
                    round(high_price, 4),
                    round(low_price, 4),
                    round(close_price, 4),
                    1_000_000 + (day_offset * 1_000),
                    100 + day_offset,
                )
            )

            if day_offset >= 20:
                expiration_7d = (trade_day + timedelta(days=7)).isoformat()
                option_bars.append(
                    OptionQuoteBar(
                        f"AAAAC{day_offset:02d}",
                        "AAAA",
                        "call",
                        trade_date,
                        expiration_7d,
                        round(open_price, 2),
                        1.00,
                        1.25,
                        0.98,
                        1.20,
                        12_000,
                        120,
                    )
                )

            previous_close = close_price

        with connect(db_path) as connection:
            initialize_database(connection)
            replace_spot_history(connection, "AAAA3", spot_bars)
            replace_option_history(connection, "AAAA", option_bars)

        templates = [item for item in build_option_discovery_templates(dte_targets=[7]) if item.option_side == "call"]
        summaries = mine_option_discovery_patterns(
            db_path=db_path,
            tickers_file=tickers_path,
            start_date="2026-01-01",
            end_date="2026-02-15",
            template_definitions=templates,
            max_pattern_size=2,
        )

        self.assertTrue(summaries)
        self.assertTrue(any(item.state_size == 2 for item in summaries))

        approved, rejected, rejection_reasons = split_discovery_results(
            summaries,
            min_success_rate_pct=50.0,
            min_profit_factor=1.0,
            min_average_trade_return_pct=0.0,
            min_trades=5,
            min_tickers=1,
            require_positive_net=True,
        )

        self.assertTrue(approved)
        self.assertIn(
            "disc_call_atm_7dte_d0__1f__gap_pct=dn_1_3",
            {item.code for item in approved},
        )

        approved_trades = collect_discovery_pattern_trades(
            db_path=db_path,
            tickers_file=tickers_path,
            start_date="2026-01-01",
            end_date="2026-02-15",
            approved_summaries=approved,
            template_definitions=templates,
            max_pattern_size=2,
        )
        self.assertTrue(approved_trades)
        self.assertTrue(
            any(item.strategy_code == "disc_call_atm_7dte_d0__1f__gap_pct=dn_1_3" for item in approved_trades)
        )

        exported_path = export_discovery_csv(approved, export_path)
        self.assertTrue(exported_path.exists())
        self.assertIn("state_signature", export_path.read_text(encoding="utf-8"))

        entries = build_discovery_registry_entries(approved, rejected, rejection_reasons)
        merged = merge_registry_entries([], entries)
        self.assertEqual(len(merged), len(entries))
        self.assertTrue(any(item.status == "approved" for item in merged))

    def test_refine_discovery_shortlist_filters_and_deduplicates_patterns(self) -> None:
        temp_dir = _workspace_test_dir("options-discovery-refine")
        self.addCleanup(lambda: rmtree(temp_dir, ignore_errors=True))
        summaries_path = temp_dir / "approved.csv"
        trades_path = temp_dir / "approved-trades.csv"

        approved_summaries = [
            {
                "code": "pattern_best",
                "label": "Best",
                "family": "option_state_discovery",
                "template_code": "disc_call_atm_7dte_d0",
                "template_label": "CALL 7 DTE",
                "option_side": "call",
                "dte_target_days": 7,
                "trade_direction": "long_call",
                "entry_rule": "open",
                "exit_offset_days": 0,
                "state_size": 2,
                "state_signature": "A;B",
                "feature_keys": "a,b",
                "min_trade_return_pct": "0.0000",
                "tickers_with_matches": 3,
                "total_occurrences": 6,
                "successful_occurrences": 4,
                "success_rate_pct": "66.6667",
                "profitable_trades": 4,
                "profitable_trade_rate_pct": "66.6667",
                "average_asset_move_pct": "1.5000",
                "average_trade_return_pct": "1.5000",
                "net_trade_return_pct": "9.0000",
                "cumulative_return_pct": "8.0000",
                "profit_factor": "1.5000",
                "first_trade_date": "2025-10-10",
                "last_trade_date": "2026-02-10",
            },
            {
                "code": "pattern_dup",
                "label": "Dup",
                "family": "option_state_discovery",
                "template_code": "disc_call_atm_7dte_d0",
                "template_label": "CALL 7 DTE",
                "option_side": "call",
                "dte_target_days": 7,
                "trade_direction": "long_call",
                "entry_rule": "open",
                "exit_offset_days": 0,
                "state_size": 2,
                "state_signature": "A;C",
                "feature_keys": "a,c",
                "min_trade_return_pct": "0.0000",
                "tickers_with_matches": 3,
                "total_occurrences": 6,
                "successful_occurrences": 4,
                "success_rate_pct": "66.6667",
                "profitable_trades": 4,
                "profitable_trade_rate_pct": "66.6667",
                "average_asset_move_pct": "1.4000",
                "average_trade_return_pct": "1.4000",
                "net_trade_return_pct": "8.4000",
                "cumulative_return_pct": "7.0000",
                "profit_factor": "1.4500",
                "first_trade_date": "2025-10-10",
                "last_trade_date": "2026-02-10",
            },
            {
                "code": "pattern_bad_validation",
                "label": "Bad Validation",
                "family": "option_state_discovery",
                "template_code": "disc_put_atm_7dte_d0",
                "template_label": "PUT 7 DTE",
                "option_side": "put",
                "dte_target_days": 7,
                "trade_direction": "long_put",
                "entry_rule": "open",
                "exit_offset_days": 0,
                "state_size": 2,
                "state_signature": "D;E",
                "feature_keys": "d,e",
                "min_trade_return_pct": "0.0000",
                "tickers_with_matches": 3,
                "total_occurrences": 6,
                "successful_occurrences": 3,
                "success_rate_pct": "50.0000",
                "profitable_trades": 3,
                "profitable_trade_rate_pct": "50.0000",
                "average_asset_move_pct": "1.1000",
                "average_trade_return_pct": "1.1000",
                "net_trade_return_pct": "6.6000",
                "cumulative_return_pct": "5.0000",
                "profit_factor": "1.3000",
                "first_trade_date": "2025-10-10",
                "last_trade_date": "2026-02-10",
            },
        ]
        with summaries_path.open("w", encoding="utf-8", newline="") as file_obj:
            writer = csv.DictWriter(file_obj, fieldnames=list(approved_summaries[0].keys()))
            writer.writeheader()
            writer.writerows(approved_summaries)

        trades = [
            StrategyTrade("pattern_best", "Best", "option_state_discovery", "AAAA3", "2025-10-10", "2025-10-10", "long_call", 0.0, 1.0, 1.2, 20.0, 20.0, True, True, "OPT1", "2025-10-17", 7, "time_exit"),
            StrategyTrade("pattern_best", "Best", "option_state_discovery", "BBBB3", "2025-11-10", "2025-11-10", "long_call", 0.0, 1.0, 1.1, 10.0, 10.0, True, True, "OPT2", "2025-11-17", 7, "time_exit"),
            StrategyTrade("pattern_best", "Best", "option_state_discovery", "CCCC3", "2025-12-10", "2025-12-10", "long_call", 0.0, 1.0, 0.9, -10.0, -10.0, False, False, "OPT3", "2025-12-17", 7, "time_exit"),
            StrategyTrade("pattern_best", "Best", "option_state_discovery", "AAAA3", "2026-01-10", "2026-01-10", "long_call", 0.0, 1.0, 1.2, 20.0, 20.0, True, True, "OPT4", "2026-01-17", 7, "time_exit"),
            StrategyTrade("pattern_best", "Best", "option_state_discovery", "BBBB3", "2026-02-10", "2026-02-10", "long_call", 0.0, 1.0, 1.1, 10.0, 10.0, True, True, "OPT5", "2026-02-17", 7, "time_exit"),
            StrategyTrade("pattern_best", "Best", "option_state_discovery", "CCCC3", "2026-02-20", "2026-02-20", "long_call", 0.0, 1.0, 0.9, -10.0, -10.0, False, False, "OPT6", "2026-02-27", 7, "time_exit"),
            StrategyTrade("pattern_dup", "Dup", "option_state_discovery", "AAAA3", "2025-10-10", "2025-10-10", "long_call", 0.0, 1.0, 1.19, 19.0, 19.0, True, True, "OPT1", "2025-10-17", 7, "time_exit"),
            StrategyTrade("pattern_dup", "Dup", "option_state_discovery", "BBBB3", "2025-11-10", "2025-11-10", "long_call", 0.0, 1.0, 1.09, 9.0, 9.0, True, True, "OPT2", "2025-11-17", 7, "time_exit"),
            StrategyTrade("pattern_dup", "Dup", "option_state_discovery", "CCCC3", "2025-12-10", "2025-12-10", "long_call", 0.0, 1.0, 0.91, -9.0, -9.0, False, False, "OPT3", "2025-12-17", 7, "time_exit"),
            StrategyTrade("pattern_dup", "Dup", "option_state_discovery", "AAAA3", "2026-01-10", "2026-01-10", "long_call", 0.0, 1.0, 1.19, 19.0, 19.0, True, True, "OPT4", "2026-01-17", 7, "time_exit"),
            StrategyTrade("pattern_dup", "Dup", "option_state_discovery", "BBBB3", "2026-02-10", "2026-02-10", "long_call", 0.0, 1.0, 1.09, 9.0, 9.0, True, True, "OPT5", "2026-02-17", 7, "time_exit"),
            StrategyTrade("pattern_dup", "Dup", "option_state_discovery", "CCCC3", "2026-02-20", "2026-02-20", "long_call", 0.0, 1.0, 0.91, -9.0, -9.0, False, False, "OPT6", "2026-02-27", 7, "time_exit"),
            StrategyTrade("pattern_bad_validation", "Bad Validation", "option_state_discovery", "AAAA3", "2025-10-10", "2025-10-10", "long_put", 0.0, 1.0, 1.15, 15.0, 15.0, True, True, "P1", "2025-10-17", 7, "time_exit"),
            StrategyTrade("pattern_bad_validation", "Bad Validation", "option_state_discovery", "BBBB3", "2025-11-10", "2025-11-10", "long_put", 0.0, 1.0, 1.10, 10.0, 10.0, True, True, "P2", "2025-11-17", 7, "time_exit"),
            StrategyTrade("pattern_bad_validation", "Bad Validation", "option_state_discovery", "CCCC3", "2025-12-10", "2025-12-10", "long_put", 0.0, 1.0, 0.95, -5.0, -5.0, False, False, "P3", "2025-12-17", 7, "time_exit"),
            StrategyTrade("pattern_bad_validation", "Bad Validation", "option_state_discovery", "AAAA3", "2026-01-10", "2026-01-10", "long_put", 0.0, 1.0, 0.95, -5.0, -5.0, False, False, "P4", "2026-01-17", 7, "time_exit"),
            StrategyTrade("pattern_bad_validation", "Bad Validation", "option_state_discovery", "BBBB3", "2026-02-10", "2026-02-10", "long_put", 0.0, 1.0, 0.90, -10.0, -10.0, False, False, "P5", "2026-02-17", 7, "time_exit"),
            StrategyTrade("pattern_bad_validation", "Bad Validation", "option_state_discovery", "CCCC3", "2026-02-20", "2026-02-20", "long_put", 0.0, 1.0, 0.85, -15.0, -15.0, False, False, "P6", "2026-02-27", 7, "time_exit"),
        ]
        export_strategy_trades_csv(trades, trades_path)

        summaries = load_discovery_summaries(summaries_path)
        baselines = {
            "disc_call_atm_7dte_d0": DiscoveryTemplateBaseline(
                "disc_call_atm_7dte_d0",
                "CALL 7 DTE",
                "call",
                7,
                100,
                45,
                45.0,
                0.2,
                20.0,
                1.05,
                "2025-10-01",
                "2026-02-28",
            ),
            "disc_put_atm_7dte_d0": DiscoveryTemplateBaseline(
                "disc_put_atm_7dte_d0",
                "PUT 7 DTE",
                "put",
                7,
                100,
                40,
                40.0,
                0.1,
                10.0,
                1.0,
                "2025-10-01",
                "2026-02-28",
            ),
        }

        refined, rejection_reasons, refined_trades = refine_discovery_shortlist(
            summaries=summaries,
            trades_csv_path=trades_path,
            baselines=baselines,
            validation_start_date="2026-01-01",
            min_state_size=2,
            min_avg_uplift_pct=0.5,
            min_pf_uplift=0.20,
            min_total_trades=6,
            min_tickers=3,
            min_train_trades=3,
            min_validation_trades=3,
            min_train_profit_factor=1.10,
            min_validation_profit_factor=1.10,
            min_train_average_trade_return_pct=0.0,
            min_validation_average_trade_return_pct=0.0,
            min_active_months=4,
            min_positive_month_ratio=0.50,
            min_validation_active_months=2,
            min_validation_positive_month_ratio=0.50,
            overlap_threshold=0.85,
        )

        self.assertEqual([item.code for item in refined], ["pattern_best"])
        self.assertIn("pattern_dup", rejection_reasons)
        self.assertIn("pattern_bad_validation", rejection_reasons)
        self.assertEqual(len(refined_trades), 6)


if __name__ == "__main__":
    unittest.main()
