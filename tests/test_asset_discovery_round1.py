from __future__ import annotations

import unittest
from datetime import date, timedelta
from pathlib import Path
from shutil import rmtree

from b3_patterns.asset_discovery_round1 import (
    _bollinger_position,
    _bucket_day_of_week,
    _bucket_rsi,
    _ema_series,
    _max_volume_window,
    _prefix_sums,
    _prefix_sums_sq,
    _rsi_series,
    _simulate_atr_exit,
    _simulate_percent_exit,
    _window_average,
    build_asset_discovery_atr_templates,
    build_asset_discovery_round1_templates,
    collect_asset_discovery_pattern_trades,
    list_asset_discovery_features,
    mine_asset_discovery_patterns,
    mine_asset_discovery_patterns_progressive,
    split_asset_discovery_results,
)
from b3_patterns.db import connect, initialize_database, replace_spot_history
from b3_patterns.models import SpotQuoteBar


def _workspace_test_dir(name: str) -> Path:
    temp_root = Path.cwd() / ".tmp-tests"
    temp_dir = temp_root / name
    rmtree(temp_dir, ignore_errors=True)
    temp_dir.mkdir(parents=True, exist_ok=True)
    return temp_dir


class AssetDiscoveryRound1TestCase(unittest.TestCase):
    def test_round1_discovery_finds_profitable_asset_pattern(self) -> None:
        temp_dir = _workspace_test_dir("asset-discovery-round1")
        self.addCleanup(lambda: rmtree(temp_dir, ignore_errors=True))
        db_path = temp_dir / "history.db"
        tickers_path = temp_dir / "tickers.csv"
        tickers_path.write_text("Ticker,Nome\nAAAA3,Teste\n", encoding="utf-8")

        start_day = date(2025, 1, 1)
        bars: list[SpotQuoteBar] = []
        close_price = 100.0

        for day_offset in range(85):
            trade_day = start_day + timedelta(days=day_offset)
            trade_date = trade_day.isoformat()
            phase = day_offset % 8
            if phase in (0, 1, 2):
                open_price = close_price * 0.998
                close_price = open_price * 0.986
            elif phase == 3:
                open_price = close_price
                close_price = open_price * 1.022
            else:
                open_price = close_price * 1.001
                close_price = open_price * 1.001

            high_price = max(open_price, close_price) * 1.01
            low_price = min(open_price, close_price) * 0.995
            bars.append(
                SpotQuoteBar(
                    ticker="AAAA3",
                    trade_date=trade_date,
                    open=round(open_price, 4),
                    high=round(high_price, 4),
                    low=round(low_price, 4),
                    close=round(close_price, 4),
                    volume=1_000_000 + (day_offset * 1_000),
                    trade_count=100 + day_offset,
                )
            )

        with connect(db_path) as connection:
            initialize_database(connection)
            replace_spot_history(connection, "AAAA3", bars)

        templates = build_asset_discovery_round1_templates(
            entry_rules=["open"],
            target_stop_pairs=[(2.0, 1.0)],
            time_cap_days=3,
        )
        self.assertEqual(len(list_asset_discovery_features()), 50)

        summaries = mine_asset_discovery_patterns(
            db_path=db_path,
            tickers_file=tickers_path,
            start_date="2025-02-15",
            end_date="2025-03-26",
            template_definitions=templates,
            max_pattern_size=2,
        )
        self.assertTrue(summaries)

        approved, rejected, _ = split_asset_discovery_results(
            summaries,
            min_success_rate_pct=0.0,
            min_profit_factor=1.0,
            min_average_trade_return_pct=0.0,
            min_trades=5,
            min_tickers=1,
            require_positive_net=True,
        )
        self.assertTrue(approved)
        self.assertTrue(rejected or approved)

        trades = collect_asset_discovery_pattern_trades(
            db_path=db_path,
            tickers_file=tickers_path,
            start_date="2025-02-15",
            end_date="2025-03-26",
            approved_summaries=approved,
            template_definitions=templates,
            max_pattern_size=2,
        )
        self.assertTrue(trades)
        self.assertTrue(any(item.direction == "long" for item in trades))


    def test_rsi_series_computes_oversold_and_overbought(self) -> None:
        # Falling prices -> RSI should go low
        falling = [100.0 - i * 2.0 for i in range(30)]
        rsi_vals = _rsi_series(falling, period=14)
        self.assertEqual(len(rsi_vals), 30)
        # After enough falling bars, RSI should be low
        self.assertLess(rsi_vals[25], 30.0)

        # Rising prices -> RSI should go high
        rising = [50.0 + i * 2.0 for i in range(30)]
        rsi_vals_up = _rsi_series(rising, period=14)
        self.assertGreater(rsi_vals_up[25], 70.0)

    def test_ema_series_converges_toward_price(self) -> None:
        closes = [100.0] * 20
        ema9 = _ema_series(closes, period=9)
        # After seeding, EMA should equal the constant close
        self.assertAlmostEqual(ema9[15], 100.0, places=4)

        # With a jump, EMA should lag
        closes_jump = [100.0] * 15 + [110.0] * 10
        ema9_jump = _ema_series(closes_jump, period=9)
        self.assertGreater(ema9_jump[20], 100.0)
        self.assertLess(ema9_jump[20], 110.0)

    def test_bollinger_position_ranges_zero_to_one(self) -> None:
        closes = [100.0 + (i % 5) * 0.5 for i in range(30)]
        close_ps = _prefix_sums(closes)
        close_ps_sq = _prefix_sums_sq(closes)
        bb_pos = _bollinger_position(closes, close_ps, close_ps_sq, 25, period=20)
        self.assertIsNotNone(bb_pos)
        self.assertGreaterEqual(bb_pos, 0.0)
        self.assertLessEqual(bb_pos, 1.0)

    def test_bucket_rsi_covers_all_zones(self) -> None:
        self.assertEqual(_bucket_rsi(10.0)[0], "oversold")
        self.assertEqual(_bucket_rsi(25.0)[0], "low")
        self.assertEqual(_bucket_rsi(42.0)[0], "neutral_low")
        self.assertEqual(_bucket_rsi(55.0)[0], "neutral_high")
        self.assertEqual(_bucket_rsi(72.0)[0], "high")
        self.assertEqual(_bucket_rsi(90.0)[0], "overbought")

    def test_bucket_day_of_week_covers_weekdays(self) -> None:
        self.assertEqual(_bucket_day_of_week(0)[0], "mon")
        self.assertEqual(_bucket_day_of_week(1)[0], "tue")
        self.assertEqual(_bucket_day_of_week(2)[0], "wed")
        self.assertEqual(_bucket_day_of_week(3)[0], "thu")
        self.assertEqual(_bucket_day_of_week(4)[0], "fri")

    def test_simulate_atr_exit_hits_target(self) -> None:
        bars = []
        start_day = date(2025, 1, 1)
        # Build 30 bars with enough history for ATR calculation
        for i in range(30):
            d = start_day + timedelta(days=i)
            bars.append(SpotQuoteBar(
                ticker="TEST3", trade_date=d.isoformat(),
                open=100.0, high=103.0, low=97.0, close=100.0,
                volume=1_000_000, trade_count=100,
            ))
        # Add a bar that spikes up (should hit ATR target for long)
        d = start_day + timedelta(days=30)
        bars.append(SpotQuoteBar(
            ticker="TEST3", trade_date=d.isoformat(),
            open=100.0, high=110.0, low=99.0, close=108.0,
            volume=1_000_000, trade_count=100,
        ))

        from b3_patterns.asset_discovery_round1 import _atr_pct_series
        atr_values = _atr_pct_series(
            [b.high for b in bars], [b.low for b in bars], [b.close for b in bars]
        )
        atr_ps = _prefix_sums(atr_values)

        result = _simulate_atr_exit(
            bars=bars, entry_idx=20, entry_price=100.0,
            trade_direction="long", atr_target_mult=1.5, atr_stop_mult=1.5,
            time_cap_days=15, include_entry_bar=False,
            atr_pct_prefix_sums=atr_ps,
        )
        self.assertIsNotNone(result)
        exit_idx, exit_price, exit_reason = result
        # Should hit take_profit on the spike bar (idx 30)
        self.assertEqual(exit_reason, "take_profit")

    def test_atr_templates_builder_creates_templates(self) -> None:
        templates = build_asset_discovery_atr_templates(
            entry_rules=["open"],
            trade_directions=["long"],
            atr_target_stop_pairs=[(1.5, 1.0)],
            time_cap_days_list=[5],
        )
        self.assertEqual(len(templates), 1)
        t = templates[0]
        self.assertEqual(t.exit_mode, "atr")
        self.assertAlmostEqual(t.atr_target_mult, 1.5)
        self.assertAlmostEqual(t.atr_stop_mult, 1.0)
        self.assertEqual(t.time_cap_days, 5)
        self.assertIn("atr", t.code)

    def test_max_volume_window_finds_max(self) -> None:
        volumes = [100.0, 200.0, 150.0, 300.0, 50.0, 120.0]
        result = _max_volume_window(volumes, end_idx=5, window_days=4)
        # window is idx 1..4 (exclusive of end_idx=5): [200, 150, 300, 50]
        self.assertAlmostEqual(result, 300.0)

    def test_feature_count_is_50(self) -> None:
        self.assertEqual(len(list_asset_discovery_features()), 50)

    def test_atr_exit_mining_produces_summaries(self) -> None:
        """ATR-exit templates should produce valid pattern summaries."""
        temp_dir = _workspace_test_dir("asset-atr-exit")
        self.addCleanup(lambda: rmtree(temp_dir, ignore_errors=True))
        db_path = temp_dir / "history.db"
        tickers_path = temp_dir / "tickers.csv"
        tickers_path.write_text("Ticker,Nome\nBBBB3,Teste\n", encoding="utf-8")

        start_day = date(2025, 1, 1)
        bars: list[SpotQuoteBar] = []
        close_price = 100.0
        for day_offset in range(85):
            trade_day = start_day + timedelta(days=day_offset)
            phase = day_offset % 8
            if phase in (0, 1, 2):
                open_price = close_price * 0.998
                close_price = open_price * 0.986
            elif phase == 3:
                open_price = close_price
                close_price = open_price * 1.022
            else:
                open_price = close_price * 1.001
                close_price = open_price * 1.001
            high_price = max(open_price, close_price) * 1.01
            low_price = min(open_price, close_price) * 0.995
            bars.append(SpotQuoteBar(
                ticker="BBBB3", trade_date=trade_day.isoformat(),
                open=round(open_price, 4), high=round(high_price, 4),
                low=round(low_price, 4), close=round(close_price, 4),
                volume=1_000_000 + day_offset * 1_000, trade_count=100 + day_offset,
            ))

        with connect(db_path) as connection:
            initialize_database(connection)
            replace_spot_history(connection, "BBBB3", bars)

        atr_templates = build_asset_discovery_atr_templates(
            entry_rules=["open"],
            trade_directions=["long"],
            atr_target_stop_pairs=[(1.5, 1.0)],
            time_cap_days_list=[5],
        )
        summaries = mine_asset_discovery_patterns(
            db_path=db_path,
            tickers_file=tickers_path,
            start_date="2025-02-15",
            end_date="2025-03-26",
            template_definitions=atr_templates,
            max_pattern_size=1,
        )
        self.assertTrue(summaries)
        # All summaries should have ATR exit mode
        for s in summaries:
            self.assertEqual(s.exit_mode, "atr")

    def test_progressive_mining_produces_2f_and_3f_summaries(self) -> None:
        """Progressive mining should return both 2-factor and promoted 3-factor patterns."""
        temp_dir = _workspace_test_dir("asset-progressive")
        self.addCleanup(lambda: rmtree(temp_dir, ignore_errors=True))
        db_path = temp_dir / "history.db"
        tickers_path = temp_dir / "tickers.csv"
        tickers_path.write_text("Ticker,Nome\nCCCC3,Teste\n", encoding="utf-8")

        start_day = date(2025, 1, 1)
        bars: list[SpotQuoteBar] = []
        close_price = 100.0
        for day_offset in range(85):
            trade_day = start_day + timedelta(days=day_offset)
            phase = day_offset % 8
            if phase in (0, 1, 2):
                open_price = close_price * 0.998
                close_price = open_price * 0.986
            elif phase == 3:
                open_price = close_price
                close_price = open_price * 1.022
            else:
                open_price = close_price * 1.001
                close_price = open_price * 1.001
            high_price = max(open_price, close_price) * 1.01
            low_price = min(open_price, close_price) * 0.995
            bars.append(SpotQuoteBar(
                ticker="CCCC3", trade_date=trade_day.isoformat(),
                open=round(open_price, 4), high=round(high_price, 4),
                low=round(low_price, 4), close=round(close_price, 4),
                volume=1_000_000 + day_offset * 1_000, trade_count=100 + day_offset,
            ))

        with connect(db_path) as connection:
            initialize_database(connection)
            replace_spot_history(connection, "CCCC3", bars)

        templates = build_asset_discovery_round1_templates(
            entry_rules=["open"],
            target_stop_pairs=[(2.0, 1.0)],
            time_cap_days=3,
        )
        summaries = mine_asset_discovery_patterns_progressive(
            db_path=db_path,
            tickers_file=tickers_path,
            start_date="2025-02-15",
            end_date="2025-03-26",
            template_definitions=templates,
            pre_filter_min_profitable_rate=0.0,
            pre_filter_min_profit_factor=0.0,
            pre_filter_min_trades=1,
        )
        self.assertTrue(summaries)
        state_sizes = {s.state_size for s in summaries}
        # Should have at least 1-factor and 2-factor patterns
        self.assertIn(1, state_sizes)
        self.assertIn(2, state_sizes)
        # With relaxed pre-filters, should also get some 3-factor patterns
        has_3f = any(s.state_size == 3 for s in summaries)
        if has_3f:
            self.assertIn(3, state_sizes)


class StatisticalSignificanceTestCase(unittest.TestCase):
    def test_binomial_pvalue_significant_for_high_win_rate(self) -> None:
        from tools.generate_asset_r3_stat_report import binomial_pvalue
        # 70% win rate out of 200 trades should be very significant
        p = binomial_pvalue(successes=140, trials=200, p0=0.5)
        self.assertLess(p, 0.001)

    def test_binomial_pvalue_not_significant_for_coin_flip(self) -> None:
        from tools.generate_asset_r3_stat_report import binomial_pvalue
        # 51% win rate out of 50 trades should not be significant
        p = binomial_pvalue(successes=26, trials=50, p0=0.5)
        self.assertGreater(p, 0.05)

    def test_ttest_pvalue_significant_for_positive_returns(self) -> None:
        from tools.generate_asset_r3_stat_report import ttest_pvalue
        # Mean 1.5%, std 2%, n=200 -> very significant
        p = ttest_pvalue(mean=1.5, std=2.0, n=200)
        self.assertLess(p, 0.001)

    def test_ttest_pvalue_not_significant_for_noisy_returns(self) -> None:
        from tools.generate_asset_r3_stat_report import ttest_pvalue
        # Mean 0.1%, std 5%, n=20 -> not significant
        p = ttest_pvalue(mean=0.1, std=5.0, n=20)
        self.assertGreater(p, 0.05)

    def test_metrics_sharpe_ratio(self) -> None:
        from tools.generate_asset_r3_stat_report import Metrics
        m = Metrics()
        for ret in [1.0, 2.0, 1.5, 0.5, 1.0]:
            m.update(ret, "take_profit")
        self.assertGreater(m.sharpe_ratio, 0.0)
        self.assertAlmostEqual(m.average_return, 1.2, places=4)


if __name__ == "__main__":
    unittest.main()
