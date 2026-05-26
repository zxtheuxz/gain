from __future__ import annotations

import unittest
from pathlib import Path
from shutil import rmtree

from b3_patterns.tickers import load_tickers, normalize_ticker


class TickersTestCase(unittest.TestCase):
    def test_normalize_ticker_adds_b3_suffix(self) -> None:
        self.assertEqual(normalize_ticker("petr4"), "PETR4.SA")
        self.assertEqual(normalize_ticker("vale3.sa"), "VALE3.SA")

    def test_load_tickers_preserves_order_and_deduplicates(self) -> None:
        temp_root = Path.cwd() / ".tmp-tests"
        temp_dir = temp_root / "tickers"
        rmtree(temp_dir, ignore_errors=True)
        temp_dir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: rmtree(temp_dir, ignore_errors=True))
        csv_path = temp_dir / "tickers.csv"
        csv_path.write_text(
            "Ticker,Nome\nPETR4,Petrobras\nVALE3,Vale\nPETR4,Petrobras\n",
            encoding="utf-8",
        )

        self.assertEqual(load_tickers(csv_path), ["PETR4.SA", "VALE3.SA"])

    def test_load_tickers_accepts_plain_markdown_list(self) -> None:
        temp_root = Path.cwd() / ".tmp-tests"
        temp_dir = temp_root / "tickers-md"
        rmtree(temp_dir, ignore_errors=True)
        temp_dir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: rmtree(temp_dir, ignore_errors=True))
        md_path = temp_dir / "lista.md"
        md_path.write_text(
            "# Lista liquida\n\nPETR4\nVALE3\n- ITUB4\nPETR4\n",
            encoding="utf-8",
        )

        self.assertEqual(load_tickers(md_path), ["PETR4.SA", "VALE3.SA", "ITUB4.SA"])


if __name__ == "__main__":
    unittest.main()
