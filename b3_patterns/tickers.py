from __future__ import annotations

import csv
from pathlib import Path


def normalize_ticker(raw_ticker: str) -> str:
    ticker = raw_ticker.strip().upper()
    if not ticker:
        raise ValueError("Ticker vazio encontrado na lista.")
    if ticker.endswith(".SA"):
        return ticker
    return f"{ticker}.SA"


def _append_unique_ticker(tickers: list[str], seen: set[str], raw_ticker: str) -> bool:
    normalized = normalize_ticker(raw_ticker)
    if normalized in seen:
        return False
    seen.add(normalized)
    tickers.append(normalized)
    return True


def _load_plain_ticker_lines(path: Path, limit: int | None) -> list[str]:
    tickers: list[str] = []
    seen: set[str] = set()

    with path.open("r", encoding="utf-8-sig") as file_obj:
        for raw_line in file_obj:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            token = line.removeprefix("-").strip().split()[0].strip("`|,;")
            if not token:
                continue
            _append_unique_ticker(tickers, seen, token)
            if limit is not None and len(tickers) >= limit:
                break

    return tickers


def load_tickers(csv_path: str | Path, limit: int | None = None) -> list[str]:
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"Arquivo de tickers nao encontrado: {path}")

    tickers: list[str] = []
    seen: set[str] = set()

    if path.suffix.lower() in {".md", ".txt"}:
        tickers = _load_plain_ticker_lines(path, limit)
        if not tickers:
            raise ValueError("Nenhum ticker valido foi carregado da lista.")
        return tickers

    with path.open("r", encoding="utf-8-sig", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        if "Ticker" not in (reader.fieldnames or []):
            raise ValueError("CSV de tickers precisa conter a coluna 'Ticker'.")

        for row in reader:
            _append_unique_ticker(tickers, seen, row["Ticker"])
            if limit is not None and len(tickers) >= limit:
                break

    if not tickers:
        raise ValueError("Nenhum ticker valido foi carregado do CSV.")

    return tickers
