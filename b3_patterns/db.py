from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator

from .models import OptionQuoteBar, PriceBar, SpotQuoteBar


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS price_history (
    ticker TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    adj_close REAL NOT NULL,
    volume INTEGER NOT NULL,
    PRIMARY KEY (ticker, trade_date)
);

CREATE TABLE IF NOT EXISTS sync_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_price_history_ticker_date
ON price_history (ticker, trade_date);

CREATE TABLE IF NOT EXISTS spot_quote_history (
    ticker TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume INTEGER NOT NULL,
    trade_count INTEGER NOT NULL,
    PRIMARY KEY (ticker, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_spot_quote_history_ticker_date
ON spot_quote_history (ticker, trade_date);

CREATE TABLE IF NOT EXISTS option_quote_history (
    option_symbol TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    underlying_root TEXT NOT NULL,
    option_side TEXT NOT NULL,
    expiration_date TEXT NOT NULL,
    strike_price REAL NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume INTEGER NOT NULL,
    trade_count INTEGER NOT NULL,
    PRIMARY KEY (option_symbol, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_option_quote_root_date
ON option_quote_history (underlying_root, trade_date);

CREATE INDEX IF NOT EXISTS idx_option_quote_expiration
ON option_quote_history (expiration_date);
"""


@contextmanager
def connect(db_path: str | Path) -> Iterator[sqlite3.Connection]:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA journal_mode=WAL;")
        connection.execute("PRAGMA foreign_keys=ON;")
        connection.row_factory = sqlite3.Row
        yield connection
        connection.commit()
    finally:
        connection.close()


def initialize_database(connection: sqlite3.Connection) -> None:
    connection.executescript(SCHEMA_SQL)


def replace_ticker_history(
    connection: sqlite3.Connection,
    ticker: str,
    rows: list[PriceBar],
) -> None:
    connection.execute("DELETE FROM price_history WHERE ticker = ?", (ticker,))
    connection.executemany(
        """
        INSERT INTO price_history (
            ticker,
            trade_date,
            open,
            high,
            low,
            close,
            adj_close,
            volume
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row.ticker,
                row.trade_date,
                row.open,
                row.high,
                row.low,
                row.close,
                row.adj_close,
                row.volume,
            )
            for row in rows
        ],
    )


def replace_spot_history(
    connection: sqlite3.Connection,
    ticker: str,
    rows: list[SpotQuoteBar],
) -> None:
    connection.execute("DELETE FROM spot_quote_history WHERE ticker = ?", (ticker,))
    connection.executemany(
        """
        INSERT INTO spot_quote_history (
            ticker,
            trade_date,
            open,
            high,
            low,
            close,
            volume,
            trade_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row.ticker,
                row.trade_date,
                row.open,
                row.high,
                row.low,
                row.close,
                row.volume,
                row.trade_count,
            )
            for row in rows
        ],
    )


def replace_option_history(
    connection: sqlite3.Connection,
    underlying_root: str,
    rows: list[OptionQuoteBar],
) -> None:
    connection.execute(
        "DELETE FROM option_quote_history WHERE underlying_root = ?",
        (underlying_root,),
    )
    connection.executemany(
        """
        INSERT INTO option_quote_history (
            option_symbol,
            trade_date,
            underlying_root,
            option_side,
            expiration_date,
            strike_price,
            open,
            high,
            low,
            close,
            volume,
            trade_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row.option_symbol,
                row.trade_date,
                row.underlying_root,
                row.option_side,
                row.expiration_date,
                row.strike_price,
                row.open,
                row.high,
                row.low,
                row.close,
                row.volume,
                row.trade_count,
            )
            for row in rows
        ],
    )


def set_last_sync(connection: sqlite3.Connection, synced_at: datetime) -> None:
    connection.execute(
        """
        INSERT INTO sync_metadata (key, value)
        VALUES ('last_synced_at', ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (synced_at.isoformat(timespec="seconds"),),
    )


def set_metadata_value(
    connection: sqlite3.Connection,
    key: str,
    value: str,
) -> None:
    connection.execute(
        """
        INSERT INTO sync_metadata (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, value),
    )


def get_last_sync(connection: sqlite3.Connection) -> datetime | None:
    row = connection.execute(
        "SELECT value FROM sync_metadata WHERE key = 'last_synced_at'"
    ).fetchone()
    if row is None:
        return None
    return datetime.fromisoformat(row["value"])


def get_metadata_value(connection: sqlite3.Connection, key: str) -> str | None:
    row = connection.execute(
        "SELECT value FROM sync_metadata WHERE key = ?",
        (key,),
    ).fetchone()
    if row is None:
        return None
    return str(row["value"])
