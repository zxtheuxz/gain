from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .db import connect, get_last_sync, initialize_database, replace_ticker_history, set_last_sync
from .models import PriceBar


@dataclass(slots=True)
class SyncSummary:
    processed_tickers: int
    synced_tickers: int
    failed_tickers: list[str]
    database_path: str


def _optional_import_yfinance():
    try:
        import yfinance as yf  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Dependencia ausente: instale yfinance com `pip install -r requirements.txt`."
        ) from exc
    return yf


def _coerce_trade_date(raw_date) -> str:
    if hasattr(raw_date, "to_pydatetime"):
        raw_date = raw_date.to_pydatetime()
    if hasattr(raw_date, "tzinfo") and raw_date.tzinfo is not None:
        raw_date = raw_date.replace(tzinfo=None)
    if hasattr(raw_date, "date"):
        raw_date = raw_date.date()
    return raw_date.isoformat()


def fetch_ticker_history(
    ticker: str,
    window_days: int,
    lookback_calendar_days: int,
) -> list[PriceBar]:
    yf = _optional_import_yfinance()
    history = yf.Ticker(ticker).history(
        period=f"{lookback_calendar_days}d",
        auto_adjust=False,
        actions=False,
    )

    if history.empty:
        raise ValueError("Yahoo Finance retornou serie vazia.")

    history = history.reset_index()
    rows: list[PriceBar] = []

    for _, price_row in history.tail(window_days).iterrows():
        adj_close = price_row.get("Adj Close")
        if adj_close is None:
            adj_close = price_row["Close"]

        rows.append(
            PriceBar(
                ticker=ticker,
                trade_date=_coerce_trade_date(price_row["Date"]),
                open=float(price_row["Open"]),
                high=float(price_row["High"]),
                low=float(price_row["Low"]),
                close=float(price_row["Close"]),
                adj_close=float(adj_close),
                volume=int(price_row["Volume"]),
            )
        )

    if len(rows) < 3:
        raise ValueError("Serie insuficiente para analise.")

    return rows


def is_sync_stale(db_path: str | Path) -> bool:
    path = Path(db_path)
    if not path.exists():
        return True

    with connect(path) as connection:
        initialize_database(connection)
        last_sync = get_last_sync(connection)

    if last_sync is None:
        return True

    return last_sync.date() < datetime.now().astimezone().date()


def sync_history(
    tickers: list[str],
    db_path: str | Path,
    window_days: int = 90,
    max_workers: int = 8,
) -> SyncSummary:
    if window_days < 3:
        raise ValueError("A janela minima para sincronizacao e 3 dias.")

    lookback_calendar_days = max(window_days * 3, 90)
    failed_tickers: list[str] = []
    downloaded_rows: list[tuple[str, list[PriceBar]]] = []
    worker_count = max(1, min(max_workers, len(tickers)))

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {
            executor.submit(
                fetch_ticker_history,
                ticker=ticker,
                window_days=window_days,
                lookback_calendar_days=lookback_calendar_days,
            ): ticker
            for ticker in tickers
        }
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                rows = future.result()
            except Exception as exc:  # noqa: BLE001
                failed_tickers.append(f"{ticker}: {exc}")
                continue
            downloaded_rows.append((ticker, rows))

    with connect(db_path) as connection:
        initialize_database(connection)
        for ticker, rows in sorted(downloaded_rows, key=lambda item: item[0]):
            replace_ticker_history(connection, ticker, rows)
        if downloaded_rows:
            set_last_sync(connection, datetime.now().astimezone())

    return SyncSummary(
        processed_tickers=len(tickers),
        synced_tickers=len(downloaded_rows),
        failed_tickers=sorted(failed_tickers),
        database_path=str(Path(db_path)),
    )
