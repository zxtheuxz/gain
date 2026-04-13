from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from shutil import copyfileobj
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zipfile import ZipFile

from .db import (
    connect,
    initialize_database,
    replace_option_history,
    replace_spot_history,
    set_metadata_value,
)
from .models import OptionQuoteBar, SpotQuoteBar
from .tickers import load_tickers


COTAHIST_URL_PATTERNS = (
    "https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_A{year}.ZIP",
    "https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_A{year}.TXT",
)


@dataclass(slots=True)
class CotahistSyncSummary:
    processed_years: list[int]
    spot_tickers: int
    option_roots: int
    database_path: str
    files_used: list[str]


def _parse_decimal(raw_value: str) -> float:
    digits = raw_value.strip()
    if not digits:
        return 0.0
    return int(digits) / 100.0


def _parse_integer(raw_value: str) -> int:
    digits = raw_value.strip()
    if not digits:
        return 0
    return int(digits)


def _parse_date(raw_value: str) -> str:
    digits = raw_value.strip()
    return datetime.strptime(digits, "%Y%m%d").date().isoformat()


def _download_cotahist_year(year: int, destination_dir: str | Path) -> Path:
    target_dir = Path(destination_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    request_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/135.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
    }

    for pattern in COTAHIST_URL_PATTERNS:
        url = pattern.format(year=year)
        destination_path = target_dir / Path(url).name
        try:
            request = Request(url, headers=request_headers)
            with urlopen(request) as response, destination_path.open("wb") as file_obj:
                copyfileobj(response, file_obj)
        except (HTTPError, URLError):
            continue
        return destination_path

    raise RuntimeError(
        f"Nao foi possivel baixar o COTAHIST oficial para {year}. "
        "Use --cotahist-files para informar arquivos locais."
    )


def _yield_txt_lines(cotahist_file: str | Path):
    path = Path(cotahist_file)
    if path.suffix.lower() == ".zip":
        with ZipFile(path) as archive:
            txt_members = [item for item in archive.namelist() if item.lower().endswith(".txt")]
            if not txt_members:
                raise ValueError(f"Arquivo ZIP sem TXT valido: {path}")
            with archive.open(txt_members[0], "r") as file_obj:
                for raw_line in file_obj:
                    yield raw_line.decode("latin-1").rstrip("\r\n")
        return

    with path.open("r", encoding="latin-1") as file_obj:
        for raw_line in file_obj:
            yield raw_line.rstrip("\r\n")


def _extract_root(symbol: str) -> str:
    return symbol[:4].upper()


def _build_root_mapping(tickers: list[str]) -> tuple[set[str], set[str]]:
    normalized_tickers = {ticker.removesuffix(".SA").upper() for ticker in tickers}
    roots = {_extract_root(item) for item in normalized_tickers}
    return normalized_tickers, roots


def _parse_cotahist_files(
    cotahist_files: list[str | Path],
    tickers: list[str],
) -> tuple[dict[str, list[SpotQuoteBar]], dict[str, list[OptionQuoteBar]]]:
    ticker_set, root_set = _build_root_mapping(tickers)
    spot_rows: dict[str, list[SpotQuoteBar]] = defaultdict(list)
    option_rows: dict[str, list[OptionQuoteBar]] = defaultdict(list)

    for cotahist_file in cotahist_files:
        for line in _yield_txt_lines(cotahist_file):
            if len(line) < 245 or line[:2] != "01":
                continue

            trade_date = _parse_date(line[2:10])
            symbol = line[12:24].strip().upper()
            market_type = _parse_integer(line[24:27])

            if market_type == 10 and symbol in ticker_set:
                spot_rows[symbol].append(
                    SpotQuoteBar(
                        ticker=symbol,
                        trade_date=trade_date,
                        open=_parse_decimal(line[56:69]),
                        high=_parse_decimal(line[69:82]),
                        low=_parse_decimal(line[82:95]),
                        close=_parse_decimal(line[108:121]),
                        volume=_parse_integer(line[152:170]),
                        trade_count=_parse_integer(line[147:152]),
                    )
                )
                continue

            if market_type not in {70, 80}:
                continue

            underlying_root = _extract_root(symbol)
            if underlying_root not in root_set:
                continue

            expiration_raw = line[202:210].strip()
            if not expiration_raw:
                continue

            option_rows[underlying_root].append(
                OptionQuoteBar(
                    option_symbol=symbol,
                    underlying_root=underlying_root,
                    option_side="call" if market_type == 70 else "put",
                    trade_date=trade_date,
                    expiration_date=_parse_date(expiration_raw),
                    strike_price=_parse_decimal(line[188:201]),
                    open=_parse_decimal(line[56:69]),
                    high=_parse_decimal(line[69:82]),
                    low=_parse_decimal(line[82:95]),
                    close=_parse_decimal(line[108:121]),
                    volume=_parse_integer(line[152:170]),
                    trade_count=_parse_integer(line[147:152]),
                )
            )

    normalized_spot_rows = {
        ticker: sorted(rows, key=lambda item: item.trade_date)
        for ticker, rows in spot_rows.items()
    }
    normalized_option_rows = {
        root: sorted(rows, key=lambda item: (item.trade_date, item.option_symbol))
        for root, rows in option_rows.items()
    }
    return normalized_spot_rows, normalized_option_rows


def sync_cotahist_history(
    db_path: str | Path,
    tickers_file: str | Path,
    years: list[int] | None = None,
    cotahist_files: list[str | Path] | None = None,
    download_missing: bool = True,
    data_dir: str | Path = "data/cotahist",
) -> CotahistSyncSummary:
    today = date.today()
    effective_years = years or sorted({today.year - 1, today.year})
    files_to_process: list[str | Path] = []

    if cotahist_files:
        files_to_process.extend(cotahist_files)

    if download_missing:
        downloaded_years = set()
        for file_path in files_to_process:
            stem = Path(file_path).stem.upper()
            for year in effective_years:
                if str(year) in stem:
                    downloaded_years.add(year)
        for year in effective_years:
            if year in downloaded_years:
                continue
            files_to_process.append(_download_cotahist_year(year, data_dir))

    if not files_to_process:
        raise ValueError("Nenhum arquivo COTAHIST foi informado ou baixado.")

    tickers = load_tickers(tickers_file)
    spot_rows, option_rows = _parse_cotahist_files(files_to_process, tickers)

    with connect(db_path) as connection:
        initialize_database(connection)
        for ticker, rows in spot_rows.items():
            replace_spot_history(connection, ticker, rows)
        for underlying_root, rows in option_rows.items():
            replace_option_history(connection, underlying_root, rows)
        set_metadata_value(
            connection,
            "cotahist_last_synced_at",
            datetime.now().astimezone().isoformat(timespec="seconds"),
        )
        set_metadata_value(
            connection,
            "cotahist_years",
            ",".join(str(item) for item in effective_years),
        )

    return CotahistSyncSummary(
        processed_years=effective_years,
        spot_tickers=len(spot_rows),
        option_roots=len(option_rows),
        database_path=str(Path(db_path)),
        files_used=[str(Path(item)) for item in files_to_process],
    )
