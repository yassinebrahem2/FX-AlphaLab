"""Dukascopy OHLCV Bronze collector.

Fetches 1-minute BID candles from Dukascopy's public bi5 HTTP feed — no
authentication, no terminal dependency.

Bronze output layout (mirrors GDELT events):
    data/raw/dukascopy/{INSTRUMENT}/{YYYY}/{MM}/{YYYYMMDD}.parquet

Each file holds all 1-min bars for one calendar day (UTC midnight boundary).
Empty files (0 rows) serve as non-trading-day sentinels so subsequent runs
don't re-attempt already-checked dates.

Feed details
------------
URL:  https://datafeed.dukascopy.com/datafeed/{INSTRUMENT}/{YEAR}/{MONTH_0IDX:02d}/{DAY:02d}/BID_candles_min_1.bi5
      Month in URL is 0-indexed (January = "00").
Format: LZMA-compressed binary, 24 bytes/record.
        Per record: ms_since_midnight (uint32 BE), open, high, low, close (uint32 BE ×4),
        volume (float32 BE).  Prices are integer-encoded: divide by point to get decimal.
Point:  1e5 for EUR/GBP/CHF-base pairs; 1e3 for JPY-base pairs.
"""

from __future__ import annotations

import lzma
import struct
import time
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from src.ingestion.collectors.base_collector import BaseCollector

# Price divisors per instrument (integer encoding → decimal)
_POINT: dict[str, float] = {
    "EURUSD": 1e5,
    "GBPUSD": 1e5,
    "USDCHF": 1e5,
    "USDJPY": 1e3,
    "EURGBP": 1e5,
    "USDCAD": 1e5,
    "AUDUSD": 1e5,
    "NZDUSD": 1e5,
    "EURJPY": 1e3,
    "GBPJPY": 1e3,
}

_DUKA_BASE = "https://datafeed.dukascopy.com/datafeed"
_RECORD_FMT = ">IIIIIf"  # big-endian: 5×uint32 + 1×float32
_RECORD_SIZE = struct.calcsize(_RECORD_FMT)  # 24 bytes

_BRONZE_SCHEMA = pa.schema(
    [
        pa.field("timestamp_utc", pa.timestamp("ms", tz="UTC")),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("volume", pa.float32()),
    ]
)


class DukascopyCollector(BaseCollector):
    """Download 1-min BID candles for configured FX instruments.

    Supports mid-day resume: only missing Bronze files are fetched.  Pass
    backfill=True to force re-download of all dates in the requested range
    (useful after a failed partial run or to refresh today's intraday bars).
    """

    SOURCE_NAME = "dukascopy"
    DEFAULT_LOOKBACK_DAYS = 365 * 4

    DEFAULT_INSTRUMENTS: list[str] = ["EURUSD", "GBPUSD", "USDJPY", "USDCHF"]

    _REQUEST_DELAY = 0.1  # seconds between requests (politeness)
    _MAX_RETRIES = 3
    _RETRY_BASE = 1.0  # seconds; doubles each attempt

    def __init__(
        self,
        output_dir: Path | None = None,
        instruments: list[str] | None = None,
        log_file: Path | None = None,
    ) -> None:
        from src.shared.config import Config

        super().__init__(
            output_dir=output_dir or Config.DATA_DIR / "raw" / "dukascopy",
            log_file=log_file or Config.LOGS_DIR / "collectors" / "dukascopy_collector.log",
        )
        self.instruments = instruments or list(self.DEFAULT_INSTRUMENTS)
        self._session = requests.Session()
        self._session.headers["User-Agent"] = "Mozilla/5.0 (compatible; FX-AlphaLab/1.0)"

    # ── BaseCollector interface ────────────────────────────────────────────────

    def collect(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        backfill: bool = False,
    ) -> dict[str, int]:
        """Download missing Bronze files for all configured instruments.

        Args:
            start_date: First calendar date to collect (default: DEFAULT_LOOKBACK_DAYS ago).
            end_date: Last calendar date to collect (default: yesterday UTC).
            backfill: When True, re-download every date regardless of existing files.

        Returns:
            Mapping of instrument → total 1-min rows written in this run.
        """
        today = datetime.now(timezone.utc).date()

        if end_date is None:
            # Default: yesterday — today's D1 bar isn't closed yet
            end_day = today - pd.Timedelta(days=1).to_pytimedelta()
        else:
            end_day = self._to_utc(end_date).date()

        if start_date is None:
            start_day = end_day - pd.Timedelta(days=self.DEFAULT_LOOKBACK_DAYS).to_pytimedelta()
        else:
            start_day = self._to_utc(start_date).date()

        results: dict[str, int] = {}
        for instrument in self.instruments:
            count = self._collect_instrument(instrument, start_day, end_day, backfill)
            results[instrument] = count
            self.logger.info("%s: %d rows written", instrument, count)

        return results

    def health_check(self) -> bool:
        """Verify the Dukascopy feed is reachable with a known historical date."""
        url = f"{_DUKA_BASE}/EURUSD/2024/00/02/BID_candles_min_1.bi5"
        try:
            r = self._session.get(url, timeout=10)
            return r.status_code == 200 and len(r.content) > 0
        except requests.RequestException:
            return False

    # ── Private helpers ────────────────────────────────────────────────────────

    def _collect_instrument(self, instrument: str, start: date, end: date, backfill: bool) -> int:
        """Iterate calendar days, skip Saturdays and already-downloaded files."""
        total_rows = 0
        current = start
        one_day = pd.Timedelta(days=1).to_pytimedelta()

        while current <= end:
            if current.weekday() == 5:  # Saturday — no FX trading
                current += one_day
                continue

            path = self._bronze_path(instrument, current)

            if path.exists() and not backfill:
                current += one_day
                continue

            df = self._fetch_day(instrument, current)
            self._write_bronze(df, path)
            total_rows += len(df)

            time.sleep(self._REQUEST_DELAY)
            current += one_day

        return total_rows

    def _fetch_day(self, instrument: str, d: date) -> pd.DataFrame:
        """Fetch and decode one calendar day.  Returns empty DataFrame for non-trading days."""
        # Dukascopy URL month is 0-indexed
        url = (
            f"{_DUKA_BASE}/{instrument}/{d.year}"
            f"/{d.month - 1:02d}/{d.day:02d}/BID_candles_min_1.bi5"
        )

        for attempt in range(self._MAX_RETRIES):
            try:
                r = self._session.get(url, timeout=15)
                if r.status_code == 404:
                    return pd.DataFrame(columns=list(_BRONZE_SCHEMA.names))
                r.raise_for_status()
                return self._decode_bi5(r.content, instrument, d)

            except requests.HTTPError:
                if attempt == self._MAX_RETRIES - 1:
                    self.logger.warning(
                        "%s %s: HTTP error after %d retries", instrument, d, self._MAX_RETRIES
                    )
                    return pd.DataFrame(columns=list(_BRONZE_SCHEMA.names))
                time.sleep(self._RETRY_BASE * (2**attempt))

            except (requests.RequestException, lzma.LZMAError, struct.error) as exc:
                if attempt == self._MAX_RETRIES - 1:
                    self.logger.error("%s %s: %r", instrument, d, exc)
                    return pd.DataFrame(columns=list(_BRONZE_SCHEMA.names))
                time.sleep(self._RETRY_BASE * (2**attempt))

        return pd.DataFrame(columns=list(_BRONZE_SCHEMA.names))

    def _decode_bi5(self, raw_bytes: bytes, instrument: str, d: date) -> pd.DataFrame:
        """Decompress LZMA and unpack binary records into a 1-min OHLCV DataFrame."""
        decompressed = lzma.decompress(raw_bytes)
        n = len(decompressed) // _RECORD_SIZE
        if n == 0:
            return pd.DataFrame(columns=list(_BRONZE_SCHEMA.names))

        point = _POINT.get(instrument, 1e5)
        base_ts = pd.Timestamp(year=d.year, month=d.month, day=d.day, tz="UTC")

        timestamps: list[pd.Timestamp] = []
        opens: list[float] = []
        highs: list[float] = []
        lows: list[float] = []
        closes: list[float] = []
        volumes: list[float] = []

        for i in range(n):
            offset = i * _RECORD_SIZE
            ms, open_value, high_value, low_value, close_value, vol = struct.unpack_from(
                _RECORD_FMT, decompressed, offset
            )
            timestamps.append(base_ts + pd.Timedelta(milliseconds=int(ms)))
            opens.append(round(open_value / point, 6))
            highs.append(round(high_value / point, 6))
            lows.append(round(low_value / point, 6))
            closes.append(round(close_value / point, 6))
            volumes.append(float(vol))

        return pd.DataFrame(
            {
                "timestamp_utc": timestamps,
                "open": opens,
                "high": highs,
                "low": lows,
                "close": closes,
                "volume": volumes,
            }
        )

    def _bronze_path(self, instrument: str, d: date) -> Path:
        return (
            self.output_dir
            / instrument
            / str(d.year)
            / f"{d.month:02d}"
            / f"{d.strftime('%Y%m%d')}.parquet"
        )

    def _write_bronze(self, df: pd.DataFrame, path: Path) -> None:
        """Write Bronze Parquet atomically (temp file → rename)."""
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        try:
            if df.empty:
                # Write schema-only sentinel so this date is skipped on future runs
                table = pa.table(
                    {
                        name: pa.array([], type=field.type)
                        for name, field in zip(_BRONZE_SCHEMA.names, _BRONZE_SCHEMA)
                    },
                    schema=_BRONZE_SCHEMA,
                )
            else:
                table = pa.Table.from_pandas(df, schema=_BRONZE_SCHEMA, preserve_index=False)

            pq.write_table(
                table,
                str(tmp),
                compression="zstd",
                compression_level=12,
                use_dictionary=False,
                write_statistics=False,
            )
            tmp.rename(path)
        except Exception:
            tmp.unlink(missing_ok=True)
            raise

        self.logger.debug("Written %s (%d rows)", path.name, len(df))
