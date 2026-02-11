"""MT5 Data Collector using the MetaTrader5 Python API.

Collects:
    - Historical OHLCV data for configurable FX pairs
    - Supports multiple timeframes (H1, H4, D1)

Architecture:
    - MT5Connector: Low-level connection lifecycle and data fetching
    - MT5Collector: High-level orchestration, §3.1 Bronze layer compliance

Bronze Layer (Raw Data):
    - Preserves all MT5 fields without transformation
    - Adds source="mt5" column
    - Exports to data/raw/mt5/
    - Time field stored as Unix epoch (as returned by MT5)

Preprocessing to Silver layer is handled by PriceNormalizer.

API: https://www.mql5.com/en/docs/python_metatrader5
"""

import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from src.ingestion.collectors.base_collector import BaseCollector
from src.shared.config import Config

# MT5 is optional (Windows-only, requires terminal installation)
try:
    import MetaTrader5 as mt5  # noqa: N813
except ImportError:
    mt5 = None  # type: ignore[assignment]


class MT5Connector:
    """Thin wrapper around the MetaTrader5 Python API.

    Responsibilities:
        - Manage MT5 connection lifecycle (connect / shutdown)
        - Fetch raw OHLC data from the terminal

    Out of scope: data cleaning, feature engineering, indicators, trading.
    """

    # Lazy-initialised because mt5 constants require the module at runtime
    _TIMEFRAMES: dict[str, int] | None = None

    @classmethod
    def _get_timeframes(cls) -> dict[str, int]:
        if mt5 is None:
            raise ImportError(
                "MetaTrader5 module not found. "
                "Install with: pip install MetaTrader5 (Windows only)"
            )
        if cls._TIMEFRAMES is None:
            cls._TIMEFRAMES = {
                "H1": mt5.TIMEFRAME_H1,
                "H4": mt5.TIMEFRAME_H4,
                "D1": mt5.TIMEFRAME_D1,
            }
        return cls._TIMEFRAMES

    @property
    def timeframes(self) -> dict[str, int]:
        return self._get_timeframes()

    def __init__(self, max_retries: int = 3, retry_delay_sec: int = 2) -> None:
        if mt5 is None:
            raise ImportError(
                "MetaTrader5 module not found. "
                "Install with: pip install MetaTrader5 (Windows only)"
            )
        self.connected: bool = False
        self.max_retries = max_retries
        self.retry_delay_sec = retry_delay_sec

    def connect(self) -> None:
        """Initialise the MT5 terminal connection with retries."""
        if self.connected:
            return
        for _ in range(self.max_retries):
            if mt5.initialize():
                self.connected = True
                return
            time.sleep(self.retry_delay_sec)
        raise RuntimeError(f"MT5 initialisation failed after {self.max_retries} attempts")

    def shutdown(self) -> None:
        """Shut down the MT5 terminal connection."""
        if self.connected:
            mt5.shutdown()
            self.connected = False

    def _ensure_symbol(self, symbol: str) -> None:
        if not mt5.symbol_select(symbol, True):
            raise RuntimeError(f"Symbol not available in MT5: {symbol}")

    def fetch_ohlc(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        """Fetch raw OHLC data from MT5 for a date range.

        Args:
            symbol: FX symbol (e.g. "EURUSD").
            timeframe: One of "H1", "H4", "D1".
            start: Range start (UTC).
            end: Range end (UTC).

        Returns:
            Raw DataFrame with MT5 OHLC fields.

        Raises:
            ValueError: If timeframe is not supported.
            RuntimeError: If MT5 returns no data or symbol is unavailable.
        """
        if timeframe not in self.timeframes:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        if not self.connected:
            self.connect()

        self._ensure_symbol(symbol)

        rates = mt5.copy_rates_range(
            symbol,
            self.timeframes[timeframe],
            start,
            end,
        )

        if rates is None or len(rates) == 0:
            raise RuntimeError(f"No data returned for {symbol} ({timeframe})")

        return pd.DataFrame(rates)


class MT5Collector(BaseCollector):
    """Production-grade MT5 data collector following §3.1 Bronze contract.

    Fetches OHLCV data for configurable FX pairs and timeframes.
    Raw CSV output preserves all MT5 fields + source column.

    Bronze Layer Responsibilities:
        - Fetch raw data from MT5 terminal
        - Add source="mt5" column
        - Lowercase snake_case column names
        - Export to data/raw/mt5/
        - NO preprocessing (time kept as Unix epoch)

    Default pairs: EURUSD, GBPUSD, USDJPY, EURGBP, USDCHF
    Default timeframes: H1, H4, D1
    """

    SOURCE_NAME = "mt5"

    DEFAULT_PAIRS = ["EURUSD", "GBPUSD", "USDJPY", "EURGBP", "USDCHF"]
    DEFAULT_TIMEFRAMES = ["H1", "H4", "D1"]
    DEFAULT_YEARS = 3

    # Politeness delay between consecutive symbol fetches (seconds)
    REQUEST_DELAY = 0.5

    def __init__(
        self,
        output_dir: Path | None = None,
        pairs: list[str] | None = None,
        timeframes: list[str] | None = None,
        years: int = DEFAULT_YEARS,
        log_file: Path | None = None,
    ) -> None:
        """Initialise MT5 collector.

        Args:
            output_dir: Directory for raw CSV exports (default: data/raw/mt5/).
            pairs: FX pairs to collect (default: DEFAULT_PAIRS).
            timeframes: Timeframes to collect (default: DEFAULT_TIMEFRAMES).
            years: Number of years of history to fetch (default: 3).
            log_file: Optional path for file-based logging.
        """
        super().__init__(
            output_dir=output_dir or Config.DATA_DIR / "raw" / "mt5",
            log_file=log_file or Config.LOGS_DIR / "collectors" / "mt5_collector.log",
        )
        self.pairs = pairs or self.DEFAULT_PAIRS
        self.timeframes = timeframes or self.DEFAULT_TIMEFRAMES
        self.years = years
        self.connector = MT5Connector()

    # ------------------------------------------------------------------
    # BaseCollector interface
    # ------------------------------------------------------------------

    def collect(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Collect OHLCV data for all configured pairs and timeframes.

        Args:
            start_date: Start of date range (default: `years` ago from now, UTC).
            end_date: End of date range (default: now UTC).

        Returns:
            Mapping of "{pair}_{timeframe}" to DataFrame.

        Raises:
            RuntimeError: If MT5 connection fails or all symbols fail.
        """
        end = end_date or datetime.now(tz=timezone.utc)
        start = start_date or end - timedelta(days=self.years * 365)
        self.logger.info("Collecting MT5 data %s to %s", start.date(), end.date())

        self.connector.connect()
        datasets: dict[str, pd.DataFrame] = {}

        try:
            for pair in self.pairs:
                for timeframe in self.timeframes:
                    key = f"{pair}_{timeframe}"
                    self.logger.info("Fetching %s", key)

                    try:
                        df = self._fetch_and_normalise(pair, timeframe, start, end)
                        datasets[key] = df
                        self.logger.info("Collected %d records for %s", len(df), key)
                    except Exception as exc:
                        self.logger.error("Failed %s: %s", key, exc)
                        continue

                    time.sleep(self.REQUEST_DELAY)
        finally:
            self.connector.shutdown()

        if not datasets:
            raise RuntimeError("No data collected from MT5 (all symbols failed)")

        return datasets

    def health_check(self) -> bool:
        """Verify MT5 terminal is accessible."""
        try:
            self.connector.connect()
            self.connector.shutdown()
            return True
        except Exception as exc:
            self.logger.error("MT5 health check failed: %s", exc)
            return False

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    def _fetch_and_normalise(
        self,
        pair: str,
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        """Fetch raw data and apply §3.1 Bronze normalisation.

        Bronze Layer Contract:
            - All MT5 fields preserved (time, open, high, low, close, tick_volume, spread, real_volume)
            - source column = "mt5"
            - snake_case lowercase column names
            - time field kept as raw Unix epoch (NO conversion)

        Preprocessing to ISO 8601 UTC is handled by PriceNormalizer.

        Args:
            pair: FX symbol (e.g. "EURUSD").
            timeframe: One of "H1", "H4", "D1".
            start: Range start (UTC).
            end: Range end (UTC).

        Returns:
            Raw DataFrame with §3.1 Bronze compliance.
        """
        df = self.connector.fetch_ohlc(pair, timeframe, start, end)

        # §3.1: add source column
        df["source"] = self.SOURCE_NAME

        # §3.1: enforce lowercase snake_case
        df.columns = [col.lower() for col in df.columns]

        # §3.1: preserve raw time field (Unix epoch) - no conversion
        # PriceNormalizer will convert to ISO 8601 UTC for Silver layer

        return df
