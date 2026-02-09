"""FRED Data Collector using the official FRED API.

Collects macroeconomic indicators and financial stress indices:
    - STLFSI4: St. Louis Fed Financial Stress Index
    - DFF: Federal Funds Effective Rate
    - CPIAUCSL: Consumer Price Index for All Urban Consumers
    - UNRATE: Unemployment Rate

Implements caching to avoid redundant API calls and handles rate limits (120 calls/min).

API Documentation: https://fred.stlouisfed.org/docs/api/
Get API Key: https://fred.stlouisfed.org/docs/api/api_key.html

Example:
    >>> from pathlib import Path
    >>> from datetime import datetime
    >>> from data.ingestion.fred_collector import FREDCollector
    >>>
    >>> collector = FREDCollector()
    >>> # Get single series
    >>> df = collector.get_series("DFF", start_date=datetime(2023, 1, 1))
    >>> # Get multiple series
    >>> data = collector.collect(start_date=datetime(2023, 1, 1))
    >>> # Access individual DataFrames
    >>> stress_index = data["financial_stress"]
    >>> fed_funds = data["federal_funds_rate"]
"""

import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from fredapi import Fred

from data.ingestion.base_collector import BaseCollector
from shared.config import Config


@dataclass(frozen=True)
class FREDSeries:
    """Immutable descriptor for a FRED data series."""

    series_id: str
    name: str
    description: str
    frequency: str  # D=Daily, M=Monthly, W=Weekly, etc.
    units: str


class FREDCollector(BaseCollector):
    """Collector for FRED macroeconomic indicators.

    Uses the official FRED API via fredapi package.
    Normalized output stored in datasets/macro/ following §3.4.

    Features:
        - Automatic rate limiting (120 calls/min)
        - Local JSON caching to avoid redundant API calls
        - Batch retrieval of multiple series
        - Comprehensive error handling
        - UTC ISO 8601 timestamp normalization

    Rate Limits:
        FRED API allows 120 requests per minute. This collector automatically
        throttles requests to stay within limits.
    """

    SOURCE_NAME = "fred"

    # Rate limiting: 120 calls/min = 1 call every 0.5 seconds
    MIN_REQUEST_INTERVAL = 0.5  # seconds
    CACHE_EXPIRY_DAYS = 1  # cache data for 1 day

    # Predefined series
    FINANCIAL_STRESS = FREDSeries(
        series_id="STLFSI4",
        name="financial_stress",
        description="St. Louis Fed Financial Stress Index",
        frequency="W",
        units="Index",
    )

    FEDERAL_FUNDS_RATE = FREDSeries(
        series_id="DFF",
        name="federal_funds_rate",
        description="Federal Funds Effective Rate",
        frequency="D",
        units="Percent",
    )

    CPI = FREDSeries(
        series_id="CPIAUCSL",
        name="cpi",
        description="Consumer Price Index for All Urban Consumers: All Items",
        frequency="M",
        units="Index 1982-1984=100",
    )

    UNEMPLOYMENT_RATE = FREDSeries(
        series_id="UNRATE",
        name="unemployment_rate",
        description="Unemployment Rate",
        frequency="M",
        units="Percent",
    )

    _ALL_SERIES: tuple[FREDSeries, ...] = (
        FINANCIAL_STRESS,
        FEDERAL_FUNDS_RATE,
        CPI,
        UNEMPLOYMENT_RATE,
    )

    def __init__(
        self,
        api_key: str | None = None,
        output_dir: Path | None = None,
        cache_dir: Path | None = None,
        log_file: Path | None = None,
    ) -> None:
        """Initialize the FRED collector.

        Args:
            api_key: FRED API key (defaults to Config.FRED_API_KEY).
            output_dir: Directory for CSV exports (defaults to datasets/fred/raw).
            cache_dir: Directory for JSON cache (defaults to datasets/fred/cache).
            log_file: Optional path for file-based logging.

        Raises:
            ValueError: If api_key is not provided and not in Config.
        """
        super().__init__(
            output_dir=output_dir or Config.DATA_DIR / "datasets" / "macro",
            log_file=log_file or Config.LOGS_DIR / "fred_collector.log",
        )

        self._api_key = api_key or Config.FRED_API_KEY
        if not self._api_key or self._api_key == "your_fred_api_key_here":
            raise ValueError(
                "FRED API key is required. Set FRED_API_KEY in .env or pass api_key parameter. "
                "Get a free key at https://fred.stlouisfed.org/docs/api/api_key.html"
            )

        self._fred = Fred(api_key=self._api_key)
        self._cache_dir = cache_dir or Config.DATA_DIR / "cache" / "fred"
        self._cache_dir.mkdir(parents=True, exist_ok=True)

        self._last_request_time: float = 0.0

        self.logger.info(
            "FREDCollector initialized, output_dir=%s, cache_dir=%s",
            self.output_dir,
            self._cache_dir,
        )

    # ------------------------------------------------------------------
    # BaseCollector interface
    # ------------------------------------------------------------------

    def collect(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Collect all predefined FRED series.

        Args:
            start_date: Start of range (default: 2 years ago).
            end_date: End of range (default: today).

        Returns:
            Dictionary mapping series name to DataFrame, e.g.:
            {
                "financial_stress": DataFrame[timestamp_utc, series_id, value, source, frequency, units],
                "federal_funds_rate": DataFrame[...],
                "cpi": DataFrame[...],
                "unemployment_rate": DataFrame[...]
            }

        Raises:
            ValueError: If start_date is after end_date.

        Example:
            >>> collector = FREDCollector()
            >>> data = collector.collect(start_date=datetime(2023, 1, 1))
            >>> fed_funds = data["federal_funds_rate"]
            >>> print(fed_funds.head())
        """
        start = start_date or datetime.now() - timedelta(days=730)
        end = end_date or datetime.now()

        if start > end:
            raise ValueError(f"start_date ({start.date()}) must be before end_date ({end.date()})")

        self.logger.info("Collecting FRED data from %s to %s", start.date(), end.date())

        result = {}
        for series in self._ALL_SERIES:
            df = self.get_series(series.series_id, start_date=start, end_date=end)
            if not df.empty:
                result[series.name] = df
                self.logger.info("Collected %s: %d observations", series.series_id, len(df))
            else:
                self.logger.warning("No data for %s", series.series_id)

        self.logger.info("Done — collected %d series", len(result))
        return result

    def health_check(self) -> bool:
        """Verify FRED API is reachable.

        Returns:
            True if API responds successfully, False otherwise.

        Example:
            >>> collector = FREDCollector()
            >>> if collector.health_check():
            ...     print("FRED API is available")
        """
        try:
            # Try fetching info for a well-known series
            self._fred.get_series_info("DFF")
            return True
        except Exception as e:
            self.logger.error("FRED health check failed: %s", e)
            return False

    # ------------------------------------------------------------------
    # FRED-specific collection methods
    # ------------------------------------------------------------------

    def get_series(
        self,
        series_id: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        use_cache: bool = True,
    ) -> pd.DataFrame:
        """Get a single FRED series.

        Args:
            series_id: FRED series identifier (e.g., "DFF", "UNRATE").
            start_date: Start date for data retrieval.
            end_date: End date for data retrieval.
            use_cache: If True, check cache before making API call.

        Returns:
            DataFrame with columns: [timestamp_utc, series_id, value, source, frequency, units]

        Raises:
            ValueError: If series_id is invalid or empty, or date range is invalid.
            Exception: If API request fails after retries.

        Example:
            >>> collector = FREDCollector()
            >>> df = collector.get_series("DFF", start_date=datetime(2023, 1, 1))
            >>> print(df.columns)
            Index(['timestamp_utc', 'series_id', 'value', 'source', 'frequency', 'units'])
        """
        if not series_id or not series_id.strip():
            raise ValueError("series_id cannot be empty")

        start = start_date or datetime.now() - timedelta(days=730)
        end = end_date or datetime.now()

        if start > end:
            raise ValueError(f"start_date ({start.date()}) must be before end_date ({end.date()})")

        # Check cache first
        if use_cache:
            cached_df = self._load_from_cache(series_id, start, end)
            if cached_df is not None:
                self.logger.info("Loaded %s from cache", series_id)
                return cached_df

        # Respect rate limits
        self._throttle_request()

        try:
            # Get series info and data
            info = self._fred.get_series_info(series_id)
            series_data = self._fred.get_series(
                series_id,
                observation_start=start.strftime("%Y-%m-%d"),
                observation_end=end.strftime("%Y-%m-%d"),
            )

            if series_data.empty:
                self.logger.warning("No data returned for series %s", series_id)
                return pd.DataFrame()

            # Convert to §3.4 normalized format with UTC timestamps
            df = pd.DataFrame(
                {
                    "timestamp_utc": pd.to_datetime(series_data.index).strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ),
                    "series_id": series_id,
                    "value": series_data.values,
                    "source": self.SOURCE_NAME,
                    "frequency": info.get("frequency_short", "N/A"),
                    "units": info.get("units", "N/A"),
                }
            )

            # Cache the result
            self._save_to_cache(df, series_id, start, end)

            return df

        except ValueError as e:
            self.logger.error("Invalid series ID '%s': %s", series_id, e)
            raise ValueError(f"Invalid FRED series ID '{series_id}'") from e
        except Exception as e:
            self.logger.error("Failed to fetch series '%s': %s", series_id, e)
            raise

    def get_multiple_series(
        self,
        series_ids: list[str],
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        use_cache: bool = True,
    ) -> dict[str, pd.DataFrame]:
        """Get multiple FRED series in batch.

        Args:
            series_ids: List of FRED series identifiers.
            start_date: Start date for data retrieval.
            end_date: End date for data retrieval.
            use_cache: If True, check cache before making API calls.

        Returns:
            Dictionary mapping series_id to DataFrame.

        Raises:
            ValueError: If series_ids is empty or contains invalid entries.

        Example:
            >>> collector = FREDCollector()
            >>> series = ["DFF", "UNRATE", "CPIAUCSL"]
            >>> data = collector.get_multiple_series(series, start_date=datetime(2023, 1, 1))
            >>> fed_funds = data["DFF"]
            >>> unemployment = data["UNRATE"]
        """
        if not series_ids:
            raise ValueError("series_ids list cannot be empty")

        start = start_date or datetime.now() - timedelta(days=730)
        end = end_date or datetime.now()

        if start > end:
            raise ValueError(f"start_date ({start.date()}) must be before end_date ({end.date()})")

        self.logger.info(
            "Fetching %d series from %s to %s", len(series_ids), start.date(), end.date()
        )

        result = {}
        for series_id in series_ids:
            try:
                df = self.get_series(series_id, start, end, use_cache)
                if not df.empty:
                    result[series_id] = df
            except Exception as e:
                self.logger.error("Failed to fetch %s: %s", series_id, e)
                # Continue with other series

        self.logger.info("Successfully fetched %d/%d series", len(result), len(series_ids))
        return result

    def export_all_to_csv(
        self,
        data: dict[str, pd.DataFrame] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, Path]:
        """Collect all series and export each to CSV in §3.4 normalized format.

        Args:
            data: Pre-collected data dict (if None, will collect).
            start_date: Start date for collection if data is None.
            end_date: End date for collection if data is None.

        Returns:
            Dictionary mapping series_id to exported file path.

        Example:
            >>> collector = FREDCollector()
            >>> paths = collector.export_all_to_csv(start_date=datetime(2023, 1, 1))
            >>> print(paths["DFF"])
            .../fred_DFF_2023-01-01_2023-12-31.csv
        """
        if data is None:
            start = start_date or datetime.now() - timedelta(days=730)
            end = end_date or datetime.now()
            data = self.collect(start_date=start, end_date=end)
        else:
            # Extract date range from data if not provided
            start = start_date or datetime.now() - timedelta(days=730)
            end = end_date or datetime.now()

        start_str = start.strftime("%Y-%m-%d")
        end_str = end.strftime("%Y-%m-%d")

        paths = {}
        # Map series names back to series IDs
        name_to_id = {
            "financial_stress": "STLFSI4",
            "federal_funds_rate": "DFF",
            "cpi": "CPIAUCSL",
            "unemployment_rate": "UNRATE",
        }

        for name, df in data.items():
            try:
                series_id = name_to_id.get(name, name.upper())
                # Use §3.4 naming: {source}_{series_id}_{start}_{end}.csv
                filename = f"{self.SOURCE_NAME}_{series_id}_{start_str}_{end_str}.csv"
                path = self.output_dir / filename
                df.to_csv(path, index=False, encoding="utf-8")
                paths[series_id] = path
                self.logger.info("Exported %s (%d records) to %s", series_id, len(df), path)
            except Exception as e:
                self.logger.error("Failed to export %s: %s", name, e)

        return paths

    def export_consolidated_csv(
        self,
        data: dict[str, pd.DataFrame] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        filename: str = "fred_indicators",
    ) -> Path:
        """Export all series to a single consolidated CSV file.

        This method provides backward compatibility with the original requirement
        of exporting to a single 'fred_indicators.csv' file. All series are
        combined into one DataFrame with proper alignment by date.

        Args:
            data: Pre-collected data dict (if None, will collect).
            start_date: Start date for collection if data is None.
            end_date: End date for collection if data is None.
            filename: Output filename without extension (default: "fred_indicators").

        Returns:
            Path to the exported consolidated CSV file.

        Example:
            >>> collector = FREDCollector()
            >>> path = collector.export_consolidated_csv(start_date=datetime(2023, 1, 1))
            >>> print(path)
            .../fred_indicators_20260209.csv
        """
        if data is None:
            data = self.collect(start_date, end_date)

        if not data:
            raise ValueError("No data to export")

        # Combine all series into a single DataFrame
        combined_dfs = []
        for series_name, df in data.items():
            if df.empty:
                continue
            # Add series name as a column and keep relevant columns
            df_copy = df.copy()
            df_copy["series_name"] = series_name
            combined_dfs.append(df_copy)

        if not combined_dfs:
            raise ValueError("All dataframes are empty, nothing to export")

        # Concatenate all series
        consolidated = pd.concat(combined_dfs, ignore_index=True)

        # Sort by timestamp_utc and series_id for consistent output
        consolidated = consolidated.sort_values(["timestamp_utc", "series_id"]).reset_index(
            drop=True
        )

        # Export using standard naming convention
        date_str = datetime.now().strftime("%Y%m%d")
        path = self.output_dir / f"{filename}_{date_str}.csv"
        consolidated.to_csv(path, index=False, encoding="utf-8")
        self.logger.info("Exported consolidated %d records to %s", len(consolidated), path)
        return path

    # ------------------------------------------------------------------
    # Caching
    # ------------------------------------------------------------------

    def _get_cache_path(self, series_id: str) -> Path:
        """Generate cache file path for a series."""
        return self._cache_dir / f"{series_id}.json"

    def _load_from_cache(
        self,
        series_id: str,
        start_date: datetime,
        end_date: datetime,
    ) -> pd.DataFrame | None:
        """Load series data from cache if valid.

        Args:
            series_id: FRED series identifier.
            start_date: Requested start date.
            end_date: Requested end date.

        Returns:
            Cached DataFrame if valid and covers requested range, None otherwise.
        """
        cache_path = self._get_cache_path(series_id)
        if not cache_path.exists():
            return None

        try:
            # Check cache age
            cache_age = datetime.now().timestamp() - cache_path.stat().st_mtime
            if cache_age > (self.CACHE_EXPIRY_DAYS * 86400):
                self.logger.debug("Cache expired for %s", series_id)
                return None

            # Load cache
            with open(cache_path, encoding="utf-8") as f:
                cache_data = json.load(f)

            # Verify cache covers requested date range
            cache_start = datetime.fromisoformat(cache_data["start_date"])
            cache_end = datetime.fromisoformat(cache_data["end_date"])

            if cache_start <= start_date and cache_end >= end_date:
                df = pd.DataFrame(cache_data["data"])
                # Ensure timestamp_utc column exists (backward compatibility)
                if "date" in df.columns and "timestamp_utc" not in df.columns:
                    df["timestamp_utc"] = pd.to_datetime(df["date"]).dt.strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    )
                    df = df.drop(columns=["date"])
                # Filter to requested range (convert to timezone-naive for comparison)
                timestamp_col = pd.to_datetime(df["timestamp_utc"]).dt.tz_localize(None)
                df_filtered = df[(timestamp_col >= start_date) & (timestamp_col <= end_date)]
                return df_filtered

            return None

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            self.logger.warning("Invalid cache for %s: %s", series_id, e)
            return None

    def _save_to_cache(
        self,
        df: pd.DataFrame,
        series_id: str,
        start_date: datetime,
        end_date: datetime,
    ) -> None:
        """Save series data to cache.

        Args:
            df: DataFrame to cache.
            series_id: FRED series identifier.
            start_date: Start date of cached data.
            end_date: End date of cached data.
        """
        cache_path = self._get_cache_path(series_id)

        try:
            cache_data = {
                "series_id": series_id,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "cached_at": datetime.now().isoformat(),
                "data": df.to_dict(orient="records"),
            }

            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(cache_data, f, indent=2)

            self.logger.debug("Cached %s (%d records)", series_id, len(df))

        except Exception as e:
            self.logger.warning("Failed to cache %s: %s", series_id, e)

    def clear_cache(self, series_id: str | None = None) -> None:
        """Clear cache for a specific series or all series.

        Args:
            series_id: If provided, clear cache for this series only.
                      If None, clear all cache.

        Example:
            >>> collector = FREDCollector()
            >>> collector.clear_cache("DFF")  # Clear single series
            >>> collector.clear_cache()  # Clear all cache
        """
        if series_id:
            cache_path = self._get_cache_path(series_id)
            if cache_path.exists():
                cache_path.unlink()
                self.logger.info("Cleared cache for %s", series_id)
        else:
            for cache_file in self._cache_dir.glob("*.json"):
                cache_file.unlink()
            self.logger.info("Cleared all cache")

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    def _throttle_request(self) -> None:
        """Ensure minimum interval between API requests to respect rate limits."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.MIN_REQUEST_INTERVAL:
            sleep_time = self.MIN_REQUEST_INTERVAL - elapsed
            time.sleep(sleep_time)
        self._last_request_time = time.time()
