"""Price data preprocessor for Bronze → Silver OHLCV transformation.

Transforms raw OHLCV data from multiple sources (MT5, ECB, etc.) to standardized
Silver schema following §3.2.1 contract.

Supported sources:
    - MT5: Full OHLCV bars from MetaTrader5
    - ECB: Daily reference exchange rates (converted to synthetic OHLCV)

Silver Schema (OHLCV):
    - timestamp_utc: ISO 8601 UTC timestamp
    - pair: Currency pair (e.g. "EURUSD")
    - timeframe: Interval (e.g. "H1", "H4", "D1")
    - open, high, low, close: OHLC prices
    - volume: Trading volume
    - source: Data source identifier

Validation:
    - OHLC consistency (high >= open/close/low, low <= open/close/high)
    - No duplicate timestamps per pair/timeframe
    - Valid UTC timestamps
    - No missing critical fields
"""

from datetime import datetime
from pathlib import Path

import pandas as pd

from src.ingestion.preprocessors.base_preprocessor import BasePreprocessor
from src.shared.config import Config


class PriceNormalizer(BasePreprocessor):
    """Preprocessor for OHLCV data following §3.2.1 Silver contract.

    Reads from data/raw/{source}/ and transforms to standardized schema.
    Supports multiple sources: MT5 (full OHLCV), ECB (daily reference rates).
    Outputs to data/processed/ohlcv/ as Parquet files.
    """

    CATEGORY = "ohlcv"

    # Required columns in Silver schema
    REQUIRED_COLUMNS = [
        "timestamp_utc",
        "pair",
        "timeframe",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "source",
    ]

    # Mapping of ECB CURRENCY codes to standard pair notation
    _ECB_CURRENCY_MAP: dict[str, str] = {
        "USD": "EURUSD",
        "GBP": "EURGBP",
        "JPY": "EURJPY",
        "CHF": "EURCHF",
    }

    def __init__(
        self,
        input_dir: Path | None = None,
        output_dir: Path | None = None,
        log_file: Path | None = None,
        sources: list[str] | None = None,
    ) -> None:
        """Initialize the price normalizer.

        Args:
            input_dir: Root directory containing Bronze data (default: data/raw/).
            output_dir: Directory for Silver exports (default: data/processed/ohlcv/).
            log_file: Optional path for file-based logging.
            sources: List of sources to process (default: ["mt5", "ecb"]).
        """
        super().__init__(
            input_dir=input_dir or Config.DATA_DIR / "raw",
            output_dir=output_dir or Config.DATA_DIR / "processed" / "ohlcv",
            log_file=log_file or Config.LOGS_DIR / "preprocessors" / "price_normalizer.log",
        )
        self.sources = sources or ["mt5", "ecb"]

    def preprocess(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Transform Bronze OHLCV data to Silver schema.

        Reads CSV files from input_dir/{source}/, applies transformations:
        - Convert time field to ISO 8601 UTC timestamp
        - Standardize column names
        - Extract pair and timeframe from filename or data
        - Validate OHLC consistency
        - Deduplicate records

        Args:
            start_date: Filter data starting from this date.
            end_date: Filter data up to this date.

        Returns:
            Mapping of "{pair}_{timeframe}" to standardized DataFrame.

        Raises:
            ValueError: If no valid data files found or validation fails.
        """
        if not self.input_dir.exists():
            raise ValueError(f"Input directory does not exist: {self.input_dir}")

        datasets: dict[str, pd.DataFrame] = {}

        # Process each source
        for source in self.sources:
            source_dir = self.input_dir / source
            if not source_dir.exists():
                self.logger.warning("Source directory not found: %s", source_dir)
                continue

            if source == "mt5":
                source_data = self._preprocess_mt5(source_dir, start_date, end_date)
            elif source == "ecb":
                source_data = self._preprocess_ecb(source_dir, start_date, end_date)
            else:
                self.logger.warning("Unknown source: %s", source)
                continue

            datasets.update(source_data)

        if not datasets:
            raise ValueError("No valid datasets processed")

        return datasets

    def _preprocess_mt5(
        self,
        source_dir: Path,
        start_date: datetime | None,
        end_date: datetime | None,
    ) -> dict[str, pd.DataFrame]:
        """Preprocess MT5 Bronze data.

        Args:
            source_dir: Directory containing MT5 CSV files.
            start_date: Filter start date.
            end_date: Filter end date.

        Returns:
            Mapping of "{pair}_{timeframe}" to standardized DataFrame.
        """
        # Find all MT5 CSV files matching pattern: mt5_{PAIR}_{TIMEFRAME}_{YYYYMMDD}.csv
        csv_files = list(source_dir.glob("mt5_*.csv"))
        if not csv_files:
            self.logger.warning("No MT5 CSV files found in %s", source_dir)
            return {}

        self.logger.info("Found %d MT5 Bronze CSV files to preprocess", len(csv_files))
        datasets: dict[str, pd.DataFrame] = {}

        for csv_path in csv_files:
            try:
                # Extract pair and timeframe from filename
                # Format: mt5_{PAIR}_{TIMEFRAME}_{DATE}.csv
                parts = csv_path.stem.split("_")
                if len(parts) < 3:
                    self.logger.warning("Skipping invalid filename: %s", csv_path.name)
                    continue

                pair = parts[1]
                timeframe = parts[2]
                key = f"{pair}_{timeframe}"

                self.logger.info("Processing %s from %s", key, csv_path.name)

                # Read Bronze data
                df = pd.read_csv(csv_path)

                # Transform to Silver schema
                df_silver = self._transform_mt5_to_silver(df, pair, timeframe)

                # Apply date filtering if specified
                if start_date or end_date:
                    df_silver = self._filter_by_date(df_silver, start_date, end_date)

                # Validate
                if not self.validate(df_silver):
                    self.logger.error("Validation failed for %s", key)
                    continue

                datasets[key] = df_silver
                self.logger.info("Processed %d records for %s", len(df_silver), key)

            except Exception as exc:
                self.logger.error("Failed to process %s: %s", csv_path.name, exc)
                continue

        return datasets

    def _preprocess_ecb(
        self,
        source_dir: Path,
        start_date: datetime | None,
        end_date: datetime | None,
    ) -> dict[str, pd.DataFrame]:
        """Preprocess ECB Bronze exchange rate data.

        Args:
            source_dir: Directory containing ECB CSV files.
            start_date: Filter start date.
            end_date: Filter end date.

        Returns:
            Mapping of "{pair}_D1" to standardized DataFrame.
        """
        # Find all ECB exchange rate files
        csv_files = list(source_dir.glob("ecb_exchange_rates_*.csv"))
        if not csv_files:
            self.logger.warning("No ECB exchange rate files found in %s", source_dir)
            return {}

        self.logger.info("Found %d ECB exchange rate files", len(csv_files))

        # Read and concatenate all files
        dfs = []
        for csv_path in csv_files:
            try:
                df = pd.read_csv(csv_path)
                dfs.append(df)
            except Exception as e:
                self.logger.error("Failed to read %s: %s", csv_path, e)
                continue

        if not dfs:
            self.logger.warning("No valid data read from ECB files")
            return {}

        raw = pd.concat(dfs, ignore_index=True)
        self.logger.info("Loaded %d raw ECB exchange rate records", len(raw))

        # Filter by date range if specified
        if start_date or end_date:
            raw["_temp_date"] = pd.to_datetime(raw["TIME_PERIOD"])
            if start_date:
                raw = raw[raw["_temp_date"] >= start_date]
            if end_date:
                raw = raw[raw["_temp_date"] <= end_date]
            raw = raw.drop(columns=["_temp_date"])

        # Transform to OHLCV schema per currency pair
        datasets: dict[str, pd.DataFrame] = {}
        for currency_code, pair in self._ECB_CURRENCY_MAP.items():
            pair_data = raw[raw["CURRENCY"] == currency_code].copy()
            if pair_data.empty:
                self.logger.warning("No data for currency %s", currency_code)
                continue

            ohlcv = self._transform_ecb_to_silver(pair_data, pair)
            if not ohlcv.empty:
                key = f"{pair}_D1"  # ECB provides daily data
                datasets[key] = ohlcv
                self.logger.info("Transformed %d ECB records for %s", len(ohlcv), pair)

        return datasets

    def validate(self, df: pd.DataFrame) -> bool:
        """Validate that DataFrame conforms to Silver OHLCV schema.

        Checks:
        - All required columns present
        - No missing values in critical fields
        - OHLC consistency (high >= open/close/low, low <= open/close/high)
        - No duplicate timestamps per pair/timeframe
        - timestamp_utc is valid datetime

        Args:
            df: DataFrame to validate.

        Returns:
            True if valid.

        Raises:
            ValueError: If validation fails with details.
        """
        if df.empty:
            raise ValueError("DataFrame is empty")

        # Check required columns
        missing_cols = set(self.REQUIRED_COLUMNS) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        # Check for missing values
        critical_fields = ["timestamp_utc", "pair", "timeframe", "open", "high", "low", "close"]
        for field in critical_fields:
            if df[field].isna().any():
                raise ValueError(f"Missing values found in '{field}'")

        # Validate timestamp_utc is datetime (allow both datetime64 and object dtype with datetime values)
        if not pd.api.types.is_datetime64_any_dtype(df["timestamp_utc"]):
            # Try to convert if not already datetime
            try:
                pd.to_datetime(df["timestamp_utc"])
            except Exception:
                raise ValueError("timestamp_utc must be datetime type or convertible to datetime")

        # OHLC consistency
        high_valid = (
            (df["high"] >= df["open"]) & (df["high"] >= df["close"]) & (df["high"] >= df["low"])
        )
        low_valid = (
            (df["low"] <= df["open"]) & (df["low"] <= df["close"]) & (df["low"] <= df["high"])
        )

        if not high_valid.all():
            invalid_count = (~high_valid).sum()
            raise ValueError(
                f"OHLC consistency violation: {invalid_count} records with invalid high prices"
            )

        if not low_valid.all():
            invalid_count = (~low_valid).sum()
            raise ValueError(
                f"OHLC consistency violation: {invalid_count} records with invalid low prices"
            )

        # Check for duplicates
        if "pair" in df.columns and "timeframe" in df.columns:
            duplicates = df.duplicated(subset=["timestamp_utc", "pair", "timeframe"], keep=False)
            if duplicates.any():
                dup_count = duplicates.sum()
                raise ValueError(f"Found {dup_count} duplicate timestamp records")

        self.logger.info("Validation passed: %d records", len(df))
        return True

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    def _transform_mt5_to_silver(
        self,
        df: pd.DataFrame,
        pair: str,
        timeframe: str,
    ) -> pd.DataFrame:
        """Transform Bronze MT5 data to Silver schema.

        Args:
            df: Raw DataFrame from Bronze layer.
            pair: Currency pair (e.g. "EURUSD").
            timeframe: Timeframe identifier (e.g. "H1").

        Returns:
            Standardized DataFrame following §3.2.1 schema.
        """
        df = df.copy()

        # Convert Unix epoch to ISO 8601 UTC timestamp
        if "time" in df.columns:
            df["timestamp_utc"] = pd.to_datetime(df["time"], unit="s", utc=True)
            df.drop(columns=["time"], inplace=True)

        # Add pair and timeframe columns
        df["pair"] = pair
        df["timeframe"] = timeframe

        # Rename MT5 tick_volume to volume (or use real_volume if available)
        if "tick_volume" in df.columns:
            df["volume"] = df["tick_volume"]
            df.drop(columns=["tick_volume"], inplace=True)

        # Drop MT5-specific fields not needed in Silver layer
        columns_to_drop = ["spread", "real_volume"]
        df.drop(columns=[col for col in columns_to_drop if col in df.columns], inplace=True)

        # Ensure all required columns are present
        for col in self.REQUIRED_COLUMNS:
            if col not in df.columns:
                self.logger.warning("Missing column '%s', will be added as NaN", col)
                df[col] = pd.NA

        # Select and order columns according to Silver schema
        df = df[self.REQUIRED_COLUMNS]

        # Deduplicate by timestamp (keep first occurrence)
        df = df.drop_duplicates(subset=["timestamp_utc"], keep="first")

        # Sort by timestamp
        df = df.sort_values("timestamp_utc").reset_index(drop=True)

        return df

    def _transform_ecb_to_silver(
        self,
        df: pd.DataFrame,
        pair: str,
    ) -> pd.DataFrame:
        """Transform ECB exchange rate data to Silver OHLCV schema.

        ECB provides daily reference rates (single price per day).
        We create synthetic OHLCV bars where open=high=low=close=rate.

        Args:
            df: Raw ECB DataFrame filtered for one currency.
            pair: Standard pair notation (e.g., "EURUSD").

        Returns:
            DataFrame conforming to §3.2.1 OHLCV schema.
        """
        df = df.copy()

        # Convert TIME_PERIOD to UTC timestamp
        timestamps = pd.to_datetime(df["TIME_PERIOD"])
        # ECB reference rates are published at 16:00 CET (typically 15:00 UTC)
        timestamps = timestamps + pd.Timedelta(hours=15)

        # Parse rate values
        rates = pd.to_numeric(df["OBS_VALUE"], errors="coerce")

        # Create OHLCV DataFrame (ECB rates: open=high=low=close)
        ohlcv = pd.DataFrame(
            {
                "timestamp_utc": timestamps,
                "pair": pair,
                "timeframe": "D1",  # Daily data
                "open": rates,
                "high": rates,
                "low": rates,
                "close": rates,
                "volume": 0,  # ECB doesn't provide volume
                "source": df["source"],
            }
        )

        # Drop invalid rows
        ohlcv = ohlcv.dropna(subset=["open"])

        # Deduplicate (keep last in case of duplicates)
        ohlcv = ohlcv.drop_duplicates(subset=["timestamp_utc", "pair", "timeframe"], keep="last")

        # Sort by timestamp
        ohlcv = ohlcv.sort_values("timestamp_utc").reset_index(drop=True)

        return ohlcv

    def _filter_by_date(
        self,
        df: pd.DataFrame,
        start_date: datetime | None,
        end_date: datetime | None,
    ) -> pd.DataFrame:
        """Filter DataFrame by date range.

        Args:
            df: DataFrame with timestamp_utc column.
            start_date: Start date (inclusive).
            end_date: End date (inclusive).

        Returns:
            Filtered DataFrame.
        """
        if start_date:
            df = df[df["timestamp_utc"] >= start_date]
        if end_date:
            df = df[df["timestamp_utc"] <= end_date]

        return df
