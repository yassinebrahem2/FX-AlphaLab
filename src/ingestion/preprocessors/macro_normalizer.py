"""Macro Data Normalizer - Bronze to Silver transformation.

Transforms raw macro data from data/raw/{source}/ to Silver schema (§3.2.2):
- Converts date to timestamp_utc (ISO 8601)
- Standardizes columns: [timestamp_utc, series_id, value, source, frequency, units]
- Validates data types and removes duplicates
- Exports to data/processed/macro/

Supported sources:
    - FRED: US economic indicators (fed funds rate, CPI, unemployment, etc.)
    - ECB: European Central Bank policy rates (deposit facility, main refinancing)

Silver Schema (§3.2.2):
    timestamp_utc: ISO 8601 UTC timestamp (YYYY-MM-DDTHH:MM:SSZ)
    series_id: Series identifier (e.g., "DFF", "UNRATE", "ECB_DFR", "ECB_MRR_FR")
    value: Numeric observation value
    source: Data source ("fred" or "ecb")
    frequency: Observation frequency (D=Daily, M=Monthly, W=Weekly, B=Business, etc.)
    units: Measurement units (e.g., "Percent", "Index")

Example:
    >>> from datetime import datetime
    >>> from pathlib import Path
    >>> from src.ingestion.preprocessors.macro_normalizer import MacroNormalizer
    >>>
    >>> normalizer = MacroNormalizer(
    ...     input_dir=Path("data/raw"),  # Root raw directory
    ...     output_dir=Path("data/processed/macro"),
    ...     sources=["fred", "ecb"]
    ... )
    >>> # Process all Bronze files from all sources
    >>> data = normalizer.preprocess()
    >>> # Export to Silver
    >>> for series_id, df in data.items():
    ...     normalizer.export(df, series_id, start_date, end_date)
"""

from datetime import datetime
from pathlib import Path

import pandas as pd

from src.ingestion.preprocessors.base_preprocessor import BasePreprocessor


class MacroNormalizer(BasePreprocessor):
    """Preprocessor for macro data (Bronze → Silver).

    Reads raw macro data from data/raw/{source}/ and transforms to Silver schema.
    Supports FRED and ECB data sources.
    Output stored in data/processed/macro/ following §3.2.2.

    Features:
        - UTC timestamp normalization
        - Type validation and coercion
        - Duplicate removal
        - Missing value handling
        - Schema enforcement
        - Multi-source support
    """

    CATEGORY = "macro"

    # Silver schema columns (§3.2.2)
    SILVER_COLUMNS = [
        "timestamp_utc",
        "series_id",
        "value",
        "source",
        "frequency",
        "units",
    ]

    # Required columns in Bronze data (FRED format)
    BRONZE_COLUMNS = ["date", "series_id", "value", "source", "frequency", "units"]

    # Mapping of ECB rate codes to human-readable series IDs
    _ECB_RATE_MAP: dict[str, tuple[str, str]] = {
        "DFR": ("ECB_DFR", "Deposit Facility Rate"),
        "MRR_FR": ("ECB_MRR", "Main Refinancing Operations Rate"),
        "MRR_MBR": ("ECB_MLF", "Marginal Lending Facility Rate"),
    }

    def __init__(
        self,
        input_dir: Path | None = None,
        output_dir: Path | None = None,
        log_file: Path | None = None,
        sources: list[str] | None = None,
    ) -> None:
        """Initialize the macro normalizer.

        Args:
            input_dir: Root directory containing Bronze data (default: data/raw/).
            output_dir: Directory for Silver exports (default: data/processed/macro/).
            log_file: Optional path for file-based logging.
            sources: List of sources to process (default: ["fred", "ecb"]).
        """
        from src.shared.config import Config

        super().__init__(
            input_dir=input_dir or Config.DATA_DIR / "raw",
            output_dir=output_dir or Config.DATA_DIR / "processed" / "macro",
            log_file=log_file or Config.LOGS_DIR / "macro_normalizer.log",
        )
        self.sources = sources or ["fred", "ecb"]

    def preprocess(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Transform Bronze macro data to Silver schema.

        Reads all CSV files from input_dir/{source}/, applies:
        - Date → timestamp_utc conversion
        - Type validation (value must be numeric)
        - Deduplication by (timestamp_utc, series_id)
        - Filtering by date range

        Args:
            start_date: Optional start date filter.
            end_date: Optional end date filter.

        Returns:
            Dictionary mapping series_id to Silver DataFrame.

        Example:
            >>> normalizer = MacroNormalizer(input_dir, output_dir)
            >>> data = normalizer.preprocess(
            ...     start_date=datetime(2023, 1, 1),
            ...     end_date=datetime(2023, 12, 31)
            ... )
            >>> dff_df = data["DFF"]
            >>> ecb_dfr_df = data["ECB_DFR"]
        """
        self.logger.info("Starting macro preprocessing from %s", self.input_dir)

        result = {}

        # Process each source
        for source in self.sources:
            source_dir = self.input_dir / source
            if not source_dir.exists():
                self.logger.warning("Source directory not found: %s", source_dir)
                continue

            if source == "fred":
                source_data = self._preprocess_fred(source_dir, start_date, end_date)
            elif source == "ecb":
                source_data = self._preprocess_ecb(source_dir, start_date, end_date)
            else:
                self.logger.warning("Unknown source: %s", source)
                continue

            result.update(source_data)

        self.logger.info("Preprocessing complete: %d series processed", len(result))
        return result

    def _preprocess_fred(
        self,
        source_dir: Path,
        start_date: datetime | None,
        end_date: datetime | None,
    ) -> dict[str, pd.DataFrame]:
        """Preprocess FRED Bronze data.

        Args:
            source_dir: Directory containing FRED CSV files.
            start_date: Optional start date filter.
            end_date: Optional end date filter.

        Returns:
            Dictionary mapping series_id to Silver DataFrame.
        """
        # Find all FRED Bronze files
        bronze_files = list(source_dir.glob("fred_*.csv"))
        if not bronze_files:
            self.logger.warning("No FRED Bronze files found in %s", source_dir)
            return {}

        self.logger.info("Found %d FRED Bronze files to process", len(bronze_files))

        result = {}
        for bronze_file in bronze_files:
            try:
                df = self._process_fred_file(bronze_file, start_date, end_date)
                if not df.empty:
                    # Extract series_id from first row
                    series_id = df["series_id"].iloc[0]
                    result[series_id] = df
                    self.logger.info(
                        "Processed %s: %d records → series %s",
                        bronze_file.name,
                        len(df),
                        series_id,
                    )
            except Exception as e:
                self.logger.error("Failed to process %s: %s", bronze_file.name, e)

        return result

    def _preprocess_ecb(
        self,
        source_dir: Path,
        start_date: datetime | None,
        end_date: datetime | None,
    ) -> dict[str, pd.DataFrame]:
        """Preprocess ECB Bronze policy rate data.

        Args:
            source_dir: Directory containing ECB CSV files.
            start_date: Optional start date filter.
            end_date: Optional end date filter.

        Returns:
            Dictionary mapping series_id to Silver DataFrame.
        """
        # Find all ECB policy rate files
        bronze_files = list(source_dir.glob("ecb_policy_rates_*.csv"))
        if not bronze_files:
            self.logger.warning("No ECB policy rate files found in %s", source_dir)
            return {}

        self.logger.info("Found %d ECB policy rate files", len(bronze_files))

        # Read and concatenate all files
        dfs = []
        for bronze_file in bronze_files:
            try:
                df = pd.read_csv(bronze_file)
                dfs.append(df)
            except Exception as e:
                self.logger.error("Failed to read %s: %s", bronze_file, e)
                continue

        if not dfs:
            self.logger.warning("No valid data read from ECB files")
            return {}

        raw = pd.concat(dfs, ignore_index=True)
        self.logger.info("Loaded %d raw ECB policy rate records", len(raw))

        # Transform to Silver schema per rate type
        result = {}
        for rate_code, (series_id, rate_name) in self._ECB_RATE_MAP.items():
            rate_data = raw[raw["PROVIDER_FM_ID"] == rate_code].copy()
            if rate_data.empty:
                self.logger.warning("No data for rate code %s", rate_code)
                continue

            silver_df = self._transform_ecb_to_silver(rate_data, series_id, rate_name)

            # Apply date filtering if specified
            if start_date or end_date:
                timestamp_col = pd.to_datetime(silver_df["timestamp_utc"])
                if start_date:
                    silver_df = silver_df[timestamp_col >= start_date]
                if end_date:
                    silver_df = silver_df[timestamp_col <= end_date]

            if not silver_df.empty:
                result[series_id] = silver_df
                self.logger.info("Transformed %d ECB records for %s", len(silver_df), series_id)

        return result

    def _process_fred_file(
        self,
        bronze_file: Path,
        start_date: datetime | None,
        end_date: datetime | None,
    ) -> pd.DataFrame:
        """Process a single FRED Bronze file to Silver schema.

        Args:
            bronze_file: Path to Bronze CSV file.
            start_date: Optional start date filter.
            end_date: Optional end date filter.

        Returns:
            Silver DataFrame with standardized schema.

        Raises:
            ValueError: If Bronze data is missing required columns.
        """
        # Read Bronze data
        df = pd.read_csv(bronze_file, encoding="utf-8")

        # Validate Bronze schema
        missing_cols = set(self.BRONZE_COLUMNS) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Bronze file {bronze_file.name} missing columns: {missing_cols}")

        # Transform to Silver schema
        df_silver = pd.DataFrame()

        # Convert date → timestamp_utc (ISO 8601)
        df_silver["timestamp_utc"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Copy and standardize columns
        df_silver["series_id"] = df["series_id"].astype(str)
        df_silver["value"] = pd.to_numeric(df["value"], errors="coerce")
        df_silver["source"] = df["source"].astype(str)
        df_silver["frequency"] = df["frequency"].astype(str)
        df_silver["units"] = df["units"].astype(str)

        # Remove rows with invalid values (NaN after coercion)
        initial_count = len(df_silver)
        df_silver = df_silver.dropna(subset=["value"])
        dropped = initial_count - len(df_silver)
        if dropped > 0:
            self.logger.warning(
                "Dropped %d rows with invalid values from %s", dropped, bronze_file.name
            )

        # Remove duplicates
        initial_count = len(df_silver)
        df_silver = df_silver.drop_duplicates(subset=["timestamp_utc", "series_id"])
        dropped = initial_count - len(df_silver)
        if dropped > 0:
            self.logger.warning("Removed %d duplicate records from %s", dropped, bronze_file.name)

        # Filter by date range if provided
        if start_date or end_date:
            timestamp_col = pd.to_datetime(df_silver["timestamp_utc"])
            if start_date:
                df_silver = df_silver[timestamp_col >= start_date]
            if end_date:
                df_silver = df_silver[timestamp_col <= end_date]

        # Sort by timestamp
        df_silver = df_silver.sort_values("timestamp_utc").reset_index(drop=True)

        # Validate Silver schema
        if not df_silver.empty:
            self.validate(df_silver)

        return df_silver

    def _transform_ecb_to_silver(
        self,
        df: pd.DataFrame,
        series_id: str,
        rate_name: str,
    ) -> pd.DataFrame:
        """Transform ECB policy rate data to Silver schema.

        Args:
            df: Raw ECB DataFrame filtered for one rate type.
            series_id: Series identifier (e.g., "ECB_DFR").
            rate_name: Human-readable rate name.

        Returns:
            DataFrame conforming to §3.2.2 macro schema.
        """
        # Convert TIME_PERIOD to timestamp_utc
        timestamps = pd.to_datetime(df["TIME_PERIOD"])
        timestamp_utc = timestamps.dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Parse rate values
        values = pd.to_numeric(df["OBS_VALUE"], errors="coerce")

        # Extract frequency (default to "B" for business days if not present)
        frequency = df["FREQ"].iloc[0] if "FREQ" in df.columns else "B"

        # Create Silver DataFrame
        silver = pd.DataFrame(
            {
                "timestamp_utc": timestamp_utc,
                "series_id": series_id,
                "value": values,
                "source": df["source"],
                "frequency": frequency,
                "units": "Percent",
            }
        )

        # Drop invalid rows
        silver = silver.dropna(subset=["value"])

        # Deduplicate (keep last in case of duplicates)
        silver = silver.drop_duplicates(subset=["timestamp_utc", "series_id"], keep="last")

        # Sort by timestamp
        silver = silver.sort_values("timestamp_utc").reset_index(drop=True)

        # Validate
        if not silver.empty:
            self.validate(silver)

        return silver

    def validate(self, df: pd.DataFrame) -> bool:
        """Validate that DataFrame conforms to Silver schema (§3.2.2).

        Checks:
        - All required columns present
        - timestamp_utc is valid ISO 8601
        - series_id is non-empty string
        - value is numeric
        - source is non-empty string
        - frequency is non-empty string
        - units is non-empty string
        - No duplicate (timestamp_utc, series_id) pairs

        Args:
            df: DataFrame to validate.

        Returns:
            True if valid.

        Raises:
            ValueError: If validation fails with details.
        """
        # Check columns
        missing_cols = set(self.SILVER_COLUMNS) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        extra_cols = set(df.columns) - set(self.SILVER_COLUMNS)
        if extra_cols:
            raise ValueError(f"Unexpected columns: {extra_cols}")

        # Validate timestamp_utc format
        try:
            pd.to_datetime(df["timestamp_utc"], format="ISO8601")
        except Exception as e:
            raise ValueError(f"Invalid timestamp_utc format: {e}") from e

        # Validate series_id
        if df["series_id"].isna().any() or (df["series_id"] == "").any():
            raise ValueError("series_id contains empty or null values")

        # Validate value is numeric
        if not pd.api.types.is_numeric_dtype(df["value"]):
            raise ValueError("value column must be numeric")

        if df["value"].isna().any():
            raise ValueError("value column contains NaN")

        # Validate source
        if df["source"].isna().any() or (df["source"] == "").any():
            raise ValueError("source contains empty or null values")

        # Validate frequency
        if df["frequency"].isna().any() or (df["frequency"] == "").any():
            raise ValueError("frequency contains empty or null values")

        # Validate units
        if df["units"].isna().any() or (df["units"] == "").any():
            raise ValueError("units contains empty or null values")

        # Check for duplicates
        duplicates = df.duplicated(subset=["timestamp_utc", "series_id"])
        if duplicates.any():
            count = duplicates.sum()
            raise ValueError(f"Found {count} duplicate (timestamp_utc, series_id) pairs")

        return True

    def process_and_export(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, Path]:
        """Convenience method: preprocess and export all series.

        Args:
            start_date: Optional start date filter.
            end_date: Optional end date filter.

        Returns:
            Dictionary mapping series_id to exported file path.

        Example:
            >>> normalizer = MacroNormalizer(input_dir, output_dir)
            >>> paths = normalizer.process_and_export(
            ...     start_date=datetime(2023, 1, 1),
            ...     end_date=datetime(2023, 12, 31)
            ... )
            >>> print(paths["DFF"])
            data/processed/macro/macro_DFF_2023-01-01_2023-12-31.csv
        """
        data = self.preprocess(start_date, end_date)

        if not data:
            self.logger.warning("No data to export")
            return {}

        paths = {}
        for series_id, df in data.items():
            try:
                # Extract date range from data
                timestamps = pd.to_datetime(df["timestamp_utc"])
                file_start = timestamps.min().to_pydatetime()
                file_end = timestamps.max().to_pydatetime()

                path = self.export(df, series_id, file_start, file_end)
                paths[series_id] = path
            except Exception as e:
                self.logger.error("Failed to export %s: %s", series_id, e)

        return paths
