"""Abstract base class for all data preprocessors.

Enforces the §3.2 Silver (Processed) Data Contract:
- UTC ISO 8601 timestamps (timestamp_utc)
- Standardized column names across all sources
- Validated data types
- Deduplicated records
- Store in data/processed/{category}/
- File naming: {category}_{identifier}_{start_date}_{end_date}.{format}

Preprocessors are responsible for Bronze → Silver transformation.
They read from data/raw/{source}/ and write to data/processed/{category}/.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.shared.utils import setup_logger


class BasePreprocessor(ABC):
    """Base class for all data preprocessors.

    Subclasses must define:
        CATEGORY (str): data category for output (e.g., "ohlcv", "macro", "events").

    Subclasses must implement:
        preprocess(): transform Bronze data to Silver schema.
        validate(): ensure data conforms to Silver contract.

    The export() method handles §3.2 file naming and format selection.
    """

    CATEGORY: str  # e.g. "ohlcv", "macro", "events", "sentiment"

    def __init__(
        self,
        input_dir: Path,
        output_dir: Path,
        log_file: Path | None = None,
    ) -> None:
        """Initialize the preprocessor.

        Args:
            input_dir: Directory containing Bronze (raw) data.
            output_dir: Directory for Silver (processed) exports.
            log_file: Optional path for file-based logging.
        """
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger = setup_logger(self.__class__.__name__, log_file)

    @abstractmethod
    def preprocess(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Transform Bronze data to Silver schema.

        Reads raw data from input_dir, applies transformations:
        - UTC timestamp conversion
        - Column standardization
        - Type validation
        - Deduplication

        Args:
            start_date: Start of the processing window.
            end_date: End of the processing window.

        Returns:
            Mapping of dataset identifier to standardized DataFrame.
        """
        ...

    @abstractmethod
    def validate(self, df: pd.DataFrame) -> bool:
        """Validate that DataFrame conforms to Silver schema.

        Args:
            df: DataFrame to validate.

        Returns:
            True if valid, False otherwise.

        Raises:
            ValueError: If validation fails with details.
        """
        ...

    def export(
        self,
        df: pd.DataFrame,
        identifier: str,
        start_date: datetime,
        end_date: datetime,
        format: str = "csv",
    ) -> Path:
        """Export DataFrame to Silver layer following §3.2 naming convention.

        File path: {output_dir}/{CATEGORY}_{identifier}_{YYYY-MM-DD}_{YYYY-MM-DD}.{format}

        Args:
            df: DataFrame to export.
            identifier: Dataset identifier (e.g., "EURUSD_H1", "DFF").
            start_date: Start date of the data.
            end_date: End date of the data.
            format: Output format ("csv" or "parquet").

        Returns:
            Path to the written file.

        Raises:
            ValueError: If the DataFrame is empty or format is invalid.
        """
        if df.empty:
            raise ValueError(f"Cannot export empty DataFrame for '{identifier}'")

        if format not in ("csv", "parquet"):
            raise ValueError(f"Invalid format '{format}'. Must be 'csv' or 'parquet'.")

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        parts = [self.CATEGORY] + ([identifier] if identifier else []) + [start_str, end_str]
        filename = f"{'_'.join(parts)}.{format}"
        path = self.output_dir / filename

        if format == "csv":
            df.to_csv(path, index=False, encoding="utf-8")
        else:  # parquet
            df.to_parquet(path, index=False, engine="pyarrow")

        self.logger.info("Exported %d records to %s", len(df), path)
        return path
