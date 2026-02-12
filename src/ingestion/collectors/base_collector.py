"""Abstract base class for all data source collectors.

Enforces the §3.1 Bronze (Raw) Data Contract:
- Preserve all source fields (no transformation)
- Add `source` column
- snake_case column names, UTF-8 encoding
- Store in data/raw/{source}/
- File naming: {source}_{dataset}_{YYYYMMDD}.csv

Collectors are responsible ONLY for Bronze layer (raw data collection).
Preprocessing (Bronze → Silver) is handled by preprocessors.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.shared.utils import setup_logger


class BaseCollector(ABC):
    """Base class for all data collectors.

    Subclasses must define:
        SOURCE_NAME (str): identifier used in file naming (e.g. "ecb", "fred").

    Subclasses must implement:
        collect(): fetch all datasets for a date range.
        health_check(): verify the source is reachable.

    The export_csv() method handles §3.1 raw CSV naming automatically.
    """

    SOURCE_NAME: str  # e.g. "ecb", "fred", "mt5"

    def __init__(self, output_dir: Path, log_file: Path | None = None) -> None:
        """Initialize the collector.

        Args:
            output_dir: Directory for raw CSV exports (created if missing).
            log_file: Optional path for file-based logging.
        """
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger = setup_logger(self.__class__.__name__, log_file)

    @abstractmethod
    def collect(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Collect all datasets from the source.

        Args:
            start_date: Start of the collection window.
            end_date: End of the collection window.

        Returns:
            Mapping of dataset name to DataFrame, each conforming to §3.1.
        """
        ...

    @abstractmethod
    def health_check(self) -> bool:
        """Verify the data source is reachable and responding.

        Returns:
            True if the source is available, False otherwise.
        """
        ...

    def export_csv(self, df: pd.DataFrame, dataset_name: str) -> Path:
        """Export a DataFrame to raw CSV following the §3.1 naming convention.

        File path: {output_dir}/{SOURCE_NAME}_{dataset_name}_{YYYYMMDD}.csv

        Args:
            df: DataFrame to export.
            dataset_name: Dataset identifier (e.g. "policy_rates").

        Returns:
            Path to the written file.

        Raises:
            ValueError: If the DataFrame is empty.
        """
        if df.empty:
            raise ValueError(f"Cannot export empty DataFrame for '{dataset_name}'")

        date_str = datetime.now().strftime("%Y%m%d")
        path = self.output_dir / f"{self.SOURCE_NAME}_{dataset_name}_{date_str}.csv"
        df.to_csv(path, index=False, encoding="utf-8")
        self.logger.info("Exported %d records to %s", len(df), path)
        return path
