"""Abstract base class for document-oriented preprocessors.

For text-heavy, semi-structured data sources (news, speeches, articles)
where input is JSONL format from DocumentCollector sources.

Handles Bronze → Silver transformation for document data:
- Read JSONL files (one JSON object per line)
- Clean and normalize text content
- Extract structured features (sentiment, entities, etc.)
- Export to partitioned Parquet (Hive-style: source/year/month)

Preprocessors read from data/raw/news/{source}/ and write to:
  data/processed/sentiment/source={source}/year={year}/month={month}/sentiment_cleaned.parquet
"""

import hashlib
import json
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.shared.utils import setup_logger


class DocumentPreprocessor(ABC):
    """Base class for document-oriented data preprocessors.

    Use this for text-heavy sources where Bronze layer is JSONL format.
    For tabular CSV sources, use BasePreprocessor instead.

    Subclasses must implement:
        preprocess(): transform Bronze JSONL to Silver DataFrame.
        validate(): ensure data conforms to Silver contract.

    The export_partitioned() method handles Hive-style partitioning automatically:
      Output: data/processed/sentiment/source={source}/year={year}/month={month}/sentiment_cleaned.parquet
    """

    # No CATEGORY needed - all document data goes to data/processed/sentiment/

    def __init__(
        self,
        input_dir: Path,
        output_dir: Path,
        log_file: Path | None = None,
    ) -> None:
        """Initialize the document preprocessor.

        Args:
            input_dir: Directory containing Bronze JSONL data (e.g., data/raw/news/).
            output_dir: Root directory for Silver exports (e.g., data/processed/sentiment/).
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
    ) -> pd.DataFrame:
        """Transform Bronze JSONL documents to Silver DataFrame.

        Reads JSONL files from input_dir, applies transformations:
        - Text cleaning
        - Feature extraction (sentiment, entities, etc.)
        - Schema standardization
        - Deduplication

        Must include columns: source, timestamp_utc (for partitioning by year/month)

        Args:
            start_date: Start of the processing window.
            end_date: End of the processing window.

        Returns:
            Standardized DataFrame with all processed documents.
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

    def read_jsonl(self, file_path: Path) -> list[dict]:
        """Read JSONL file into list of dictionaries.

        Args:
            file_path: Path to JSONL file.

        Returns:
            List of document dictionaries.

        Raises:
            ValueError: If file doesn't exist or contains invalid JSON.
        """
        if not file_path.exists():
            raise ValueError(f"JSONL file not found: {file_path}")

        documents = []
        with open(file_path, encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    documents.append(json.loads(line))
                except json.JSONDecodeError as e:
                    self.logger.warning(
                        "Invalid JSON at %s line %d: %s", file_path.name, line_num, e
                    )
                    continue

        self.logger.info("Read %d documents from %s", len(documents), file_path)
        return documents

    def find_jsonl_files(
        self,
        source: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[Path]:
        """Find JSONL files in input directory.

        Args:
            source: Specific source to filter (e.g., "fed", "ecb"). If None, all sources.
            start_date: Filter files by date in filename (optional).
            end_date: Filter files by date in filename (optional).

        Returns:
            List of JSONL file paths.
        """
        if source:
            search_dir = self.input_dir / source
            if not search_dir.exists():
                self.logger.warning("Source directory not found: %s", search_dir)
                return []
            pattern = "**/*.jsonl"
        else:
            search_dir = self.input_dir
            pattern = "**/*.jsonl"

        files = list(search_dir.glob(pattern))
        self.logger.info("Found %d JSONL files in %s", len(files), search_dir)
        return files

    def clean_text(self, text: str) -> str:
        """Clean and normalize text content.

        Args:
            text: Raw text string.

        Returns:
            Cleaned text with normalized whitespace.
        """
        if not text:
            return ""

        # Remove excessive whitespace
        text = " ".join(text.split())

        # Remove common artifacts
        text = text.replace("\xa0", " ")  # Non-breaking space
        text = text.replace("\u200b", "")  # Zero-width space

        return text.strip()

    def generate_article_id(self, url: str | None, title: str, timestamp: str, source: str) -> str:
        """Generate stable article ID from document attributes.

        Uses URL if available (most reliable), otherwise hashes title + timestamp + source.

        Args:
            url: Article URL (preferred identifier).
            title: Article title.
            timestamp: Publication timestamp.
            source: Source identifier.

        Returns:
            16-character hex string (SHA256 truncated).
        """
        if url:
            content = url
        else:
            content = f"{source}|{title}|{timestamp}"

        return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]

    def export_partitioned(
        self,
        df: pd.DataFrame,
        partition_cols: list[str] | None = None,
    ) -> dict[str, Path]:
        """Export DataFrame to partitioned Parquet following Hive convention.

        Structure: {output_dir}/source={source}/year={year}/month={month}/sentiment_cleaned.parquet

        Example:
            data/processed/sentiment/source=fed/year=2026/month=02/sentiment_cleaned.parquet

        Args:
            df: DataFrame to export. Must contain 'source' and 'timestamp_utc' columns.
            partition_cols: Columns to partition by (default: ["source", "year", "month"]).

        Returns:
            Dictionary mapping partition key (e.g., "source=fed/year=2026/month=02") to file path.

        Raises:
            ValueError: If DataFrame is empty or missing required columns.
        """
        if df.empty:
            raise ValueError("Cannot export empty DataFrame")

        if partition_cols is None:
            partition_cols = ["source", "year", "month"]

        # Validate required columns
        if "source" not in df.columns:
            raise ValueError("DataFrame must contain 'source' column for partitioning")
        if "timestamp_utc" not in df.columns:
            raise ValueError("DataFrame must contain 'timestamp_utc' column for partitioning")

        # Add year/month columns from timestamp_utc if not present
        df = df.copy()
        df["_temp_timestamp"] = pd.to_datetime(df["timestamp_utc"])
        if "year" not in df.columns:
            df["year"] = df["_temp_timestamp"].dt.year
        if "month" not in df.columns:
            df["month"] = df["_temp_timestamp"].dt.month
        df = df.drop(columns=["_temp_timestamp"])

        # Validate partition columns exist
        missing = set(partition_cols) - set(df.columns)
        if missing:
            raise ValueError(f"DataFrame missing partition columns: {missing}")

        output_paths = {}

        # Group by partition columns and write each partition
        for partition_values, group_df in df.groupby(partition_cols, dropna=False):
            # Build partition path: source=fed/year=2026/month=02/
            if not isinstance(partition_values, tuple):
                partition_values = (partition_values,)

            partition_path = self.output_dir
            for col, val in zip(partition_cols, partition_values):
                partition_path = partition_path / f"{col}={val}"

            partition_path.mkdir(parents=True, exist_ok=True)

            # File name: sentiment_cleaned.parquet (consistent across all partitions)
            file_path = partition_path / "sentiment_cleaned.parquet"

            # Drop only year/month columns (temporary partitioning helpers)
            # Keep source column - it's part of the schema and needed for queries
            cols_to_drop = [col for col in ["year", "month"] if col in group_df.columns]
            output_df = group_df.drop(columns=cols_to_drop)

            # Write Parquet
            output_df.to_parquet(file_path, index=False, engine="pyarrow", compression="snappy")

            partition_key = "/".join(
                f"{col}={val}" for col, val in zip(partition_cols, partition_values)
            )
            output_paths[partition_key] = file_path

            self.logger.info("Exported %d records to %s", len(output_df), file_path)

        self.logger.info("Exported %d partitions total", len(output_paths))
        return output_paths
