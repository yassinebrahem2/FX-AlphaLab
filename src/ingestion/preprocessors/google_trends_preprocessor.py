"""Bronze to Silver preprocessor for Google Trends FX sentiment data."""

from __future__ import annotations

import re
from functools import reduce
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from src.shared.utils import setup_logger


class GoogleTrendsPreprocessor:
    """Merge Google Trends Bronze CSVs into a single Silver Parquet file."""

    def __init__(
        self,
        input_dir: Path,
        output_dir: Path,
        log_file: Path | None = None,
    ) -> None:
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger = setup_logger(self.__class__.__name__, log_file)

    def run(self, backfill: bool = False) -> dict[str, int]:
        silver_path = self.output_dir / "google_trends_weekly.parquet"

        if silver_path.exists() and not backfill:
            return {"google_trends_weekly": pq.read_metadata(silver_path).num_rows}

        csv_paths = sorted(self.input_dir.glob("trends_*.csv"))
        if not csv_paths:
            self.logger.warning("No Google Trends Bronze CSV files found in %s", self.input_dir)
            return {"google_trends_weekly": 0}

        frames: list[pd.DataFrame] = []
        theme_pattern = re.compile(r"trends_(.+)_\d+\.csv$")

        for csv_path in csv_paths:
            match = theme_pattern.match(csv_path.name)
            if match is None:
                continue

            theme = match.group(1)
            frame = pd.read_csv(csv_path)
            frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
            frame = frame.dropna(subset=["date"])

            renamed_columns = {
                column: self._theme_column_name(theme, column)
                for column in frame.columns
                if column != "date"
            }
            frame = frame.rename(columns=renamed_columns)
            frames.append(frame)

        if not frames:
            self.logger.warning("No valid Google Trends CSV files found in %s", self.input_dir)
            return {"google_trends_weekly": 0}

        combined = reduce(lambda left, right: pd.merge(left, right, on="date", how="outer"), frames)
        combined = combined.sort_values("date").drop_duplicates(subset=["date"], keep="last")

        keyword_columns = sorted(column for column in combined.columns if column != "date")
        combined = combined.reindex(columns=["date", *keyword_columns])

        for column in keyword_columns:
            combined[column] = pd.to_numeric(combined[column], errors="coerce").astype("float64")

        combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
        combined = combined.dropna(subset=["date"]).reset_index(drop=True)

        silver_path.parent.mkdir(parents=True, exist_ok=True)
        combined.to_parquet(silver_path, engine="pyarrow", index=False)

        return {"google_trends_weekly": len(combined)}

    def health_check(self) -> bool:
        return self.input_dir.exists() and any(self.input_dir.glob("trends_*.csv"))

    @staticmethod
    def _theme_column_name(theme: str, column: str) -> str:
        normalized = re.sub(r"[^0-9a-zA-Z]+", "_", column.lower())
        normalized = re.sub(r"_+", "_", normalized).strip("_")
        return f"{theme}__{normalized}"
