import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.shared.utils import setup_logger

__all__ = ["GDELTSignalNode"]


class GDELTSignalNode:
    """Signal node for GDELT volatility/regime modulation.

    GDELT is NOT a directional sentiment source. Empirical IC analysis shows:
    - Mean |IC| vs next-day log return = 0.026 (below 0.03 threshold)
    - Mean |IC| vs next-day |return| = 0.035–0.073, p<0.05 across all pairs
    - Conclusion: GDELT is a volatility/regime modulator

    The node reads Bronze JSONL directly. No Silver intermediary exists
    or is needed — transformation is trivial and has no second consumer.
    """

    ROLLING_WINDOW = 30
    MIN_PERIODS = 10
    MIN_DAILY_ARTICLES = 3

    def __init__(self, bronze_dir: Path, log_file: Path | None = None) -> None:
        """Initialize GDELT signal node.

        Args:
            bronze_dir: Path to data/raw/news/gdelt/ containing aggregated JSONL files
            log_file: Optional path for log output
        """
        self.bronze_dir = bronze_dir
        self.logger = setup_logger(self.__class__.__name__, log_file)

    def compute(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Compute GDELT volatility signal.

        Reads Bronze JSONL files, aggregates tone and article count by day,
        applies rolling z-score normalization, and returns DataFrame.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            DataFrame with columns: date, tone_mean, article_count,
            tone_zscore, attention_zscore

        Raises:
            ValueError: If no matching JSONL files found in date range
        """
        # Step 1: Discover JSONL files
        jsonl_files = self._discover_files(start_date, end_date)
        if not jsonl_files:
            raise ValueError(
                f"No JSONL files found in {self.bronze_dir} for "
                f"{start_date.date()} to {end_date.date()}"
            )

        # Step 2: Load and parse records
        records = self._load_records(jsonl_files, start_date, end_date)

        # Step 3: Daily aggregation
        daily_df = self._daily_aggregation(records)

        # Step 4: Reindex to fill missing calendar dates
        daily_df = self._reindex_calendar(daily_df, start_date.date(), end_date.date())

        # Step 5: Rolling z-score
        daily_df = self._apply_rolling_zscore(daily_df)

        # Step 6: Mask low-coverage days
        daily_df.loc[daily_df["article_count"] < self.MIN_DAILY_ARTICLES, "tone_zscore"] = pd.NA

        # Step 7: Drop rows outside date range
        daily_df = daily_df[
            (daily_df["date"] >= pd.Timestamp(start_date.date()))
            & (daily_df["date"] <= pd.Timestamp(end_date.date()))
        ].copy()

        # Log summary
        valid_zscores = daily_df["tone_zscore"].notna().sum()
        self.logger.info(
            "Loaded %d files, %d records, %d unique days, %d with valid tone_zscore",
            len(jsonl_files),
            len(records),
            len(daily_df),
            valid_zscores,
        )

        return daily_df.reset_index(drop=True)

    def _discover_files(self, start_date: datetime, end_date: datetime) -> list[Path]:
        """Discover JSONL files that overlap the date range.

        Files are named aggregated_YYYYMM.jsonl. Include file if its month
        overlaps [start_date, end_date].
        """
        if not self.bronze_dir.exists():
            return []

        files = sorted(self.bronze_dir.glob("aggregated_*.jsonl"))
        overlapping = []

        for fpath in files:
            try:
                yyyymm_str = fpath.stem.replace("aggregated_", "")
                year = int(yyyymm_str[:4])
                month = int(yyyymm_str[4:6])
                month_start = pd.Timestamp(year, month, 1).date()
                # Get last day of month
                if month == 12:
                    month_end = pd.Timestamp(year + 1, 1, 1) - pd.Timedelta(days=1)
                else:
                    month_end = pd.Timestamp(year, month + 1, 1) - pd.Timedelta(days=1)
                month_end = month_end.date()

                if month_start <= end_date.date() and month_end >= start_date.date():
                    overlapping.append(fpath)
            except (ValueError, IndexError):
                self.logger.warning("Skipping malformed filename: %s", fpath)
                continue

        return overlapping

    def _load_records(
        self, jsonl_files: list[Path], start_date: datetime, end_date: datetime
    ) -> list[dict]:
        """Load and parse records from JSONL files."""
        records = []
        start_date_obj = start_date.date()
        end_date_obj = end_date.date()

        for fpath in jsonl_files:
            with fpath.open("r", encoding="utf-8") as fh:
                for line_number, line in enumerate(fh, start=1):
                    stripped = line.strip()
                    if not stripped:
                        continue

                    try:
                        item = json.loads(stripped)
                    except json.JSONDecodeError as exc:
                        self.logger.warning(
                            "Skipping malformed JSON in %s line=%d: %s",
                            fpath,
                            line_number,
                            exc,
                        )
                        continue

                    if not isinstance(item, dict):
                        self.logger.warning(
                            "Skipping non-object JSON in %s line=%d",
                            fpath,
                            line_number,
                        )
                        continue

                    # Extract date
                    date_obj = self._parse_date(item)
                    if date_obj is None:
                        continue

                    # Check if within range
                    if not (start_date_obj <= date_obj <= end_date_obj):
                        continue

                    # Extract tone
                    tone = self._parse_tone(item)
                    if tone is None:
                        continue

                    records.append({"date": date_obj, "tone": tone})

        return records

    def _parse_date(self, record: dict) -> pd.Timestamp | None:
        """Parse date from record, trying timestamp_published then
        timestamp_collected."""
        for field in ["timestamp_published", "timestamp_collected"]:
            if field in record:
                try:
                    return pd.to_datetime(record[field], utc=True).date()
                except (ValueError, TypeError):
                    continue
        return None

    def _parse_tone(self, record: dict) -> float | None:
        """Parse tone field as float."""
        if "tone" not in record:
            return None
        try:
            return float(record["tone"])
        except (ValueError, TypeError):
            return None

    def _daily_aggregation(self, records: list[dict]) -> pd.DataFrame:
        """Aggregate records into daily tone_mean and article_count."""
        if not records:
            return pd.DataFrame(
                {
                    "date": pd.Series(dtype="datetime64[ns]"),
                    "tone_mean": pd.Series(dtype="float64"),
                    "article_count": pd.Series(dtype="int64"),
                }
            )

        df = pd.DataFrame(records)
        df["date"] = pd.to_datetime(df["date"])

        # Daily article count (all records)
        daily_count = df.groupby("date").size().rename("article_count")

        # Daily tone mean (only non-zero tones)
        non_zero = df[df["tone"] != 0.0].copy()
        daily_tone = non_zero.groupby("date")["tone"].mean().rename("tone_mean")

        # Combine with proper column order
        result = pd.concat([daily_tone, daily_count], axis=1).reset_index()
        result = result[["date", "tone_mean", "article_count"]]
        return result

    def _reindex_calendar(self, df: pd.DataFrame, start_date_obj, end_date_obj) -> pd.DataFrame:
        """Reindex to fill missing calendar dates with NaN tone_mean and 0
        article_count."""
        if df.empty:
            date_range = pd.date_range(
                start=pd.Timestamp(start_date_obj),
                end=pd.Timestamp(end_date_obj),
                freq="D",
            )
            return pd.DataFrame(
                {
                    "date": date_range,
                    "tone_mean": pd.NA,
                    "article_count": 0,
                }
            )

        df["date"] = pd.to_datetime(df["date"])
        full_range = pd.date_range(
            start=pd.Timestamp(start_date_obj),
            end=pd.Timestamp(end_date_obj),
            freq="D",
        )
        df_reindexed = df.set_index("date").reindex(full_range)
        df_reindexed.index.name = "date"
        # Fill missing article_count with 0, tone_mean stays as NaN
        df_reindexed["article_count"] = df_reindexed["article_count"].fillna(0).astype(int)
        return df_reindexed.reset_index()

    def _apply_rolling_zscore(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply rolling z-score normalization."""
        # Rolling z-score for tone
        rolling_mean = (
            df["tone_mean"]
            .rolling(
                window=self.ROLLING_WINDOW,
                min_periods=self.MIN_PERIODS,
                center=False,
            )
            .mean()
        )
        rolling_std = (
            df["tone_mean"]
            .rolling(
                window=self.ROLLING_WINDOW,
                min_periods=self.MIN_PERIODS,
                center=False,
            )
            .std()
        )

        with pd.option_context("mode.copy_on_write", True):
            df = df.copy()
            # Replace rolling_std == 0 with NaN before dividing
            rolling_std_safe = rolling_std.copy()
            rolling_std_safe[rolling_std_safe == 0] = pd.NA
            df["tone_zscore"] = (df["tone_mean"] - rolling_mean) / rolling_std_safe

        # Rolling z-score for attention
        rolling_mean_att = (
            df["article_count"]
            .rolling(
                window=self.ROLLING_WINDOW,
                min_periods=self.MIN_PERIODS,
                center=False,
            )
            .mean()
        )
        rolling_std_att = (
            df["article_count"]
            .rolling(
                window=self.ROLLING_WINDOW,
                min_periods=self.MIN_PERIODS,
                center=False,
            )
            .std()
        )

        with pd.option_context("mode.copy_on_write", True):
            rolling_std_att_safe = rolling_std_att.copy()
            rolling_std_att_safe[rolling_std_att_safe == 0] = pd.NA
            df["attention_zscore"] = (df["article_count"] - rolling_mean_att) / rolling_std_att_safe

        return df
