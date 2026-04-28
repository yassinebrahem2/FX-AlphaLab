"""Signal node for Google Trends FX attention."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from src.shared.utils import setup_logger

__all__ = ["GoogleTrendsSignalNode"]


class GoogleTrendsSignalNode:
    """Signal node for Google Trends attention fusion."""

    ROLLING_WINDOW = 30
    MIN_PERIODS = 10

    THEME_PREFIXES: dict[str, str] = {
        "macro": "macro_indicators__",
        "central_bank": "central_banks__",
        "risk_sentiment": "risk_sentiment__",
        "fx_pairs": "fx_pairs__",
    }

    def __init__(self, silver_dir: Path, log_file: Path | None = None) -> None:
        self.silver_dir = silver_dir
        self._silver_path = silver_dir / "google_trends_weekly.parquet"
        self.logger = setup_logger(self.__class__.__name__, log_file)

    def compute(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Compute daily Google Trends attention signals from weekly Silver data."""
        if not self._silver_path.exists():
            raise ValueError(f"Google Trends Silver file not found: {self._silver_path}")

        weekly_df = pd.read_parquet(self._silver_path)
        weekly_df["date"] = pd.to_datetime(weekly_df["date"], errors="coerce")
        weekly_df = weekly_df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

        for theme_name, prefix in self.THEME_PREFIXES.items():
            theme_columns = [column for column in weekly_df.columns if column.startswith(prefix)]
            if theme_columns:
                weekly_df[f"{theme_name}_attention_index"] = weekly_df[theme_columns].mean(axis=1)
            else:
                weekly_df[f"{theme_name}_attention_index"] = pd.Series(
                    [pd.NA] * len(weekly_df), index=weekly_df.index, dtype="float64"
                )

        for theme_name in self.THEME_PREFIXES:
            index_column = f"{theme_name}_attention_index"
            zscore_column = f"{theme_name}_attention_zscore"

            rolling_mean = (
                weekly_df[index_column]
                .rolling(window=self.ROLLING_WINDOW, min_periods=self.MIN_PERIODS, center=False)
                .mean()
            )
            rolling_std = (
                weekly_df[index_column]
                .rolling(window=self.ROLLING_WINDOW, min_periods=self.MIN_PERIODS, center=False)
                .std()
            )

            rolling_std_safe = rolling_std.copy()
            rolling_std_safe[rolling_std_safe == 0] = pd.NA
            weekly_df[zscore_column] = (weekly_df[index_column] - rolling_mean) / rolling_std_safe

        silver_min_date = weekly_df["date"].min().date()
        silver_max_date = weekly_df["date"].max().date()
        full_start = pd.Timestamp(min(silver_min_date, start_date.date()))
        full_end = pd.Timestamp(max(silver_max_date, end_date.date()))
        full_range = pd.date_range(start=full_start, end=full_end, freq="D")

        output_columns = [
            "date",
            "macro_attention_index",
            "central_bank_attention_index",
            "risk_sentiment_attention_index",
            "fx_pairs_attention_index",
            "macro_attention_zscore",
            "central_bank_attention_zscore",
            "risk_sentiment_attention_zscore",
            "fx_pairs_attention_zscore",
        ]
        value_columns = [column for column in output_columns if column != "date"]

        daily_df = weekly_df[output_columns].set_index("date").reindex(full_range)
        daily_df.index.name = "date"
        daily_df[value_columns] = daily_df[value_columns].ffill()
        daily_df = daily_df.reset_index()

        daily_df["date"] = pd.to_datetime(daily_df["date"], errors="coerce")
        daily_df = daily_df[
            (daily_df["date"] >= pd.Timestamp(start_date.date()))
            & (daily_df["date"] <= pd.Timestamp(end_date.date()))
        ].copy()
        daily_df = daily_df[output_columns]

        for column in value_columns:
            daily_df[column] = pd.to_numeric(daily_df[column], errors="coerce").astype("float64")

        daily_df["date"] = pd.to_datetime(daily_df["date"], errors="coerce")

        valid_macro_zscores = daily_df["macro_attention_zscore"].notna().sum()
        self.logger.info(
            "Loaded %d silver weeks, produced %d rows, %d with valid macro_attention_zscore",
            len(weekly_df),
            len(daily_df),
            valid_macro_zscores,
        )

        return daily_df.reset_index(drop=True)
