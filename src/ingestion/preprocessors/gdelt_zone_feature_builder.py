from __future__ import annotations

from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from src.agents.geopolitical.signal import DIRECTED_EDGES, ZONE_COUNTRIES, ZONE_NODES
from src.shared.utils import setup_logger

FEATURE_NAMES = ["log_count", "goldstein", "conflict_frac", "avg_tone", "log_mentions"]


class GDELTZoneFeatureBuilder:
    """Build daily GDELT zone features and edge weights from cleaned Silver data."""

    def __init__(
        self,
        clean_silver_dir: Path,
        output_path: Path,
        log_file: Path | None = None,
    ) -> None:
        self.clean_silver_dir = clean_silver_dir
        self.output_path = output_path
        self.logger = setup_logger(self.__class__.__name__, log_file)

    def run(self, start_date: datetime, end_date: datetime, backfill: bool = False) -> int:
        paths = self._discover_parquet_paths(start_date, end_date)
        if not paths:
            self.logger.warning("No clean Silver parquet files found in %s", self.clean_silver_dir)
            return 0

        frames = []
        for path in paths:
            try:
                df = pd.read_parquet(path)
            except Exception as exc:
                self.logger.warning("Skipping unreadable Silver file %s: %s", path, exc)
                continue
            if df.empty:
                continue
            df = df.copy()
            df["event_day"] = pd.to_datetime(df["event_date"], utc=True).dt.floor("D")
            frames.append(df)

        if not frames:
            return 0

        events = pd.concat(frames, ignore_index=True)
        days = sorted(events["event_day"].dropna().unique())
        if not days:
            return 0

        rows = [self._compute_day_features(events, day) for day in days]
        output_df = pd.DataFrame(rows)

        if self.output_path.exists() and not backfill:
            existing = pd.read_parquet(self.output_path)
            existing = existing.copy()
            existing["date"] = pd.to_datetime(existing["date"]).dt.date
            new_dates = set(output_df["date"]) - set(existing["date"])
            if not new_dates:
                return len(existing)
            output_df = pd.concat(
                [existing, output_df[output_df["date"].isin(new_dates)]], ignore_index=True
            )

        output_df = output_df.sort_values("date").reset_index(drop=True)
        output_df.to_parquet(self.output_path, engine="pyarrow", index=False)
        return len(output_df)

    def health_check(self) -> bool:
        if self.clean_silver_dir.exists() and any(self.clean_silver_dir.rglob("*.parquet")):
            return True

        self.logger.warning(
            "GDELT zone feature builder health check failed: no Silver parquet files in %s",
            self.clean_silver_dir,
        )
        return False

    def _discover_parquet_paths(self, start_date: datetime, end_date: datetime) -> list[Path]:
        paths: list[Path] = []
        current = pd.Timestamp(start_date).floor("D")
        end = pd.Timestamp(end_date).floor("D")

        while current <= end:
            path = (
                self.clean_silver_dir
                / f"year={current.year}"
                / f"month={current.month:02d}"
                / "gdelt_events_cleaned.parquet"
            )
            if path.exists():
                paths.append(path)

            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1, day=1)
            else:
                current = current.replace(month=current.month + 1, day=1)

        return paths

    def _compute_day_features(
        self, events: pd.DataFrame, day: pd.Timestamp
    ) -> dict[str, float | datetime.date]:
        day_df = events[events["event_day"] == day]
        row: dict[str, float | datetime.date] = {"date": day.date()}

        for zone in ZONE_NODES:
            zone_key = zone.lower()
            zone_df = self._zone_slice(day_df, zone)
            features = self._zone_features(zone_df)
            for name, value in zip(FEATURE_NAMES, features, strict=True):
                row[f"{zone_key}_{name}"] = float(value)

        for zi, zj in DIRECTED_EDGES:
            edge_value = self._edge_weight(day_df, zi, zj)
            row[f"edge_{zi.lower()}_{zj.lower()}"] = float(edge_value)

        return row

    def _zone_slice(self, day_df: pd.DataFrame, zone: str) -> pd.DataFrame:
        countries = ZONE_COUNTRIES[zone]
        mask = day_df["actor1_country_code"].isin(countries) | day_df["actor2_country_code"].isin(
            countries
        )
        return day_df[mask]

    def _zone_features(self, zone_df: pd.DataFrame) -> np.ndarray:
        if zone_df.empty:
            return np.zeros(5, dtype="float64")

        quad_values = zone_df["quad_class"].astype("float64")
        features = np.array(
            [
                np.log1p(len(zone_df)),
                zone_df["goldstein_scale"].mean(),
                quad_values.isin([3.0, 4.0]).mean(),
                zone_df["avg_tone"].mean(),
                np.log1p(zone_df["num_mentions"].sum()),
            ],
            dtype="float64",
        )
        return np.nan_to_num(features, nan=0.0)

    def _edge_weight(self, day_df: pd.DataFrame, zi: str, zj: str) -> float:
        mask = day_df["actor1_country_code"].isin(ZONE_COUNTRIES[zi]) & day_df[
            "actor2_country_code"
        ].isin(ZONE_COUNTRIES[zj])
        return float(np.log1p(mask.sum()))
