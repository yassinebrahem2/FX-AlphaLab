from calendar import monthrange
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from src.shared.utils import setup_logger


class GDELTEventsPreprocessor:
    """Bronze to Silver preprocessor for GDELT Events data."""

    SILVER_COLUMNS = [
        "event_date",
        "event_id",
        "actor1_name",
        "actor1_country_code",
        "actor1_type1_code",
        "actor2_name",
        "actor2_country_code",
        "actor2_type1_code",
        "event_code",
        "event_base_code",
        "event_root_code",
        "quad_class",
        "goldstein_scale",
        "num_mentions",
        "num_sources",
        "num_articles",
        "avg_tone",
        "actor1_geo_country_code",
        "actor2_geo_country_code",
        "action_geo_country_code",
        "action_geo_full_name",
        "source_url",
    ]

    # Maps raw GDELT column names → Silver snake_case names
    BRONZE_RENAME = {
        "GLOBALEVENTID": "event_id",
        "SQLDATE": "event_date",
        "Actor1Name": "actor1_name",
        "Actor1CountryCode": "actor1_country_code",
        "Actor1Type1Code": "actor1_type1_code",
        "Actor2Name": "actor2_name",
        "Actor2CountryCode": "actor2_country_code",
        "Actor2Type1Code": "actor2_type1_code",
        "EventCode": "event_code",
        "EventBaseCode": "event_base_code",
        "EventRootCode": "event_root_code",
        "QuadClass": "quad_class",
        "GoldsteinScale": "goldstein_scale",
        "NumMentions": "num_mentions",
        "NumSources": "num_sources",
        "NumArticles": "num_articles",
        "AvgTone": "avg_tone",
        "Actor1Geo_CountryCode": "actor1_geo_country_code",
        "Actor2Geo_CountryCode": "actor2_geo_country_code",
        "ActionGeo_CountryCode": "action_geo_country_code",
        "ActionGeo_FullName": "action_geo_full_name",
        "SOURCEURL": "source_url",
    }

    STRING_COLUMNS = [
        "event_id",
        "actor1_name",
        "actor1_country_code",
        "actor1_type1_code",
        "actor2_name",
        "actor2_country_code",
        "actor2_type1_code",
        "event_code",
        "event_base_code",
        "event_root_code",
        "actor1_geo_country_code",
        "actor2_geo_country_code",
        "action_geo_country_code",
        "action_geo_full_name",
        "source_url",
    ]

    INT_COLUMNS = ["quad_class", "num_mentions", "num_sources", "num_articles"]
    FLOAT_COLUMNS = ["goldstein_scale", "avg_tone"]

    def __init__(
        self,
        input_dir: Path,
        output_dir: Path,
        log_file: Path | None = None,
    ) -> None:
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.logger = setup_logger(self.__class__.__name__, log_file)

    def run(
        self,
        start_date: datetime,
        end_date: datetime,
        backfill: bool = False,
    ) -> dict[str, int]:
        results: dict[str, int] = {}
        for chunk_start, chunk_end in self._monthly_chunks(start_date, end_date):
            month_key = chunk_start.strftime("%Y%m")
            silver_path = self._silver_path(chunk_start)

            if silver_path.exists() and not backfill:
                results[month_key] = self._count_existing_rows(silver_path)
                self.logger.info(
                    "Skipping %s (already exists, %d rows)", month_key, results[month_key]
                )
                continue

            bronze_files = self._bronze_files(chunk_start, chunk_end)
            if not bronze_files:
                self.logger.warning("No bronze files for %s", month_key)
                results[month_key] = 0
                continue

            chunks: list[pd.DataFrame] = []
            for bronze_path in bronze_files:
                try:
                    df = pd.read_parquet(bronze_path, columns=self._available_columns(bronze_path))
                except Exception as exc:
                    self.logger.warning("Skipping unreadable bronze file %s: %s", bronze_path, exc)
                    continue
                if not df.empty:
                    chunks.append(df)

            if not chunks:
                results[month_key] = 0
                continue

            df = pd.concat(chunks, ignore_index=True)
            df = self._transform(df)

            if df.empty:
                results[month_key] = 0
                continue

            silver_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(silver_path, engine="pyarrow", index=False)
            results[month_key] = len(df)
            self.logger.info("Wrote %s: %d rows", month_key, len(df))

        return results

    def health_check(self) -> bool:
        if self.input_dir.exists() and any(self.input_dir.rglob("*.parquet")):
            return True
        self.logger.warning(
            "GDELT health check failed: no Bronze parquet files in %s", self.input_dir
        )
        return False

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Rename raw GDELT columns to Silver snake_case names
        df = df.rename(columns={k: v for k, v in self.BRONZE_RENAME.items() if k in df.columns})

        # Ensure all expected columns exist (fill missing with NA)
        for col in self.SILVER_COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA

        # Vectorized datetime parse + drop unparseable rows
        df["event_date"] = pd.to_datetime(df["event_date"], utc=True, errors="coerce")
        df = df.dropna(subset=["event_date", "event_id"])

        # Drop rows where event_id is empty string
        df = df[df["event_id"].astype(str).str.strip() != ""]

        for col in self.STRING_COLUMNS:
            df[col] = (
                df[col].astype(str).str.strip().replace({"nan": pd.NA, "None": pd.NA, "": pd.NA})
            )
            df[col] = df[col].astype("string")

        for col in self.INT_COLUMNS:
            df[col] = pd.array(pd.to_numeric(df[col], errors="coerce"), dtype="Int64")

        for col in self.FLOAT_COLUMNS:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

        return df.reindex(columns=self.SILVER_COLUMNS)

    def _available_columns(self, path: Path) -> list[str] | None:
        """Return only the Bronze columns we care about, or None to read all."""
        try:
            schema = pq.read_schema(path)
            available = set(schema.names)
            wanted = set(self.BRONZE_RENAME.keys())
            overlap = wanted & available
            return list(overlap) if overlap else None
        except Exception:
            return None

    def _bronze_files(self, chunk_start: datetime, chunk_end: datetime) -> list[Path]:
        files: list[Path] = []
        current = chunk_start
        while current <= chunk_end:
            path = (
                self.input_dir
                / f"{current.year}"
                / f"{current.month:02d}"
                / f"{current.strftime('%Y%m%d')}.parquet"
            )
            if path.exists():
                files.append(path)
            current = current.replace(day=current.day) + pd.Timedelta(days=1)
        return files

    def _monthly_chunks(self, start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
        chunks: list[tuple[datetime, datetime]] = []
        current = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        while current <= end:
            year, month = current.year, current.month
            last_day = monthrange(year, month)[1]
            month_end = current.replace(day=last_day, hour=23, minute=59, second=59)

            chunk_start = max(current, start)
            chunk_end = min(month_end, end)
            chunks.append((chunk_start, chunk_end))

            if month == 12:
                current = current.replace(year=year + 1, month=1, day=1)
            else:
                current = current.replace(month=month + 1, day=1)

        return chunks

    def _silver_path(self, dt: datetime) -> Path:
        return (
            self.output_dir
            / f"year={dt.year}"
            / f"month={dt.month:02d}"
            / "gdelt_events_cleaned.parquet"
        )

    def _count_existing_rows(self, path: Path) -> int:
        return pq.read_metadata(path).num_rows
