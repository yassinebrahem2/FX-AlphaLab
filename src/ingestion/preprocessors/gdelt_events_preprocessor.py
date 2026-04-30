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
                continue

            bronze_files = self._bronze_files(chunk_start, chunk_end)
            if not bronze_files:
                results[month_key] = 0
                continue

            records: list[dict] = []
            for bronze_path in bronze_files:
                try:
                    df = pd.read_parquet(bronze_path)
                except Exception as exc:
                    self.logger.warning("Skipping unreadable bronze file %s: %s", bronze_path, exc)
                    continue

                if df.empty:
                    continue

                # We expect bronze to already contain normalized records
                for raw in df.to_dict("records"):
                    parsed = self._parse_record(raw)
                    if parsed is None:
                        continue
                    records.append(parsed)

            if not records:
                results[month_key] = 0
                continue

            df = self._build_frame(records)
            silver_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(silver_path, engine="pyarrow", index=False)
            results[month_key] = len(df)

        return results

    def health_check(self) -> bool:
        if self.input_dir.exists() and any(self.input_dir.rglob("*.parquet")):
            return True

        self.logger.warning(
            "GDELT health check failed: no Bronze parquet files in %s", self.input_dir
        )
        return False

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

    def _parse_record(self, raw: dict) -> dict | None:
        event_id = self._clean_str(raw.get("event_id"))
        if not event_id:
            return None

        event_date_raw = raw.get("event_date")
        event_date = pd.to_datetime(event_date_raw, utc=True, errors="coerce")
        if pd.isna(event_date):
            return None

        return {
            "event_date": event_date,
            "event_id": event_id,
            "actor1_name": self._clean_str(raw.get("actor1_name")),
            "actor1_country_code": self._clean_str(raw.get("actor1_country_code")),
            "actor1_type1_code": self._clean_str(raw.get("actor1_type1_code")),
            "actor2_name": self._clean_str(raw.get("actor2_name")),
            "actor2_country_code": self._clean_str(raw.get("actor2_country_code")),
            "actor2_type1_code": self._clean_str(raw.get("actor2_type1_code")),
            "event_code": self._clean_str(raw.get("event_code")),
            "event_base_code": self._clean_str(raw.get("event_base_code")),
            "event_root_code": self._clean_str(raw.get("event_root_code")),
            "quad_class": raw.get("quad_class"),
            "goldstein_scale": raw.get("goldstein_scale"),
            "num_mentions": raw.get("num_mentions"),
            "num_sources": raw.get("num_sources"),
            "num_articles": raw.get("num_articles"),
            "avg_tone": raw.get("avg_tone"),
            "actor1_geo_country_code": self._clean_str(raw.get("actor1_geo_country_code")),
            "actor2_geo_country_code": self._clean_str(raw.get("actor2_geo_country_code")),
            "action_geo_country_code": self._clean_str(raw.get("action_geo_country_code")),
            "action_geo_full_name": self._clean_str(raw.get("action_geo_full_name")),
            "source_url": self._clean_str(raw.get("source_url")),
        }

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

    def _build_frame(self, records: list[dict]) -> pd.DataFrame:
        df = pd.DataFrame(records, columns=self.SILVER_COLUMNS)
        df["event_date"] = pd.to_datetime(df["event_date"], utc=True)

        for column in [
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
        ]:
            df[column] = df[column].astype("string")

        for column in self.INT_COLUMNS:
            df[column] = pd.array(pd.to_numeric(df[column], errors="coerce"), dtype="Int64")

        for column in self.FLOAT_COLUMNS:
            df[column] = pd.to_numeric(df[column], errors="coerce").astype("float64")

        df = df.dropna(subset=["event_date"])
        df = df.reindex(columns=self.SILVER_COLUMNS)
        return df

    def _clean_str(self, value: object) -> str | None:
        if value is None:
            return None

        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass

        text = str(value).strip()
        return text or None
