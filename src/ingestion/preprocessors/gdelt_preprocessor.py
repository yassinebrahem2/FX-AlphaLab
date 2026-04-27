import json
from calendar import monthrange
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from src.shared.utils import setup_logger


class GDELTPreprocessor:
    """Bronze to Silver preprocessor for GDELT news data."""

    SILVER_COLUMNS = [
        "timestamp_utc",
        "url",
        "source_domain",
        "source",
        "tone",
        "positive_score",
        "negative_score",
        "polarity",
        "activity_ref_density",
        "self_group_ref_density",
        "word_count",
        "themes",
        "locations",
        "organizations",
    ]

    FLOAT_COLUMNS = [
        "tone",
        "positive_score",
        "negative_score",
        "polarity",
        "activity_ref_density",
        "self_group_ref_density",
    ]

    V2TONE_FIELDS = [
        "tone",
        "positive_score",
        "negative_score",
        "polarity",
        "activity_ref_density",
        "self_group_ref_density",
        "word_count",
    ]

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

            bronze_path = self.input_dir / f"gdelt_{month_key}_raw.jsonl"
            if not bronze_path.exists():
                results[month_key] = 0
                continue

            records: list[dict] = []
            with bronze_path.open("r", encoding="utf-8") as file_handle:
                for line_number, line in enumerate(file_handle, start=1):
                    stripped = line.strip()
                    if not stripped:
                        continue

                    try:
                        raw = json.loads(stripped)
                    except json.JSONDecodeError as exc:
                        self.logger.warning(
                            "Skipping malformed JSON in %s line=%d: %s",
                            bronze_path,
                            line_number,
                            exc,
                        )
                        continue

                    if not isinstance(raw, dict):
                        self.logger.warning(
                            "Skipping non-object JSON in %s line=%d",
                            bronze_path,
                            line_number,
                        )
                        continue

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
        if self.input_dir.exists() and any(self.input_dir.glob("gdelt_*_raw.jsonl")):
            return True

        self.logger.warning("GDELT health check failed: no Bronze files in %s", self.input_dir)
        return False

    def _parse_record(self, raw: dict) -> dict | None:
        url = raw.get("url")
        if not url:
            return None

        timestamp_raw = raw.get("timestamp_published")
        if not timestamp_raw:
            return None

        try:
            timestamp_utc = pd.to_datetime(timestamp_raw, utc=True)
        except (ValueError, TypeError):
            return None

        if pd.isna(timestamp_utc):
            return None

        tone_fields = self._parse_v2tone(raw.get("v2tone", ""))

        return {
            "timestamp_utc": timestamp_utc,
            "url": str(url),
            "source_domain": raw.get("source_domain"),
            "source": raw.get("source") or "gdelt",
            "tone": tone_fields["tone"],
            "positive_score": tone_fields["positive_score"],
            "negative_score": tone_fields["negative_score"],
            "polarity": tone_fields["polarity"],
            "activity_ref_density": tone_fields["activity_ref_density"],
            "self_group_ref_density": tone_fields["self_group_ref_density"],
            "word_count": tone_fields["word_count"],
            "themes": self._split_semicolon(raw.get("themes")),
            "locations": self._split_semicolon(raw.get("locations")),
            "organizations": self._split_semicolon(raw.get("organizations")),
        }

    def _parse_v2tone(self, raw: str) -> dict[str, float | int | None]:
        values = {field: None for field in self.V2TONE_FIELDS}

        if raw is None:
            return values

        text = str(raw).strip()
        if not text:
            return values

        parts = [part.strip() for part in text.split(",")]
        for index, field in enumerate(self.V2TONE_FIELDS):
            if index >= len(parts):
                break

            token = parts[index]
            if not token:
                continue

            try:
                if field == "word_count":
                    values[field] = int(float(token))
                else:
                    values[field] = float(token)
            except (TypeError, ValueError):
                values[field] = None

        return values

    def _split_semicolon(self, raw: str | None) -> list[str]:
        if raw is None:
            return []

        return [token.strip() for token in str(raw).split(";") if token.strip()]

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
            / "sentiment_cleaned.parquet"
        )

    def _count_existing_rows(self, path: Path) -> int:
        return pq.read_metadata(path).num_rows

    def _build_frame(self, records: list[dict]) -> pd.DataFrame:
        df = pd.DataFrame(records, columns=self.SILVER_COLUMNS)
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

        for column in self.FLOAT_COLUMNS:
            df[column] = pd.to_numeric(df[column], errors="coerce").astype("float64")

        df["word_count"] = pd.array(pd.to_numeric(df["word_count"], errors="coerce"), dtype="Int64")
        df = df.reindex(columns=self.SILVER_COLUMNS)
        return df
