import hashlib
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery
from google.cloud.bigquery.job import QueryJob

from src.ingestion.collectors.base_collector import BaseCollector


class GDELTEventsCollector(BaseCollector):
    """Collector for GDELT Events data (Bronze layer)."""

    SOURCE_NAME = "gdelt_events"
    DEFAULT_LOOKBACK_DAYS: int = 30

    TIER_1 = {"reuters.com", "bloomberg.com", "ft.com"}
    TIER_2 = {"wsj.com", "cnbc.com"}

    def __init__(
        self,
        output_dir: Path,
        log_file: Path | None = None,
        project_id: str | None = None,
    ) -> None:
        super().__init__(output_dir=output_dir, log_file=log_file)
        self.project_id = project_id
        self.client: bigquery.Client | None = None

    def _get_client(self) -> bigquery.Client:
        if self.client is not None:
            return self.client

        if self.project_id:
            self.client = bigquery.Client(project=self.project_id)
        else:
            self.client = bigquery.Client()

        return self.client

    def _run_query_with_retry(
        self,
        query: str,
        job_config: bigquery.QueryJobConfig | None = None,
        max_retries: int = 3,
    ) -> QueryJob:
        delay = 2
        client = self._get_client()

        for attempt in range(max_retries):
            try:
                return client.query(query, job_config=job_config)
            except GoogleAPIError as exc:
                self.logger.warning(
                    "Query failed (attempt %d/%d): %s",
                    attempt + 1,
                    max_retries,
                    exc,
                )

                if attempt == max_retries - 1:
                    self.logger.error("Max retries reached. Aborting.")
                    raise

                time.sleep(delay)
                delay *= 2

        raise RuntimeError("Unreachable: retries exhausted without raising.")

    def collect(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        backfill: bool = False,
    ) -> dict[str, int]:
        if not start_date or not end_date:
            raise ValueError("start_date and end_date must be provided.")

        output_path = self.output_dir / f"gdelt_events_{start_date.strftime('%Y%m')}_raw.jsonl"
        if output_path.exists() and not backfill:
            with output_path.open("r", encoding="utf-8") as file_handle:
                n_existing = sum(1 for line in file_handle if line.strip())
            return {"aggregated": n_existing}

        current_day = start_date.date()
        end_day = end_date.date()
        records: list[dict] = []
        seen_event_ids: set[str] = set()

        while current_day <= end_day:
            next_day = current_day + timedelta(days=1)
            start_str = current_day.isoformat()
            end_str = next_day.isoformat()
            query = f"""
                SELECT
                    GlobalEventID,
                    SQLDATE,
                    Actor1Name,
                    Actor1Code,
                    Actor1CountryCode,
                    Actor1Type1Code,
                    Actor2Name,
                    Actor2Code,
                    Actor2CountryCode,
                    Actor2Type1Code,
                    EventCode,
                    EventBaseCode,
                    EventRootCode,
                    QuadClass,
                    GoldsteinScale,
                    NumMentions,
                    NumSources,
                    NumArticles,
                    AvgTone,
                    Actor1Geo_CountryCode,
                    Actor1Geo_FullName,
                    Actor2Geo_CountryCode,
                    Actor2Geo_FullName,
                    ActionGeo_CountryCode,
                    ActionGeo_FullName,
                    SOURCEURL
                FROM `gdelt-bq.gdeltv2.events`
                WHERE DATE(_PARTITIONTIME) >= "{start_str}"
                  AND DATE(_PARTITIONTIME) < "{end_str}"
                  AND (
                      Actor1CountryCode IN ('USA','GBR','EUZ','JPN','CHE','FRA','DEU','ITA')
                      OR Actor2CountryCode IN ('USA','GBR','EUZ','JPN','CHE','FRA','DEU','ITA')
                  )
            """

            dry_cfg = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            dry_job = self._run_query_with_retry(query, dry_cfg)
            total_bytes = getattr(dry_job, "total_bytes_processed", 0)
            if not isinstance(total_bytes, (int, float)):
                total_bytes = 0

            gb_scanned = total_bytes / (1024**3)
            if gb_scanned > 5:
                raise RuntimeError(f"Query too expensive ({gb_scanned:.2f} GB).")

            job = self._run_query_with_retry(query)
            df = job.result().to_dataframe(create_bqstorage_client=False)

            for row in df.to_dict("records"):
                event_id = self._event_id(row.get("GlobalEventID"))
                if not event_id:
                    continue

                if event_id in seen_event_ids:
                    continue

                seen_event_ids.add(event_id)
                records.append(
                    {
                        "source": self.SOURCE_NAME,
                        "timestamp_collected": datetime.now(timezone.utc).isoformat(),
                        "event_id": event_id,
                        "event_date": self._event_date(row.get("SQLDATE")),
                        "actor1_name": self._clean_str(row.get("Actor1Name")),
                        "actor1_code": self._clean_str(row.get("Actor1Code")),
                        "actor1_country_code": self._clean_str(row.get("Actor1CountryCode")),
                        "actor1_type1_code": self._clean_str(row.get("Actor1Type1Code")),
                        "actor2_name": self._clean_str(row.get("Actor2Name")),
                        "actor2_code": self._clean_str(row.get("Actor2Code")),
                        "actor2_country_code": self._clean_str(row.get("Actor2CountryCode")),
                        "actor2_type1_code": self._clean_str(row.get("Actor2Type1Code")),
                        "event_code": self._clean_str(row.get("EventCode")),
                        "event_base_code": self._clean_str(row.get("EventBaseCode")),
                        "event_root_code": self._clean_str(row.get("EventRootCode")),
                        "quad_class": self._parse_int(row.get("QuadClass")),
                        "goldstein_scale": self._parse_float(row.get("GoldsteinScale")),
                        "num_mentions": self._parse_int(row.get("NumMentions")),
                        "num_sources": self._parse_int(row.get("NumSources")),
                        "num_articles": self._parse_int(row.get("NumArticles")),
                        "avg_tone": self._parse_float(row.get("AvgTone")),
                        "actor1_geo_country_code": self._clean_str(
                            row.get("Actor1Geo_CountryCode")
                        ),
                        "actor1_geo_full_name": self._clean_str(row.get("Actor1Geo_FullName")),
                        "actor2_geo_country_code": self._clean_str(
                            row.get("Actor2Geo_CountryCode")
                        ),
                        "actor2_geo_full_name": self._clean_str(row.get("Actor2Geo_FullName")),
                        "action_geo_country_code": self._clean_str(
                            row.get("ActionGeo_CountryCode")
                        ),
                        "action_geo_full_name": self._clean_str(row.get("ActionGeo_FullName")),
                        "source_url": self._clean_str(row.get("SOURCEURL")),
                    }
                )

            current_day += timedelta(days=1)

        if not records:
            return {"aggregated": 0}

        records.sort(
            key=lambda record: (
                self._credibility_tier(record.get("source_url")),
                record.get("event_date") or "",
                hashlib.sha256(str(record.get("event_id", "")).encode("utf-8")).hexdigest(),
            )
        )

        with output_path.open("w", encoding="utf-8") as file_handle:
            for record in records:
                file_handle.write(json.dumps(record, ensure_ascii=False) + "\n")

        return {"aggregated": len(records)}

    def health_check(self) -> bool:
        try:
            client = self._get_client()
            job = client.query("SELECT 1")
            _ = job.result()
            return True
        except GoogleAPIError:
            return False

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

    def _event_id(self, value: object) -> str | None:
        text = self._clean_str(value)
        if not text:
            return None

        if text.endswith(".0"):
            try:
                return str(int(float(text)))
            except ValueError:
                return text

        return text

    def _event_date(self, value: object) -> str | None:
        text = self._clean_str(value)
        if not text:
            return None

        parsed = pd.to_datetime(text, format="%Y%m%d", errors="coerce")
        if pd.isna(parsed):
            return None

        return pd.Timestamp(parsed).strftime("%Y%m%d")

    def _parse_int(self, value: object) -> int | None:
        if value is None:
            return None

        numeric = pd.to_numeric(value, errors="coerce")
        if pd.isna(numeric):
            return None

        return int(numeric)

    def _parse_float(self, value: object) -> float | None:
        if value is None:
            return None

        numeric = pd.to_numeric(value, errors="coerce")
        if pd.isna(numeric):
            return None

        return float(numeric)

    def _credibility_tier(self, source_url: str | None) -> int:
        host = urlparse(source_url or "").netloc.lower()
        if any(domain in host for domain in self.TIER_1):
            return 1
        if any(domain in host for domain in self.TIER_2):
            return 2
        return 3
