import hashlib
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery
from google.cloud.bigquery.job import QueryJob

from src.ingestion.collectors.base_collector import BaseCollector


class GDELTGKGCollector(BaseCollector):
    """Collector for GDELT GKG data (Bronze layer)."""

    SOURCE_NAME = "gdelt_gkg"
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

        output_path = self.output_dir / f"gdelt_{start_date.strftime('%Y%m')}_raw.jsonl"
        if output_path.exists() and not backfill:
            with output_path.open("r", encoding="utf-8") as file_handle:
                n_existing = sum(1 for line in file_handle if line.strip())
            self.logger.info(
                "Skipping collection for %s to %s: %s already exists with %d rows",
                start_date.date(),
                end_date.date(),
                output_path,
                n_existing,
            )
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
                    DATE,
                    SourceCommonName,
                    DocumentIdentifier,
                    V2Tone,
                    V2Themes AS Themes,
                    V2Locations AS Locations,
                    V2Organizations AS Organizations
                FROM `gdelt-bq.gdeltv2.gkg_partitioned`
                WHERE DATE(_PARTITIONTIME) >= "{start_str}"
                  AND DATE(_PARTITIONTIME) < "{end_str}"
                  AND (
                      V2Themes LIKE '%ECON_CURRENCY%'
                      OR V2Themes LIKE '%ECON_CENTRAL_BANK%'
                  )
                  AND (
                      V2Themes LIKE '%EUR%'
                      OR V2Themes LIKE '%USD%'
                      OR V2Themes LIKE '%GBP%'
                      OR V2Themes LIKE '%JPY%'
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
                url = self._clean_str(row.get("DocumentIdentifier"))
                if not url:
                    continue

                url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
                if url_hash in seen_event_ids:
                    continue

                seen_event_ids.add(url_hash)
                records.append(
                    {
                        "source": self.SOURCE_NAME,
                        "timestamp_collected": datetime.now(timezone.utc).isoformat(),
                        "timestamp_published": self._parse_timestamp(row.get("DATE")),
                        "url": url,
                        "source_domain": self._clean_str(row.get("SourceCommonName")),
                        "v2tone": self._clean_str(row.get("V2Tone")) or "",
                        "themes": self._clean_str(row.get("Themes")) or "",
                        "locations": self._clean_str(row.get("Locations")) or "",
                        "organizations": self._clean_str(row.get("Organizations")) or "",
                    }
                )

            current_day += timedelta(days=1)

        if not records:
            return {"aggregated": 0}

        records.sort(
            key=lambda record: (
                self._credibility_tier(record.get("source_domain")),
                record.get("timestamp_published") or "",
                hashlib.sha256(str(record.get("url", "")).encode("utf-8")).hexdigest(),
            )
        )

        with output_path.open("w", encoding="utf-8") as file_handle:
            for record in records:
                file_handle.write(json.dumps(record, ensure_ascii=False) + "\n")

        self.logger.info("Collected %d documents -> %s", len(records), output_path)
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

    def _parse_timestamp(self, value: object) -> str | None:
        text = self._clean_str(value)
        if not text:
            return None

        parsed = pd.to_datetime(text, errors="coerce")
        if pd.isna(parsed):
            return None

        return pd.Timestamp(parsed, tz="UTC").isoformat()

    def _credibility_tier(self, source_domain: str | None) -> int:
        host = (source_domain or "").lower()
        if any(domain in host for domain in self.TIER_1):
            return 1
        if any(domain in host for domain in self.TIER_2):
            return 2
        return 3
