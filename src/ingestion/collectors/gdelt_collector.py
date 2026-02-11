from datetime import datetime, timedelta
from pathlib import Path
import json
import hashlib
import pandas as pd
import time

from google.cloud.bigquery.job import QueryJob
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

from src.ingestion.collectors.base_collector import BaseCollector


class GDELTCollector(BaseCollector):
    """Collector for GDELT financial news data (Bronze layer)."""

    SOURCE_NAME = "news"

    TIER_1 = {"reuters.com", "bloomberg.com", "ft.com"}
    TIER_2 = {"wsj.com", "cnbc.com"}

    # -------------------------------------------------------
    # Initialization
    # -------------------------------------------------------
    def __init__(
        self,
        output_dir: Path,
        log_file: Path | None = None,
        project_id: str | None = None,
    ) -> None:
        super().__init__(output_dir=output_dir, log_file=log_file)

        self.project_id = project_id
        self.client = (
            bigquery.Client(project=project_id)
            if project_id
            else bigquery.Client()
        )

        self.gdelt_dir = self.output_dir / "gdelt"
        self.gdelt_dir.mkdir(parents=True, exist_ok=True)
    def _run_query_with_retry(
        self,
        query: str,
        job_config: bigquery.QueryJobConfig | None = None,
        max_retries: int = 3,
    ) -> QueryJob:
        """Execute a BigQuery query with exponential backoff retry."""

        delay = 2  # initial backoff in seconds

        for attempt in range(max_retries):
            try:
                return self.client.query(query, job_config=job_config)

            except GoogleAPIError as e:
                self.logger.warning(
                    "Query failed (attempt %d/%d): %s",
                    attempt + 1,
                    max_retries,
                    e,
                )

                if attempt == max_retries - 1:
                    self.logger.error("Max retries reached. Aborting.")
                    raise e

                time.sleep(delay)
                delay *= 2  # exponential backoff

    # -------------------------------------------------------
    # Helpers
    # -------------------------------------------------------
    def _assign_credibility_tier(self, domain: str | None) -> int:
        domain = (domain or "").lower()
        if any(d in domain for d in self.TIER_1):
            return 1
        if any(d in domain for d in self.TIER_2):
            return 2
        return 3

    def _hash_url(self, url: str) -> str:
        return hashlib.sha256(url.encode("utf-8")).hexdigest()

    def _parse_field(self, field_value):
        if not field_value:
            return []
        return [item.strip() for item in str(field_value).split(";")]

    # -------------------------------------------------------
    # Main Collection Logic
    # -------------------------------------------------------
    def collect(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, pd.DataFrame]:

        if not start_date or not end_date:
            raise ValueError("start_date and end_date must be provided.")

        current_day = start_date.date()
        end_day = end_date.date()

        all_dataframes = []

        try:
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

                # -------------------------
                # DRY RUN
                # -------------------------
                job_config = bigquery.QueryJobConfig(
                    dry_run=True,
                    use_query_cache=False,
                )

                dry_job = self._run_query_with_retry(query, job_config)

                gb_scanned = dry_job.total_bytes_processed / (1024**3)

                self.logger.info(
                    "Dry run for %s: %.4f GB",
                    start_str,
                    gb_scanned,
                )

                if gb_scanned > 5:
                    raise RuntimeError(
                        f"Query too expensive ({gb_scanned:.2f} GB)."
                    )

                # -------------------------
                # EXECUTION
                # -------------------------
                job = self._run_query_with_retry(query)


                df = job.result().to_dataframe(
                    create_bqstorage_client=False
                )

                self.logger.info(
                    "Fetched %d rows for %s",
                    len(df),
                    start_str,
                )

                all_dataframes.append(df)

                # -------------------------
                # WRITE BRONZE JSONL
                # -------------------------
                output_file = (
                    self.gdelt_dir
                    / f"aggregated_{current_day.strftime('%Y%m%d')}.jsonl"
                )

                seen_hashes: set[str] = set()

                with open(output_file, "w", encoding="utf-8") as f:
                    for _, row in df.iterrows():

                        url = row["DocumentIdentifier"]
                        if not url:
                            continue

                        url_hash = self._hash_url(url)

                        if url_hash in seen_hashes:
                            continue
                        seen_hashes.add(url_hash)

                        tier = self._assign_credibility_tier(
                            row["SourceCommonName"]
                        )

                        record = {
                            "source": "GDELT",
                            "timestamp_collected": datetime.utcnow().isoformat(),
                            "timestamp_published": str(row["DATE"]),
                            "url": url,
                            "source_domain": row["SourceCommonName"],
                            "tone": row["V2Tone"],
                            "themes": self._parse_field(row["Themes"]),
                            "locations": self._parse_field(row["Locations"]),
                            "organizations": self._parse_field(
                                row["Organizations"]
                            ),
                            "metadata": {
                                "credibility_tier": tier,
                                "url_hash": url_hash,
                            },
                        }

                        f.write(json.dumps(record) + "\n")

                self.logger.info(
                    "Bronze JSONL written to %s (%d unique records)",
                    output_file,
                    len(seen_hashes),
                )

                current_day += timedelta(days=1)

            combined_df = (
                pd.concat(all_dataframes, ignore_index=True)
                if all_dataframes
                else pd.DataFrame()
            )

            return {"gdelt_raw": combined_df}

        except GoogleAPIError as e:
            self.logger.error("GDELT query failed: %s", e)
            raise

    # -------------------------------------------------------
    # Health Check
    # -------------------------------------------------------
    def health_check(self) -> bool:
        try:
            job = self.client.query("SELECT 1")
            _ = job.result()
            self.logger.info("BigQuery health check successful.")
            return True
        except GoogleAPIError as e:
            self.logger.error("BigQuery health check failed: %s", e)
            return False
