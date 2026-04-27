import hashlib
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery
from google.cloud.bigquery.job import QueryJob

from src.ingestion.collectors.base_collector import BaseCollector


class GDELTCollector(BaseCollector):
    """Collector for GDELT financial news data (Bronze layer)."""

    SOURCE_NAME = "gdelt"
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

    # -------------------------------------------------------
    # Internal Lazy Client Getter (supports test injection)
    # -------------------------------------------------------
    def _get_client(self) -> bigquery.Client:
        """
        Lazily get a BigQuery client.

        Tests can inject a mock via:
            collector.client = mock_client
        """
        if self.client is not None:
            return self.client

        # Production fallback (may require ADC credentials)
        if self.project_id:
            self.client = bigquery.Client(project=self.project_id)
        else:
            self.client = bigquery.Client()

        return self.client

    # -------------------------------------------------------
    # Retry Logic
    # -------------------------------------------------------
    def _run_query_with_retry(
        self,
        query: str,
        job_config: bigquery.QueryJobConfig | None = None,
        max_retries: int = 3,
    ) -> QueryJob:
        """Execute a BigQuery query with exponential backoff retry."""
        delay = 2
        client = self._get_client()

        for attempt in range(max_retries):
            try:
                return client.query(query, job_config=job_config)

            except GoogleAPIError as e:
                self.logger.warning(
                    "Query failed (attempt %d/%d): %s",
                    attempt + 1,
                    max_retries,
                    e,
                )

                if attempt == max_retries - 1:
                    self.logger.error("Max retries reached. Aborting.")
                    raise

                time.sleep(delay)
                delay *= 2

        # Should be unreachable
        raise RuntimeError("Unreachable: retries exhausted without raising.")

    # -------------------------------------------------------
    # Main Collection Logic (PURE)
    # -------------------------------------------------------
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
            with output_path.open("r", encoding="utf-8") as fh:
                n_existing = sum(1 for line in fh if line.strip())
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

        documents: list[dict] = []
        seen_hashes: set[str] = set()

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
                # Dry run (cost protection)
                # -------------------------
                dry_cfg = bigquery.QueryJobConfig(
                    dry_run=True,
                    use_query_cache=False,
                )

                dry_job = self._run_query_with_retry(query, dry_cfg)

                # Handle mocked jobs safely (tests may not define total_bytes_processed)
                total_bytes = getattr(dry_job, "total_bytes_processed", 0)

                # If it's a MagicMock or non-numeric, treat as 0
                if not isinstance(total_bytes, int | float):
                    total_bytes = 0

                gb_scanned = total_bytes / (1024**3)

                self.logger.info(
                    "Dry run for %s: %.4f GB scanned",
                    start_str,
                    gb_scanned,
                )

                if gb_scanned > 5:
                    raise RuntimeError(f"Query too expensive ({gb_scanned:.2f} GB).")

                # -------------------------
                # Execute real query
                # -------------------------
                job = self._run_query_with_retry(query)
                df = job.result().to_dataframe(create_bqstorage_client=False)

                for row in df.to_dict("records"):
                    raw_date = row.get("DATE")

                    timestamp_published = None
                    if raw_date:
                        try:
                            # GDELT DATE format is usually YYYYMMDDHHMMSS
                            dt = datetime.strptime(str(raw_date), "%Y%m%d%H%M%S")
                            timestamp_published = dt.isoformat() + "Z"
                        except Exception:
                            # Fallback: store as string
                            timestamp_published = str(raw_date)

                    url = row.get("DocumentIdentifier")
                    if not url:
                        continue

                    url_hash = hashlib.sha256(str(url).encode("utf-8")).hexdigest()
                    if url_hash in seen_hashes:
                        continue

                    seen_hashes.add(url_hash)

                    source_domain = row.get("SourceCommonName")

                    documents.append(
                        {
                            "source": self.SOURCE_NAME,
                            "timestamp_collected": datetime.now(timezone.utc).isoformat(),
                            "timestamp_published": timestamp_published,
                            "url": url,
                            "source_domain": source_domain,
                            "v2tone": str(row.get("V2Tone") or ""),
                            "themes": str(row.get("Themes") or ""),
                            "locations": str(row.get("Locations") or ""),
                            "organizations": str(row.get("Organizations") or ""),
                        }
                    )

                current_day += timedelta(days=1)

            if not documents:
                return {"aggregated": 0}

            def _credibility_tier(domain: str | None) -> int:
                lowered = (domain or "").lower()
                if any(d in lowered for d in self.TIER_1):
                    return 1
                if any(d in lowered for d in self.TIER_2):
                    return 2
                return 3

            documents.sort(
                key=lambda d: (
                    _credibility_tier(d.get("source_domain")),
                    d.get("timestamp_published", ""),
                    hashlib.sha256(str(d.get("url", "")).encode("utf-8")).hexdigest(),
                )
            )

            with output_path.open("w", encoding="utf-8") as fh:
                for record in documents:
                    fh.write(json.dumps(record, ensure_ascii=False) + "\n")

            self.logger.info("Collected %d documents -> %s", len(documents), output_path)

            return {"aggregated": len(documents)}

        except GoogleAPIError as e:
            self.logger.error("GDELT query failed: %s", e)
            raise

    # -------------------------------------------------------
    # Health Check
    # -------------------------------------------------------
    def health_check(self) -> bool:
        try:
            client = self._get_client()
            job = client.query("SELECT 1")
            _ = job.result()
            return True
        except GoogleAPIError:
            return False
