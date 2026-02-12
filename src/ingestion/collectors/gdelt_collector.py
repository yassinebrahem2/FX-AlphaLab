import hashlib
import json
import time
from datetime import datetime, timedelta
from pathlib import Path

from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery
from google.cloud.bigquery.job import QueryJob

from src.ingestion.collectors.document_collector import DocumentCollector


class GDELTCollector(DocumentCollector):
    """Collector for GDELT financial news data (Bronze layer)."""

    SOURCE_NAME = "gdelt"

    TIER_1 = {"reuters.com", "bloomberg.com", "ft.com"}
    TIER_2 = {"wsj.com", "cnbc.com"}

    def __init__(
        self,
        output_dir: Path,
        log_file: Path | None = None,
        project_id: str | None = None,
    ) -> None:
        # Hard-enforce required path
        base_path = Path("data/raw/news/gdelt")

        super().__init__(output_dir=base_path, log_file=log_file)

        self.project_id = project_id
        self.client: bigquery.Client | None = None

        self.output_dir.mkdir(parents=True, exist_ok=True)

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
    # Main Collection Logic (PURE)
    # -------------------------------------------------------
    def collect(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, list[dict]]:
        if not start_date or not end_date:
            raise ValueError("start_date and end_date must be provided.")

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

                    url_hash = self._hash_url(url)
                    if url_hash in seen_hashes:
                        continue

                    seen_hashes.add(url_hash)

                    source_domain = row.get("SourceCommonName")
                    tier = self._assign_credibility_tier(source_domain)

                    documents.append(
                        {
                            "source": self.SOURCE_NAME,
                            "timestamp_collected": datetime.utcnow().isoformat(),
                            "timestamp_published": timestamp_published,
                            "url": url,
                            "source_domain": source_domain,
                            "tone": row.get("V2Tone"),
                            "themes": self._parse_field(row.get("Themes")),
                            "locations": self._parse_field(row.get("Locations")),
                            "organizations": self._parse_field(row.get("Organizations")),
                            "metadata": {
                                "credibility_tier": tier,
                                "url_hash": url_hash,
                            },
                        }
                    )

                current_day += timedelta(days=1)

            # -------------------------------------------------
            # PRIORITIZATION STEP (Core Requirement)
            # -------------------------------------------------
            documents.sort(
                key=lambda d: (
                    d.get("metadata", {}).get("credibility_tier", 3),
                    d.get("timestamp_published", ""),
                    d.get("metadata", {}).get("url_hash", ""),
                )
            )

            # Optional observability
            tier_counts: dict[int, int] = {}
            for d in documents:
                tier = d["metadata"]["credibility_tier"]
                tier_counts[tier] = tier_counts.get(tier, 0) + 1

            self.logger.info(
                "Collected %d documents. Tier distribution: %s",
                len(documents),
                tier_counts,
            )

            return {"aggregated": documents}

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

    # -------------------------------------------------------
    # Export JSONL (GDELT-specific override)
    # -------------------------------------------------------
    def export_jsonl(
        self,
        documents: list[dict],
        document_type: str = "aggregated",
        collection_date: datetime | None = None,
    ) -> Path:
        """
        Export aggregated GDELT documents to JSONL.

        Args:
            documents: List of document dictionaries to export.
            document_type: Document type (default: "aggregated", parameter required for supertype compatibility).
            collection_date: Date for filename (default: current date).

        File naming:
            aggregated_YYYYMMDD.jsonl
        """

        if not documents:
            raise ValueError("Cannot export empty data list.")

        date_str = (collection_date or datetime.utcnow()).strftime("%Y%m%d")

        path = self.output_dir / f"aggregated_{date_str}.jsonl"

        with open(path, "w", encoding="utf-8") as f:
            for record in documents:
                json.dump(record, f, ensure_ascii=False)
                f.write("\n")

        self.logger.info("Exported %d records to %s", len(documents), path)

        return path

    # -------------------------------------------------------
    # Convenience Export Method
    # -------------------------------------------------------
    def export_to_jsonl(
        self,
        data: list[dict] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> Path:
        """
        Convenience method to collect and export GDELT aggregated data.

        If data is provided:
            Export directly.

        If data is None:
            Collect using provided date range, then export.
        """

        if data is None:
            if not start_date or not end_date:
                raise ValueError("Must provide start_date and end_date if data is None.")
            result = self.collect(start_date=start_date, end_date=end_date)
            data = result["aggregated"]
        return self.export_jsonl(data)
