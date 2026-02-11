from datetime import datetime, timedelta
from pathlib import Path
import json
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

from src.ingestion.collectors.base_collector import BaseCollector


class GDELTCollector(BaseCollector):
    """Collector for GDELT financial news data (Bronze layer)."""

    SOURCE_NAME = "news"

    def __init__(
        self,
        output_dir: Path,
        log_file: Path | None = None,
        project_id: str | None = None,
    ) -> None:
        super().__init__(output_dir=output_dir, log_file=log_file)

        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
        # Bronze subfolder: data/raw/news/gdelt/
        self.gdelt_dir = self.output_dir / "gdelt"
        self.gdelt_dir.mkdir(parents=True, exist_ok=True)

    def collect(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, pd.DataFrame]:
        """
        Collect minimal GDELT GKG data for a given date range.
        QUOTA-SAFE VERSION (partition-aware + dry-run).
        """

        if not start_date or not end_date:
            raise ValueError("start_date and end_date must be provided.")

        # 🔒 Dev restriction: only allow single-day queries
        if start_date.date() != end_date.date():
            raise ValueError("For development, query only ONE day at a time.")

        start_day = start_date.date()
        next_day = start_day + timedelta(days=1)

        start_str = start_day.isoformat()
        end_str = next_day.isoformat()

        # ✅ Correct dataset + partition filter
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

                -- FX Theme Filter
              AND (
        V2Themes LIKE '%ECON_CURRENCY%'
        OR V2Themes LIKE '%ECON_CENTRAL_BANK%'
      )

  -- Currency Mention Filter
              AND (
        V2Themes LIKE '%EUR%'
        OR V2Themes LIKE '%USD%'
        OR V2Themes LIKE '%GBP%'
        OR V2Themes LIKE '%JPY%'
      )
            LIMIT 10
        """

        try:
            # -------------------------
            # DRY RUN (cost estimation)
            # -------------------------
            job_config = bigquery.QueryJobConfig(
                dry_run=True,
                use_query_cache=False,
            )

            dry_job = self.client.query(query, job_config=job_config)

            bytes_scanned = dry_job.total_bytes_processed
            gb_scanned = bytes_scanned / (1024**3)

            self.logger.info("Dry run: query would scan %.4f GB", gb_scanned)

            # Allow up to 5GB during dev (safe)
            if gb_scanned > 5:
                raise RuntimeError(
                    f"Query too expensive ({gb_scanned:.2f} GB). Aborting."
                )

            # -------------------------
            # REAL EXECUTION
            # -------------------------
            job = self.client.query(query)
            results = job.result()
            df = results.to_dataframe()

            self.logger.info(
                "Fetched %d rows from GDELT for date %s",
                len(df),
                start_str,
            )

            # -------------------------
            # WRITE BRONZE JSONL
            # -------------------------
            output_file = self.gdelt_dir / f"aggregated_{start_day.strftime('%Y%m%d')}.jsonl"

            with open(output_file, "w", encoding="utf-8") as f:
                for _, row in df.iterrows():
                    record = {
                        "source": "GDELT",
                        "timestamp_collected": datetime.utcnow().isoformat(),
                        "timestamp_published": str(row["DATE"]),
                        "url": row["DocumentIdentifier"],
                        "source_domain": row["SourceCommonName"],
                        "tone": row["V2Tone"],
                        "themes": self._parse_field(row["Themes"]),
                        "locations": self._parse_field(row["Locations"]),
                        "organizations": self._parse_field(row["Organizations"]),
                        "metadata": {},
                    }

                    f.write(json.dumps(record) + "\n")

                self.logger.info("Bronze JSONL written to %s", output_file)

            return {"gdelt_raw": df}

        except GoogleAPIError as e:
            self.logger.error("GDELT query failed: %s", e)
            raise

    def _parse_field(self, field_value):
        """Convert semicolon-separated GDELT fields to list."""
        if not field_value:
            return []
        return [item.strip() for item in str(field_value).split(";")]


    def health_check(self) -> bool:
        """Verify BigQuery connectivity using a lightweight test query."""
        try:
            job = self.client.query("SELECT 1")
            _ = job.result()
            self.logger.info("BigQuery health check successful.")
            return True
        except GoogleAPIError as e:
            self.logger.error("BigQuery health check failed: %s", e)
            return False
