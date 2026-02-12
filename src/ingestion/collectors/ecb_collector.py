"""ECB Data Collector using the official ECB Data Portal API (SDMX 2.1 REST).

Bronze Layer: Collects RAW data from ECB SDMX API and stores in data/raw/ecb/.
This collector preserves all source fields without transformation (§3.1).

Collects:
    - ECB key policy rates (deposit facility, main refinancing operations)
    - EUR daily reference exchange rates (USD, GBP, JPY, CHF)

Supports full and incremental (updatedAfter) collection for exchange rates.
Note: policy rates (FM dataflow) do not support incremental updates.

Preprocessing (Bronze → Silver) is handled by:
    - macro_normalizer.py for policy rates
    - price_normalizer.py for exchange rates

API: https://data.ecb.europa.eu/help/api/data
"""

import io
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.ingestion.collectors.base_collector import BaseCollector
from src.shared.config import Config


@dataclass(frozen=True)
class ECBDataset:
    """Immutable descriptor for an ECB SDMX dataset."""

    name: str
    dataflow: str
    key: str
    description: str
    frequency: str  # D=Daily, B=Business/event-based


class ECBCollector(BaseCollector):
    """Collector for ECB policy rates and EUR exchange rates.

    Uses the official ECB SDMX 2.1 REST API with automatic retry logic.
    Raw output is stored in data/raw/ecb/ following §3.1 Bronze contract.

    Returns raw CSV data with:
    - All source fields preserved
    - Added `source="ecb"` column
    - No transformation or schema mapping
    """

    SOURCE_NAME = "ecb"

    BASE_URL = "https://data-api.ecb.europa.eu/service"
    DEFAULT_TIMEOUT = 30
    MAX_RETRIES = 3
    RETRY_BACKOFF = 2.0
    REQUEST_DELAY = 1.0  # politeness delay between consecutive API calls (seconds)

    POLICY_RATES = ECBDataset(
        name="policy_rates",
        dataflow="FM",
        key="B.U2.EUR.4F.KR.MRR_FR+DFR+MRR_MBR.LEV",
        description="ECB Key Interest Rates",
        frequency="B",
    )

    EXCHANGE_RATES = ECBDataset(
        name="exchange_rates",
        dataflow="EXR",
        key="D.USD+GBP+JPY+CHF.EUR.SP00.A",
        description="EUR Reference Exchange Rates",
        frequency="D",
    )

    def __init__(
        self,
        output_dir: Path | None = None,
        log_file: Path | None = None,
    ) -> None:
        super().__init__(
            output_dir=output_dir or Config.DATA_DIR / "raw" / "ecb",
            log_file=log_file or Config.LOGS_DIR / "collectors" / "ecb_collector.log",
        )
        self._session = self._create_session()
        self.logger.info("ECBCollector initialized, output_dir=%s", self.output_dir)

    # ------------------------------------------------------------------
    # BaseCollector interface
    # ------------------------------------------------------------------

    def collect(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Collect raw policy rates and exchange rates for the given date range.

        Returns raw DataFrames with all ECB source fields preserved + source column.

        Args:
            start_date: Start of range (default: 2 years ago).
            end_date: End of range (default: today).

        Returns:
            {"policy_rates": DataFrame, "exchange_rates": DataFrame}
            Each DataFrame has all original ECB columns + "source" column.
        """
        start = start_date or datetime.now() - timedelta(days=730)
        end = end_date or datetime.now()
        self.logger.info("Collecting ECB data %s to %s", start.date(), end.date())

        policy_rates = self.collect_policy_rates(start, end)
        time.sleep(self.REQUEST_DELAY)
        exchange_rates = self.collect_exchange_rates(start, end)

        self.logger.info(
            "Done — policy_rates=%d rows, exchange_rates=%d rows",
            len(policy_rates),
            len(exchange_rates),
        )
        return {"policy_rates": policy_rates, "exchange_rates": exchange_rates}

    def health_check(self) -> bool:
        """Check ECB API availability by requesting today's exchange rates."""
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            url = self._build_url(self.EXCHANGE_RATES, start_period=today, end_period=today)
            return self._session.get(url, timeout=10).ok
        except requests.exceptions.RequestException:
            return False

    # ------------------------------------------------------------------
    # ECB-specific collection methods
    # ------------------------------------------------------------------

    def collect_policy_rates(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> pd.DataFrame:
        """Collect raw ECB key policy rates.

        The FM (event-based) dataflow does not support incremental updates
        via updatedAfter; the full range is always fetched.

        Args:
            start_date: Start date (default: 2 years ago).
            end_date: End date (default: today).

        Returns:
            DataFrame with all ECB source columns + "source" column.
            No transformation applied - raw data only.
        """
        start = start_date or datetime.now() - timedelta(days=730)
        end = end_date or datetime.now()
        raw = self._fetch(
            self.POLICY_RATES,
            start_period=start.strftime("%Y-%m-%d"),
            end_period=end.strftime("%Y-%m-%d"),
        )
        # Add source column for Bronze layer
        if not raw.empty:
            raw["source"] = self.SOURCE_NAME
        return raw

    def collect_exchange_rates(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        updated_after: datetime | None = None,
    ) -> pd.DataFrame:
        """Collect raw EUR reference exchange rates.

        Args:
            start_date: Start date (default: 2 years ago).
            end_date: End date (default: today).
            updated_after: ISO 8601 timestamp; if set, only revisions since
                this time are returned (incremental mode).

        Returns:
            DataFrame with all ECB source columns + "source" column.
            No transformation applied - raw data only.
        """
        start = start_date or datetime.now() - timedelta(days=730)
        end = end_date or datetime.now()
        raw = self._fetch(
            self.EXCHANGE_RATES,
            start_period=start.strftime("%Y-%m-%d"),
            end_period=end.strftime("%Y-%m-%d"),
            updated_after=updated_after,
        )
        # Add source column for Bronze layer
        if not raw.empty:
            raw["source"] = self.SOURCE_NAME
        return raw

    def incremental_update(self, last_update: datetime) -> dict[str, pd.DataFrame]:
        """Fetch data updated since last_update.

        Exchange rates support incremental mode via updatedAfter; policy rates
        are always fetched in full (FM dataflow limitation).

        Args:
            last_update: Timestamp of the previous successful collection.

        Returns:
            {"policy_rates": DataFrame, "exchange_rates": DataFrame}
        """
        self.logger.info("Incremental update since %s", last_update)
        policy_rates = self.collect_policy_rates()
        time.sleep(self.REQUEST_DELAY)
        exchange_rates = self.collect_exchange_rates(updated_after=last_update)
        return {"policy_rates": policy_rates, "exchange_rates": exchange_rates}

    # ------------------------------------------------------------------
    # Private: HTTP layer
    # ------------------------------------------------------------------

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=self.MAX_RETRIES,
            backoff_factor=self.RETRY_BACKOFF,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _build_url(
        self,
        dataset: ECBDataset,
        start_period: str | None = None,
        end_period: str | None = None,
        updated_after: datetime | None = None,
    ) -> str:
        """Construct an ECB SDMX REST URL with query parameters."""
        params = ["format=csvdata"]
        if start_period:
            params.append(f"startPeriod={start_period}")
        if end_period:
            params.append(f"endPeriod={end_period}")
        if updated_after:
            ts = quote(updated_after.strftime("%Y-%m-%dT%H:%M:%S+00:00"), safe="")
            params.append(f"updatedAfter={ts}")
        return f"{self.BASE_URL}/data/{dataset.dataflow}/{dataset.key}?{'&'.join(params)}"

    def _fetch(
        self,
        dataset: ECBDataset,
        start_period: str | None = None,
        end_period: str | None = None,
        updated_after: datetime | None = None,
    ) -> pd.DataFrame:
        """Fetch raw CSV rows from the ECB SDMX API.

        Args:
            dataset: Target ECB dataset.
            start_period: YYYY-MM-DD start.
            end_period: YYYY-MM-DD end.
            updated_after: Revision cutoff for incremental updates.

        Returns:
            Raw DataFrame (all columns as strings).

        Raises:
            ValueError: Dataset key is invalid (HTTP 404).
            requests.exceptions.RequestException: Network / HTTP failure.
        """
        url = self._build_url(dataset, start_period, end_period, updated_after)
        self.logger.debug("GET %s", url)

        try:
            response = self._session.get(url, timeout=self.DEFAULT_TIMEOUT)
            response.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                raise ValueError(f"Invalid ECB dataset: {dataset.dataflow}/{dataset.key}") from exc
            raise

        if not response.content:
            self.logger.warning("Empty response body for %s", dataset.name)
            return pd.DataFrame()

        df = pd.read_csv(io.BytesIO(response.content), dtype=str)
        self.logger.info("Received %d rows for %s", len(df), dataset.name)
        return df
