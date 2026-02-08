"""ECB Data Collector using the official ECB Data Portal API (SDMX 2.1 REST).

Collects:
    - ECB key policy rates (deposit facility, main refinancing operations)
    - EUR daily reference exchange rates (USD, GBP, JPY, CHF)

Supports full and incremental (updatedAfter) collection for exchange rates.
Note: policy rates (FM dataflow) do not support incremental updates.

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

from data.ingestion.base_collector import BaseCollector
from shared.config import Config


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
    Raw output is stored in datasets/ecb/raw/ following §3.1.
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

    _RATE_CODE_MAP: dict[str, str] = {
        "DFR": "Deposit Facility Rate",
        "MRR_FR": "Main Refinancing Operations Rate",
        "MRR_MBR": "Marginal Lending Facility Rate",
    }

    _VALID_FX_CURRENCIES: frozenset[str] = frozenset({"USD", "GBP", "JPY", "CHF"})

    def __init__(
        self,
        output_dir: Path | None = None,
        log_file: Path | None = None,
    ) -> None:
        super().__init__(
            output_dir=output_dir or Config.DATA_DIR / "datasets" / "ecb" / "raw",
            log_file=log_file or Config.LOGS_DIR / "ecb_collector.log",
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
        """Collect policy rates and exchange rates for the given date range.

        Args:
            start_date: Start of range (default: 2 years ago).
            end_date: End of range (default: today).

        Returns:
            {"policy_rates": DataFrame, "exchange_rates": DataFrame}
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
        """Collect ECB key policy rates.

        The FM (event-based) dataflow does not support incremental updates
        via updatedAfter; the full range is always fetched.

        Args:
            start_date: Start date (default: 2 years ago).
            end_date: End date (default: today).

        Returns:
            DataFrame[date, rate_type, rate, frequency, unit, source]
        """
        start = start_date or datetime.now() - timedelta(days=730)
        end = end_date or datetime.now()
        raw = self._fetch(
            self.POLICY_RATES,
            start_period=start.strftime("%Y-%m-%d"),
            end_period=end.strftime("%Y-%m-%d"),
        )
        return self._process_policy_rates(raw)

    def collect_exchange_rates(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        updated_after: datetime | None = None,
    ) -> pd.DataFrame:
        """Collect EUR reference exchange rates.

        Args:
            start_date: Start date (default: 2 years ago).
            end_date: End date (default: today).
            updated_after: ISO 8601 timestamp; if set, only revisions since
                this time are returned (incremental mode).

        Returns:
            DataFrame[date, currency_pair, rate, frequency, source]
        """
        start = start_date or datetime.now() - timedelta(days=730)
        end = end_date or datetime.now()
        raw = self._fetch(
            self.EXCHANGE_RATES,
            start_period=start.strftime("%Y-%m-%d"),
            end_period=end.strftime("%Y-%m-%d"),
            updated_after=updated_after,
        )
        return self._process_exchange_rates(raw)

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
                raise ValueError(
                    f"Invalid ECB dataset: {dataset.dataflow}/{dataset.key}"
                ) from exc
            raise

        if not response.content:
            self.logger.warning("Empty response body for %s", dataset.name)
            return pd.DataFrame()

        df = pd.read_csv(io.BytesIO(response.content), dtype=str)
        self.logger.info("Received %d rows for %s", len(df), dataset.name)
        return df

    # ------------------------------------------------------------------
    # Private: processing layer
    # ------------------------------------------------------------------

    def _process_policy_rates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize raw ECB policy rates into the §3.1 output schema.

        Args:
            df: Raw DataFrame from _fetch (all columns are strings).

        Returns:
            DataFrame[date, rate_type, rate, frequency, unit, source]
        """
        _empty = pd.DataFrame(
            columns=["date", "rate_type", "rate", "frequency", "unit", "source"]
        )
        if df.empty:
            return _empty

        relevant = df[df["PROVIDER_FM_ID"].isin(self._RATE_CODE_MAP)].copy()
        if relevant.empty:
            self.logger.warning("No recognised rate codes in policy rates response")
            return _empty

        result = pd.DataFrame(
            {
                "date": pd.to_datetime(relevant["TIME_PERIOD"]).dt.date,
                "rate_type": relevant["PROVIDER_FM_ID"].map(self._RATE_CODE_MAP),
                "rate": pd.to_numeric(relevant["OBS_VALUE"], errors="coerce"),
                "frequency": relevant["FREQ"] if "FREQ" in relevant.columns else "B",
                "unit": "Percent",
                "source": "ECB",
            }
        )
        result = result.dropna(subset=["rate"]).sort_values("date").reset_index(drop=True)
        self.logger.info("Processed %d policy rate records", len(result))
        return result

    def _process_exchange_rates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize raw ECB exchange rates into the §3.1 output schema.

        Args:
            df: Raw DataFrame from _fetch (all columns are strings).

        Returns:
            DataFrame[date, currency_pair, rate, frequency, source]
        """
        _empty = pd.DataFrame(columns=["date", "currency_pair", "rate", "frequency", "source"])
        if df.empty:
            return _empty

        relevant = df[df["CURRENCY"].isin(self._VALID_FX_CURRENCIES)].copy()
        if relevant.empty:
            self.logger.warning("No valid currencies in exchange rates response")
            return _empty

        result = pd.DataFrame(
            {
                "date": pd.to_datetime(relevant["TIME_PERIOD"]).dt.date,
                "currency_pair": "EUR/" + relevant["CURRENCY"],
                "rate": pd.to_numeric(relevant["OBS_VALUE"], errors="coerce"),
                "frequency": relevant["FREQ"] if "FREQ" in relevant.columns else "D",
                "source": "ECB",
            }
        )
        result = (
            result.dropna(subset=["rate"])
            .sort_values(["date", "currency_pair"])
            .reset_index(drop=True)
        )
        self.logger.info("Processed %d exchange rate records", len(result))
        return result


def main() -> None:
    """CLI entry point for ECB data collection."""
    import argparse

    parser = argparse.ArgumentParser(description="Collect ECB data via SDMX 2.1 REST API")
    parser.add_argument("--output-dir", type=Path, help="Override output directory")
    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--updated-after",
        type=str,
        help="Incremental mode: fetch only data updated after this ISO 8601 timestamp",
    )
    parser.add_argument("--no-export", action="store_true", help="Skip CSV export")
    args = parser.parse_args()

    start = datetime.fromisoformat(args.start_date) if args.start_date else None
    end = datetime.fromisoformat(args.end_date) if args.end_date else None
    updated_after = datetime.fromisoformat(args.updated_after) if args.updated_after else None

    collector = ECBCollector(output_dir=args.output_dir)
    results = (
        collector.incremental_update(updated_after)
        if updated_after
        else collector.collect(start, end)
    )

    if not args.no_export:
        for name, df in results.items():
            if not df.empty:
                collector.export_csv(df, name)

    print("\nECB Data Collection Summary")
    print("=" * 40)
    for name, df in results.items():
        print(f"  {name}: {len(df)} records")


if __name__ == "__main__":
    main()
