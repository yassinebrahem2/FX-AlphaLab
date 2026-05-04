"""Collection orchestrator for FX-AlphaLab.

Owns the logic of running a source's collector correctly given config.
Pure Python class with no FastAPI or APScheduler imports.
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from src.shared.config.sources import SourcesConfig
from src.shared.utils import setup_logger


@dataclass
class CollectionResult:
    """Result of a collection run."""

    source_id: str
    rows_written: int
    backfill_performed: bool
    error: str | None = None


class CollectionOrchestrator:
    """Orchestrates data collection for all sources given config."""

    def __init__(self, config: SourcesConfig, root: Path) -> None:
        """Initialize orchestrator.

        Args:
            config: SourcesConfig loaded from sources.yaml
            root: Project root directory (used to resolve data paths)
        """
        self.config = config
        self.root = root
        self.logger = setup_logger(
            self.__class__.__name__, log_file=root / "logs" / "collectors" / "orchestrator.log"
        )

        # Internal registry: source_id -> callable that runs the collector
        # Each callable takes (source_config, fetch_from) and returns rows written.
        self._registry: dict[str, Callable] = {
            "dukascopy_d1": self._collect_dukascopy_d1,
            "dukascopy_h4": self._collect_dukascopy_h4,
            "dukascopy_h1": self._collect_dukascopy_h1,
            "gdelt_events": self._collect_gdelt_events,
            "gdelt_gkg": self._collect_gdelt_gkg,
            "fred_macro": self._collect_fred_macro,
            "ecb_macro": self._collect_ecb_macro,
            "fed_documents": self._collect_fed_documents,
            "ecb_documents": self._collect_ecb_documents,
            "boe_documents": self._collect_boe_documents,
            "google_trends": self._collect_google_trends,
            "stocktwits": self._collect_stocktwits,
            "forex_factory": self._collect_forex_factory,
        }

    def close(self) -> None:
        """Close all logging file handlers to release file locks.

        Must be called explicitly in tests or when the orchestrator is no longer needed.
        On Windows, open file handles prevent temp directory cleanup.
        """
        for handler in list(self.logger.handlers):
            handler.close()
            self.logger.removeHandler(handler)

    def run_source(self, source_id: str) -> CollectionResult:
        """Run collection for a single source.

        Logic:
        1. Look up source config entry
        2. Check Silver: determine if minimum data exists
        3. If below minimum or fetch_from is set: run full backfill
        4. Otherwise: incremental fetch (2 days overlap for late-arriving data)
        5. Return CollectionResult with rows written, backfill flag, and any error

        Args:
            source_id: Source identifier (e.g., "fred_macro", "dukascopy_d1")

        Returns:
            CollectionResult with outcome
        """
        try:
            # Validate source exists in config
            if source_id not in self.config.sources:
                raise ValueError(
                    f"Unknown source: {source_id}. Available sources: "
                    f"{list(self.config.sources.keys())}"
                )

            if source_id not in self._registry:
                raise ValueError(
                    f"No collector registered for source: {source_id}. "
                    f"Registered sources: {list(self._registry.keys())}"
                )

            source_config = self.config.sources[source_id]
            if not source_config.enabled:
                self.logger.info(f"Source {source_id} is disabled, skipping collection")
                return CollectionResult(
                    source_id=source_id, rows_written=0, backfill_performed=False
                )

            self.logger.info(f"Running collection for source: {source_id}")

            # Check if Silver has minimum required data
            has_minimum = self._silver_has_minimum(source_id)
            should_backfill = not has_minimum or source_config.fetch_from is not None

            # Determine fetch window
            if should_backfill and source_config.fetch_from is not None:
                # Backfill from explicit anchor date to today
                fetch_from = datetime.fromisoformat(source_config.fetch_from).date()
                self.logger.info(f"{source_id}: Backfill from configured anchor date {fetch_from}")
                backfill_performed = True
            elif should_backfill and source_config.min_silver_days is not None:
                # Backfill from min_silver_days back to today
                fetch_from = (
                    datetime.utcnow() - timedelta(days=source_config.min_silver_days)
                ).date()
                self.logger.info(
                    f"{source_id}: Backfill required (below minimum {source_config.min_silver_days} days). "
                    f"Fetching from {fetch_from}"
                )
                backfill_performed = True
            elif should_backfill:
                # Backfill with no specific window (use collector default)
                self.logger.info(f"{source_id}: Backfill required but no min_silver_days set")
                backfill_performed = True
            else:
                # Incremental: last 2 days for overlap (handles late-arriving data)
                fetch_from = (datetime.utcnow() - timedelta(days=2)).date()
                self.logger.info(f"{source_id}: Incremental collection from {fetch_from}")
                backfill_performed = False

            # Call registered collector
            collector_func = self._registry[source_id]
            rows_written = collector_func(
                source_config=source_config, fetch_from=fetch_from if not should_backfill else None
            )

            self.logger.info(
                f"{source_id}: Completed successfully. "
                f"Rows written: {rows_written}, Backfill: {backfill_performed}"
            )

            return CollectionResult(
                source_id=source_id,
                rows_written=rows_written,
                backfill_performed=backfill_performed,
                error=None,
            )

        except Exception as e:
            self.logger.error(f"Collection failed for {source_id}: {e}", exc_info=True)
            return CollectionResult(
                source_id=source_id, rows_written=0, backfill_performed=False, error=str(e)
            )

    def _silver_has_minimum(self, source_id: str) -> bool:
        """Check if Silver has minimum required rows for a source.

        Parquet-based sources: count rows in existing Silver parquet files.
        Macro sources: check macro_signal.parquet date range.

        Args:
            source_id: Source identifier

        Returns:
            True if Silver exists and meets minimum, False otherwise
        """
        source_config = self.config.sources[source_id]
        min_days = source_config.min_silver_days

        if min_days is None:
            self.logger.debug(f"{source_id}: No minimum Silver window specified")
            return False

        try:
            # Map source to Silver parquet location
            silver_dir = self.root / "data" / "processed"

            if source_id.startswith("dukascopy_"):
                # OHLCV parquet files: ohlcv_{PAIR}_{TIMEFRAME}_{START}_{END}.parquet
                ohlcv_dir = silver_dir / "ohlcv"
                if not ohlcv_dir.exists():
                    self.logger.debug(f"{source_id}: OHLCV dir does not exist")
                    return False
                parquets = list(ohlcv_dir.glob("ohlcv_*.parquet"))
                if not parquets:
                    self.logger.debug(f"{source_id}: No OHLCV parquets found")
                    return False
                # Sum rows across all parquet files for this timeframe
                total_rows = 0
                for pf in parquets:
                    try:
                        count = len(pd.read_parquet(pf, columns=["timestamp_utc"]))
                        total_rows += count
                    except Exception as e:
                        self.logger.warning(f"Failed to read {pf}: {e}")
                        continue
                result = total_rows >= min_days
                self.logger.debug(
                    f"{source_id}: Found {total_rows} rows, minimum {min_days}, has_minimum={result}"
                )
                return result

            elif source_id.startswith("gdelt_"):
                # GDELT events/GKG parquet files
                if source_id == "gdelt_events":
                    events_dir = silver_dir / "events"
                    if not events_dir.exists():
                        self.logger.debug(f"{source_id}: Events dir does not exist")
                        return False
                    parquets = list(events_dir.glob("*.parquet"))
                elif source_id == "gdelt_gkg":
                    gkg_dir = silver_dir / "gkg"
                    if not gkg_dir.exists():
                        self.logger.debug(f"{source_id}: GKG dir does not exist")
                        return False
                    parquets = list(gkg_dir.glob("*.parquet"))
                else:
                    return False

                if not parquets:
                    self.logger.debug(f"{source_id}: No parquets found")
                    return False
                total_rows = 0
                for pf in parquets:
                    try:
                        count = len(pd.read_parquet(pf, columns=["timestamp_utc"]))
                        total_rows += count
                    except Exception as e:
                        self.logger.warning(f"Failed to read {pf}: {e}")
                        continue
                result = total_rows >= min_days
                self.logger.debug(
                    f"{source_id}: Found {total_rows} rows, minimum {min_days}, has_minimum={result}"
                )
                return result

            elif source_id == "stocktwits":
                # StockTwits sentiment parquets (partitioned by source/year/month)
                stocktwits_dir = silver_dir / "sentiment" / "source=stocktwits"
                if not stocktwits_dir.exists():
                    self.logger.debug(f"{source_id}: StockTwits sentiment dir does not exist")
                    return False
                parquets = list(stocktwits_dir.glob("**/*.parquet"))
                if not parquets:
                    self.logger.debug(f"{source_id}: No parquets found")
                    return False
                total_rows = 0
                for pf in parquets:
                    try:
                        count = len(pd.read_parquet(pf, columns=["timestamp_utc"]))
                        total_rows += count
                    except Exception as e:
                        self.logger.warning(f"Failed to read {pf}: {e}")
                        continue
                result = total_rows >= min_days
                self.logger.debug(
                    f"{source_id}: Found {total_rows} rows, minimum {min_days}, has_minimum={result}"
                )
                return result

            elif source_id == "google_trends":
                # Google Trends parquets
                trends_dir = silver_dir / "sentiment" / "source=google_trends"
                if not trends_dir.exists():
                    self.logger.debug(f"{source_id}: Google Trends dir does not exist")
                    return False
                parquets = list(trends_dir.glob("**/*.parquet"))
                if not parquets:
                    self.logger.debug(f"{source_id}: No parquets found")
                    return False
                total_rows = 0
                for pf in parquets:
                    try:
                        count = len(pd.read_parquet(pf, columns=["timestamp_utc"]))
                        total_rows += count
                    except Exception as e:
                        self.logger.warning(f"Failed to read {pf}: {e}")
                        continue
                result = total_rows >= min_days
                self.logger.debug(
                    f"{source_id}: Found {total_rows} rows, minimum {min_days}, has_minimum={result}"
                )
                return result

            elif source_id in (
                "fred_macro",
                "ecb_macro",
                "fed_documents",
                "ecb_documents",
                "boe_documents",
            ):
                # Macro data: check macro_signal.parquet
                macro_signal_path = silver_dir / "macro" / "macro_signal.parquet"
                if not macro_signal_path.exists():
                    self.logger.debug(f"{source_id}: macro_signal.parquet does not exist")
                    return False
                try:
                    df = pd.read_parquet(macro_signal_path, columns=["timestamp_utc"])
                    rows = len(df)
                    result = rows >= min_days
                    self.logger.debug(
                        f"{source_id}: Found {rows} rows in macro_signal, minimum {min_days}, has_minimum={result}"
                    )
                    return result
                except Exception as e:
                    self.logger.warning(f"Failed to read macro_signal.parquet: {e}")
                    return False

            elif source_id == "forex_factory":
                # ForexFactory events CSV
                events_dir = silver_dir / "events"
                if not events_dir.exists():
                    self.logger.debug(f"{source_id}: Events dir does not exist")
                    return False
                csvs = list(events_dir.glob("events_*.csv"))
                if not csvs:
                    self.logger.debug(f"{source_id}: No event CSVs found")
                    return False
                try:
                    # Read the latest CSV (lexicographically last = latest date range)
                    latest_csv = sorted(csvs)[-1]
                    df = pd.read_csv(latest_csv)
                    rows = len(df)
                    result = rows >= min_days
                    self.logger.debug(
                        f"{source_id}: Found {rows} rows, minimum {min_days}, has_minimum={result}"
                    )
                    return result
                except Exception as e:
                    self.logger.warning(f"Failed to read events CSV: {e}")
                    return False

            else:
                self.logger.warning(f"Unknown source type for Silver check: {source_id}")
                return False

        except Exception as e:
            self.logger.error(f"Error checking Silver minimum for {source_id}: {e}", exc_info=True)
            return False

    # ============================================================================
    # Collector stubs — each returns rows_written or raises NotImplementedError
    # ============================================================================

    def _collect_dukascopy_d1(self, source_config, fetch_from) -> int:  # pragma: no cover
        """Collect daily OHLCV data from Dukascopy."""
        raise NotImplementedError(
            "Dukascopy D1 collector not yet integrated. "
            "Awaiting DukascopyCollector interface finalization."
        )

    def _collect_dukascopy_h4(self, source_config, fetch_from) -> int:  # pragma: no cover
        """Collect 4-hour OHLCV data from Dukascopy."""
        raise NotImplementedError(
            "Dukascopy H4 collector not yet integrated. "
            "Awaiting DukascopyCollector interface finalization."
        )

    def _collect_dukascopy_h1(self, source_config, fetch_from) -> int:  # pragma: no cover
        """Collect 1-hour OHLCV data from Dukascopy."""
        raise NotImplementedError(
            "Dukascopy H1 collector not yet integrated. "
            "Awaiting DukascopyCollector interface finalization."
        )

    def _collect_gdelt_events(self, source_config, fetch_from) -> int:  # pragma: no cover
        """Collect GDELT event stream."""
        raise NotImplementedError(
            "GDELT Events collector not yet integrated. "
            "Awaiting GDELTEventsCollector interface finalization."
        )

    def _collect_gdelt_gkg(self, source_config, fetch_from) -> int:  # pragma: no cover
        """Collect GDELT Global Knowledge Graph."""
        raise NotImplementedError(
            "GDELT GKG collector not yet integrated. "
            "Awaiting GDELTGKGCollector interface finalization."
        )

    def _collect_fred_macro(self, source_config, fetch_from) -> int:  # pragma: no cover
        """Collect US Fed macro indicators (FRED)."""
        raise NotImplementedError(
            "FRED macro collector not yet integrated. "
            "Awaiting FREDMacroCollector interface finalization."
        )

    def _collect_ecb_macro(self, source_config, fetch_from) -> int:  # pragma: no cover
        """Collect ECB macro indicators."""
        raise NotImplementedError(
            "ECB macro collector not yet integrated. "
            "Awaiting ECBMacroCollector interface finalization."
        )

    def _collect_fed_documents(self, source_config, fetch_from) -> int:  # pragma: no cover
        """Collect Fed press releases and documents."""
        raise NotImplementedError(
            "Fed documents collector not yet integrated. "
            "Awaiting FedDocumentsCollector interface finalization."
        )

    def _collect_ecb_documents(self, source_config, fetch_from) -> int:  # pragma: no cover
        """Collect ECB press releases and documents."""
        raise NotImplementedError(
            "ECB documents collector not yet integrated. "
            "Awaiting ECBDocumentsCollector interface finalization."
        )

    def _collect_boe_documents(self, source_config, fetch_from) -> int:  # pragma: no cover
        """Collect BoE press releases and documents."""
        raise NotImplementedError(
            "BoE documents collector not yet integrated. "
            "Awaiting BoeDocumentsCollector interface finalization."
        )

    def _collect_google_trends(self, source_config, fetch_from) -> int:  # pragma: no cover
        """Collect Google Trends data."""
        raise NotImplementedError(
            "Google Trends collector not yet integrated. "
            "Awaiting GoogleTrendsCollector interface finalization."
        )

    def _collect_stocktwits(self, source_config, fetch_from) -> int:  # pragma: no cover
        """Collect StockTwits sentiment."""
        raise NotImplementedError(
            "StockTwits collector not yet integrated. "
            "Awaiting StockTwitsCollector interface finalization."
        )

    def _collect_forex_factory(self, source_config, fetch_from) -> int:  # pragma: no cover
        """Collect Forex Factory economic calendar."""
        raise NotImplementedError(
            "Forex Factory collector not yet integrated. "
            "Awaiting ForexFactoryCollector interface finalization."
        )
