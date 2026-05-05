"""Collection orchestrator for FX-AlphaLab.

Owns the logic of running a source's collector correctly given config.
Pure Python class with no FastAPI or APScheduler imports.
"""

import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
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
            "dukascopy_m15": self._collect_dukascopy_m15,
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

    @staticmethod
    def _read_parquet_ts(pf: Path) -> pd.Series:
        """Read timestamp_utc from a parquet file regardless of index vs column layout."""
        df = pd.read_parquet(pf)
        if "timestamp_utc" in df.columns:
            return pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce").dropna()
        if df.index.name == "timestamp_utc":
            ts = pd.to_datetime(df.index, utc=True, errors="coerce")
            return ts[ts.notna()]
        raise KeyError("timestamp_utc")

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
            collector_result = collector_func(
                source_config=source_config, fetch_from=fetch_from if not should_backfill else None
            )
            if isinstance(collector_result, CollectionResult):
                result = collector_result
            else:
                result = CollectionResult(
                    source_id=source_id,
                    rows_written=collector_result,
                    backfill_performed=backfill_performed,
                    error=None,
                )

            # Prefer error-level logging when collector reported an error
            if result.error:
                self.logger.error(f"{source_id}: Collection failed — {result.error}")
            else:
                self.logger.info(
                    f"{source_id}: Completed successfully. "
                    f"Rows written: {result.rows_written}, Backfill: {result.backfill_performed}"
                )

            return result

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
                max_ts = None
                for pf in parquets:
                    try:
                        ts_series = self._read_parquet_ts(pf)
                        total_rows += len(ts_series)
                        ts = ts_series.max()
                        if max_ts is None or ts > max_ts:
                            max_ts = ts
                    except Exception as e:
                        self.logger.warning(f"Failed to read {pf}: {e}")
                        continue
                if total_rows < min_days:
                    return False
                if max_ts is None:
                    return False
                return self._check_recency(source_id, max_ts, source_config)

            elif source_id.startswith("gdelt_"):
                # GDELT events/GKG parquet files
                if source_id == "gdelt_events":
                    events_dir = silver_dir / "gdelt_events"
                    if not events_dir.exists():
                        self.logger.debug(f"{source_id}: Events dir does not exist")
                        return False
                    parquets = list(events_dir.glob("**/*.parquet"))
                elif source_id == "gdelt_gkg":
                    gkg_dir = silver_dir / "sentiment" / "source=gdelt"
                    if not gkg_dir.exists():
                        self.logger.debug(f"{source_id}: GKG dir does not exist")
                        return False
                    parquets = list(gkg_dir.glob("**/*.parquet"))
                else:
                    return False

                if not parquets:
                    self.logger.debug(f"{source_id}: No parquets found")
                    return False
                total_rows = 0
                max_ts = None
                for pf in parquets:
                    try:
                        ts_series = self._read_parquet_ts(pf)
                        total_rows += len(ts_series)
                        ts = ts_series.max()
                        if max_ts is None or ts > max_ts:
                            max_ts = ts
                    except Exception as e:
                        self.logger.warning(f"Failed to read {pf}: {e}")
                        continue
                if total_rows < min_days:
                    return False
                if max_ts is None:
                    return False
                return self._check_recency(source_id, max_ts, source_config)

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
                max_ts = None
                for pf in parquets:
                    try:
                        ts_series = self._read_parquet_ts(pf)
                        total_rows += len(ts_series)
                        ts = ts_series.max()
                        if max_ts is None or ts > max_ts:
                            max_ts = ts
                    except Exception as e:
                        self.logger.warning(f"Failed to read {pf}: {e}")
                        continue
                if total_rows < min_days:
                    return False
                if max_ts is None:
                    return False
                return self._check_recency(source_id, max_ts, source_config)

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
                max_ts = None
                for pf in parquets:
                    try:
                        ts_series = self._read_parquet_ts(pf)
                        total_rows += len(ts_series)
                        ts = ts_series.max()
                        if max_ts is None or ts > max_ts:
                            max_ts = ts
                    except Exception as e:
                        self.logger.warning(f"Failed to read {pf}: {e}")
                        continue
                if total_rows < min_days:
                    return False
                if max_ts is None:
                    return False
                return self._check_recency(source_id, max_ts, source_config)

            elif source_id in ("fred_macro", "ecb_macro"):
                # macro_all.parquet mtime == last successful MacroNormalizer run — no need to parse
                macro_all_path = silver_dir / "macro" / "macro_all.parquet"
                if not macro_all_path.exists():
                    return False
                staleness_limit = timedelta(hours=(source_config.interval_hours or 24) * 2)
                age_seconds = (
                    datetime.now(timezone.utc).timestamp() - macro_all_path.stat().st_mtime
                )
                return age_seconds <= staleness_limit.total_seconds()

            elif source_id in ("fed_documents", "ecb_documents", "boe_documents"):
                # Document sources: check macro/news partitioned layout (news_cleaned.parquet)
                source_name = source_id.replace("_documents", "")
                news_dir = silver_dir / "macro" / "news" / source_name
                if not news_dir.exists():
                    return False
                parquets = list(news_dir.glob("year=*/month=*/news_cleaned.parquet"))
                if not parquets:
                    return False
                total_rows = 0
                max_ts = None
                for pf in parquets:
                    try:
                        ts_series = self._read_parquet_ts(pf)
                        total_rows += len(ts_series)
                        ts = ts_series.max()
                        if max_ts is None or ts > max_ts:
                            max_ts = ts
                    except Exception as e:
                        self.logger.warning(f"Failed to read {pf}: {e}")
                        continue
                if total_rows < min_days:
                    return False
                if max_ts is None:
                    return False
                return self._check_recency(source_id, max_ts, source_config)

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
                    df_ts = pd.read_csv(latest_csv, usecols=["timestamp_utc"])
                    rows = len(df_ts)
                    if rows < min_days:
                        return False
                    max_ts = pd.to_datetime(df_ts["timestamp_utc"]).max()
                    if max_ts is None:
                        return False
                    return self._check_recency(source_id, max_ts, source_config)
                except Exception as e:
                    self.logger.warning(f"Failed to read events CSV: {e}")
                    return False

            else:
                self.logger.warning(f"Unknown source type for Silver check: {source_id}")
                return False

        except Exception as e:
            self.logger.error(f"Error checking Silver minimum for {source_id}: {e}", exc_info=True)
            return False

    def _check_recency(self, source_id: str, max_ts, source_config) -> bool:
        """Check if Silver data is recent enough.

        Args:
            source_id: Source identifier
            max_ts: Maximum timestamp from Silver data
            source_config: SourceConfig for this source

        Returns:
            True if data is recent enough, False if stale
        """
        staleness_limit = timedelta(hours=max(source_config.interval_hours * 3, 72))
        now_utc = datetime.now(timezone.utc)
        max_ts_aware = (
            max_ts if getattr(max_ts, "tzinfo", None) else max_ts.replace(tzinfo=timezone.utc)
        )

        if now_utc - max_ts_aware > staleness_limit:
            self.logger.info(
                f"{source_id}: Silver is stale (last={max_ts_aware.date()}, "
                f"staleness_limit={staleness_limit}), forcing backfill"
            )
            return False

        return True

    def _bronze_has_recent_data(
        self,
        bronze_dir: Path,
        staleness_limit: timedelta,
        glob_pattern: str = "**/*.parquet",
    ) -> bool:
        """True if bronze_dir has files modified within staleness_limit."""
        if not bronze_dir.exists():
            return False
        files = list(bronze_dir.glob(glob_pattern))
        if not files:
            return False
        latest_mtime = max(f.stat().st_mtime for f in files)
        age_seconds = datetime.now(timezone.utc).timestamp() - latest_mtime
        return age_seconds <= staleness_limit.total_seconds()

    def _write_news_parquets(self, bronze_dir: Path, output_dir: Path, source: str) -> int:
        """Read JSONL Bronze documents, extract text + timestamp, write partitioned parquets.

        Output layout: {output_dir}/year={YYYY}/month={MM}/news_cleaned.parquet
        Schema: [timestamp_utc, text, source]
        MacroSignalBuilder reads these for hawk/dove tone calculation.
        """
        import json

        jsonl_files = list(bronze_dir.glob("**/*.jsonl"))
        if not jsonl_files:
            self.logger.warning("%s: No JSONL files found in %s", source, bronze_dir)
            return 0

        records = []
        for path in jsonl_files:
            try:
                with open(path, encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        doc = json.loads(line)
                        ts_raw = doc.get("timestamp_published") or doc.get("timestamp_collected")
                        if not ts_raw:
                            continue
                        title = doc.get("title") or ""
                        content = doc.get("content") or doc.get("full_text") or ""
                        text = (title + ". " + content[:500]).strip(". ") if content else title
                        if not text:
                            continue
                        records.append({"timestamp_utc": ts_raw, "text": text, "source": source})
            except Exception as e:
                self.logger.warning("Failed to read %s: %s", path, e)

        if not records:
            return 0

        df = pd.DataFrame(records)
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
        df = df.dropna(subset=["timestamp_utc"]).drop_duplicates(subset=["timestamp_utc", "text"])
        df["year"] = df["timestamp_utc"].dt.year
        df["month"] = df["timestamp_utc"].dt.strftime("%m")

        rows_written = 0
        for (year, month), group in df.groupby(["year", "month"]):
            partition_path = output_dir / f"year={year}" / f"month={month}"
            partition_path.mkdir(parents=True, exist_ok=True)
            out = group.drop(columns=["year", "month"]).reset_index(drop=True)
            out.to_parquet(
                partition_path / "news_cleaned.parquet",
                index=False,
                engine="pyarrow",
                compression="snappy",
            )
            rows_written += len(out)

        self.logger.info("%s: Wrote %d news records to %s", source, rows_written, output_dir)
        return rows_written

    # ============================================================================
    # Collector stubs — each returns rows_written or raises NotImplementedError
    # ============================================================================

    def _collect_dukascopy(self, source_config, fetch_from) -> int:
        """Shared helper for all Dukascopy timeframes (D1, H4, H1).

        Args:
            source_config: SourceConfig for this collector
            fetch_from: None for backfill, or date to start from for incremental

        Returns:
            Total Silver rows written
        """
        from datetime import datetime as dt_class

        from src.ingestion.collectors.dukascopy_collector import DukascopyCollector
        from src.ingestion.preprocessors.dukascopy_preprocessor import DukascopyPreprocessor

        is_backfill = fetch_from is None

        # Set date range for collector
        if is_backfill:
            collector_start_date = None
            collector_end_date = None
        else:
            collector_start_date = dt_class.combine(fetch_from, time.min, tzinfo=timezone.utc)
            # End at yesterday UTC midnight (D1 bar must be closed)
            today_utc = dt_class.now(timezone.utc).date()
            collector_end_date = dt_class.combine(
                today_utc - timedelta(days=1), time.min, tzinfo=timezone.utc
            )

        # Collect Bronze 1-minute bars
        bronze_dir = self.root / "data" / "raw" / "dukascopy"
        staleness_limit = timedelta(hours=max(source_config.interval_hours * 3, 72))
        preprocessor_instance = DukascopyPreprocessor()
        instruments = preprocessor_instance.instruments

        collector = DukascopyCollector(output_dir=bronze_dir)
        if is_backfill:
            all_bronze_fresh = all(
                self._bronze_has_recent_data(bronze_dir / instr, staleness_limit)
                for instr in instruments
            )

            if all_bronze_fresh:
                self.logger.info(
                    "%s: Bronze is recent for all instruments - skipping network fetch, "
                    "reprocessing only",
                    source_config.description,
                )
            else:
                any_bronze_fresh = any(
                    self._bronze_has_recent_data(bronze_dir / instr, staleness_limit)
                    for instr in instruments
                )
                collector.collect(
                    start_date=collector_start_date,
                    end_date=collector_end_date,
                    backfill=not any_bronze_fresh,
                )
        else:
            collector.collect(
                start_date=collector_start_date,
                end_date=collector_end_date,
                backfill=False,
            )

        # Preprocess Bronze → Silver (all timeframes)
        # Always use backfill=True for preprocessor: Silver is rebuilt from all Bronze
        # This is cheap (resample only) and ensures freshness
        silver_results = preprocessor_instance.preprocess(backfill=True)

        # Count total rows across all instrument×timeframe combinations
        rows_written = sum(len(df) for df in silver_results.values())
        return rows_written

    def _collect_dukascopy_d1(self, source_config, fetch_from) -> int:  # pragma: no cover
        """Collect daily OHLCV data from Dukascopy."""
        return self._collect_dukascopy(source_config, fetch_from)

    def _collect_dukascopy_h4(self, source_config, fetch_from) -> int:  # pragma: no cover
        """Collect 4-hour OHLCV data from Dukascopy."""
        return self._collect_dukascopy(source_config, fetch_from)

    def _collect_dukascopy_h1(self, source_config, fetch_from) -> int:  # pragma: no cover
        """Collect 1-hour OHLCV data from Dukascopy."""
        return self._collect_dukascopy(source_config, fetch_from)

    def _collect_dukascopy_m15(self, source_config, fetch_from) -> int:  # pragma: no cover
        """Collect 15-minute OHLCV data from Dukascopy."""
        return self._collect_dukascopy(source_config, fetch_from)

    def _collect_gdelt_events(self, source_config, fetch_from) -> int:  # pragma: no cover
        """Collect GDELT event stream."""
        from datetime import datetime as dt_class

        from src.ingestion.collectors.gdelt_events_collector import GDELTEventsCollector
        from src.ingestion.preprocessors.gdelt_events_preprocessor import GDELTEventsPreprocessor

        bronze_dir = self.root / "data" / "raw" / "gdelt_events"
        silver_dir = self.root / "data" / "processed" / "gdelt_events"

        is_backfill = fetch_from is None

        # End date: yesterday UTC midnight (GDELT publishes previous day's complete file)
        today_utc = dt_class.now(timezone.utc).date()
        end_dt = dt_class.combine(today_utc - timedelta(days=1), time.min, tzinfo=timezone.utc)

        # Start date
        if is_backfill:
            lookback_days = (
                source_config.min_silver_days or GDELTEventsCollector.DEFAULT_LOOKBACK_DAYS
            )
            start_dt = end_dt - timedelta(days=lookback_days)
        else:
            start_dt = dt_class.combine(fetch_from, time.min, tzinfo=timezone.utc)

        staleness_limit = timedelta(hours=max(source_config.interval_hours * 3, 72))

        # Collect Bronze (only in backfill path)
        collector = GDELTEventsCollector(output_dir=bronze_dir)
        if is_backfill and self._bronze_has_recent_data(bronze_dir, staleness_limit):
            self.logger.info("gdelt_events: Bronze is recent - skipping network fetch")
        else:
            collector.collect(start_date=start_dt, end_date=end_dt, backfill=is_backfill)

        # Preprocess Bronze → Silver
        preprocessor = GDELTEventsPreprocessor(input_dir=bronze_dir, output_dir=silver_dir)
        silver_results = preprocessor.run(
            start_date=start_dt, end_date=end_dt, backfill=is_backfill
        )

        # Rebuild zone_features_daily.parquet from updated Silver events
        from src.ingestion.preprocessors.gdelt_zone_feature_builder import GDELTZoneFeatureBuilder

        zone_path = (
            self.root / "data" / "processed" / "geopolitical" / "zone_features_daily.parquet"
        )
        zone_path.parent.mkdir(parents=True, exist_ok=True)
        builder = GDELTZoneFeatureBuilder(clean_silver_dir=silver_dir, output_path=zone_path)
        builder.run(start_date=start_dt, end_date=end_dt, backfill=is_backfill)

        # Count total rows
        rows_written = sum(silver_results.values())
        return rows_written

    def _collect_gdelt_gkg(self, source_config, fetch_from) -> int:  # pragma: no cover
        """Collect GDELT Global Knowledge Graph."""
        from datetime import datetime as dt_class

        from src.ingestion.collectors.gdelt_gkg_collector import GDELTGKGCollector
        from src.ingestion.preprocessors.gdelt_gkg_preprocessor import GDELTGKGPreprocessor

        bronze_dir = self.root / "data" / "raw" / "gdelt_gkg"
        silver_dir = self.root / "data" / "processed" / "sentiment" / "source=gdelt"

        is_backfill = fetch_from is None

        # End date: yesterday UTC midnight
        today_utc = dt_class.now(timezone.utc).date()
        end_dt = dt_class.combine(today_utc - timedelta(days=1), time.min, tzinfo=timezone.utc)

        # Start date
        if is_backfill:
            lookback_days = source_config.min_silver_days or GDELTGKGCollector.DEFAULT_LOOKBACK_DAYS
            start_dt = end_dt - timedelta(days=lookback_days)
        else:
            start_dt = dt_class.combine(fetch_from, time.min, tzinfo=timezone.utc)

        # Get project_id from environment
        project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")

        staleness_limit = timedelta(hours=max(source_config.interval_hours * 3, 72))

        # Collect Bronze (only in backfill path)
        collector = GDELTGKGCollector(output_dir=bronze_dir, project_id=project_id)
        if is_backfill and self._bronze_has_recent_data(
            bronze_dir, staleness_limit, glob_pattern="**/*.jsonl"
        ):
            self.logger.info("gdelt_gkg: Bronze is recent - skipping network fetch")
        else:
            collector.collect(start_date=start_dt, end_date=end_dt, backfill=is_backfill)

        # Preprocess Bronze → Silver
        preprocessor = GDELTGKGPreprocessor(input_dir=bronze_dir, output_dir=silver_dir)
        silver_results = preprocessor.run(
            start_date=start_dt, end_date=end_dt, backfill=is_backfill
        )

        # Count total rows
        rows_written = sum(silver_results.values())
        return rows_written

    def _collect_fred_macro(
        self,
        source_config,
        fetch_from,
    ) -> CollectionResult:  # pragma: no cover
        """Collect US Fed macro indicators (FRED)."""
        try:
            from src.ingestion.collectors.fred_collector import FREDCollector
            from src.ingestion.preprocessors.macro_normalizer import MacroNormalizer
            from src.shared.config import Config

            # Determine date window (always timezone-aware UTC)
            end_dt = datetime.now(timezone.utc)
            if fetch_from is not None:
                # fetch_from is a date object from run_source() — convert to datetime
                start_dt = datetime(
                    fetch_from.year, fetch_from.month, fetch_from.day, tzinfo=timezone.utc
                )
            else:
                lookback_days = source_config.min_silver_days or int(
                    max(source_config.interval_hours * 3 / 24, 72 / 24)
                )
                start_dt = end_dt - timedelta(days=lookback_days)

            # Bronze directories and staleness
            fred_bronze = self.root / "data" / "raw" / "fred"
            ecb_bronze = self.root / "data" / "raw" / "ecb"
            staleness_limit = timedelta(hours=max(source_config.interval_hours * 3, 72))

            rows_written = 0

            # Collect missing Bronze sources as needed
            fred_ok = self._bronze_has_recent_data(fred_bronze, staleness_limit, "**/*.csv")
            ecb_ok = self._bronze_has_recent_data(ecb_bronze, staleness_limit, "**/*.csv")

            # If FRED Bronze missing, collect it
            if not fred_ok:
                collector = FREDCollector(
                    api_key=Config.FRED_API_KEY,
                    output_dir=fred_bronze,
                )
                fred_counts = collector.collect(start_date=start_dt, end_date=end_dt)
                # fred_counts may be dict(series_id -> DataFrame) or dict -> int
                if isinstance(fred_counts, dict):
                    for name, val in fred_counts.items():
                        if hasattr(val, "__len__") and not isinstance(val, int):
                            # Assume DataFrame-like
                            try:
                                collector.export_csv(val, name)
                            except Exception:
                                pass
                            rows_written += len(val)
                        else:
                            rows_written += int(val)
                else:
                    try:
                        rows_written += int(fred_counts)
                    except Exception:
                        pass
            else:
                self.logger.info("fred_macro: FRED Bronze is recent — skipping FRED network fetch")

            # If ECB Bronze missing, collect it
            if not ecb_ok:
                from src.ingestion.collectors.ecb_collector import ECBCollector

                ecb_collector = ECBCollector(output_dir=ecb_bronze)
                ecb_counts = ecb_collector.collect(start_date=start_dt, end_date=end_dt)
                if isinstance(ecb_counts, dict):
                    for name, val in ecb_counts.items():
                        if hasattr(val, "__len__") and not isinstance(val, int):
                            try:
                                ecb_collector.export_csv(val, name)
                            except Exception:
                                pass
                            rows_written += len(val)
                        else:
                            rows_written += int(val)
                else:
                    try:
                        rows_written += int(ecb_counts)
                    except Exception:
                        pass
            else:
                self.logger.info("fred_macro: ECB Bronze is recent — skipping ECB network fetch")

            # MacroNormalizer always reprocesses all Bronze — date range is for API fetching only
            normalizer = MacroNormalizer(
                input_dir=self.root / "data" / "raw",
                output_dir=self.root / "data" / "processed" / "macro",
                sources=["fred", "ecb"],
            )
            normalized = normalizer.preprocess(start_date=None, end_date=None, backfill=True)

            # Prefer reporting normalized rows when available (single consolidated result)
            if normalized:
                try:
                    norm_rows = sum(len(df) for df in normalized.values())
                    rows_written = int(norm_rows)
                except Exception:
                    # Fallback to Bronze-collected row counts
                    pass

            return CollectionResult(
                source_id="fred_macro",
                rows_written=rows_written,
                backfill_performed=fetch_from is None,
                error=None,
            )
        except Exception as e:
            return CollectionResult(
                source_id="fred_macro",
                rows_written=0,
                backfill_performed=fetch_from is None,
                error=str(e),
            )

    def _collect_ecb_macro(
        self,
        source_config,
        fetch_from,
    ) -> CollectionResult:  # pragma: no cover
        """Collect ECB macro indicators."""
        try:
            from src.ingestion.collectors.ecb_collector import ECBCollector
            from src.ingestion.preprocessors.macro_normalizer import MacroNormalizer

            # Determine date window (UTC-aware)
            end_dt = datetime.now(timezone.utc)
            if fetch_from is not None:
                start_dt = datetime(
                    fetch_from.year, fetch_from.month, fetch_from.day, tzinfo=timezone.utc
                )
            else:
                lookback_days = source_config.min_silver_days or int(
                    max(source_config.interval_hours * 3 / 24, 72 / 24)
                )
                start_dt = end_dt - timedelta(days=lookback_days)

            # Bronze directories and staleness
            fred_bronze = self.root / "data" / "raw" / "fred"
            ecb_bronze = self.root / "data" / "raw" / "ecb"
            staleness_limit = timedelta(hours=max(source_config.interval_hours * 3, 72))

            rows_written = 0

            fred_ok = self._bronze_has_recent_data(fred_bronze, staleness_limit, "**/*.csv")
            ecb_ok = self._bronze_has_recent_data(ecb_bronze, staleness_limit, "**/*.csv")

            # Collect missing sources
            if not fred_ok:
                from src.ingestion.collectors.fred_collector import FREDCollector

                fred_collector = FREDCollector(api_key=None, output_dir=fred_bronze)
                fred_counts = fred_collector.collect(start_date=start_dt, end_date=end_dt)
                rows_written += sum(fred_counts.values())
            else:
                self.logger.info("ecb_macro: FRED Bronze is recent — skipping FRED network fetch")

            if not ecb_ok:
                ecb_collector = ECBCollector(output_dir=ecb_bronze)
                ecb_counts = ecb_collector.collect(start_date=start_dt, end_date=end_dt)
                rows_written += sum(ecb_counts.values())
            else:
                self.logger.info("ecb_macro: ECB Bronze is recent — skipping ECB network fetch")

            normalizer = MacroNormalizer(
                input_dir=self.root / "data" / "raw",
                output_dir=self.root / "data" / "processed" / "macro",
                sources=["fred", "ecb"],
            )
            normalized = normalizer.preprocess(start_date=None, end_date=None, backfill=True)

            if normalized:
                try:
                    norm_rows = sum(len(df) for df in normalized.values())
                    rows_written = int(norm_rows)
                except Exception:
                    pass

            return CollectionResult(
                source_id="ecb_macro",
                rows_written=rows_written,
                backfill_performed=fetch_from is None,
                error=None,
            )
        except Exception as e:
            return CollectionResult(
                source_id="ecb_macro",
                rows_written=0,
                backfill_performed=fetch_from is None,
                error=str(e),
            )

    def _collect_fed_documents(
        self, source_config, fetch_from
    ) -> CollectionResult:  # pragma: no cover
        """Collect Fed press releases and documents.

        Simple Bronze JSONL -> partitioned macro/news news_cleaned.parquet writer.
        No FinBERT, no NewsPreprocessor.
        """
        try:
            from src.ingestion.collectors.fed_scraper_collector import FedScraperCollector

            source_name = "fed"
            bronze_dir = self.root / "data" / "raw" / "news" / source_name
            news_silver_dir = self.root / "data" / "processed" / "macro" / "news" / source_name
            staleness_limit = timedelta(hours=max(source_config.interval_hours * 3, 72))

            end_dt = datetime.now(timezone.utc)
            if fetch_from is not None:
                # Incremental: fetch_from is a date object from run_source()
                start_dt = datetime(
                    fetch_from.year, fetch_from.month, fetch_from.day, tzinfo=timezone.utc
                )
            else:
                # Backfill: derive from min_silver_days so scrapers get the full required window
                min_days = source_config.min_silver_days or 730
                start_dt = end_dt - timedelta(days=min_days + 30)

            docs_collected = 0

            if not self._bronze_has_recent_data(bronze_dir, staleness_limit, "**/*.jsonl"):
                collector = FedScraperCollector(output_dir=bronze_dir)
                data = collector.collect(
                    start_date=start_dt.replace(tzinfo=None),
                    end_date=end_dt.replace(tzinfo=None),
                )
                collector.export_all(data=data)
                docs_collected = sum(len(v) for v in data.values())
            else:
                self.logger.info("fed_documents: Bronze is recent — skipping network fetch")

            # Simple JSONL -> partitioned parquet extraction for MacroSignalBuilder
            rows_written = self._write_news_parquets(bronze_dir, news_silver_dir, source_name)

            return CollectionResult(
                source_id="fed_documents",
                rows_written=rows_written or docs_collected,
                backfill_performed=fetch_from is None,
                error=None,
            )
        except Exception as e:
            return CollectionResult(
                source_id="fed_documents",
                rows_written=0,
                backfill_performed=fetch_from is None,
                error=str(e),
            )

    def _collect_ecb_documents(
        self, source_config, fetch_from
    ) -> CollectionResult:  # pragma: no cover
        """Collect ECB press releases and documents (simple JSONL -> partitioned news)."""
        try:
            from src.ingestion.collectors.ecb_scraper_collector import ECBScraperCollector

            source_name = "ecb"
            bronze_dir = self.root / "data" / "raw" / "news" / source_name
            news_silver_dir = self.root / "data" / "processed" / "macro" / "news" / source_name
            staleness_limit = timedelta(hours=max(source_config.interval_hours * 3, 72))

            end_dt = datetime.now(timezone.utc)
            if fetch_from is not None:
                start_dt = datetime(
                    fetch_from.year, fetch_from.month, fetch_from.day, tzinfo=timezone.utc
                )
            else:
                min_days = source_config.min_silver_days or 730
                start_dt = end_dt - timedelta(days=min_days + 30)

            docs_collected = 0

            if not self._bronze_has_recent_data(bronze_dir, staleness_limit, "**/*.jsonl"):
                collector = ECBScraperCollector(output_dir=bronze_dir)
                data = collector.collect(
                    start_date=start_dt.replace(tzinfo=None),
                    end_date=end_dt.replace(tzinfo=None),
                )
                collector.export_all(data=data)
                docs_collected = sum(len(v) for v in data.values())
            else:
                self.logger.info("ecb_documents: Bronze is recent — skipping network fetch")

            rows_written = self._write_news_parquets(bronze_dir, news_silver_dir, source_name)

            return CollectionResult(
                source_id="ecb_documents",
                rows_written=rows_written or docs_collected,
                backfill_performed=fetch_from is None,
                error=None,
            )
        except Exception as e:
            return CollectionResult(
                source_id="ecb_documents",
                rows_written=0,
                backfill_performed=fetch_from is None,
                error=str(e),
            )

    def _collect_boe_documents(
        self, source_config, fetch_from
    ) -> CollectionResult:  # pragma: no cover
        """Collect BoE press releases and documents (simple JSONL -> partitioned news)."""
        try:
            from src.ingestion.collectors.boe_scraper_collector import BoEScraperCollector

            source_name = "boe"
            bronze_dir = self.root / "data" / "raw" / "news" / source_name
            news_silver_dir = self.root / "data" / "processed" / "macro" / "news" / source_name
            staleness_limit = timedelta(hours=max(source_config.interval_hours * 3, 72))

            end_dt = datetime.now(timezone.utc)
            if fetch_from is not None:
                start_dt = datetime(
                    fetch_from.year, fetch_from.month, fetch_from.day, tzinfo=timezone.utc
                )
            else:
                min_days = source_config.min_silver_days or 730
                start_dt = end_dt - timedelta(days=min_days + 30)

            docs_collected = 0

            if not self._bronze_has_recent_data(bronze_dir, staleness_limit, "**/*.jsonl"):
                collector = BoEScraperCollector(output_dir=bronze_dir)
                data = collector.collect(
                    start_date=start_dt.replace(tzinfo=None),
                    end_date=end_dt.replace(tzinfo=None),
                )
                collector.export_all(data=data)
                docs_collected = sum(len(v) for v in data.values())
            else:
                self.logger.info("boe_documents: Bronze is recent — skipping network fetch")

            rows_written = self._write_news_parquets(bronze_dir, news_silver_dir, source_name)

            return CollectionResult(
                source_id="boe_documents",
                rows_written=rows_written or docs_collected,
                backfill_performed=fetch_from is None,
                error=None,
            )
        except Exception as e:
            return CollectionResult(
                source_id="boe_documents",
                rows_written=0,
                backfill_performed=fetch_from is None,
                error=str(e),
            )

    def _collect_google_trends(
        self,
        source_config,
        fetch_from,
    ) -> CollectionResult:  # pragma: no cover
        """Collect Google Trends data."""
        try:
            from src.ingestion.collectors.google_trends_collector import GoogleTrendsCollector
            from src.ingestion.preprocessors.google_trends_preprocessor import (
                GoogleTrendsPreprocessor,
            )

            start_dt = datetime.fromisoformat(source_config.fetch_from).replace(tzinfo=timezone.utc)
            end_dt = datetime.now(timezone.utc)

            collector = GoogleTrendsCollector(
                output_dir=self.root / "data" / "raw" / "google_trends",
            )
            bronze_dir = self.root / "data" / "raw" / "google_trends"
            staleness_limit = timedelta(hours=max(source_config.interval_hours * 3, 72))

            if fetch_from is None and self._bronze_has_recent_data(
                bronze_dir, staleness_limit, glob_pattern="**/*.csv"
            ):
                self.logger.info("google_trends: Bronze is recent - skipping network fetch")
                rows_written = 0
            else:
                result_counts = collector.collect(start_date=start_dt, end_date=end_dt, force=True)
                rows_written = sum(result_counts.values())

            preprocessor = GoogleTrendsPreprocessor(
                input_dir=self.root / "data" / "raw" / "google_trends",
                output_dir=self.root / "data" / "processed" / "sentiment",
            )
            preprocessor.run(backfill=True)

            return CollectionResult(
                source_id="google_trends",
                rows_written=rows_written,
                backfill_performed=True,
                error=None,
            )
        except Exception as e:
            return CollectionResult(
                source_id="google_trends",
                rows_written=0,
                backfill_performed=True,
                error=str(e),
            )

    def _collect_stocktwits(self, source_config, fetch_from) -> int:  # pragma: no cover
        """Collect StockTwits sentiment."""
        raise NotImplementedError(
            "StockTwits collector not yet integrated. "
            "Awaiting StockTwitsCollector interface finalization."
        )

    def _collect_forex_factory(self, source_config, fetch_from) -> int:  # pragma: no cover
        """Collect Forex Factory economic calendar."""
        from datetime import datetime as dt_class

        from src.ingestion.collectors.forexfactory_collector import ForexFactoryCalendarCollector
        from src.ingestion.preprocessors.calendar_parser import CalendarPreprocessor

        bronze_dir = self.root / "data" / "raw" / "forexfactory"
        silver_dir = self.root / "data" / "processed" / "events"

        is_backfill = fetch_from is None
        staleness_limit = timedelta(hours=max(source_config.interval_hours * 3, 72))

        # ForexFactory uses naive datetimes (no tzinfo)
        if is_backfill:
            lookback_days = (
                source_config.min_silver_days or ForexFactoryCalendarCollector.DEFAULT_LOOKBACK_DAYS
            )
            start_dt = dt_class.utcnow() - timedelta(days=lookback_days)
        else:
            start_dt = dt_class.combine(fetch_from, time.min)

        end_dt = dt_class.utcnow()

        # Collect Bronze with try/finally to ensure driver cleanup
        collector = ForexFactoryCalendarCollector(output_dir=bronze_dir)
        rows_from_bronze = 0
        try:
            if is_backfill and self._bronze_has_recent_data(
                bronze_dir, staleness_limit, glob_pattern="**/*.csv"
            ):
                self.logger.info("forex_factory: Bronze is recent - skipping network fetch")
            else:
                # Bronze result: {"calendar": row_count}
                bronze_result = collector.collect(
                    start_date=start_dt,
                    end_date=end_dt,
                    backfill=True,
                )
                rows_from_bronze = bronze_result.get("calendar", 0)
        finally:
            collector.close()

        # Preprocess Bronze → Silver
        preprocessor = CalendarPreprocessor(input_dir=bronze_dir, output_dir=silver_dir)
        # Wrap naive datetimes with UTC for preprocessor
        start_dt_utc = start_dt.replace(tzinfo=timezone.utc)
        end_dt_utc = end_dt.replace(tzinfo=timezone.utc)
        preprocessor.preprocess(
            start_date=start_dt_utc,
            end_date=end_dt_utc,
            backfill=True,
        )

        # Contract: return Bronze calendar rows collected in this run.
        return rows_from_bronze
