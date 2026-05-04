"""APScheduler integration for collection orchestration and inference.

Manages:
- Collection jobs: one per enabled source, on configured interval
- Inference job: daily cron trigger with configured schedule

Uses AsyncIOScheduler with threadpool executor for sync collectors.
"""

from datetime import datetime

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from src.ingestion.orchestrator import CollectionOrchestrator
from src.shared.config.sources import SourcesConfig
from src.shared.utils import setup_logger


class SchedulerService:
    """Manages collection and inference scheduling with APScheduler."""

    def __init__(self, config: SourcesConfig, orchestrator: CollectionOrchestrator) -> None:
        """Initialize scheduler service.

        Args:
            config: SourcesConfig loaded from sources.yaml
            orchestrator: CollectionOrchestrator instance
        """
        self.config = config
        self.orchestrator = orchestrator
        self.logger = setup_logger(self.__class__.__name__)

        # Create AsyncIOScheduler for event loop integration
        self.scheduler = AsyncIOScheduler()
        self.scheduler.configure({"apscheduler.timezone": "UTC"})

    def start(self) -> None:
        """Register and start all jobs.

        Registers:
        - Collection jobs: one per enabled source with IntervalTrigger
        - Inference job: CronTrigger using config.inference.schedule_cron

        All collector jobs run in threadpool to avoid blocking async event loop.
        """
        self.logger.info("Starting scheduler service")

        # Get current time in UTC for trigger calculations
        utc_now = datetime.now(pytz.utc)

        # Register collection jobs for each enabled source
        for source_id, source_config in self.config.sources.items():
            if not source_config.enabled:
                self.logger.debug(f"Source {source_id} disabled, skipping job registration")
                continue

            job_id = f"collect_{source_id}"
            trigger = IntervalTrigger(hours=source_config.interval_hours)

            self.scheduler.add_job(
                func=self._run_collection_job,
                trigger=trigger,
                args=[source_id],
                id=job_id,
                name=f"Collection: {source_id}",
                executor="threadpool",
                replace_existing=True,
            )

            next_fire_time = trigger.get_next_fire_time(None, utc_now)
            self.logger.info(
                f"Registered collection job: {source_id} "
                f"(interval={source_config.interval_hours}h, next_fire={next_fire_time})"
            )

        # Register inference job with cron trigger
        inference_cron = self.config.inference.schedule_cron
        inference_job_id = "infer_daily"
        cron_trigger = CronTrigger.from_crontab(inference_cron)

        self.scheduler.add_job(
            func=self._run_inference_job,
            trigger=cron_trigger,
            args=[self.config.inference.dry_run],
            id=inference_job_id,
            name="Inference: Daily",
            executor="threadpool",
            replace_existing=True,
        )

        next_fire_time = cron_trigger.get_next_fire_time(None, utc_now)
        self.logger.info(
            f"Registered inference job: cron='{inference_cron}', "
            f"next_fire={next_fire_time}, dry_run={self.config.inference.dry_run}"
        )

        # Start the scheduler
        self.scheduler.start()
        self.logger.info("Scheduler started successfully")

    def shutdown(self) -> None:
        """Shut down the scheduler gracefully."""
        if self.scheduler.running:
            self.logger.info("Shutting down scheduler")
            self.scheduler.shutdown(wait=True)
            self.logger.info("Scheduler shut down successfully")

    def _run_collection_job(self, source_id: str) -> None:
        """Execute a collection job for a source.

        Args:
            source_id: Source identifier
        """
        self.logger.info(f"Collection job started: {source_id}")
        try:
            result = self.orchestrator.run_source(source_id)
            if result.error:
                self.logger.error(f"Collection job failed for {source_id}: {result.error}")
            else:
                self.logger.info(
                    f"Collection job completed for {source_id}: "
                    f"rows_written={result.rows_written}, backfill={result.backfill_performed}"
                )
        except Exception as e:
            self.logger.error(f"Collection job exception for {source_id}: {e}", exc_info=True)

    def _run_inference_job(self, dry_run: bool) -> None:
        """Execute the daily inference job.

        Imports InferencePipeline inside method to avoid circular import at module load time.

        Args:
            dry_run: If True, skip DB writes
        """
        self.logger.info(f"Inference job started (dry_run={dry_run})")
        try:
            # Local import to avoid circular dependency at module load
            from src.inference.pipeline import InferencePipeline

            pipeline = InferencePipeline()
            result = pipeline.run(dry_run=dry_run)

            self.logger.info(
                f"Inference job completed: "
                f"target_date={result.target_date}, "
                f"top_pick={result.report.top_pick}, "
                f"stale_sources={len(result.stale_warnings)}, "
                f"db_rows={result.db_rows}"
            )

            if result.stale_warnings:
                self.logger.warning(f"Stale sources detected: {list(result.stale_warnings.keys())}")

        except Exception as e:
            self.logger.error(f"Inference job exception: {e}", exc_info=True)
