"""Unified abstract base class for streaming collectors."""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path

from src.shared.utils import setup_logger


class BaseCollector(ABC):
    SOURCE_NAME: str
    DEFAULT_LOOKBACK_DAYS: int

    def __init__(self, output_dir: Path, log_file: Path | None = None) -> None:
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger = setup_logger(self.__class__.__name__, log_file)

    @abstractmethod
    def collect(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        backfill: bool = False,
    ) -> dict[str, int]: ...

    @abstractmethod
    def health_check(self) -> bool: ...

    @staticmethod
    def _to_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
