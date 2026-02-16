"""Data ingestion module - collectors, preprocessors, and repositories."""

from src.ingestion.collectors import (
    BaseCollector,
    ECBCollector,
    ForexFactoryCalendarCollector,
    FREDCollector,
    MT5Collector,
)

__all__ = [
    "BaseCollector",
    "ECBCollector",
    "ForexFactoryCalendarCollector",
    "FREDCollector",
    "MT5Collector",
]
