"""Data ingestion collectors module."""

from src.ingestion.collectors.base_collector import BaseCollector
from src.ingestion.collectors.ecb_collector import ECBCollector
from src.ingestion.collectors.forex_factory_collector import (
    ForexFactoryCalendarCollector,
)
from src.ingestion.collectors.fred_collector import FREDCollector
from src.ingestion.collectors.mt5_collector import MT5Collector

__all__ = [
    "BaseCollector",
    "ECBCollector",
    "ForexFactoryCalendarCollector",
    "FREDCollector",
    "MT5Collector",
]