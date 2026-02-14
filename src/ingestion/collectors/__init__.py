"""Data collectors for various sources."""

from src.ingestion.collectors.base_collector import BaseCollector
from src.ingestion.collectors.boe_collector import BoECollector
from src.ingestion.collectors.document_collector import DocumentCollector
from src.ingestion.collectors.ecb_collector import ECBCollector
from src.ingestion.collectors.ecb_news_collector import ECBNewsCollector
from src.ingestion.collectors.forexfactory_collector import ForexFactoryCalendarCollector
from src.ingestion.collectors.fred_collector import FREDCollector
from src.ingestion.collectors.mt5_collector import MT5Collector

__all__ = [
    "BaseCollector",
    "BoECollector",
    "DocumentCollector",
    "ECBCollector",
    "ECBNewsCollector",
    "FREDCollector",
    "ForexFactoryCalendarCollector",
    "MT5Collector",
]
