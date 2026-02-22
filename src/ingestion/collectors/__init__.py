"""Data collectors for various sources."""

from src.ingestion.collectors.base_collector import BaseCollector
from src.ingestion.collectors.boe_collector import BoECollector
from src.ingestion.collectors.boe_scraper_collector import BoEScraperCollector
from src.ingestion.collectors.document_collector import DocumentCollector
from src.ingestion.collectors.ecb_collector import ECBCollector
from src.ingestion.collectors.ecb_news_collector import ECBNewsCollector
from src.ingestion.collectors.ecb_scraper_collector import ECBScraperCollector
from src.ingestion.collectors.fed_collector import FedCollector
from src.ingestion.collectors.fed_scraper_collector import FedScraperCollector
from src.ingestion.collectors.forexfactory_collector import ForexFactoryCalendarCollector
from src.ingestion.collectors.fred_collector import FREDCollector
from src.ingestion.collectors.mt5_collector import MT5Collector
from src.ingestion.collectors.stocktwits_collector import StocktwitsCollector

__all__ = [
    "BaseCollector",
    "BoECollector",
    "BoEScraperCollector",
    "DocumentCollector",
    "ECBCollector",
    "ECBNewsCollector",
    "ECBScraperCollector",
    "FedCollector",
    "FedScraperCollector",
    "FREDCollector",
    "ForexFactoryCalendarCollector",
    "MT5Collector",
    "StocktwitsCollector",
]
