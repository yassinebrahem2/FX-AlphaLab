"""Collectors package.

Must be safe to import even if optional deps (selenium, uc, etc.) aren't installed.
"""

from src.ingestion.collectors.base_collector import BaseCollector

__all__ = ["BaseCollector"]

def _optional(module_path: str, name: str):
    try:
        module = __import__(module_path, fromlist=[name])
        globals()[name] = getattr(module, name)
        __all__.append(name)
    except ImportError:
        # Optional dependency missing in this environment -> skip
        pass

_optional("src.ingestion.collectors.ecb_collector", "ECBCollector")
_optional("src.ingestion.collectors.forexfactory_collector", "ForexFactoryCalendarCollector")
_optional("src.ingestion.collectors.fred_collector", "FREDCollector")
_optional("src.ingestion.collectors.mt5_collector", "MT5Collector")
_optional("src.ingestion.collectors.google_trends_collector", "GoogleTrendsCollector")

_optional("src.ingestion.collectors.ecb_scraper_collector", "ECBScraperCollector")
_optional("src.ingestion.collectors.fed_scraper_collector", "FedScraperCollector")
_optional("src.ingestion.collectors.boe_scraper_collector", "BoEScraperCollector")
