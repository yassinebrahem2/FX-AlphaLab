"""Data preprocessors for Bronze → Silver transformation."""

from src.ingestion.preprocessors.base_preprocessor import BasePreprocessor
from src.ingestion.preprocessors.calendar_parser import CalendarPreprocessor
from src.ingestion.preprocessors.document_preprocessor import DocumentPreprocessor
from src.ingestion.preprocessors.macro_normalizer import MacroNormalizer
from src.ingestion.preprocessors.price_normalizer import PriceNormalizer

try:
    from src.ingestion.preprocessors.news_preprocessor import NewsPreprocessor
except ImportError:
    NewsPreprocessor = None  # type: ignore[assignment,misc]

__all__ = [
    "BasePreprocessor",
    "CalendarPreprocessor",
    "DocumentPreprocessor",
    "MacroNormalizer",
    "NewsPreprocessor",
    "PriceNormalizer",
]
