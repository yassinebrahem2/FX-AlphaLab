"""Test currency normalization in calendar pipeline."""

import pytest
from pathlib import Path
from src.ingestion.preprocessors.calendar_parser import CalendarPreprocessor


class TestCurrencyNormalization:
    """Verify European country codes map to EUR."""

    @pytest.fixture
    def preprocessor(self, tmp_path):
        """Create preprocessor with temp output dir."""
        return CalendarPreprocessor(
            input_dir=tmp_path / "input",
            output_dir=tmp_path / "output",
        )

    def test_eurozone_countries_map_to_eur(self, preprocessor):
        """Verify German, French, Italian, Spanish events map to EUR."""
        eurozone_events = [
            {"date": "2026-04-01", "time": "10:00", "event": "German WPI m/m", "currency": "n/a", "impact": "medium", "actual": "1.5", "forecast": "1.2", "previous": "1.0", "source": "test"},
            {"date": "2026-04-01", "time": "11:00", "event": "French GDP", "currency": "n/a", "impact": "high", "actual": "0.5", "forecast": "0.4", "previous": "0.3", "source": "test"},
            {"date": "2026-04-01", "time": "13:00", "event": "Italian CPI", "currency": "n/a", "impact": "medium", "actual": "2.1", "forecast": "2.0", "previous": "1.9", "source": "test"},
            {"date": "2026-04-01", "time": "14:00", "event": "Spanish PMI", "currency": "n/a", "impact": "low", "actual": "50", "forecast": "49", "previous": "48", "source": "test"},
        ]
        
        for event in eurozone_events:
            normalized = preprocessor._normalize_event(event)
            assert normalized["currency"] == "EUR", \
                f"Event '{event['event']}' should map to EUR, got {normalized['currency']}"

    def test_standard_currencies_unchanged(self, preprocessor):
        """Verify standard currency codes remain unchanged."""
        standard_events = [
            {"date": "2026-04-01", "time": "10:00", "event": "NFP", "currency": "US", "impact": "high", "actual": "250000", "forecast": "200000", "previous": "180000", "source": "test"},
            {"date": "2026-04-01", "time": "11:00", "event": "UK GDP", "currency": "GB", "impact": "high", "actual": "0.5", "forecast": "0.4", "previous": "0.3", "source": "test"},
            {"date": "2026-04-01", "time": "12:00", "event": "BoJ Rate Decision", "currency": "JP", "impact": "high", "actual": "0.25", "forecast": "0.25", "previous": "0.25", "source": "test"},
        ]
        
        expected_currencies = ["USD", "GBP", "JPY"]
        for event, expected in zip(standard_events, expected_currencies):
            normalized = preprocessor._normalize_event(event)
            assert normalized["currency"] == expected, \
                f"Event with currency={event['currency']} should map to {expected}, got {normalized['currency']}"

    def test_country_extraction_from_event_name(self, preprocessor):
        """Verify country extraction from event name (German → EUR, not DE)."""
        event = {
            "date": "2026-04-01",
            "time": "10:00",
            "event": "German Manufacturing PMI",
            "currency": "unknown",  # Raw doesn't specify currency
            "impact": "medium",
            "actual": "48",
            "forecast": "49",
            "previous": "50",
            "source": "test"
        }
        
        normalized = preprocessor._normalize_event(event)
        # Country should be extracted as "DE" from event name, then mapped to EUR
        assert normalized["currency"] == "EUR", \
            f"German event should map to EUR, got {normalized['currency']}"
