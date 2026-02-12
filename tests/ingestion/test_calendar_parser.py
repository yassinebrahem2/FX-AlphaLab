"""
Unit tests for Calendar Preprocessor (Bronze → Silver).

Tests Silver layer transformation including:
- Country code normalization (ISO 3166 alpha-2)
- Timestamp conversion (UTC ISO 8601)
- Numeric value parsing (K, M, B, T suffixes, percentages)
- Event ID generation
- Impact level normalization
- Validation rules
"""

import csv

import pandas as pd
import pytest

from src.ingestion.preprocessors.calendar_parser import CalendarPreprocessor


class TestCalendarPreprocessor:
    """Test cases for CalendarPreprocessor class."""

    @pytest.fixture
    def preprocessor(self, tmp_path):
        """Create a preprocessor instance with temp directories."""
        input_dir = tmp_path / "raw" / "calendar"
        output_dir = tmp_path / "processed" / "events"
        input_dir.mkdir(parents=True)
        output_dir.mkdir(parents=True)

        return CalendarPreprocessor(input_dir=input_dir, output_dir=output_dir)

    @pytest.fixture
    def sample_bronze_csv(self, tmp_path):
        """Create a sample Bronze layer CSV file."""
        input_dir = tmp_path / "raw" / "calendar"
        input_dir.mkdir(parents=True, exist_ok=True)

        csv_path = input_dir / "investing_economic_events_20240208.csv"

        # Write sample Bronze data
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "date",
                    "time",
                    "country",
                    "event",
                    "impact",
                    "actual",
                    "forecast",
                    "previous",
                    "event_url",
                    "scraped_at",
                    "source",
                ],
            )
            writer.writeheader()
            writer.writerows(
                [
                    {
                        "date": "2024-02-08",
                        "time": "13:30",
                        "country": "United States",
                        "event": "Non-Farm Payrolls",
                        "impact": "High",
                        "actual": "150K",
                        "forecast": "180K",
                        "previous": "160K",
                        "event_url": "https://investing.com/events/nfp",
                        "scraped_at": "2024-02-08T12:00:00Z",
                        "source": "investing.com",
                    },
                    {
                        "date": "2024-02-08",
                        "time": "14:00",
                        "country": "Eurozone",
                        "event": "ECB Interest Rate",
                        "impact": "Medium",
                        "actual": "4.50%",
                        "forecast": "4.50%",
                        "previous": "4.50%",
                        "event_url": "https://investing.com/events/ecb",
                        "scraped_at": "2024-02-08T12:00:00Z",
                        "source": "investing.com",
                    },
                    {
                        "date": "2024-02-08",
                        "time": "15:00",
                        "country": "United States",
                        "event": "CPI",
                        "impact": "High",
                        "actual": "",
                        "forecast": "3.2%",
                        "previous": "3.4%",
                        "event_url": "https://investing.com/events/cpi",
                        "scraped_at": "2024-02-08T12:00:00Z",
                        "source": "investing.com",
                    },
                ]
            )

        return csv_path

    def test_initialization(self, preprocessor):
        """Test preprocessor initialization."""
        assert preprocessor.CATEGORY == "events"
        assert preprocessor.input_dir.exists()
        assert preprocessor.output_dir.exists()

    def test_to_country_code_names(self, preprocessor):
        """Test country name to ISO code mapping."""
        assert preprocessor._to_country_code("United States") == "US"
        assert preprocessor._to_country_code("United Kingdom") == "GB"
        assert preprocessor._to_country_code("Eurozone") == "EU"
        assert preprocessor._to_country_code("Japan") == "JP"

    def test_to_country_code_currencies(self, preprocessor):
        """Test currency code to ISO code mapping."""
        assert preprocessor._to_country_code("USD") == "US"
        assert preprocessor._to_country_code("GBP") == "GB"
        assert preprocessor._to_country_code("EUR") == "EU"
        assert preprocessor._to_country_code("JPY") == "JP"

    def test_to_country_code_empty(self, preprocessor):
        """Test empty/None country code."""
        assert preprocessor._to_country_code(None) == ""
        assert preprocessor._to_country_code("") == ""

    def test_to_country_code_camelcase(self, preprocessor):
        """Test CamelCase country names from row IDs."""
        assert preprocessor._to_country_code("UnitedStates") == "US"
        assert preprocessor._to_country_code("UnitedKingdom") == "GB"
        assert preprocessor._to_country_code("EuroZone") == "EU"
        assert preprocessor._to_country_code("NewZealand") == "NZ"
        assert preprocessor._to_country_code("SouthKorea") == "KR"

    def test_parse_numeric_to_float_percentage(self, preprocessor):
        """Test parsing percentage values."""
        assert preprocessor._parse_numeric_to_float("4.50%") == 4.50
        assert preprocessor._parse_numeric_to_float("0.2%") == 0.2
        assert preprocessor._parse_numeric_to_float("-0.3%") == -0.3

    def test_parse_numeric_to_float_suffixes(self, preprocessor):
        """Test parsing K, M, B, T suffixes."""
        assert preprocessor._parse_numeric_to_float("150K") == 150_000
        assert preprocessor._parse_numeric_to_float("1.5M") == 1_500_000
        assert preprocessor._parse_numeric_to_float("2.3B") == 2_300_000_000
        assert preprocessor._parse_numeric_to_float("1.060T") == 1_060_000_000_000

    def test_parse_numeric_to_float_plain(self, preprocessor):
        """Test parsing plain numeric values."""
        assert preprocessor._parse_numeric_to_float("216.0") == 216.0
        assert preprocessor._parse_numeric_to_float("100") == 100.0
        assert preprocessor._parse_numeric_to_float("-50") == -50.0

    def test_parse_numeric_to_float_none_and_empty(self, preprocessor):
        """Test parsing None and empty values."""
        assert preprocessor._parse_numeric_to_float(None) is None
        assert preprocessor._parse_numeric_to_float("") is None
        assert preprocessor._parse_numeric_to_float("-") is None
        assert preprocessor._parse_numeric_to_float("N/A") is None

    def test_build_timestamp_utc(self, preprocessor):
        """Test UTC timestamp building from date and time."""
        result = preprocessor._build_timestamp_utc("2024-02-08", "13:30")
        assert result == "2024-02-08T13:30:00Z"

    def test_build_timestamp_utc_no_time(self, preprocessor):
        """Test timestamp with no time."""
        result = preprocessor._build_timestamp_utc("2024-02-08", None)
        assert result == "2024-02-08T00:00:00Z"

    def test_build_timestamp_utc_no_date(self, preprocessor):
        """Test timestamp with no date."""
        result = preprocessor._build_timestamp_utc(None, "13:30")
        assert result is None

    def test_generate_event_id(self, preprocessor):
        """Test event ID generation."""
        event_id = preprocessor._generate_event_id(
            "2024-02-08T13:30:00Z", "US", "Non-Farm Payrolls"
        )
        assert isinstance(event_id, str)
        assert len(event_id) == 16

        # Same input should generate same ID
        event_id2 = preprocessor._generate_event_id(
            "2024-02-08T13:30:00Z", "US", "Non-Farm Payrolls"
        )
        assert event_id == event_id2

        # Different input should generate different ID
        event_id3 = preprocessor._generate_event_id(
            "2024-02-08T14:00:00Z", "US", "Non-Farm Payrolls"
        )
        assert event_id != event_id3

    def test_normalize_event(self, preprocessor):
        """Test full event normalization."""
        raw = {
            "date": "2024-02-08",
            "time": "13:30",
            "country": "United States",
            "event": "Non-Farm Payrolls",
            "impact": "High",
            "actual": "150K",
            "forecast": "180K",
            "previous": "160K",
            "source": "investing.com",
        }
        normalized = preprocessor._normalize_event(raw)

        assert normalized["timestamp_utc"] == "2024-02-08T13:30:00Z"
        assert normalized["country"] == "US"
        assert normalized["event_name"] == "Non-Farm Payrolls"
        assert normalized["impact"] == "high"
        assert normalized["actual"] == 150_000
        assert normalized["forecast"] == 180_000
        assert normalized["previous"] == 160_000
        assert normalized["source"] == "investing.com"
        assert "event_id" in normalized

    def test_normalize_event_missing_actual(self, preprocessor):
        """Test normalization with missing actual value."""
        raw = {
            "date": "2024-02-08",
            "time": "15:00",
            "country": "United States",
            "event": "CPI",
            "impact": "High",
            "actual": None,
            "forecast": "3.2%",
            "previous": "3.4%",
            "source": "investing.com",
        }
        normalized = preprocessor._normalize_event(raw)

        assert normalized["actual"] is None
        assert normalized["forecast"] == 3.2
        assert normalized["previous"] == 3.4

    def test_preprocess(self, preprocessor, sample_bronze_csv):
        """Test preprocessing Bronze data to Silver."""
        result = preprocessor.preprocess()

        assert "events" in result
        df = result["events"]

        assert len(df) == 3
        assert list(df.columns) == [
            "timestamp_utc",
            "event_id",
            "country",
            "event_name",
            "impact",
            "actual",
            "forecast",
            "previous",
            "source",
        ]

        # Check first event
        assert df.iloc[0]["country"] == "US"
        assert df.iloc[0]["event_name"] == "Non-Farm Payrolls"
        assert df.iloc[0]["impact"] == "high"
        assert df.iloc[0]["actual"] == 150_000
        assert df.iloc[0]["source"] == "investing.com"

        # Check second event
        assert df.iloc[1]["country"] == "EU"
        assert df.iloc[1]["actual"] == 4.5

        # Check third event (missing actual)
        assert pd.isnull(df.iloc[2]["actual"])
        assert df.iloc[2]["forecast"] == 3.2

    def test_preprocess_no_bronze_files(self, preprocessor):
        """Test preprocessing with no Bronze files."""
        result = preprocessor.preprocess()
        assert result == {}

    def test_validate_success(self, preprocessor):
        """Test validation with valid Silver DataFrame."""
        df = pd.DataFrame(
            [
                {
                    "timestamp_utc": "2024-02-08T13:30:00Z",
                    "event_id": "abc123def456",
                    "country": "US",
                    "event_name": "Non-Farm Payrolls",
                    "impact": "high",
                    "actual": 150_000.0,
                    "forecast": 180_000.0,
                    "previous": 160_000.0,
                    "source": "investing.com",
                }
            ]
        )

        assert preprocessor.validate(df) is True

    def test_validate_missing_columns(self, preprocessor):
        """Test validation with missing required columns."""
        df = pd.DataFrame(
            [
                {
                    "timestamp_utc": "2024-02-08T13:30:00Z",
                    "country": "US",
                    "event_name": "Test",
                }
            ]
        )

        with pytest.raises(ValueError, match="Missing required columns"):
            preprocessor.validate(df)

    def test_validate_null_values(self, preprocessor):
        """Test validation with null values in required columns."""
        df = pd.DataFrame(
            [
                {
                    "timestamp_utc": None,
                    "event_id": "abc123",
                    "country": "US",
                    "event_name": "Test",
                    "impact": "high",
                    "actual": None,
                    "forecast": None,
                    "previous": None,
                    "source": "test",
                }
            ]
        )

        with pytest.raises(ValueError, match="null values"):
            preprocessor.validate(df)

    def test_validate_invalid_impact(self, preprocessor):
        """Test validation with invalid impact values."""
        df = pd.DataFrame(
            [
                {
                    "timestamp_utc": "2024-02-08T13:30:00Z",
                    "event_id": "abc123",
                    "country": "US",
                    "event_name": "Test",
                    "impact": "invalid",
                    "actual": None,
                    "forecast": None,
                    "previous": None,
                    "source": "test",
                }
            ]
        )

        with pytest.raises(ValueError, match="Invalid impact values"):
            preprocessor.validate(df)

    def test_validate_duplicate_event_ids(self, preprocessor):
        """Test validation with duplicate event IDs."""
        df = pd.DataFrame(
            [
                {
                    "timestamp_utc": "2024-02-08T13:30:00Z",
                    "event_id": "abc123",
                    "country": "US",
                    "event_name": "Test 1",
                    "impact": "high",
                    "actual": None,
                    "forecast": None,
                    "previous": None,
                    "source": "test",
                },
                {
                    "timestamp_utc": "2024-02-08T14:00:00Z",
                    "event_id": "abc123",
                    "country": "EU",
                    "event_name": "Test 2",
                    "impact": "medium",
                    "actual": None,
                    "forecast": None,
                    "previous": None,
                    "source": "test",
                },
            ]
        )

        with pytest.raises(ValueError, match="duplicate event_id"):
            preprocessor.validate(df)

    def test_export(self, preprocessor):
        """Test exporting Silver DataFrame to CSV."""
        df = pd.DataFrame(
            [
                {
                    "timestamp_utc": "2024-02-08T13:30:00Z",
                    "event_id": "abc123",
                    "country": "US",
                    "event_name": "Non-Farm Payrolls",
                    "impact": "high",
                    "actual": 150_000.0,
                    "forecast": 180_000.0,
                    "previous": 160_000.0,
                    "source": "investing.com",
                }
            ]
        )

        from datetime import datetime

        output_path = preprocessor.export(
            df,
            identifier="2024-02-08_2024-02-08",
            start_date=datetime(2024, 2, 8),
            end_date=datetime(2024, 2, 8),
            format="csv",
        )

        assert output_path.exists()
        assert "events_2024-02-08_2024-02-08_2024-02-08_2024-02-08.csv" in str(output_path)

        # Verify content
        df_read = pd.read_csv(output_path)
        assert len(df_read) == 1
        assert df_read.iloc[0]["country"] == "US"


class TestIntegration:
    """Integration tests for Bronze → Silver transformation."""

    def test_full_preprocessing_workflow(self, tmp_path):
        """Test complete Bronze → Silver workflow."""
        # Create Bronze data
        bronze_dir = tmp_path / "raw" / "calendar"
        silver_dir = tmp_path / "processed" / "events"
        bronze_dir.mkdir(parents=True)
        silver_dir.mkdir(parents=True)

        bronze_csv = bronze_dir / "investing_economic_events_20240208.csv"

        with open(bronze_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "date",
                    "time",
                    "country",
                    "event",
                    "impact",
                    "actual",
                    "forecast",
                    "previous",
                    "event_url",
                    "scraped_at",
                    "source",
                ],
            )
            writer.writeheader()
            writer.writerow(
                {
                    "date": "2024-02-08",
                    "time": "13:30",
                    "country": "United States",
                    "event": "Non-Farm Payrolls",
                    "impact": "High",
                    "actual": "150K",
                    "forecast": "180K",
                    "previous": "160K",
                    "event_url": "https://example.com",
                    "scraped_at": "2024-02-08T12:00:00Z",
                    "source": "investing.com",
                }
            )

        # Preprocess
        preprocessor = CalendarPreprocessor(input_dir=bronze_dir, output_dir=silver_dir)
        result = preprocessor.preprocess()

        assert "events" in result
        df = result["events"]

        # Validate
        assert preprocessor.validate(df) is True

        # Export
        from datetime import datetime

        output_path = preprocessor.export(
            df,
            identifier="2024-02-08_2024-02-08",
            start_date=datetime(2024, 2, 8),
            end_date=datetime(2024, 2, 8),
            format="csv",
        )

        assert output_path.exists()

        # Verify Silver CSV
        df_silver = pd.read_csv(output_path)
        assert len(df_silver) == 1
        assert df_silver.iloc[0]["country"] == "US"
        assert df_silver.iloc[0]["event_name"] == "Non-Farm Payrolls"
        assert df_silver.iloc[0]["impact"] == "high"
        assert df_silver.iloc[0]["actual"] == 150_000.0
        assert df_silver.iloc[0]["source"] == "investing.com"
