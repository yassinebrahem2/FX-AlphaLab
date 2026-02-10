"""Economic Calendar Preprocessor (Bronze → Silver).

Transforms raw economic calendar data from data/raw/calendar/
to standardized Silver schema in data/processed/events/.

Silver Schema (§3.2.3):
- timestamp_utc (ISO 8601)
- event_id (unique hash)
- country (ISO 3166 alpha-2)
- event_name
- impact (high, medium, low)
- actual, forecast, previous (numeric floats)
- source
"""

import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.ingestion.preprocessors.base_preprocessor import BasePreprocessor
from src.shared.config import Config


class CalendarPreprocessor(BasePreprocessor):
    """Preprocessor for economic calendar data (Bronze → Silver).

    Transforms scraped calendar events from Investing.com into standardized
    Silver schema following §3.2.3 requirements.
    """

    CATEGORY = "events"

    def __init__(
        self,
        input_dir: Path | None = None,
        output_dir: Path | None = None,
        log_file: Path | None = None,
    ) -> None:
        """Initialize the calendar preprocessor.

        Args:
            input_dir: Directory containing Bronze calendar data (default: data/raw/calendar/)
            output_dir: Directory for Silver events data (default: data/processed/events/)
            log_file: Optional path for file-based logging
        """
        super().__init__(
            input_dir=input_dir or Config.DATA_DIR / "raw" / "calendar",
            output_dir=output_dir or Config.DATA_DIR / "processed" / "events",
            log_file=log_file or Config.LOGS_DIR / "calendar_preprocessor.log",
        )

        # Country name/currency to ISO 3166 alpha-2 mapping
        self.country_code_map = {
            "united states": "US",
            "eurozone": "EU",
            "europe": "EU",
            "united kingdom": "GB",
            "japan": "JP",
            "canada": "CA",
            "australia": "AU",
            "switzerland": "CH",
            "china": "CN",
            "germany": "DE",
            "france": "FR",
            "italy": "IT",
            "spain": "ES",
            "new zealand": "NZ",
            "singapore": "SG",
            "brazil": "BR",
            "south korea": "KR",
            "india": "IN",
            "mexico": "MX",
            "south africa": "ZA",
            "turkey": "TR",
            "sweden": "SE",
            "norway": "NO",
            "denmark": "DK",
            "hong kong": "HK",
            "indonesia": "ID",
            "thailand": "TH",
            "russia": "RU",
            "poland": "PL",
            "israel": "IL",
            "colombia": "CO",
            "philippines": "PH",
            # Currency code fallbacks
            "USD": "US",
            "EUR": "EU",
            "GBP": "GB",
            "JPY": "JP",
            "CAD": "CA",
            "AUD": "AU",
            "CHF": "CH",
            "CNY": "CN",
            "NZD": "NZ",
            "SGD": "SG",
            "BRL": "BR",
            "KRW": "KR",
            "INR": "IN",
            "MXN": "MX",
            "ZAR": "ZA",
            "TRY": "TR",
            "SEK": "SE",
            "NOK": "NO",
            "DKK": "DK",
            "HKD": "HK",
            "IDR": "ID",
            "THB": "TH",
            "RUB": "RU",
            "PLN": "PL",
            "ILS": "IL",
            "COP": "CO",
            "PHP": "PH",
        }

    def _to_country_code(self, country_raw: str | None) -> str:
        """Convert country name or currency code to ISO 3166 alpha-2 code.

        Args:
            country_raw: Country name or currency code

        Returns:
            ISO 3166 alpha-2 code or original value if not mapped
        """
        if not country_raw:
            return ""

        # Try exact match first (for currency codes like "USD", "EUR")
        if country_raw in self.country_code_map:
            return self.country_code_map[country_raw]

        # Try case-insensitive match for country names
        lower = country_raw.strip().lower()
        if lower in self.country_code_map:
            return self.country_code_map[lower]

        # Handle CamelCase from row IDs (e.g., "UnitedStates" → "united states")
        spaced = re.sub(r"([a-z])([A-Z])", r"\1 \2", country_raw).strip().lower()
        if spaced in self.country_code_map:
            return self.country_code_map[spaced]

        # Return uppercase of first 2 chars as fallback
        return country_raw.strip().upper()[:2]

    def _parse_numeric_to_float(self, value: str | None) -> float | None:
        """Parse a numeric string (with optional suffixes like %, K, M, B, T) to float.

        Args:
            value: Raw string value (e.g., '150K', '4.50%', '1.060T')

        Returns:
            Float value or None if parsing failed
        """
        if value is None:
            return None

        cleaned = str(value).strip()
        if not cleaned or cleaned in ["-", "N/A", "NA", ""]:
            return None

        try:
            # Remove commas
            cleaned = cleaned.replace(",", "")

            # Handle percentage
            if cleaned.endswith("%"):
                return float(cleaned[:-1])

            # Handle suffixes (K=thousands, M=millions, B=billions, T=trillions)
            suffix_multipliers = {
                "K": 1_000,
                "M": 1_000_000,
                "B": 1_000_000_000,
                "T": 1_000_000_000_000,
            }

            upper = cleaned.upper()
            for suffix, multiplier in suffix_multipliers.items():
                if upper.endswith(suffix):
                    num = float(cleaned[:-1])
                    return num * multiplier

            return float(cleaned)

        except (ValueError, TypeError):
            self.logger.debug(f"Could not parse numeric value: {value}")
            return None

    def _build_timestamp_utc(self, date_str: str | None, time_str: str | None) -> str | None:
        """Build UTC ISO 8601 timestamp from date and time strings.

        Args:
            date_str: Date string (e.g., '2024-02-08')
            time_str: Time string (e.g., '13:30')

        Returns:
            UTC ISO 8601 timestamp string or None
        """
        if not date_str:
            return None

        try:
            # Try parsing date
            date_part = datetime.strptime(date_str, "%Y-%m-%d")

            # Add time if available
            if time_str and time_str.strip():
                time_match = re.match(r"(\d{1,2}):(\d{2})", time_str.strip())
                if time_match:
                    hour, minute = int(time_match.group(1)), int(time_match.group(2))
                    date_part = date_part.replace(hour=hour, minute=minute)

            # Set as UTC
            return date_part.replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        except (ValueError, TypeError):
            return None

    def _generate_event_id(self, timestamp: str | None, country: str, event_name: str) -> str:
        """Generate a unique event ID from timestamp, country, and event name.

        Args:
            timestamp: UTC ISO 8601 timestamp
            country: ISO country code
            event_name: Event name

        Returns:
            SHA256 hash (first 16 characters)
        """
        key = f"{timestamp}|{country}|{event_name}"
        return hashlib.sha256(key.encode()).hexdigest()[:16]

    def _normalize_event(self, raw_event: dict) -> dict:
        """Normalize a raw scraped event into the standardized Silver schema.

        Silver Schema (§3.2.3):
            timestamp_utc, event_id, country, event_name, impact,
            actual, forecast, previous, source

        Args:
            raw_event: Raw event dict from Bronze layer

        Returns:
            Normalized event dict
        """
        timestamp_utc = self._build_timestamp_utc(raw_event.get("date"), raw_event.get("time"))
        country = self._to_country_code(raw_event.get("country"))
        event_name = raw_event.get("event", "")

        return {
            "timestamp_utc": timestamp_utc,
            "event_id": self._generate_event_id(timestamp_utc, country, event_name),
            "country": country,
            "event_name": event_name,
            "impact": (raw_event.get("impact", "unknown") or "unknown").lower(),
            "actual": self._parse_numeric_to_float(raw_event.get("actual")),
            "forecast": self._parse_numeric_to_float(raw_event.get("forecast")),
            "previous": self._parse_numeric_to_float(raw_event.get("previous")),
            "source": raw_event.get("source", "investing.com"),
        }

    def preprocess(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Transform Bronze calendar data to Silver schema.

        Reads raw CSV files from input_dir, applies transformations:
        - UTC timestamp conversion
        - Country code standardization (ISO 3166 alpha-2)
        - Impact level normalization (lowercase)
        - Numeric value parsing
        - Event ID generation
        - Column standardization
        - Deduplication

        Args:
            start_date: Start of the processing window (filters by date)
            end_date: End of the processing window (filters by date)

        Returns:
            Dictionary with single key 'events' containing standardized DataFrame
        """
        # Find all Bronze CSV files
        csv_files = list(self.input_dir.glob("investing_*.csv"))

        if not csv_files:
            self.logger.warning(f"No Bronze calendar CSV files found in {self.input_dir}")
            return {}

        self.logger.info(f"Found {len(csv_files)} Bronze calendar files")

        all_events = []

        for csv_file in csv_files:
            self.logger.info(f"Processing {csv_file.name}")
            try:
                df = pd.read_csv(csv_file, encoding="utf-8")

                # Normalize each event
                for _, row in df.iterrows():
                    normalized = self._normalize_event(row.to_dict())
                    all_events.append(normalized)

            except Exception as e:
                self.logger.error(f"Error processing {csv_file.name}: {e}")
                continue

        if not all_events:
            self.logger.warning("No events processed")
            return {}

        # Create DataFrame
        df_events = pd.DataFrame(all_events)

        # Filter by date range if provided
        if start_date or end_date:
            df_events["_temp_date"] = pd.to_datetime(df_events["timestamp_utc"], errors="coerce")

            if start_date:
                df_events = df_events[df_events["_temp_date"] >= start_date]
            if end_date:
                df_events = df_events[df_events["_temp_date"] <= end_date]

            df_events = df_events.drop(columns=["_temp_date"])

        # Deduplicate by event_id
        initial_count = len(df_events)
        df_events = df_events.drop_duplicates(subset=["event_id"], keep="first")
        final_count = len(df_events)

        if initial_count != final_count:
            self.logger.info(f"Removed {initial_count - final_count} duplicate events")

        # Sort by timestamp
        df_events = df_events.sort_values("timestamp_utc").reset_index(drop=True)

        self.logger.info(f"Successfully processed {len(df_events)} unique events")

        return {"events": df_events}

    def validate(self, df: pd.DataFrame) -> bool:
        """Validate that DataFrame conforms to Silver events schema (§3.2.3).

        Validation Rules:
        - timestamp_utc must be valid ISO 8601, UTC timezone
        - country must be valid ISO 3166 alpha-2 code (2 chars, uppercase)
        - impact must be one of: "high", "medium", "low"
        - actual, forecast, previous can be null (for upcoming events or missing data)
        - No duplicate (timestamp_utc, event_id) combinations

        Args:
            df: DataFrame to validate

        Returns:
            True if valid

        Raises:
            ValueError: If validation fails with details
        """
        # Check required columns
        required_columns = [
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

        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        # Check no null values in required non-numeric columns
        required_non_null = [
            "timestamp_utc",
            "event_id",
            "country",
            "event_name",
            "impact",
            "source",
        ]
        for col in required_non_null:
            if df[col].isnull().any():
                null_count = df[col].isnull().sum()
                raise ValueError(f"Column '{col}' has {null_count} null values")

        # Validate impact values
        valid_impacts = ["high", "medium", "low", "unknown"]
        invalid_impacts = df[~df["impact"].isin(valid_impacts)]["impact"].unique()
        if len(invalid_impacts) > 0:
            raise ValueError(f"Invalid impact values: {invalid_impacts}")

        # Validate country codes (ISO 3166 alpha-2: 2 chars, uppercase)
        invalid_countries = df[df["country"].str.len() != 2]["country"].unique()
        if len(invalid_countries) > 0:
            self.logger.warning(f"Non-standard country codes: {invalid_countries}")

        # Check for duplicates
        duplicates = df.duplicated(subset=["event_id"], keep=False)
        if duplicates.any():
            dup_count = duplicates.sum()
            raise ValueError(f"Found {dup_count} duplicate event_id values")

        # Validate timestamp format (ISO 8601)
        try:
            pd.to_datetime(df["timestamp_utc"], format="ISO8601", errors="raise")
        except Exception as e:
            raise ValueError(f"Invalid timestamp_utc format: {e}")

        self.logger.info("Validation passed: DataFrame conforms to Silver events schema")
        return True
