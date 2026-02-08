"""
Macro / Economic Data Schema (RAW)
STEP 4 – Exogenous Event Data
"""

MACRO_SCHEMA_RAW = {
    # --- Identity ---
    "event_id": str,                    # Stable ID from source (or derived hash)
    "country": str,                     # ISO-like: US, EU, UK, JP, CH
    "currency": str,                    # USD, EUR, GBP, JPY, CHF

    # --- Event Description ---
    "indicator": str,                   # CPI, GDP, NFP, PMI
    "frequency": str,                   # monthly, quarterly
    "unit": str,                        # %, index, level

    # --- Timing ---
    "scheduled_datetime_utc": "datetime64[ns]",  # When event was expected
    "release_datetime_utc": "datetime64[ns]",    # When data was released
    "ingested_at_utc": "datetime64[ns]",          # When *you* ingested it

    # --- Values ---
    "actual_value": float,              # Published value
    "previous_value": float,            # Previous release (nullable)
    "revision_value": float,            # Revised value if applicable (nullable)

    # --- Provenance ---
    "source": str                       # FRED, ECB, Investing, etc.
}
