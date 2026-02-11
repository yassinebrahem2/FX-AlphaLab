# Economic Calendar Data Collection

Economic calendar data provides scheduled macroeconomic events, releases, and their market impact from Investing.com.

## Overview

**Data Type**: Scheduled economic events
**Source**: Investing.com Economic Calendar (web scraping)
**Update Frequency**: Real-time
**Setup Required**: None (public data)
**Output**: Bronze CSV → Silver normalized CSV

## Setup

No API key required. Verify connectivity:

```bash
python -m scripts.collect_calendar_data --health-check
```

Expected output:
```
[INFO] Health check: PASSED
```

## Data Coverage

### Supported Countries/Regions
- **United States (us)**: Fed decisions, employment, inflation
- **Eurozone (eu)**: ECB decisions, GDP, unemployment
- **United Kingdom (uk)**: BoE decisions, economic indicators
- **Japan (jp)**: BoJ decisions, trade data
- **Switzerland (ch)**: SNB decisions, economic data

### Event Types
- Central bank policy decisions
- Employment reports (Non-Farm Payrolls, unemployment)
- Inflation data (CPI, PPI)
- GDP releases
- Trade balance and activity indices

### Impact Levels
- **High**: Major market-moving events
- **Medium**: Significant indicators
- **Low**: Minor releases

## Usage

### Command Line (Recommended)

#### Today's Events
```bash
python -m scripts.collect_calendar_data --today --preprocess
```

#### Date Range
```bash
python -m scripts.collect_calendar_data --start 2023-01-01 --end 2023-01-31 --preprocess
```

#### Specific Countries
```bash
python -m scripts.collect_calendar_data --countries us,eu,uk --today --preprocess
```

#### All Options
```bash
python -m scripts.collect_calendar_data --help

Options:
  --start DATE          Start date (YYYY-MM-DD), default: today
  --end DATE            End date (YYYY-MM-DD), default: 7 days from start
  --today               Collect today's events only
  --countries CODES     Comma-separated country codes (us,eu,uk,jp,ch)
                        Default: us,eu,uk,jp,ch
  --preprocess          Run Silver preprocessing after collection
  --health-check        Verify API connectivity only
  --verbose, -v         Enable debug logging
```

### Programmatic Usage

```python
from src.ingestion.collectors.calendar_collector import EconomicCalendarCollector

# Initialize collector
collector = EconomicCalendarCollector()

# Collect today's events
events = collector.collect_events(
    start_date=\"2024-02-10\",
    end_date=\"2024-02-10\",
    countries=[\"us\", \"eu\", \"uk\"]
)

# Save to Bronze CSV
collector.save_to_csv(events)

# Get as DataFrame
import pandas as pd
df = pd.DataFrame(events)
```

## Output Schema

### Bronze Layer (Raw Events)

**Location**: `data/raw/calendar/`
**Format**: CSV
**Filename**: `economic_events_{YYYYMMDD}.csv`

| Column | Type | Description |
|--------|------|-------------|
| `date` | string | Event date (YYYY-MM-DD) |
| `time` | string | Event time (local timezone) |
| `country` | string | Country name |
| `event` | string | Event description |
| `impact` | string | High, Medium, Low, Unknown |
| `actual` | string | Released value (null if not yet released) |
| `forecast` | string | Expected value |
| `previous` | string | Prior period value |
| `event_url` | string | Detail page URL (optional) |
| `scraped_at` | string | Collection timestamp (UTC) |

### Silver Layer (Normalized Events)

**Location**: `data/processed/events/`
**Format**: CSV
**Filename**: `events_{start}_{end}.csv`
**Preprocessor**: `CalendarPreprocessor`

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | string | Unique identifier (hash) |
| `timestamp_utc` | datetime | ISO 8601 UTC |
| `country_code` | string | ISO 2-letter code (US, EU, GB, JP, CH) |
| `currency` | string | Currency (USD, EUR, GBP, JPY, CHF) |
| `event_name` | string | Event description |
| `impact` | string | high, medium, low |
| `actual` | float | Actual value (parsed numeric) |
| `forecast` | float | Forecast value (parsed numeric) |
| `previous` | float | Previous value (parsed numeric) |
| `source` | string | \"investing.com\" |

## Web Scraping Ethics

The collector implements responsible scraping:

### Robots.txt Compliance
- Checks and respects robots.txt rules
- Never scrapes disallowed paths
- Honors crawl delays

### Rate Limiting
- Random delays: 3-5 seconds between requests
- User-agent rotation
- Exponential backoff on errors

### Respectful Behavior
- Minimal server load
- Proper error handling
- Caches where possible

## Error Handling

| Error | Behavior |
|-------|----------|
| Network failure | Retry with exponential backoff |
| HTTP 429 (rate limit) | Honors retry-after header |
| Missing actual values | Event still captured (forecast/previous remain) |
| HTML structure change | Logs warning, attempts fallback selectors |
| Robots.txt block | Skips request, logs warning |

## Value Parsing

The preprocessor normalizes various value formats:

```python
\"150K\" → 150000.0
\"4.5%\" → 4.5
\"-0.3B\" → -300000000.0
\"N/A\" → None
```

Supports:
- Percentages (%)
- Thousands (K)
- Millions (M)
- Billions (B)
- Negative values
- Decimal values

## Health Check

```bash
python -m scripts.collect_calendar_data --health-check
```

Verifies:
- ✅ Investing.com is accessible
- ✅ Network connectivity
- ✅ Robots.txt compliance

## Logging

**Console**: INFO level
**File**: `logs/collectors/calendar_collector.log` (DEBUG level)

Enable verbose mode:
```bash
python -m scripts.collect_calendar_data --verbose
```

## Example Workflow

```bash
# 1. Health check
python -m scripts.collect_calendar_data --health-check

# 2. Collect this week's events (US & EU only)
python -m scripts.collect_calendar_data \\
    --start 2024-02-10 \\
    --end 2024-02-17 \\
    --countries us,eu \\
    --preprocess

# 3. Verify output
ls data/raw/calendar/        # Bronze CSV
ls data/processed/events/    # Silver CSV
```

## Use Cases

### Pre-Trade Planning
Identify high-impact events before they occur:
```python
import pandas as pd

# Load processed events
df = pd.read_csv(\"data/processed/events/events_2024-02-10_2024-02-10.csv\")

# Filter high-impact events
high_impact = df[df[\"impact\"] == \"high\"]
print(high_impact[[\"timestamp_utc\", \"country_code\", \"event_name\"]])
```

### Post-Event Analysis
Analyze surprise factors (actual vs forecast):
```python
# Calculate surprises
df[\"surprise\"] = df[\"actual\"] - df[\"forecast\"]
df[\"surprise_pct\"] = (df[\"surprise\"] / df[\"forecast\"]) * 100

# Large surprises
surprises = df[abs(df[\"surprise_pct\"]) > 10]
```

### Event Calendar
Build a trading calendar:
```python
# Next week's USD events
next_week = df[
    (df[\"currency\"] == \"USD\") &
    (df[\"impact\"].isin([\"high\", \"medium\"]))
].sort_values(\"timestamp_utc\")
```

## Requirements

```
beautifulsoup4>=4.12
requests>=2.31
pandas>=2.0
lxml
```

## Testing

```bash
pytest tests/ingestion/test_calendar_collector.py -v
pytest tests/ingestion/test_calendar_parser.py -v
```

Tests use mocked HTTP responses (no actual web scraping).

## Limitations

- **Time zones**: Event times are in local timezone (converted to UTC in Silver)
- **Historical data**: Limited to what's available on Investing.com
- **Actual values**: May appear with delay after event time
- **Website changes**: Subject to HTML structure changes (requires selector updates)
- **Rate enforcement**: Aggressive scraping may result in temporary blocks

## Maintenance

If scraping fails due to website changes:

1. Check logs: `logs/collectors/calendar_collector.log`
2. Inspect HTML selectors in `src/ingestion/collectors/calendar_collector.py`
3. Update CSS selectors if structure changed
4. Run tests: `pytest tests/ingestion/test_calendar_collector.py -v`
