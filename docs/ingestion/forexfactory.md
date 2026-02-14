# Forex Factory Calendar Data Collection

Economic calendar data from Forex Factory providing scheduled macroeconomic events, releases, and their market impact.

## Overview

**Data Type**: Scheduled economic events
**Source**: Forex Factory Economic Calendar (https://www.forexfactory.com/calendar)
**Update Frequency**: Real-time
**Setup Required**: None (public data)
**Output**: Bronze CSV → Silver normalized CSV

## Features

- **Comprehensive Coverage**: All major currencies (USD, EUR, GBP, JPY, CHF, AUD, CAD, NZD, CNY)
- **Impact Levels**: High, Medium, Low impact classification
- **Complete Data**: Event name, time, forecast, actual, and previous values
- **Efficient Collection**: Month-view strategy reduces API requests by ~30x
- **Lazy Loading Support**: Human-like scrolling behavior triggers dynamic content loading
- **Cloudflare Bypass**: Undetected ChromeDriver for seamless data access
- **GMT Timezone**: Automated timezone configuration via UI interaction
- **Ethical Scraping**: Respects robots.txt with conservative rate limiting
- **Error Resilient**: Comprehensive error handling and validation

## Setup

No API key required. Verify connectivity:

```bash
python scripts/collect_forexfactory_data.py --health-check
```

Expected output:
```
[INFO] Health check: PASSED
```

## Data Coverage

### Supported Currencies
- **USD**: US Dollar events (Fed decisions, employment, inflation)
- **EUR**: Euro events (ECB decisions, GDP, unemployment)
- **GBP**: British Pound events (BoE decisions, economic indicators)
- **JPY**: Japanese Yen events (BoJ decisions, trade data)
- **CHF**: Swiss Franc events (SNB decisions)
- **AUD, CAD, NZD, CNY**: Additional major currencies

### Event Types
- Central bank policy decisions and statements
- Employment reports (Non-Farm Payrolls, unemployment rates)
- Inflation data (CPI, PPI, core inflation)
- GDP releases and economic growth
- Trade balance and manufacturing indices
- Retail sales and consumer confidence

### Impact Levels
- **High**: Major market-moving events (Fed decisions, NFP, CPI)
- **Medium**: Significant indicators (retail sales, manufacturing PMI)
- **Low**: Minor releases (building permits, minor economic data)

## Usage

### Command Line (Recommended)

#### Today's Events
```bash
python scripts/collect_forexfactory_data.py --today
```

#### Date Range
```bash
python scripts/collect_forexfactory_data.py --start 2023-01-01 --end 2023-01-31
```

#### With Validation
```bash
python scripts/collect_forexfactory_data.py --today --validate
```

#### All Options
```bash
python scripts/collect_forexfactory_data.py --help

Options:
  --start DATE          Start date (YYYY-MM-DD), default: today
  --end DATE            End date (YYYY-MM-DD), default: same as start
  --today               Collect today's events only
  --health-check        Verify API connectivity only
  --validate            Validate scraped data quality
  --preprocess          Run Silver preprocessing after collection
  --output DIR          Custom output directory
  --verbose, -v         Enable debug logging
```

### Programmatic Usage

```python
from datetime import datetime
from src.ingestion.collectors.forexfactory_collector import ForexFactoryCalendarCollector

# Initialize collector
collector = ForexFactoryCalendarCollector()

# Collect today's events
events = collector.collect_events(
    start_date="2024-02-10",
    end_date="2024-02-10"
)

# Save to Bronze CSV
output_path = collector.save_to_csv(events)

# Get as DataFrame
df = collector.get_events_dataframe(events)

# Validate data quality
is_valid, errors = collector.validate_scraped_data(events)
```

### Advanced Usage

```python
from datetime import datetime, timedelta

# Initialize with custom settings
collector = ForexFactoryCalendarCollector(
    min_delay=5.0,  # 5 second minimum delay
    max_delay=8.0,  # 8 second maximum delay
    max_retries=2,  # Retry up to 2 times
    output_dir="/custom/path"
)

# Collect last week's data
end_date = datetime.now()
start_date = end_date - timedelta(days=7)

data = collector.collect(
    start_date=start_date,
    end_date=end_date
)

if "calendar" in data:
    df = data["calendar"]
    print(f"Collected {len(df)} events")

    # Export using BaseCollector method
    collector.export_csv(df, "economic_events")
```

## Output Schema

### Bronze Layer (Raw Events)

**Location**: `data/raw/forexfactory/`
**Format**: CSV
**Filename**: `forexfactory_calendar_{YYYYMMDD}.csv`

| Column | Type | Description |
|--------|------|-------------|
| `date` | string | Event date (YYYY-MM-DD) |
| `time` | string | Event time (local timezone) |
| `currency` | string | Currency code (USD, EUR, etc.) |
| `event` | string | Event description |
| `impact` | string | High, Medium, Low, Unknown |
| `actual` | string | Released value (null if not yet released) |
| `forecast` | string | Expected value |
| `previous` | string | Prior period value |
| `event_url` | string | Detail page URL (optional) |
| `source_url` | string | Source URL |
| `scraped_at` | string | Collection timestamp (ISO 8601 UTC) |
| `source` | string | "forexfactory.com" |

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
| `source` | string | "forexfactory.com" |

## Web Scraping Ethics

The collector implements responsible scraping practices:

### Robots.txt Compliance
- Automatically fetches and respects robots.txt rules
- Checks permissions before each request
- Never scrapes disallowed paths
- Forex Factory robots.txt has minimal restrictions (only sitemap listed)

### Rate Limiting (Conservative)
- **Minimum delay**: 3 seconds between requests
- **Maximum delay**: 5 seconds with random jitter
- **Exponential backoff**: On 503 errors
- **No aggressive retries**: Max 2 retries to avoid IP bans
- **429 handling**: Exits gracefully on rate limiting
- **Human-like scrolling**: Random scroll distances (60-100% viewport) with pauses (0.3-0.8s)
- **Natural behavior**: 15% chance of small backwards scrolls to mimic reading
- **Smooth scrolling**: CSS smooth scroll enabled for realistic page navigation

### User-Agent Rotation
Rotates between legitimate browser user agents:
- Chrome 120 (Windows/Mac)
- Firefox 121 (Windows)
- Safari 17.1 (Mac)
- Chrome 120 (Linux)

### Respectful Behavior
- Minimal server load with conservative delays
- Proper error handling without hammering
- Connection pooling via session reuse
- Timeout handling (30 seconds default)

## Technical Implementation

### Month-View Optimization

The collector uses an intelligent fetching strategy to minimize requests:

**Single Day** (1 request):
```
?day=feb12.2026
```

**Same Week** (1 request):
```
?week=feb12.2026
```

**Same Month** (1 request):
```
?month=feb.2026
```

**Multiple Months** (N requests):
```
?month=jan.2026
?month=feb.2026
```

**Performance Impact**:
- 31-day month: 1 request vs 31 requests (97% reduction)
- 2-month range: 2 requests vs 60+ requests (96% reduction)

### Human-Like Scrolling for Lazy Loading

Forex Factory uses virtual scrolling/lazy loading to reduce initial page load. The collector simulates human browsing behavior:

**Scroll Characteristics**:
- **Distance**: Random 60-100% of viewport height per scroll
- **Timing**: 0.3-0.8 second pauses (human reading speed)
- **Backtracking**: 15% chance of small backwards scroll (5-15% viewport)
- **Smoothness**: CSS `scrollBehavior: 'smooth'` enabled
- **Movement**: Relative scrolling via `scrollBy()` for natural motion

**Algorithm**:
```javascript
// Enable smooth scrolling
document.documentElement.style.scrollBehavior = 'smooth';

while (current_position < total_height) {
    // Random scroll distance
    scroll_distance = viewport_height * random(0.6, 1.0);
    window.scrollBy(0, scroll_distance);

    // Human reading pause
    sleep(random(0.3, 0.8));

    // Occasional backwards scroll (15% probability)
    if (random() < 0.15) {
        window.scrollBy(0, -viewport_height * random(0.05, 0.15));
    }

    // Check for new content loaded
    if (document.body.scrollHeight > total_height) {
        total_height = document.body.scrollHeight;
    }
}
```

**Results**:
- 97-99% capture rate (426/430 events on typical month)
- Skipped rows are legitimate (weekends marked with `calendar__row--no-event`)
- 16-22 scroll actions per month (~12 seconds)

### Timezone Configuration

Events are automatically configured to GMT timezone via UI interaction:

**Method**: Simulates user clicking timezone dropdown
**Target**: Settings > Timezone preference
**Fallback**: GMT parameter in URL (`?timezone=GMT`)
**Session**: Once per WebDriver session
**Verification**: Checks dropdown value after interaction

This ensures consistent UTC timestamps across all collected events.

### Cloudflare Bypass

Uses `undetected-chromedriver` to bypass Cloudflare protection:

**Primary Method**: Undetected ChromeDriver (v3+)
**Fallback**: Standard Selenium WebDriver + longer waits
**Detection Avoidance**:
- Random user agents
- Real Chrome binary (not headless)
- Human-like scrolling patterns
- Natural timing variations

**Challenge Handling**:
- Waits for "Just a moment..." page to clear
- Max 30 seconds for challenge completion
- Retries with fresh driver instance if needed

## Error Handling

| Error | Behavior |
|-------|----------|
| Network failure | Retry with exponential backoff (max 2 retries) |
| HTTP 429 (rate limit) | Exit gracefully, log warning |
| HTTP 503 (unavailable) | Exponential backoff retry |
| Missing actual values | Event still captured (forecast/previous remain) |
| HTML structure change | Logs warning, attempts fallback selectors |
| Robots.txt block | Skips request, logs warning |
| Invalid date format | Returns empty list, logs error |
| Empty response | Returns empty list, logs warning |

### Data Validation

The collector includes comprehensive data validation:

```python
is_valid, errors = collector.validate_scraped_data(events)
```

Checks for:
- Empty events list
- Missing required fields (date, currency, event, impact)
- Invalid date formats
- Malformed data rows

## Value Parsing

The preprocessor normalizes various value formats:

```python
"150K" → 150000.0
"4.5%" → 4.5
"-0.3B" → -300000000.0
"N/A" → None
"-" → None
```

Supports:
- Percentages (%)
- Thousands (K)
- Millions (M)
- Billions (B)
- Negative values
- Decimal values
- Null indicators (-, N/A, na, empty)

## Health Check

```bash
python scripts/collect_forexfactory_data.py --health-check
```

Verifies:
- ✅ Forex Factory is accessible
- ✅ Network connectivity
- ✅ Robots.txt compliance
- ✅ Response time < 10 seconds

## Logging

**Console**: INFO level
**File**: `logs/collectors/forexfactory_collector.log` (DEBUG level)

Enable verbose mode:
```bash
python scripts/collect_forexfactory_data.py --today --verbose
```

Log levels:
- **DEBUG**: Request details, HTML parsing, rate limiting
- **INFO**: Collection progress, events count, exports
- **WARNING**: Retry attempts, validation issues
- **ERROR**: Failed requests, parsing errors, blocked access

## Example Workflow

```bash
# 1. Health check
python scripts/collect_forexfactory_data.py --health-check

# 2. Collect this week's events
python scripts/collect_forexfactory_data.py \
    --start 2024-02-10 \
    --end 2024-02-17 \
    --validate

# 3. Verify output
ls data/raw/forexfactory/        # Bronze CSV
ls data/processed/events/        # Silver CSV (if --preprocess used)

# 4. View collected data
head data/raw/forexfactory/forexfactory_calendar_*.csv
```

## Use Cases

### Pre-Trade Planning
Identify high-impact events before they occur:

```python
import pandas as pd

# Load processed events
df = pd.read_csv("data/processed/events/events_2024-02-10_2024-02-10.csv")

# Filter high-impact USD events
high_impact = df[
    (df["currency"] == "USD") &
    (df["impact"] == "high")
]
print(high_impact[["timestamp_utc", "event_name", "impact"]])
```

### Post-Event Analysis
Analyze surprise factors (actual vs forecast):

```python
# Calculate surprises
df["surprise"] = df["actual"] - df["forecast"]
df["surprise_pct"] = (df["surprise"] / df["forecast"].abs()) * 100

# Large surprises (> 10% deviation)
surprises = df[abs(df["surprise_pct"]) > 10]
```

### Event Calendar
Build a trading calendar for specific currency:

```python
# Next week's EUR events
eur_events = df[
    (df["currency"] == "EUR") &
    (df["impact"].isin(["high", "medium"]))
].sort_values("timestamp_utc")
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
# Run all Forex Factory collector tests
pytest tests/ingestion/test_forexfactory_collector.py -v

# Run specific test
pytest tests/ingestion/test_forexfactory_collector.py::TestForexFactoryCalendarCollector::test_initialization -v

# Run with coverage
pytest tests/ingestion/test_forexfactory_collector.py -v --cov
```

Tests use mocked HTTP responses (no actual web scraping during testing):
- HTML parsing
- Rate limiting
- Error handling
- CSV export
- Data validation
- Health checks

## Limitations

- **Time zones**: Event times are in local timezone (converted to UTC in Silver)
- **Historical data**: Limited to what's available on Forex Factory
- **Actual values**: May appear with delay after event time
- **Website changes**: Subject to HTML structure changes (requires selector updates)
- **Rate enforcement**: Aggressive scraping may result in temporary blocks
- **JavaScript rendering**: Some dynamic content may not be captured

## Comparison with Investing.com Calendar

| Feature | Forex Factory | Investing.com |
|---------|---------------|---------------|
| Coverage | All major currencies | US, EU, UK focus |
| Real-time | Yes | Yes |
| Historical | Limited | Moderate |
| Rate limiting | 3-5 seconds | 3-5 seconds |
| API | Web scraping only | Web scraping only |
| Community | Yes (forums) | No |

## Troubleshooting

### No events collected

1. Check health: `python scripts/collect_forexfactory_data.py --health-check`
2. Check logs: `logs/collectors/forexfactory_collector.log`
3. Verify internet connection
4. Check if Forex Factory is accessible in browser

### Rate limited (429)

1. Wait 5-10 minutes before retrying
2. Increase delays: `--min-delay 5 --max-delay 10`
3. Reduce date range
4. Use `--today` for minimal requests

### HTML parsing errors

1. Check if Forex Factory changed their layout
2. Review logs for specific parsing errors
3. Update selectors in `forexfactory_collector.py`
4. Run tests: `pytest tests/ingestion/test_forexfactory_collector.py -v`

### Empty CSV files

1. Verify events were collected (check logs)
2. Validate data: `--validate` flag
3. Check output directory permissions
4. Review validation errors in logs

## Maintenance

If scraping fails due to website changes:

1. Check logs: `logs/collectors/forexfactory_collector.log`
2. Inspect HTML selectors in `src/ingestion/collectors/forexfactory_collector.py`
3. Update CSS selectors if structure changed:
   - `_parse_calendar_row()` method
   - `_fetch_calendar_for_date()` method
4. Run tests: `pytest tests/ingestion/test_forexfactory_collector.py -v`
5. Test manually: `python scripts/collect_forexfactory_data.py --today --verbose`

## Architecture

### Class Structure

```python
ForexFactoryCalendarCollector(BaseCollector)
├── SOURCE_NAME = "forexfactory"
├── __init__()                       # Initialize with rate limiting
├── _load_robots_txt()              # Fetch robots.txt
├── _get_random_delay()             # Rate limiting jitter
├── _get_random_user_agent()        # User agent rotation
├── _apply_rate_limit()             # Enforce delays
├── _is_calendar_access_allowed()   # robots.txt check
├── _init_driver()                  # Initialize Selenium WebDriver
├── _set_timezone_to_gmt()          # Configure timezone via UI
├── _fetch_page_with_selenium()     # Selenium with human scrolling
├── _make_request()                 # HTTP with retry logic (fallback)
├── _parse_impact_level()           # Parse impact from HTML
├── _clean_value()                  # Clean scraped values
├── _parse_calendar_row()           # Parse event row (10/11 cell formats)
├── _parse_calendar_page()          # Parse full calendar page
├── _is_event_in_range()            # Filter events by date range
├── _fetch_calendar_by_url()        # Fetch day/week/month view
├── _fetch_calendar_data()          # Fetch date range (smart strategy)
├── collect_events()                # Public collection method
├── save_to_csv()                   # Export to Bronze
├── get_events_dataframe()          # Convert to DataFrame
├── validate_scraped_data()         # Data quality check
├── collect()                       # BaseCollector interface
├── health_check()                  # BaseCollector interface
└── close()                         # Cleanup Selenium resources
```

### Rate Limiting Flow

```
Request Start
    ↓
Apply Rate Limit (3-5s delay)
    ↓
Check robots.txt
    ↓
Rotate User-Agent
    ↓
HTTP Request
    ↓
Success? → Return Response
    ↓ No
Check Status Code
    ↓
429? → Exit Gracefully (no retry)
    ↓
503? → Exponential Backoff Retry (max 2)
    ↓
Other Error? → Retry with Backoff (max 2)
    ↓
Max Retries? → Return None
```

## Contributing

When contributing to the Forex Factory collector:

1. **Test thoroughly**: Use mocked responses, never hit live site in tests
2. **Respect rate limits**: Keep delays conservative (3-5 seconds minimum)
3. **Handle errors gracefully**: Don't escalate bans with aggressive retries
4. **Update documentation**: Keep this doc in sync with code changes
5. **Follow patterns**: Use same structure as other collectors

## References

- **Website**: https://www.forexfactory.com/calendar
- **Robots.txt**: https://www.forexfactory.com/robots.txt
- **Collector**: `src/ingestion/collectors/forexfactory_collector.py`
- **Script**: `scripts/collect_forexfactory_data.py`
- **Tests**: `tests/ingestion/test_forexfactory_collector.py`
- **Base Class**: `src/ingestion/collectors/base_collector.py`
