# FRED Data Collector

## Overview

The FRED (Federal Reserve Economic Data) collector retrieves macroeconomic indicators and financial stress indices from the St. Louis Federal Reserve's FRED API. This is one of the primary data sources for fundamental analysis in the FX-AlphaLab system.

## Features

- Official FRED API integration via `fredapi` package
- Retrieves 4 key macroeconomic series
- Local JSON caching to minimize API calls
- Automatic rate limiting (120 calls/min)
- Comprehensive error handling
- Batch retrieval support
- CSV export following §3.4 normalized macro format

## Data Series

The collector retrieves the following series by default:

| Series ID | Name | Description | Frequency | Units |
|-----------|------|-------------|-----------|-------|
| STLFSI4 | Financial Stress | St. Louis Fed Financial Stress Index | Weekly | Index |
| DFF | Fed Funds Rate | Federal Funds Effective Rate | Daily | Percent |
| CPIAUCSL | CPI | Consumer Price Index for All Urban Consumers | Monthly | Index (1982-84=100) |
| UNRATE | Unemployment | Unemployment Rate | Monthly | Percent |

## Prerequisites

### 1. Get FRED API Key

1. Visit https://fred.stlouisfed.org/docs/api/api_key.html
2. Create a free account (instant approval)
3. Copy your API key

### 2. Configure Environment

Add your API key to `.env`:

```bash
FRED_API_KEY=your_actual_api_key_here
```

### 3. Install Dependencies

The collector uses the `fredapi` package, which is already included in the project dependencies:

```bash
pip install -e ".[dev]"
```

## Usage

### Basic Collection

```python
from datetime import datetime
from data.ingestion.fred_collector import FREDCollector

# Initialize collector
collector = FREDCollector()

# Collect all predefined series
data = collector.collect(
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2023, 12, 31)
)

# Access individual series
financial_stress = data["financial_stress"]
fed_funds = data["federal_funds_rate"]
cpi = data["cpi"]
unemployment = data["unemployment_rate"]
```

### Single Series Retrieval

```python
# Get a specific series
df = collector.get_series(
    "DFF",  # Federal Funds Rate
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2023, 12, 31)
)

print(df.head())
#             timestamp_utc series_id  value source frequency     units
# 0  2023-01-01T00:00:00Z       DFF   4.33   fred         D   Percent
# 1  2023-01-02T00:00:00Z       DFF   4.33   fred         D   Percent
# ...
```

### Batch Retrieval

```python
# Get multiple custom series
series_ids = ["DFF", "UNRATE", "GDP", "CPIAUCSL"]
data = collector.get_multiple_series(
    series_ids,
    start_date=datetime(2020, 1, 1)
)

for series_id, df in data.items():
    print(f"{series_id}: {len(df)} records")
```

### Export to CSV

#### Per-Series Export (Recommended for Data Pipeline)

```python
# Collect and export each series to separate CSV files
paths = collector.export_all_to_csv(
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2023, 12, 31)
)

for series_name, path in paths.items():
    print(f"{series_name} -> {path}")
# Output:
# STLFSI4 -> data/datasets/macro/fred_STLFSI4_2023-01-01_2023-12-31.csv
# DFF -> data/datasets/macro/fred_DFF_2023-01-01_2023-12-31.csv
# CPIAUCSL -> data/datasets/macro/fred_CPIAUCSL_2023-01-01_2023-12-31.csv
# UNRATE -> data/datasets/macro/fred_UNRATE_2023-01-01_2023-12-31.csv
```

#### Consolidated Export (Single File)

```python
# Export all series to a single consolidated CSV file
path = collector.export_consolidated_csv(
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2023, 12, 31),
    filename="fred_indicators"  # optional, defaults to "fred_indicators"
)

print(f"Consolidated file: {path}")
# Output: data/datasets/macro/fred_indicators_20260209.csv

# The consolidated CSV includes a 'series_name' column to distinguish between series
# Columns: [timestamp_utc, series_id, value, source, frequency, units, series_name]
```

### Health Check

```python
if collector.health_check():
    print("FRED API is available")
    data = collector.collect()
else:
    print("FRED API is unavailable")
```

## Caching

The collector implements a local JSON caching mechanism to:
- Minimize redundant API calls
- Speed up repeated queries
- Reduce risk of hitting rate limits

### Cache Behavior

- Cache files stored in `data/cache/fred/`
- Cache validity: 1 day
- Cache keys: series ID + date range
- Automatic cache invalidation after expiry

### Cache Management

```python
# Use cache (default)
df = collector.get_series("DFF", use_cache=True)

# Bypass cache
df = collector.get_series("DFF", use_cache=False)

# Clear cache for specific series
collector.clear_cache("DFF")

# Clear all cache
collector.clear_cache()
```

## Rate Limiting

FRED API allows **120 requests per minute**. The collector automatically:

1. Tracks time between requests
2. Enforces minimum 0.5s interval between calls
3. Logs rate limit compliance

This is handled transparently—no manual throttling needed.

## Error Handling

### Invalid Series ID

```python
try:
    df = collector.get_series("INVALID_SERIES_ID")
except ValueError as e:
    print(f"Error: {e}")
    # Error: Invalid FRED series ID 'INVALID_SERIES_ID'
```

### Network Failures

The collector uses `fredapi` which handles HTTP errors and retries. Network issues are logged and raised as exceptions.

### Missing API Key

```python
# Without API key in .env
try:
    collector = FREDCollector()
except ValueError as e:
    print(e)
    # FRED API key is required. Set FRED_API_KEY in .env...
```

## Output Format

All DataFrames follow the §3.4 normalized macro format:

| Column | Type | Description |
|--------|------|-------------|
| timestamp_utc | string | UTC timestamp (ISO 8601: YYYY-MM-DDTHH:MM:SSZ) |
| series_id | string | FRED series identifier |
| value | float | Indicator value |
| source | string | Always "fred" |
| frequency | string | D=Daily, W=Weekly, M=Monthly |
| units | string | Measurement units |

### CSV Naming Convention

Files are named following the §3.4 pattern:

```
{source}_{series_id}_{start_date}_{end_date}.csv
```

Example:
```
fred_DFF_2023-01-01_2023-12-31.csv
fred_CPIAUCSL_2023-01-01_2023-12-31.csv
```

## Advanced Usage

### Custom Series

```python
# Retrieve any FRED series by ID
vix = collector.get_series(
    "VIXCLS",  # CBOE Volatility Index
    start_date=datetime(2023, 1, 1)
)

gold = collector.get_series(
    "GOLDAMGBD228NLBM",  # Gold Price
    start_date=datetime(2023, 1, 1)
)
```

### Custom Output Directory

```python
from pathlib import Path

collector = FREDCollector(
    output_dir=Path("custom/output/path"),
    cache_dir=Path("custom/cache/path")
)
```

### Integration with Scheduled Jobs

```python
from datetime import datetime, timedelta

def daily_fred_update():
    """Daily FRED data collection job."""
    collector = FREDCollector()

    # Check API availability
    if not collector.health_check():
        logger.error("FRED API unavailable")
        return

    # Collect last 7 days (to catch revisions)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)

    data = collector.collect(start_date=start_date, end_date=end_date)
    paths = collector.export_all_to_csv(data=data)

    logger.info(f"Exported {len(paths)} files")
    return paths
```

## Finding Series IDs

To find additional FRED series:

1. Visit https://fred.stlouisfed.org/
2. Search for your indicator (e.g., "GDP", "inflation", "unemployment")
3. The series ID is in the URL: `https://fred.stlouisfed.org/series/SERIES_ID`

Popular series:
- `GDP` - Gross Domestic Product
- `DEXUSEU` - US / Euro Foreign Exchange Rate
- `T10Y2Y` - 10-Year Treasury Constant Maturity Minus 2-Year
- `BAMLH0A0HYM2` - ICE BofA US High Yield Index

## API Limits

| Limit Type | Value | Notes |
|------------|-------|-------|
| Requests/minute | 120 | Automatically enforced |
| Daily requests | No limit | As of 2024 |
| Historical data | Varies | Most series have decades of data |
| API cost | Free | No usage fees |

## Troubleshooting

### Problem: "FRED API key is required"

**Solution:** Add your API key to `.env`:
```bash
FRED_API_KEY=your_actual_key_here
```

### Problem: Empty DataFrame returned

**Possible causes:**
1. Series ID doesn't exist
2. Date range outside available data
3. Network issues

**Solution:**
```python
# 1. Verify series exists at https://fred.stlouisfed.org/series/SERIES_ID
# 2. Check series info
info = collector._fred.get_series_info("DFF")
print(f"Available: {info['observation_start']} to {info['observation_end']}")

# 3. Check health
assert collector.health_check(), "API unavailable"
```

### Problem: Rate limit warnings

**Solution:** The collector handles this automatically, but if you see warnings:
- Reduce concurrent requests
- Increase `MIN_REQUEST_INTERVAL` if needed
- Use caching more aggressively

### Problem: ValueError "start_date must be before end_date"

**Cause:** Invalid date range provided.

**Solution:**
```python
# Ensure start_date < end_date
start = datetime(2023, 1, 1)
end = datetime(2023, 12, 31)
collector.collect(start_date=start, end_date=end)
```

### Problem: ValueError "series_id cannot be empty"

**Cause:** Empty or whitespace-only series_id passed to `get_series()`.

**Solution:**
```python
# Use valid FRED series ID
collector.get_series("DFF")  # Valid
# collector.get_series("")    # Invalid
# collector.get_series("   ") # Invalid
```

### Problem: ValueError "series_ids list cannot be empty"

**Cause:** Empty list passed to `get_multiple_series()`.

**Solution:**
```python
# Provide at least one series ID
collector.get_multiple_series(["DFF", "UNRATE"])  # Valid
# collector.get_multiple_series([])               # Invalid
```

## Testing

Run the test suite:

```bash
# All FRED tests
pytest tests/test_fred_collector.py -v

# Specific test class
pytest tests/test_fred_collector.py::TestGetSeries -v

# With coverage
pytest tests/test_fred_collector.py --cov=data.ingestion.fred_collector
```

## Architecture

```
FREDCollector
├── Inherits: BaseCollector
├── Dependencies:
│   ├── fredapi.Fred (official FRED client)
│   ├── pandas (data manipulation)
│   └── shared.config (API key management)
├── Features:
│   ├── Rate limiting (120/min)
│   ├── JSON caching (1 day TTL)
│   ├── Batch retrieval
│   └── CSV export (§3.4 compliant)
└── Output:
    ├── data/datasets/macro/*.csv
    └── data/cache/fred/*.json
```

## References

- **FRED API Docs:** https://fred.stlouisfed.org/docs/api/
- **FRED Python Package:** https://github.com/mortada/fredapi
- **Series Search:** https://fred.stlouisfed.org/
- **Get API Key:** https://fred.stlouisfed.org/docs/api/api_key.html

## Related

- [`data/ingestion/ecb_collector.py`](../../data/ingestion/ecb_collector.py) - ECB data collector
- [`data/ingestion/base_collector.py`](../../data/ingestion/base_collector.py) - Base collector interface
- [`tests/test_fred_collector.py`](../../tests/test_fred_collector.py) - Test suite
