# FRED Data Collection

Federal Reserve Economic Data (FRED) provides macroeconomic indicators and financial stress indices from the St. Louis Federal Reserve.

## Overview

**Data Type**: Macroeconomic time series
**Source**: FRED API (official)
**Update Frequency**: Varies by series (daily, weekly, monthly)
**Setup Required**: Free API key
**Output**: Bronze CSV → Silver normalized CSV

> **💡 Tip**: For most use cases, use the unified [macro data collector](macro.md) which combines FRED and ECB data into a single consolidated file. This document covers FRED-only collection for advanced scenarios.

## Setup

### 1. Get API Key

1. Visit https://fred.stlouisfed.org/docs/api/api_key.html
2. Create a free account (instant approval)
3. Copy your API key

### 2. Configure Environment

Add to `.env`:
```bash
FRED_API_KEY=your_actual_api_key_here
```

### 3. Verify Installation

```bash
python -m scripts.collect_fred_data --health-check
```

Expected output:
```
[INFO] Health check: PASSED
```

## Data Series

The collector retrieves 4 key macroeconomic indicators:

| Series ID | Name | Frequency | Units | Description |
|-----------|------|-----------|-------|-------------|
| `STLFSI4` | Financial Stress Index | Weekly | Index | Market stress indicator |
| `DFF` | Federal Funds Rate | Daily | Percent | Effective fed funds rate |
| `CPIAUCSL` | Consumer Price Index | Monthly | Index | Inflation measure (1982-84=100) |
| `UNRATE` | Unemployment Rate | Monthly | Percent | U.S. unemployment rate |

## Usage

### Command Line (Recommended)

#### Bronze Only
```bash
python -m scripts.collect_fred_data
```

#### Bronze + Silver (Full Pipeline)
```bash
python -m scripts.collect_fred_data --preprocess
```

#### Custom Date Range
```bash
python -m scripts.collect_fred_data --start 2023-01-01 --end 2023-12-31 --preprocess
```

#### All Options
```bash
python -m scripts.collect_fred_data --help

Options:
  --start DATE          Start date (YYYY-MM-DD), default: 2 years ago
  --end DATE            End date (YYYY-MM-DD), default: today
  --preprocess          Run Silver preprocessing after collection
  --health-check        Verify API connectivity only
  --clear-cache         Clear local cache before collecting
  --no-cache            Bypass cache, force fresh API calls
  --verbose, -v         Enable debug logging
```

### Programmatic Usage

```python
from datetime import datetime
from src.ingestion.collectors.fred_collector import FREDCollector

# Initialize collector
collector = FREDCollector()

# Collect all predefined series
data = collector.collect(
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2023, 12, 31)
)

# Access individual series
financial_stress = data["STLFSI4"]
fed_funds = data["DFF"]
cpi = data["CPIAUCSL"]
unemployment = data["UNRATE"]

# Export Bronze CSV files
paths = collector.export_all_to_csv(data=data)
```

#### Custom Series
```python
# Get a specific series
df = collector.get_series(
    "GDP",  # Real Gross Domestic Product
    start_date=datetime(2020, 1, 1)
)
```

## Output Schema

### Bronze Layer (Raw)

**Location**: `data/raw/fred/`
**Format**: CSV (one file per series)
**Filename**: `fred_{SERIES_ID}_{YYYYMMDD}.csv`

**Example**: `fred_DFF_20260210.csv`

| Column | Type | Description |
|--------|------|-------------|
| `date` | date | Observation date (YYYY-MM-DD) |
| `value` | float | Series value |
| `series_id` | string | FRED series identifier |

### Silver Layer (Normalized)

**Location**: `data/processed/macro/`
**Format**: CSV
**Filename**: `fred_{SERIES_ID}_{start}_{end}.csv`

**Example**: `fred_DFF_2023-01-01_2023-12-31.csv`

| Column | Type | Description |
|--------|------|-------------|
| `timestamp_utc` | datetime | ISO 8601 UTC timestamp |
| `series_id` | string | FRED series identifier |
| `value` | float | Observation value |
| `source` | string | Always \"fred\" |
| `frequency` | string | D (daily), W (weekly), M (monthly) |
| `units` | string | Percent, Index, etc. |

## Caching

The collector implements local JSON caching to minimize API calls:

**Cache Location**: `data/cache/fred/{series_id}.json`

- First request fetches from API and caches result
- Subsequent requests use cache (instant)
- Cache bypass: `--no-cache` flag
- Cache clear: `--clear-cache` flag

## Rate Limiting

FRED API limits: **120 requests per minute**

The collector automatically throttles requests to stay within limits.

## Error Handling

| Error | Behavior |
|-------|----------|
| Missing API key | ValueError on initialization |
| Invalid API key | API returns 400, logged as error |
| Invalid series ID | Returns empty DataFrame, logged as warning |
| Network timeout | Retries with exponential backoff |
| Rate limit hit | Automatic throttling |

## Health Check

```bash
python -m scripts.collect_fred_data --health-check
```

Verifies:
- ✅ API key is configured
- ✅ API is responding
- ✅ Network connectivity

## Logging

**Console**: INFO level (success/failure messages)
**File**: `logs/collectors/fred_collector.log` (DEBUG level)

Enable verbose mode:
```bash
python -m scripts.collect_fred_data --verbose
```

## Example Workflow

```bash
# 1. Health check
python -m scripts.collect_fred_data --health-check

# 2. Collect last 2 years (Bronze + Silver)
python -m scripts.collect_fred_data --preprocess

# 3. Verify output
ls data/raw/fred/          # Bronze CSV files
ls data/processed/macro/   # Silver CSV files
```

## API Reference

- **FRED Website**: https://fred.stlouisfed.org
- **API Docs**: https://fred.stlouisfed.org/docs/api/
- **Get API Key**: https://fred.stlouisfed.org/docs/api/api_key.html
- **Series Search**: https://fred.stlouisfed.org/categories

## Requirements

```
fredapi>=0.5.0
pandas>=2.0
python-dotenv
```

## Testing

```bash
pytest tests/ingestion/test_fred_collector.py -v
```

All tests use mocked API responses (no actual API calls).
