# ECB Data Collection

European Central Bank (ECB) provides EUR exchange rates and monetary policy interest rates via the official SDMX 2.1 REST API.

## Overview

**Data Type**: Exchange rates & policy rates
**Source**: ECB Data Portal (official SDMX API)
**Update Frequency**: Daily (exchange rates), Event-based (policy rates)
**Setup Required**: None (public API)
**Output**: Bronze CSV → Silver Parquet (rates) / CSV (macro)

## Setup

No API key or account required. Verify connectivity:

```bash
python -m scripts.collect_ecb_data --health-check
```

Expected output:
```
[INFO] Health check: PASSED
```

## Data Sets

### 1. Exchange Rates
EUR reference rates against major currencies.

| Currency Pair | Frequency | Description |
|--------------|-----------|-------------|
| EUR/USD | Daily | Euro to US Dollar |
| EUR/GBP | Daily | Euro to British Pound |
| EUR/JPY | Daily | Euro to Japanese Yen |
| EUR/CHF | Daily | Euro to Swiss Franc |

### 2. Policy Rates
ECB key interest rates (event-based, changes only).

| Rate Type | Description |
|-----------|-------------|
| Deposit Facility Rate (DFR) | Rate for overnight deposits |
| Main Refinancing Operations (MRO) | Primary lending rate |
| Marginal Lending Facility (MLF) | Emergency lending rate |

## Usage

### Command Line (Recommended)

#### Bronze Only
```bash
python -m scripts.collect_ecb_data
```

#### Bronze + Silver (Full Pipeline)
```bash
python -m scripts.collect_ecb_data --preprocess
```

#### Specific Dataset
```bash
# Exchange rates only
python -m scripts.collect_ecb_data --dataset exchange_rates --preprocess

# Policy rates only
python -m scripts.collect_ecb_data --dataset policy_rates --preprocess
```

#### Custom Date Range
```bash
python -m scripts.collect_ecb_data --start 2023-01-01 --end 2023-12-31 --preprocess
```

#### All Options
```bash
python -m scripts.collect_ecb_data --help

Options:
  --start DATE          Start date (YYYY-MM-DD), default: 2 years ago
  --end DATE            End date (YYYY-MM-DD), default: today
  --dataset {exchange_rates,policy_rates,all}
                        Dataset filter, default: all
  --preprocess          Run Silver preprocessing after collection
  --health-check        Verify API connectivity only
  --verbose, -v         Enable debug logging
```

### Programmatic Usage

```python
from datetime import datetime
from src.ingestion.collectors.ecb_collector import ECBCollector

# Initialize collector
collector = ECBCollector()

# Collect all datasets
data = collector.collect(
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2023, 12, 31)
)

# Access datasets
exchange_rates = data["exchange_rates"]
policy_rates = data["policy_rates"]

# Export Bronze CSV
for name, df in data.items():
    collector.export_csv(df, name)
```

#### Individual Datasets
```python
# Collect only exchange rates
exchange_rates = collector.collect_exchange_rates()

# Collect only policy rates
policy_rates = collector.collect_policy_rates()
```

## Output Schema

### Bronze Layer (Raw)

**Location**: `data/raw/ecb/`
**Format**: CSV
**Filenames**:
- `ecb_exchange_rates_{YYYYMMDD}.csv`
- `ecb_policy_rates_{YYYYMMDD}.csv`

#### Exchange Rates CSV
| Column | Type | Description |
|--------|------|-------------|
| `date` | date | Exchange rate date |
| `currency_pair` | string | e.g., EUR/USD |
| `rate` | float | Exchange rate |
| `frequency` | string | \"D\" (daily) |
| `source` | string | \"ecb\" |

#### Policy Rates CSV
| Column | Type | Description |
|--------|------|-------------|
| `date` | date | Rate effective date |
| `rate_type` | string | DFR, MRO, or MLF |
| `rate` | float | Interest rate (percent) |
| `frequency` | string | \"B\" (business/event) |
| `unit` | string | \"Percent\" |
| `source` | string | \"ecb\" |

### Silver Layer (Normalized)

**Exchange Rates → OHLCV**
**Location**: `data/processed/ohlcv/`
**Format**: Parquet
**Preprocessor**: `PriceNormalizer`

Daily exchange rates are converted to OHLCV format (open=high=low=close=rate):

| Column | Type | Description |
|--------|------|-------------|
| `timestamp_utc` | datetime | ISO 8601 UTC |
| `pair` | string | Currency pair (EURUSD format) |
| `timeframe` | string | \"D1\" |
| `open` | float | Rate |
| `high` | float | Rate |
| `low` | float | Rate |
| `close` | float | Rate |
| `volume` | float | 0.0 |

**Policy Rates → Macro**
**Location**: `data/processed/macro/`
**Format**: CSV
**Preprocessor**: `MacroNormalizer`

| Column | Type | Description |
|--------|------|-------------|
| `timestamp_utc` | datetime | ISO 8601 UTC |
| `series_id` | string | ECB_DFR, ECB_MRO, ECB_MLF |
| `value` | float | Interest rate |
| `source` | string | \"ecb\" |
| `frequency` | string | \"B\" (event-based) |
| `units` | string | \"Percent\" |

## Incremental Updates

Exchange rates support incremental updates (fetch only new data):

```python
from datetime import datetime

# Fetch only data updated since timestamp
last_update = datetime(2024, 2, 1, 12, 0, 0)
results = collector.incremental_update(last_update)
```

**Note**: Policy rates do not support incremental updates (ECB API limitation). They are always fetched in full.

## Rate Limiting

- **Politeness delay**: 1 second between requests
- **Retry logic**: 3 attempts with exponential backoff (1s, 2s, 4s)
- **Timeout**: 30 seconds per request

## Error Handling

| Error | Behavior |
|-------|----------|
| HTTP 404 | ValueError (invalid dataset key) |
| HTTP 429/5xx | Automatic retry with backoff |
| Network timeout | Raises `requests.exceptions.Timeout` |
| Empty response | Returns empty DataFrame, logs warning |
| Invalid values | Dropped with warning (pd.to_numeric coerce) |

## Health Check

```bash
python -m scripts.collect_ecb_data --health-check
```

Verifies:
- ✅ ECB API is responding
- ✅ Network connectivity
- ✅ Data portal availability

## Logging

**Console**: INFO level
**File**: `logs/collectors/ecb_collector.log` (DEBUG level)

Enable verbose mode:
```bash
python -m scripts.collect_ecb_data --verbose
```

## Example Workflow

```bash
# 1. Health check
python -m scripts.collect_ecb_data --health-check

# 2. Collect all datasets (Bronze + Silver)
python -m scripts.collect_ecb_data --preprocess

# 3. Verify output
ls data/raw/ecb/                # Bronze CSV
ls data/processed/ohlcv/        # Exchange rates (Parquet)
ls data/processed/macro/        # Policy rates (CSV)
```

## API Reference

- **ECB Data Portal**: https://data.ecb.europa.eu/
- **API Documentation**: https://data.ecb.europa.eu/help/api/data
- **SDMX Standard**: https://sdmx.org/

### Dataflows Used
- **EXR**: Exchange Rates (`D.USD+GBP+JPY+CHF.EUR.SP00.A`)
- **FM**: Financial Markets / Policy Rates (`B.U2.EUR.4F.KR.MRR_FR+DFR+MRR_MBR.LEV`)

## Requirements

```
pandas>=2.0
requests>=2.31
urllib3>=2.0
```

## Testing

```bash
pytest tests/ingestion/test_ecb_collector.py -v
```

All tests use mocked HTTP responses (no actual API calls).

## Limitations

- Policy rates are event-based (only rate change dates, not daily)
- Incremental updates not supported for policy rates
- Marginal Lending Facility Rate may have gaps in historical data
- Exchange rates are reference rates (not real-time trading rates)
