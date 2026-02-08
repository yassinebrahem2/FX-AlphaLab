# ECB Data Collector

Production-grade ECB data collection via the official ECB Data Portal API (SDMX 2.1 REST).

Implements the `BaseCollector` interface defined in [base_collector.py](base_collector.py) and follows the §3.1 raw CSV contract.

## Features

- **Policy Rates Collection**: ECB key interest rates (deposit facility, main refinancing operations)
- **Exchange Rates Collection**: EUR reference rates vs USD, GBP, JPY, CHF
- **Incremental Updates**: Uses `updatedAfter` parameter for efficient syncing of exchange rates
- **Production-Grade Error Handling**: Automatic retry logic with exponential backoff (3 retries, 2.0s backoff)
- **Vectorized Processing**: Fast pandas operations (no `iterrows()`)
- **Comprehensive Logging**: Full audit trail logged to `logs/ecb_collector.log`
- **Health Check**: API availability verification

## Usage

### Basic Collection (Last 2 Years)

```python
from data.ingestion.ecb_collector import ECBCollector

collector = ECBCollector()
results = collector.collect()

print(f"Policy rates: {len(results['policy_rates'])} records")
print(f"Exchange rates: {len(results['exchange_rates'])} records")

# Export to CSV (§3.1 naming: ecb_policy_rates_YYYYMMDD.csv)
for name, df in results.items():
    if not df.empty:
        collector.export_csv(df, name)
```

**Output location**: `datasets/ecb/raw/ecb_policy_rates_20260208.csv`, `datasets/ecb/raw/ecb_exchange_rates_20260208.csv`

### Custom Date Range

```python
from datetime import datetime
from data.ingestion.ecb_collector import ECBCollector

collector = ECBCollector()

# Collect specific date range
results = collector.collect(
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2024, 12, 31),
)

# Export
for name, df in results.items():
    collector.export_csv(df, name)
```

### Incremental Updates

```python
from datetime import datetime
from data.ingestion.ecb_collector import ECBCollector

collector = ECBCollector()

# Only fetch data updated since last collection
# Note: Policy rates are always fetched in full (FM dataflow limitation)
last_update = datetime(2024, 2, 1, 12, 0, 0)
results = collector.incremental_update(last_update)
```

### Individual Datasets

```python
from data.ingestion.ecb_collector import ECBCollector

collector = ECBCollector()

# Collect only policy rates
policy_rates = collector.collect_policy_rates()
collector.export_csv(policy_rates, "policy_rates")

# Collect only exchange rates
exchange_rates = collector.collect_exchange_rates()
collector.export_csv(exchange_rates, "exchange_rates")
```

### Custom Output Directory

```python
from pathlib import Path
from data.ingestion.ecb_collector import ECBCollector

collector = ECBCollector(output_dir=Path("/custom/output/dir"))
results = collector.collect()
```

### Health Check

```python
from data.ingestion.ecb_collector import ECBCollector

collector = ECBCollector()
if collector.health_check():
    results = collector.collect()
else:
    print("ECB API is unavailable")
```

### CLI Usage

```bash
# Collect all datasets (default: last 2 years)
python -m data.ingestion.ecb_collector

# Custom date range
python -m data.ingestion.ecb_collector \
    --start-date 2023-01-01 \
    --end-date 2024-12-31

# Incremental update (exchange rates only, policy rates always full)
python -m data.ingestion.ecb_collector \
    --updated-after "2024-02-01T12:00:00"

# Custom output directory
python -m data.ingestion.ecb_collector \
    --output-dir /path/to/output

# Skip CSV export (return DataFrames only)
python -m data.ingestion.ecb_collector --no-export
```

## Output Schema (Raw CSV - §3.1)

Raw collector output follows the §3.1 contract: preserves all source fields, snake_case column names, UTF-8 encoding.

### Policy Rates CSV

**File**: `datasets/ecb/raw/ecb_policy_rates_YYYYMMDD.csv`

| Column | Type | Description |
|--------|------|-------------|
| `date` | date | Rate effective date (event-based) |
| `rate_type` | string | Rate name (Deposit Facility Rate, Main Refinancing Operations Rate, Marginal Lending Facility Rate) |
| `rate` | float | Interest rate in percent |
| `frequency` | string | Data frequency (B=Business/event-based) |
| `unit` | string | Unit of measurement (Percent) |
| `source` | string | Data source (ECB) |

### Exchange Rates CSV

**File**: `datasets/ecb/raw/ecb_exchange_rates_YYYYMMDD.csv`

| Column | Type | Description |
|--------|------|-------------|
| `date` | date | Exchange rate date (daily) |
| `currency_pair` | string | Currency pair (e.g., EUR/USD, EUR/GBP, EUR/JPY, EUR/CHF) |
| `rate` | float | Exchange rate |
| `frequency` | string | Data frequency (D=Daily) |
| `source` | string | Data source (ECB) |

> **Note**: These are raw outputs. The `data/preprocessing/` normalizers will transform these into the standardized §3.2-3.7 format with `timestamp_utc` columns.

## Architecture

```
ECBCollector (inherits BaseCollector)
│
├── collect()                 # BaseCollector interface - collects all datasets
├── health_check()            # BaseCollector interface - verifies API availability
├── export_csv()              # BaseCollector method - exports with §3.1 naming
│
├── collect_policy_rates()    # Domain-specific method
├── collect_exchange_rates()  # Domain-specific method
├── incremental_update()      # Domain-specific method
│
└── Private methods:
    ├── _fetch()              # HTTP layer
    ├── _process_policy_rates()   # Vectorized transformation
    └── _process_exchange_rates() # Vectorized transformation
```

## ECB API Details

### Dataflows Used

- **FM** (Financial Markets): ECB policy rates
  - Key: `B.U2.EUR.4F.KR.MRR_FR+DFR+MRR_MBR.LEV`
  - Frequency: Event-based (B) - actual rate change dates only
  - Rates: DFR (Deposit Facility), MRO (Main Refinancing Operations), MLF (Marginal Lending Facility)
  - **Limitation**: `updatedAfter` not supported by ECB for event-based dataflows

- **EXR** (Exchange Rates): EUR reference rates
  - Key: `D.USD+GBP+JPY+CHF.EUR.SP00.A`
  - Frequency: Daily (D)
  - Type: SP00 (Foreign exchange reference rates)
  - Supports `updatedAfter` for incremental updates

### API Parameters

- `startPeriod`: Start date (YYYY-MM-DD)
- `endPeriod`: End date (YYYY-MM-DD)
- `updatedAfter`: ISO 8601 timestamp for incremental updates (exchange rates only)
- `format=csvdata`: Request CSV format

### Retry Logic

- **Max retries**: 3
- **Backoff factor**: 2.0 (exponential: 1s, 2s, 4s)
- **Retry on HTTP**: 429 (rate limit), 500, 502, 503, 504 (server errors)
- **Timeout**: 30 seconds per request
- **Politeness delay**: 1 second between consecutive requests

## Error Handling

| Error | Behavior |
|-------|----------|
| **HTTP 404** (invalid dataset key) | Raises `ValueError` |
| **HTTP 429/5xx** (server errors) | Automatic retry with backoff |
| **Timeout** | Propagates `requests.exceptions.Timeout` |
| **Empty response** | Returns empty DataFrame (logs warning) |
| **Invalid numeric values** | Dropped with warning log (`pd.to_numeric(..., errors="coerce")`) |
| **Unknown rate codes** | Filtered out (only DFR, MRO, MLF preserved) |
| **Unknown currencies** | Filtered out (only USD, GBP, JPY, CHF preserved) |

## Requirements

```
pandas>=2.0
requests>=2.31
urllib3>=2.0
```

## API Reference

- **ECB Data Portal**: https://data.ecb.europa.eu/
- **API Documentation**: https://data.ecb.europa.eu/help/api/data
- **Base URL**: https://data-api.ecb.europa.eu/service/

## Implementation Notes

- **Vectorized processing**: Uses pandas `.isin()`, `.map()`, `pd.to_numeric()` for performance (no `iterrows()`)
- **Immutable constants**: Rate mappings and valid currencies are `dict` and `frozenset` class attributes
- **Separation of concerns**: HTTP fetch → raw DataFrame → process → export
- **Private session**: `_session` attribute follows Python conventions for internal implementation details
- **Type hints**: Modern Python 3.10+ syntax (`Path | None`, `dict[str, str]`)
- **Testing**: 100% coverage via mocked HTTP responses (see [tests/test_ecb_collector.py](../../tests/test_ecb_collector.py))

## Limitations

- **Marginal Lending Facility Rate**: Present in the API key but may not always be populated in the FM dataflow
- **Incremental Updates for Policy Rates**: The `updatedAfter` parameter is **not supported** by the ECB API for event-based (FM) dataflows. Policy rates are always fetched in full when using `incremental_update()`.
- **Date format**: Raw output uses `date` objects (YYYY-MM-DD), not `timestamp_utc` (ISO 8601). Normalizers convert to UTC timestamps in preprocessing.
