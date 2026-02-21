# Macro Data Collection (Unified)

Consolidated collection of macroeconomic indicators from FRED (US) and ECB (European) sources into a single output file.

## Overview

**Data Type**: Macroeconomic time series (consolidated)
**Sources**: FRED API + ECB SDMX 2.1 REST API
**Update Frequency**: Varies by series (daily, weekly, monthly)
**Setup Required**: FRED API key (free)
**Output**: Single consolidated file with both FRED and ECB data

## Why Unified Collection?

Small tabular datasets should follow the **Events pattern** (single consolidated file) rather than the OHLCV pattern (partitioned files):

- ✅ Single file simplifies querying and analysis
- ✅ One script collects from both sources
- ✅ Both sources properly labeled with `source` column
- ✅ Follows best practice for small time-series data (<10K rows)

**Old approach** (deprecated):
- 6 separate files: `macro_CPIAUCSL_*.csv`, `macro_DFF_*.csv`, `macro_ECB_DFR_*.csv`, etc.
- Required running 2 scripts separately
- Manual merge lost source attribution

**New approach** (current):
- 1 consolidated file: `macro_all_2021-01-01_2025-12-31.csv`
- Single unified script
- Both sources automatically labeled

## Setup

### 1. Get FRED API Key

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
python -m scripts.collect_macro_data --health-check
```

Expected output:
```
[INFO] FRED health check: PASSED
[INFO] ECB health check: PASSED
```

## Data Series

### FRED (US Indicators)

| Series ID | Name | Frequency | Units | Description |
|-----------|------|-----------|-------|-------------|
| `STLFSI4` | Financial Stress Index | Weekly | Index | Market stress indicator |
| `DFF` | Federal Funds Rate | Daily | Percent | Effective fed funds rate |
| `CPIAUCSL` | Consumer Price Index | Monthly | Index | Inflation measure (1982-84=100) |
| `UNRATE` | Unemployment Rate | Monthly | Percent | U.S. unemployment rate |

### ECB (European Rates)

| Series ID | Name | Frequency | Units | Description |
|-----------|------|-----------|-------|-------------|
| `ECB_DFR` | Deposit Facility Rate | Irregular | Percent | ECB deposit facility rate |
| `ECB_MRR` | Main Refinancing Rate | Irregular | Percent | ECB main refinancing operations rate |

## Usage

### Command Line (Recommended)

#### Full Pipeline (Bronze + Silver Consolidated)
```bash
python -m scripts.collect_macro_data --preprocess
```

#### Custom Date Range
```bash
python -m scripts.collect_macro_data --start 2023-01-01 --end 2023-12-31 --preprocess
```

#### Health Check Only
```bash
python -m scripts.collect_macro_data --health-check
```

#### All Options
```bash
python -m scripts.collect_macro_data --help

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
from src.ingestion.collectors.ecb_collector import ECBCollector
from src.ingestion.preprocessors.macro_normalizer import MacroNormalizer

# Initialize collectors
fred = FREDCollector()
ecb = ECBCollector()

# Collect data
fred_data = fred.collect(start_date=datetime(2023, 1, 1), end_date=datetime(2023, 12, 31))
ecb_data = ecb.collect(start_date=datetime(2023, 1, 1), end_date=datetime(2023, 12, 31))

# Normalize to Silver layer (consolidated)
normalizer = MacroNormalizer(
    bronze_dir=fred.output_dir,
    silver_dir=Path("data/processed/macro")
)
result = normalizer.process_and_export(
    source_files=fred_data + ecb_data,
    consolidated=True  # Single file output
)
```

## Output Format

### Bronze Layer
Raw CSV files in `data/raw/fred/` and `data/raw/ecb/`:
```
fred_CPIAUCSL_20250210.csv
fred_DFF_20250210.csv
fred_STLFSI4_20250210.csv
fred_UNRATE_20250210.csv
ecb_policy_rates_20250210.csv
```

### Silver Layer (Consolidated)
Single normalized file in `data/processed/macro/`:
```
macro_all_2021-01-01_2025-12-31.csv
```

**Schema**:
```csv
timestamp_utc,series_id,value,source,frequency,units
2021-01-01,CPIAUCSL,260.474,fred,monthly,index
2021-01-01,DFF,0.09,fred,daily,percent
2021-01-04,STLFSI4,-0.779,fred,weekly,index
2021-01-01,UNRATE,6.7,fred,monthly,percent
2021-01-01,ECB_DFR,-0.5,ecb,irregular,percent
2021-01-01,ECB_MRR,0.0,ecb,irregular,percent
```

**Key Features**:
- All timestamps in UTC
- Both sources labeled (`fred`, `ecb`)
- Sorted by timestamp, then series_id
- Standard schema across all series

## Data Quality

### Validation Checks

The pipeline performs automatic validation:
- ✅ UTC timezone conversion
- ✅ Duplicate timestamp removal
- ✅ Chronological ordering
- ✅ Source attribution
- ✅ Schema compliance

### Example Verification

```python
import pandas as pd

df = pd.read_csv("data/processed/macro/macro_all_2021-01-01_2025-12-31.csv")

# Check sources are present
print(df['source'].value_counts())
# fred    2205
# ecb       36

# Check all series included
print(df['series_id'].unique())
# ['CPIAUCSL', 'DFF', 'ECB_DFR', 'ECB_MRR', 'STLFSI4', 'UNRATE']

# Verify timestamp range
print(f"Start: {df['timestamp_utc'].min()}")
print(f"End: {df['timestamp_utc'].max()}")
```

## Individual Source Collection

If you need to collect from a single source only:

### FRED Only
```bash
python -m scripts.collect_fred_data --preprocess
```
See [fred.md](fred.md) for details.

### ECB Only
```bash
python -m scripts.collect_ecb_data --preprocess
```
See [ecb_rates.md](ecb_rates.md) for details.

**Note**: Individual collection still produces consolidated output (not fragmented files).

## Migration from Old Approach

If you have old fragmented files (`macro_CPIAUCSL_*.csv`, etc.):

1. Delete old files:
   ```bash
   rm data/processed/macro/macro_*_2021-01-01_2025-12-31.csv
   ```

2. Re-collect with unified script:
   ```bash
   python -m scripts.collect_macro_data --preprocess
   ```

3. Verify consolidation:
   ```bash
   ls data/processed/macro/
   # Should show only: macro_all_2021-01-01_2025-12-31.csv
   ```

## Troubleshooting

### "FRED API key not found"
- Check `.env` file exists in project root
- Verify `FRED_API_KEY=...` is set correctly
- Restart terminal to reload environment

### "No ECB data collected"
- ECB policy rates change infrequently (36 records for 2021-2025)
- This is expected behavior
- Verify Bronze files exist in `data/raw/ecb/`

### "Only FRED source in output"
- Ensure you're using `collect_macro_data.py` (unified script)
- Check both collectors ran successfully in logs
- Verify both Bronze directories have files

### Missing Dependencies
```bash
pip install -e ".[dev]"
```

## API Rate Limits

### FRED
- 120 requests per minute
- Collector automatically throttles with 0.5s delay
- Cache enabled by default to minimize API calls

### ECB
- No documented rate limits
- SDMX 2.1 REST API is public
- Cache enabled by default

## Related Documentation

- [FRED Collector](fred.md) - Individual FRED collection details
- [ECB Rates Collector](ecb_rates.md) - Individual ECB collection details
- [Data Ingestion Overview](overview.md) - Complete architecture
