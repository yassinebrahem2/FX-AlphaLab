# Data Ingestion Overview

FX-AlphaLab implements a production-grade data ingestion system with a standardized two-stage pipeline architecture.

## Architecture

### Two-Stage Pipeline

```
┌─────────────────────────────────────────────────────────────┐
│  BRONZE LAYER (Raw Collection)                              │
│  • Fetch data from source APIs                              │
│  • Preserve all source fields                               │
│  • Save to data/raw/[source]/                               │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  SILVER LAYER (Normalization)                               │
│  • Standardized schema across all sources                   │
│  • UTC timestamps, lowercase snake_case                     │
│  • Save to data/processed/[category]/                       │
└─────────────────────────────────────────────────────────────┘
```

### Library Pattern

All collectors and preprocessors are **pure Python libraries**:
- No CLI logic in module files
- Reusable in notebooks, APIs, tests, agents
- CLI orchestration handled by `scripts/collect_*_data.py`

## Data Sources

| Source | Type | Update Frequency | Setup Required |
|--------|------|------------------|----------------|
| **FRED** | Macroeconomic indicators | Daily | API key (free) |
| **ECB** | Exchange rates & policy rates | Daily | None |
| **MT5** | FX price data (OHLCV) | Real-time | Windows + MT5 terminal |
| **Calendar** | Economic events | Real-time | None |

## Quick Start

### 1. Install Dependencies

```bash
pip install -e ".[dev]"
```

### 2. Configure Environment

Create `.env` file:
```bash
# Required for FRED
FRED_API_KEY=your_fred_api_key_here

# Database (for Silver layer storage)
DATABASE_URL=postgresql://user:pass@localhost:5432/fx_alphalab
```

### 3. Collect Data

Each source has a dedicated script in `scripts/`:

```bash
# FRED macroeconomic data
python -m scripts.collect_fred_data --preprocess

# ECB exchange rates and policy rates
python -m scripts.collect_ecb_data --preprocess

# MT5 FX prices (Windows only)
python -m scripts.collect_mt5_data --preprocess

# Economic calendar events
python -m scripts.collect_calendar_data --today --preprocess
```

## Output Directories

```
data/
├── raw/                    # Bronze layer (source format)
│   ├── fred/              # FRED CSV files
│   ├── ecb/               # ECB CSV files
│   ├── mt5/               # MT5 CSV files
│   └── calendar/          # Calendar CSV files
│
└── processed/             # Silver layer (normalized)
    ├── macro/             # Macroeconomic indicators (CSV)
    ├── ohlcv/             # Price data (Parquet)
    └── events/            # Economic events (CSV)
```

## Schema Standards

### Bronze Layer (Raw)
- Preserve all source fields
- Snake_case column names
- UTF-8 encoding
- CSV format
- Filename: `{source}_{dataset}_{YYYYMMDD}.csv`

### Silver Layer (Normalized)
- Standard columns: `timestamp_utc`, `series_id`/`pair`, `value`/`open`/`high`/`low`/`close`
- ISO 8601 timestamps (UTC)
- Lowercase snake_case
- CSV for macro/events, Parquet for OHLCV
- Filename: `{source}_{identifier}_{start}_{end}.{ext}`

## Common Operations

### Health Checks

Verify API connectivity:
```bash
python -m scripts.collect_fred_data --health-check
python -m scripts.collect_ecb_data --health-check
python -m scripts.collect_mt5_data --health-check
python -m scripts.collect_calendar_data --health-check
```

### Date Ranges

All scripts support custom date ranges:
```bash
python -m scripts.collect_fred_data --start 2023-01-01 --end 2023-12-31 --preprocess
```

### Programmatic Usage

Import collectors as libraries:
```python
from src.ingestion.collectors.fred_collector import FREDCollector
from src.ingestion.preprocessors.macro_normalizer import MacroNormalizer

# Bronze collection
collector = FREDCollector()
data = collector.collect()

# Silver preprocessing
normalizer = MacroNormalizer()
silver_data = normalizer.preprocess()
```

## Testing

Run all ingestion tests:
```bash
pytest tests/ingestion/ -v
```

Run specific collector tests:
```bash
pytest tests/ingestion/test_fred_collector.py -v
pytest tests/ingestion/test_ecb_collector.py -v
pytest tests/ingestion/test_mt5_collector.py -v
pytest tests/ingestion/test_calendar_collector.py -v
```

## Documentation

Detailed guides for each data source:
- [FRED](fred.md) - Federal Reserve Economic Data
- [ECB](ecb.md) - European Central Bank
- [MT5](mt5.md) - MetaTrader 5 (FX prices)
- [Calendar](calendar.md) - Economic calendar events

## Logging

All collectors log to:
- Console: INFO level and above
- File: `logs/collectors/{source}_collector.log` (DEBUG level)

Configure logging level:
```bash
python -m scripts.collect_fred_data --verbose  # Enable DEBUG logs
```

## Error Handling

All collectors implement:
- Automatic retry with exponential backoff
- Graceful degradation (partial failures continue)
- Comprehensive error messages
- Full audit trail in logs

## Rate Limiting

Each source respects API limits:
- **FRED**: 120 requests/minute (automatic throttling)
- **ECB**: 1 second delay between requests
- **MT5**: 0.5 second delay between symbol fetches
- **Calendar**: 3-5 second random delays + robots.txt compliance
