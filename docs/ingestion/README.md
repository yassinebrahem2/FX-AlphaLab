# Data Ingestion Documentation

Production-grade data collection system for FX-AlphaLab with standardized two-stage Bronze/Silver pipeline.

## Quick Links

- **[Overview](overview.md)** - Architecture, setup, and common operations
- **[FRED](fred.md)** - Federal Reserve macroeconomic data
- **[ECB](ecb.md)** - European Central Bank exchange rates & policy rates
- **[MT5](mt5.md)** - MetaTrader 5 FX price data (Windows only)
- **[Calendar](calendar.md)** - Economic calendar events from Investing.com

## Quick Start

### 1. Install
```bash
pip install -e ".[dev]"
```

### 2. Configure
```bash
# .env file
FRED_API_KEY=your_fred_api_key_here
```

### 3. Collect Data
```bash
# FRED macroeconomic indicators
python -m scripts.collect_fred_data --preprocess

# ECB exchange rates and policy rates
python -m scripts.collect_ecb_data --preprocess

# MT5 FX prices (Windows only)
python -m scripts.collect_mt5_data --preprocess

# Economic calendar events
python -m scripts.collect_calendar_data --today --preprocess
```

## Data Pipeline

```
Source API → Bronze (raw CSV) → Silver (normalized Parquet/CSV)
```

All collectors support:
- ✅ Health checks (`--health-check`)
- ✅ Custom date ranges (`--start`, `--end`)
- ✅ Verbose logging (`--verbose`)
- ✅ Programmatic usage (import as libraries)

## Output Structure

```
data/
├── raw/                  # Bronze layer
│   ├── fred/
│   ├── ecb/
│   ├── mt5/
│   └── calendar/
└── processed/            # Silver layer
    ├── macro/
    ├── ohlcv/
    └── events/
```

## Testing

```bash
pytest tests/ingestion/ -v
```

## Support

For issues or questions, see individual collector documentation.
