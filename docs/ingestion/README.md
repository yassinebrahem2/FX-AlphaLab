# Data Ingestion Documentation

Production-grade data collection system for FX-AlphaLab with standardized two-stage Bronze/Silver pipeline.

## Quick Links

- **[Overview](overview.md)** - Architecture, setup, and common operations
- **[FRED](fred.md)** - Federal Reserve macroeconomic data
- **[ECB Rates](ecb_rates.md)** - European Central Bank exchange rates & policy rates
- **[ECB News](ecb_news.md)** - European Central Bank press releases & speeches
- **[MT5](mt5.md)** - MetaTrader 5 FX price data (Windows only)
- **[Calendar](calendar.md)** - Economic calendar events from Investing.com
- **[Forex Factory](forexfactory.md)** - Economic calendar events from Forex Factory

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

# ECB news and sentiment data
python -m scripts.collect_ecb_news_data

# MT5 FX prices (Windows only)
python -m scripts.collect_mt5_data --preprocess

# Economic calendar events
python -m scripts.collect_calendar_data --today --preprocess

# Forex Factory calendar events
python -m scripts.collect_forexfactory_data --today
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
│   ├── calendar/         # Investing.com events
│   ├── forexfactory/     # Forex Factory events
│   ├── fred/             # FRED macro data
│   ├── mt5/              # MT5 price data
│   └── ecb/              # ECB data
└── processed/            # Silver layer
    ├── macro/            # Normalized macro indicators
    ├── ohlcv/            # Price data
    ├── events/           # Economic events
    └── sentiment/        # Sentiment analysis
```

## Testing

```bash
pytest tests/ingestion/ -v
```

## Support

For issues or questions, see individual collector documentation.
