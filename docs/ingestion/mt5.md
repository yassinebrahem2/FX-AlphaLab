# MT5 Data Collection

MetaTrader 5 (MT5) provides historical and real-time OHLCV price data for FX pairs through the official MT5 Python API.

## Overview

**Data Type**: FX price data (OHLCV candles)
**Source**: MetaTrader 5 terminal
**Update Frequency**: Real-time
**Setup Required**: Windows + MT5 terminal + broker account
**Output**: Bronze CSV → Silver Parquet

## Prerequisites

⚠️ **Windows Only**: MT5 terminal is Windows-exclusive

### Required Software
- Windows 10/11
- MetaTrader 5 desktop terminal
- Demo or live broker account
- Python package: `MetaTrader5`

### Installation Steps

#### 1. Install MetaTrader 5

Download from https://www.metatrader5.com and run the installer.

**Important**: Desktop terminal required (not web version).

#### 2. Create Demo Account

1. Open MT5
2. File > Open an Account
3. Choose any broker
4. Select **Demo Account**
5. Complete registration and log in

Verify: You should see account number, balance, and live prices.

#### 3. Enable FX Symbols

1. Open **Market Watch** (Ctrl+M)
2. Right-click > Symbols
3. Enable these pairs:
   - EURUSD
   - GBPUSD
   - USDJPY
   - EURGBP
   - USDCHF

Verify: Bid/ask columns show updating prices.

#### 4. Test Python Connectivity

```python
import MetaTrader5 as mt5

if not mt5.initialize():
    raise RuntimeError("MT5 initialization failed")

print("MT5 version:", mt5.version())
info = mt5.account_info()
print(f"Account: {info.login}, Balance: {info.balance}")
mt5.shutdown()
```

Expected: No errors, MT5 version and account info printed.

### Setup Verification

```bash
python -m scripts.collect_mt5_data --health-check
```

Expected output:
```
[INFO] Health check: PASSED
```

## Configuration

### Default Settings

| Parameter | Default | Description |
|-----------|---------|-------------|
| Pairs | EURUSD, GBPUSD, USDJPY, EURGBP, USDCHF | FX pairs to collect |
| Timeframes | H1, H4, D1 | Bar sizes |
| Years | 3 | Historical data depth |

### Supported Timeframes

| Code | Description | Bars/Day |
|------|-------------|----------|
| `H1` | 1-hour bars | 24 |
| `H4` | 4-hour bars | 6 |
| `D1` | Daily bars | 1 |

## Usage

### Command Line (Recommended)

#### Bronze Only (Default Configuration)
```bash
python -m scripts.collect_mt5_data
```

#### Bronze + Silver (Full Pipeline)
```bash
python -m scripts.collect_mt5_data --preprocess
```

#### Custom Pairs and Timeframes
```bash
python -m scripts.collect_mt5_data \
    --pairs EURUSD,GBPUSD \
    --timeframes H1,D1 \
    --preprocess
```

#### Custom Date Range
```bash
python -m scripts.collect_mt5_data \
    --start 2022-01-01 \
    --end 2024-12-31 \
    --preprocess
```

#### Specific Years of History
```bash
python -m scripts.collect_mt5_data --years 5 --preprocess
```

#### All Options
```bash
python -m scripts.collect_mt5_data --help

Options:
  --pairs PAIRS         Comma-separated FX pairs
                        Default: EURUSD,GBPUSD,USDJPY,EURGBP,USDCHF
  --timeframes TFS      Comma-separated timeframes
                        Default: H1,H4,D1
  --years N             Years of history to fetch (default: 3)
  --start DATE          Start date (YYYY-MM-DD, overrides --years)
  --end DATE            End date (YYYY-MM-DD, default: today)
  --preprocess          Run Silver preprocessing after collection
  --health-check        Verify MT5 connectivity only
  --verbose, -v         Enable debug logging
```

### Programmatic Usage

```python
from datetime import datetime, timezone
from src.ingestion.collectors.mt5_collector import MT5Collector

# Initialize with defaults
collector = MT5Collector()

# Collect all configured pairs/timeframes
data = collector.collect()

# Export Bronze CSV
for name, df in data.items():
    collector.export_csv(df, name)
```

#### Custom Configuration
```python
# Custom pairs, timeframes, and history
collector = MT5Collector(
    pairs=["EURUSD", "GBPUSD"],
    timeframes=["H1", "D1"],
    years=5
)

# Custom date range
data = collector.collect(
    start_date=datetime(2022, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2024, 12, 31, tzinfo=timezone.utc)
)
```

## Output Schema

### Bronze Layer (Raw OHLCV)

**Location**: `data/raw/mt5/`
**Format**: CSV
**Filename**: `mt5_{PAIR}_{TIMEFRAME}_{YYYYMMDD}.csv`

**Example**: `mt5_EURUSD_H1_20260210.csv`

| Column | Type | Description |
|--------|------|-------------|
| `time` | integer | Unix timestamp (UTC) |
| `open` | float | Open price |
| `high` | float | High price |
| `low` | float | Low price |
| `close` | float | Close price |
| `tick_volume` | integer | Tick count |
| `spread` | integer | Spread in points |
| `real_volume` | integer | Real volume (0 for FX) |
| `source` | string | "mt5" |

### Silver Layer (Normalized OHLCV)

**Location**: `data/processed/ohlcv/`
**Format**: Parquet
**Preprocessor**: `PriceNormalizer`
**Filename**: `mt5_{PAIR}_{TIMEFRAME}_{start}_{end}.parquet`

**Example**: `mt5_EURUSD_H1_2022-01-01_2024-12-31.parquet`

| Column | Type | Description |
|--------|------|-------------|
| `timestamp_utc` | datetime | ISO 8601 UTC timestamp |
| `pair` | string | Currency pair (e.g., "EURUSD") |
| `timeframe` | string | Bar size (H1, H4, D1) |
| `open` | float | Open price |
| `high` | float | High price |
| `low` | float | Low price |
| `close` | float | Close price |
| `volume` | float | Tick volume |
| `source` | string | "mt5" |

## Architecture

The MT5 integration uses a two-class design:

### MT5Connector (Infrastructure)
Low-level MT5 API wrapper:
- Connection lifecycle
- Raw data fetching
- Terminal communication

### MT5Collector (Business Logic)
High-level data collection:
- BaseCollector interface
- Multi-pair/timeframe iteration
- CSV export and naming
- Error handling and logging

This separation allows:
- ✅ Testing without MT5 installation
- ✅ Easy MT5 API version updates
- ✅ Reusable connector in other contexts

## Error Handling

| Error | Behavior |
|-------|----------|
| MT5 not installed | ImportError at initialization |
| Terminal not running | RuntimeError after retry (3 attempts, 2s backoff) |
| Symbol unavailable | Logs warning, continues to next symbol |
| No data for range | Logs warning, continues to next pair/timeframe |
| All symbols fail | RuntimeError("No data collected") |
| Any exception | Terminal always shut down (finally block) |

**Partial failure handling**: If 3 out of 15 pairs/timeframes fail, the other 12 are still returned.

## Rate Limiting

- **Politeness delay**: 0.5 seconds between consecutive fetches
- Prevents overloading MT5 terminal
- Ensures stable data retrieval

## Health Check

```bash
python -m scripts.collect_mt5_data --health-check
```

Verifies:
- ✅ MT5 terminal is installed
- ✅ Terminal is running
- ✅ Broker account is logged in
- ✅ Python can connect to terminal

## Logging

**Console**: INFO level
**File**: `logs/collectors/mt5_collector.log` (DEBUG level)

Enable verbose mode:
```bash
python -m scripts.collect_mt5_data --verbose
```

## Example Workflow

```bash
# 1. Health check
python -m scripts.collect_mt5_data --health-check

# 2. Collect default data (Bronze + Silver)
python -m scripts.collect_mt5_data --preprocess

# 3. Verify output
ls data/raw/mt5/             # Bronze CSV files
ls data/processed/ohlcv/     # Silver Parquet files
```

## Troubleshooting

### "MT5 initialization failed"

**Solutions**:
- Ensure MT5 terminal is **running**
- Verify broker account is **logged in**
- Restart MT5 terminal
- Run Python as same user that installed MT5

### "No data returned for SYMBOL"

**Solutions**:
- Check symbol name: use `EURUSD` not `EUR/USD`
- Ensure symbol visible in Market Watch with live prices
- Check broker historical data availability
- Try smaller date range

### "Symbol not available in MT5"

**Solutions**:
- Market Watch > right-click > Symbols
- Search for symbol and enable it
- Wait for prices to update
- Some brokers have limited symbol lists

### Permission Errors

**Solutions**:
- Avoid running MT5 as admin if Python runs as normal user
- Ensure both MT5 and Python have same privilege level
- Close and reopen MT5

## Requirements

```
MetaTrader5>=5.0.0    # Windows only
pandas>=2.0
```

## API Reference

- **MT5 Python Documentation**: https://www.mql5.com/en/docs/python_metatrader5
- **MT5 Download**: https://www.metatrader5.com/en/download
- **copy_rates_range**: https://www.mql5.com/en/docs/python_metatrader5/mt5copyratesrange_py

## Testing

```bash
pytest tests/ingestion/test_mt5_collector.py -v
```

Tests are fully CI-safe—MetaTrader5 module is mocked (no MT5 installation required).

## Limitations

- **Windows only**: MT5 terminal not available on macOS/Linux
- **Terminal dependency**: MT5 must be running during collection
- **Broker-specific**: Historical data depth varies by broker
- **FX only**: Configured for FX pairs (stocks/commodities require symbol updates)
- **Real volume**: Always 0 for FX (tick volume used instead)
