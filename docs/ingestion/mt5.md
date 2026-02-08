# MT5 Data Collector

Historical OHLCV price data collection via the MetaTrader 5 Python API.

Implements the `BaseCollector` interface defined in [base_collector.py](../../data/ingestion/base_collector.py) and follows the §3.1 raw CSV contract.

## Prerequisites

- **Windows** (MT5 terminal is Windows-only)
- MT5 terminal installed and running
- A demo or live broker account logged in
- Python package: `pip install MetaTrader5`

## MT5 Terminal Setup

### 1. Install MetaTrader 5

Download from https://www.metatrader5.com and run the installer.

The **desktop terminal** is required (not the web version).

### 2. Create a Demo Account

1. Open MT5
2. File > Open an Account
3. Choose any demo broker
4. Select **Demo Account**
5. Complete registration and log in

Verify you can see: account number, balance, and live prices in Market Watch.

### 3. Enable Required Symbols

1. Open **Market Watch** (Ctrl+M)
2. Right-click > Symbols
3. Enable the required pairs:
   - EURUSD
   - GBPUSD
   - USDJPY
   - EURGBP
   - USDCHF

Ensure prices are updating (the bid/ask columns should show live values).

### 4. Verify Python Connectivity

```python
import MetaTrader5 as mt5

if not mt5.initialize():
    raise RuntimeError("MT5 initialization failed")

print("MT5 version:", mt5.version())
info = mt5.account_info()
print(f"Account: {info.login}, Balance: {info.balance}")
mt5.shutdown()
```

Expected: no errors, MT5 version and account info printed.

## Usage

### Basic Collection (Default: 3 Years, All Pairs)

```python
from data.ingestion.mt5_collector import MT5Collector

collector = MT5Collector()
results = collector.collect()

# Export to CSV (§3.1 naming: mt5_EURUSD_H1_YYYYMMDD.csv)
for name, df in results.items():
    collector.export_csv(df, name)
```

**Output location**: `datasets/prices/raw/mt5_EURUSD_H1_20260208.csv`, etc.

### Custom Date Range

```python
from datetime import datetime, timezone
from data.ingestion.mt5_collector import MT5Collector

collector = MT5Collector()
results = collector.collect(
    start_date=datetime(2022, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
)
```

### Custom Pairs and Timeframes

```python
from data.ingestion.mt5_collector import MT5Collector

collector = MT5Collector(
    pairs=["EURUSD", "GBPUSD"],
    timeframes=["D1"],
    years=5,
)
results = collector.collect()
```

### Health Check

```python
from data.ingestion.mt5_collector import MT5Collector

collector = MT5Collector()
if collector.health_check():
    results = collector.collect()
else:
    print("MT5 terminal is not accessible")
```

### CLI Usage

```bash
# Collect all defaults (5 pairs, 3 timeframes, 3 years)
python -m data.ingestion.mt5_collector

# Custom pairs and timeframes
python -m data.ingestion.mt5_collector \
    --pairs EURUSD,GBPUSD \
    --timeframes H1,D1

# Custom date range
python -m data.ingestion.mt5_collector \
    --start-date 2022-01-01 \
    --end-date 2025-01-01

# Override number of years (when not using --start-date)
python -m data.ingestion.mt5_collector --years 5

# Custom output directory
python -m data.ingestion.mt5_collector --output-dir /path/to/output

# Skip CSV export
python -m data.ingestion.mt5_collector --no-export
```

## Output Schema (Raw CSV — §3.1)

**File**: `datasets/prices/raw/mt5_{pair}_{timeframe}_{YYYYMMDD}.csv`

All MT5 fields are preserved (§3.1: "do not strip fields"):

| Column | Type | Description |
|--------|------|-------------|
| `time` | datetime (UTC) | Bar open time |
| `open` | float | Open price |
| `high` | float | High price |
| `low` | float | Low price |
| `close` | float | Close price |
| `tick_volume` | integer | Tick volume |
| `spread` | integer | Spread in points |
| `real_volume` | integer | Real volume (0 for FX) |
| `source` | string | Always `mt5` |

> These are raw outputs. The `data/preprocessing/` normaliser will transform them into the standardised §3.3 format with `timestamp_utc`, `pair`, and `timeframe` columns.

## Architecture

```
MT5Connector (infrastructure)         MT5Collector (business logic)
│                                     │
├── connect()                         ├── collect()          ← BaseCollector
├── shutdown()                        ├── health_check()     ← BaseCollector
├── fetch_ohlc(symbol, tf, start,     ├── export_csv()       ← BaseCollector
│              end)                   │
└── timeframes property               └── _fetch_and_normalise()
                                          ├── calls connector.fetch_ohlc()
                                          ├── adds source column
                                          ├── lowercases column names
                                          └── converts time to datetime
```

**Why two classes?**

- `MT5Connector` is pure infrastructure: connection lifecycle, raw MT5 API calls. It knows nothing about §3.1 or CSV export.
- `MT5Collector` is business logic: pair/timeframe iteration, §3.1 compliance, BaseCollector interface. It delegates all MT5 interaction to the connector.

This separation means:
- Connector can be tested with direct MT5 mocks
- Collector can be tested by mocking the connector
- If MT5 API changes, only the connector needs updating

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `pairs` | EURUSD, GBPUSD, USDJPY, EURGBP, USDCHF | FX pairs to collect |
| `timeframes` | H1, H4, D1 | Bar timeframes |
| `years` | 3 | Years of history (when start_date not specified) |
| `output_dir` | `datasets/prices/raw/` | CSV export directory |
| `REQUEST_DELAY` | 0.5s | Delay between consecutive fetches |

### Supported Timeframes

| Code | MT5 Constant | Description |
|------|--------------|-------------|
| `H1` | `TIMEFRAME_H1` | 1 hour bars |
| `H4` | `TIMEFRAME_H4` | 4 hour bars |
| `D1` | `TIMEFRAME_D1` | Daily bars |

## Error Handling

| Error | Behaviour |
|-------|-----------|
| **MT5 not installed** | `ImportError` at constructor time |
| **Terminal not running** | `RuntimeError` after retry attempts (default: 3 retries, 2s backoff) |
| **Symbol unavailable** | `RuntimeError`; `collect()` logs and continues to next symbol |
| **No data for range** | `RuntimeError`; `collect()` logs and continues |
| **All symbols fail** | `RuntimeError("No data collected from MT5")` |
| **Any exception during collect** | Connector is always shut down (finally block) |

Partial failure is handled gracefully: if 3 out of 15 symbol/timeframe combinations fail, the other 12 are still returned.

## Troubleshooting

### `MT5 initialization failed`

- Ensure MT5 terminal is installed (not just the web version)
- Ensure MT5 is running and logged into a broker account
- Restart MT5 and retry
- Run Python as the same OS user that installed MT5

### `No data returned for SYMBOL`

- Check symbol name: use `EURUSD`, not `EUR/USD`
- Ensure the symbol is visible in Market Watch with live prices
- Check broker data availability (some brokers limit history)
- Try a smaller date range

### `Symbol not available in MT5`

- Open Market Watch (Ctrl+M) > right-click > Symbols
- Search for the symbol and enable it
- Wait for prices to start updating

### Permission errors

- Close MT5 and reopen
- Avoid running MT5 as admin if Python is running as a normal user (or vice versa)

## Requirements

```
MetaTrader5    # Windows-only, pip install MetaTrader5
pandas>=2.0
```

## API Reference

- **MT5 Python docs**: https://www.mql5.com/en/docs/python_metatrader5
- **MT5 download**: https://www.metatrader5.com/en/download
- **copy_rates_range**: https://www.mql5.com/en/docs/python_metatrader5/mt5copyratesrange_py

## Testing

Tests are fully CI-safe — the MetaTrader5 module is mocked at the `sys.modules` level so no MT5 installation is required to run them:

```bash
python -m pytest tests/test_mt5_collector.py -v
```
