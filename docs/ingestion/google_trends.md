# Google Trends Collector (pytrends) — Bronze Attention Signals

This document describes the Google Trends data collector added to FX-AlphaLab to retrieve Interest Over Time signals using the `pytrends` library (unofficial Google Trends API).

It also documents:
- how the collector was executed to generate historical Bronze-layer CSV backfills, and
- the packaging/import changes needed to support running collectors in **separate virtual environments** (e.g., `.venv-trends`) without breaking the main environment.

Reference: pytrends repository — https://github.com/GeneralMills/pytrends


## Why a separate environment was needed (dependency conflict)

The repo’s main development environment includes dependencies that conflict with the Google Trends dependency stack.

In practice, this showed up as a dependency-version conflict around `urllib3` when using a pytrends-only environment vs. selenium-based collectors.

To avoid touching the shared dev environment (and avoid changing/locking dependencies on dev), I created a dedicated virtual environment for Google Trends only:

- Local env name: `.venv-trends`
- Goal: isolate `pytrends` and its compatible dependency set without breaking the main project’s selenium-based collectors.

Important note:
Even with a separate venv, Python package imports inside `src/ingestion/__init__.py` and `src/ingestion/collectors/__init__.py` can trigger selenium-based collectors **at import-time**, which will fail in `.venv-trends` if selenium/undetected-chromedriver are not installed. This is a packaging/import behavior issue, not a venv issue.

---

## Packaging change: make ingestion imports safe across environments

### Problem
Running in `.venv-trends` initially failed even when the Google Trends collector itself did not require selenium, because importing the ingestion package triggered the collectors package, and `collectors/__init__.py` performed eager imports of selenium-based collectors (e.g. ECBScraperCollector), causing immediate `ModuleNotFoundError` for `selenium` / `undetected_chromedriver`.

### Solution
Two small changes were introduced to prevent optional collectors from being imported unless their dependencies exist:

#### 1) `src/ingestion/__init__.py` was made minimal
Instead of importing the entire `collectors` package, it imports `BaseCollector` directly from the module:

- Before (problematic): `from src.ingestion.collectors import BaseCollector`
- After (safe): `from src.ingestion.collectors.base_collector import BaseCollector`

This prevents `src.ingestion` from forcing evaluation of `src.ingestion.collectors.__init__` when not needed.

#### 2) `src/ingestion/collectors/__init__.py` uses optional imports
Collector re-exports are now “best effort”:
- `BaseCollector` is always imported.
- All other collectors are imported via a helper `_optional(...)` that safely skips collectors if:
  - their dependency is not installed (ImportError), or
  - the exported class name differs from expected (AttributeError).

This preserves the convenience API for the main environment (where all collectors are available), while allowing the Google Trends environment to import `src.ingestion` without triggering selenium-based imports.



## What was implemented

### 1) Collector: `GoogleTrendsCollector` (Bronze layer)
File: `src/ingestion/collectors/google_trends_collector.py`

Purpose:
- Collect Google Trends Interest Over Time time-series for multiple keywords.
- Queries ONE keyword at a time for stability (Google Trends data is normalized 0–100 per keyword/timeframe/geo).
- Normalizes output to a Bronze-friendly schema and exports as CSV.

Key behavior:
- Uses pytrends `TrendReq` to call:
  - `build_payload(kw_list=[keyword], timeframe=..., geo=..., gprop=...)`
  - `interest_over_time()`
- Converts results to a standardized table:

Output schema (Bronze):
- date (UTC timestamp)
- keyword (string)
- value (int)
- is_partial (bool)
- source = "google_trends"

Rate limiting:
- Adds polite delays between keyword calls:
  - REQUEST_DELAY = 10 seconds + small random jitter
- Handles rate-limit errors (429 / “too many requests”):
  - For the first attempt: sleeps 180 seconds and retries once for that keyword.

Output location (default):
- `data/raw/attention/google_trends/`

Export filename pattern:
- `google_trends_interest_over_time_{START}_{END}_{GEO}_{GPROP}_kw{N}_{HHMMSS}_{YYYYMMDD}.csv`

---

### 2) Collection script (CLI)
File: `scripts/collect_google_trends_data.py`

Purpose:
- CLI entrypoint to run Google Trends collection and export Bronze CSV.

CLI features:
- Date range selection:
  --start YYYY-MM-DD
  --end   YYYY-MM-DD
  If not provided, defaults to ~2 years back to today.
- Keyword override:
  --keywords "k1,k2,k3"
- Geography filter:
  --geo US / DE / GB / JP (empty = global)
- Google property filter:
  --gprop news / images / youtube / (empty = web)
- Health check mode:
  --health-check
- Verbose logs:
  --verbose / -v

---

### 3) Tests
File: `tests/.../test_google_trends_collector.py` (or equivalent)

What is covered:
- Timeframe conversion:
  - defaults to "today 5-y" when dates missing
  - converts explicit UTC dates to "YYYY-MM-DD YYYY-MM-DD"
- Collect returns normalized schema by mocking pytrends:
  - ensures resulting DataFrame contains required columns
  - ensures `source == "google_trends"`
- Export CSV works (writes file to output_dir)
- Collector respects passed keywords (build_payload called with correct kw_list)

---

## Environment setup (Google Trends only)

Because of the urllib3 / selenium conflict, Google Trends collection should be run in `.venv-trends`.

Suggested requirements file (Google Trends env):
- pytrends==4.9.2
- urllib3<2
- pandas
- requests

(If this file is stored under `.venv-trends/requirements-trends.txt`, install using the correct relative path.)

Install:
- `.\.venv-trends\Scripts\python.exe -m pip install -r .\.venv-trends\requirements-trends.txt`

Run:
- Use the venv Python directly:
  `.\.venv-trends\Scripts\python.exe -m scripts.collect_google_trends_data ...`

---

## Commands executed (historical backfill runs)

All runs used:
- start: 2021-01-01
- end:   2026-02-17
- exported Bronze CSVs to:
  `data/raw/attention/google_trends/`

## Historical Backfill Runs (Batches Executed)

All batches were executed using the isolated Google Trends environment:
  .\.venv-trends\Scripts\python.exe

Common parameters across runs:
- start: 2021-01-01
- end:   2026-02-17
- output dir: data/raw/attention/google_trends/
- output: one CSV per run (≈1345 rows for 5-keyword runs)
- note: occasional 429 rate-limits were handled via 180s backoff + retry in the collector

---

### Batch 1 — GLOBAL (web): US macro (web)
Keywords:
- inflation, CPI, Federal Reserve, FOMC, bond yields

Command:
.\.venv-trends\Scripts\python.exe -m scripts.collect_google_trends_data --start 2021-01-01 --end 2026-02-17 --keywords "inflation,CPI,Federal Reserve,FOMC,bond yields"

Output:
 ...GLOBAL_web_kw5_170656...csv

---

### Batch 2 — GLOBAL (web): ECB macro (web)
Keywords:
- European Central Bank, ECB, Christine Lagarde, eurozone inflation, PMI

Command:
.\.venv-trends\Scripts\python.exe -m scripts.collect_google_trends_data --start 2021-01-01 --end 2026-02-17 --keywords "European Central Bank,ECB,Christine Lagarde,eurozone inflation,PMI"

Output:
 ...GLOBAL_web_kw5_171134...csv

---

### Batch 3 — GLOBAL (web): FX / rates (web)
Keywords:
- EURUSD, dollar index, forex, rate hike, rate cut

Command:
.\.venv-trends\Scripts\python.exe -m scripts.collect_google_trends_data --start 2021-01-01 --end 2026-02-17 --keywords "EURUSD,dollar index,forex,rate hike,rate cut"

Output:
 ...GLOBAL_web_kw5_171527...csv

---

### Batch 4 — GLOBAL (web): Risk sentiment (web)
Keywords:
- recession, safe haven, market crash, volatility, VIX

Command:
.\.venv-trends\Scripts\python.exe -m scripts.collect_google_trends_data --start 2021-01-01 --end 2026-02-17 --keywords "recession,safe haven,market crash,volatility,VIX"

Output:
 ...GLOBAL_web_kw5_171302...csv

---

### Batch 5 — GLOBAL (news): US macro (news)
Keywords:
- inflation, CPI, Federal Reserve, FOMC, bond yields

Command:
.\.venv-trends\Scripts\python.exe -m scripts.collect_google_trends_data --start 2021-01-01 --end 2026-02-17 --gprop news --keywords "inflation,CPI,Federal Reserve,FOMC,bond yields"

Output:
 ...GLOBAL_news_kw5_171815...csv

---

## Geo Segmentation Runs

### Batch 6 — GB (web)
Keywords:
- GBPUSD, GBP exchange rate, pound dollar, Bank of England, BoE

Command:
.\.venv-trends\Scripts\python.exe -m scripts.collect_google_trends_data --start 2021-01-01 --end 2026-02-17 --geo GB --keywords "GBPUSD,GBP exchange rate,pound dollar,Bank of England,BoE"

Output:
...GB_web_kw5_174038...csv

---

### Batch 7 — JP (web)
Keywords:
- USDJPY, Japanese yen, yen intervention, Bank of Japan, BOJ

Command:
.\.venv-trends\Scripts\python.exe -m scripts.collect_google_trends_data --start 2021-01-01 --end 2026-02-17 --geo JP --keywords "USDJPY,Japanese yen,yen intervention,Bank of Japan,BOJ"

Output:
 ...JP_web_kw5_174509...csv

---

### Batch 8 — US (news)
Keywords:
- inflation, CPI, Federal Reserve, FOMC, bond yields

Command:
.\.venv-trends\Scripts\python.exe -m scripts.collect_google_trends_data --start 2021-01-01 --end 2026-02-17 --geo US --gprop news --keywords "inflation,CPI,Federal Reserve,FOMC,bond yields"

Output:
 ...US_news_kw5_174927...csv

---

### Batch 9 — DE (web)
Keywords:
- European Central Bank, ECB, Christine Lagarde, eurozone inflation, PMI

Command:
.\.venv-trends\Scripts\python.exe -m scripts.collect_google_trends_data --start 2021-01-01 --end 2026-02-17 --geo DE --keywords "European Central Bank,ECB,Christine Lagarde,eurozone inflation,PMI"

Output:
 ...DE_web_kw5_175400...csv

---

## Extra Run (Keyword gap fill)

### Batch 10 — GLOBAL (web): extra keywords
Keywords:
- DXY, euro dollar

Command:
.\.venv-trends\Scripts\python.exe -m scripts.collect_google_trends_data --start 2021-01-01 --end 2026-02-17 --keywords "DXY,euro dollar"

Output:
(exported to data/raw/attention/google_trends/ with a GLOBAL_web filename)



Observed behavior:
- On first keyword for several runs, Google returned 429 rate limits.
- Collector handled it by sleeping 180s and retrying once, then continuing normally.
- Each run exported ~1345 records (5 keywords across the same timeframe).

---

## Output files

CSV outputs are stored under:
- `data/raw/attention/google_trends/`

Each output contains normalized rows:
- one row per (date, keyword)
- plus metadata:
  - is_partial
  - source="google_trends"

---

## Notes / limitations

1) Google Trends normalization:
   Values are 0–100 scaled within the query window and are not absolute volumes.
   Comparisons across keywords require care (and ideally consistent querying strategy).

2) Rate limiting:
   Backfills across many years are more likely to hit 429.
   Current strategy:
   - delay between keywords
   - one retry with 180s backoff on 429
   Further improvements could include:
   - proxy support
   - exponential backoff
   - caching / split timeframe windows

3) Import-time dependency issue:
   If the ingestion packages import selenium-based collectors in `__init__.py`,
   then running GoogleTrendsCollector in `.venv-trends` may fail unless those imports are made lazy/optional.
   The venv isolation prevents dependency conflicts, but does not stop Python package `__init__` files from importing optional modules.

---

## How to validate quickly

Health check:
- `.\.venv-trends\Scripts\python.exe -m scripts.collect_google_trends_data --health-check`

Run tests:
- `pytest -k google_trends -v`

Verify output:
- open the latest CSV in:
  `data/raw/attention/google_trends/`
  and confirm columns:
  date, keyword, value, is_partial, source

---
