# Google Trends Sentiment Node Pipeline — Retail Attention to Regime Signal

---

## Visualization View (Production)

### Outside the Node

**Google Trends API**
Weekly Relative Search Volume (RSV) series for 34 FX-relevant keywords across 4 thematic groups, fetched via pytrends. Each series is normalized 0–100 within its batch window — the 100 anchor is the highest-traffic week for those keywords in the requested period.

**Bronze Storage**
Eight CSV files, one per pytrends batch. Each file contains the complete series from the collection start date to the collection end date. Files are overwritten completely on each production refresh — they are not date-partitioned and not immutable.

**Google Trends Preprocessor**
Merges all 8 Bronze CSVs on `date`. Renames each keyword column to `{theme}__{snake_case(keyword)}`. Casts all keyword columns to `float64`. Writes a single flat Parquet file.

**Silver Storage**
Single Parquet file at `data/processed/sentiment/source=google_trends/google_trends_weekly.parquet`. 262 rows × 35 columns. This is the canonical data source for the node.

---

### Inside the Node

**Theme Group Indexing**
For each of the 4 thematic groups, computes the row-wise mean of all Silver columns sharing that group's prefix. Produces 4 weekly attention indices: `macro_attention_index`, `central_bank_attention_index`, `risk_sentiment_attention_index`, `fx_pairs_attention_index`.

**Rolling Z-Score**
Normalizes each index using a 30-week rolling window (minimum 10 periods). Rolling std of exactly zero is masked to NaN before division — no inf values are produced. The warmup period (weeks 1–9) yields NaN z-scores.

**Weekly → Daily Forward-Fill**
Reindexes the weekly signal to a full daily calendar. Each calendar day receives the value of the most recent preceding Sunday. Days before the first Silver week are NaN. Days after the last Silver week carry the last available values forward.

**Signal Output**
One row per calendar day across the requested date range:
- **`macro_attention_zscore`** — how elevated retail macro search attention is relative to the trailing 30-week baseline. Positive spike signals heightened uncertainty regime.
- **`central_bank_attention_zscore`** — elevation in central bank search attention relative to baseline.
- **`risk_sentiment_attention_zscore`** — elevation in risk/volatility keyword attention.
- **`fx_pairs_attention_zscore`** — diagnostic only; FX ticker symbols have near-zero average RSV.

---

### Downstream

**SentimentAgent**
Receives the Google Trends output as an auxiliary attention context input — not as a directional vote. Elevated `macro_attention_zscore` signals a heightened uncertainty regime, which dampens confidence in directional signals from StockTwits and Reddit nodes.

---

### Flow

```
Google Trends API (pytrends)
               ↓
GoogleTrendsCollector → Bronze CSVs  (data/raw/google_trends/)
               ↓
GoogleTrendsPreprocessor → Silver Parquet
          (data/processed/sentiment/source=google_trends/)
               ↓
       [ GOOGLE TRENDS SIGNAL NODE ]
       Theme Group Indexing
  (macro · central_bank · risk_sentiment · fx_pairs)
               ↓
       Rolling Z-Score
       (30-week window)
               ↓
    Weekly → Daily Forward-Fill
               ↓
  GoogleTrendsSignalNode output
  macro_attention_zscore | central_bank_attention_zscore
  risk_sentiment_attention_zscore | fx_pairs_attention_zscore
               ↓
         SentimentAgent
    (attention/regime context input)
```

---

## Overview

The Google Trends pipeline transforms weekly retail search attention — collected from the Google Trends API across 34 FX-relevant keywords — into four daily regime signals. These signals do not predict FX direction. Benchmark analysis over 2021–2025 (262 weeks, EURUSD/GBPUSD/USDJPY) shows that no structured model beats the zero-return baseline for signed weekly return prediction, and direction balanced accuracy is approximately 0.50 across all pairs and feature sets. The same analysis shows consistent predictive value for next-week absolute return when using macro indicators attention (absret_fwd_1w lasso rmse=0.005069 vs zero-return baseline 0.007828). Google Trends is used exclusively as a volatility and attention context modulator.

---

## Stage 1 — Data Collection (Bronze Layer)

**Source:** Google Trends API via pytrends library

**What happens:** The collector fetches weekly RSV data for 34 keywords organized into 4 thematic groups split across 8 pytrends batches (≤5 keywords per batch, pytrends limit). Each batch is fetched as a complete series from `start_date` to `end_date`. A sleep of 2–5 seconds (random) is applied between requests to avoid rate limiting, with exponential backoff retry on failure. Each batch file is written only if it does not already exist (`force=False`). `force=True` re-fetches all 8 batches and overwrites all files.

**Output:** CSV files at `data/raw/google_trends/`
- Naming: `trends_{theme}_{batch_index}.csv`
- 8 files total — one per pytrends batch

**Bronze record schema (per file):**

| Field | Type | Description |
|---|---|---|
| `date` | str | Sunday week-start date, YYYY-MM-DD |
| keyword columns | int | RSV values 0–100, up to 5 columns per file |

**Keyword taxonomy — 34 keywords across 4 themes:**

| Theme | Batch 0 | Batch 1 |
|---|---|---|
| `fx_pairs` | EURUSD, GBPUSD, USDJPY, euro dollar, pound dollar | Japanese yen, GBP exchange rate, USDCHF, Swiss franc |
| `central_banks` | ECB, European Central Bank, Christine Lagarde, Federal Reserve, FOMC | Bank of England, BoE, Bank of Japan, BOJ |
| `macro_indicators` | CPI, inflation, eurozone inflation, PMI, bond yields | rate hike, rate cut, recession, UK inflation |
| `risk_sentiment` | VIX, volatility, safe haven, market crash, DXY | dollar index, forex |

**Default collection start: 2021-01-01.** The 2022 inflation and rate hike cycle is the highest-traffic macro event in this period, anchoring the RSV 100 scale at a genuine historical extreme. Changing the start date changes this anchor and makes z-scores incomparable to historical analysis. Production refresh re-fetches the full range from this start date to today.

**Coverage:** 2021-01-01 → present (refreshed weekly)

**Important:** Bronze files are not date-partitioned and are not immutable. Each production refresh overwrites all 8 files with a fresh complete series. There is no incremental append — the pytrends normalization window makes partial updates invalid.

---

## Stage 2 — Preprocessing (Silver Layer)

**What happens:** The preprocessor globs all `trends_*.csv` files in the Bronze directory. For each file, the theme name is extracted from the filename using the pattern `trends_({theme})_\d+\.csv`. Each non-date column is renamed to `{theme}__{snake_case(keyword)}` — lowercased, spaces and non-alphanumeric characters replaced with underscores. All 8 frames are outer-merged on `date` (preserving the union of all available weeks), sorted by date ascending, and deduplicated. All keyword columns are cast to `float64`. The merged result is written as a single Parquet file.

**Output:** `data/processed/sentiment/source=google_trends/google_trends_weekly.parquet`

No Hive partitioning — 262 weekly rows does not warrant it.

**Silver schema:**

| Column | Dtype | Description |
|---|---|---|
| `date` | datetime64[ns] | Sunday week-start date, timezone-naive |
| `central_banks__boe` | float64 | RSV 0–100 |
| `central_banks__bank_of_england` | float64 | |
| ... (9 central_banks columns) | float64 | |
| `fx_pairs__eurusd` | float64 | |
| ... (9 fx_pairs columns) | float64 | |
| `macro_indicators__cpi` | float64 | |
| ... (9 macro_indicators columns) | float64 | |
| `risk_sentiment__dxy` | float64 | |
| ... (7 risk_sentiment columns) | float64 | |

35 columns total (1 date + 34 keywords). Keyword columns sorted alphabetically after date.

The Silver layer is the canonical input for the node. Bronze CSVs are never read again after preprocessing.

---

## Stage 3 — Signal Computation

**What happens:** The node reads the full Silver Parquet in a single call — 262 rows regardless of the requested date range (negligible cost). For each thematic group, it computes the row-wise mean of all columns sharing that group's prefix, producing 4 weekly attention indices. A 30-week rolling z-score is applied to each index. The weekly series is then forward-filled to a full daily calendar: each calendar day gets the value of the most recent preceding Sunday. The output is filtered to the requested `[start_date, end_date]`.

**Theme group indexing:**
- `macro_attention_index` — mean of all `macro_indicators__*` columns (9 keywords)
- `central_bank_attention_index` — mean of all `central_banks__*` columns (9 keywords)
- `risk_sentiment_attention_index` — mean of all `risk_sentiment__*` columns (7 keywords)
- `fx_pairs_attention_index` — mean of all `fx_pairs__*` columns (9 keywords)

Row-wise mean skips NaN by default — near-zero keywords (e.g. `eurozone_inflation`) are included and do not corrupt the index.

**Rolling normalization:**

| Parameter | Value |
|---|---|
| Rolling window | 30 weeks |
| Minimum periods | 10 weeks |
| Zero std handling | rolling_std == 0 → NaN |
| Forward-fill | ffill() only — no backfill, no interpolation |

Days before the first Silver week produce NaN. Days after the last Silver week carry the last available values forward.

---

## Signal Output

One row per calendar day. Passed to SentimentAgent as a DataFrame.

| Column | Dtype | Description |
|---|---|---|
| `date` | datetime64[ns] | Calendar day |
| `macro_attention_index` | float64 | Raw weekly mean of macro_indicators RSV, forward-filled |
| `central_bank_attention_index` | float64 | Raw weekly mean of central_banks RSV, forward-filled |
| `risk_sentiment_attention_index` | float64 | Raw weekly mean of risk_sentiment RSV, forward-filled |
| `fx_pairs_attention_index` | float64 | Raw weekly mean of fx_pairs RSV, forward-filled |
| `macro_attention_zscore` | float64 | 30w rolling z-score of macro index. Positive = elevated attention regime |
| `central_bank_attention_zscore` | float64 | 30w rolling z-score of central bank attention |
| `risk_sentiment_attention_zscore` | float64 | 30w rolling z-score of risk/volatility attention |
| `fx_pairs_attention_zscore` | float64 | 30w rolling z-score of FX pair attention — diagnostic only |

**Signal semantics — empirically validated (2021–2025, 262 weeks):**
- `macro_attention_zscore` is the primary fusion signal: elevated macro search attention predicts higher next-week absolute FX return (lasso absret_fwd_1w rmse=0.005069 vs baseline 0.007828).
- `central_bank_attention_zscore` ranks #1 in subset ablation by mean rank — strong secondary signal.
- Neither signal has meaningful directional IC — balanced accuracy ≈ 0.50 for all pairs.
- `fx_pairs_attention_zscore` is diagnostic only: FX ticker symbols (EURUSD, GBPUSD) average RSV of 0.8–1.2 vs natural-language equivalents (euro dollar: 31.7, Japanese yen: 22.8).

---

## Known Validation Debt

Walk-forward validation (train < 2024-01-01, test ≥ 2024-01-01) has not been run for `macro_attention_zscore` or `central_bank_attention_zscore`. Current performance figures (lasso RMSE, feature ranks) are full-sample (in-sample only). These signals ship as Tier B (regime overlay) pending out-of-sample confirmation.

---

## Pipeline Summary

```
Google Trends API (pytrends)
        ↓
GoogleTrendsCollector → Bronze CSVs  (data/raw/google_trends/trends_{theme}_{batch}.csv)
        ↓
GoogleTrendsPreprocessor → Silver Parquet
        (data/processed/sentiment/source=google_trends/google_trends_weekly.parquet)
        ↓
GoogleTrendsSignalNode.compute(start_date, end_date)
  └── theme group mean indexing (4 attention indices)
  └── 30w rolling z-score (min_periods=10)
  └── weekly → daily forward-fill
        ↓
DataFrame [date · 4 attention indices · macro_attention_zscore · 3 secondary z-scores]
        ↓
SentimentAgent — attention/regime context input
```
