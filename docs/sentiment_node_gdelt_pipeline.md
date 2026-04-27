# GDELT Sentiment Node Pipeline — BigQuery to Volatility Signal

---

## Visualization View (Production)

### Outside the Node

**BigQuery — GDELT GKG**
Public dataset `gdelt-bq.gdeltv2.gkg_partitioned`. Each row is one article processed by the GDELT Global Knowledge Graph: media source, publication timestamp, V2Tone score (7 numeric fields), and GDELT taxonomy tags for themes, locations, and organizations.

**Bronze Storage**
One JSONL file per calendar month. Raw strings exactly as delivered from BigQuery — V2Tone is stored as the original comma-separated string, themes/locations/organizations as semicolon-separated strings. Immutable.

**GDELT Preprocessor**
Parses the raw strings into typed columns: V2Tone split into 7 named float/int fields, semicolon strings split into lists, `timestamp_published` normalized to UTC datetime. Outputs clean Parquet partitioned by year and month.

**Silver Storage**
Hive-partitioned Parquet under `data/processed/sentiment/source=gdelt/`. This is the canonical data source for the node — the only thing it reads.

---

### Inside the Node

**Daily Aggregation**
Groups all articles by calendar date. Computes two metrics per day: `tone_mean` (mean V2Tone across articles with non-zero tone) and `article_count` (total articles published that day). Missing calendar dates are filled in with `NaN` tone and zero article count.

**Rolling Z-Score**
Normalizes both metrics using a 30-day rolling window (minimum 10 periods). Days with fewer than 3 articles have `tone_zscore` masked to `NaN` — not enough coverage to trust the tone estimate.

**Signal Output**
One row per calendar day across the requested date range:
- **`tone_zscore`** — how negative today's macro news sentiment is relative to the past 30 days
- **`attention_zscore`** — how much news volume today is relative to the past 30 days

---

### Downstream

**SentimentAgent**
Receives the GDELT output as a regime/modulator input — not as a directional vote. Negative `tone_zscore` signals elevated macro uncertainty, which dampens confidence in directional signals from StockTwits and Reddit nodes.

---

### Flow

```
BigQuery (gdelt-bq.gdeltv2.gkg_partitioned)
             ↓
    GDELTCollector → Bronze JSONL  (data/raw/news/gdelt/)
             ↓
  GDELTPreprocessor → Silver Parquet  (data/processed/sentiment/source=gdelt/)
             ↓
          [ GDELT SIGNAL NODE ]
          Daily Aggregation
          (tone_mean · article_count)
             ↓
        Rolling Z-Score
        (30-day window)
             ↓
       GDELTSignalNode output
     tone_zscore | attention_zscore
             ↓
        SentimentAgent
     (regime modulation input)
```

---

## Overview

The GDELT pipeline transforms a global news media stream — sourced from the GDELT Project via BigQuery — into two daily regime signals: macro tone z-score and news attention z-score. These signals do not predict FX direction. Empirical IC analysis over 2021–2025 (683k articles, 1540 trading days) shows mean directional IC of 0.026 — below the 0.03 usability threshold. The same analysis shows consistent volatility IC of 0.051–0.073 (p<0.05) across all four FX pairs. GDELT is used exclusively as a volatility and regime modulator.

---

## Stage 1 — Data Collection (Bronze Layer)

**Source:** BigQuery public dataset `gdelt-bq.gdeltv2.gkg_partitioned`

**What happens:** The collector runs day-by-day queries over a requested date range. Each query targets articles from the GDELT Global Knowledge Graph that contain FX-relevant themes. A dry-run cost check is performed before each real query — queries estimated at more than 5 GB are aborted. Results are deduplicated in memory by URL before writing. Records are sorted by source credibility tier (Reuters/Bloomberg/FT first, then WSJ/CNBC, then all others), then by publication timestamp, then by URL hash. The sorted JSONL is written as a single monthly file.

**Output:** JSONL files at `data/raw/news/gdelt/`
- Naming: `gdelt_YYYYMM_raw.jsonl`
- One file per calendar month

**Bronze record schema:**

| Field | Type | Description |
|---|---|---|
| `source` | str | "gdelt" |
| `timestamp_collected` | str | UTC ISO string, naive (no tz suffix) |
| `timestamp_published` | str | UTC ISO string with Z suffix |
| `url` | str | Article URL |
| `source_domain` | str | e.g. "reuters.com" |
| `v2tone` | str | Raw comma-separated 7-field GDELT V2Tone string |
| `themes` | str | Raw semicolon-separated GDELT theme tokens with character offsets |
| `locations` | str | Raw semicolon-separated GDELT location strings |
| `organizations` | str | Raw semicolon-separated GDELT organization strings |

**BigQuery filter applied at collection time:**
- Theme contains `ECON_CURRENCY` or `ECON_CENTRAL_BANK`
- AND theme contains `EUR`, `USD`, `GBP`, or `JPY`

**Coverage:** 2021-01-01 → 2025-12-31

---

## Stage 2 — Preprocessing (Silver Layer)

**What happens:** The preprocessor reads each Bronze JSONL file and parses the raw strings into typed columns. `timestamp_published` is normalized to a UTC-aware datetime. `v2tone` is split on commas into 7 named fields — the first field is the overall tone score, the remaining six are positive score, negative score, polarity, activity reference density, self/group reference density, and word count. `themes`, `locations`, and `organizations` are split on semicolons into Python lists. Records with a missing URL or unparseable timestamp are dropped. Records with an empty or malformed `v2tone` are kept with `tone=NaN` — they still contribute a valid timestamp for counting purposes.

**Output:** Parquet files at `data/processed/sentiment/source=gdelt/year={YYYY}/month={MM}/sentiment_cleaned.parquet`

**Silver schema:**

| Column | Dtype | Description |
|---|---|---|
| `timestamp_utc` | datetime64[ns, UTC] | Parsed from `timestamp_published` |
| `url` | str | |
| `source_domain` | str | |
| `source` | str | "gdelt" |
| `tone` | float64 | V2Tone field 0 — overall document tone |
| `positive_score` | float64 | V2Tone field 1 |
| `negative_score` | float64 | V2Tone field 2 |
| `polarity` | float64 | V2Tone field 3 |
| `activity_ref_density` | float64 | V2Tone field 4 |
| `self_group_ref_density` | float64 | V2Tone field 5 |
| `word_count` | Int64 | V2Tone field 6, nullable integer |
| `themes` | list[str] | Semicolon-split GDELT theme tokens |
| `locations` | list[str] | Semicolon-split GDELT location strings |
| `organizations` | list[str] | Semicolon-split GDELT organization strings |

The Silver layer is the canonical input for the node. Bronze files are never read again after preprocessing.

---

## Stage 3 — Signal Computation

**What happens:** The node discovers all monthly Silver Parquet files that overlap the requested date range by scanning the Hive directory structure (`year=*/month=*/sentiment_cleaned.parquet`). It reads only the `timestamp_utc` and `tone` columns. Records with `tone=NaN` are dropped before aggregation — they do not count toward `article_count`.

**Daily aggregation:**
- `article_count` — number of articles published on that calendar date (with valid tone)
- `tone_mean` — mean V2Tone across all articles on that date, excluding exact zeros

**Rolling normalization:**

| Parameter | Value |
|---|---|
| Rolling window | 30 days |
| Minimum periods | 10 days |
| Low coverage mask | days with `article_count < 3` → `tone_zscore = NaN` |

The rolling window uses all data loaded for the date range. When called over a multi-year span the warmup period is only the first 10 days of the dataset. Missing calendar dates (weekends, data gaps) are filled: `tone_mean = NaN`, `article_count = 0`.

---

## Signal Output

One row per calendar day. Passed to SentimentAgent as a DataFrame.

| Column | Dtype | Description |
|---|---|---|
| `date` | datetime64[ns] | Calendar date |
| `tone_mean` | float64 | Mean daily V2Tone, zero-excluded |
| `article_count` | int64 | Daily article count |
| `tone_zscore` | float64 | 30d rolling z-score of tone_mean. Negative = elevated macro negativity |
| `attention_zscore` | float64 | 30d rolling z-score of article_count. Positive spike = news flow surge |

**Signal semantics — empirically validated:**
- `tone_zscore` is negatively correlated with next-day |return| across all four pairs (IC: -0.051 to -0.073, p<0.05). More negative tone → higher expected volatility.
- `attention_zscore` is positively correlated with USDJPY next-day |return| (IC=+0.055, p=0.033).
- Neither signal has meaningful directional IC (mean |IC|=0.026 across all pairs and filters).

---

## Pipeline Summary

```
BigQuery (gdelt-bq.gdeltv2.gkg_partitioned)
        ↓
GDELTCollector → Bronze JSONL  (data/raw/news/gdelt/gdelt_YYYYMM_raw.jsonl)
        ↓
GDELTPreprocessor → Silver Parquet
        (data/processed/sentiment/source=gdelt/year=YYYY/month=MM/sentiment_cleaned.parquet)
        ↓
GDELTSignalNode.compute(start_date, end_date)
  └── daily aggregation (tone_mean · article_count)
  └── 30d rolling z-score (min_periods=10)
  └── low-coverage mask (article_count < 3 → tone_zscore = NaN)
        ↓
DataFrame [date · tone_mean · article_count · tone_zscore · attention_zscore]
        ↓
SentimentAgent — regime modulation input
```
