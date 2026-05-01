# Technical Agent Pipeline — MT5 to Signal

---

## Visualization View (Production)

### Outside the Agent

**MetaTrader5 Terminal**
Live FX broker connection. Streams OHLCV price bars for 4 pairs across 3 timeframes (H1, H4, D1).

**Raw Storage**
Bars are written to disk exactly as received. No transformation. Immutable.

**Price Normalizer**
Validates and standardizes the raw bars: UTC timestamps, OHLC consistency checks, deduplication. Outputs clean structured records ready for any downstream consumer.

**Processed Storage**
Standardized OHLCV parquet files. This is the canonical data source for the agent — the only thing it reads.

---

### Inside the Technical Agent

**Feature Engineering**
Computes 15 technical indicators from the OHLCV bars: momentum (RSI, MACD, short/long momentum), volatility (ATR, Bollinger Bands), and trend (3 exponential moving averages), plus log returns. Also computes a 252-day ATR percentile rank used exclusively for the volatility regime label — this is not fed into the model.

**MinMax Scaler**
Normalizes all 15 features to [0, 1] using statistics learned during training. Applied identically at inference — no refitting.

**LSTM Model (× 3)**
One trained model per timeframe (H1, H4, D1). Each receives a sliding window of recent bars and outputs a single probability: how likely is the next bar to close higher? Architecture: 2 stacked LSTM layers → dropout → single sigmoid output.

**Multi-Timeframe Fusion**
Combines the three probability outputs into one directional score using equal weights (empirically validated — no timeframe predicts daily direction better than the others). The D1 model anchors the output: it provides the timestamp, the volatility regime label, and the indicator snapshot. Per-timeframe directions are preserved as `timeframe_votes`.

**Explainability**
Two layers computed at inference with no additional cost:
- **Indicator Snapshot** — the 5 most interpretable indicator values from the D1 close (RSI, MACD histogram sign, Bollinger Band % position, price above/below EMA200, ATR percentile rank). Describes the market state that accompanied the model's call — not gradient-based attribution.
- **Timeframe Votes** — the individual directional vote (0/1) from each of D1, H4, H1. A unanimous 3/3 vote is more reliable than a 2/1 split, especially given the model's near-random AUC.

**Signal Output**
One `TechnicalSignal` per pair per day:
- **Direction** — bullish or bearish
- **Confidence** — how far the fused probability is from the decision boundary (typically low, ~0.02–0.04)
- **Volatility Regime** — high or low, based on where today's ATR sits within the past year
- **Indicator Snapshot** — key market state indicators at inference time
- **Timeframe Votes** — per-timeframe directional votes

---

### Downstream

**Coordinator**
Receives `TechnicalSignal` alongside signals from the Macro Agent and Sentiment Agent. Uses direction + confidence for stacking. Uses volatility regime for position sizing.

---

### Flow

```
MT5 Terminal → Raw Storage → Price Normalizer → Processed Storage
                                                        ↓
                                              [ TECHNICAL AGENT ]
                                              Feature Engineering
                                                        ↓
                                  Indicator Snapshot ←──┤ (unscaled last bar)
                                                        ↓
                                               MinMax Scaling
                                                        ↓
                                         LSTM D1 | LSTM H4 | LSTM H1
                                                        ↓
                                         Multi-Timeframe Fusion (equal weights)
                                         Timeframe Votes preserved
                                                        ↓
                                              TechnicalSignal
                              direction | confidence | volatility_regime
                              indicator_snapshot | timeframe_votes
                                                        ↓
                                                  Coordinator
```

---

## Overview

The technical pipeline transforms raw FX price data from MetaTrader5 into a daily directional signal per currency pair. It runs through four stages: collection, preprocessing, feature engineering, and LSTM inference with multi-timeframe fusion.

---

## Stage 1 — Data Collection (Bronze Layer)

**Source:** MetaTrader5 terminal (Windows-only, live connection)

**What happens:** The MT5 collector connects to the terminal and pulls historical OHLCV bars for each configured pair and timeframe. Raw data is written to disk exactly as received — no transformation, no cleaning.

**Output:** CSV files at `data/raw/mt5/`
- Naming: `mt5_{pair}_{timeframe}_{YYYYMMDD}.csv`
- Fields: timestamp (Unix epoch), open, high, low, close, volume, source="mt5"

**Pairs covered:** EURUSDm, GBPUSDm, USDJPYm, USDCHFm
**Timeframes:** H1 (1-hour), H4 (4-hour), D1 (daily)

---

## Stage 2 — Preprocessing (Silver Layer)

**What happens:** The PriceNormalizer reads the raw CSV files and applies the Silver schema contract. This includes: converting Unix timestamps to UTC ISO 8601, validating OHLC consistency (high ≥ open/close/low), deduplicating timestamps, and standardizing column names.

**Output:** Parquet files at `data/processed/ohlcv/`
- Naming: `ohlcv_{pair}_{timeframe}_{start}_{end}.parquet`
- Schema: `timestamp_utc, pair, timeframe, open, high, low, close, volume, source`

The Silver layer is the canonical input for all downstream agents. Raw Bronze files are never touched again.

---

## Stage 3 — Feature Engineering

**What happens:** Each Silver parquet is loaded and passed through `add_features()`, which computes 15 technical indicators from the OHLCV columns. A 16th column (`atr_pct_rank`) is also computed but used only for volatility labeling — it is not a model input.

**15 model input features:**

| Category | Features |
|---|---|
| Returns | log return, squared log return |
| Momentum | RSI(14), MACD(12/26), MACD signal, MACD histogram, momentum(5), momentum(20) |
| Volatility | ATR(14), Bollinger Band upper/lower/% position |
| Trend | EMA(20), EMA(50), EMA(200) |

**Volatility regime label** (separate from model input):
- `atr_pct_rank` = rolling 252-day ATR percentile rank
- Threshold: > 0.6 → `"high"`, ≤ 0.6 → `"low"`

**Target (training only):** binary — did the next bar close higher? (1/0)

---

## Stage 4 — LSTM Inference

**Architecture:** 2-layer LSTM, 128 hidden units, 30% dropout, single sigmoid output

**Input:** sliding window of 30 or 60 consecutive bars (sequence length varies by timeframe), MinMax-scaled to [0,1] using a scaler fit on training data only

**Training split (chronological, no leakage):**
- Train: up to 2023-12-31
- Validation: 2024-01-01 → 2024-06-30
- Test: 2024-07-01 onwards

**Output per timeframe model:** `prob_up` — probability that the next bar closes higher

**Signal quality (empirically measured on test set):**
- ROC-AUC: 0.50–0.54 across all pairs and timeframes
- FX next-bar direction is close to a random walk — this is expected, not a model deficiency

**Production artifacts:** `models/production/{pair}_{tf}.pkl` — 12 models total (4 pairs × 3 timeframes)

---

## Stage 5 — Multi-Timeframe Fusion

**What happens:** For each pair, the three timeframe models (D1, H4, H1) each produce a `prob_up`. These are fused into one signal using equal weights (1/3 each), empirically validated in `notebooks/technical_agent_03_mtf_fusion_weights.ipynb` — no learned fusion method outperformed equal weights by a meaningful margin.

**Fusion formula:**
```
fused_score  = (1/3) × (D1_prob_up − 0.5)
             + (1/3) × (H4_prob_up − 0.5)
             + (1/3) × (H1_prob_up − 0.5)

direction    = 1 (bullish) if fused_score ≥ 0, else 0 (bearish)
confidence   = |fused_score| × 2   [0–1 scale]
```

D1 is the mandatory anchor — it provides the `volatility_regime`, timestamp, and metadata for the fused output.

---

## Stage 5 — Explainability

Two layers are computed at inference time with no additional model calls.

**Layer 1 — Indicator Snapshot** (`IndicatorSnapshot`)

Five human-readable indicator values captured from the last D1 bar, before MinMax scaling.
The LSTM is a black box — these values do not causally explain its output, but they describe
the market state that accompanied the call, which is what the LLM narrator needs.

| Field | Description |
|-------|-------------|
| `rsi` | RSI(14) value, 0–100. > 70 = overbought, < 30 = oversold |
| `macd_hist` | MACD histogram. Positive = bullish momentum, negative = bearish |
| `bb_pct` | Bollinger Band % position, 0–1. > 0.8 = near upper band (extended) |
| `above_ema200` | bool. True = price in long-term bull structure |
| `atr_pct_rank` | ATR 252-day percentile, 0–1. Mirrors volatility_regime |

**Layer 2 — Timeframe Votes** (`dict[str, int]`)

The individual directional vote from each timeframe model before fusion. Keys: "D1", "H4", "H1".
Values: 1 = bullish, 0 = bearish.

Unanimous agreement (3/3) is meaningfully more reliable than a split (2/1) given the model's
empirically measured AUC of 0.50–0.54. A 2/1 split signals lower structural conviction.

---

## Final Output — `TechnicalSignal`

One signal per pair per day, passed to the Coordinator.

| Field | Type | Description |
|---|---|---|
| `pair` | str | e.g. "EURUSDm" |
| `timeframe` | str | "MTF" (multi-timeframe fused) |
| `timestamp_utc` | Timestamp | Date of the signal (from D1 anchor) |
| `direction` | int | 1 = bullish, 0 = bearish |
| `prob_up` | float | Fused probability of next bar closing higher |
| `prob_down` | float | 1 − prob_up |
| `confidence` | float | 0–1, typically 0.02–0.04 (weak signal) |
| `volatility_regime` | str | "high" or "low" (from D1 ATR percentile) |
| `threshold_used` | float | Decision boundary (default 0.5) |
| `model_version` | str | Artifact version tag |
| `indicator_snapshot` | IndicatorSnapshot | Layer 1 — key market state indicators at inference time |
| `timeframe_votes` | dict[str, int] | Layer 2 — per-timeframe directional votes (D1/H4/H1) |

---

## Pipeline Summary

```
MetaTrader5 Terminal
       ↓
MT5Collector → Bronze CSV  (data/raw/mt5/)
       ↓
PriceNormalizer → Silver Parquet  (data/processed/ohlcv/)
       ↓
add_features() → 15 indicators + atr_pct_rank
       ↓
IndicatorSnapshot ← last unscaled bar (Layer 1 explainability)
       ↓
TechnicalAgent × 3 timeframes (D1, H4, H1)
  └── 2-layer LSTM → prob_up per timeframe
       ↓
fuse_timeframe_signals() — equal weights (1/3 each)
  └── timeframe_votes preserved (Layer 2 explainability)
       ↓
TechnicalSignal → Coordinator
  direction | confidence | volatility_regime
  indicator_snapshot | timeframe_votes
```
