# Sentiment Agent Pipeline — Social and Media Signals to SentimentSignal

---

## Visualization View (Production)

### Outside the Agent

**StockTwits Silver Checkpoint**
`data/processed/stocktwits/labels_checkpoint.jsonl` — one record per message: `message_id`, `symbol`, `timestamp_published`, `prob_bullish`, `prob_bearish`. Produced by StocktwitsPreprocessor running FinTwitBERT-sentiment inference on Bronze posts. Immutable once written.

**GDELT Silver Parquet**
`data/processed/sentiment/source=gdelt/year={YYYY}/month={MM}/sentiment_cleaned.parquet` — one record per article: parsed tone fields, themes, locations, organizations. Produced by GDELTPreprocessor from Bronze JSONL files.

**Google Trends Silver Parquet**
`data/processed/google_trends/google_trends_fx_YYYY-MM-DD_YYYY-MM-DD.parquet` — weekly RSV for 34 FX-relevant keywords across 4 thematic groups, forward-filled to daily.

**Reddit Silver Parquet**
`data/processed/sentiment/source=reddit/reddit_daily_signal.parquet` — daily per-pair aggregate: `risk_off_score`, `risk_on_score`, `sentiment_strength_wmean`, `signal_post_count`, from 25,458 LLM-labeled Reddit posts.

---

### Inside the Sentiment Agent

**StocktwitsSignalNode** — Tier A (Alpha)
Reads the Silver checkpoint, applies exponential decay (half-life 5 days, 14-day window), aggregates to daily `net_sentiment` and `signal_post_count` per pair. The USDJPY `net_sentiment` is the only Bonferroni-proof alpha signal in this agent. IC vs next-day USDJPY realized vol: h=1 −0.163 (p=0.000241), h=3 −0.186 (p=0.000029). Higher bullish sentiment → lower volatility (risk-on = JPY calm).

**GDELTSignalNode** — Tier B (Regime Overlay)
Groups Silver articles by day, computes `tone_mean` and `article_count`, applies 30-day rolling z-score normalization. Outputs `tone_zscore` (macro negativity relative to recent baseline) and `attention_zscore` (news volume surge). Both are regime signals — they gate confidence in alpha signals, not provide alpha themselves.

**GoogleTrendsSignalNode** — Tier B (Regime Overlay)
Reads weekly Silver RSV, forward-fills to daily, applies 52-week rolling z-score. Outputs `macro_attention_zscore` (composite of macro-theme keywords). Captures retail attention surges that precede volatility events. Regime signal only — no directional IC.

**RedditSignalNode** — Tier C (Context / Explainability)
Loads daily Reddit signal parquet. Used only when `include_context=True` — not part of the main signal path. Provides rolling 30-day activity z-score and pair-level risk sentiment for human-readable context. IC vs forward vol and direction is near-zero — this node is explicitly reactive, not predictive.

**SentimentAgent (aggregator)**
Calls all four nodes over the requested date range. Assembles results into a per-day `SentimentSignal`. Computes `composite_stress_flag` as an attention convergence signal: fires when both `gdelt_attention_zscore` and `macro_attention_zscore` exceed the stress threshold (default 1.0) simultaneously. Derives `stress_sources` (Layer 1 explainability) — a named list of which sources fired. Reddit context is attached to the output only when requested via `include_context=True`.

---

### Downstream

**Coordinator**
Receives `SentimentSignal` alongside `TechnicalSignal`, `MacroSignal`, and `GeopoliticalSignal`. Uses `usdjpy_stocktwits_vol_signal` as a volatility input for all four USD crosses (USDJPY primary, cross-pair scope active in 2025 regime). Uses `composite_stress_flag` and the two z-score fields as regime modulators. Null `usdjpy_stocktwits_vol_signal` means no StockTwits activity today — the coordinator must handle null explicitly.

---

### Flow

```
StockTwits Silver Checkpoint  (labels_checkpoint.jsonl)
        ↓
StocktwitsSignalNode
  exponential decay  half-life=5d · window=14d
  net_sentiment | signal_post_count per pair
        ↓ (USDJPY only → Tier A alpha)

GDELT Silver Parquet  (source=gdelt/)
        ↓
GDELTSignalNode
  daily aggregation → 30d rolling z-score
  tone_zscore | attention_zscore
        ↓ (Tier B regime overlay)

Google Trends Silver Parquet
        ↓
GoogleTrendsSignalNode
  forward-fill weekly→daily → 52w rolling z-score
  macro_attention_zscore
        ↓ (Tier B regime overlay)

Reddit Silver Parquet  (optional, include_context=True only)
        ↓
RedditSignalNode
  30d activity rolling z-score · pair-level risk sentiment
        ↓ (Tier C context / explainability)

[ SENTIMENT AGENT ]
  composite_stress_flag = (gdelt_attention_zscore > 1.0)
                        AND (macro_attention_zscore > 1.0)
  stress_sources ← Layer 1 (which sources fired)       ← always populated
  SentimentContext ← Layer 2 (per-source breakdown)    ← include_context only
        ↓
SentimentSignal
  usdjpy_stocktwits_vol_signal | usdjpy_stocktwits_active
  gdelt_tone_zscore | gdelt_attention_zscore
  macro_attention_zscore | composite_stress_flag
  stress_sources (Layer 1) | context: SentimentContext (Layer 2, optional)
        ↓
Coordinator
```

---

## Overview

The sentiment pipeline aggregates four heterogeneous data sources into a single daily `SentimentSignal`. The architecture is explicitly tiered rather than fused: one Tier A signal contributes alpha, two Tier B signals contribute regime context, and one Tier C node contributes human-readable explainability. There is no fusion layer, no confidence-weighted averaging, and no per-pair directional score. The signal is flat and sparse — most fields can be null on a given day.

---

## Node 1 — StockTwits (Tier A — Alpha)

**Source:** `data/processed/stocktwits/labels_checkpoint.jsonl`
**Coverage:** 2021-09-15 to present. 4 pairs: EURUSD, GBPUSD, USDCHF, USDJPY.

**Signal construction:** Each post contributes `prob_bullish` as a continuous signal (not the `predicted_label` binary). Posts are expanded across a 14-day decay window with exponential decay (half-life 5 days). Daily aggregation produces `net_sentiment = bullish_score − bearish_score` and `signal_post_count` per pair. `signal_post_count > 0` is the gate for whether the signal is active on a given day.

**Validated signal — USDJPY volatility:**

| Horizon | IC | p-value | N |
|---|---|---|---|
| h=1 | −0.163 | 0.000241 | 500 |
| h=2 | −0.169 | 0.000152 | 500 |
| h=3 | −0.186 | 0.000029 | 500 |

Survives global Bonferroni correction at h=3. Walk-forward: 2024 IC=−0.218 (p=0.003), 2025 IC=−0.115 (p=0.042) — significant in both years. Direction: higher bullish net_sentiment → lower next-day USDJPY realized vol (risk-on = JPY calm). Directional IC is non-significant across all tests.

**Cross-pair scope:** In 2025, USDJPY `net_sentiment` also predicts USDCHF vol (h=1 IC=−0.148, p=0.001; survives Bonferroni) and EURUSD vol (h=3 IC=−0.090, p=0.045). Cross-pair IC was flat or wrong-sign in 2024 — a regime shift, not stable history. Vol-residual confounder test confirms the USDCHF cross-pair signal is genuine (residual h=1 IC=−0.118, p=0.008).

**Own-pair IC for EURUSD, GBPUSD, USDCHF:** zero across all tests and all horizons. These pairs' own StockTwits signals are not exposed in `SentimentSignal`. The coordinator applies the USDJPY signal to all four USD crosses with pair-specific weights calibrated to validated IC magnitude.

**Signal exposed:** `usdjpy_stocktwits_vol_signal` (float | None), `usdjpy_stocktwits_active` (bool). Null = no StockTwits activity today, not neutral sentiment.

---

## Node 2 — GDELT (Tier B — Regime Overlay)

**Source:** `data/processed/sentiment/source=gdelt/year={YYYY}/month={MM}/sentiment_cleaned.parquet`
**Coverage:** 2021-01-01 → present. ~683k articles over 2021–2025, 1540 trading days.

**Signal construction:** Daily aggregation of `tone` field across all articles. Rolling 30-day z-score normalization (minimum 10 periods). Days with fewer than 3 articles have `tone_zscore` masked to NaN.

**Validated signal (in-sample, walk-forward pending):**
- `tone_zscore`: IC vs next-day |return|: −0.051 to −0.073 across all four pairs (p<0.05). More negative tone → higher expected volatility.
- `attention_zscore`: IC vs USDJPY next-day |return|: +0.055 (p=0.033).
- Neither signal has directional IC (mean |IC|=0.026).

**Signal exposed:** `gdelt_tone_zscore` (float | NaN), `gdelt_attention_zscore` (float | NaN).

**Validation debt:** walk-forward (train < 2024-01-01, test ≥ 2024-01-01) not yet run. Current IC figures are in-sample only.

---

## Node 3 — Google Trends (Tier B — Regime Overlay)

**Source:** `data/processed/google_trends/google_trends_fx_*.parquet`
**Coverage:** 2021-01-01 → present. 34 keywords across 4 thematic groups (macro_news, central_bank, fx_pairs, risk_appetite). Weekly RSV forward-filled to daily.

**Signal construction:** Weekly RSV series z-scored using a 52-week rolling window (minimum 26 periods). A composite `macro_attention_zscore` is computed as the mean z-score across the macro_news and risk_appetite keyword groups.

**Validated signal (in-sample, walk-forward pending):**
- `macro_attention_zscore`: lasso RMSE 0.005069 vs baseline 0.007828 for next-week absolute return prediction.
- `central_bank_attention_zscore`: ranks #1 in subset ablation by mean rank.
- No meaningful directional IC — balanced accuracy ≈ 0.50.

**Signal exposed:** `macro_attention_zscore` (float | NaN).

**Validation debt:** walk-forward not yet run. Current performance figures are in-sample only.

---

## Node 4 — Reddit (Tier C — Context / Explainability)

**Source:** `data/processed/sentiment/source=reddit/reddit_daily_signal.parquet`
**Coverage:** 2021–2025. 25,458 LLM-labeled posts (r/Forex 50%, r/investing 25%, r/stocks 25%). 3,449 posts pass the signal filter (content_type ≠ NOISE, stance_clarity ≠ QUESTION).

**Signal construction:** Pair attribution via regex on title+body. Engagement weighting: `w = log1p(score_clip_p99) × content_weight` (FUNDAMENTAL/NEWS_REACTION receive 2×). Daily aggregates: `risk_off_score`, `risk_on_score`, `sentiment_strength_wmean`, `signal_post_count`. Rolling 30-day activity z-score computed over global post volume.

**IC result:** Near-zero across all directional and volatility tests. Reddit is predominantly reactive — strongest lag is −1 (markets move, Reddit reacts). Predictive lag +1 rho ≈ 0.054 vs EURUSD absolute return; does not survive BH correction. Excluded from the alpha and regime signal paths.

**Deployment:** not called unless `include_context=True`. When active, populates `SentimentContext.reddit_pair_views` with per-pair `risk_off_score`, `risk_on_score`, `sentiment_strength_wmean`, and `SentimentContext.reddit_global_activity_zscore`. Used for human-readable signal explainability at the coordinator level.

---

## Composite Stress Flag

`composite_stress_flag` is True when both attention z-scores simultaneously exceed the stress threshold:

```python
composite_stress_flag = (gdelt_attention_zscore > 1.0) AND (macro_attention_zscore > 1.0)
```

Fires when both independent attention signals — news flow surge (GDELT) and retail search surge (Google Trends) — converge. Neither alone is sufficient. This signal indicates an elevated macro-attention regime that is likely to suppress the USDJPY vol signal's reliability (high-attention regimes weaken the risk-on interpretation of StockTwits sentiment).

---

## Explainability

Two layers computed at prediction time.

**Layer 1 — `stress_sources: list[str]`** (always populated)

Which z-scores exceeded `STRESS_ZSCORE_THRESHOLD = 1.0`. Makes `composite_stress_flag` interpretable rather than opaque. Possible values:

| Value | Meaning |
|---|---|
| `[]` | Neither source elevated; flag is False |
| `["gdelt_attention"]` | Only GDELT elevated; flag still False |
| `["macro_attention"]` | Only GTrends elevated; flag still False |
| `["gdelt_attention", "macro_attention"]` | Both elevated; flag is True |

**Layer 2 — `context: SentimentContext | None`** (populated when `include_context=True`)

Typed breakdown of per-source signals. `None` by default — Reddit computation is expensive and not needed for alpha generation.

| Field | Type | Description |
|---|---|---|
| `reddit_global_activity_zscore` | float \| None | Rolling 30d z-score of global Reddit post count |
| `reddit_pair_views` | dict[str, dict] \| None | Tier C: per-pair Reddit breakdown (risk_off, risk_on, strength_wmean) |
| `stocktwits_pair_breakdown` | dict[str, dict] \| None | All 4 pairs: bullish_score, bearish_score, net_sentiment |

---

## Final Output — `SentimentSignal`

One signal per day (not per pair), passed to the Coordinator.

| Field | Type | Tier | Description |
|---|---|---|---|
| `timestamp_utc` | Timestamp | — | UTC day the signal represents |
| `usdjpy_stocktwits_vol_signal` | float \| None | A | USDJPY net_sentiment when signal_post_count > 0, else None |
| `usdjpy_stocktwits_active` | bool | A | True when StockTwits signal_post_count > 0 |
| `gdelt_tone_zscore` | float \| None | B | 30d rolling z-score of GDELT tone. Negative = elevated macro negativity |
| `gdelt_attention_zscore` | float \| None | B | 30d rolling z-score of GDELT article count |
| `macro_attention_zscore` | float \| None | B | 52w rolling z-score of Google Trends macro keywords |
| `composite_stress_flag` | bool | B | True when both attention z-scores exceed threshold simultaneously |
| `stress_sources` | list[str] | B/L1 | Layer 1 — which sources triggered the stress flag |
| `context` | SentimentContext \| None | C/L2 | Layer 2 — typed per-source breakdown (include_context only) |

---

## Pipeline Summary

```
StockTwits Silver  (labels_checkpoint.jsonl)
       ↓
StocktwitsSignalNode
  └── decay · aggregation → net_sentiment · signal_post_count (USDJPY)
       ↓
GDELT Silver  (source=gdelt/)
       ↓
GDELTSignalNode
  └── daily agg → 30d z-score → tone_zscore · attention_zscore
       ↓
Google Trends Silver
       ↓
GoogleTrendsSignalNode
  └── forward-fill → 52w z-score → macro_attention_zscore
       ↓
Reddit Silver  (optional — include_context=True only)
       ↓
RedditSignalNode
  └── 30d activity z-score · pair-level risk sentiment
       ↓
SentimentAgent.compute_batch()
  ├── composite_stress_flag = gdelt_attention > 1.0 AND macro_attention > 1.0
  ├── stress_sources  ← Layer 1 (always populated)
  └── SentimentContext  ← Layer 2 (include_context=True only)
       ↓
SentimentSignal → Coordinator
  usdjpy_stocktwits_vol_signal (Tier A · vol predictor)
  gdelt_tone_zscore · gdelt_attention_zscore (Tier B · regime)
  macro_attention_zscore · composite_stress_flag (Tier B · regime)
  stress_sources (Layer 1) | context: SentimentContext (Layer 2 · optional)
```
