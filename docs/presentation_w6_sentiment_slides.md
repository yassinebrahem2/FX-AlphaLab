# W6 Presentation — Sentiment Analysis Section
> Structure: Title → per data group (Pipeline → EDA → Insights) → cross-cutting analysis → summary

---

## Slide 1 — Section Title & Data Sources

**Title:** Sentiment Analysis — 5 Signal Sources, One Question: Do They Move the Market?

**Text:**
> We attempted to collect 5 years of sentiment data (2021–2026) across 5 sources and asked a single empirical question: do any of these signals carry real information about FX price behavior?

**Sources:**

| Source | What We Collected | How Collected | Size |
|--------|-------------------|---------------|------|
| Fed | Press releases, speeches, policy statements | Custom web scraper | 1,134 docs |
| ECB | Press releases, speeches, policy statements | Custom web scraper | 1,111 docs |
| BoE | Press releases, speeches, policy statements | Custom web scraper | 1,029 docs |
| Reddit r/Forex | Retail trader post titles | Arctic Shift API | 3,474 posts |
| StockTwits | Self-labeled bullish/bearish trade intentions | StockTwits API | 1,177 posts |
| Google Trends | Weekly search interest index per FX pair | Google Trends API | 2,337 weekly rows |
| GDELT | Global news event stream | GDELT API — **pipeline deferred** | — |

**Graph:** None — overview slide.

---

---
## GROUP 1 — Central Bank Documents (Fed · ECB · BoE)
---

## Slide 2 — CB Documents: Pipeline

**Title:** Central Bank Documents — Scoring Institutional Tone with FinBERT

**How we processed the data:**
- Each document (speech, press release, policy statement) is scored by FinBERT on `headline + first 400 characters of body`
- FinBERT outputs three probabilities: P(positive), P(negative), P(neutral) → `sentiment_score = P(positive) − P(negative)` ∈ [−1, +1]
- **Fan-out logic:** a hawkish Fed statement means "USD is strengthening" — but that has opposite implications depending on the pair. USD strengthening is good for USDJPY (USD is bought) and bad for EURUSD (USD is the other side). So one document produces multiple signals, one per pair, with the sign flipped where needed
- **Known limitation going in:** 400-character body truncation misses most of a 5,000-word speech — long-document scoring is documented as a limitation for the modeling phase

**Graph:** Pipeline diagram (hand-drawn): Document → FinBERT → fan-out per pair → adjusted signal.

---

## Slide 3 — CB Documents: EDA

**Title:** Central Bank Tone — Fed Mostly Neutral, ECB & BoE More Expressive

- **Fed:** 97.8% neutral — deliberately hedged language
- **ECB:** 40.8% extreme scores — clearest tonal signal of the three
- **BoE:** 24.8% extreme scores — moderate
- Tone shifts align with the 2022–2023 hiking cycle across all banks

**Graph:** FinBERT sentiment score time series per CB source — from Part 2.

---

## Slide 4 — CB Documents: Method Comparison

**Title:** FinBERT vs VADER vs TextBlob — Methods Disagree Completely

| Pair | Spearman ρ |
|------|-----------|
| FinBERT ↔ VADER | 0.116 |
| FinBERT ↔ TextBlob | −0.067 |
| VADER ↔ TextBlob | 0.069 |

- ρ < 0.12 — methods measure fundamentally different things
- VADER/TextBlob miss CB hedged vocabulary → **FinBERT is the only valid choice**

**Graph:** Method comparison heatmap — from Part 3.

---

## Slide 5 — CB Documents: Full Body Scoring

**Title:** Beyond Headlines — CBRT-RoBERTa on the Full Document

- 400-char truncation misses forward guidance entirely
- Fix: score each sentence individually, aggregate as `mean(P_hawkish − P_dovish)`
- Results: Fed **−0.109** (dovish) · ECB **+0.044** · BoE **+0.054** — consistent with the 2024–2025 rate cycle

**Graph:** Hawkish/dovish score distribution per source — from Part 12.

---

---
## GROUP 2 — Reddit r/Forex
---

## Slide 6 — Reddit: Pipeline

**Title:** Reddit r/Forex — Retail Discourse via Arctic Shift API

- Collected via **Arctic Shift API**; FinBERT applied to post titles
- **2021 backfill artifact:** 11,383 posts in 2021 vs 558 in 2022 (95% drop) — not representative → **analysis restricted to 2022+**
- Topic heterogeneity noted upfront: trade setups, complaints, P&L, memes — not all posts are macro-informative

**Graph:** Post volume per year (showing the 2021 spike) — from Part 4.

---

## Slide 7 — Reddit: EDA & Insights

**Title:** Reddit — Negative-Dominant, Low Signal-to-Noise

- 79.7% neutral, 13.4% negative, 6.9% positive — community skews toward loss reporting and frustration
- EURUSD accounts for 45% of pair-tagged posts; only ~18% carry "Fundamental Analysis" flair
- No seasonality (p = 0.13) — event-driven, not calendar-driven
- FinBERT not validated on informal language — a known limitation

**Verdict:** Supplementary ensemble feature; not an anchor signal.

**Graph:** Sentiment distribution + pair breakdown — from Part 4.

---

---
## GROUP 3 — StockTwits
---

## Slide 8 — StockTwits: Pipeline

**Title:** StockTwits — Self-Labeled Intentions, No NLP Needed

- Traders explicitly tag each post as **Bullish** or **Bearish** — no NLP required
- Aggregated to monthly `bullish_ratio = N_bullish / (N_bullish + N_bearish)`, centered around 0.5
- Minimum 5 labeled posts per (pair, month) required to suppress noise

**Graph:** Post count per month (showing sparsity) — from Part 5.

---

## Slide 9 — StockTwits: EDA & Insights

**Title:** StockTwits — Slight Bullish Bias, Too Sparse to Use Alone

- Slight bullish bias: mean bullish_ratio = 0.52, p = 0.04 — consistent with known retail long bias
- Only **70 valid monthly observations** after the ≥5 threshold; only EURUSD and USDJPY have consistent coverage
- No seasonality (p = 0.83)
- Labels are user-generated → no FinBERT model dependency (an advantage)

**Verdict:** Low-weight ensemble feature; revisit when volume grows.

**Graph:** `bullish_ratio` time series per pair — from Part 5.

---

---
## GROUP 4 — Google Trends
---

## Slide 10 — Google Trends: Pipeline

**Title:** Google Trends — Attention Proxy, Not Sentiment

- Measures search intensity (0–100) — **attention, not direction**
- Raw index was mislabeled as `sentiment_score` in the Silver layer → fixed before analysis
- Rolling 52-week z-score applied: converts absolute search levels into relative spikes, comparable across pairs and time
- z > 1.5 defined as an attention spike

**Graph:** Raw index vs z-score overlay — from Part 6.

---

## Slide 11 — Google Trends: EDA & Insights

**Title:** Spikes Are Rare — But Carry the Strongest Volatility Effect (d = 0.857)

- Normalized to μ ≈ 0, σ ≈ 1; spikes (z > 1.5) in only 2–5% of weeks — event-driven, no seasonality (p = 0.59)
- Spike vs normal: p = 0.0003, Cohen's d = **0.857** — largest effect across all sources
- **Not directional** — Granger fails (p > 0.75); signals volatility, not price direction
- Use case: volatility regime flag

**Graph 1:** Z-score time series per pair with spike threshold — from Part 6.
**Graph 2:** Returns during spike vs normal weeks, d = 0.857 annotated — from Part 7.

---

---
## CROSS-CUTTING ANALYSIS
---

## Slide 12 — Market Impact: Do Signals Move FX Prices?

**Title:** Volatility Amplification Confirmed — Direction Not

| Comparison | p-value | Cohen's d |
|------------|---------|-----------|
| Strong CB signal vs quiet (EURUSD) | < 0.0001 | 0.300 |
| Weak CB signal vs quiet (EURUSD) | < 0.0001 | 0.294 |
| All 3 signal groups (Kruskal-Wallis) | < 0.0001 | — |
| BoE signal vs quiet (GBPUSD) | < 0.0001 | 0.359 |
| **Google Trends spike vs normal** | **0.0003** | **0.857** |
| Granger: sentiment → return | > 0.14 | — |
| Granger: return → sentiment | > 0.75 | — |

- Signals amplify volatility — robust across all sources
- No Granger causality — sentiment does not predict direction at monthly resolution
- These are not contradictory: **volatility amplification ≠ directional prediction**

**Graph:** 3-panel box plot with Cohen's d annotations — from Part 7.

---

## Slide 13 — Source Independence & Ensemble Rationale

**Title:** All Sources Are Near-Independent — Ensemble Is Justified

- Max pairwise correlation: **|r| = 0.20** — no two sources measure the same thing
- Each source adds independent information → combining them is theoretically sound

**Graph:** Pairwise correlation heatmap — from Part 8.

---

## Slide 14 — Correlation with MT5 Returns

**Title:** How Well Does Each Source Correlate with EURUSD Price?

| Source | Pearson r with EURUSD monthly return |
|--------|--------------------------------------|
| Fed (adjusted) | −0.101 |
| ECB (adjusted) | −0.041 |
| Reddit | +0.075 |
| StockTwits | +0.099 |
| Google Trends | −0.105 |

- All correlations |r| ≤ 0.11 — no single source is a strong standalone predictor
- Confirms signals must be combined and used at higher frequency to extract value

**Graph:** Correlation bar chart (sources vs EURUSD return) — from Part 9 panel cell.

---

## Slide 15 — Modeling Baseline

**Title:** No Model Beats the Baseline — Monthly Resolution Is Insufficient

- All sources together → only 7 usable observations (StockTwits kills coverage)
- Working set: Fed + ECB + Google Trends + Reddit → **n = 47**; features lagged 1 month

| Model | CV Accuracy |
|-------|------------|
| Majority Baseline | 54.3% |
| Logistic Regression | 51.4% |
| Random Forest | 57.1% |
| **Gradient Boosting** | **62.9% ± 0.23** |
| XGBoost | 57.1% |
| LightGBM | 54.3% |

- Gradient Boosting looks best but variance is too high (±0.23, ~5–8 obs per fold) — not reliable
- **No model convincingly beats the baseline** — consistent with Granger findings

**Graph:** CV accuracy bar chart vs majority baseline — from Part 9.

---

## Slide 16 — Summary & Roadmap

**Title:** What We Know — What We Build Next

**Key findings:**
1. CB event days are significantly more volatile (d ≈ 0.30)
2. Google Trends spikes are the strongest signal found (d = 0.857)
3. No Granger causality at monthly resolution — direction signal doesn't survive aggregation
4. All sources are near-independent (max |r| = 0.20) — ensemble is justified
5. No model beats the baseline reliably — monthly features are insufficient for direction

**W7 priorities:**

| Fix | Why |
|-----|-----|
| Fix Fed collector → full speech bodies | Currently only 38–49 word summaries |
| GDELT pipeline fix + backfill | Biggest coverage gap |
| Switch to event-driven features (same-day H1) | Monthly aggregation discards timing |
| Combine with macro surprise features (Notebook 04a) | CPI surprise ρ = −0.45, NFP ρ = −0.43 — stronger than any sentiment source |
| Backfill 10+ years of CB docs → weak supervision labels | Enables domain fine-tuning |

**Graph:** Source quality summary table — from Part 11.

---
