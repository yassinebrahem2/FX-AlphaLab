# Alpha Generator Pipeline — Signals to Actionable FX Recommendation

---

## Visualization View (Production)

### Inputs

**Technical Agent**
Daily directional signal per pair from LSTM multi-timeframe fusion. Direction (bullish/bearish),
confidence score, volatility regime, indicator snapshot, and per-timeframe votes. Validated at
AUC 0.50–0.54 — near-random on direction, but the top-8.5% confidence gate on USDJPY shows
58.6% accuracy (binomial p=0.014).

**Macro Agent**
Monthly directional signal per pair from hybrid VAR-LSTM macro model with economic calendar
overlay. Stable signal (1–2% flip rate). Directional IC vs 5d forward return: +0.115 for GBPUSD
(medium confidence), +0.060 for EURUSD and USDJPY (low), +0.033 for USDCHF (low).

**Geopolitical Agent**
Daily bilateral FX risk score from a 15-seed GAT ensemble on GDELT zone features. Validated as a
forward volatility signal (IC = +0.099 vs fwd_vol_3d, OOS 2025). Independent of StockTwits
(partial IC = −0.30 on USDJPY active days; geo R² = 0.005). Collapses to a global stress indicator
(all 4 pairs receive the same signal due to zone collapse).

**Sentiment Agent — StockTwits**
USDJPY-specific forward volatility signal from scraped StockTwits crowd sentiment. IC = −0.44
vs fwd_vol_10d (p < 0.000001). Active on 40% of USDJPY days — naturally FLAT on the remaining
60%. Dominates the geopolitical signal on active days; geo adds nothing when StockTwits is present.
Sign-inverted at consumption (higher signal = lower vol).

**Signal Table**
`data/processed/coordinator/signals_aligned.parquet` — one row per pair per trading day,
all agent outputs joined. 4,172 rows × 20 signal columns, 2022–2025. `fwd_*` columns present
for evaluation only — stripped before any report computation.

---

### Inside the Alpha Generator (Coordinator)

**Signal Router**
For each pair, routes to the appropriate directional source based on validated empirical IC:
- USDJPY, top-8.5% tech confidence days → `tech_usdjpy` (high tier, 1d horizon)
- USDJPY, all other days → `macro_usdjpy` (low tier, 5d horizon)
- GBPUSD → `macro_gbpusd` (medium tier, 5d horizon)
- EURUSD, USDCHF → respective macro signal (low tier, 5d horizon)

No signal is used without a validated empirical IC or accuracy gate. The macro regime signal
and GDELT attention signal were explicitly dropped from the directional track.

**Vol Track Fusion**
Computes a sign-normalized volatility forecast used exclusively for SL/TP sizing:
- USDJPY, StockTwits active: use StockTwits (inverted) — geo adds nothing on these days
- USDJPY, StockTwits inactive: use geo bilateral risk (IC = +0.109 on USDJPY geo-only days)
- EUR/GBP/CHF pairs: use geo bilateral risk (IC = +0.12–0.14)

**OLS Vol Calibration**
Maps the normalized vol signal to expected daily log-return standard deviation using an OLS
regression fit on training data (2022–2023) only. The calibrated `estimated_vol_3d` is then
used to set SL and TP distances — it never influences the direction decision. Calibration
constants should be refit quarterly (expanding window).

| Source | Intercept | Slope | Test IC |
|--------|-----------|-------|---------|
| Geo | 0.004057 | 0.002405 | +0.116 (2024–2025) |
| StockTwits | 0.006403 | 0.004930 | +0.230 (2025 only) |
| Baseline (no signal) | 0.004524 | — | — |

**Conviction Scoring**
`conviction = tier_weight × direction_edge`

| Source | Edge | Tier weight | Conviction |
|--------|------|-------------|------------|
| tech_usdjpy | 0.086 (accuracy − 0.5) | 1.0 (high) | 0.086 |
| macro_gbpusd | 0.115 (IC_5d) | 0.6 (medium) | 0.069 |
| macro_eurusd | 0.060 (IC_5d) | 0.3 (low) | 0.018 |
| macro_usdjpy | 0.058 (IC_5d) | 0.3 (low) | 0.017 |
| macro_usdchf | 0.033 (IC_5d) | 0.3 (low) | 0.010 |

**HOLD Gate**
Filters out days with no meaningful signal:
- `max(conviction) < 0.02` → hold unconditionally
- `regime == high_attention AND max(conviction) < 0.05` → hold
- In practice, GBPUSD medium conviction (0.069) prevents HOLD on nearly every day where
  macro_direction is available. HOLD fires only on data-gap days.

**Position Sizing**
Notional allocation as % of equity, discounted in elevated-attention regime:
- high tier (USDJPY tech-gate): 2.0% × (0.75 if high_attention)
- medium tier (GBPUSD macro): 1.0% × (0.75 if high_attention)
- low tier (other macro): 0.5% × (0.75 if high_attention)

**SL/TP Construction**
SL and TP distances are set as a fixed multiple of the calibrated expected move:
- 1d horizon: `expected_move = estimated_vol_3d`
- 5d horizon: `expected_move = estimated_vol_3d × √5`
- `SL = 1.5 × expected_move × 100` (as % of entry)
- `TP = 2.5 × expected_move × 100`
- Risk/reward ratio: always 1.67

Typical values: SL 1.42–1.60%, TP 2.37–2.67%

**Regime Overlay**
`macro_attention_zscore > 1.0` triggers `high_attention` regime. Effect: position sizes scaled
down 25%, HOLD conviction floor raised to 0.05. Does not change direction.

**CoordinatorReport**
The final output packet, produced deterministically. Contains:
- `top_pick` — the pair to trade (GBPUSD 83.3% of days, USDJPY 16.7%)
- `overall_action` — "trade" or "hold"
- One `PairAnalysis` per pair: direction, conviction, position %, SL %, TP %, estimated vol,
  regime, all raw signal values
- `narrative_context` — pre-formatted dict for the LLM narrator (all numbers pre-computed)

**LLM Narrator**
Receives `narrative_context` and produces 4–6 sentences of plain-English rationale. Strict rule:
the LLM narrates the pre-computed numbers — it never invents or modifies any figure. Output
covers: what to trade and why, how much to risk, where to place SL/TP, regime context, and a
one-line summary of secondary pairs.

---

### Downstream — User Interface

**CoordinatorReport JSON**
Persisted to `outputs/reports/{date}.json` after each daily run. Stale-input labeling: if any
upstream source fails to refresh, the report is still produced with a `stale_inputs` field listing
the affected sources.

**Streamlit Dashboard**
Reads the latest `CoordinatorReport`. Displays: overall action (TRADE/HOLD), top pick with
direction and confidence tier, position size %, SL/TP % distances, regime context, secondary
pairs summary, and the LLM narrative. The user trades manually in MT5 — no broker integration.

---

### Flow

```
Technical Agent    Macro Agent    Geopolitical Agent    StockTwits Node
       │                │                 │                     │
       └────────────────┴─────────────────┴─────────────────────┘
                                    │
                            Signal Table (parquet)
                         signals_aligned.parquet
                         fwd_* columns stripped
                                    │
                        ┌───────────────────────┐
                        │   ALPHA GENERATOR     │
                        │                       │
                        │  Signal Router        │
                        │  ↓                    │
                        │  Vol Fusion           │
                        │  ↓                    │
                        │  OLS Vol Calibration  │
                        │  ↓                    │
                        │  Conviction Scoring   │
                        │  ↓                    │
                        │  HOLD Gate            │
                        │  ↓                    │
                        │  Position Sizing      │
                        │  ↓                    │
                        │  SL/TP Construction   │
                        │  ↓                    │
                        │  Regime Overlay       │
                        │  ↓                    │
                        │  CoordinatorReport    │
                        │  ↓                    │
                        │  LLM Narrator         │
                        └───────────────────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    │                               │
            outputs/reports/{date}.json    Streamlit Dashboard
                                                    │
                                             User → MT5
```

---

## Overview

The alpha generator (Coordinator) is the top-level decision layer of FX-AlphaLab. It consumes
validated signals from four specialized agents, applies a deterministic fusion and sizing policy,
and produces a single actionable recommendation per day: which pair to trade, in which direction,
with what size, and where to exit.

It does not learn from new data during inference. All parameters (conviction weights, position
sizes, OLS calibration constants, HOLD thresholds) are fixed from empirical validation. The only
data it reads at inference time is the current day's signal table row.

---

## Stage 1 — Signal Table Construction

**What happens:** Each agent runs independently and produces a signal for date T:
- Technical agent: inference on OHLCV data up to T
- Macro agent: month-end macro parquet lookup + calendar event overlay
- Geopolitical agent: GDELT zone features for T−1 (one-day lag from source)
- Sentiment agent: StockTwits scores for T (null when no data)

All signals are joined by `(date, pair)` into a single flat parquet.

**Output:** `data/processed/coordinator/signals_aligned.parquet`
- Schema: date, pair, tech_direction, tech_confidence, tech_vol_regime,
  geo_bilateral_risk, geo_risk_regime, macro_direction, macro_confidence,
  macro_{carry,regime,fundamental,surprise,bias}_score, macro_dominant_driver,
  usdjpy_stocktwits_vol_signal, gdelt_tone_zscore, gdelt_attention_zscore,
  macro_attention_zscore, composite_stress_flag, fwd_ret_1d, fwd_ret_5d,
  fwd_vol_3d, fwd_vol_5d, fwd_vol_10d
- `fwd_*` columns: present for offline evaluation only, stripped before CoordinatorReport

---

## Stage 2 — Directional Signal Routing

**What happens:** For each pair, `coordinator_predict()` selects the appropriate directional
source based on empirically validated IC/accuracy gates. The routing is deterministic and
stateless — no learned weights, no runtime calibration.

**USDJPY routing logic:**
```
if tech_confidence ≥ USDJPY_P75_THRESHOLD (0.0887):
    direction ← tech_direction     # top 8.5% confidence days
    tier = "high", horizon = "1d"
else:
    direction ← macro_direction    # 82.7% of USDJPY days
    tier = "low", horizon = "5d"
```

`USDJPY_P75_THRESHOLD` is computed from training data (2022–2023) only. The tech gate fires
on 16.7% of all USDJPY days over 2022–2025 (174 / 1,043).

**GBPUSD / EURUSD / USDCHF:** directly consume `macro_direction` with pair-specific IC and
tier assignments.

---

## Stage 3 — Volatility Track Fusion

**What happens:** The vol signal is sign-normalized before calibration (StockTwits IC is negative
— higher sentiment z-score predicts lower vol, so the raw signal is inverted).

**Fusion rule:**
- USDJPY + StockTwits active (40% of USDJPY days): use StockTwits (inverted). On these days,
  geo partial IC = −0.302 but its R² contribution to StockTwits is 0.005 — geo is irrelevant.
- USDJPY + StockTwits inactive: use geo (IC = +0.109 at fwd_vol_3d on geo-only USDJPY days)
- EUR/GBP/CHF: use geo (IC = +0.12–0.14 at fwd_vol_3d)

The vol track only influences SL/TP sizing. It does not affect direction or conviction.

---

## Stage 4 — Conviction, Sizing, and Report Construction

**Conviction scoring:** `conviction = tier_weight × direction_edge`

Tier weights express empirical confidence in each signal source: 1.0 for the tech gate (validated
by binomial test), 0.6 for GBPUSD macro (IC=+0.115, p<0.001), 0.3 for other macro signals.

**SL/TP determinism:** SL and TP distances are computed solely from the calibrated vol forecast
and the signal horizon. The R:R ratio is always 1.67 (2.5/1.5). This means SL/TP widens on
high-vol days and narrows on low-vol days — the system automatically scales risk to market
conditions without any manual adjustment.

**HOLD gate:** In practice, GBPUSD medium conviction (0.069) clears the 0.02 floor on every
day where macro_direction is available. The system was in TRADE 100% of the 2022–2025 days
with a valid signal. HOLD is a safety net for data-gap days.

**Output — `CoordinatorReport`:** Fully deterministic. The LLM receives pre-computed numbers
only — it is not permitted to derive, modify, or invent any figure.

---

## Stage 5 — Backtesting (NB05)

**Execution model:**
- Entry: T+1 first H1 bar open (signal for T is available after T's NY close; entry on T+1
  avoids any look-ahead from T's OHLCV data being used by the technical agent)
- SL/TP: resolved on H1 bars during T+1; worst-case assumption when both SL and TP cross on the
  same bar (SL takes priority)
- Check-in: at T+1 end of day — if signal continues, hold; if signal flips or HOLD fires, close
- Cost: 2 pips round-trip per entry (no slippage model)

**Results (2022–2025, 150 trades):**

| Metric | Value |
|--------|-------|
| Total return | +0.40% |
| Sharpe ratio | 1.077 |
| Max drawdown | −0.13% |
| Win rate | 50.7% |
| Avg win | +1.36% notional |
| Avg loss | −0.95% notional |
| Profit factor | 1.48 |
| Avg hold | 5.8 days |

**Per pair:**
- GBPUSD: 76 trades, 51.3% win, +0.20% equity contribution
- USDJPY: 74 trades, 50.0% win, +0.20% equity contribution

**Annual trend:** 2022 +0.26%, 2023 +0.22%, 2024 −0.02%, 2025 −0.06%. The macro GBPUSD signal
loses edge in the 2024–2025 low-volatility FX regime — worth monitoring in live operation.

**Interpretation:** Total return is small because position sizes are small (0.75–2% of equity).
The Sharpe of 1.08 and max DD of 0.13% confirm the system is structurally sound. Scale position
sizes proportionally in live operation based on actual account equity.

---

## Signal Quality Summary

| Signal | Track | Validated | IC / Accuracy | Notes |
|--------|-------|-----------|---------------|-------|
| GBPUSD macro direction | Directional | Yes (walk-forward) | IC = +0.115 vs fwd_ret_5d | Anchor — 83.3% of top picks |
| USDJPY tech gate (top 8.5%) | Directional | Yes (binomial p=0.014) | Accuracy = 58.6% | 16.7% of top picks |
| Geo bilateral risk | Vol sizing | Yes (OOS 2025) | IC = +0.099 vs fwd_vol_3d | Global stress; zone collapse |
| StockTwits USDJPY | Vol sizing | Yes (Bonferroni-proof) | IC = −0.44 vs fwd_vol_10d | Active 40% of USDJPY days |
| Google Trends attention | Regime context | No (in-sample only) | IC = +0.16 vs fwd_vol_10d | Walk-forward pending |
| GDELT GKG tone | Vol context | No (in-sample only) | IC = +0.08 vs fwd_vol_10d | Walk-forward pending |
| EURUSD / USDCHF macro | Directional | Partial (low IC) | IC = +0.060 / +0.033 | Low tier; sizing only |
| GDELT attention | — | Dropped | IC = −0.04 | Anti-predictive |

---

## Final Output — `CoordinatorReport`

One report per trading day. Consumed by the Streamlit dashboard and persisted as JSON.

| Field | Type | Description |
|-------|------|-------------|
| `date` | Timestamp | Signal date |
| `pairs` | list[PairAnalysis] | One analysis per pair (4 total) |
| `top_pick` | str \| None | Pair to trade; None when overall_action = "hold" |
| `overall_action` | str | "trade" or "hold" |
| `hold_reason` | str \| None | Why HOLD fired (None when trading) |
| `global_regime` | str | "high_attention", "normal", or "unknown" |
| `narrative_context` | dict | Pre-formatted packet for LLM prompt |

**`PairAnalysis` fields:**

| Field | Description |
|-------|-------------|
| `direction` | 1 = long base, 0 = short base, None = no data |
| `suggested_action` | "LONG", "SHORT", or "FLAT" |
| `confidence_tier` | "high", "medium", "low", or "none" |
| `direction_source` | Which agent provided the directional call |
| `direction_horizon` | "1d" or "5d" |
| `conviction_score` | tier_weight × direction_edge |
| `position_size_pct` | % of equity to allocate |
| `sl_pct` | Stop-loss distance from entry as % of price |
| `tp_pct` | Take-profit distance from entry as % of price |
| `estimated_vol_3d` | OLS-calibrated expected daily vol (log-return units) |
| `vol_source` | "stocktwits", "geo", or "none" |
| `regime` | "high_attention", "normal", or "unknown" |
| `raw_signals` | All agent outputs for LLM context |

---

## Live Inference Notes

- `current_price` (from MT5) needed to convert SL/TP % to pips:
  `pips = sl_pct / 100 × price / pip_size`
- `account_equity` needed to convert position_size_pct to notional:
  `size_currency = position_size_pct / 100 × account_equity`
- Vol calibration constants (GEO_SLOPE/INTERCEPT, ST_SLOPE/INTERCEPT) should be refit
  quarterly using an expanding window — do not refit daily
- StockTwits is null on ~60% of USDJPY days — the fallback (geo signal) fires automatically
- If any upstream source fails, the report still runs with `stale_inputs: [source_name, ...]`
  populated. The user sees the staleness warning in the dashboard

---

## Pipeline Summary

```
MT5 / FRED / ECB / GDELT / StockTwits / Google Trends / ForexFactory
       ↓
Bronze Storage (raw, immutable)
       ↓
Silver Storage (preprocessed parquets)
       ↓
Technical Agent → TechnicalSignal (direction, confidence, vol_regime)
Macro Agent     → MacroSignal (direction, confidence, dominant_driver, top_events)
Geo Agent       → GeopoliticalSignal (bilateral_risk_score, risk_regime, top_events)
Sentiment Agent → SentimentSignal (stocktwits_vol_signal, gdelt_tone, attention_z)
       ↓
signals_aligned.parquet  [fwd_* stripped at report time]
       ↓
coordinator_predict()  →  CoordinatorSignal per pair
       ↓
build_pair_analysis()  →  PairAnalysis (conviction, pos%, SL, TP, vol_est)
       ↓
build_coordinator_report()  →  CoordinatorReport
       ↓
build_llm_prompt(report)  →  LLM narrative (prose only, no invented numbers)
       ↓
outputs/reports/{date}.json
       ↓
Streamlit Dashboard  →  User  →  Manual execution in MT5
```
