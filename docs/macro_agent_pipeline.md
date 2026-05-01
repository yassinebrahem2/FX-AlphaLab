# Macro Agent Pipeline — Monthly Macro Signal to Coordinator

---

## Visualization View (Production)

### Outside the Agent

**Macro Signals Store** — `data/processed/macro/macro_signal.parquet`
Precomputed monthly macro directional outputs per pair. Fields: `pair`, `timestamp_utc` (month-end), `module_c_direction`, `macro_confidence`, `carry_signal_score`, `regime_context_score`, `fundamental_mispricing_score`, `macro_surprise_score`, `macro_bias_score`.

**Economic Events Store** — `data/processed/events/events_2021-01-01_2025-12-31.csv`
Structured economic calendar. Fields: `timestamp_utc`, `event_id`, `country`, `event_name`, `impact` (high/medium/low/non-economic), `actual`, `forecast`, `previous`, `source`.

**Market Context Stores**
- D1 OHLCV parquets per pair: `data/processed/ohlcv/ohlcv_{PAIR}_D1_*.parquet`
- VIX daily series: `data/processed/macro/macro_VIXCLS_2021-01-01_2026-02-21.csv`

**Model Artifacts Store** — `models/production/macro/`
- `lgb_models.joblib` — LightGBM pipelines for EURUSD and USDCHF
- `catboost_models.joblib` — CatBoost models for GBPUSD and USDJPY

---

### Inside the Macro Agent

**Node 1: Macroeconomics Node (Baseline Macro Direction)**
Loads monthly macro outputs once and serves temporal lookups for requests.

Key role:
- Returns the baseline macro direction package for a pair at inference time.
- Uses month-aware lookup logic: a request for 2025-02-15 maps to the 2025-01-31 signal.

Output contribution:
- All baseline fields: direction, confidence, carry/regime/fundamental/bias scores.
- `macro_surprise_score` remains 0.0 unless Node 2 is active.

**Node 2: Calendar Events Node (Event Surprise Overlay)**
Performs one expensive load-time pass to pre-score events using trained models, then serves lightweight monthly surprise aggregation at inference.

Load-time responsibilities:
- Reads LGB/CatBoost artifacts and feature schemas.
- Expands events per covered pair using `PAIR_COUNTRY_SIGN` routing.
- Applies `IMPACT_WEIGHT` filtering (non-economic events excluded).
- Builds event-level price and VIX context features.
- Computes interaction features and batch-scores event probabilities.
- Stores a compact pre-scored event table (`_event_scores`) for fast runtime access.

Predict-time responsibilities (`predict_with_context()`):
- Filters pre-scored events to the requested pair and signal month window.
- Keeps only events with valid surprise data (`has_surprise_data == 1`).
- Aggregates weighted surprise impact into one bounded score in [-1, 1].
- Returns top-K events by contribution magnitude as `TopCalendarEvent` objects.

Output contribution:
- `macro_surprise_score` overlay (replaces Node 1 neutral value).
- `top_calendar_events` — Layer 2 explainability (top 5 surprise drivers).

**Macro Agent Orchestrator (Node Fusion)**
Coordinates both nodes while keeping Node 1 as mandatory baseline and Node 2 as optional augmentation.

Fusion behavior:
- Always gets baseline macro signal from Node 1.
- If Node 2 is configured, calls `predict_with_context()` and replaces `macro_surprise_score` and `top_calendar_events`.
- Computes `dominant_driver` — the sub-score with the highest absolute value among `carry_signal_score`, `regime_context_score`, `fundamental_mispricing_score`, `macro_surprise_score`.
- If Node 2 fails at runtime, logs warning and safely returns Node 1 signal.

---

### Downstream

**Coordinator**
Consumes the final macro signal contract and combines it with technical and sentiment outputs for cross-agent decisioning.

---

### Flow

```
Macro Signals Store (macro_signal.parquet)
Economic Events Store (events CSV)
Market Context Stores (OHLCV parquets, VIX CSV)
Model Artifacts Store (lgb_models.joblib, catboost_models.joblib)
       ↓
[ MACRO AGENT ]
Node 1: month-end lookup → baseline direction + confidence + sub-scores
       ↓
Node 2 (optional): predict_with_context()
  ├── filter to pair + month window
  ├── weighted surprise aggregation → macro_surprise_score in [-1, 1]
  └── top-5 events by |contribution| → top_calendar_events (Layer 2)
       ↓
Orchestrator fusion:
  ├── replace macro_surprise_score (if Node 2 active)
  ├── replace top_calendar_events (if Node 2 active)
  └── compute dominant_driver (Layer 1)
       ↓
MacroSignal → Coordinator
```

---

## Overview

The macro pipeline converts precomputed monthly macro intelligence and event-driven surprise information into one production macro signal. The design is intentionally two-node:

1. Node 1 provides stable baseline macro direction and confidence.
2. Node 2 adds a dynamic event surprise overlay for the same monthly decision horizon.

This separation keeps the baseline deterministic while allowing event sensitivity to evolve independently.

---

## Stage 1 — Baseline Macro Ingestion

Node 1 loads `macro_signal.parquet` and indexes it by `(pair, timestamp_utc)`.

Temporal alignment rule:
- Signal timestamped 2025-01-31 was generated at month-end and is valid from Feb 1 onwards.
- Request for 2025-02-15 → look up latest signal with `timestamp_utc ≤ 2025-01-31`.
- Pair normalization: "EURUSDm" → "EURUSD" (strips trailing `m`).

Guarantees:
- Consistent monthly baseline per supported pair.
- Deterministic temporal alignment.
- No retraining or feature recomputation inside the serving path.

---

## Stage 2 — Event Universe Expansion

Node 2 reads the events universe and expands records across covered pairs using `PAIR_COUNTRY_SIGN`.

```
PAIR_COUNTRY_SIGN = {
    "EURUSD": {"US": -1},
    "GBPUSD": {"GB": +1, "US": -1},
    "USDCHF": {"US": +1, "CH": -1},
    "USDJPY": {"US": +1, "JP": -1},
}
```

`country_sign` encodes the directional interpretation: a US beat is bearish for EURUSD (USD strengthens → EUR/USD falls → sign −1).

Impact weights applied:

| Impact Level  | Weight |
|---------------|--------|
| high          | 1.0    |
| medium        | 0.5    |
| low           | 0.25   |
| non-economic  | 0.0 (excluded) |

---

## Stage 3 — Context Feature Construction

Node 2 enriches each event with market context around the event anchor window.

Feature families:
- Returns: 1-day and 3-day lookback returns.
- Rolling volatility: 3-day, 7-day, 20-day standard deviation.
- ATR features: ATR(5), ATR(20), ATR ratio (regime proxy).
- Bollinger Band width: 5-day, 20-day, squeeze ratio.
- Surprise primitives: `surprise_raw` (actual − forecast), `surprise_direction` (±1), beat/miss indicators.
- VIX features: 20-day rank, 5-day mean, spike flag, regime label (calm/normal/stress).
- 15 interaction features: `surprise × vol_high`, `surprise × vix_rank`, etc.

All features are shift(1) aligned so no event-day data leaks into the feature window.

Result:
- A model-ready event feature table built once per load cycle, stored in `_event_scores`.

---

## Stage 4 — Batch Event Scoring

Node 2 executes model inference in batch per pair.

Model routing:

| Pair    | Model      | Artifact             |
|---------|------------|----------------------|
| EURUSD  | LightGBM   | `lgb_models.joblib`  |
| USDCHF  | LightGBM   | `lgb_models.joblib`  |
| GBPUSD  | CatBoost   | `catboost_models.joblib` |
| USDJPY  | CatBoost   | `catboost_models.joblib` |

Output per event: `prob` — probability of directional price impact.

Operational behavior:
- Pair-level scoring is isolated so one pair failure does not block others.
- CatBoost import failure downgrades GBPUSD/USDJPY to `prob = 0.0` (neutral).

---

## Stage 5 — Monthly Surprise Aggregation

At prediction time, `predict_with_context()` converts event scores into one monthly surprise scalar plus top surprise drivers.

Aggregation formula:

```
score_i      = prob_i × surprise_direction_i × country_sign_i × impact_weight_i
total_weight = Σ (prob_i × impact_weight_i)
result       = Σ score_i / total_weight     [clipped to −1, 1]
```

Interpretation:
- Positive → net event support for base-currency strength.
- Negative → net support for quote-currency strength (or base weakness).
- Near-zero → mixed or weak event evidence.

Top-K events:

```
contribution_i = score_i / total_weight
top_events     = top 5 events by |contribution_i|
```

Each `TopCalendarEvent` carries: event name, country, surprise direction, surprise magnitude (|actual − forecast|), impact weight, model probability, and normalized contribution.

---

## Explainability

Two layers are computed at prediction time with no additional model calls.

**Layer 1 — Dominant Driver** (`dominant_driver: str`)

Identifies which of the four component sub-scores has the highest absolute value, giving the LLM narrator a single sentence anchor for the macro call.

| Value                          | Meaning                                      |
|--------------------------------|----------------------------------------------|
| `"carry_signal_score"`         | Interest rate differential is primary driver |
| `"regime_context_score"`       | Macro regime context dominates               |
| `"fundamental_mispricing_score"` | Valuation gap is primary driver            |
| `"macro_surprise_score"`       | Economic surprise events dominate (Node 2)   |

Selection: `argmax(|carry|, |regime|, |fundamental_mispricing|, |macro_surprise|)`. `macro_bias_score` is the aggregate output, not a component, and is excluded from this selection.

**Layer 2 — Top Calendar Events** (`top_calendar_events: list[TopCalendarEvent] | None`)

The top 5 economic events by absolute normalized contribution to `macro_surprise_score`. Only populated when Node 2 is active.

| Field                | Type  | Description                                         |
|----------------------|-------|-----------------------------------------------------|
| `event_name`         | str   | e.g. "Nonfarm Payrolls", "CPI"                      |
| `country`            | str   | ISO country code (US, GB, JP, CH, EU)               |
| `surprise_direction` | float | +1 = beat, −1 = miss                                |
| `surprise_magnitude` | float | \|actual − forecast\| (raw units)                  |
| `impact_weight`      | float | 1.0 / 0.5 / 0.25 per impact level                  |
| `prob`               | float | Model probability of directional price impact       |
| `contribution`       | float | `score_i / total_weight`; top events sum to `macro_surprise_score` |

---

## Final Output — Macro Signal Contract

| Field                         | Type                          | Description                                    |
|-------------------------------|-------------------------------|------------------------------------------------|
| `pair`                        | str                           | e.g. "EURUSDm"                                 |
| `signal_timestamp`            | Timestamp                     | Month-end from parquet (e.g. 2025-01-31)       |
| `module_c_direction`          | str                           | "up" or "down"                                 |
| `macro_confidence`            | float                         | 0–1 confidence in the directional call         |
| `carry_signal_score`          | float                         | Interest rate differential component           |
| `regime_context_score`        | float                         | Macro regime context component                 |
| `fundamental_mispricing_score`| float                         | Valuation gap component                        |
| `macro_surprise_score`        | float                         | Event surprise overlay (0.0 if Node 2 inactive)|
| `macro_bias_score`            | float                         | Composite aggregate score                      |
| `node_version`                | str                           | "macro_economics_v1"                           |
| `dominant_driver`             | str                           | Layer 1 — sub-score with highest \|value\|     |
| `top_calendar_events`         | list[TopCalendarEvent] \| None | Layer 2 — top 5 surprise event drivers         |

---

## Node Responsibilities Summary

**Node 1 (Macroeconomics):**
- Baseline monthly macro lookup.
- Temporal alignment (request date → valid signal month).
- Baseline signal integrity.

**Node 2 (Calendar Events):**
- Event feature build and batch scoring at load time.
- Monthly surprise aggregation at predict time.
- Top-K event extraction for Layer 2 explainability.
- Optional overlay — graceful degradation if unavailable.

**Orchestrator:**
- Node lifecycle management.
- Safe fusion and fault containment.
- `dominant_driver` computation (Layer 1).
- Single macro signal interface to downstream consumers.

---

## Pipeline Summary

```
data/processed/macro/macro_signal.parquet
data/processed/events/events_2021-01-01_2025-12-31.csv
data/processed/ohlcv/ohlcv_{PAIR}_D1_*.parquet
data/processed/macro/macro_VIXCLS_2021-01-01_2026-02-21.csv
models/production/macro/lgb_models.joblib
models/production/macro/catboost_models.joblib
       ↓
Node 1: MacroeconomicsNode.predict(pair, date)
  └── month-end temporal lookup → 9 baseline fields
       ↓
Node 2 (optional): CalendarEventsNode.predict_with_context(pair, signal_month_end)
  └── weighted surprise aggregation + top-5 events
       ↓
MacroAgent orchestrator:
  ├── replace macro_surprise_score + top_calendar_events (Node 2)
  └── compute dominant_driver (argmax of |sub-scores|)   ← Layer 1
       ↓
MacroSignal → Coordinator
  direction | confidence | sub-scores
  dominant_driver (Layer 1) | top_calendar_events (Layer 2)
```
