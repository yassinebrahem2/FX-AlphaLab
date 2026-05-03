# Geopolitical Agent Pipeline — GDELT Events to Bilateral FX Risk Signal

---

## Visualization View (Production)

### Outside the Agent

**GDELT 1.0 HTTP Feed**
Free daily event exports from `data.gdeltproject.org`. No credentials, no cost. Covers all global
geopolitical events — actor countries, event types, Goldstein severity scores, media tone, mention
counts.

**Bronze Storage**
Raw GDELT events written to disk exactly as downloaded, zone-actor filtered at collection time.
One parquet file per calendar day. Immutable.

**GDELTEventsPreprocessor**
Validates and cleans the raw event rows: snake_case column names, UTC timestamps, typed numeric
columns. Produces model-agnostic clean event records partitioned by year and month.

**Clean Silver Storage**
Monthly parquet files of cleaned event rows. The canonical event-level record — used by the agent
at inference time for Layer 2 explainability (top GDELT events driving a zone's risk).

**GDELTZoneFeatureBuilder**
Aggregates clean Silver events into one row per calendar day. Computes 5 numeric features per zone
(event volume, severity, conflict fraction, tone, salience) and 20 directed edge weights between
zones (actual GDELT event flows, not model-internal values).

**Zone Features Silver**
A single flat parquet file: one row per day, 46 columns. The agent's primary data source — used
both as the rolling normalization buffer and as the source for Layer 3 graph visualization data.

---

### Inside the Geopolitical Agent

**Delta Feature Computation**
Before normalization, a 5-day trend signal is derived: `Δgeo[t] = X[t] − mean(X[t−5 : t])`.
This captures *rising or falling* geopolitical stress, not just the absolute level. Both raw levels
and deltas are concatenated into a 10-dim input per zone before z-scoring.

**Rolling Z-Score Normalization**
Both the combined [levels, delta] node features and the targets are normalized using a strictly
backward-looking 30-day rolling window. The model never sees absolute event volumes — only relative
stress compared to the recent baseline. This makes the signal regime-agnostic across different
geopolitical eras.

**GAT Ensemble (15 seeds)**
A 5-node zone graph (USD, EUR, GBP, JPY, CHF) with dense attention. Two Graph Attention layers
(10→32→8) feed a shared linear head that outputs one risk score per zone. All 15 models run in
parallel at inference; their zone risk scores are averaged. Necessary because 614 training days
produce high seed-to-seed variance.

**Bilateral Aggregation**
Zone risk scores are combined per pair: `score[base_zone] + score[quote_zone]`. USD appears in
three of the four pairs — its risk score is shared and consistent across all of them. Adding new
pairs in future requires no retraining.

**Explainability Layers**
Three layers attached to every signal at zero extra model cost:
- **Layer 1** — which features are driving each zone's risk score (level feature z-scores + dominant driver)
- **Layer 2** — top GDELT events from the dominant zone on that day (ranked by salience)
- **Layer 3** — full 5-node graph state with directed edge weights for network visualization

---

### Downstream

**Coordinator**
Receives `GeopoliticalSignal` alongside signals from the Technical Agent, Macro Agent, and
Sentiment Agent. Uses `bilateral_risk_score` as a Tier A alpha input for all four pairs on the
**volatility track** (3-day forward vol prediction). Uses `risk_regime` for position sizing and
exposure scaling.

---

### Flow

```
GDELT 1.0 HTTP → Bronze (daily parquets)
                        ↓
            GDELTEventsPreprocessor
                        ↓
            Clean Silver (monthly parquets) ──────────────────→ Layer 2 (top events)
                        ↓
            GDELTZoneFeatureBuilder
                        ↓
            Zone Features Silver (single flat parquet) ────────→ Layer 3 (graph)
                        ↓
                [ GEOPOLITICAL AGENT ]
                Delta features: Δgeo = X[t] − mean(X[t−5:t])
                Concat [levels, delta] → 10-dim per zone
                Rolling 30-day z-score (combined)
                        ↓
                GAT Ensemble (15 seeds)
                5-node zone graph → zone risk scores
                        ↓
                Bilateral aggregation
                score[base] + score[quote]
                        ↓
                GeopoliticalSignal
        bilateral_risk_score | risk_regime | explainability
                        ↓
                    Coordinator (VOL track, Tier A)
```

---

## Overview

The geopolitical pipeline converts raw GDELT event activity into a daily bilateral FX volatility
signal. It runs through four stages: collection, two preprocessing stages (clean events and zone
feature aggregation), and GAT ensemble inference with three explainability layers attached.

The agent predicts **3-day forward realized FX volatility** (z-scored). The 3-day window is the
empirically confirmed horizon: multi-horizon sweep (3/5/10/20d) showed only 3d generalizes OOS.
Longer horizons fail because vol mean-reversion dominates past 3 days. Delta features (geo stress
trend) are the key predictive signal — raw levels alone test IC=0.07, levels+delta test IC=0.10.

---

## Stage 1 — Data Collection (Bronze Layer)

**Source:** GDELT 1.0 HTTP download — `http://data.gdeltproject.org/events/YYYYMMDD.export.CSV.zip`

**What happens:** The GDELTEventsCollector downloads the daily export, parses the 58-column
tab-separated file, and filters to events where Actor1 or Actor2 belongs to a covered currency
zone. Zone filtering is applied at collection time as a deliberate storage tradeoff — non-zone
events are never needed. All other filtering is deferred to the preprocessor.

**Output:** `data/raw/gdelt_events/{YYYY}/{MM}/{YYYYMMDD}.parquet`
- Raw GDELT CamelCase column names preserved
- Fields include: Actor1CountryCode, Actor2CountryCode, GoldsteinScale, QuadClass, AvgTone,
  NumMentions, SourceUrl (and others)

**Date range:** 2022-01-01 onward (Russia-Ukraine invasion is the training anchor — cannot be absent)

**Zone coverage:**

| Zone | Countries |
|------|-----------|
| USD | USA |
| EUR | EUR + 20 Eurozone member states (DEU, FRA, ITA, ESP, NLD, AUT, BEL, CYP, EST, FIN, GRC, HRV, IRL, LTU, LUX, LVA, MLT, PRT, SVK, SVN) |
| GBP | GBR |
| JPY | JPN |
| CHF | CHE |

Note: `"EUR"` covers ECB and EU institutional actors. `"EUZ"` is not used.

---

## Stage 2 — Clean Event Preprocessing (Silver Layer)

**What happens:** `GDELTEventsPreprocessor` reads each day's Bronze parquet and produces validated,
typed event records. Column names are converted to snake_case, timestamps are parsed to UTC,
numeric columns are cast to float64/Int64, and malformed rows are dropped.

**Output:** `data/processed/gdelt_events/year={YYYY}/month={MM}/gdelt_events_cleaned.parquet`
- One file per month, all days concatenated
- Key fields: `event_date` (UTC), `actor1_country_code`, `actor2_country_code`, `actor1_name`,
  `actor2_name`, `goldstein_scale`, `quad_class`, `avg_tone`, `num_mentions`, `source_url`

This layer is model-agnostic — it knows nothing about zones, features, or the GAT model. It is
consumed at inference time by the agent's Layer 2 explainability to retrieve the specific GDELT
events driving a zone's elevated risk on a given day.

---

## Stage 3 — Zone Feature Aggregation (Silver Layer)

**What happens:** `GDELTZoneFeatureBuilder` reads the clean Silver event rows and aggregates them
into one row per calendar day, producing the model-specific representation the GAT was trained on.

**Output:** `data/processed/geopolitical/zone_features_daily.parquet`
- Single flat file, one row per day, sorted by date
- 46 columns: `date` + 25 node features + 20 directed edge weights

**5 node features per zone** (computed from zone-filtered events on that day):

| Feature | Column pattern | Formula |
|---------|---------------|---------|
| Event volume | `{zone}_log_count` | `log1p(event_count)` |
| Severity | `{zone}_goldstein` | `mean(GoldsteinScale)` — negative = conflict |
| Conflict fraction | `{zone}_conflict_frac` | `fraction(QuadClass ∈ {3, 4})` |
| Media tone | `{zone}_avg_tone` | `mean(AvgTone)` |
| Salience | `{zone}_log_mentions` | `log1p(sum(NumMentions))` |

Zones with no events on a day get 0.0 for all five features. NaN values in source columns
are treated as 0.0 after aggregation.

**20 directed edge weights** (`edge_{zi}_{zj}` for all zone pairs where zi ≠ zj):
- Filter events where Actor1 ∈ zone_i AND Actor2 ∈ zone_j (both conditions)
- `edge_weight = log1p(count)` — raw event flow, not model-internal attention

These edge weights are used directly for Layer 3 network visualization. They represent actual
geopolitical event flows between zones, not GAT attention weights.

---

## Stage 4 — GAT Ensemble Inference

**Cold-start requirement:** minimum 10 days of zone features history for z-score normalization.
Full 35-day window (30 z-score + 5 delta) required for production-quality signals.

**Lag-1 alignment:** zone features from GDELT calendar day `t` are used to predict FX vol on
trading day `t+1`. At inference for target date T, the agent looks up `gdelt_date = T − 1 day`.

**Delta feature computation (strictly no lookahead):**
For each of the 5 node feature dimensions, compute a 5-day trend:
```
Δgeo[t] = X[t] − mean(X[t−5 : t])
```
History window excludes the target day. Zero-padded for the first 5 days in the buffer.
This removes the mean-reversion confound: raw levels correlate with current vol which then reverts;
deltas capture whether stress is actively rising or falling — the genuinely predictive component.

**Combined feature construction:**
Levels (5-dim) and deltas (5-dim) are concatenated per zone → 10-dim input per zone. The
combined [levels, delta] array is z-scored together:
```
z[t] = (combined[t] − mean(combined[t−30 : t])) / std(combined[t−30 : t])
```
History window excludes the target day itself.

**GAT architecture (must match saved weights exactly):**
- Layer 1: GATLayerV2 — **10-dim** → 32-dim (4 heads, concat)
- BatchNorm1d on 5 zone representations
- Layer 2: GATLayerV2 — 32-dim → 8-dim (4 heads, averaged)
- Shared head: Linear(8 → 1) per zone
- Output: 5 zone risk scalars — higher = more stress relative to 30-day baseline
- Total parameters: ~1,800

**Static dense adjacency:** all nodes attend to all others including self-loops. Dynamic
z-scored adjacency was tested and rejected — too noisy for rarely-active zone pairs.

**Ensemble:** 15 random-seed models averaged at inference. Reduces seed-to-seed IC variance.

**Bilateral aggregation:**
```
bilateral_risk_score = zone_risk[base_zone] + zone_risk[quote_zone]
risk_regime = "high" if bilateral_risk_score > 1.0 else "low"
```

**Production artifacts:**
- `models/production/gdelt_gat_final_states.pt` — 15 state dicts
- `models/production/gdelt_gat_config.json` — hyperparameters: `in_features=10`, `fwd_vol_days=3`, `delta_win=5`

---

## Stage 5 — Explainability

Three layers are computed at inference time with no additional model calls.

**Layer 1 — Feature Attribution** (`ZoneFeatureExplanation`, one per zone side of the pair)

The 5 z-scored *level* features (first 5 dims of the 10-dim input) for a zone are directly
interpretable. The dominant driver is whichever level feature has the highest absolute z-score.
Tells the LLM narrator whether elevated risk is driven by conflict escalation, tone deterioration,
media attention surge, etc. Delta features are not surfaced here — they are the model's internal
predictive enhancement, not the human-readable narrative.

**Layer 2 — Top GDELT Events** (`list[TopEvent]`, from dominant zone)

The dominant zone (higher `|risk_score|` between base and quote) is used to look up the top 5
events from the clean Silver for `gdelt_date`. Events are ranked by `num_mentions × |goldstein_scale|`.
Returns actor names, event tone, Goldstein severity, quad class, and source URLs. Empty list if
clean Silver is unavailable for that date.

**Layer 3 — Graph State** (`ZoneGraphData`)

All 5 zone risk scores and all 20 directed edge weights (from zone features Silver, not attention
weights) packaged for network visualization. Edge weight keys formatted as `"ZI→ZJ"` (uppercase
with arrow). Intended for rendering as a directed graph where node size = risk score and edge
thickness = event flow volume, with conflict vs cooperative events in different colours.

---

## Final Output — `GeopoliticalSignal`

One signal per pair per day, passed to the Coordinator.

| Field | Type | Description |
|-------|------|-------------|
| `pair` | str | e.g. "EURUSD" (trailing m stripped) |
| `timestamp_utc` | Timestamp | Target FX trading date (UTC) |
| `base_ccy` | str | Base currency |
| `quote_ccy` | str | Quote currency |
| `base_zone` | str | Zone for base currency |
| `quote_zone` | str | Zone for quote currency |
| `bilateral_risk_score` | float | GAT output: zone_risk[base] + zone_risk[quote] |
| `risk_regime` | str | "high" if bilateral_risk_score > 1.0 else "low" |
| `base_zone_explanation` | ZoneFeatureExplanation | Layer 1 — level feature z-scores + dominant driver |
| `quote_zone_explanation` | ZoneFeatureExplanation | Layer 1 — level feature z-scores + dominant driver |
| `top_events` | list[TopEvent] | Layer 2 — top GDELT events from dominant zone |
| `graph` | ZoneGraphData | Layer 3 — all 5 zone scores + 20 directed edge weights |

---

## Empirical Performance (NB19, Test Set: 2025, unseen)

**Target: fwd_vol_3d** = std(log_ret[t+1 : t+4]) — strictly forward-looking 3 trading days.

| Pair | Val IC (2024) | Test IC (2025) |
|------|--------------|----------------|
| EURUSD | 0.1715 | **0.1086** |
| GBPUSD | 0.2274 | **0.0918** |
| USDJPY | 0.1437 | **0.0859** |
| USDCHF | 0.1541 | **0.1109** |
| **Mean** | **0.1742** | **0.0993** |

Quarterly stability: **8/8 quarters positive** (val + test 2024–2025).
Stopping rule threshold: 0.05 → **passed with margin**.

Metric: Spearman IC between predicted bilateral risk score and 3-day forward FX volatility
(z-scored, window=30). Temporal splits: Train 2022–2023 (614 days), Val 2024 (314 days),
Test 2025 (313 days).

**Coordinator tier: A** — all four pairs positive on both val and test sets.

---

## Multi-Horizon Sweep (NB19)

Only the 3-day horizon generalizes OOS. All other horizons fail:

| Horizon | Val IC | Test IC | Decision |
|---------|--------|---------|----------|
| fwd_3d | 0.178 | **0.099** | ✓ Production |
| fwd_5d | 0.027 | -0.022 | ✗ Rejected |
| fwd_10d | 0.062 | 0.010 | ✗ Rejected |
| fwd_20d | -0.023 | -0.099 | ✗ Rejected |

Explanation: geopolitical events integrate into FX vol over ~3 trading days. Beyond that, vol
mean-reversion dominates the signal.

---

## Delta Feature Impact

Adding Δgeo = X[t] − mean(X[t−5:t]) per feature per zone alongside raw levels:

| Features | Test IC | Quarterly positive |
|----------|---------|-------------------|
| Levels only | 0.070 | 6/8 |
| Levels + Δgeo | **0.099** | **8/8** |

The delta signal addresses the mean-reversion confound: raw geo levels correlate with current vol
(which then mean-reverts → negative fwd IC). The trend of stress is the genuinely predictive
component, not the absolute level.

---

## Key Architectural Decisions

| Decision | Outcome |
|----------|---------|
| **3-day fwd vol target** | Only horizon that generalizes OOS. 5/10/20d all fail. |
| **Delta features (5-day trend)** | +43% test IC improvement (0.070 → 0.099). 8/8 quarters positive. |
| **10-dim input (levels + delta)** | Combined z-scored together, matching training. |
| Rolling 30-day z-score | Fast regime adaptation beats longer windows. win=30 > 60 > 90 > 120. |
| EUR-full zone (21 countries) | Beats EUR-core (top-5 GDP). More events = less noise. |
| 15-seed ensemble | Reduces initialization variance. Necessary given small training set. |
| Static dense adjacency | Dynamic z-scored A_edge destabilizes training. Rejected. |
| Trend features (3d momentum, NB18) | Overfit with levels only. Superseded by delta features. |

---

## Pipeline Summary

```
GDELT 1.0 HTTP download (free, no credentials)
       ↓
GDELTEventsCollector → Bronze  (data/raw/gdelt_events/{YYYY}/{MM}/{YYYYMMDD}.parquet)
       ↓
GDELTEventsPreprocessor → Clean Silver  (data/processed/gdelt_events/year={Y}/month={M}/)
       ↓
GDELTZoneFeatureBuilder → Zone Features Silver  (data/processed/geopolitical/zone_features_daily.parquet)
       ↓
GeopoliticalAgent
  └── delta: Δgeo[t] = X[t] − mean(X[t−5:t])  (5-day geo stress trend)
  └── concat [levels, delta] → 10-dim per zone
  └── rolling 30-day z-score on combined (no lookahead)
  └── GAT ensemble × 15 seeds → 5 zone risk scores
  └── bilateral: score[base] + score[quote]
  └── Layer 1: level feature z-scores (free)
  └── Layer 2: top events ← Clean Silver
  └── Layer 3: graph state ← Zone Features Silver
       ↓
GeopoliticalSignal → Coordinator (VOL track, Tier A, all 4 pairs)
```
