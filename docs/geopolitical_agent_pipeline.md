# Geopolitical Agent Pipeline — GDELT to Bilateral FX Risk Signal

---

## Visualization View (Production)

### Outside the Agent

**GDELT Event Universe**
Raw global event rows are the upstream source. These are transformed by a dedicated geopolitical feature pipeline, not directly consumed by the agent.

**Geopolitical Index Builder (Rule-Based)**
The upstream pipeline builds an explainable zone-level daily geopolitical risk panel with this transformation chain:

`raw GDELT event rows -> event normalization -> event clustering -> zone inference -> geopolitical event filtering -> severity scoring -> volume normalization -> lagged baseline normalization -> smoothing -> full date x zone panel`

**Artifact Package (Upstream Deliverables)**
The geopolitical pipeline exports agent-ready artifacts:
- artifacts/agent_ready_geo_risk_daily.csv
- artifacts/geo_risk_driver_contributions.csv
- artifacts/geo_risk_run_metadata.json
- artifacts/fx_pair_zone_mapping.csv
- artifacts/fx_pair_zone_mapping.json

**Processed Storage (Canonical Agent Input)**
The production artifact is stored in this repository at data/processed/geopolitical/geo_risk_daily.csv and used as the single source for geopolitical inference.

---

### Inside the Geopolitical Agent

**Artifact Loader**
Loads geo_risk_daily.csv, parses UTC timestamps, parses top_drivers JSON strings, and builds a MultiIndex on (date, zone) for constant-time lookups.

**Pair Parser and Zone Mapper**
Parses FX symbols into base and quote currencies (supports MT5 trailing m suffix) and maps currencies to covered zones:
- EUR -> EUR
- USD -> USD
- GBP -> GBP
- JPY -> JPY
- CHF -> CHF

**Bilateral Risk Engine**
For each pair and date:
- base_zone_risk = geopolitical risk index for base zone
- quote_zone_risk = geopolitical risk index for quote zone
- risk_diff = base_zone_risk - quote_zone_risk
- risk_level = max(base_zone_risk, quote_zone_risk)

This makes the signal bilateral and pair-specific, not a single global score.

**Regime and Trend Classifier**
- risk_regime = high if risk_level > 100.0, else low
- dominant zone = zone with higher risk_level contribution
- risk_trend computed on dominant zone using smoothed series:
  - rising if ewm_7 > ewm_30 + 2.0
  - falling if ewm_7 < ewm_30 - 2.0
  - stable otherwise

**Driver Extraction**
top_drivers are taken from the dominant zone row so the explanation follows whichever side is currently risk-dominant.

---

### Downstream

**Coordinator**
Consumes GeopoliticalSignal alongside Technical and other agent signals.
Typical usage:
- risk_diff for directional relative-risk context
- risk_regime for environment gating
- risk_trend and top_drivers for explainability and narrative context

---

### Flow

```
GDELT Raw Event Rows
       ↓
Geopolitical Rule-Based Builder
(normalize -> cluster -> zone infer -> filter -> score -> normalize -> smooth)
       ↓
agent_ready_geo_risk_daily.csv
       ↓
Repository Canonical Input
(data/processed/geopolitical/geo_risk_daily.csv)
       ↓
[ GEOPOLITICAL AGENT ]
load + parse + index(date, zone)
       ↓
pair parse + zone map
       ↓
bilateral risk computation
       ↓
regime + trend + top drivers
       ↓
GeopoliticalSignal
       ↓
Coordinator
```

---

## Overview

The geopolitical pipeline converts raw GDELT event activity into a daily zone-level geopolitical risk panel, then transforms that panel into pair-level bilateral risk signals for FX.

The agent itself is intentionally lightweight at runtime: it performs deterministic lookups and transformations on precomputed artifacts.

---

## Stage 1 — Upstream Geopolitical Risk Construction

**Type:** Explainable rule-based index (no black-box checkpoint)

**Why rule-based:**
- Directly aligned with project spec
- Fully explainable and debuggable
- Produces stable agent-ready outputs

**Coverage:** USD, EUR, GBP, JPY, CHF

**Output contract:** full daily date x zone panel to avoid downstream join gaps.

---

## Stage 2 — Artifact Handoff to Repository

The production artifact is copied into repository storage at:
- data/processed/geopolitical/geo_risk_daily.csv

This is the canonical input consumed by the GeopoliticalAgent.

Expected core columns include:
- date
- zone
- geo_risk_index
- top_drivers
- geo_risk_raw
- geo_risk_norm
- n_events
- n_unique_event_ids
- total_news_volume
- geo_risk_ewm_7
- geo_risk_ewm_14
- geo_risk_ewm_30
- geo_risk_roll_14
- geo_risk_roll_30

---

## Stage 3 — Runtime Load and Indexing

At first predict call (lazy load):
- parse date as UTC and floor to day
- parse top_drivers from JSON string to list
- set MultiIndex (date, zone)

This provides deterministic and fast retrieval for bilateral computations.

---

## Stage 4 — Pair-Level Inference

Input:
- pair (for example EURUSDm)
- date (UTC timestamp)

Steps:
1. Remove trailing m suffix if present.
2. Parse base and quote currencies.
3. Map currencies to zones.
4. Lookup zone rows on (date, zone).
5. Compute bilateral metrics and classification outputs.

Error guards:
- ValueError for uncovered currencies/zones
- KeyError for missing date-zone rows

---

## Final Output — GeopoliticalSignal

One signal per pair per day.

| Field | Type | Description |
|---|---|---|
| pair | str | FX pair symbol |
| timestamp_utc | Timestamp | UTC day used for lookup |
| base_ccy | str | Base currency |
| quote_ccy | str | Quote currency |
| base_zone | str | Base zone |
| quote_zone | str | Quote zone |
| base_zone_risk | float | Zone risk index (base) |
| quote_zone_risk | float | Zone risk index (quote) |
| risk_diff | float | base_zone_risk - quote_zone_risk |
| risk_level | float | max(base_zone_risk, quote_zone_risk) |
| risk_regime | str | high if risk_level > 100.0 else low |
| risk_trend | str | rising, falling, or stable |
| top_drivers | list[str] | Dominant-zone key drivers |

---

## Artifact-First Pattern (Notebook Reference)

The notebook pipeline pattern in this repository is artifact-first and agent-ready:
- strict input checks and mode gates
- explicit artifacts directory contract
- JSON, CSV, Parquet exports for downstream agents

That same production principle is used here: the agent consumes stable precomputed artifacts rather than rebuilding geopolitical indices inside runtime inference.

---

## Pipeline Summary

```
Upstream GDELT geopolitics pipeline
  -> agent_ready_geo_risk_daily.csv
  -> copied to data/processed/geopolitical/geo_risk_daily.csv
  -> GeopoliticalAgent.load()
  -> GeopoliticalAgent.predict(pair, date)
  -> GeopoliticalSignal (risk_diff, risk_level, risk_regime, risk_trend, top_drivers)
  -> Coordinator
```
