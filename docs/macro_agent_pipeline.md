# Macro Agent Pipeline — Monthly Macro Signal to Coordinator

---

## Visualization View (Production)

### Outside the Agent

**Macro Signals Store**
Precomputed monthly macro directional outputs are produced upstream and persisted as the official source for macro baseline direction and confidence.

**Economic Events Store**
Structured calendar events include timestamps, countries, event metadata, and surprise-related fields used for event impact scoring.

**Market Context Stores**
Daily market context inputs (price-derived volatility features and risk context proxies such as implied volatility index levels) are prepared outside the agent and consumed at load time.

**Model Artifacts Store**
Trained event-scoring models and their feature metadata are persisted and loaded for batch event probability scoring.

---

### Inside the Macro Agent

**Node 1: Macroeconomics Node (Baseline Macro Direction)**
Loads monthly macro outputs once and serves temporal lookups for requests.

Key role:
- Returns the baseline macro direction package for a pair at inference time.
- Uses month-aware lookup logic so a request date maps to the most recent valid monthly signal.

Output contribution:
- Baseline direction and confidence block for the macro signal contract.
- Baseline macro surprise remains neutral unless Node 2 is active.

**Node 2: Calendar Events Node (Event Surprise Overlay)**
Performs one expensive load-time pass to pre-score events using trained models, then serves lightweight monthly surprise aggregation at inference.

Load-time responsibilities:
- Reads model artifacts and feature schemas.
- Expands events per covered pair using pair-specific country sign rules.
- Builds event-level features by joining event timestamps with market context.
- Computes interaction features and batch-scores event probabilities.
- Stores a compact pre-scored event table for fast runtime access.

Predict-time responsibilities:
- Filters pre-scored events to the requested pair and signal month window.
- Keeps only events with valid surprise data.
- Aggregates weighted surprise impact into one bounded score in [-1, 1].

Output contribution:
- Provides the macro surprise overlay.

**Macro Agent Orchestrator (Node Fusion)**
Coordinates both nodes while keeping Node 1 as mandatory baseline and Node 2 as optional augmentation.

Fusion behavior:
- Always gets baseline macro signal from Node 1.
- If Node 2 is configured and available, replaces only the macro surprise field with Node 2 aggregate.
- Leaves all other baseline fields unchanged.
- If Node 2 fails at runtime, logs warning and safely returns Node 1 signal.

---

### Downstream

**Coordinator**
Consumes the final macro signal contract and combines it with technical and sentiment outputs for cross-agent decisioning.

---

### Flow

Macro Signals Store + Economic Events Store + Market Context Stores + Model Artifacts Store
→ [Macro Agent]
→ Node 1 baseline lookup
→ Node 2 monthly surprise aggregation (optional)
→ fused macro signal
→ Coordinator

---

## Overview

The macro pipeline converts precomputed monthly macro intelligence and event-driven surprise information into one production macro signal. The design is intentionally two-node:

1. Node 1 provides stable baseline macro direction and confidence.
2. Node 2 adds a dynamic event surprise overlay for the same monthly decision horizon.

This separation keeps the baseline deterministic while allowing event sensitivity to evolve independently.

---

## Stage 1 — Baseline Macro Ingestion

Node 1 loads monthly macro outputs and indexes them for fast pair-and-time lookup.

What this stage guarantees:
- Consistent monthly baseline per supported pair.
- Deterministic temporal alignment from request date to valid signal month.
- No retraining or feature recomputation inside the serving path.

---

## Stage 2 — Event Universe Expansion

Node 2 reads the events universe and expands records across covered pairs using pair-specific country routing and sign conventions.

What this stage does:
- Applies impact filtering and impact weighting.
- Builds pair-specific event slices from the same global event stream.
- Preserves directional interpretation using country-sign mapping.

---

## Stage 3 — Context Feature Construction

Node 2 enriches each event with market context around the event anchor window.

Feature families:
- Short-horizon returns and rolling volatility.
- Range and regime-style proxies.
- Surprise primitives (direction, magnitude, indicators).
- Cross-feature interactions between surprise and regime/risk context.

Result:
- A model-ready event feature table built once per load cycle.

---

## Stage 4 — Batch Event Scoring

Node 2 executes model inference in batch per covered pair using artifact-specific metadata.

What this stage outputs:
- Event-level probability of directional impact.
- Compact scored-event store for runtime aggregation.

Operational behavior:
- Pair-level scoring is isolated so one pair failure does not block all others.
- Missing or invalid scoring paths degrade safely to neutral contribution.

---

## Stage 5 — Monthly Surprise Aggregation

At prediction time, Node 2 converts event-level scores into one monthly surprise scalar.

Aggregation logic:
- Restrict to pair + month window.
- Keep only events with valid surprise information.
- Compute signed weighted contribution per event.
- Normalize by total effective weight.
- Clip final value to [-1, 1].

Interpretation:
- Positive values indicate net event support for base-currency strength under that pair mapping.
- Negative values indicate net support for quote-currency strength (or base weakness).
- Near-zero values indicate mixed or weak event evidence.

---

## Final Output — Macro Signal Contract

The final macro signal delivered by the orchestrator contains:
- Pair identity and signal month timestamp.
- Baseline direction and confidence block from Node 1.
- Carry/regime/fundamental macro context from Node 1.
- Surprise overlay from Node 2 when active, otherwise baseline neutral value.
- Version tag for serving lineage.

---

## Node Responsibilities Summary

**Node 1 (Macroeconomics):**
- Baseline monthly macro lookup.
- Temporal alignment.
- Baseline signal integrity.

**Node 2 (Calendar Events):**
- Event feature build and batch scoring.
- Monthly surprise aggregation.
- Optional overlay behavior.

**Orchestrator:**
- Node lifecycle management.
- Safe fusion and fault containment.
- Single macro signal interface to downstream consumers.

---

## Pipeline Summary

External stores provide baseline macro outputs, events, context inputs, and model artifacts.

Inside the macro agent:
1. Baseline monthly lookup (Node 1)
2. Optional monthly surprise overlay (Node 2)
3. Field-level fusion in the orchestrator

Final result:
- One macro signal per pair and decision timestamp, ready for coordinator-level multi-agent fusion.
