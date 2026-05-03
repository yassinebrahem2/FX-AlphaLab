# Sentiment Node Pipeline — Labeled Reddit Data to Signal

---

## Visualization View (Production)

### Outside the Node

**Reddit Post Stream**
Raw Reddit submissions from selected finance communities arrive continuously with text, metadata, and engagement fields.

**Labeling Service**
A labeling layer assigns structured sentiment tags to each post (content intent, directional strength, risk posture, clarity, and sarcasm/irony).

**Labeled Storage**
Labeled records are persisted as the canonical source for this node. This node does not relabel posts at inference time.

---

### Inside the Labeled Data Node

**Quality Gate**
Drops low-information records (noise and unclear stance) and keeps only posts with usable signal characteristics.

**Pair Attribution Router**
Assigns each record to a pair-specific bucket when explicit pair evidence exists; otherwise routes to a global sentiment bucket.

**Weighted Signal Builder**
Builds daily sentiment primitives using engagement-aware weighting and content-type weighting.

**Temporal Smoother**
Applies short-horizon decay and rolling windows to stabilize noisy social signals.

**Regime Predictor**
Uses the prepared feature set to estimate next-period sentiment-driven market regime probability.

**Node Output Formatter**
Publishes one daily node output with bounded scores and confidence for downstream fusion.

---

### Downstream

**Sentiment Agent Coordinator**
Receives this node output alongside image and audio nodes, then fuses into a unified sentiment signal.

---

### Flow

Reddit Post Stream → Labeling Service → Labeled Storage
→ Quality Gate → Pair Attribution Router → Weighted Signal Builder
→ Temporal Smoother → Regime Predictor → Node Output Formatter
→ Sentiment Agent Coordinator

---

## Overview

This node transforms labeled Reddit text data into a production-grade daily sentiment signal. The node is designed to be robust to sparse pair mentions and high social noise by prioritizing strict filtering, weighted aggregation, and temporally stable features.

---

## Stage 1 — Labeled Data Ingestion

**What happens:**
- Load labeled Reddit records from canonical storage.
- Standardize timestamps to UTC daily boundaries.
- Keep engagement and source metadata required for weighting and diagnostics.

**Output:**
A normalized daily-ready labeled table.

---

## Stage 2 — Signal Eligibility Filtering

**What happens:**
- Remove non-informative content classes.
- Require minimum stance/target clarity.
- Preserve a quality flag for each retained record.

**Output:**
A filtered set of signal-bearing records.

---

## Stage 3 — Pair Routing and Global Fallback

**What happens:**
- Assign record to a pair bucket when attribution is explicit.
- Route unattributed records to global bucket.
- Maintain per-bucket sample-size diagnostics.

**Output:**
Pair-level and global daily record groups.

---

## Stage 4 — Weighted Daily Feature Construction

**What happens:**
- Compute daily risk-on and risk-off intensity.
- Compute weighted directional strength.
- Compute participation confidence from signal-bearing share.
- Compute content-composition shares.

**Weighting policy:**
- Engagement-weighted contributions.
- Content-type multiplier (macro/news-heavy posts receive higher weight).

**Output:**
Daily base feature vector per bucket.

---

## Stage 5 — Temporal Stabilization

**What happens:**
- Apply exponential decay with short half-life to preserve recency.
- Add rolling-window features to reduce day-to-day variance.
- Preserve missing-data policy for low-activity days.

**Output:**
Smoothed and model-ready daily features.

---

## Stage 6 — Regime Probability Inference

**What happens:**
- Run trained classifier on daily feature vector.
- Produce calibrated probability for next-period high-volatility regime.
- Clip and validate outputs to contract bounds.

**Output:**
Daily probability plus confidence metadata.

---

## Final Output Contract

| Field Group | Description |
|---|---|
| Time | Signal date (UTC daily) |
| Scope | Pair bucket or global bucket |
| Sentiment Core | Risk-on score, risk-off score, weighted directional strength |
| Confidence | Signal-bearing post count and confidence ratio |
| Forecast | Calibrated regime probability |
| Health | Coverage and fallback flags |

---

## Operational Notes

- Default serving mode is global sentiment when pair attribution is sparse.
- Low-confidence days are emitted with explicit reliability flags rather than dropped.
- Node remains fully deterministic at inference (no relabeling, no online retraining).
