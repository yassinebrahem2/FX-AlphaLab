# Sentiment Node Pipeline — Reddit Audio to Signal

---

## Visualization View (Production)

### Outside the Node

**Audio Post Stream**
Audio-bearing Reddit content arrives with media metadata and source post context.

**Audio Processing Layer**
Speech-to-text, acoustic feature extraction, and transcript cleanup are completed upstream.

**Audio Features Storage**
Audio and transcript features are persisted and versioned for node consumption.

---

### Inside the Audio Node

**Audio Eligibility Filter**
Keeps only records with valid transcript/acoustic quality and sufficient duration.

**Transcript Sentiment Builder**
Builds text-based sentiment signals from transcript-level features.

**Acoustic Signal Builder**
Builds prosody and stress proxies from acoustic descriptors.

**Fusion and Temporal Smoothing**
Combines transcript and acoustic signals, then aggregates over time with recency weighting.

**Audio Score Calibrator**
Converts fused audio sentiment into bounded score with confidence and coverage flags.

**Node Output Formatter**
Publishes audio-node output for sentiment fusion.

---

### Downstream

**Sentiment Agent Coordinator**
Uses audio-node output as an auxiliary modality with confidence-aware weighting.

---

### Flow

Audio Post Stream → Audio Processing Layer → Audio Features Storage
→ Audio Eligibility Filter → Transcript Sentiment Builder + Acoustic Signal Builder
→ Fusion and Temporal Smoothing → Audio Score Calibrator → Node Output Formatter
→ Sentiment Agent Coordinator

---

## Overview

This node captures spoken sentiment cues from Reddit media. Because audio coverage is typically sparse and uneven, the pipeline prioritizes strict eligibility checks, conservative confidence modeling, and robust fallback behavior.

---

## Stage 1 — Audio Feature Ingestion

**What happens:**
- Load transcript and acoustic feature records.
- Normalize timestamps to UTC buckets.
- Validate schema consistency and source completeness.

**Output:**
Canonical audio-feature dataset.

---

## Stage 2 — Eligibility and Quality Controls

**What happens:**
- Remove entries with missing/low-quality transcripts.
- Remove entries with invalid acoustic vectors.
- Enforce minimum quality thresholds and track rejection reasons.

**Output:**
Signal-eligible audio subset.

---

## Stage 3 — Transcript-Derived Sentiment Features

**What happens:**
- Compute directional and intensity features from transcripts.
- Compute uncertainty and neutrality markers.
- Preserve transcript confidence for downstream weighting.

**Output:**
Per-record transcript sentiment primitives.

---

## Stage 4 — Acoustic-Derived Sentiment Features

**What happens:**
- Compute speaking-rate/prosody stress proxies.
- Compute short-window acoustic summary features.
- Normalize and validate acoustic ranges.

**Output:**
Per-record acoustic sentiment primitives.

---

## Stage 5 — Multimodal Audio Fusion

**What happens:**
- Fuse transcript and acoustic primitives with confidence-aware weights.
- Aggregate to stable temporal bucketed signals.
- Apply recency weighting and rolling smoothing.

**Output:**
Time-indexed fused audio sentiment features.

---

## Stage 6 — Score Calibration and Reliability

**What happens:**
- Map fused features to bounded audio-node score.
- Compute reliability from sample count and quality distribution.
- Emit fallback when coverage is too thin.

**Output:**
Final audio-node score package for coordinator fusion.

---

## Final Output Contract

| Field Group | Description |
|---|---|
| Time | Signal date (UTC) |
| Coverage | Valid audio count and quality ratio |
| Transcript Core | Directional and intensity sentiment features |
| Acoustic Core | Prosody and stress proxy features |
| Confidence | Reliability score for modality weight control |
| Bounded Score | Final node score within contract bounds |
| Health | Low-coverage fallback and diagnostics flags |

---

## Operational Notes

- Audio should be treated as a sparse auxiliary modality.
- Reliability controls are mandatory for production safety.
- Node is designed to degrade gracefully to neutral under low coverage.
