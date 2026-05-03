# Sentiment Node Pipeline — Reddit Images to Signal

---

## Visualization View (Production)

### Outside the Node

**Image Post Stream**
Image-bearing Reddit posts arrive with media links and post metadata.

**Media Processing Layer**
Image extraction, OCR, visual embeddings, and media-quality checks are completed upstream.

**Image Features Storage**
Processed image-level features are persisted as canonical input for this node.

---

### Inside the Images Node

**Media Quality Filter**
Removes unusable images (corrupt, too small, low-information overlays).

**Visual Signal Extractor**
Consumes precomputed visual attributes such as chart likelihood, color regimes, textual cues, and sentiment proxies.

**Context Joiner**
Combines image features with post metadata (time, subreddit context, engagement).

**Temporal Aggregator**
Builds daily and weekly image sentiment aggregates with recency weighting.

**Image Signal Calibrator**
Converts aggregate image signals into bounded node scores and reliability tags.

**Node Output Formatter**
Publishes image-node output for sentiment fusion.

---

### Downstream

**Sentiment Agent Coordinator**
Combines image-node output with labeled-data and audio-node outputs.

---

### Flow

Image Post Stream → Media Processing Layer → Image Features Storage
→ Media Quality Filter → Visual Signal Extractor → Context Joiner
→ Temporal Aggregator → Image Signal Calibrator → Node Output Formatter
→ Sentiment Agent Coordinator

---

## Overview

This node captures non-text sentiment from Reddit media by turning chart visuals, OCR hints, and image composition into stable temporal signals. It is designed as an auxiliary node with explicit reliability controls for sparse media periods.

---

## Stage 1 — Image Feature Ingestion

**What happens:**
- Load precomputed image feature records.
- Normalize timestamps to UTC daily buckets.
- Validate required visual and metadata fields.

**Output:**
Clean image-feature table with one row per usable media post.

---

## Stage 2 — Quality and Eligibility Checks

**What happens:**
- Exclude low-quality or non-informative images.
- Require minimum OCR/visual confidence for signal eligibility.
- Track exclusion reasons for monitoring.

**Output:**
Signal-eligible media subset.

---

## Stage 3 — Visual Sentiment Feature Composition

**What happens:**
- Build chart-intent and visual-tone indicators.
- Build OCR-derived directional hints.
- Build color/risk proxies and media-type composition features.

**Output:**
Image sentiment primitives per post.

---

## Stage 4 — Temporal Signal Construction

**What happens:**
- Aggregate to daily and weekly features.
- Apply recency weighting and rolling smoothers.
- Compute media coverage and confidence indicators.

**Output:**
Time-indexed image signal features.

---

## Stage 5 — Score Calibration and Bounding

**What happens:**
- Convert aggregate image features into bounded score space.
- Apply confidence-aware dampening under low coverage.
- Emit reliability flags and fallback markers.

**Output:**
Final image-node score and confidence package.

---

## Final Output Contract

| Field Group | Description |
|---|---|
| Time | Signal date (UTC) |
| Coverage | Valid image count, coverage ratio |
| Image Core | Visual sentiment index, chart-intent share, OCR directional proxy |
| Confidence | Reliability score derived from coverage and quality |
| Bounded Score | Final node score within defined range |
| Health | Sparse-coverage fallback indicators |

---

## Operational Notes

- This node is complementary, not a standalone trading signal.
- Sparse image periods are expected; low-confidence handling is mandatory.
- All expensive media processing remains outside inference path.
