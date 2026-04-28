# StockTwits Sentiment Node Pipeline — Social Trading Posts to Directional Signal

---

## Visualization View (Production)

### Outside the Node

**StockTwits API**
Public stream endpoint per FX symbol. Delivers social trading posts with native platform bullish/bearish labels on a subset of messages.

**Bronze Storage**
One JSONL file per symbol per collection run. Posts written exactly as received. Immutable.

**StocktwitsPreprocessor**
Transforms Bronze posts into a Silver inference checkpoint. Replicates training text normalization exactly, runs batched FinTwitBERT-sentiment inference, and writes `prob_bullish`, `prob_bearish`, and `predicted_label` per post. Incremental — skips posts already in the checkpoint.

**Silver Checkpoint**
Canonical input for the signal node. Immutable record of all inference outputs. Never rebuilt from scratch unless the model is retrained.

---

### Inside the Node

**Checkpoint Reader**
Loads the Silver checkpoint line by line. Malformed JSON lines are skipped with a warning.

**Date Parser**
Converts `timestamp_published` ISO strings to UTC-normalized dates. Rows with unparseable timestamps are dropped.

**Window Filter**
Restricts posts to `[start_date − 14 days, end_date]`. Posts older than the 14-day decay window have negligible weight.

**Decay Engine**
Each post on day `t` is projected forward across offsets 0 → 13, weighted by `exp(−λ × offset)` where `λ = ln(2) / 5`. A post 5 days old carries half the weight of a post from today.

**Daily Aggregator**
Groups decay-expanded posts by date (global) and by date × symbol (pair-level). Computes weighted means via safe division — zero-weight days return 0.0.

---

### Downstream

**SentimentAgent**
Receives global and pair-level DataFrames. Uses `net_sentiment` per pair as the directional component and `mean_model_confidence` to scale conviction.

---

### Flow

```
StockTwits API (EURUSD / GBPUSD / USDCHF / USDJPY streams)
       ↓
StocktwitsCollector → Bronze JSONL  (data/raw/news/stocktwits/{symbol}_{date}.jsonl)
       ↓
StocktwitsPreprocessor
  normalize_text() + preprocess_for_model()
  FinTwitBERT-sentiment inference (batched, threshold=0.58)
  → Silver checkpoint  (data/processed/stocktwits/labels_checkpoint.jsonl)
       ↓
      [ STOCKTWITS SIGNAL NODE ]
      Checkpoint Reader + Date Parser
                 ↓
         Window Filter (14-day lookback)
                 ↓
      Decay Engine  λ = ln(2)/5, half-life 5 days
                 ↓
      Daily Aggregator (global + pair-level)
      safe division → bullish_score | bearish_score
                 ↓
  net_sentiment | signal_post_count | mean_model_confidence
                 ↓
         SentimentAgent
```

---

## Overview

The StockTwits pipeline transforms raw FX social trading posts into daily per-pair directional sentiment signals. It runs in two decoupled stages: a preprocessing stage that runs batched transformer inference and writes a Silver checkpoint, and a signal computation stage that applies exponential decay and aggregates the model's continuous bullish/bearish probabilities into daily weighted scores at both global and per-pair level.

---

## Stage 1 — Data Collection (Bronze Layer)

**Source:** StockTwits public stream API, one request per FX symbol

**What happens:** `StocktwitsCollector` queries the stream endpoint per symbol, paginates using cursor-based logic, deduplicates within each run, and writes raw posts to disk exactly as received.

**Output:** JSONL files at `data/raw/news/stocktwits/`
- Naming: `{symbol}_{YYYYMMDD}.jsonl`

**Bronze record key fields:**

| Field | Type | Description |
|---|---|---|
| `message_id` | str | Unique post ID |
| `symbol` | str | FX pair (set by collector from the request) |
| `body` | str | Post text |
| `sentiment` | str | Native platform label (optional, not used in inference) |
| `timestamp_published` | str | ISO 8601 UTC |
| `followers_count` | int | Author follower count |
| `username` | str | |
| `user_id` | str | |

**Pairs covered:** EURUSD, GBPUSD, USDCHF, USDJPY

The `symbol` field is set by the collector from the request — no text-based pair inference is needed at this stage.

---

## Stage 2 — Preprocessing and Inference (Silver Layer)

**Class:** `StocktwitsPreprocessor` at `src/ingestion/preprocessors/stocktwits_preprocessor.py`

**What happens:**
1. Globs all `*.jsonl` files in the Bronze directory, loads all posts, deduplicates by `message_id` across files (first occurrence wins).
2. Compares against the Silver checkpoint — posts already labeled are skipped.
3. Posts whose body is empty after text normalization are skipped.
4. Runs batched FinTwitBERT-sentiment inference on remaining posts.
5. Appends results to the Silver checkpoint, flushing after each batch.

**Text preprocessing (must match training exactly):**

```
normalize_text(body):
  - None / NaN → ""
  - \n, \r → space
  - URLs → " URL "
  - $EURUSD (any 6-letter cashtag) → " EURUSD " (uppercase, $ stripped)
  - normalize whitespace

preprocess_for_model(text):
  - normalize_text(text)
  - .lower()
  - keep only [a-z0-9$\s]
  - normalize whitespace + strip
```

**Model:** FinTwitBERT-sentiment (BERT-based, finance + social media pretrained, sentiment-head initialized)
- Fine-tuned weights: `models/sentiment/stocktwits/model.safetensors` + `config.json`
- Tokenizer: `StephanAkkerman/FinTwitBERT-sentiment` (HuggingFace)
- Max length: 128 tokens
- Inference: batched (`batch_size=32`), `torch.no_grad()`, softmax on logits
- Device: CUDA if available, else CPU

**Decision threshold:** `prob_bullish ≥ 0.58` → `bullish`, else `bearish`
Selected on the validation set to maximize balanced accuracy. Closes the default argmax recall gap between bearish (0.735 → 0.837) and bullish (0.876 → 0.865).

**Output:** `data/processed/stocktwits/labels_checkpoint.jsonl`

| Field | Type | Description |
|---|---|---|
| `message_id` | str | Unique post ID |
| `symbol` | str | FX pair |
| `timestamp_published` | str | ISO 8601 UTC |
| `prob_bullish` | float | Model probability for bullish class |
| `prob_bearish` | float | Model probability for bearish class |
| `predicted_label` | str | "bullish" or "bearish" at threshold 0.58 |
| `model` | str | "FinTwitBERT-sentiment" |

---

## Model Background

FinTwitBERT-sentiment was fine-tuned on a frozen chronological benchmark of 915 native-labeled StockTwits posts (640 train / 137 val / 138 test) covering the same 4 FX pairs as the collector.

**Training progression:** Classical baselines first (TF-IDF + LR: 0.751, TF-IDF + SVM: 0.729), then three transformer families evaluated (FinBERT, FinTwitBERT, FinTwitBERT-sentiment). Anti-overfitting study on the winner: epoch/LR/weight-decay sweeps + dropout experiment (dropout=0.2 rejected).

**Final configuration:**

| Setting | Value |
|---|---|
| Model family | FinTwitBERT-sentiment |
| Epochs | 2 |
| Learning rate | 2e-5 |
| Weight decay | 0.01 |
| Class weights | None |
| Decision threshold | 0.58 (validation-tuned) |

**Final performance:**

| Metric | Value |
|---|---|
| Tuned test accuracy | 0.855 |
| Tuned test balanced accuracy | 0.851 |
| Tuned test macro F1 | 0.844 |
| Bearish recall | 0.837 |
| Bullish recall | 0.865 |
| Recall gap | 0.028 |
| Train–val overfit gap (bal acc) | 0.036 |

Improvement over best sparse baseline: +10pp balanced accuracy (0.751 → 0.851).

---

## Stage 3 — Signal Computation

**Class:** `StocktwitsSignalNode` at `src/agents/sentiment/stocktwits_node.py`

**What happens:** Loads the Silver checkpoint, parses `timestamp_published` to UTC-normalized dates, applies the 14-day decay window filter, then expands each post across all offset days with exponential decay weights. Groups by date (global) and by date × symbol (pair-level), aggregates weighted bullish/bearish scores via safe division.

**Weighting:** Equal per post (`w = 1.0`). The model's continuous probability is the signal — `prob_bullish` is used directly rather than `predicted_label`, so a post with `prob_bullish=0.59` and one with `prob_bullish=0.95` both carry different signal weight even though both are labeled "bullish".

**Exponential decay:**

| Parameter | Value |
|---|---|
| Half-life | 5 days |
| Decay constant λ | ln(2) / 5 ≈ 0.1386 |
| Decay window | 14 days |

Each post on day `t` is expanded to days `t, t+1, ..., t+13` with weight `exp(−λ × offset)`. At offset 5 (half-life), weight = 0.5. At offset 14, weight ≈ 0.14.

**Per-day aggregation:**

```
effective_weight  = w × exp(−λ × offset_days)
bullish_num       = effective_weight × prob_bullish
bearish_num       = effective_weight × prob_bearish
confidence_num    = effective_weight × max(prob_bullish, prob_bearish)

bullish_score          = Σ(bullish_num)   / Σ(effective_weight)    [0, 1]
bearish_score          = Σ(bearish_num)   / Σ(effective_weight)    [0, 1]
net_sentiment          = bullish_score − bearish_score              [−1, +1]
mean_model_confidence  = Σ(confidence_num) / Σ(effective_weight)   [0, 1]
```

All divisions use `np.divide` with `where=ew > 0, out=np.zeros_like(ew)` — zero-weight days produce 0.0.

---

## Signal Output

`compute(start_date, end_date)` returns two DataFrames.

**Global daily** — one row per date:

| Column | Dtype | Description |
|---|---|---|
| `date` | datetime64[ns, UTC] | Calendar day |
| `raw_post_count` | int | Total posts collected on this day |
| `signal_post_count` | float | Sum of effective decayed weights |
| `bullish_score` | float 0–1 | Decay-weighted mean of prob_bullish |
| `bearish_score` | float 0–1 | Decay-weighted mean of prob_bearish |
| `net_sentiment` | float −1 to +1 | bullish_score − bearish_score |
| `mean_model_confidence` | float 0–1 | Decay-weighted mean model certainty |

**Pair daily** — one row per date × symbol (4 symbols: EURUSD, GBPUSD, USDCHF, USDJPY):

Same columns as global daily, plus `symbol`.

`get_signal(target_date)` is a convenience wrapper: calls `compute()` over the decay buffer and returns a single dict for the most recent date with non-zero signal. Raises `ValueError` if no such date exists within `DECAY_WINDOW_DAYS + 5`.

---

## Pipeline Summary

```
StockTwits API (EURUSD / GBPUSD / USDCHF / USDJPY streams)
       ↓
StocktwitsCollector → Bronze JSONL  (data/raw/news/stocktwits/)
       ↓
StocktwitsPreprocessor
  normalize_text() + preprocess_for_model()
  FinTwitBERT-sentiment  (models/sentiment/stocktwits/)
  threshold = 0.58
  → Silver checkpoint  (data/processed/stocktwits/labels_checkpoint.jsonl)
       ↓
StocktwitsSignalNode.compute(start_date, end_date)
  └── equal weights (w=1.0) · prob_bullish as continuous signal
  └── exponential decay  half-life=5d · window=14d
  └── safe division → bullish_score | bearish_score | net_sentiment
       ↓
tuple[global_daily, pair_daily]
  net_sentiment | signal_post_count | mean_model_confidence
       ↓
SentimentAgent
```
