# Reddit Sentiment Node Pipeline — FX Forum Posts to Risk Sentiment Signal

---

## Visualization View (Production)

### Outside the Node

**Arctic Shift Photon Reddit API**
Historical and near-realtime Reddit post archive accessible via REST. Returns posts per subreddit ordered by `created_utc`, paginated by time cursor.

**Bronze Storage**
One JSONL file per subreddit, incrementally appended. Posts written exactly as received from the API. Each file accumulates all posts for that subreddit across all collection runs.

**RedditPreprocessor**
Filters raw posts for quality, then sends each post (title + body truncated to 1200 chars) to a Groq-hosted LLM (`gpt-oss-120b`) for structured labeling. Writes one labeled record per post to the Silver checkpoint. Incremental — posts already in the checkpoint are skipped.

**Silver Checkpoint**
One JSONL file containing all LLM-labeled posts across all subreddits. Canonical input for the signal node.

---

### Inside the Node

**Checkpoint Reader**
Loads the Silver checkpoint line by line. Malformed JSON lines are skipped with a warning.

**Date Parser**
Converts `created_utc` Unix timestamps to UTC-normalized dates. NOISE posts and posts with unparseable timestamps are dropped before any weighting.

**Window Filter**
Restricts posts to `[start_date − 14 days, end_date]`. The 14-day lookback is the exponential decay window.

**Weighting Engine**
Each post receives a combined weight: `w = log1p(score_clip) × content_weight`, where `score_clip` is the Reddit upvote count clipped at the p99 value of the batch, and `content_weight` is assigned by content type (FUNDAMENTAL=2, NEWS_REACTION=2, TECHNICAL=1, POSITION_DISCLOSURE=1). Posts with no upvotes still carry their content weight.

**Decay Engine**
Each post on day `t` is projected forward across offsets 0 → 13 with weight `exp(−λ × offset)` where `λ = ln(2) / 5`.

**Pair Attribution**
Pair assignment comes from `target_pair` in the Silver label. If null, the node searches title and body text for a known pair ticker. Posts with no pair match are assigned to the `"global"` bucket.

**Daily Aggregator**
Groups decay-expanded posts by date (global) and by date × pair (pair-level). Aggregates risk_off, risk_on, sentiment_strength, and fundamental_share via weighted means. Safe division — zero-weight days produce 0.0.

---

### Downstream

**SentimentAgent**
Receives global and pair-level DataFrames. Uses `risk_off_score` / `risk_on_score` as the macro risk tone component and `sentiment_strength_wmean` per pair as the directional component.

---

### Flow

```
Arctic Shift Photon Reddit API
           ↓
RedditCollector → Bronze JSONL  (data/raw/reddit/{subreddit}_raw.jsonl)
           ↓
RedditPreprocessor
  quality filters
  Groq gpt-oss-120b  (src/ingestion/preprocessors/prompts/reddit_labeling_system.txt)
  8-field structured label per post
  → Silver checkpoint  (data/processed/reddit/labels_checkpoint.jsonl)
           ↓
        [ REDDIT SIGNAL NODE ]
        Checkpoint Reader + Date Parser
        NOISE filter · ALLOWED_CONTENT only
                   ↓
         Window Filter (14-day lookback)
                   ↓
        Weighting: log1p(score_clip) × content_weight
                   ↓
        Decay Engine  λ = ln(2)/5, half-life 5 days
                   ↓
        Pair Attribution (label → text fallback → global)
                   ↓
        Daily Aggregator (global + pair-level)
        safe division → risk_off_score | risk_on_score | sentiment_strength_wmean
                   ↓
           SentimentAgent
```

---

## Overview

The Reddit pipeline transforms posts from FX and macro finance subreddits into daily risk sentiment and directional signals. It runs in two decoupled stages: a preprocessing stage that uses an LLM to assign structured labels (content type, risk sentiment, directional strength, target pair) to each post, and a signal computation stage that applies engagement weighting and exponential decay to produce aggregated daily scores at both global and per-pair level.

---

## Stage 1 — Data Collection (Bronze Layer)

**Source:** Arctic Shift Photon Reddit API (`https://arctic-shift.photon-reddit.com/api/posts/search`)

**What happens:** `RedditCollector` fetches posts subreddit-by-subreddit using time-cursor pagination sorted ascending by `created_utc`. On each run, the cursor resumes from the last collected timestamp in the existing file. With `backfill=True`, the cursor advances from the last known post, filling the full gap to `end_date`. With `backfill=False` (default), the cursor starts from `max(last_ts + 1s, end_date − 30 days)`. Deduplication is performed in-memory via post ID before writing.

**Output:** JSONL files at `data/raw/reddit/`
- Naming: `{subreddit_lower}_raw.jsonl`
- One file per subreddit, incrementally appended across runs

**Subreddits collected:** Forex, investing, stocks (configurable)

**Bronze record key fields:**

| Field | Type | Description |
|---|---|---|
| `id` | str | Reddit post ID |
| `title` | str | Post title |
| `selftext` | str | Post body |
| `score` | int | Reddit upvote count at collection time |
| `created_utc` | int | Unix timestamp (UTC) |
| `subreddit` | str | Subreddit name |
| `link_flair_text` | str | Post flair (e.g. "Fundamental Analysis") |
| `author` | str | Reddit username |
| `removed_by_category` | str \| null | Non-null if post was removed by moderators |

---

## Stage 2 — LLM Labeling (Silver Layer)

**Class:** `RedditPreprocessor` at `src/ingestion/preprocessors/reddit_preprocessor.py`

**What happens:**
1. Loads all Bronze JSONL files for the configured subreddits.
2. Compares post IDs against the Silver checkpoint — already-labeled posts are skipped.
3. Applies quality filters: removed posts, deleted or AutoModerator authors, posts shorter than 20 chars, and URL-only bodies are excluded.
4. Sends each remaining post to `gpt-oss-120b` (via Groq) as a structured labeling request. The system prompt is loaded from `src/ingestion/preprocessors/prompts/reddit_labeling_system.txt`. Post body is truncated to 1200 chars.
5. Validates the 8-field JSON response. Invalid responses are retried up to `max_retries` times.
6. Appends the labeled record to the Silver checkpoint with flush after each write.

**LLM:** `gpt-oss-120b` via Groq, `temperature=0.0`, `max_tokens=512`, `response_format=json_object`

**Multi-key rate limit management:** A `KeyPool` tracks per-key backoff state. On `RateLimitError`, the key is placed in exponential backoff (5s × 2^n, capped at 300s). Requests always use the key with the earliest available time.

**Label schema — 8 fields assigned per post:**

| Field | Values | Description |
|---|---|---|
| `content_type` | TECHNICAL, FUNDAMENTAL, NEWS_REACTION, POSITION_DISCLOSURE, NOISE | Dominant topic of the post |
| `sarcasm_irony_score` | 0, 1, 2 | 0=none, 1=ambiguous, 2=clear sarcasm (forces sentiment_strength=0) |
| `sentiment_strength` | −2, −1, 0, +1, +2 | Direction and magnitude of market sentiment |
| `target_pair` | str or null | Currency pair inferred from post content |
| `risk_sentiment` | RISK_ON, RISK_OFF, NEUTRAL | Overall macro risk tone of the post |
| `target_clarity` | 0, 1, 2 | 0=none, 1=implied colloquial, 2=explicit notation |
| `stance_clarity` | CLEAR, CONDITIONAL, QUESTION, NONE | How the author presents their view |
| `reasoning` | str (max 15 words) | LLM's key observation, written before committing to labels |

**Output:** `data/processed/reddit/labels_checkpoint.jsonl`

Full Silver record schema (per line):

| Field | Type | Description |
|---|---|---|
| `id` | str | Reddit post ID |
| `content_type` | str | LLM label |
| `sarcasm_irony_score` | int | LLM label |
| `sentiment_strength` | int | LLM label |
| `target_pair` | str \| null | LLM label |
| `risk_sentiment` | str | LLM label |
| `target_clarity` | int | LLM label |
| `stance_clarity` | str | LLM label |
| `reasoning` | str | LLM reasoning trace |
| `score` | int | Reddit upvote count (from Bronze) |
| `created_utc` | int | Unix timestamp UTC (from Bronze) |
| `subreddit` | str | Subreddit name (from Bronze) |
| `model` | str | "gpt-oss-120b" |
| `labeled_at` | str | ISO 8601 UTC datetime of labeling |

---

## Stage 3 — Signal Computation

**Class:** `RedditSignalNode` at `src/agents/sentiment/reddit_node.py`

**What happens:** Loads the Silver checkpoint, parses `created_utc` to UTC-normalized dates, filters to ALLOWED_CONTENT types (FUNDAMENTAL, NEWS_REACTION, TECHNICAL, POSITION_DISCLOSURE — NOISE is excluded), assigns weights, applies exponential decay, attributes posts to pairs, and aggregates to daily features at global and pair level.

**Content weights:**

| content_type | weight |
|---|---|
| FUNDAMENTAL | 2 |
| NEWS_REACTION | 2 |
| TECHNICAL | 1 |
| POSITION_DISCLOSURE | 1 |

**Engagement weight:** `w_score = log1p(score_clip)` where `score_clip = score.clip(upper=score.quantile(0.99))`. Combined weight: `w = w_score × w_content`.

**Exponential decay:**

| Parameter | Value |
|---|---|
| Half-life | 5 days |
| Decay constant λ | ln(2) / 5 ≈ 0.1386 |
| Decay window | 14 days |

**Pair attribution (in order):**
1. `target_pair` from Silver label, normalized to 6-char notation
2. Fallback: regex search of `title + body` text for a known pair ticker
3. Fallback: assigned to `"global"` bucket

**Per-day aggregation:**

```
effective_weight     = w × exp(−λ × offset_days)
risk_off_num         = effective_weight × (risk_sentiment == "RISK_OFF")
risk_on_num          = effective_weight × (risk_sentiment == "RISK_ON")
sentiment_num        = effective_weight × sentiment_strength
fundamental_num      = effective_weight × (content_type in {FUNDAMENTAL, NEWS_REACTION})

risk_off_score              = Σ(risk_off_num)    / Σ(effective_weight)   [0, 1]
risk_on_score               = Σ(risk_on_num)     / Σ(effective_weight)   [0, 1]
sentiment_strength_wmean    = Σ(sentiment_num)   / Σ(effective_weight)   [−2, +2]
fundamental_share           = Σ(fundamental_num) / Σ(effective_weight)   [0, 1]
confidence                  = signal_post_count  / raw_post_count         [0, 1]
```

All divisions use `np.divide` with `where=ew > 0, out=np.zeros_like(ew)`.

---

## Signal Output

`compute(start_date, end_date)` returns two DataFrames.

**Global daily** — one row per date:

| Column | Dtype | Description |
|---|---|---|
| `date` | datetime64[ns, UTC] | Calendar day |
| `raw_post_count` | int | Total posts collected on this day (all content types) |
| `signal_post_count` | float | Sum of effective decayed weights (ALLOWED_CONTENT only) |
| `risk_off_score` | float 0–1 | Decay-weighted share of RISK_OFF posts |
| `risk_on_score` | float 0–1 | Decay-weighted share of RISK_ON posts |
| `sentiment_strength_wmean` | float −2 to +2 | Decay-weighted mean directional sentiment strength |
| `fundamental_share` | float 0–1 | Share of signal weight from FUNDAMENTAL + NEWS_REACTION posts |
| `confidence` | float 0–1 | signal_post_count / raw_post_count |

**Pair daily** — one row per date × pair (11 pairs: EURUSD, GBPUSD, USDJPY, XAUUSD, AUDUSD, USDCAD, USDCHF, NZDUSD, EURGBP, EURJPY, GBPJPY):

Same columns as global daily, plus `pair`.

`get_signal(target_date)` is a convenience wrapper: calls `compute()` over the decay buffer and returns a single dict for the most recent date with non-zero signal. Raises `ValueError` if no such date exists.

---

## Pipeline Summary

```
Arctic Shift Photon Reddit API  (r/Forex · r/investing · r/stocks)
           ↓
RedditCollector → Bronze JSONL  (data/raw/reddit/{subreddit}_raw.jsonl)
           ↓
RedditPreprocessor
  quality filters (removed, deleted, too-short, URL-only excluded)
  gpt-oss-120b via Groq  ·  system prompt: reddit_labeling_system.txt
  8-field structured label per post
  → Silver checkpoint  (data/processed/reddit/labels_checkpoint.jsonl)
           ↓
RedditSignalNode.compute(start_date, end_date)
  └── NOISE excluded · ALLOWED_CONTENT only
  └── w = log1p(score_clip) × content_weight
  └── exponential decay  half-life=5d · window=14d
  └── pair attribution: label → text fallback → global
  └── safe division → risk_off_score | risk_on_score | sentiment_strength_wmean
           ↓
tuple[global_daily, pair_daily]
  risk_off_score | risk_on_score | sentiment_strength_wmean | fundamental_share | confidence
           ↓
SentimentAgent
```
