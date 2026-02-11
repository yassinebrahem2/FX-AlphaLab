# Bank of England (BoE) Collector

This collector ingests **Bank of England** (BoE) content required for **GBP-related sentiment analysis**.

It fetches RSS feeds for:
- Press releases
- MPC statements (Monetary Policy Committee)
- Speeches
- Monetary policy summaries

Output is written to the **Bronze layer** in **JSONL** format (1 JSON object per line).

---

## Overview

**Collector**: `BoECollector`  
**Source**: Bank of England (official website + RSS)  
**Data Type**: News + monetary policy communications  
**Update Frequency**: Event-based (as BoE publishes new items)  
**Output**: Bronze JSONL  
**Storage**: `data/raw/news/boe/`

---

## Setup

Install dependencies (already in project requirements):

```bash
pip install feedparser beautifulsoup4 requests
```

---

## Health Check

```bash
python -c "from src.ingestion.collectors.boe_collector import BoECollector; print(BoECollector().health_check())"
```

Expected output:
```
True
```

---

## Usage

### Programmatic Usage

```python
from datetime import datetime, timezone
from src.ingestion.collectors.boe_collector import BoECollector

collector = BoECollector()

data = collector.collect(
    start_date=datetime(2026, 2, 1, tzinfo=timezone.utc),
    end_date=datetime(2026, 2, 11, tzinfo=timezone.utc),
)

# data is a dict[str, list[dict]]
print(data.keys())
```

### Export JSONL (Bronze)

```python
# Export a single document type
collector.export_jsonl(data["press_releases"], "press_releases")

# Export everything
paths = collector.export_all_to_jsonl(data)
print(paths)
```

---

## Default Date Range

If `start_date` / `end_date` are not provided:

- `end_date = now (UTC)`
- `start_date = end_date - 90 days`

This satisfies the requirement:
> “3 months minimum for Week 6”

---

## Document Types

The collector categorizes items into the following keys:

| Key | Description |
|-----|------------|
| `press_releases` | BoE news / press releases |
| `statements` | MPC statements |
| `speeches` | Speeches (includes speaker extraction) |
| `summaries` | Monetary policy summaries / minutes |

The value stored in each record uses the same value for:
- the dict key
- `document_type`

---

## Bronze JSONL Output

### Output Directory

```
data/raw/news/boe/
```

### Filenames

Each export produces:

```
{document_type}_{YYYYMMDD}.jsonl
```

Example:
```
press_releases_20260211.jsonl
speeches_20260211.jsonl
statements_20260211.jsonl
summaries_20260211.jsonl
```

---

## Output Schema (Bronze Contract)

Each JSONL line is one record with this schema:

```json
{
  "source": "BoE",
  "timestamp_collected": "2026-02-11T20:47:18.893696+00:00",
  "timestamp_published": "2026-02-10T16:30:00+00:00",
  "url": "https://www.bankofengland.co.uk/speech/...",
  "title": "Measure, Model, Tackle, Tailor...",
  "content": "<RAW_HTML_OR_RAW_TEXT>",
  "document_type": "speeches",
  "speaker": "James Talbot",
  "metadata": {
    "rss_feed": "https://www.bankofengland.co.uk/rss/speeches",
    "rss_id": "...",
    "rss_summary": "...",
    "rss_tags": ["..."],
    "fetch_error": null,
    "fetch_error_type": null
  }
}
```

### Notes

- `timestamp_published` is normalized to **UTC ISO-8601**
- `speaker` is only populated for `speeches` (otherwise `null`)
- `content` is stored as **raw HTML** (Bronze principle: immutable raw)

---

## Rate Limiting

- A politeness delay is applied between entries:
  - `REQUEST_DELAY` seconds (default: 1s)

---

## Retry / Backoff

HTTP/network errors are handled using:
- retries
- exponential backoff

This prevents failures from killing the full collection run.

If an article fetch fails even after retries:
- the collector stores `content = RSS summary` (fallback)
- the error is preserved inside metadata:
  - `metadata.fetch_error`
  - `metadata.fetch_error_type`

---

## Speaker Extraction (Speeches)

For speech URLs, the collector attempts to extract a speaker name from the page.

Example output:

```json
{
  "document_type": "speeches",
  "speaker": "James Talbot"
}
```

---

## Testing

Run unit tests:

```bash
pytest tests/ingestion/test_boe_collector.py -v
```

Tests cover:
- RSS parsing
- timestamp normalization
- document type classification
- date filtering (inclusive bounds)
- rate limiting (sleep called)
- error handling + fallback behavior
- JSONL export methods (tmp_path)

All tests use mocked HTTP responses (no live BoE calls).

---

## Limitations / Notes

- BoE RSS feeds occasionally return transient `5xx` errors.
  Retry logic prevents most failures.
- Some pages may contain heavy HTML; Bronze stores raw HTML.
  Transformation/cleaning belongs in Silver preprocessors.
