# Bank of England (BoE) Collector

## Overview

The **BoECollector** ingests official communications from the **Bank of
England (BoE)** to support GBP-related sentiment analysis and
macroeconomic signal processing.

The collector retrieves content from official RSS feeds and linked BoE
webpages, normalizes timestamps to UTC, classifies document types, and
stores raw immutable records in the **Bronze layer** as JSONL files.

------------------------------------------------------------------------

## Key Characteristics

-   **Source**: Bank of England (official RSS feeds + website pages)
-   **Layer**: Bronze (immutable raw storage)
-   **Output Format**: JSONL (1 JSON object per line)
-   **Storage Path**: `data/raw/news/boe/`
-   **Collection Mode**: Date-range based
-   **Default Lookback**: 90 days (if not specified)
-   **Timezone Normalization**: UTC ISO-8601

------------------------------------------------------------------------

## Supported RSS Feeds

The collector pulls from:

-   https://www.bankofengland.co.uk/RSS/News
-   https://www.bankofengland.co.uk/rss/news
-   https://www.bankofengland.co.uk/rss/speeches
-   https://www.bankofengland.co.uk/rss/prudential-regulation-publications

------------------------------------------------------------------------

## Document Classification

Each RSS entry is classified using URL patterns and title heuristics.

  -----------------------------------------------------------------------------
  Bucket Key          document_type Value                   Description
  ------------------- ------------------------------------- -------------------
  `press_releases`    `press_release`                       General BoE news

  `speeches`          `boe_speech`                          Official speeches

  `summaries`         `monetary_policy_summary`             Monetary policy
                                                            summaries/minutes

  `mpc`               `mpc_statement`                       MPC-related
                                                            statements
  -----------------------------------------------------------------------------

Returned structure from `collect()`:

``` python
dict[str, list[dict]]
```

Each key represents a bucket of documents.

------------------------------------------------------------------------

## Usage

### Programmatic Collection

``` python
from datetime import datetime, timezone
from src.ingestion.collectors.boe_collector import BoECollector

collector = BoECollector()

data = collector.collect(
    start_date=datetime(2026, 2, 1, tzinfo=timezone.utc),
    end_date=datetime(2026, 2, 11, tzinfo=timezone.utc),
)

print(data.keys())
```

### Export to Bronze JSONL

``` python
# Export a single bucket
collector.export_jsonl(data["press_releases"], "press_releases")

# Export all buckets
paths = collector.export_all(data)
print(paths)
```

------------------------------------------------------------------------

## Default Date Behavior

If no dates are provided:

-   `end_date = now (UTC)`
-   `start_date = end_date - 90 days`

This ensures a minimum 3‑month lookback window.

------------------------------------------------------------------------

## Bronze Output Structure

### File Naming Convention

    {bucket}_{YYYYMMDD}.jsonl

Example:

    press_releases_20260211.jsonl
    speeches_20260211.jsonl
    mpc_20260211.jsonl
    summaries_20260211.jsonl

------------------------------------------------------------------------

## Bronze Record Schema

Each line in the JSONL file follows this schema:

``` json
{
  "source": "BoE",
  "timestamp_collected": "2026-02-11T20:47:18.893696+00:00",
  "timestamp_published": "2026-02-10T16:30:00+00:00",
  "url": "https://www.bankofengland.co.uk/speech/...",
  "title": "Example Title",
  "content": "Full extracted page text",
  "document_type": "boe_speech",
  "speaker": "Jane Doe",
  "metadata": {
    "rss_feed": "...",
    "rss_id": "...",
    "rss_summary": "...",
    "rss_tags": ["..."],
    "fetch_error": null,
    "fetch_error_type": null
  }
}
```

### Important Notes

-   `timestamp_published` is normalized to **UTC ISO-8601**
-   `speaker` is populated only for speech documents
-   `content` contains extracted page text (fallback to RSS summary if
    fetch fails)
-   All fetch errors are preserved in metadata
-   Bronze data remains immutable (no transformation applied)

------------------------------------------------------------------------

## Speaker Extraction (Speeches Only)

For speech URLs, the collector attempts to extract the speaker using:

1.  `<meta name="author">`
2.  Text near `<h1>` header (e.g., "Speech by NAME")
3.  Scanning `<main>` content for patterns

If extraction fails, `speaker = null`.

------------------------------------------------------------------------

## Error Handling & Resilience

### Retry Strategy

-   HTTP retries enabled
-   Exponential backoff
-   Retries on: 429, 500, 502, 503, 504
-   Respect `Retry-After` headers
-   Only retries `GET` requests

### Fallback Behavior

If article page fetch fails after retries:

-   `content` falls back to RSS summary (if available)
-   `metadata.fetch_error` stores the error message
-   `metadata.fetch_error_type` stores the exception class name

Collection continues without crashing.

------------------------------------------------------------------------

## Rate Limiting

A politeness delay is applied between processed entries:

    REQUEST_DELAY = 1.0 seconds

Ensures respectful interaction with BoE infrastructure.

------------------------------------------------------------------------

## Health Check

Validate RSS connectivity:

``` bash
python -c "from src.ingestion.collectors.boe_collector import BoECollector; print(BoECollector().health_check())"
```

Returns `True` if at least one RSS feed is reachable and contains
entries.

------------------------------------------------------------------------

## Testing

Run unit tests:

``` bash
pytest tests/ingestion/test_boe_collector.py -v
```

Test coverage includes:

-   Timestamp normalization
-   Document classification
-   Date filtering (inclusive bounds)
-   Rate limiting behavior
-   Error handling and fallback logic
-   JSONL export functionality
-   Health check behavior
-   Speaker extraction logic

All tests use mocked HTTP responses (no live BoE calls).

------------------------------------------------------------------------

## Architecture Notes

-   Uses `requests.Session` with configured retry adapter
-   Parses RSS via `feedparser`
-   Extracts page content using `BeautifulSoup`
-   Designed to keep Bronze layer immutable
-   Transformation/cleaning belongs to downstream Silver layer

------------------------------------------------------------------------

## Conclusion

The BoECollector provides a robust, resilient, and production-ready
ingestion mechanism for Bank of England communications, ensuring:

-   Deterministic Bronze contracts
-   Safe error handling
-   Proper classification
-   Clean UTC timestamp normalization
-   Reliable export workflows
