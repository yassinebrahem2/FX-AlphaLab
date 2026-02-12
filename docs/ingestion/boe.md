# Bank of England (BoE) Collector

## Overview

The **BoECollector** ingests official Bank of England communications
required for GBP-related sentiment and macro analysis.

It collects structured textual data from official BoE RSS feeds and
linked pages, and stores them in the **Bronze layer** following the
project's medallion architecture principles.

**Collector Class:** `BoECollector`\
**Base Class:** `DocumentCollector`\
**Source:** Bank of England (official website + RSS feeds)\
**Output Format:** JSONL (Bronze, immutable raw data)\
**Storage Path:** `data/raw/news/boe/`

------------------------------------------------------------------------

## Data Sources

The collector fetches from official RSS feeds including:

-   https://www.bankofengland.co.uk/RSS/News\
-   https://www.bankofengland.co.uk/rss/news\
-   https://www.bankofengland.co.uk/rss/speeches\
-   https://www.bankofengland.co.uk/rss/prudential-regulation-publications

Each RSS entry links to an official BoE webpage, from which full content
is fetched.

------------------------------------------------------------------------

## Document Categories

Collected items are categorized into the following buckets:

  -------------------------------------------------------------------------------
  Bucket Key     Description                          document_type Value
  -------------- ------------------------------------ ---------------------------
  `statements`   General BoE press releases / news    `press_release`

  `mpc`          Monetary Policy Committee related    `mpc_statement`
                 communications

  `speeches`     Official speeches                    `boe_speech`

  `summaries`    Monetary policy summaries / minutes  `monetary_policy_summary`
  -------------------------------------------------------------------------------

### Notes

-   The **bucket key** determines the output filename.
-   The `document_type` field preserves the specific source
    classification.
-   `statements` replaces the earlier `press_releases` bucket to align
    with project taxonomy.

------------------------------------------------------------------------

## Bronze Layer Output

### Output Directory

    data/raw/news/boe/

### Filename Format

    {document_type_bucket}_{YYYYMMDD}.jsonl

Example:

    statements_20260212.jsonl
    speeches_20260212.jsonl
    mpc_20260212.jsonl
    summaries_20260212.jsonl

### Important Note on Filename Date

`{YYYYMMDD}` corresponds to the **collection/export run date (UTC)** ---
the day the collector was executed.

The actual publication date of each document is stored per record under:

    timestamp_published

Example:

-   File: `statements_20260212.jsonl`
-   `timestamp_collected`: 2026-02-12T00:32:24+00:00
-   `timestamp_published`: 2026-02-10T16:30:00+00:00

This ensures deterministic Bronze partitioning by ingestion date while
preserving original publication timestamps.

------------------------------------------------------------------------

## Bronze Record Schema

Each JSONL line contains:

``` json
{
  "source": "BoE",
  "timestamp_collected": "UTC ISO-8601",
  "timestamp_published": "UTC ISO-8601",
  "url": "Original article URL",
  "title": "Article title",
  "content": "Raw HTML content",
  "document_type": "Specific document type",
  "speaker": "Speaker name (if speech)",
  "metadata": {
    "rss_feed": "...",
    "rss_id": "...",
    "rss_summary": "...",
    "rss_tags": [...],
    "fetch_error": null,
    "fetch_error_type": null
  }
}
```

### Bronze Principles

-   Raw HTML content is preserved.
-   No transformations or cleaning are applied.
-   All available source metadata is retained.
-   Errors during article fetch are recorded in metadata without failing
    the collection run.

------------------------------------------------------------------------

## Date Range Behavior

### collect(start_date, end_date)

-   If no dates are provided:
    -   `end_date = now (UTC)`
    -   `start_date = end_date - 90 days`
-   Date filtering uses inclusive bounds.
-   All timestamps are normalized to UTC.

This satisfies the requirement of collecting at least 3 months of
historical data.

------------------------------------------------------------------------

## Speaker Extraction (Speeches)

For speech documents, the collector attempts to extract the speaker name
using:

1.  `<meta name="author">`
2.  Text near `<h1>`
3.  Pattern matching for "speech by NAME"

Speaker extraction is best-effort and non-blocking.

------------------------------------------------------------------------

## Reliability & Error Handling

### Retry Logic

-   Exponential backoff using `urllib3 Retry`
-   Retries on: 429, 500, 502, 503, 504
-   Respects `Retry-After` headers

### Rate Limiting

-   `REQUEST_DELAY` (default: 1 second) between requests

### Fallback Behavior

If full article fetch fails:

-   RSS summary is used as fallback content
-   Error details are recorded in:
    -   `metadata.fetch_error`
    -   `metadata.fetch_error_type`

Collection continues without crashing.

------------------------------------------------------------------------

## Health Check

The collector implements:

``` python
health_check() -> bool
```

Returns `True` if at least one RSS feed is reachable and contains
entries.

Example:

``` bash
python -c "from src.ingestion.collectors.boe_collector import BoECollector; print(BoECollector().health_check())"
```

------------------------------------------------------------------------

## Testing

Unit tests cover:

-   RSS parsing
-   Timestamp normalization
-   Document type classification
-   MPC classification via title keywords
-   Date filtering (inclusive bounds)
-   Error handling & fallback
-   Rate limiting behavior
-   JSONL export validation

All HTTP calls are mocked --- no live network calls required for tests.

Run tests:

``` bash
pytest tests/ingestion/test_boe_collector.py -v
```

------------------------------------------------------------------------

## Architecture Compliance

-   Inherits from `DocumentCollector`
-   Returns `dict[str, list[dict]]`
-   No file I/O inside `collect()`
-   Exports JSONL format
-   Preserves Bronze data contract
-   Compatible with medallion architecture (Bronze → Silver → Gold)

------------------------------------------------------------------------

## Summary

The BoECollector:

-   Fetches and categorizes BoE communications
-   Normalizes timestamps to UTC
-   Preserves raw content
-   Handles retries and rate limits safely
-   Records metadata and fetch errors
-   Produces Bronze-layer JSONL outputs partitioned by collection date

The implementation is production-safe, test-covered, and aligned with
project ingestion standards.
