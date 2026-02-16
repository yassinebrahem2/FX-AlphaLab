# Bank of England Sitemap Scraper

Historical document collection for Bank of England using XML sitemap API for efficient backfill.

## Overview

The `BoEScraperCollector` provides efficient historical backfill for BoE documents by leveraging the XML sitemap API. Unlike the RSS-based `BoECollector` (which only provides ~90 days of data), this scraper can collect complete archives going back to 2021.

**Key Features:**
- Sitemap-based discovery (1 request for all URLs)
- Simple HTTP requests + BeautifulSoup4 (no Selenium)
- Supports speeches, MPC minutes, summaries, and press releases
- Date range filtering via sitemap lastmod
- Automatic document type classification
- Full content extraction
- Polite crawling delays
- JSONL export format

## Architecture

### Data Source

**Sitemap API**: `https://www.bankofengland.co.uk/_api/sitemap/getsitemap`
- Returns XML with all site URLs and last-modified dates
- Single request retrieves ~1,000+ document URLs (2021-present)
- Standard sitemap.org XML format

### Collection Process

```
┌─────────────────────────────────────────────────────────────┐
│                  BoEScraperCollector                        │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
        ┌───────────────────────────────────────┐
        │  Fetch XML sitemap (1 request)        │
        └───────────────────────────────────────┘
                            │
                            ▼
        ┌───────────────────────────────────────┐
        │  Parse <url> elements:                │
        │  - <loc> → URL                        │
        │  - <lastmod> → last modified date     │
        └───────────────────────────────────────┘
                            │
                ┌───────────┴────────────┐
                │                        │
                ▼                        ▼
    ┌─────────────────────┐  ┌──────────────────────┐
    │  Filter by pattern  │  │  Filter by date      │
    │  /speech/202[0-6]/  │  │  (lastmod)           │
    │  /news/202[0-6]/    │  │                      │
    │  /minutes/202[0-6]/ │  │                      │
    │  /monetary-policy-  │  │                      │
    │   summary           │  │                      │
    └─────────────────────┘  └──────────────────────┘
                │                        │
                └────────────┬───────────┘
                             ▼
        ┌────────────────────────────────────────┐
        │  Fetch each URL's HTML (static pages)  │
        │  Extract:                              │
        │  - Title: <h1 itemprop="name">         │
        │  - Date: div.published-date            │
        │  - Content: div.content-block          │
        │  - Speaker: (speeches only)            │
        └────────────────────────────────────────┘
                             │
                             ▼
        ┌────────────────────────────────────────┐
        │  Categorize by URL pattern:            │
        │  - speeches → boe_speech               │
        │  - mpc → mpc_statement                 │
        │  - summaries → monetary_policy_summary │
        │  - statements → press_release          │
        └────────────────────────────────────────┘
                             │
                             ▼
        ┌────────────────────────────────────────┐
        │  Export to JSONL:                      │
        │  - data/raw/news/boe/speeches_YYYYMMDD │
        │  - data/raw/news/boe/mpc_YYYYMMDD      │
        │  - data/raw/news/boe/summaries_YYYYMMDD│
        │  - data/raw/news/boe/statements_YYYYMMDD│
        └────────────────────────────────────────┘
```

## Usage

### Basic Example

```python
from datetime import datetime
from pathlib import Path
from src.ingestion.collectors.boe_scraper_collector import BoEScraperCollector

# Initialize collector
collector = BoEScraperCollector()

# Collect documents for date range
data = collector.collect(
    start_date=datetime(2021, 1, 1),
    end_date=datetime(2025, 12, 31)
)

# Export to JSONL
paths = collector.export_all(data=data)

# Output:
# {
#     'speeches': Path('data/raw/news/boe/speeches_20260216.jsonl'),
#     'mpc': Path('data/raw/news/boe/mpc_20260216.jsonl'),
#     'summaries': Path('data/raw/news/boe/summaries_20260216.jsonl'),
#     'statements': Path('data/raw/news/boe/statements_20260216.jsonl')
# }
```

### Custom Output Directory

```python
# Use custom output path
collector = BoEScraperCollector(
    output_dir=Path("custom/output/path")
)
```

### Health Check

```python
# Verify BoE sitemap is reachable
if collector.health_check():
    data = collector.collect()
else:
    print("BoE sitemap unreachable")
```

## Document Schema

Each document in the JSONL output follows this Bronze layer schema:

```json
{
  "source": "BoE",
  "timestamp_collected": "2026-02-16T10:30:00+00:00",
  "timestamp_published": "2024-12-15T00:00:00+00:00",
  "url": "https://www.bankofengland.co.uk/speech/2024/december/test-speech",
  "title": "The world today – remarks by Andrew Bailey",
  "content": "Full speech text content...",
  "document_type": "boe_speech",
  "speaker": "Andrew Bailey",
  "metadata": {
    "sitemap_lastmod": "2024-12-15T10:00:00Z",
    "bucket": "speeches"
  }
}
```

## Document Type Classification

The collector maps URL patterns to standardized document types:

| URL Pattern | Document Type | Bucket | Description |
|-------------|---------------|--------|-------------|
| `/speech/` | `boe_speech` | `speeches` | Official speeches by BoE officials |
| `/monetary-policy-summary` | `monetary_policy_summary` | `summaries` | MPC policy summaries |
| `/monetary-policy-committee/`, `/mpc/` | `mpc_statement` | `mpc` | MPC minutes and statements |
| `/news/`, `/minutes/` (other) | `press_release` | `statements` | Press releases and announcements |

## HTML Parsing Details

### Common Structure (All Document Types)

```html
<h1 itemprop="name">Document Title</h1>
<div class="published-date">Published on 08 February 2026</div>
<div class="content-block">
    <p>Main document content...</p>
</div>
```

**Extraction:**
- Title: `<h1 itemprop="name">`
- Date: `div.published-date` (format: "Published on DD Month YYYY")
- Content: `div.content-block` (with script/style/nav removal)
- Speaker: For speeches, extracted from page text ("speech by NAME")

## Performance

### Efficiency Comparison

| Method | Requests | Time | Coverage | Notes |
|--------|----------|------|----------|-------|
| BoE Sitemap | ~1,001 | ~15-20m | 2021-present (~1,000 docs) | 1 sitemap + ~1,000 pages |
| RSS Feed | 4 | ~10s | Last ~90 days (~111 docs) | Limited historical data |

The sitemap scraper provides **complete historical coverage** vs RSS's limited recent data.

### Polite Crawling

- **Between URLs**: 0.5-1.0 seconds (randomized)
- **Retry logic**: Exponential backoff on failures
- **Total time**: ~15-20 minutes for 5-year archive

## Error Handling

Gracefully handles:
- Network failures (retry with exponential backoff)
- Invalid XML (logs and returns empty)
- Missing dates (fallback to sitemap lastmod)
- HTTP errors (logs and continues)
- Missing content (warns and skips)
- Malformed HTML (skips document)

## Limitations

1. **Sitemap dependency**: Relies on BoE maintaining sitemap API
2. **Date resolution**: Publication dates at day-level (no time)
3. **Static content**: Only works for static HTML pages
4. **No real-time**: Sitemap updated periodically (not instant)
5. **URL patterns**: Assumes BoE maintains consistent URL structure

## Testing

Run tests with pytest:

```bash
# All BoE scraper tests
pytest tests/ingestion/test_boe_scraper_collector.py -v

# Specific test class
pytest tests/ingestion/test_boe_scraper_collector.py::TestCollect -v

# With coverage
pytest tests/ingestion/test_boe_scraper_collector.py --cov=src/ingestion/collectors/boe_scraper_collector
```

## Troubleshooting

### No documents collected

**Symptoms:** `collect()` returns empty dict

**Causes:**
1. Sitemap API down
2. Date range too narrow
3. URL patterns changed

**Solutions:**
```python
# Check health
if not collector.health_check():
    print("Sitemap unreachable")

# Widen date range
data = collector.collect(
    start_date=datetime(2020, 1, 1),
    end_date=datetime.now()
)
```

### HTTP errors

**Symptoms:** `HTTPError` or timeout exceptions

**Solutions:**
- Check internet connection
- Verify BoE website: https://www.bankofengland.co.uk
- Check sitemap API: https://www.bankofengland.co.uk/_api/sitemap/getsitemap

### Parsing failures

**Symptoms:** Warnings like "No title found" or "Insufficient content"

**Causes:**
- BoE changed HTML structure
- Document is not full article (e.g., redirect page)

**Solutions:**
- Check specific failing URL manually
- Update HTML selectors if structure changed

## Integration

### With News Preprocessor

```python
from src.ingestion.collectors.boe_scraper_collector import BoEScraperCollector
from src.ingestion.preprocessors.news_preprocessor import NewsPreprocessor

# 1. Collect Bronze data
collector = BoEScraperCollector()
paths = collector.export_all(
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 12, 31)
)

# 2. Preprocess to Silver
preprocessor = NewsPreprocessor()
for doc_type, jsonl_path in paths.items():
    parquet_path = preprocessor.process(
        jsonl_path,
        output_dir=Path("data/processed/sentiment")
    )
```

### Automated Collection Script

The `collect_news_data.py` script automatically routes to the scraper for historical ranges:

```bash
# Recent data (≤90 days) → Uses RSS
python scripts/collect_news_data.py --start 2026-01-01 --end 2026-02-16 --source boe

# Historical data (>90 days) → Uses sitemap scraper
python scripts/collect_news_data.py --start 2021-01-01 --end 2025-12-31 --source boe
```

## See Also

- [BoE RSS Collector](boe_collector.md) - For recent documents (last 90 days)
- [Fed Scraper](fed_scraper.md) - Similar year-based approach
- [ECB Scraper](ecb_scraper.md) - Selenium-based pagination approach
- [News Preprocessor](../preprocessing/news_preprocessor.md) - Bronze → Silver transformation
