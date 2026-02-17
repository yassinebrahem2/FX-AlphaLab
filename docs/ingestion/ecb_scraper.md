# ECB Scraper (Historical Backfill)

Selenium-based scraper for collecting historical ECB press releases, speeches, monetary policy statements, and economic bulletins from the ECB website archive.

> **Note**: This scraper complements the [ECB News Collector](ecb_news.md) (RSS-based). The RSS feed only provides ~10 days of recent content. Use this scraper for historical backfill spanning weeks or months.

## Overview

**Data Type**: News, speeches, policy documents (historical)
**Source**: ECB website archive (Selenium + requests)
**Update Frequency**: On-demand backfill
**Setup Required**: Chrome/Chromium browser installed
**Output**: Bronze JSONL (same format as ECB News Collector)

## How It Works

The scraper uses a two-phase approach:

1. **Discovery** (Selenium): Navigates JS-rendered archive listing pages, scrolls to load all content, extracts article URLs
2. **Content Extraction** (requests + BS4): Fetches individual article pages (static HTML), extracts title, full text, speaker, dates

This hybrid approach is necessary because ECB archive listing pages are entirely JavaScript-rendered (no static HTML, no hidden API, no sitemaps for press content), while individual articles are plain HTML.

## Intelligent Routing

The `collect_news_data.py` script automatically selects the right collector:

| Date Range | Collector Used | Method |
|------------|---------------|--------|
| ≤ 14 days | `ECBNewsCollector` | RSS feed (fast, reliable) |
| > 14 days | `ECBScraperCollector` | Selenium scraping (slower, historical) |

The threshold is configurable via `ECB_RSS_THRESHOLD_DAYS` in `scripts/collect_news_data.py`.

## Archive Sections

The scraper covers 4 ECB archive sections:

| Section | Archive URL Path | Default Category |
|---------|-----------------|------------------|
| Speeches | `/press/key/html/index.en.html` | speeches |
| Press Releases | `/press/pr/html/index.en.html` | pressreleases |
| Press Conferences | `/press/pressconf/html/index.en.html` | policy |
| Monetary Policy | `/press/govcdec/mopo/html/index.en.html` | policy |

## Usage

### Via collect_news_data.py (Recommended)

The unified news collection script handles routing automatically:

```bash
# Historical backfill (>14 days triggers scraper automatically)
python -m scripts.collect_news_data --source ecb --start 2025-06-01 --end 2026-02-15

# Recent data (≤14 days uses RSS automatically)
python -m scripts.collect_news_data --source ecb --days 7
```

### Programmatic Usage

```python
from datetime import datetime, timezone
from src.ingestion.collectors.ecb_scraper_collector import ECBScraperCollector

collector = ECBScraperCollector(headless=True)

data = collector.collect(
    start_date=datetime(2025, 6, 1, tzinfo=timezone.utc),
    end_date=datetime(2026, 2, 15, tzinfo=timezone.utc),
)

# Access by document type
speeches = data["speeches"]
policy = data["policy"]

# Export to JSONL
paths = collector.export_all(data)

# Always close the Selenium driver when done
collector.close()
```

## Output Schema

Identical to the ECB News Collector — see [ecb_news.md](ecb_news.md#output-schema).

**Location**: `data/raw/news/ecb/`
**Format**: JSONL (one JSON object per line)
**Filename**: `{document_type}_{YYYYMMDD}.jsonl`

Additional metadata field: `metadata.scrape_method = "historical"` (vs `"rss"` for the news collector).

## Deduplication

Two layers of deduplication prevent duplicate documents:

1. **Cross-collector**: Loads existing URLs from all JSONL files in `data/raw/news/ecb/` before scraping. Articles already collected via RSS are skipped.
2. **Within-run**: Tracks discovered URLs in memory to avoid processing the same article from multiple archive sections.

## Rate Limiting & robots.txt

- **Crawl-delay**: 5 seconds between requests (per ECB `robots.txt`)
- **Human-like scrolling**: Random scroll distances (60-100% viewport), pauses (0.5-1.5s), 15% backscroll probability
- **No disallowed paths**: ECB `robots.txt` does not block `/press/` paths
- **Content fetching**: Uses `requests` with retry logic and exponential backoff

## Error Handling

| Error | Behavior |
|-------|----------|
| Chrome not installed | Raises `WebDriverException` with descriptive message |
| Archive page timeout | Logs warning, returns empty list for that section |
| Individual article fetch fails | Logs warning, skips article, continues |
| Invalid date range (start > end) | Raises `ValueError` |
| Empty content extracted | Skips document, logs warning |
| Driver crash mid-scrape | `close()` called in finally block, driver set to None |
| Malformed existing JSONL | Skips bad lines, loads valid URLs |

## Dependencies

- `selenium` — browser automation
- `undetected-chromedriver` — stealth Chrome driver (preferred, falls back to standard `selenium.webdriver`)
- `webdriver-manager` — automatic ChromeDriver version management (optional)
- `beautifulsoup4` — HTML parsing for article content
- `requests` — HTTP client for article fetching

All are existing project dependencies (shared with ForexFactory collector).

## Testing

```bash
# Run scraper tests
pytest tests/ingestion/test_ecb_scraper_collector.py -v

# Run shared utility tests
pytest tests/ingestion/test_ecb_utils.py -v

# Run all ECB-related tests
pytest tests/ingestion/test_ecb_scraper_collector.py tests/ingestion/test_ecb_utils.py tests/ingestion/test_ecb_news_collector.py -v
```

## Shared Utilities

Both `ECBNewsCollector` and `ECBScraperCollector` delegate common logic to `ecb_utils.py`:

- `classify_document_type()` — regex-based document classification
- `extract_speaker_name()` — speaker extraction from titles/content
- `fetch_full_content()` — HTML content extraction with BS4
- `create_ecb_session()` — requests session with retry/backoff
- `ECBNewsDocument` — shared dataclass for document structure

## Troubleshooting

### "Chrome not found"
- Install Chrome or Chromium
- Ensure `chromedriver` is on PATH, or let `webdriver-manager` handle it

### Scraping is slow
- Expected: 5-second crawl delay per robots.txt, plus scroll time per archive page
- Each archive section requires full page scroll to load all articles
- Estimate ~2-5 minutes per section depending on date range

### "No articles found in date range"
- Verify the ECB website is accessible
- Check that archive pages load in a regular browser
- Expand date range to confirm articles exist

### Duplicate documents across runs
- Normal if date ranges overlap between RSS and scraper
- Deduplication prevents actual duplicates in output files
