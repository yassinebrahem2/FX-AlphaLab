

# Federal Reserve Scraper Collector

Historical document collection for Federal Reserve press releases and speeches using year-based archive pages.

## Overview

The `FedScraperCollector` provides efficient historical backfill for Federal Reserve documents by fetching year-specific static HTML pages. Unlike the RSS-based `FedCollector` (which only provides ~10-14 days of recent data), this scraper can collect complete archives going back years.

**Key Features:**
- Simple HTTP requests + BeautifulSoup4 (no Selenium overhead)
- Year-based fetching (one page per year)
- Supports press releases and speeches
- Exact date range filtering
- Automatic document type classification
- Full content extraction
- Polite crawling delays
- JSONL export format

## Architecture

### Data Sources

1. **Press Releases**: `/newsevents/pressreleases/{YEAR}-press.htm`
   - Contains all press releases for a specific year
   - Categories: Monetary Policy, Enforcement Actions, Banking Regulations, etc.
   - ~140 documents per year on average

2. **Speeches**: `/newsevents/speech/{YEAR}-speeches.htm`
   - Contains all speeches for a specific year
   - Includes speaker names and locations
   - ~50 speeches per year on average

### Collection Process

```
┌─────────────────────────────────────────────────────────────┐
│                     FedScraperCollector                     │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
        ┌───────────────────────────────────────┐
        │  Calculate year range from dates      │
        │  (e.g., 2021-2025 → [2021,...,2025])  │
        └───────────────────────────────────────┘
                            │
                ┌───────────┴────────────┐
                │                        │
                ▼                        ▼
    ┌─────────────────────┐  ┌──────────────────────┐
    │  Press Releases     │  │  Speeches            │
    │  (each year)        │  │  (each year)         │
    └─────────────────────┘  └──────────────────────┘
                │                        │
                ▼                        ▼
    ┌─────────────────────┐  ┌──────────────────────┐
    │  Parse <li> items   │  │  Parse <div> rows    │
    │  - Date (MM/DD/YY)  │  │  - Date (<time>)     │
    │  - Title (<a>)      │  │  - Title (<a><em>)   │
    │  - Category (<em>)  │  │  - Speaker (.speaker)│
    └─────────────────────┘  └──────────────────────┘
                │                        │
                ▼                        ▼
    ┌─────────────────────────────────────────────┐
    │  Filter by exact start_date/end_date        │
    └─────────────────────────────────────────────┘
                            │
                            ▼
    ┌─────────────────────────────────────────────┐
    │  Fetch full content from article URLs       │
    │  (with retry logic + polite delays)         │
    └─────────────────────────────────────────────┘
                            │
                            ▼
    ┌─────────────────────────────────────────────┐
    │  Categorize by document type:               │
    │  - policy (Monetary Policy)                 │
    │  - regulation (Banking/Enforcement)         │
    │  - speeches (Speeches)                      │
    │  - other (Other Announcements)              │
    └─────────────────────────────────────────────┘
                            │
                            ▼
    ┌─────────────────────────────────────────────┐
    │  Export to JSONL:                           │
    │  - data/raw/news/fed/policy_YYYYMMDD.jsonl  │
    │  - data/raw/news/fed/speeches_YYYYMMDD.jsonl│
    │  - etc.                                     │
    └─────────────────────────────────────────────┘
```

## Usage

### Basic Example

```python
from datetime import datetime
from pathlib import Path
from src.ingestion.collectors.fed_scraper_collector import FedScraperCollector

# Initialize collector
collector = FedScraperCollector()

# Collect documents for date range
data = collector.collect(
    start_date=datetime(2021, 1, 1),
    end_date=datetime(2025, 12, 31),
    include_speeches=True
)

# Export to JSONL
paths = collector.export_all(data=data)

# Output:
# {
#     'policy': Path('data/raw/news/fed/policy_20260215.jsonl'),
#     'regulation': Path('data/raw/news/fed/regulation_20260215.jsonl'),
#     'speeches': Path('data/raw/news/fed/speeches_20260215.jsonl')
# }
```

### Press Releases Only

```python
# Skip speeches for faster collection
data = collector.collect(
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 12, 31),
    include_speeches=False  # Only press releases
)
```

### Custom Output Directory

```python
# Use custom output path
collector = FedScraperCollector(
    output_dir=Path("custom/output/path")
)
```

### Health Check

```python
# Verify Fed website is reachable
if collector.health_check():
    data = collector.collect()
else:
    print("Fed website is unreachable")
```

## Document Schema

Each document in the JSONL output follows this Bronze layer schema:

```json
{
  "source": "fed",
  "timestamp_collected": "2026-02-15T10:30:00Z",
  "timestamp_published": "2024-06-15T00:00:00Z",
  "url": "/newsevents/pressreleases/monetary20240615a.htm",
  "title": "Federal Reserve Board announces approval...",
  "content": "Full article text content...",
  "document_type": "policy",
  "category": "Monetary Policy",
  "speaker": null,
  "metadata": {}
}
```

**For speeches:**

```json
{
  "source": "fed",
  "timestamp_collected": "2026-02-15T10:30:00Z",
  "timestamp_published": "2024-12-15T00:00:00Z",
  "url": "/newsevents/speech/miran20241215a.htm",
  "title": "The Inflation Outlook",
  "content": "Speech text content...",
  "document_type": "speeches",
  "category": "speeches",
  "speaker": "Stephen I. Miran",
  "metadata": {
    "location": "At the School of International and Public Affairs, Columbia University"
  }
}
```

## Document Type Classification

The collector maps Fed categories to standardized document types:

| Fed Category                          | Document Type | Description                       |
|---------------------------------------|---------------|-----------------------------------|
| Monetary Policy                       | `policy`      | FOMC statements, policy decisions |
| Orders on Banking Applications        | `regulation`  | Banking application approvals     |
| Enforcement Actions                   | `regulation`  | Enforcement orders                |
| Banking and Consumer Regulatory Policy| `regulation`  | Regulatory announcements          |
| Other Announcements                   | `other`       | Miscellaneous announcements       |
| Speeches                              | `speeches`    | Official speeches and remarks     |

## HTML Parsing Details

### Press Releases Structure

```html
<div class="row">
    <div class="col-xs-3 col-md-2 eventlist__time">
        <time>12/30/2025</time>
    </div>
    <div class="col-xs-9 col-md-10 eventlist__event">
        <p><a href="/newsevents/pressreleases/monetary20251230a.htm">
            <em>Minutes of the Federal Open Market Committee, December 9–10, 2025</em>
        </a></p>
        <p class='eventlist__press'><em><strong>Monetary Policy</strong></em></p>
    </div>
</div>
```

**Extraction:**
- Date: `<time>` tag (MM/DD/YYYY format)
- Title + URL: `<a>` tag (title in `<em>`)
- Category: `<p class="eventlist__press"> > <strong>` tag

### Speeches Structure

```html
<div class="row">
    <div class="col-xs-3 col-md-2 eventlist__time">
        <time>12/15/2025</time>
    </div>
    <div class="col-xs-9 col-md-10 eventlist__event">
        <p><a href="/newsevents/speech/miran20251215a.htm">
            <em>The Inflation Outlook</em>
        </a></p>
        <p class="news__speaker">Governor Stephen I. Miran</p>
        <p>At the School of International and Public Affairs, Columbia University</p>
    </div>
</div>
```

**Extraction:**
- Date: `<time>` tag (MM/DD/YYYY format)
- Title + URL: `<a>` tag (title in `<em>`)
- Speaker: `<p class="news__speaker">` (with title prefix removed)
- Location: Next `<p>` after speaker

## Performance

### Efficiency Comparison

| Method              | Years | Requests | Time  | Notes                        |
|---------------------|-------|----------|-------|------------------------------|
| Fed Scraper         | 5     | 10       | ~30s  | 5 press + 5 speeches         |
| RSS Feed            | N/A   | 1        | ~2s   | Only last ~10 days           |
| JavaScript Scraper  | 5     | 1135+    | ~10m  | Paginated view (227 pages/yr)|

The Fed scraper is **~20x faster** than JavaScript pagination for multi-year ranges while providing complete historical coverage.

### Polite Crawling

The collector implements polite delays:
- **Between years**: 1-2 seconds (randomized)
- **Between articles**: 0.5-1 seconds (randomized)
- **Retry logic**: Exponential backoff on failures

## Error Handling

The collector gracefully handles:
- Network failures (retry with exponential backoff)
- Invalid HTML (skips malformed items)
- Missing dates (skips items without valid dates)
- HTTP errors (logs and continues)
- Missing content (warns and skips)

## Limitations

1. **No robots.txt**: Fed website has no robots.txt, but we still implement polite crawling
2. **Date resolution**: Dates are at day-level (no time information)
3. **Static content**: Only works for static HTML (won't work if Fed switches to JavaScript)
4. **Year boundaries**: Must fetch complete years (can't request Q1 2024 without fetching all 2024)

## Testing

Run tests with pytest:

```bash
# All Fed scraper tests
pytest tests/ingestion/test_fed_scraper_collector.py -v

# Specific test class
pytest tests/ingestion/test_fed_scraper_collector.py::TestCollect -v

# With coverage
pytest tests/ingestion/test_fed_scraper_collector.py --cov=src/ingestion/collectors/fed_scraper_collector
```

## Troubleshooting

### No documents collected

**Symptoms:** `collect()` returns empty dict or zero documents

**Causes:**
1. Date range too narrow (no documents in that range)
2. Fed website changed HTML structure
3. Network connectivity issues

**Solutions:**
```python
# Check health
if not collector.health_check():
    print("Fed website unreachable")

# Widen date range
data = collector.collect(
    start_date=datetime(2020, 1, 1),
    end_date=datetime.now()
)

# Check logs for parsing errors
collector.logger.setLevel(logging.DEBUG)
```

### HTTP errors

**Symptoms:** `HTTPError` or timeout exceptions

**Solutions:**
- Check internet connection
- Verify Fed website is up: https://www.federalreserve.gov
- Increase timeout (edit `DEFAULT_TIMEOUT` in collector)

### Parsing failures

**Symptoms:** Warnings like "Failed to fetch article"

**Causes:**
- Fed changed HTML structure
- Invalid URLs in archive pages
- Articles removed/moved

**Solutions:**
- Update HTML selectors if structure changed
- Check specific failing URLs manually
- Report issue if systematic

## Integration

### With News Preprocessor

```python
from src.ingestion.collectors.fed_scraper_collector import FedScraperCollector
from src.ingestion.preprocessors.news_preprocessor import NewsPreprocessor

# 1. Collect Bronze data
collector = FedScraperCollector()
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

### Scheduled Collection

```python
from apscheduler.schedulers.blocking import BlockingScheduler

def collect_fed_daily():
    """Collect yesterday's Fed documents."""
    collector = FedScraperCollector()
    yesterday = datetime.now() - timedelta(days=1)

    data = collector.collect(
        start_date=yesterday,
        end_date=yesterday
    )

    collector.export_all(data=data)

scheduler = BlockingScheduler()
scheduler.add_job(collect_fed_daily, 'cron', hour=1)  # Daily at 1 AM
scheduler.start()
```

## See Also

- [Fed RSS Collector](fed_collector.md) - For recent documents (last 10-14 days)
- [ECB Scraper](ecb_scraper.md) - Similar architecture for ECB documents
- [News Preprocessor](../preprocessing/news_preprocessor.md) - Bronze → Silver transformation
