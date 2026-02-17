# Federal Reserve News Collection

> **⚠️ DEPRECATED**: This RSS-based collector is deprecated. Use [fed_scraper.md](fed_scraper.md) instead.
>
> **Reasons for deprecation:**
> - RSS feed lacks official category metadata (must guess from keywords)
> - Inconsistent document taxonomy vs scraper (fomc_statement vs policy)
> - Less accurate classification (keyword matching vs Fed's official categories)
> - Scraper provides better coverage and accuracy using Fed's HTML metadata
>
> **This documentation is preserved for reference only.**

---

Federal Reserve press releases, FOMC statements, speeches, testimony, and meeting minutes from the official RSS feed.

## Overview

**Data Type**: News documents (text-heavy, semi-structured)
**Source**: Federal Reserve RSS Feed (official)
**Update Frequency**: Event-based (real-time via RSS)
**Setup Required**: None (public RSS feed)
**Output**: Bronze JSONL (one JSON object per line)

The Federal Reserve is the **second-highest priority** source for USD-related sentiment analysis, providing critical insights into monetary policy decisions, economic outlook, and regulatory actions.

## Document Types

The collector categorizes publications into 5 document types:

| Document Type | Description | Example Frequency | Importance |
|--------------|-------------|-------------------|------------|
| **FOMC Statements** | Monetary policy decisions | 8 per year | Critical |
| **Speeches** | Remarks by Fed officials | Weekly | High |
| **Press Releases** | Announcements, enforcement actions | Daily | Medium |
| **Testimony** | Congressional testimony | Monthly | High |
| **Meeting Minutes** | FOMC meeting minutes | 8 per year | Critical |

## Setup

No API key or account required. Verify connectivity:

```bash
python -m scripts.collect_fed_data --health-check
```

Expected output:
```
[INFO] Health check: PASSED
[INFO] RSS feed accessible: https://www.federalreserve.gov/feeds/press_all.xml
```

## Data Schema (Bronze Layer)

Each document in the JSONL export contains:

```json
{
  "source": "fed",
  "timestamp_collected": "2024-01-15T12:00:00+00:00",
  "timestamp_published": "2024-01-15T14:00:00+00:00",
  "url": "https://www.federalreserve.gov/newsevents/...",
  "title": "Federal Reserve issues FOMC statement",
  "content": "Full text content extracted from the URL...",
  "document_type": "fomc_statement",
  "speaker": "Chair Powell",
  "metadata": {
    "rss_summary": "Brief description from RSS feed",
    "rss_published": "Mon, 15 Jan 2024 14:00:00 GMT",
    "feed_id": "https://..."
  }
}
```

### Field Descriptions

| Field | Type | Description |
|-------|------|-------------|
| `source` | string | Always "fed" |
| `timestamp_collected` | string | ISO 8601 UTC timestamp when data was collected |
| `timestamp_published` | string | ISO 8601 UTC timestamp when document was published |
| `url` | string | Full URL to the original publication |
| `title` | string | Publication title from RSS feed |
| `content` | string | Full text content extracted from the publication URL |
| `document_type` | string | Classification: `fomc_statement`, `speech`, `press_release`, `testimony`, or `minutes` |
| `speaker` | string | Speaker name for speeches (e.g., "Chair Powell"), empty for other types |
| `metadata` | object | Additional RSS feed metadata as nested dictionary |

## Usage

### Command Line (Recommended)

#### Collect Recent Publications (Last 3 Months)
```bash
python -m scripts.collect_fed_data
```

Output:
```
data/raw/news/fed/statements_20240115.jsonl
data/raw/news/fed/speeches_20240115.jsonl
data/raw/news/fed/press_releases_20240115.jsonl
data/raw/news/fed/testimony_20240115.jsonl
data/raw/news/fed/minutes_20240115.jsonl
```

#### Custom Date Range
```bash
python -m scripts.collect_fed_data --start 2023-01-01 --end 2023-12-31
```

#### All Options
```bash
python -m scripts.collect_fed_data --help

Options:
  --start DATE          Start date (YYYY-MM-DD), default: 90 days ago
  --end DATE            End date (YYYY-MM-DD), default: today
  --health-check        Verify RSS feed connectivity only
  --verbose, -v         Enable debug logging
```

### Programmatic Usage

#### Basic Collection

```python
from datetime import datetime
from pathlib import Path
from src.ingestion.collectors.fed_collector import FedCollector

# Initialize collector
collector = FedCollector()

# Collect recent publications (last 90 days by default)
data = collector.collect()

# Access by document type
fomc_statements = data["statements"]         # List of FOMC statement dicts
speeches = data["speeches"]                  # List of speech dicts
press_releases = data["press_releases"]      # List of press release dicts
testimony = data["testimony"]                # List of testimony dicts
minutes = data["minutes"]                    # List of meeting minutes dicts

print(f"Collected {len(fomc_statements)} FOMC statements")
print(f"Collected {len(speeches)} speeches")
```

#### Custom Date Range

```python
from datetime import datetime

collector = FedCollector()

# Collect specific time period
data = collector.collect(
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2023, 12, 31)
)

# Process each document
for doc in data["statements"]:
    print(f"Title: {doc['title']}")
    print(f"Published: {doc['timestamp_published']}")
    print(f"Content length: {len(doc['content'])} chars")
    print(f"URL: {doc['url']}\n")
```

#### Export to JSONL

```python
from datetime import datetime

collector = FedCollector()

# Collect data
data = collector.collect(start_date=datetime(2024, 1, 1))

# Export all document types to separate JSONL files
paths = collector.export_all(data=data)

for doc_type, path in paths.items():
    print(f"Exported {doc_type} to {path}")
```

Output:
```
Exported statements to data/raw/news/fed/statements_20240115.jsonl
Exported speeches to data/raw/news/fed/speeches_20240115.jsonl
...
```

#### Custom Output Directory

```python
from pathlib import Path

# Use custom output directory
collector = FedCollector(output_dir=Path("data/custom_fed"))

data = collector.collect()
paths = collector.export_all(data=data)
```

## Examples

### Example 1: Analyze FOMC Sentiment

```python
from src.ingestion.collectors.fed_collector import FedCollector
from datetime import datetime

collector = FedCollector()

# Collect last year's statements
data = collector.collect(
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2023, 12, 31)
)

# Analyze FOMC statements
for statement in data["statements"]:
    title = statement["title"]
    content = statement["content"]
    date = statement["timestamp_published"]

    # Check for hawkish/dovish language
    hawkish_terms = ["raise", "tighten", "reduce", "inflation"]
    dovish_terms = ["lower", "ease", "support", "growth"]

    hawkish_count = sum(content.lower().count(term) for term in hawkish_terms)
    dovish_count = sum(content.lower().count(term) for term in dovish_terms)

    sentiment = "hawkish" if hawkish_count > dovish_count else "dovish"

    print(f"{date[:10]} | {sentiment.upper()} ({hawkish_count}/{dovish_count})")
    print(f"  {title}\n")
```

### Example 2: Track Chair Powell Speeches

```python
from src.ingestion.collectors.fed_collector import FedCollector

collector = FedCollector()
data = collector.collect()

# Filter for Chair Powell speeches
powell_speeches = [
    speech for speech in data["speeches"]
    if "Powell" in speech.get("speaker", "")
]

print(f"Found {len(powell_speeches)} speeches by Chair Powell\n")

for speech in powell_speeches:
    print(f"{speech['timestamp_published'][:10]}")
    print(f"  {speech['title']}")
    print(f"  URL: {speech['url']}\n")
```

### Example 3: Export to Different Formats

```python
import json
import csv
from src.ingestion.collectors.fed_collector import FedCollector

collector = FedCollector()
data = collector.collect()

# 1. Export to JSONL (default, recommended)
paths = collector.export_all(data=data)

# 2. Convert to CSV (optional, loses nested structure)
import pandas as pd
for doc_type, documents in data.items():
    if documents:
        # Flatten metadata
        for doc in documents:
            doc['metadata'] = json.dumps(doc['metadata'])

        df = pd.DataFrame(documents)
        df.to_csv(f"data/raw/news/fed/{doc_type}.csv", index=False)

# 3. Export to separate text files (for NLP analysis)
from pathlib import Path
output_dir = Path("data/raw/news/fed/text")
output_dir.mkdir(exist_ok=True)

for doc_type, documents in data.items():
    for i, doc in enumerate(documents):
        filename = f"{doc_type}_{i:03d}.txt"
        output_dir.joinpath(filename).write_text(doc['content'], encoding='utf-8')
```

## Output Structure

JSONL files are written to `data/raw/news/fed/` with the naming convention:

```
data/raw/news/fed/
├── statements_YYYYMMDD.jsonl        # FOMC statements
├── speeches_YYYYMMDD.jsonl          # Speeches and remarks
├── press_releases_YYYYMMDD.jsonl    # Press releases
├── testimony_YYYYMMDD.jsonl         # Congressional testimony
└── minutes_YYYYMMDD.jsonl           # Meeting minutes
```

Each file contains one JSON object per line (JSONL format):
- **Human-readable**: Can inspect with text editor
- **Streamable**: Process line-by-line without loading all into memory
- **Appendable**: Can add new documents without rewriting entire file
- **Standard**: Industry standard for NLP/text corpora

## Reading JSONL Files

### Python

```python
import json
from pathlib import Path

# Read JSONL file
path = Path("data/raw/news/fed/statements_20240115.jsonl")

documents = []
with open(path, 'r', encoding='utf-8') as f:
    for line in f:
        doc = json.loads(line)
        documents.append(doc)

print(f"Loaded {len(documents)} documents")
```

### Command Line

```bash
# View first document (formatted)
head -1 data/raw/news/fed/statements_20240115.jsonl | python -m json.tool

# Count documents
wc -l data/raw/news/fed/statements_20240115.jsonl

# Search for keywords
grep -i "inflation" data/raw/news/fed/statements_20240115.jsonl
```

## Data Quality

- **Completeness**: All RSS feed entries are collected (subject to date filter)
- **Uniqueness**: Duplicate entries (same URL) are preserved as-is (Bronze principle)
- **Freshness**: RSS feed is updated in real-time as Fed publishes new content
- **Immutability**: Raw data preserved exactly as received (no transformation in Bronze)

## Performance

- **Initial collection** (90 days): ~30 seconds (depends on document count)
- **Daily incremental**: ~5-10 seconds (typically 0-5 new documents)
- **Rate limiting**: 1.5 second delay between requests (polite crawling)
- **Retry logic**: Automatic retry with exponential backoff on errors

## Troubleshooting

### Issue: "RSS feed not accessible"

**Cause**: Network connectivity or Fed server issues

**Solution**:
```bash
# Verify internet connection
python -m scripts.collect_fed_data --health-check

# Try direct curl
curl -I https://www.federalreserve.gov/feeds/press_all.xml
```

### Issue: "Empty content extracted"

**Cause**: Fed website HTML structure changed or individual URL failed

**Solution**:
- Check specific URL manually in browser
- Content extraction uses multiple fallback selectors
- Empty content is preserved (not an error) - indicates extraction failure

### Issue: "No documents collected"

**Cause**: Date range has no publications or all filtered out

**Solution**:
```python
# Check date range
collector = FedCollector()
data = collector.collect(
    start_date=datetime(2020, 1, 1),  # Widen date range
    end_date=datetime.now()
)
```

## Next Steps

After Bronze collection:
1. **Silver Layer**: Entity recognition, sentiment analysis (future)
2. **Gold Layer**: Aggregate metrics, policy signal extraction (future)
3. **Integration**: Feed into FX alpha models for USD-related signals

## RSS Feed Information

- **Feed URL**: https://www.federalreserve.gov/feeds/press_all.xml
- **Format**: RSS 2.0
- **Update frequency**: Real-time (as Fed publishes)
- **Historical depth**: ~6-12 months in feed
- **Documentation**: https://www.federalreserve.gov/feeds/feeds.htm

## See Also

- [Federal Reserve Official News](https://www.federalreserve.gov/newsevents.htm)
- [FOMC Calendar](https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm)
- [Project Architecture](../../README.md)
- [Bronze Layer Contract](../../PROJECT_PLAN_DRAFT.md.md#bronze-layer)
