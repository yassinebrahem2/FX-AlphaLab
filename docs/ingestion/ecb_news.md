# ECB News Collection

European Central Bank press releases, speeches, monetary policy statements, and economic bulletins collection for sentiment analysis.

> **Note**: This documentation covers ECB **news/text data**. For ECB **exchange rates and policy rates** (numeric data), see [ecb_rates.md](ecb_rates.md).

## Overview

**Data Type**: News, speeches, policy documents
**Source**: ECB RSS feed (official)
**Update Frequency**: Real-time (as published)
**Setup Required**: None (public RSS feed)
**Output**: Bronze JSONL → Silver sentiment-scored data

## Setup

No API key or authentication required. The ECB RSS feed is publicly accessible.

### Verify Installation

```bash
python -m scripts.collect_ecb_news_data --health-check
```

Expected output:
```
[INFO] Health check: PASSED
```

## Data Types

The collector categorizes ECB documents into 4 types:

| Document Type | Description | Example Keywords | Priority |
|---------------|-------------|------------------|----------|
| `policy` | Monetary policy decisions | "interest rate", "governing council" | Highest |
| `speeches` | Speeches by ECB officials | "speech by", "remarks by" | High |
| `bulletins` | Economic bulletins | "economic bulletin", "quarterly" | Medium |
| `pressreleases` | General press releases | All others (default) | Standard |

## Extracted Fields

Each document contains:

| Field | Type | Description |
|-------|------|-------------|
| `source` | string | Always "ecb" |
| `timestamp_collected` | datetime | When document was collected (UTC ISO 8601) |
| `timestamp_published` | datetime | When document was published (UTC ISO 8601) |
| `url` | string | Full URL to original document |
| `title` | string | Document title |
| `content` | string | Full text content |
| `document_type` | string | One of: policy_decision, speech, bulletin, press_release |
| `speaker` | string\|null | Speaker name (for speeches only) |
| `language` | string | Document language (usually "en") |
| `metadata` | object | Additional RSS metadata (id, tags) |

## Usage

### Command Line (Recommended)

#### Collect Last 3 Months (Default)
```bash
python -m scripts.collect_ecb_news_data
```

#### Collect with Custom Date Range
```bash
python -m scripts.collect_ecb_news_data --start 2025-11-01 --end 2026-02-11
```

#### Collect Specific Duration
```bash
# Last 6 months
python -m scripts.collect_ecb_news_data --months 6

# Last 30 days
python -m scripts.collect_ecb_news_data --days 30
```

#### All Options
```bash
python -m scripts.collect_ecb_news_data --help

Options:
  --start DATE          Start date (YYYY-MM-DD), default: 3 months ago
  --end DATE            End date (YYYY-MM-DD), default: today
  --months N            Collect last N months (overrides --start)
  --days N              Collect last N days (overrides --start)
  --health-check        Verify RSS feed connectivity only
  --verbose, -v         Enable debug logging
```

### Programmatic Usage

```python
from datetime import datetime, timedelta, timezone
from src.ingestion.collectors.ecb_news_collector import ECBNewsCollector

# Initialize collector (uses Config defaults: data/raw/news/ecb)
collector = ECBNewsCollector()

# Or specify custom output directory
# from pathlib import Path
# collector = ECBNewsCollector(output_dir=Path("custom/path"))

# Collect last 3 months
data = collector.collect(
    start_date=datetime.now(timezone.utc) - timedelta(days=90),
    end_date=datetime.now(timezone.utc)
)

# Access by document type
policy_docs = data["policy"]
speeches = data["speeches"]
bulletins = data["bulletins"]
pressreleases = data["pressreleases"]

# Export to JSONL files
paths = collector.export_all(data=data)
print(f"Exported {len(paths)} document types")
```

#### Export Individual Type
```python
# Export only speeches
speeches_path = collector.export_jsonl(
    data=data["speeches"],
    document_type="speeches"
)
```

## Output Schema

### Bronze Layer (Raw JSONL)

**Location**: `data/raw/news/ecb/`
**Format**: JSONL (one JSON object per line)
**Filename**: `{document_type}_{YYYYMMDD}.jsonl`

**Examples**:
- `policy_20260211.jsonl`
- `speeches_20260211.jsonl`
- `bulletins_20260211.jsonl`
- `pressreleases_20260211.jsonl`

**JSONL Structure**:
```jsonl
{"source":"ecb","timestamp_collected":"2026-02-11T10:30:00Z","timestamp_published":"2026-02-11T09:00:00Z","url":"https://www.ecb.europa.eu/press/key/date/2026/html/ecb.sp260211.en.html","title":"Speech by Christine Lagarde, President of the ECB","content":"Full speech text here...","document_type":"speech","speaker":"Christine Lagarde","language":"en","metadata":{"rss_id":"ecb.sp260211","tags":["Monetary policy"]}}
{"source":"ecb","timestamp_collected":"2026-02-11T10:31:00Z","timestamp_published":"2026-01-30T13:45:00Z","url":"https://www.ecb.europa.eu/press/pr/date/2026/html/ecb.mp260130.en.html","title":"Monetary policy decisions","content":"Full policy statement...","document_type":"policy_decision","speaker":null,"language":"en","metadata":{"rss_id":"ecb.mp260130","tags":["Interest rates"]}}
```

### Silver Layer (Processed with Sentiment)

**Location**: `data/processed/sentiment/`
**Format**: CSV or Parquet
**Filename**: `sentiment_ecb_{start}_{end}.csv`

**Columns** (future implementation):
- `timestamp_utc`: ISO 8601 UTC timestamp
- `article_id`: Unique document identifier
- `pair`: Relevant currency pair (e.g., "EURUSD")
- `headline`: Document title
- `sentiment_score`: Float [-1.0, 1.0]
- `sentiment_label`: "positive", "neutral", "negative"
- `source`: "ecb"
- `url`: Original document URL
- `document_type`: Document category

## Document Type Classification

The collector uses regex patterns to classify documents:

### Policy Decisions
Patterns: `monetary policy decisions?`, `governing council`, `interest rate`, `key ecb interest rates`

Example: "Monetary policy decisions - January 2026"

### Speeches
Patterns: `speech by`, `remarks by`, `keynote`, `interview with`, `statement by`

Example: "Speech by Christine Lagarde, President of the ECB"

### Economic Bulletins
Patterns: `economic bulletin`, `monthly bulletin`, `quarterly bulletin`, `statistical`

Example: "Economic Bulletin Issue 1, 2026"

### Press Releases (Default)
All documents not matching other patterns fall into this category.

## Rate Limiting

The collector implements polite scraping:

- **1 second delay** between requests to ECB servers
- **Exponential backoff** for retries (max 3 attempts)
- **30 second timeout** per request

## Error Handling

| Error | Behavior |
|-------|----------|
| RSS feed unreachable | Raises RuntimeError after retries |
| Individual document fetch fails | Logs warning, uses RSS summary as fallback |
| Invalid date range | Raises ValueError |
| Empty feed | Returns empty dict with all categories |
| Malformed entry | Skips entry, logs error, continues |

## Health Check

```bash
python -m scripts.collect_ecb_news_data --health-check
```

Verifies:
- ✅ RSS feed URL is accessible
- ✅ Network connectivity
- ✅ No server errors

## Logging

**Console**: INFO level (collection progress, counts)
**File**: `logs/collectors/ecb_collector.log` (DEBUG level)

Enable verbose mode:
```bash
python -m scripts.collect_ecb_news_data --verbose
```

## Example Workflow

```bash
# 1. Health check
python -m scripts.collect_ecb_news_data --health-check

# 2. Collect last 3 months of ECB news
python -m scripts.collect_ecb_news_data

# 3. Verify output
ls data/raw/news/ecb/
# Expected output:
# policy_20260211.jsonl
# speeches_20260211.jsonl
# bulletins_20260211.jsonl
# pressreleases_20260211.jsonl

# 4. Inspect JSONL content (PowerShell)
Get-Content data/raw/news/ecb/speeches_20260211.jsonl -First 1 | python -m json.tool
```

## Speaker Extraction

For speeches, the collector automatically extracts speaker names:

**Patterns**:
- "Speech by [Name]"
- "Remarks by [Name]"
- "Interview with [Name]"
- "[Name], President of the ECB"

**Examples**:
- "Speech by Christine Lagarde" → `speaker: "Christine Lagarde"`
- "Remarks by Philip Lane at ECB conference" → `speaker: "Philip Lane"`

## Architecture Integration

### Bronze Layer (Current)
- ✅ Raw collection from RSS feed
- ✅ Document type classification
- ✅ Full content extraction
- ✅ JSONL export with all source fields

### Silver Layer (Future - W7+)
- 🔄 Sentiment analysis (NLP models)
- 🔄 Entity extraction (organizations, locations)
- 🔄 Topic modeling
- 🔄 Pair relevance scoring

### Gold Layer (Future - W9+)
- 🔄 Aggregated sentiment signals per pair
- 🔄 Impact scoring for trading signals
- 🔄 Combined with technical/macro signals

## RSS Feed Reference

**URL**: https://www.ecb.europa.eu/rss/press.html

**Content**: All ECB press releases, speeches, bulletins, and policy decisions

**Update Frequency**: Real-time as documents are published

**Format**: RSS 2.0

## Testing

Run all ECB news collector tests:
```bash
pytest tests/ingestion/test_ecb_news_collector.py -v
```

Run specific test categories:
```bash
# Test RSS parsing
pytest tests/ingestion/test_ecb_news_collector.py::TestDateParsing -v

# Test document classification
pytest tests/ingestion/test_ecb_news_collector.py::TestDocumentClassification -v

# Test export
pytest tests/ingestion/test_ecb_news_collector.py::TestExportJSONL -v
```

## Troubleshooting

### "Failed to fetch RSS feed"
- Check internet connection
- Verify ECB website is accessible: https://www.ecb.europa.eu
- Check firewall/proxy settings

### "No entries in feed"
- Normal if date range is too restrictive
- Expand date range or use defaults

### "Content extraction slow"
- Expected behavior (fetches full HTML for each document)
- Implements 1-second delays to be polite to servers
- For faster testing, use smaller date ranges

### Empty content field
- Fallback to RSS summary is automatic
- Check if document URL is accessible
- Some documents may have restricted access

## Future Enhancements

- [ ] Multi-language support (DE, FR, IT, ES)
- [ ] Caching mechanism to avoid re-fetching
- [ ] Incremental updates (only new documents)
- [ ] PDF document parsing
- [ ] Video/audio transcript extraction
- [ ] Real-time webhook integration
