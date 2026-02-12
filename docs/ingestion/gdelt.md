# GDELT Collector

## Overview

The GDELT Collector retrieves financial news data from the GDELT 2.1 dataset via Google BigQuery.

GDELT (Global Database of Events, Language, and Tone) monitors global news coverage and extracts structured metadata including:

- Publication timestamp
- Source domain
- Article URL
- Tone score (-10 to +10)
- Themes
- Locations
- Organizations mentioned

Within FX-AlphaLab, this collector feeds the **Bronze layer** with raw news signals relevant to foreign exchange markets.

The collector:

- Filters for FX-relevant themes
- Deduplicates by URL
- Prioritizes trusted financial domains
- Exports JSONL output for downstream processing

---

## Architecture

- Inherits from `DocumentCollector`
- `collect()` is pure (no file I/O)
- Returns `dict[str, list[dict]]` with key `"aggregated"` (consistent with DocumentCollector interface)
- `export_jsonl()` handles file writing
- `export_to_jsonl()` is a convenience wrapper
- No preprocessing inside the collector
- Domain credibility prioritization applied before return (sorted by tier, timestamp, hash)
- Deduplication handled via SHA256 URL hashing

---

## Google Cloud Setup

### 1️⃣ Requirements

- Google Cloud Project
- BigQuery API enabled
- Billing enabled

BigQuery provides a free tier (~1TB query data per month).

---

### 2️⃣ Service Account Credentials

Create a service account in Google Cloud Console with:

- BigQuery Data Viewer
- BigQuery Job User

Download the JSON key file.

Configure the credentials:

#### Option 1: .env File (Recommended)

Add to your `.env` file:

```env
GOOGLE_APPLICATION_CREDENTIALS=path/to/your/service_account.json
```

#### Option 2: Environment Variable

Set the environment variable directly:

**macOS / Linux:**
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service_account.json"
```

**Windows (PowerShell):**
```powershell
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\path\to\service_account.json"
```

---

## Query Logic

### Table Used

`gdelt-bq.gdeltv2.gkg_partitioned`

### Date Filtering

Queries are executed per-day:

```sql
DATE(_PARTITIONTIME) >= start_date
DATE(_PARTITIONTIME) < next_day
```

### FX Theme Filters

Only articles containing:

- `ECON_CURRENCY`
- `ECON_CENTRAL_BANK`

### Currency Filters

Only articles referencing:

- EUR
- USD
- GBP
- JPY

### Example Query

```sql
SELECT
    DATE,
    SourceCommonName,
    DocumentIdentifier,
    V2Tone,
    V2Themes AS Themes,
    V2Locations AS Locations,
    V2Organizations AS Organizations
FROM `gdelt-bq.gdeltv2.gkg_partitioned`
WHERE DATE(_PARTITIONTIME) >= "2024-01-01"
  AND DATE(_PARTITIONTIME) < "2024-01-02"
  AND (
      V2Themes LIKE '%ECON_CURRENCY%'
      OR V2Themes LIKE '%ECON_CENTRAL_BANK%'
  )
  AND (
      V2Themes LIKE '%EUR%'
      OR V2Themes LIKE '%USD%'
      OR V2Themes LIKE '%GBP%'
      OR V2Themes LIKE '%JPY%'
  )
```

### Domain Credibility Prioritization

Based on `SourceCommonName`.

**Tier 1:**
- reuters.com
- bloomberg.com
- ft.com

**Tier 2:**
- wsj.com
- cnbc.com

**Tier 3:**
- All other domains

Documents are sorted (not filtered) by:
1. `credibility_tier` (ascending)
2. `timestamp_published` (chronological)
3. `url_hash` (deterministic tie-breaker)

### Cost Guard

Each query is first executed as a dry-run:

```python
QueryJobConfig(dry_run=True)
```

If estimated scan exceeds 5 GB, execution aborts:

```python
RuntimeError("Query too expensive")
```

This prevents unexpected BigQuery charges.

## Data Schema

Each document returned by `collect()` (within the `"aggregated"` key):

```json
{
  "source": "gdelt",
  "timestamp_collected": "2026-02-12T14:09:07.317700",
  "timestamp_published": "2026-02-10T00:00:00Z",
  "url": "https://example.com/article",
  "source_domain": "reuters.com",
  "tone": "1.60427807486631,2.94117647058824,1.33689839572193,...",
  "themes": [
    "ECON_CURRENCY_EXCHANGE_RATE,2256",
    "ECON_WORLDCURRENCIES_EURO,2272",
    "ECON_CENTRAL_BANK,1234"
  ],
  "locations": [
    "1#United States#US#US##38#-97#US#1234",
    "1#Switzerland#SZ#SZ##47#8#SZ#3047"
  ],
  "organizations": [
    "Federal Reserve,154",
    "European Central Bank,892"
  ],
  "metadata": {
    "credibility_tier": 1,
    "url_hash": "11bbeec9eb5f9af1313240eaaf8bb983c51de2058f5838256e3c9e2a258fad8b"
  }
}
```

### Field Notes

- **tone**: Full V2Tone string (comma-separated values). First value is primary tone score (-10 to +10)
- **timestamp_collected**: UTC ISO 8601 at collection time
- **timestamp_published**: Parsed from GDELT DATE field to UTC ISO 8601
- **url_hash**: SHA256 hash for deduplication
- **themes/locations/organizations**: Include position offsets from GDELT (e.g., `"theme,offset"`)

## Export Behavior

### `export_jsonl(data, collection_date=None) -> Path`

Exports to:

```
data/raw/news/gdelt/aggregated_YYYYMMDD.jsonl
```

**Behavior:**
- One JSON object per line (JSONL format)
- UTF-8 encoding
- Raises `ValueError` if data list is empty
- Returns file path

### `export_to_jsonl(...) -> Path`

Convenience wrapper:
- If `data` provided → exports directly
- If `data is None` → calls `collect()` then exports (unwraps `"aggregated"` key automatically)
- Raises `ValueError` if neither data nor date range provided

## Error Handling

### Retry Logic

- Up to 3 attempts
- Exponential backoff (2s → 4s → 8s)
- Raises final `GoogleAPIError` if all retries fail

### Health Check

```python
collector.health_check()
```

Executes:
```sql
SELECT 1
```

Returns:
- `True` on success
- `False` on failure

## Example Usage

### Basic Collection and Export

```python
from pathlib import Path
from datetime import datetime
from src.ingestion.collectors.gdelt_collector import GDELTCollector

collector = GDELTCollector(
    output_dir=Path("data/raw/news/gdelt")
)

# Check connectivity
if not collector.health_check():
    raise RuntimeError("BigQuery not accessible")

# Collect data
result = collector.collect(
    start_date=datetime(2026, 2, 10),
    end_date=datetime(2026, 2, 11),
)

# result is: {"aggregated": [...]}
documents = result["aggregated"]
print(f"Collected {len(documents)} documents")

# Export
path = collector.export_jsonl(documents)
print(f"Exported to {path}")
```

### Quick Test

Run the test script:

```bash
python scripts/test_gdelt_collector_output.py
```

This will:
- Load credentials from `.env`
- Run health check
- Collect last 2 days of data
- Show document samples and statistics
- Export to JSONL

### Using Convenience Method

```python
# Collect and export in one call
path = collector.export_to_jsonl(
    start_date=datetime(2026, 2, 10),
    end_date=datetime(2026, 2, 11)
)
```

## Design Guarantees

- ✅ Pure data collection (no file I/O in `collect()`)
- ✅ Deterministic sorting (tier → timestamp → hash)
- ✅ Deduplication via SHA256 URL hashing
- ✅ Cost-safe batching (dry-run + 5GB limit)
- ✅ Domain credibility prioritization
- ✅ JSONL output format
- ✅ Fully unit tested with mocked BigQuery
- ✅ Separation of concerns (collect vs export)
- ✅ Retry logic with exponential backoff
- ✅ Follows Bronze layer contract

---

## Notes

- **BigQuery Costs:** ~0.5-1GB per day scanned. Free tier covers ~1TB/month.
- **Tier Distribution:** Tier 1/2 sources may not appear in every date range.
- **Date Format:** GDELT uses `YYYYMMDDHHMMSS` format, converted to ISO 8601.
- **Testing:** Use narrow date ranges (1-2 days) when testing to minimize costs.
