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
- Returns `list[dict]`
- `export_jsonl()` handles file writing
- `export_to_jsonl()` is a convenience wrapper
- No preprocessing inside the collector
- Domain credibility prioritization applied before return
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

Set the environment variable:

#### macOS / Linux

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service_account.json"
Windows (PowerShell)
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\path\to\service_account.json"
Query Logic
Table Used
gdelt-bq.gdeltv2.gkg_partitioned

Date Filtering

Queries are executed per-day:

DATE(_PARTITIONTIME) >= start_date
DATE(_PARTITIONTIME) < next_day
FX Theme Filters

Only articles containing:

ECON_CURRENCY

ECON_CENTRAL_BANK

Currency Filters

Only articles referencing:

EUR

USD

GBP

JPY

Example Query
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

Domain Credibility Prioritization

Based on SourceCommonName.

Tier 1

reuters.com

bloomberg.com

ft.com

Tier 2

wsj.com

cnbc.com

Tier 3

All other domains

Documents are sorted, not filtered:

credibility_tier (ascending)

publication timestamp

url_hash (deterministic tie-breaker)

Cost Guard

Each query is first executed as a dry-run:

QueryJobConfig(dry_run=True)


If estimated scan exceeds 5 GB, execution aborts:

RuntimeError("Query too expensive")


This prevents unexpected BigQuery charges.

Data Schema

Each document returned by collect():

{
  "source": "gdelt",
  "timestamp_collected": "2024-02-12T12:34:56Z",
  "timestamp_published": "2024-02-01T09:30:00Z",
  "url": "https://example.com/article",
  "source_domain": "reuters.com",
  "tone": -2.4,
  "themes": ["ECON_CURRENCY", "USD"],
  "locations": ["United States"],
  "organizations": ["Federal Reserve"],
  "metadata": {
    "credibility_tier": 1,
    "url_hash": "sha256_hash_here"
  }
}

Field Notes

tone ranges approximately from -10 (very negative) to +10 (very positive)

timestamp_collected is UTC at collection time

timestamp_published parsed to UTC ISO format

url_hash ensures deduplication

Export Behavior
export_jsonl(data, collection_date=None) -> Path

Exports to:

data/raw/news/gdelt/aggregated_YYYYMMDD.jsonl


Behavior:

One JSON object per line (JSONL format)

UTF-8 encoding

Raises ValueError if data list is empty

Returns file path

export_to_jsonl(...) -> Path

Convenience wrapper.

If:

data provided → exports directly

data is None → calls collect() then exports

Raises ValueError if neither data nor date range provided.

Error Handling
Retry Logic

Up to 3 attempts

Exponential backoff (2s → 4s → 8s)

Raises final GoogleAPIError

Health Check
collector.health_check()


Executes:

SELECT 1


Returns:

True on success

False on failure

Example Usage
from pathlib import Path
from datetime import datetime
from ingestion.collectors.gdelt_collector import GDELTCollector

collector = GDELTCollector(
    output_dir=Path("data/raw/news/gdelt")
)

data = collector.collect(
    start_date=datetime(2024, 2, 1),
    end_date=datetime(2024, 2, 3),
)

path = collector.export_to_jsonl(data=data)

print(f"Exported to {path}")

Design Guarantees

Pure data collection

Deterministic sorting

Deduplication via SHA256

Cost-safe batching

Domain credibility prioritization

JSONL output

Fully unit tested

Separation of concerns (collect vs export)


---

This version:

✔ Meets ticket requirement  
✔ Includes setup instructions  
✔ Includes credentials instructions  
✔ Includes query example  
✔ Includes schema  
✔ Includes export behavior  
✔ Includes example usage  
✔ Does NOT expose personal paths  

You are now compliant.