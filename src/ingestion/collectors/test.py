# test_gdelt_safe.py

import json
from datetime import datetime, timedelta
from pathlib import Path

from src.ingestion.collectors.gdelt_collector import GDELTCollector

collector = GDELTCollector(output_dir=Path("data/raw/news"), project_id="fx-alphalab-487111")

start = datetime(2024, 2, 1)
end = datetime(2024, 2, 3)

data = collector.collect(
    start_date=start,
    end_date=end,
)

df = data["gdelt_raw"]

print("Rows fetched from BigQuery (combined):", len(df))

# -------------------------------------------------
# Validate unique hashes in combined dataframe
# -------------------------------------------------
unique_hashes = {collector._hash_url(url) for url in df["DocumentIdentifier"] if url}

print("Unique URL hashes (in memory):", len(unique_hashes))


# -------------------------------------------------
# Validate ALL JSONL files across date range
# -------------------------------------------------
json_hashes = []
current_day = start.date()

while current_day <= end.date():
    file_path = Path("data/raw/news/gdelt") / f"aggregated_{current_day.strftime('%Y%m%d')}.jsonl"

    if not file_path.exists():
        raise FileNotFoundError(f"Missing file: {file_path}")

    with open(file_path, encoding="utf-8") as f:
        for line in f:
            record = json.loads(line)
            json_hashes.append(record["metadata"]["url_hash"])

    current_day += timedelta(days=1)


print("Total rows written to JSONL (all days):", len(json_hashes))
print("Unique hashes in JSONL (all days):", len(set(json_hashes)))

assert len(json_hashes) == len(set(json_hashes)), "Duplicates found across JSONL files!"

print("✅ Multi-day validation passed.")
