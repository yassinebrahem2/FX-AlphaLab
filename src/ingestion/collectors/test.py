# test_gdelt_safe.py

from datetime import datetime
from pathlib import Path
from src.ingestion.collectors.gdelt_collector import GDELTCollector

collector = GDELTCollector(output_dir=Path("data/raw/news"))

data = collector.collect(
    start_date=datetime(2024, 2, 1),
    end_date=datetime(2024, 2, 1),
)

print(data["gdelt_raw"].head())
print("Rows:", len(data["gdelt_raw"]))
