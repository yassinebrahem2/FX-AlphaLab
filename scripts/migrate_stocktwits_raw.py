"""Migrate legacy Stocktwits raw files into canonical append-only JSONL files."""

from __future__ import annotations

import json
from pathlib import Path

TARGET_DIR = Path("data/raw/news/stocktwits")
SYMBOLS = ["EURUSD", "GBPUSD", "USDCHF", "USDJPY"]


def _load_records(path: Path) -> list[dict]:
    records: list[dict] = []
    with path.open("r", encoding="utf-8") as file_handle:
        for line in file_handle:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                item = json.loads(stripped)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                records.append(item)
    return records


def _migrate_symbol(symbol: str) -> None:
    prefix = symbol.lower()
    canonical_path = TARGET_DIR / f"{prefix}_raw.jsonl"
    source_files = sorted(
        path for path in TARGET_DIR.glob(f"{prefix}_*.jsonl") if path.name != canonical_path.name
    )

    records_by_id: dict[int, dict] = {}
    raw_records = 0
    for source_path in source_files:
        for record in _load_records(source_path):
            message_id = record.get("message_id")
            try:
                message_id_int = int(message_id)
            except (TypeError, ValueError):
                continue
            raw_records += 1
            records_by_id.setdefault(message_id_int, record)

    deduped_records = [records_by_id[message_id] for message_id in sorted(records_by_id)]
    canonical_path.parent.mkdir(parents=True, exist_ok=True)
    with canonical_path.open("w", encoding="utf-8") as file_handle:
        for record in deduped_records:
            file_handle.write(json.dumps(record, ensure_ascii=False) + "\n")

    print(
        f"symbol={prefix} source_files={len(source_files)} raw_records={raw_records} "
        f"dedup_records={len(deduped_records)}"
    )


def main() -> None:
    for symbol in SYMBOLS:
        _migrate_symbol(symbol)


if __name__ == "__main__":
    main()
