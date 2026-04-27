"""One-time migration from old GDELT Bronze JSONL to the new Bronze schema.

This script rewrites files named aggregated_YYYYMM.jsonl into
gdelt_YYYYMM_raw.jsonl under the same directory.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

DATA_DIR = Path("data/raw/news/gdelt")


def _migrate_text_field(value: object) -> str:
    if isinstance(value, list):
        return ";".join(value)
    return str(value or "")


def _migrate_record(raw: dict) -> dict | None:
    if not raw.get("url") or not raw.get("timestamp_published"):
        return None

    return {
        "source": raw.get("source", "gdelt"),
        "timestamp_collected": raw.get("timestamp_collected", ""),
        "timestamp_published": raw.get("timestamp_published", ""),
        "url": raw.get("url", ""),
        "source_domain": raw.get("source_domain", ""),
        "v2tone": str(raw["tone"]) if raw.get("tone") is not None else "",
        "themes": _migrate_text_field(raw.get("themes")),
        "locations": _migrate_text_field(raw.get("locations")),
        "organizations": _migrate_text_field(raw.get("organizations")),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate legacy GDELT Bronze files to the new Bronze schema",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing gdelt_YYYYMM_raw.jsonl files",
    )
    return parser.parse_args()


def _month_key(path: Path) -> str:
    stem = path.stem
    prefix = "aggregated_"
    if not stem.startswith(prefix):
        raise ValueError(f"Unexpected legacy filename: {path.name}")
    return stem[len(prefix) :]


def _load_records(path: Path) -> tuple[list[dict], int, int]:
    migrated_records: list[dict] = []
    records_in = 0
    skipped_malformed = 0

    with path.open(encoding="utf-8") as file_handle:
        for line in file_handle:
            if not line.strip():
                skipped_malformed += 1
                continue

            records_in += 1
            try:
                raw = json.loads(line)
            except json.JSONDecodeError:
                skipped_malformed += 1
                continue

            if not isinstance(raw, dict):
                skipped_malformed += 1
                continue

            migrated = _migrate_record(raw)
            if migrated is None:
                skipped_malformed += 1
                continue

            migrated_records.append(migrated)

    return migrated_records, records_in, skipped_malformed


def _write_records(path: Path, records: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as file_handle:
        for record in records:
            file_handle.write(json.dumps(record, ensure_ascii=False))
            file_handle.write("\n")


def main() -> int:
    args = parse_args()

    if not DATA_DIR.exists():
        print(f"DATA_DIR does not exist: {DATA_DIR}")
        return 1

    legacy_files = sorted(DATA_DIR.glob("aggregated_*.jsonl"))
    if not legacy_files:
        print(f"No legacy files found under {DATA_DIR}")
        return 0

    files_processed = 0
    files_skipped = 0
    total_records_migrated = 0
    total_records_dropped = 0
    failed_files: list[str] = []

    for input_path in legacy_files:
        try:
            month_key = _month_key(input_path)
            output_path = DATA_DIR / f"gdelt_{month_key}_raw.jsonl"

            if output_path.exists() and not args.force:
                print(f"skip {input_path.name} -> {output_path.name} (exists)")
                files_skipped += 1
                continue

            migrated_records, records_in, skipped_malformed = _load_records(input_path)
            _write_records(output_path, migrated_records)

            records_out = len(migrated_records)
            files_processed += 1
            total_records_migrated += records_out
            total_records_dropped += skipped_malformed

            print(
                f"{input_path.name}: records_in={records_in} "
                f"records_out={records_out} skipped_malformed={skipped_malformed}"
            )
        except Exception as exc:  # pragma: no cover - one-time migration safeguard
            failed_files.append(f"{input_path.name}: {exc}")
            print(f"failed {input_path.name}: {exc}")

    print("Summary")
    print(f"  Files processed        : {files_processed}")
    print(f"  Files skipped          : {files_skipped}")
    print(f"  Total records migrated : {total_records_migrated}")
    print(f"  Total records dropped   : {total_records_dropped}")
    print(f"  Failed files           : {failed_files if failed_files else '[]'}")

    return 1 if failed_files else 0


if __name__ == "__main__":
    sys.exit(main())
