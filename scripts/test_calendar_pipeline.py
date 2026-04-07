from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
PYTHON_BIN = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"

RAW_DIR = PROJECT_ROOT / "data" / "raw" / "calendar"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

REACTION_PATH = PROCESSED_DIR / "calendar_reaction_live.csv"
ENRICHED_PATH = PROCESSED_DIR / "calendar_live_scored_reactions.csv"

REQUIRED_COLUMNS = ["timestamp_utc", "currency", "final_score", "direction_match_1h"]


def _run(command: list[str]) -> None:
    completed = subprocess.run(command, cwd=PROJECT_ROOT, capture_output=True, text=True)
    if completed.returncode != 0:
        print(completed.stdout)
        print(completed.stderr)
        raise RuntimeError(f"Command failed: {' '.join(command)}")
    if completed.stdout:
        print(completed.stdout.strip())


def _latest_raw() -> Path:
    candidates = list(RAW_DIR.glob("forexfactory_*.csv"))
    if not candidates:
        raise FileNotFoundError("No ForexFactory raw CSV found in data/raw/calendar")
    return max(candidates, key=lambda p: (p.stat().st_mtime, p.name.lower()))


def _create_runtime_fixture() -> Path:
    src = _latest_raw()
    df = pd.read_csv(src)
    if df.empty:
        raise ValueError(f"Latest raw fixture source is empty: {src}")
    fixture_path = PROCESSED_DIR / "calendar_runtime_fixture.csv"
    df.to_csv(fixture_path, index=False)
    return fixture_path


def _validate_outputs() -> None:
    if not REACTION_PATH.exists():
        raise FileNotFoundError(f"Missing output: {REACTION_PATH}")
    if not ENRICHED_PATH.exists():
        raise FileNotFoundError(f"Missing output: {ENRICHED_PATH}")

    reaction_df = pd.read_csv(REACTION_PATH)
    if reaction_df.empty:
        raise ValueError("calendar_reaction_live.csv is empty")

    missing_cols = [col for col in REQUIRED_COLUMNS if col not in reaction_df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in reaction output: {', '.join(missing_cols)}")

    print("Output validation passed")
    print(f"Rows in reaction output: {len(reaction_df)}")
    print("Required columns present: " + ", ".join(REQUIRED_COLUMNS))


def run_fixture_mode() -> None:
    fixture_path = _create_runtime_fixture()
    _run(
        [
            str(PYTHON_BIN),
            "scripts/collect_calendar_data.py",
            "--source",
            "fixture",
            "--input",
            str(fixture_path),
            "--preprocess",
        ]
    )
    _run([str(PYTHON_BIN), "scripts/run_live_calendar_reaction.py"])
    _validate_outputs()


def run_live_mode(start: str, end: str) -> None:
    _run(
        [
            str(PYTHON_BIN),
            "scripts/collect_calendar_data.py",
            "--source",
            "forexfactory",
            "--start",
            start,
            "--end",
            end,
            "--preprocess",
        ]
    )
    _run([str(PYTHON_BIN), "scripts/run_live_calendar_reaction.py"])
    _validate_outputs()


def main() -> int:
    parser = argparse.ArgumentParser(description="Test calendar pipeline end-to-end")
    parser.add_argument("--mode", choices=["fixture", "live"], required=True)
    parser.add_argument("--start", default="2026-03-01")
    parser.add_argument("--end", default="2026-03-31")
    args = parser.parse_args()

    if not PYTHON_BIN.exists():
        raise FileNotFoundError(f"Python executable not found: {PYTHON_BIN}")

    if args.mode == "fixture":
        run_fixture_mode()
    else:
        run_live_mode(args.start, args.end)

    print(f"Calendar pipeline test passed in {args.mode} mode")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Calendar pipeline test failed: {exc}")
        raise SystemExit(1)
