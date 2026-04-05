import argparse
from pathlib import Path
import pandas as pd

# ONLY import what you need
from src.ingestion.collectors.forexfactory_collector import ForexFactoryCalendarCollector
from src.ingestion.preprocessors.calendar_parser import CalendarPreprocessor


def run_live(start_date=None, end_date=None):
    collector = ForexFactoryCalendarCollector()
    data = collector.collect(start_date=start_date, end_date=end_date)

    if "calendar" not in data:
        print("No data collected")
        return None

    df = data["calendar"]
    print(f"Collected {len(df)} rows")
    return df


def run_fixture(input_path):
    df = pd.read_csv(input_path)
    print(f"Loaded fixture with {len(df)} rows")
    return df


def run_preprocess(df):
    from pathlib import Path

    processor = CalendarPreprocessor()
    result = processor.preprocess(df)
    processed = result["events"]

    print(f"Processed {len(processed)} rows")

    scored_count = int(processed["score_available"].fillna(False).sum())
    skipped_count = len(processed) - scored_count

    print(f"Scored {scored_count} events")
    print(f"Skipped {skipped_count} events due to missing forecast/actual")

    output_path = Path("data/processed/calendar_fixture_scored.csv")
    processed.to_csv(output_path, index=False)
    print(f"Saved to {output_path}")

    return processed

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["live", "fixture"], required=True)
    parser.add_argument("--input", type=str, help="Path to fixture CSV")
    parser.add_argument("--preprocess", action="store_true")

    args = parser.parse_args()

    if args.source == "live":
        df = run_live()
    else:
        if not args.input:
            raise ValueError("Fixture mode requires --input")
        df = run_fixture(args.input)

    if df is None:
        return

    if args.preprocess:
        run_preprocess(df)


if __name__ == "__main__":
    main()