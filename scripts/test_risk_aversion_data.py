"""
Data Quality Validation Script for Risk Aversion Indicators

Validates exported VIX and credit spreads data against Silver layer schema requirements.
"""

import sys
from pathlib import Path

import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.shared.config import Config  # noqa: E402


def validate_schema(df: pd.DataFrame, expected_columns: list[str], dataset_name: str) -> dict:
    """Validate dataframe schema against expected columns."""
    issues = []

    # Check column presence
    missing_cols = set(expected_columns) - set(df.columns)
    if missing_cols:
        issues.append(f"Missing columns: {missing_cols}")

    extra_cols = set(df.columns) - set(expected_columns)
    if extra_cols:
        issues.append(f"Extra columns: {extra_cols}")

    # Check timestamp format (ISO 8601 UTC)
    if 'timestamp_utc' in df.columns:
        try:
            pd.to_datetime(df['timestamp_utc'])
        except Exception as e:
            issues.append(f"Invalid timestamp format: {e}")

        # Check if timestamps end with 'Z' (UTC marker)
        if not df['timestamp_utc'].iloc[0].endswith('Z'):
            issues.append("Timestamps missing UTC 'Z' suffix")

    # Check for missing values
    null_counts = df.isnull().sum()
    if null_counts.sum() > 0:
        issues.append(f"Missing values found: {null_counts[null_counts > 0].to_dict()}")

    # Check value column is numeric
    if 'value' in df.columns:
        if not pd.api.types.is_numeric_dtype(df['value']):
            issues.append("'value' column is not numeric")

    return {
        'dataset': dataset_name,
        'rows': len(df),
        'columns': list(df.columns),
        'issues': issues,
        'valid': len(issues) == 0
    }


def validate_data_quality(df: pd.DataFrame, series_id: str) -> dict:
    """Validate data quality metrics beyond schema compliance."""
    issues = []
    warnings = []

    # Check for duplicates
    if 'timestamp_utc' in df.columns:
        dup_count = df.duplicated(subset=['timestamp_utc']).sum()
        if dup_count > 0:
            issues.append(f"{dup_count} duplicate timestamps found")

    # Check temporal ordering
    if 'timestamp_utc' in df.columns:
        timestamps = pd.to_datetime(df['timestamp_utc'])
        if not timestamps.is_monotonic_increasing:
            issues.append("Timestamps are not in chronological order")

    # Check value ranges
    if 'value' in df.columns:
        min_val = df['value'].min()
        max_val = df['value'].max()
        mean_val = df['value'].mean()
        std_val = df['value'].std()

        if series_id == "VIXCLS":
            # VIX typically 10-80, extreme events can push >80
            if min_val < 5:
                warnings.append(f"VIX min ({min_val:.2f}) unusually low")
            if max_val > 100:
                warnings.append(f"VIX max ({max_val:.2f}) extremely high")
        elif series_id == "BAMLH0A0HYM2":
            # Credit spreads typically 2-10%, crisis can push >10%
            if min_val < 1:
                warnings.append(f"Credit spread min ({min_val:.2f}%) unusually low")
            if max_val > 20:
                warnings.append(f"Credit spread max ({max_val:.2f}%) extremely high")

    # Check data completeness (should have observations for most business days)
    if 'timestamp_utc' in df.columns:
        timestamps = pd.to_datetime(df['timestamp_utc'])
        date_range = (timestamps.max() - timestamps.min()).days
        expected_obs = date_range * 5 / 7  # Approximate business days
        coverage = len(df) / expected_obs
        if coverage < 0.8:
            warnings.append(f"Data coverage {coverage:.1%} - possible gaps")

    return {
        'series_id': series_id,
        'statistics': {
            'min': min_val,
            'max': max_val,
            'mean': mean_val,
            'std': std_val
        } if 'value' in df.columns else {},
        'issues': issues,
        'warnings': warnings,
        'quality_score': 'PASS' if len(issues) == 0 else 'FAIL'
    }


def main():
    print("="*70)
    print("DATA QUALITY VALIDATION: Risk Aversion Indicators")
    print("="*70)

    # Define paths
    bronze_dir = Config.DATA_DIR / "raw" / "fred"
    silver_dir = Config.DATA_DIR / "processed" / "macro"

    # Expected schemas
    bronze_schema = ['date', 'value', 'series_id', 'frequency', 'units', 'source']
    silver_schema = ['timestamp_utc', 'series_id', 'value', 'source', 'frequency', 'units']

    datasets = [
        ('VIXCLS', 'VIX Index'),
        ('BAMLH0A0HYM2', 'US High-Yield Credit Spreads')
    ]

    all_valid = True

    for series_id, description in datasets:
        print(f"\n{'='*70}")
        print(f"Dataset: {description} ({series_id})")
        print(f"{'='*70}")

        # Validate Bronze layer
        print("\n[Bronze Layer Validation]")
        bronze_file = bronze_dir / f"fred_{series_id}_20260220.csv"

        if not bronze_file.exists():
            print(f"❌ File not found: {bronze_file}")
            all_valid = False
            continue

        bronze_df = pd.read_csv(bronze_file)
        bronze_validation = validate_schema(bronze_df, bronze_schema, f"Bronze/{series_id}")

        print(f"  Rows: {bronze_validation['rows']}")
        print(f"  Columns: {', '.join(bronze_validation['columns'])}")

        if bronze_validation['valid']:
            print("  ✅ Schema: VALID")
        else:
            print("  ❌ Schema: INVALID")
            for issue in bronze_validation['issues']:
                print(f"     - {issue}")
            all_valid = False

        # Validate Silver layer
        print("\n[Silver Layer Validation]")
        silver_file = silver_dir / f"macro_{series_id}_2021-01-01_2026-02-20.csv"

        if not silver_file.exists():
            print(f"❌ File not found: {silver_file}")
            all_valid = False
            continue

        silver_df = pd.read_csv(silver_file)
        silver_validation = validate_schema(silver_df, silver_schema, f"Silver/{series_id}")

        print(f"  Rows: {silver_validation['rows']}")
        print(f"  Columns: {', '.join(silver_validation['columns'])}")

        if silver_validation['valid']:
            print("  ✅ Schema: VALID")
        else:
            print("  ❌ Schema: INVALID")
            for issue in silver_validation['issues']:
                print(f"     - {issue}")
            all_valid = False

        # Quality checks
        print("\n[Data Quality Checks]")
        quality = validate_data_quality(silver_df, series_id)

        print(f"  Quality Score: {quality['quality_score']}")
        print("  Statistics:")
        for key, val in quality['statistics'].items():
            print(f"    {key}: {val:.2f}")

        if quality['issues']:
            print("  ❌ Issues:")
            for issue in quality['issues']:
                print(f"     - {issue}")
            all_valid = False
        else:
            print("  ✅ No critical issues")

        if quality['warnings']:
            print("  ⚠️  Warnings:")
            for warning in quality['warnings']:
                print(f"     - {warning}")

        # Compare Bronze vs Silver row counts
        if bronze_validation['rows'] != silver_validation['rows']:
            print(f"\n  ⚠️  Row count mismatch: Bronze={bronze_validation['rows']}, Silver={silver_validation['rows']}")

        # Check date ranges match
        bronze_dates = pd.to_datetime(bronze_df['date'])
        silver_dates = pd.to_datetime(silver_df['timestamp_utc'])

        print("\n  Date Coverage:")
        print(f"    Bronze: {bronze_dates.min().date()} to {bronze_dates.max().date()}")
        print(f"    Silver: {silver_dates.min().date()} to {silver_dates.max().date()}")

    # Final summary
    print(f"\n{'='*70}")
    print("VALIDATION SUMMARY")
    print(f"{'='*70}")

    if all_valid:
        print("✅ All datasets passed validation")
        print("✅ Bronze and Silver layers are schema-compliant")
        print("✅ Data quality checks passed")
        print("\nReady for:")
        print("  - Sentiment Agent feature engineering")
        print("  - Risk-on/risk-off modeling")
        print("  - FX correlation analysis")
    else:
        print("❌ Some datasets failed validation")
        print("⚠️  Review issues above before proceeding")

    print(f"{'='*70}")


if __name__ == "__main__":
    main()
