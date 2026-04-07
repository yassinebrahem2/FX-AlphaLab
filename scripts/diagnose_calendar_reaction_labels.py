#!/usr/bin/env python3
"""
Phase 17: Data Sufficiency Diagnosis
Diagnose why calendar_reaction_live.csv has sparse usable validation labels.

Inspects only the reaction dataset for direction_match_1h/4h coverage.
No fixes, no notebook changes, terminal output only.
"""

from pathlib import Path
import pandas as pd
import sys


def diagnose_calendar_reactions():
    """Main diagnostic workflow."""
    project_root = Path(__file__).parent.parent
    reaction_file = project_root / "data" / "processed" / "calendar_reaction_live.csv"
    
    print("=" * 80)
    print("CALENDAR REACTION LABELS DIAGNOSIS")
    print("=" * 80)
    print()
    
    # A) File existence + shape
    print("A) FILE EXISTENCE & SHAPE")
    print("-" * 80)
    if not reaction_file.exists():
        print(f"❌ File not found: {reaction_file}")
        return
    
    try:
        df = pd.read_csv(reaction_file)
    except Exception as e:
        print(f"❌ Failed to read CSV: {e}")
        return
    
    print(f"✓ File exists: {reaction_file}")
    print(f"  Rows: {len(df)}")
    print(f"  Columns: {df.shape[1]}")
    print(f"  Column list: {list(df.columns)}")
    print()
    
    # B) Critical column presence audit
    print("B) CRITICAL COLUMN PRESENCE AUDIT")
    print("-" * 80)
    critical_cols = [
        "score_available",
        "filtered_score",
        "final_score",
        "direction_match_1h",
        "direction_match_4h",
        "expected_direction",
        "observed_direction_1h",
        "observed_direction_4h",
        "return_1h",
        "return_4h",
        "price_before_event",
        "price_at_event",
        "price_1h_after",
        "price_4h_after",
        "currency",
        "event_type",
        "impact",
    ]
    
    present = {col: col in df.columns for col in critical_cols}
    missing_cols = [col for col, exists in present.items() if not exists]
    
    print("Present columns:")
    for col in critical_cols:
        if present[col]:
            print(f"  ✓ {col}")
    
    if missing_cols:
        print("\nMissing columns:")
        for col in missing_cols:
            print(f"  ✗ {col}")
    else:
        print("\n✓ All critical columns present")
    print()
    
    # C) Non-null coverage by critical field
    print("C) NON-NULL COVERAGE BY CRITICAL FIELD")
    print("-" * 80)
    for col in critical_cols:
        if col in df.columns:
            non_null = df[col].notna().sum()
            null_count = df[col].isna().sum()
            null_rate = 100 * null_count / len(df) if len(df) > 0 else 0
            print(f"{col:30s} | non-null: {non_null:4d} | null: {null_count:4d} ({null_rate:5.1f}%)")
    print()
    
    # D) Usable-row funnel
    print("D) USABLE-ROW FUNNEL")
    print("-" * 80)
    
    def count_rows(mask, label):
        count = mask.sum() if isinstance(mask, pd.Series) else len(df[mask])
        pct = 100 * count / len(df) if len(df) > 0 else 0
        print(f"{label:50s} {count:4d} rows ({pct:5.1f}%)")
        return count
    
    count_rows(pd.Series([True] * len(df)), "1. Total rows")
    
    if "score_available" in df.columns:
        score_avail_mask = df["score_available"] == True
        count_rows(score_avail_mask, "2. score_available == True")
    else:
        score_avail_mask = pd.Series([True] * len(df))
    
    if "final_score" in df.columns:
        final_score_mask = score_avail_mask & df["final_score"].notna()
        count_rows(final_score_mask, "3. + non-null final_score")
    else:
        final_score_mask = score_avail_mask
    
    if "filtered_score" in df.columns:
        filtered_score_mask = final_score_mask & df["filtered_score"].notna()
        count_rows(filtered_score_mask, "4. + non-null filtered_score")
    else:
        filtered_score_mask = final_score_mask
    
    # Usable 1h: direction_match_1h exists and non-null
    if "direction_match_1h" in df.columns:
        usable_1h_mask = filtered_score_mask & df["direction_match_1h"].notna()
        usable_1h_count = count_rows(usable_1h_mask, "5. + non-null direction_match_1h")
    else:
        usable_1h_count = count_rows(filtered_score_mask & False, "5. + non-null direction_match_1h (column missing)")
        usable_1h_mask = pd.Series([False] * len(df))
    
    # Usable 4h: direction_match_4h exists and non-null
    if "direction_match_4h" in df.columns:
        usable_4h_mask = filtered_score_mask & df["direction_match_4h"].notna()
        usable_4h_count = count_rows(usable_4h_mask, "6. + non-null direction_match_4h")
    else:
        usable_4h_count = count_rows(filtered_score_mask & False, "6. + non-null direction_match_4h (column missing)")
        usable_4h_mask = pd.Series([False] * len(df))
    
    # Segmentation-usable: has currency/event_type/impact for later grouping
    segmentation_mask = final_score_mask.copy()
    for col in ["currency", "event_type", "impact"]:
        if col in df.columns:
            segmentation_mask = segmentation_mask & df[col].notna()
    
    count_rows(segmentation_mask, "7. + non-null currency/event_type/impact")
    print()
    
    # E) Root-cause breakdown
    print("E) ROOT-CAUSE BREAKDOWN")
    print("-" * 80)
    
    def breakdown_1h(df, final_score_mask_in):
        """Classify rows by why they lack usable 1h labels."""
        subset = df[final_score_mask_in].copy()
        if len(subset) == 0:
            print("  (no rows with final_score to analyze)")
            return
        
        buckets = {
            "Missing direction_match_1h column": 0,
            "direction_match_1h column exists but null": 0,
            "Missing expected_direction": 0,
            "Missing observed_direction_1h": 0,
            "Missing return_1h": 0,
            "Missing price_before/at/1h_after": 0,
            "Fully usable": 0,
            "Other": 0,
        }
        
        for idx, row in subset.iterrows():
            if "direction_match_1h" not in df.columns:
                buckets["Missing direction_match_1h column"] += 1
            elif pd.isna(row.get("direction_match_1h")):
                # direction_match is null; check if we can infer why
                if pd.isna(row.get("expected_direction")):
                    buckets["Missing expected_direction"] += 1
                elif pd.isna(row.get("observed_direction_1h")):
                    buckets["Missing observed_direction_1h"] += 1
                elif pd.isna(row.get("return_1h")):
                    buckets["Missing return_1h"] += 1
                elif any(pd.isna(row.get(col)) for col in ["price_before_event", "price_at_event", "price_1h_after"]):
                    buckets["Missing price_before/at/1h_after"] += 1
                else:
                    buckets["Other"] += 1
            else:
                buckets["Fully usable"] += 1
        
        for label, count in buckets.items():
            if count > 0:
                pct = 100 * count / len(subset)
                print(f"  {label:45s} {count:4d} ({pct:5.1f}%)")
    
    def breakdown_4h(df, final_score_mask_in):
        """Classify rows by why they lack usable 4h labels."""
        subset = df[final_score_mask_in].copy()
        if len(subset) == 0:
            print("  (no rows with final_score to analyze)")
            return
        
        buckets = {
            "Missing direction_match_4h column": 0,
            "direction_match_4h column exists but null": 0,
            "Missing expected_direction": 0,
            "Missing observed_direction_4h": 0,
            "Missing return_4h": 0,
            "Missing price_before/at/4h_after": 0,
            "Fully usable": 0,
            "Other": 0,
        }
        
        for idx, row in subset.iterrows():
            if "direction_match_4h" not in df.columns:
                buckets["Missing direction_match_4h column"] += 1
            elif pd.isna(row.get("direction_match_4h")):
                # direction_match is null; check if we can infer why
                if pd.isna(row.get("expected_direction")):
                    buckets["Missing expected_direction"] += 1
                elif pd.isna(row.get("observed_direction_4h")):
                    buckets["Missing observed_direction_4h"] += 1
                elif pd.isna(row.get("return_4h")):
                    buckets["Missing return_4h"] += 1
                elif any(pd.isna(row.get(col)) for col in ["price_before_event", "price_at_event", "price_4h_after"]):
                    buckets["Missing price_before/at/4h_after"] += 1
                else:
                    buckets["Other"] += 1
            else:
                buckets["Fully usable"] += 1
        
        for label, count in buckets.items():
            if count > 0:
                pct = 100 * count / len(subset)
                print(f"  {label:45s} {count:4d} ({pct:5.1f}%)")
    
    print("1h Validation Labels:")
    breakdown_1h(df, final_score_mask)
    print()
    print("4h Validation Labels:")
    breakdown_4h(df, final_score_mask)
    print()
    
    # F) Sample problematic rows
    print("F) SAMPLE PROBLEMATIC ROWS (1h)")
    print("-" * 80)
    
    sample_cols = [
        "event_id", "currency", "event_type", "impact",
        "score_available", "final_score", "filtered_score",
        "expected_direction", "observed_direction_1h",
        "return_1h", "direction_match_1h",
        "price_before_event", "price_at_event", "price_1h_after"
    ]
    sample_cols = [col for col in sample_cols if col in df.columns]
    
    if len(final_score_mask) > 0:
        # Pick 5-10 problematic rows (where validation should exist but doesn't)
        problem_subset = df[final_score_mask].copy()
        
        if len(problem_subset) > 0:
            # Show first up to 10 rows
            display_count = min(10, len(problem_subset))
            display_df = problem_subset.iloc[:display_count][sample_cols]
            
            print(display_df.to_string())
        else:
            print("(no rows with final_score)")
    else:
        print("(no rows qualify for analysis)")
    print()
    
    # G) Final diagnosis summary
    print("G) FINAL DIAGNOSIS SUMMARY")
    print("-" * 80)
    
    # Identify top blockers
    blockers = []
    
    if len(df) == 0:
        blockers.append("Empty dataset (0 rows)")
    elif "score_available" in df.columns and (df["score_available"] != True).sum() == 0:
        blockers.append("All rows have score_available == False")
    elif "final_score" in df.columns and df["final_score"].isna().sum() == len(df):
        blockers.append("All final_score values are null")
    elif "direction_match_1h" not in df.columns and "direction_match_4h" not in df.columns:
        blockers.append("Both direction_match_1h and direction_match_4h columns missing")
    elif "direction_match_1h" in df.columns and df["direction_match_1h"].isna().all():
        blockers.append("direction_match_1h column exists but all values are null")
    elif "expected_direction" in df.columns and df["expected_direction"].isna().all():
        blockers.append("expected_direction column all null (cannot compute expected signals)")
    elif "observed_direction_1h" in df.columns and df["observed_direction_1h"].isna().all():
        blockers.append("observed_direction_1h column all null (cannot compute observed signals)")
    elif "return_1h" in df.columns and df["return_1h"].isna().all():
        blockers.append("return_1h column all null (cannot infer direction from price returns)")
    elif usable_1h_count + usable_4h_count == 0:
        blockers.append("Zero usable direction_match rows after filtering for score_available & final_score")
    else:
        blockers.append("Labels sparse but some present; check segmentation-usable rows")
    
    print("Top blockers (reasons labels are sparse):")
    for i, blocker in enumerate(blockers[:3], 1):
        print(f"{i}. {blocker}")
    print()
    
    # Schema vs data quality assessment
    schema_missing = len(missing_cols)
    if schema_missing >= 5:
        schema_verdict = "SCHEMA ISSUE: Multiple critical columns missing from dataset"
    elif schema_missing >= 2:
        schema_verdict = "PARTIAL SCHEMA: Some validation columns missing"
    else:
        schema_verdict = "Schema intact: All validation columns present"
    
    all_null_cols = [col for col in critical_cols if col in df.columns and df[col].isna().all()]
    if len(all_null_cols) >= 3:
        data_verdict = "DATA QUALITY ISSUE: Multiple columns all-null (upstream generation failure)"
    elif len(all_null_cols) >= 1:
        data_verdict = "PARTIAL DATA: Some validation columns all-null"
    else:
        data_verdict = "Data quality: Columns have mix of null/non-null values"
    
    print(f"Schema assessment: {schema_verdict}")
    print(f"Data quality assessment: {data_verdict}")
    print()
    
    # Next steps recommendation
    print("What should be fixed next:")
    if schema_missing >= 2:
        print("  1. [SCHEMA] Extend run_live_calendar_reaction.py to compute missing direction_match columns")
    if len(all_null_cols) >= 1:
        print("  2. [UPSTREAM] Debug calendar event ingestion and reaction matching to avoid all-null fields")
    if usable_1h_count + usable_4h_count < 10:
        print("  3. [DATA] Collect more reaction data or lower validation filter thresholds")
    print()
    print("=" * 80)


if __name__ == "__main__":
    diagnose_calendar_reactions()
