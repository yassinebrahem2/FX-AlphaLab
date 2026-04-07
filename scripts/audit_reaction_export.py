#!/usr/bin/env python3
"""Quick audit of reaction export schema gap."""

import pandas as pd
from pathlib import Path

print("=" * 80)
print("AUDIT: Reaction Export Schema Gap")
print("=" * 80)
print()

reaction_live_path = Path("data/processed/calendar_reaction_live.csv")
enriched_path = Path("data/processed/calendar_live_scored_reactions.csv")

# Check reaction_live output
if reaction_live_path.exists():
    df_live = pd.read_csv(reaction_live_path)
    print(f"calendar_reaction_live.csv:")
    print(f"  Rows: {len(df_live)}")
    print(f"  Columns ({len(df_live.columns)}): {list(df_live.columns)}")
else:
    print(f"calendar_reaction_live.csv: NOT FOUND")

print()

# Check enriched output
if enriched_path.exists():
    df_enriched = pd.read_csv(enriched_path)
    print(f"calendar_live_scored_reactions.csv:")
    print(f"  Rows: {len(df_enriched)}")
    print(f"  Columns (first 15): {list(df_enriched.columns[:15])}")
    print(f"  Total columns: {len(df_enriched.columns)}")
else:
    print(f"calendar_live_scored_reactions.csv: NOT FOUND")

print()

# Check scored input
scored_candidates = sorted(
    [p for p in Path("data/processed").glob("*.csv") if "scored" in p.stem.lower()],
    key=lambda p: p.stat().st_mtime,
    reverse=True,
)

if scored_candidates:
    df_scored = pd.read_csv(scored_candidates[0])
    print(f"Latest scored input: {scored_candidates[0].name}")
    print(f"  Columns: {list(df_scored.columns)}")
    print()
    print("Column availability in input:")
    print(f"  score_available: {'✓' if 'score_available' in df_scored.columns else '✗'}")
    print(f"  filtered_score: {'✓' if 'filtered_score' in df_scored.columns else '✗'}")
    print(f"  event_type: {'✓' if 'event_type' in df_scored.columns else '✗'}")
    print(f"  impact: {'✓' if 'impact' in df_scored.columns else '✗'}")
    print(f"  final_score: {'✓' if 'final_score' in df_scored.columns else '✗'}")
