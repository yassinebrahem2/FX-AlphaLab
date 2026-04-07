#!/usr/bin/env python3
"""
Phase 17: Reaction Export Audit
Detailed analysis of why calendar_reaction_live.csv is missing notebook-required columns.
"""

print("=" * 100)
print("AUDIT: run_live_calendar_reaction.py — Export Schema Gap")
print("=" * 100)
print()

# QUESTION A: Does it read upstream fields?
print("A) UPSTREAM FIELD AVAILABILITY (From scored calendar input)")
print("-" * 100)
print("""
Input: calendar_live_scored_reactions.csv (scored calendar events)
Columns present: ✓ score_available, ✓ filtered_score, ✓ event_type, ✓ impact, ✓ final_score

Script action: load_latest_scored_calendar()
  ✓ Reads all columns from scored input including score_available, filtered_score, event_type, impact
  ✓ Stores in 'events' dataframe with full metadata
  ✓ Validates that score_available column exists

Filtering: events[events["score_available"] == True]
  ✓ Uses score_available to filter scoreable events
  ✓ After filtering: 1 event matches for live data

VERDICT: ✓ All upstream metadata fields ARE available in events dataframe.
""")
print()

# QUESTION B: Does it compute direction fields?
print("B) DIRECTION FIELD COMPUTATION")
print("-" * 100)
print("""
Script computes these functions for 1h window:
  ✓ get_expected_direction(final_score)
    - Returns: UP (>0.1), DOWN (<-0.1), NEUTRAL (else), or None
    - Applied to final_score from input events
  
  ✓ get_observed_direction(return_1h)
    - Returns: UP (>0.1%), DOWN (<-0.1%), NEUTRAL (else), or None
    - Applied to return_1h computed from prices[price_at_event to price_1h_after]
  
  ✓ direction_match(expected, observed)
    - Returns: True (match), False (diverge), None (missing data or NEUTRAL)
    - Matches expected_direction to observed_direction (1h only)

Script does NOT compute separate 4h fields:
  ✗ observed_direction_4h: Missing (only observed_direction for 1h exists)
  ✗ direction_match_4h: Missing (only direction_match for 1h exists)
  
Why not computed:
  - get_observed_direction() is called only with return_1h
  - Would need separate call with return_4h to get observed_direction_4h
  - direction_match() is called only once per event, with 1h direction pair

VERDICT: ✓ Computes 1h correctly. ✗ Does NOT compute 4h variants.
""")
print()

# QUESTION C: Where are fields lost?
print("C) COLUMN SELECTION & EXPORT (Where fields are dropped)")
print("-" * 100)
print("""
Stage 1: Compute per-event into result_row dict (lines ~270-290)
  Columns ADDED to result_row:
    ✓ event_id, timestamp_utc, event_name, currency, final_score
    ✓ pair, price_before_event, price_at_event, price_1h_after, price_4h_after
    ✓ matched_price_timestamp_* (before, at, 1h, 4h windows)
    ✓ return_1h, return_4h, reaction_explanation
    ✓ expected_direction, observed_direction, direction_match
  
  Columns FROM events THAT ARE NOT ADDED to result_row:
    ✗ score_available (not included)
    ✗ filtered_score (not included)
    ✗ event_type (not included)
    ✗ impact (not included)

Stage 2: Create DataFrame & Export (lines ~291-298)
  CURRENT (recent commit): 
    result_df created with 20 columns (as above)
    enriched_df = events.merge(result_df[7 columns], on="event_id", how="left")
    → enriched_df saved to calendar_live_scored_reactions.csv (35 rows, 40 columns)
    → result_df NOT saved to calendar_reaction_live.csv
    
  LEGACY (stale file):
    calendar_reaction_live.csv exists with result_df from old version
    → File is obsolete; script no longer exports this format

VERDICT: Fields are lost in result_row construction (selective column extraction).
""")
print()

# QUESTION D: What inputs are missing?
print("D) ROOT CAUSE: Missing Input Fields vs Missing Computation")
print("-" * 100)
print("""
For expected_direction computation:
  ✓ final_score available in events
  ✓ get_expected_direction() function exists
  ✓ Result: expected_direction computed correctly

For observed_direction computation:
  ✓ return_1h computed from prices (price_at_event → price_1h_after)
  ✓ get_observed_direction() function exists
  ✗ ONLY applied to return_1h (1h window)
  ✗ NOT applied to return_4h (4h window missing)
  → Result: observed_direction only for 1h

For direction_match computation:
  ✓ Both expected_direction and observed_direction available
  ✗ direction_match() only called once per event
  ✗ Called with (expected_direction, observed_direction_1h) only
  ✗ NOT called with (expected_direction, observed_direction_4h)
  → Result: direction_match only for 1h

For segmentation metadata:
  ✓ event_type available in events (not added to result_row)
  ✓ impact available in events (not added to result_row)
  ✗ These fields would enable grouping/segmentation in notebook
  → Result: Notebook cannot segment by event_type or impact

VERDICT: 
  - Computation logic exists and works for 1h
  - 4h variants MISSING due to incomplete loop logic, not missing inputs
  - Segmentation fields LOST in selective result_row construction
""")
print()

# QUESTION E: Why only 1 row in calendar_reaction_live.csv?
print("E) ROW COUNT ANALYSIS (Why only 1 event qualified)")
print("-" * 100)
print("""
Filtering pipeline:

1. Load scored calendar:
   Input: calendar_live_scored_20260306_20260406.csv
   Rows: ~100+ events
   Filter: score_available == True
   → After filter: 35 rows (from enriched_df row count)

2. Currency mapping:
   Filter: currency_norm must map to supported pair (PAIR_MAP)
   PAIR_MAP: {USD: EURUSD, EUR: EURUSD, GBP: GBPUSD, JPY: USDJPY, CHF: USDCHF, ES/DE/FR/IT/EU: EURUSD}
   → After filter: Unknown (depends on input currency distribution)

3. Price file existence:
   Requirement: MT5 file must exist for matched pair
   Pattern: data/raw/mt5/mt5_{PAIR}_H1_*.csv
   Check: load_prices(pair) returns non-None
   Status: EURUSD file exists, others may be missing or stale
   → After filter: Likely ~35 potential matches, but most pairs lack data

4. Price coverage validation:
   Requirement 1: has_bar_before = at least 1 bar before event timestamp
   Requirement 2: has_bar_at_or_after = at least 1 bar at/after event
   Requirement 3: has_through_4h = price data extends to event+4h (warning if not)
   Status: Very restrictive when combined with live data age
   → After filter: 1 event (current result) 
   → Remaining: 34 events in enriched_df likely have NULL reaction fields

VERDICT: 
  - Data filtering is conservative (requires complete pre/at/post coverage)
  - Only paired/recent events with matching MT5 data qualify
  - Currently 1 event passes all checks → result_df has 1 row
  - 34 other events in enriched_df are preserved but mark reactions NULL
""")
print()

# FINAL SUMMARY
print("=" * 100)
print("F) FINAL DIAGNOSIS SUMMARY")
print("=" * 100)
print()

print("Why notebook has sparse validation labels:")
print()
print("1. MISSING COLUMNS IN OUTPUT:")
print("   └─ direction_match_1h/4h: Script computes as single 'direction_match' (1h only)")
print("   └─ observed_direction_1h/4h: Script computes as single 'observed_direction' (1h only)")
print("   └─ score_available, filtered_score: Available in input but not in result_row")
print("   └─ event_type, impact: Available in input but not in result_row")
print()

print("2. SCHEMA ISSUE (Primary Blocker):")
print("   Script exports result_df-derived columns but is outdated/superseded.")
print("   Recent commit switched to enriched_df (events merged with reactions),")
print("   but notebook expects raw result_df format with all computed fields.")
print()

print("3. COMPUTATION ISSUE (Secondary):")
print("   For 1h window: ✓ Full pipeline works (expected, observed, match)")
print("   For 4h window: ✗ Only return_4h computed; observed_direction_4h and")
print("                    direction_match_4h never computed.")
print()

print("4. DATA VOLUME:")
print("   Input: 35 scoreable events after filtering for score_available=True")
print("   Output: 1 row qualified for live reaction (strict price coverage check)")
print("   Gap: 34 events have no reaction data (price bars missing for live window)")
print()

print("What should be fixed next (HIGH LEVEL):")
print()
print("[FIX 1 - Schema]: Restore result_df export to calendar_reaction_live.csv")
print("   OR update notebook schema to match current enriched_df format")
print()
print("[FIX 2 - Metadata]: Add score_available, filtered_score, event_type, impact to result_row")
print()
print("[FIX 3 - 4h Direction]: Compute observed_direction_4h and direction_match_4h")
print("   in the main loop alongside 1h variants")
print()
print("[FIX 4 - Data Volume]: Relax price coverage requirements or collect richer MT5 data")
print()

print("=" * 100)
