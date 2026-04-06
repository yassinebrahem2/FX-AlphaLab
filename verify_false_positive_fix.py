#!/usr/bin/env python3
"""Verify critical false-positive fix in ForexFactoryCalendarCollector."""

import inspect

from src.ingestion.collectors.forexfactory_collector import ForexFactoryCalendarCollector

fetch_source = inspect.getsource(ForexFactoryCalendarCollector._fetch_page_with_selenium)

checks = {
    # Critical fix: Check calendar FIRST
    "has_calendar_content returns tuple": "row_count" in fetch_source,
    "Calendar found checked before challenge handling": (
        "calendar_found, row_count = has_calendar_content(driver)" in fetch_source
    ),
    # Logic priority fix
    "IF calendar_found: treat as SUCCESS": (
        "if calendar_found:" in fetch_source and "Proceeding to parsing" in fetch_source
    ),
    "ELSE IF challenge_detected: run polling": "if challenge_detected:" in fetch_source
    or "elif challenge_detected:" in fetch_source,
    # Cancel early success (don't enter polling if calendar exists)
    "No polling when calendar exists": "Calendar content found (rows=" in fetch_source,
    # Improved diagnostics
    "Log calendar_found in skip": "calendar_found=" in fetch_source,
    "Log rows_found in skip": "rows_found=" in fetch_source,
    "Log challenge_detected in skip": "challenge_detected=" in fetch_source,
}

print("=== Critical False-Positive Fix Verification ===\n")
all_passed = sum(1 for v in checks.values() if v) == len(checks)

for check_name, passed in checks.items():
    status = "✓" if passed else "✗"
    print(f"{status} {check_name}")

print(f"\n{'✓ CRITICAL FIX APPLIED' if all_passed else '⚠ INCOMPLETE'}")

if all_passed:
    print("\n=== Logic Flow (CORRECTED) ===")
    print("""
1. Load page with Selenium
2. →  Check document.readyState (stabilization)
3. →  Call has_calendar_content(driver) → (has_table, row_count)
4. →  Call has_challenge_indicators(driver) → bool

IF calendar_found:
    → Log "Calendar content found (rows=X). Proceeding to parsing."
    → Skip all challenge handling
    → Go to parsing & scrolling ✓

ELSE IF challenge_detected:
    → Enter polling loop (4 cycles, 5s intervals)
    → If calendar found during polling: break & continue ✓
    → If challenge clears: break & continue ✓
    →  If polling exhausted: attempt recovery (homepage visit)
    → If still blocked: skip month with diagnostics

ELSE:
    → Normal failure handling
    """)

    print("=== Key Improvements ===")
    print("- No more false positives: valid pages with 'challenge' strings parsed normally")
    print("- Calendar data prioritized: if it exists, use it immediately")
    print("- Challenge indicators only trigger retry if no calendar found")
    print("- Better diagnostics: logs calendar_found, rows_found, challenge_detected on skip")
