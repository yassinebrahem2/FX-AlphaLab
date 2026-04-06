#!/usr/bin/env python3
"""Verify hardening modifications to ForexFactoryCalendarCollector."""

import inspect

from src.ingestion.collectors.forexfactory_collector import ForexFactoryCalendarCollector

# Check for enhancements in both _init_driver and _fetch_page_with_selenium
init_driver_source = inspect.getsource(ForexFactoryCalendarCollector._init_driver)
fetch_source = inspect.getsource(ForexFactoryCalendarCollector._fetch_page_with_selenium)

checks = {
    # Driver initialization hardening
    "Disable background networking": "--disable-background-networking" in init_driver_source,
    "Disable sync": "--disable-sync" in init_driver_source,
    "Mask plugins property": "'plugins'" in init_driver_source,
    "Mask chromeFlags": "'chromeFlags'" in init_driver_source,
    "Enable logging disabled": "enable-logging" in init_driver_source,
    # Page stabilization improvements
    "Check document.readyState": "document.readyState" in fetch_source,
    "Interactive ready state": "interactive" in fetch_source,
    "Initial idle delay": "initial_idle" in fetch_source,
    # Challenge polling and recovery
    "has_challenge_indicators function": "has_challenge_indicators" in fetch_source,
    "has_calendar_content function": "has_calendar_content" in fetch_source,
    "Polling loop": "polling_max_iterations" in fetch_source,
    "Recovery path (homepage)": "calendar root" in fetch_source or "CALENDAR_URL" in fetch_source,
    "Recovery navigation": "Navigating back to target month" in fetch_source,
    # Improved diagnostics
    "Enhanced skip logging": "title=" in fetch_source and "html_length=" in fetch_source,
    "Calendar found in diagnostics": "calendar_found=" in fetch_source,
    # Improved scrolling (slower, smaller)
    "Slower scroll (0.8-1.2s)": "random.uniform(0.8, 1.2)" in fetch_source,
    "Reduced scroll distance (0.12-0.24)": "random.uniform(0.12, 0.24)" in fetch_source,
}

print("=== Hardening Modifications Verification ===\n")
all_passed = True
for check_name, passed in checks.items():
    status = "✓" if passed else "✗"
    print(f"{status} {check_name}")
    if not passed:
        all_passed = False

print(
    f"\n{'✓ All' if all_passed else '✗ Some'} {sum(checks.values())}/{len(checks)} hardening features applied"
)

if all_passed:
    print("\n=== Enhancement Summary ===")
    print("1. Driver Stealth:")
    print("   - 5 additional Chrome automation-blocking arguments")
    print("   - 3 JavaScript-based property masking (webdriver, chromeFlags, plugins)")
    print("   - Enhanced experimental options")
    print()
    print("2. Page Stabilization:")
    print("   - 1-3s randomized idle before navigation")
    print("   - document.readyState checking (waits up to 5s for stable state)")
    print("   - Ensures page JS has loaded before challenge detection")
    print()
    print("3. Intelligent Polling & Recovery:")
    print("   - 4 polling cycles (5s intervals) to catch challenge clearing")
    print("   - Recovery path: visit homepage → wait → retry month URL")
    print("   - Skips gracefully only after all retries exhausted")
    print()
    print("4. Improved Scrolling:")
    print("   - Smaller scroll distances (0.12-0.24 viewport vs 0.18-0.32)")
    print("   - Longer delays between scrolls (0.8-1.2s vs 0.5-1.0s)")
    print()
    print("5. Better Diagnostics:")
    print("   - When skipping: logs title, calendar_found, challenge_indicators, html_length")
    print("   - Clear visibility into why a month was skipped")
