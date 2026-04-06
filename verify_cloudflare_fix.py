#!/usr/bin/env python3
"""Verify Cloudflare resilience modifications to ForexFactoryCalendarCollector."""

import inspect

from src.ingestion.collectors.forexfactory_collector import ForexFactoryCalendarCollector

# Check the _fetch_page_with_selenium method source
source = inspect.getsource(ForexFactoryCalendarCollector._fetch_page_with_selenium)

# Verify key changes are present
checks = {
    "Random 2-5s human delay": "stabilization_wait = random.uniform(2.0, 5.0)" in source,
    "Cloudflare detection ('Un instant')": '"un instant" in page_title.lower()' in source,
    "Refresh on challenge": "driver.refresh()" in source,
    "Persistent Cloudflare check": "is_still_blocked" in source,
    "Skip logging": "Skipping month due to persistent anti-bot challenge" in source,
    "Increased scroll delay (0.5-1.0s)": "random.uniform(0.5, 1.0)" in source,
    "15-second wait on challenge": "time.sleep(15)" in source,
    "10-second wait after refresh": "time.sleep(10)" in source,
}

print("=== Cloudflare Resilience Modifications ===\n")
all_passed = True
for check_name, passed in checks.items():
    status = "✓" if passed else "✗"
    print(f"{status} {check_name}")
    if not passed:
        all_passed = False

print(
    f"\n{'✓ All' if all_passed else '✗ Some'} {sum(checks.values())}/{len(checks)} modifications applied"
)

if all_passed:
    print("\n=== Behavior Summary ===")
    print(
        "1. Detects Cloudflare challenges: Looks for 'Un instant', 'Just a moment', 'challenge', or 'cloudflare' indicators"
    )
    print("2. On detection: Waits 15 seconds, refreshes page, waits 10 more seconds")
    print("3. If still blocked: Skips month gracefully (no crash)")
    print("4. Human behavior: Random 2-5s initial delay + 0.5-1.0s scroll delays")
    print("5. Parsing/scoring: Unchanged - only collection resilience improved")
