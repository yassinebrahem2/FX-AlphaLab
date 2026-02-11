"""Simple script to monitor when rate limit clears."""

import time
from datetime import datetime

import requests


def check_access():
    """Check if we can access the calendar."""
    url = "https://www.investing.com/economic-calendar/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    try:
        response = requests.get(
            url, params={"date": "2026-02-11", "country": "us"}, headers=headers, timeout=30
        )
        return response.status_code
    except Exception as e:
        return f"ERROR: {e}"


def main():
    """Monitor rate limit recovery."""
    print("\n" + "=" * 60)
    print("Rate Limit Recovery Monitor")
    print("=" * 60)
    print(f"Started: {datetime.now().strftime('%H:%M:%S')}")
    print("\nChecking every 30 seconds until access is restored...\n")

    attempt = 0
    while True:
        attempt += 1
        current_time = datetime.now().strftime("%H:%M:%S")
        status = check_access()

        if status == 200:
            print(f"[{current_time}] Attempt {attempt:3d}: ✓ STATUS 200 - ACCESS RESTORED!")
            print("\n" + "=" * 60)
            print("Recovery time: Check logs for initial block time")
            print(f"Recovered at: {current_time}")
            print("=" * 60)
            break
        elif status == 429:
            print(f"[{current_time}] Attempt {attempt:3d}: ✗ Still blocked (429)")
        else:
            print(f"[{current_time}] Attempt {attempt:3d}: ? Status: {status}")

        time.sleep(30)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped by user.")
