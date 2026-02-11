"""Experimental script to determine Investing.com rate limits.

Tests various request patterns to understand:
- Maximum requests per time window
- Optimal delay between requests
- Recovery time after hitting 429
"""

import random
import time
from datetime import datetime

import requests


class RateLimitTester:
    """Test rate limits for Investing.com economic calendar."""

    BASE_URL = "https://www.investing.com/economic-calendar/"

    def __init__(self):
        self.session = requests.Session()
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ]
        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

    def make_request(self, country="us", date="2026-02-11"):
        """Make a single request and return status code."""
        headers = self.headers.copy()
        headers["User-Agent"] = random.choice(self.user_agents)

        params = {"date": date, "country": country}

        try:
            response = self.session.get(self.BASE_URL, params=params, headers=headers, timeout=30)
            return response.status_code, len(response.content)
        except Exception as e:
            return f"ERROR: {e}", 0

    def test_burst_requests(self, num_requests=10, delay=0.5):
        """Test rapid requests with minimal delay."""
        print(f"\n{'='*60}")
        print(f"TEST 1: Burst of {num_requests} requests with {delay}s delay")
        print(f"{'='*60}")

        results = []
        for i in range(num_requests):
            start = time.time()
            status, size = self.make_request(country="us")
            elapsed = time.time() - start

            result = {
                "request": i + 1,
                "status": status,
                "size_kb": size / 1024 if isinstance(size, int) else 0,
                "elapsed": elapsed,
                "timestamp": datetime.now().strftime("%H:%M:%S.%f")[:-3],
            }
            results.append(result)

            print(
                f"  Request {i+1:2d}: Status={status:3d} | "
                f"Size={size/1024:6.1f}KB | "
                f"Time={elapsed:.2f}s | "
                f"@{result['timestamp']}"
            )

            if status == 429:
                print(f"  ⚠️  RATE LIMITED after {i+1} requests!")

            if i < num_requests - 1:
                time.sleep(delay)

        # Summary
        success_count = sum(1 for r in results if r["status"] == 200)
        rate_limited = sum(1 for r in results if r["status"] == 429)
        print(f"\n  Summary: {success_count} successful, {rate_limited} rate-limited")
        return results

    def test_gradual_requests(self, num_requests=10, delay=5.0):
        """Test requests with larger delays."""
        print(f"\n{'='*60}")
        print(f"TEST 2: {num_requests} requests with {delay}s delay")
        print(f"{'='*60}")

        results = []
        for i in range(num_requests):
            start = time.time()
            status, size = self.make_request(country="us")
            elapsed = time.time() - start

            result = {
                "request": i + 1,
                "status": status,
                "size_kb": size / 1024 if isinstance(size, int) else 0,
                "elapsed": elapsed,
                "timestamp": datetime.now().strftime("%H:%M:%S.%f")[:-3],
            }
            results.append(result)

            print(
                f"  Request {i+1:2d}: Status={status:3d} | "
                f"Size={size/1024:6.1f}KB | "
                f"Time={elapsed:.2f}s | "
                f"@{result['timestamp']}"
            )

            if status == 429:
                print(f"  ⚠️  RATE LIMITED after {i+1} requests!")

            if i < num_requests - 1:
                time.sleep(delay)

        # Summary
        success_count = sum(1 for r in results if r["status"] == 200)
        rate_limited = sum(1 for r in results if r["status"] == 429)
        print(f"\n  Summary: {success_count} successful, {rate_limited} rate-limited")
        return results

    def test_recovery_time(self):
        """Test how long it takes to recover from rate limiting."""
        print(f"\n{'='*60}")
        print("TEST 3: Recovery time after rate limit")
        print(f"{'='*60}")

        # Trigger rate limit
        print("\n  Phase 1: Triggering rate limit...")
        for i in range(8):
            status, _ = self.make_request()
            print(f"    Request {i+1}: {status}")
            if status == 429:
                print(f"    ✓ Rate limited after {i+1} requests")
                break
            time.sleep(0.5)

        # Test recovery at intervals
        print("\n  Phase 2: Testing recovery time...")
        recovery_intervals = [10, 20, 30, 45, 60, 90, 120]

        for interval in recovery_intervals:
            print(f"\n    Waiting {interval}s...")
            time.sleep(
                interval
                if not recovery_intervals.index(interval)
                else interval - recovery_intervals[recovery_intervals.index(interval) - 1]
            )

            status, size = self.make_request()
            print(f"    After {interval}s wait: Status={status}, Size={size/1024:.1f}KB")

            if status == 200:
                print(f"    ✓ RECOVERED after ~{interval}s")
                return interval

        print("    ✗ Did not recover within 120s")
        return None

    def test_request_window(self):
        """Test if rate limit is based on sliding window or fixed window."""
        print(f"\n{'='*60}")
        print("TEST 4: Request window type (sliding vs fixed)")
        print(f"{'='*60}")

        print("\n  Making 5 requests spaced at 15s intervals over 60s window...")
        results = []

        for i in range(5):
            timestamp = datetime.now().strftime("%H:%M:%S")
            status, size = self.make_request()
            results.append((timestamp, status, size))
            print(f"    Request {i+1} @{timestamp}: Status={status}")

            if i < 4:
                time.sleep(15)

        print("\n  Immediate follow-up request (tests if window is sliding)...")
        timestamp = datetime.now().strftime("%H:%M:%S")
        status, size = self.make_request()
        print(f"    Request 6 @{timestamp}: Status={status}")

        if status == 429:
            print("    → Likely SLIDING WINDOW (still blocked despite 60s elapsed)")
        else:
            print("    → Likely FIXED WINDOW (allowed after 60s)")

        return results


def main():
    """Run all rate limit tests."""
    print("\n" + "=" * 60)
    print("Investing.com Rate Limit Testing")
    print("=" * 60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    tester = RateLimitTester()

    # Test 1: Burst requests
    input("\nPress Enter to start TEST 1 (burst requests)...")
    tester.test_burst_requests(num_requests=10, delay=1.0)

    # Test 2: Gradual requests
    input("\nPress Enter to start TEST 2 (gradual requests)...")
    tester.test_gradual_requests(num_requests=10, delay=8.0)

    # Test 3: Recovery time
    input("\nPress Enter to start TEST 3 (recovery time)...")
    recovery = tester.test_recovery_time()

    # Test 4: Window type
    input("\nPress Enter to start TEST 4 (window type)...")
    tester.test_request_window()

    print("\n" + "=" * 60)
    print("All tests complete!")
    print("=" * 60)

    if recovery:
        print(f"\n✓ Recovery time: ~{recovery}s")
    print("\nAnalyze the results above to determine optimal rate limiting strategy.")


if __name__ == "__main__":
    main()
