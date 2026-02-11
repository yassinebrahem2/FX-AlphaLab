"""Test script to verify all data collection scripts and their arguments.

This script runs each collector with various argument combinations to ensure
they work correctly. Not unit tests - actual script execution.

Usage:
    python scripts/test_collectors.py              # Run all tests
    python scripts/test_collectors.py --quick      # Run quick health checks only
    python scripts/test_collectors.py --script mt5 # Test specific collector
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path


class CollectorTester:
    """Test runner for data collection scripts."""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.results: list[dict] = []
        self.scripts_dir = Path(__file__).parent
        self.project_root = self.scripts_dir.parent

    def run_command(self, script: str, args: list[str], description: str) -> tuple[bool, str]:
        """Run a collection script with given arguments."""
        cmd = [sys.executable, str(self.scripts_dir / script)] + args

        # Add project root to PYTHONPATH
        env = os.environ.copy()
        env["PYTHONPATH"] = str(self.project_root)

        print(f"\n{'=' * 80}")
        print(f"TEST: {description}")
        print(f"CMD:  {' '.join(cmd)}")
        print(f"{'=' * 80}")

        try:
            result = subprocess.run(
                cmd,
                cwd=str(self.project_root),
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout
                env=env,
            )

            success = result.returncode == 0
            output = result.stdout + result.stderr

            if self.verbose or not success:
                print(output)

            status = "✅ PASSED" if success else "❌ FAILED"
            print(f"\n{status} (exit code: {result.returncode})")

            self.results.append(
                {
                    "script": script,
                    "args": args,
                    "description": description,
                    "success": success,
                    "exit_code": result.returncode,
                    "output": output,
                }
            )

            return success, output

        except subprocess.TimeoutExpired:
            print("❌ TIMEOUT (exceeded 5 minutes)")
            self.results.append(
                {
                    "script": script,
                    "args": args,
                    "description": description,
                    "success": False,
                    "exit_code": -1,
                    "output": "TIMEOUT",
                }
            )
            return False, "TIMEOUT"

        except Exception as e:
            print(f"❌ ERROR: {e}")
            self.results.append(
                {
                    "script": script,
                    "args": args,
                    "description": description,
                    "success": False,
                    "exit_code": -1,
                    "output": str(e),
                }
            )
            return False, str(e)

    def test_calendar_collector(self, quick: bool = False):
        """Test calendar collector with various arguments."""
        print("\n" + "=" * 80)
        print("TESTING: Calendar Collector (collect_calendar_data.py)")
        print("=" * 80)

        tests = [
            (["--health-check"], "Health check only"),
        ]

        if not quick:
            today = datetime.now().strftime("%Y-%m-%d")
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

            tests.extend(
                [
                    (["--today"], "Today's events only"),
                    (["--today", "--verbose"], "Today's events with verbose logging"),
                    (["--today", "--countries", "us,eu"], "Today's events for US and EU"),
                    (["--today", "--preprocess"], "Today's events with preprocessing"),
                    (["--start", yesterday, "--end", today], "Custom date range"),
                    (
                        ["--start", yesterday, "--end", today, "--preprocess"],
                        "Custom range with preprocessing",
                    ),
                    (["--countries", "us,uk,jp"], "Specific countries"),
                ]
            )

        for args, desc in tests:
            self.run_command("collect_calendar_data.py", args, desc)

    def test_ecb_collector(self, quick: bool = False):
        """Test ECB collector with various arguments."""
        print("\n" + "=" * 80)
        print("TESTING: ECB Collector (collect_ecb_data.py)")
        print("=" * 80)

        tests = [
            (["--health-check"], "Health check only"),
        ]

        if not quick:
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            end_date = datetime.now().strftime("%Y-%m-%d")

            tests.extend(
                [
                    (["--dataset", "exchange_rates"], "Exchange rates only"),
                    (["--dataset", "policy_rates"], "Policy rates only"),
                    (["--dataset", "all"], "All datasets"),
                    (["--start", start_date, "--end", end_date], "Last 30 days"),
                    (
                        ["--dataset", "exchange_rates", "--preprocess"],
                        "Exchange rates with preprocessing",
                    ),
                    (
                        ["--dataset", "policy_rates", "--preprocess"],
                        "Policy rates with preprocessing",
                    ),
                    (["--start", start_date, "--verbose"], "Custom range with verbose logging"),
                ]
            )

        for args, desc in tests:
            self.run_command("collect_ecb_data.py", args, desc)

    def test_fred_collector(self, quick: bool = False):
        """Test FRED collector with various arguments."""
        print("\n" + "=" * 80)
        print("TESTING: FRED Collector (collect_fred_data.py)")
        print("=" * 80)

        tests = [
            (["--health-check"], "Health check only"),
        ]

        if not quick:
            start_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
            end_date = datetime.now().strftime("%Y-%m-%d")

            tests.extend(
                [
                    ([], "Default collection"),
                    (["--start", start_date, "--end", end_date], "Last 90 days"),
                    (["--preprocess"], "Default with preprocessing"),
                    (["--no-cache"], "Bypass cache"),
                    (["--verbose"], "Verbose logging"),
                    (["--start", start_date, "--preprocess"], "Custom range with preprocessing"),
                ]
            )

        for args, desc in tests:
            self.run_command("collect_fred_data.py", args, desc)

    def test_mt5_collector(self, quick: bool = False):
        """Test MT5 collector with various arguments."""
        print("\n" + "=" * 80)
        print("TESTING: MT5 Collector (collect_mt5_data.py)")
        print("=" * 80)

        tests = [
            (["--health-check"], "Health check only"),
        ]

        if not quick:
            start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            end_date = datetime.now().strftime("%Y-%m-%d")

            tests.extend(
                [
                    (["--pairs", "EURUSD", "--timeframes", "H1"], "Single pair, single timeframe"),
                    (
                        ["--pairs", "EURUSD,GBPUSD", "--timeframes", "H1,D1"],
                        "Multiple pairs and timeframes",
                    ),
                    (["--years", "1"], "1 year of history"),
                    (["--start", start_date, "--end", end_date], "Last 7 days"),
                    (
                        ["--pairs", "EURUSD", "--timeframes", "H1", "--preprocess"],
                        "With preprocessing",
                    ),
                    (
                        ["--pairs", "EURUSD", "--timeframes", "D1", "--years", "1", "--preprocess"],
                        "1 year with preprocessing",
                    ),
                    (["--verbose"], "Verbose logging"),
                ]
            )

        for args, desc in tests:
            self.run_command("collect_mt5_data.py", args, desc)

    def print_summary(self):
        """Print test results summary."""
        print("\n" + "=" * 80)
        print("TEST SUMMARY")
        print("=" * 80)

        total = len(self.results)
        passed = sum(1 for r in self.results if r["success"])
        failed = total - passed

        print(f"\nTotal Tests: {total}")
        print(f"Passed:      {passed} ✅")
        print(f"Failed:      {failed} ❌")
        print(f"Success Rate: {(passed/total*100):.1f}%")

        if failed > 0:
            print("\n" + "-" * 80)
            print("FAILED TESTS:")
            print("-" * 80)
            for r in self.results:
                if not r["success"]:
                    print(f"\n❌ {r['script']} - {r['description']}")
                    print(f"   Args: {' '.join(r['args'])}")
                    print(f"   Exit Code: {r['exit_code']}")
                    if r["output"] and len(r["output"]) < 500:
                        print(f"   Output: {r['output'][:500]}")

        print("\n" + "=" * 80)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Test all data collection scripts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--quick",
        action="store_true",
        help="Run quick health checks only (skip full data collection tests)",
    )

    parser.add_argument(
        "--script",
        type=str,
        choices=["calendar", "ecb", "fred", "mt5"],
        help="Test specific collector only",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show all output (not just failures)",
    )

    args = parser.parse_args()

    tester = CollectorTester(verbose=args.verbose)

    print("=" * 80)
    print("DATA COLLECTOR TEST SUITE")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Mode: {'QUICK (health checks only)' if args.quick else 'FULL'}")
    print("=" * 80)

    # Run tests
    if args.script:
        if args.script == "calendar":
            tester.test_calendar_collector(quick=args.quick)
        elif args.script == "ecb":
            tester.test_ecb_collector(quick=args.quick)
        elif args.script == "fred":
            tester.test_fred_collector(quick=args.quick)
        elif args.script == "mt5":
            tester.test_mt5_collector(quick=args.quick)
    else:
        tester.test_calendar_collector(quick=args.quick)
        tester.test_ecb_collector(quick=args.quick)
        tester.test_fred_collector(quick=args.quick)
        tester.test_mt5_collector(quick=args.quick)

    # Print summary
    tester.print_summary()

    # Exit with appropriate code
    if any(not r["success"] for r in tester.results):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
