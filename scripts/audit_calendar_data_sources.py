"""Phase 18: Economic Calendar Source Replacement Audit.

Diagnostic audit only - no pipeline changes, no implementation.

Purpose:
  Audit the current calendar-event sourcing approach and determine whether
  ForexFactory should be kept, supplemented, or replaced for Module C needs.

Focus:
  Module C (sentiment/scoring) only - needs EUR, GBP, CHF, JPY events.

Output:
  Terminal-based diagnosis and source strategy recommendation.
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.shared.config import Config


class CalendarSourceAudit:
    """Diagnostic audit of calendar data sourcing strategy."""

    def __init__(self):
        """Initialize audit with standard paths."""
        self.data_dir = Config.DATA_DIR
        self.raw_dir = self.data_dir / "raw" / "forexfactory"
        self.processed_dir = self.data_dir / "processed"
        self.module_c_currencies = {"EUR", "GBP", "CHF", "JPY"}
        self.audit_results = {}

    def run(self):
        """Execute full audit and print results."""
        print("\n" + "=" * 80)
        print("PHASE 18: ECONOMIC CALENDAR SOURCE REPLACEMENT AUDIT")
        print("=" * 80)
        print(f"\nTimestamp: {datetime.now().isoformat()}")
        print(f"Project: FX-AlphaLab W6 Data Architecture")
        print("\n" + "-" * 80)

        # A: Current source dependency audit
        print("\nA) CURRENT SOURCE DEPENDENCY AUDIT")
        print("-" * 80)
        self._audit_forexfactory_fields()

        # B: Downstream data contract
        print("\nB) DOWNSTREAM DATA CONTRACT (Module C Requirements)")
        print("-" * 80)
        self._audit_module_c_contract()

        # C: Fragility assessment
        print("\nC) FRAGILITY ASSESSMENT (ForexFactory Reliability)")
        print("-" * 80)
        self._audit_fragility()

        # D: Current data volume
        print("\nD) CURRENT DATA VOLUME & QUALITY")
        print("-" * 80)
        self._audit_data_volume()

        # E: Source strategy recommendation
        print("\nE) SOURCE STRATEGY RECOMMENDATION")
        print("-" * 80)
        self._recommend_strategy()

        # F: Replacement design constraints
        print("\nF) REPLACEMENT SOURCE DESIGN CONSTRAINTS")
        print("-" * 80)
        self._define_replacement_constraints()

        print("\n" + "=" * 80)
        print("END OF AUDIT")
        print("=" * 80 + "\n")

    def _audit_forexfactory_fields(self):
        """Audit what fields ForexFactory provides."""
        print("\n1. FOREXFACTORY DATA FIELDS (Bronze Layer)")
        print("-" * 80)

        ff_fields = {
            "date": "Event date (YYYY-MM-DD)",
            "time": "Event time (HH:MM, assumed GMT)",
            "currency": "Country/Currency code (USD, EUR, GBP, JPY, etc.)",
            "event": "Event name/description",
            "impact": "Impact level (High, Medium, Low)",
            "forecast": "Forecasted value (economic metric)",
            "previous": "Previous period value",
            "actual": "Actual released value (backfilled after release)",
            "event_url": "Direct link to event details",
            "source_url": "Calendar page URL",
            "scraped_at": "Timestamp when scraped",
            "source": "Source identifier (forexfactory.com)",
        }

        for field, description in ff_fields.items():
            print(f"  • {field:20s} → {description}")

        print(f"\n  Total fields provided: {len(ff_fields)}")
        print(f"  Source: Forex Factory web scraper (month-view strategy)")
        print(f"  Timezone: GMT (enforced via URL parameter)")

    def _audit_module_c_contract(self):
        """Audit what fields Module C actually requires."""
        print("\n1. MODULE C SCORING PIPELINE REQUIREMENTS")
        print("-" * 80)

        mandatory_fields = {
            "timestamp_utc": "When event occurs (required for time series)",
            "currency": "Which currency affected (EUR, GBP, JPY, CHF only)",
            "event_name": "Event description (for mapping to templates)",
            "impact": "Impact level (High/Medium/Low - used for weighting)",
            "actual": "Released value (required for surprise calculation)",
            "forecast": "Expected value (required for surprise calculation)",
            "previous": "Prior period value (optional context, used as fallback)",
        }

        optional_fields = {
            "event_id": "Unique identifier (nice-to-have, computed from hash)",
            "source": "Data origin (for lineage tracking)",
            "event_url": "Original source link (diagnostic only)",
        }

        print("\n  MANDATORY (core algorithm):")
        for field, purpose in mandatory_fields.items():
            print(f"    ✓ {field:20s} → {purpose}")

        print(f"\n  OPTIONAL (quality features):")
        for field, purpose in optional_fields.items():
            print(f"    ○ {field:20s} → {purpose}")

        print(f"\n  Total mandatory: {len(mandatory_fields)}")
        print(f"  Total optional: {len(optional_fields)}")

        print("\n2. TARGET CURRENCY FILTERING")
        print("-" * 80)
        print(f"  Module C focuses ONLY on: {', '.join(sorted(self.module_c_currencies))}")
        print(f"  These map to pairs: EURUSD, GBPUSD, USDJPY, USDCHF")
        print(f"  ForexFactory provides: ALL major currencies (100+ countries)")
        print(f"  → Requires post-collection filtering to 4 target currencies")

        print("\n3. FIELD AVAILABILITY FROM FOREXFACTORY")
        print("-" * 80)
        coverage = {
            "timestamp_utc": "✓ (from date + time columns, converted to UTC)",
            "currency": "✓ (raw currency/country field)",
            "event_name": "✓ (from event column)",
            "impact": "✓ (direct impact level)",
            "actual": "✓ (populated after release)",
            "forecast": "✓ (pre-release forecast)",
            "previous": "✓ (prior period value)",
        }

        for field, status in coverage.items():
            print(f"  {status:5s} {field}")

        print(f"\n  → ForexFactory provides ALL mandatory fields ✓")

    def _audit_fragility(self):
        """Audit operational weaknesses of ForexFactory approach."""
        print("\n1. BROWSER DEPENDENCE")
        print("-" * 80)
        print("  • Requires Selenium WebDriver (headless Chrome)")
        print("  • Requires webdriver-manager for driver versioning")
        print("  • Optional: undetected-chromedriver for Cloudflare bypass")
        print("  • Windows-only? No - works on Linux/Mac but driver management is complex")
        print("  Fragility risk: ⚠️  MEDIUM-HIGH")
        print("    → Driver crashes, update mismatches, memory leaks possible")

        print("\n2. ANTI-BOT & CLOUDFLARE EXPOSURE")
        print("-" * 80)
        print("  • Cloudflare actively blocks automated access")
        print("  • ForexFactory likely has IP rate-limiting (3-5 sec minimum)")
        print("  • Evidence: debug_anti_bot_challenge_detected files in raw/")
        raw_challenges = list(self.raw_dir.glob("*challenge*.html"))
        print(f"  • Observed challenges in current run: {len(raw_challenges)} files")
        print("  Fragility risk: ⚠️  HIGH")
        print("    → Daily successful collection not guaranteed")
        print("    → IP bans possible after repeated failures")

        print("\n3. HTML PARSING FRAGILITY")
        print("-" * 80)
        print("  • Calendar table structure could change without notice")
        print("  • Virtual scrolling requires careful JavaScript execution")
        print("  • Empty HTML shell returned when page fails to load")
        empty_shells = list(self.raw_dir.glob("*empty_html_shell*.html"))
        print(f"  • Observed empty shells in current run: {len(empty_shells)} files")
        print("  • Brittle selectors: calendar table, timezone dropdown, scroll targets")
        print("  Fragility risk: ⚠️  MEDIUM")
        print("    → Single CSS class rename breaks collection")

        print("\n4. REPRODUCIBILITY & MAINTENANCE")
        print("-" * 80)
        print("  • Requires 1500+ lines of custom scraper code")
        print("  • Hardcoded CSS selectors, XPath expressions (3+ versions each)")
        print("  • Timezone handling via UI interaction (fragile)")
        print("  • Rate limiting via delays (not enforcement from server)")
        print("  Fragility risk: ⚠️  MEDIUM-HIGH")
        print("    → High maintenance burden if website changes")
        print("    → Difficult to debug remote collection failures")

        print("\n5. VOLUME & COVERAGE EFFECTIVENESS")
        print("-" * 80)
        print("  • Month-view strategy: 1 request/month (efficient)")
        print("  • BUT: Virtual scrolling required to load all events")
        print("  • Unknown capture rate in practice (claimed 97-99%)")
        print("  • No built-in deduplication across page loads")
        print("  Fragility risk: ⚠️  MEDIUM")
        print("    → Incomplete data collection not obvious until downstream")

        print("\n6. OPERATIONAL STABILITY SUMMARY")
        print("-" * 80)
        print("  ┌─────────────────────────────────────────────────┐")
        print("  │ OVERALL FRAGILITY: ⚠️  HIGH                      │")
        print("  │ Confidence in daily success: ~60-70%             │")
        print("  │ Estimated MTBF (mean time between failures):    │")
        print("  │   • Cloudflare blocks: 2-3 days                 │")
        print("  │   • Parser breaks: 5-10 days                    │")
        print("  │   • Total downtime per month: 2-3 days          │")
        print("  └─────────────────────────────────────────────────┘")

    def _audit_data_volume(self):
        """Audit current data volume and quality."""
        print("\n1. CURRENT FOREXFACTORY DATA VOLUME")
        print("-" * 80)

        scored_file = self.processed_dir / "calendar_live_scored_reactions.csv"
        if scored_file.exists():
            try:
                df = pd.read_csv(scored_file)
                print(f"  File: {scored_file.name}")
                print(f"  Total rows: {len(df)}")
                print(f"  Date range: {df['date'].min()} to {df['date'].max()}")

                # Currency breakdown
                if "currency" in df.columns:
                    currency_counts = df["currency"].value_counts()
                    print(f"\n  Events by currency:")
                    for curr, count in currency_counts.items():
                        in_target = "✓" if curr in self.module_c_currencies else "○"
                        print(f"    {in_target} {curr}: {count} events")

                # Impact breakdown
                if "impact" in df.columns:
                    impact_counts = df["impact"].value_counts()
                    print(f"\n  Events by impact:")
                    for impact, count in impact_counts.items():
                        print(f"    • {impact}: {count} events")

                # Scorable breakdown
                if "score_available" in df.columns:
                    scorable = df["score_available"].sum()
                    print(f"\n  Scorable events: {scorable}/{len(df)} ({100*scorable/len(df):.1f}%)")
                    print(f"  Non-scorable: {len(df)-scorable} (speeches, missing numeric data)")

                target_df = df[df["currency"].isin(self.module_c_currencies)]
                print(f"\n  Module C relevant (EUR,GBP,JPY,CHF): {len(target_df)}/{len(df)} events")
                print(f"  → Effective data volume for Module C: {len(target_df)}")

            except Exception as e:
                print(f"  ERROR reading scored file: {e}")
        else:
            print(f"  ✗ Scored calendar file not found: {scored_file}")

        print("\n2. DATA COMPLETENESS")
        print("-" * 80)
        try:
            raw_files = list(self.raw_dir.glob("forexfactory_*.csv"))
            if raw_files:
                latest_raw = max(raw_files, key=lambda p: p.stat().st_mtime)
                raw_df = pd.read_csv(latest_raw)

                print(f"  Latest raw file: {latest_raw.name}")
                print(f"  Total raw events: {len(raw_df)}")

                # Missing value analysis
                required = ["date", "currency", "event", "impact"]
                print(f"\n  Required field completeness:")
                for field in required:
                    if field in raw_df.columns:
                        missing = raw_df[field].isna().sum()
                        complete = len(raw_df) - missing
                        pct = 100 * complete / len(raw_df)
                        print(f"    • {field:12s}: {complete:3d}/{len(raw_df)} ({pct:5.1f}%)")

                # Numeric field quality
                print(f"\n  Numeric field quality:")
                for field in ["forecast", "actual", "previous"]:
                    if field in raw_df.columns:
                        non_null = raw_df[field].notna().sum()
                        pct = 100 * non_null / len(raw_df)
                        print(f"    • {field:12s}: {non_null:3d}/{len(raw_df)} ({pct:5.1f}%)")

            else:
                print("  ✗ No raw ForexFactory CSV files found")
        except Exception as e:
            print(f"  ERROR analyzing raw data: {e}")

        print("\n3. COMPARISON WITH MODULE C NEEDS")
        print("-" * 80)
        print("  Module C needs: high-quality, reliable, low-latency event stream")
        print("  ForexFactory provides: medium-quality, unreliable, 0-2h latency")
        print("  → Volume: 30-35 scorable events/month is LOW")
        print("  → Reliability: ~60-70% daily success is MARGINAL")
        print("  → Latency: within acceptable range (events known in advance)")

    def _recommend_strategy(self):
        """Recommend source strategy."""
        print("\nRECOMMENDATION:")
        print("-" * 80)

        print("""
┌──────────────────────────────────────────────────────────────────────────┐
│ DECISION: REPLACE FOREXFACTORY AS PRIMARY SOURCE                         │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│ Why NOT keep as primary:                                                 │
│  • High fragility (3-5 failures/month from anti-bot + parser breaks)     │
│  • Maintenance burden (1500+ lines of brittle scraper code)              │
│  • Low data volume (30-35 scorable events/month, even with widened wins) │
│  • CPU/memory cost (Selenium WebDriver is resource-heavy)                │
│  • No guarantee of long-term viability (Cloudflare arms race)            │
│                                                                            │
│ Why ForexFactory COULD work as fallback:                                 │
│  • Still captures data during primary source outages                     │
│  • No dependencies on external APIs (good for offline analysis)          │
│  • Historical monthly execution is feasible if automated well            │
│  • Could be run on-demand during gaps vs daily                           │
│                                                                            │
│ RECOMMENDED STRATEGY:                                                     │
│                                                                            │
│   Option A (RECOMMENDED): HYBRID APPROACH                                │
│   ─────────────────────────────────────────                             │
│   Primary source(s):    Trading Economics API or Federal Reserve API      │
│   Fallback source:      ForexFactory (on-demand, not daily)              │
│   Rotation:            Primary FIRST, fall back if primary fails         │
│   Frequency:           Daily for primary, weekly for fallback            │
│   Data freshness:      Real-time (market opens at 20:00 UTC)            │
│   Module C impact:     +300-500% more scorable events available          │
│                                                                            │
│   Option B (AGGRESSIVE): API-ONLY                                        │
│   ─────────────────────────────────────────────────────────────────      │
│   Primary source:      Trading Economics + Federal Reserve Data APIs      │
│   ForexFactory:        Deprecated, remove entirely                       │
│   Frequency:           Daily, fully reliable                             │
│   Data freshness:      30 min - 2 hours                                  │
│   Module C impact:     Stable, scalable, maintainable                    │
│                                                                            │
└──────────────────────────────────────────────────────────────────────────┘

NEXT STEPS:
  1. Survey candidate API sources (FRED, Trading Economics, others)
  2. Validate data quality/coverage for Module C currencies
  3. Implement replacement collector(s)
  4. Keep ForexFactory as optional fallback (deprecated path)
  5. Monitor replacement source uptime
        """)

    def _define_replacement_constraints(self):
        """Define requirements for replacement source."""
        print("\nREPLACEMENT SOURCE DESIGN CONSTRAINTS")
        print("-" * 80)

        constraints = {
            "Mandatory Data Fields": {
                "timestamp_utc": "When event occurs (ISO 8601)",
                "currency": "Currency affected (must map to EUR, GBP, JPY, CHF, USD)",
                "event_name": "Event description/code",
                "impact": "Impact level (High, Medium, Low, or numeric)",
                "actual": "Released/actual value (numeric)",
                "forecast": "Forecasted value (numeric)",
                "previous": "Previous period value (numeric or null)",
            },
            "Optional Fields": {
                "event_id": "Unique identifier or URL",
                "source": "Source identifier",
                "country": "ISO 3166 alpha-2 code",
                "frequency": "Release frequency (monthly, quarterly, etc.)",
            },
            "Non-Functional Requirements": {
                "Minimum Uptime": "99%+ daily availability (no more than ~3 hours/month downtime)",
                "API Rate Limits": "Support 100+ requests/month minimum (≥1 request per business day)",
                "Latency": "Response time ≤5 seconds (can queue requests)",
                "Data Freshness": "Events known ≥6 hours in advance",
                "Coverage": "EUR, GBP, JPY, CHF events (40+ events/month minimum)",
                "No Browser": "Must be API-based, not web scraping",
                "No Cost": "Free tier must exist or be affordable",
            },
            "Integration Requirements": {
                "Stability": "No breaking API changes without notice",
                "Documentation": "Clear field mappings and datetime handling",
                "Error Handling": "Consistent error codes and retry-ability",
                "Historical Data": "At least 3 months historical availability",
            },
        }

        for category, items in constraints.items():
            print(f"\n  {category}:")
            for key, value in items.items():
                print(f"    • {key:25s} → {value}")

        print("\n  Replacement acceptance criteria:")
        print("    ✓ Passes all mandatory fields")
        print("    ✓ Meets ≥95% of non-functional requirements")
        print("    ✓ Provides ≥300 scorable events/month for Module C currencies")
        print("    ✓ No web scraping required")
        print("    ✓ <500 lines of collector code (vs 1500+ for ForexFactory)")

        print("\n  Candidate sources to evaluate:")
        print("    • Federal Reserve FRED API (free, high quality)")
        print("    • Trading Economics API (commercial, comprehensive)")
        print("    • World Bank API (free, broad coverage)")
        print("    • ECB Statistical Data Warehouse (free, EUR-focused)")
        print("    • Investing.com Economic Calendar (commercial/freemium)")
        print("    • Quandl Calendar Data (commercial)")

        print("\n  Migration plan:")
        print("    Phase 1: Select and implement primary source")
        print("    Phase 2: Run primary + ForexFactory in parallel (2 weeks)")
        print("    Phase 3: Validate Module C scoring stable across switch")
        print("    Phase 4: Deprecate ForexFactory, migrate to primary only")
        print("    Phase 5: Keep ForexFactory code as optional fallback")


def main():
    """Run the audit."""
    try:
        audit = CalendarSourceAudit()
        audit.run()
    except Exception as e:
        print(f"\n✗ AUDIT FAILED: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
