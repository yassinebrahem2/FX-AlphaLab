"""CLI entry point for live inference.

Usage:
    python -m src.inference.cli                        # target = yesterday UTC
    python -m src.inference.cli --date 2025-01-10      # explicit date
    python -m src.inference.cli --dry-run              # skip DB writes, print only
"""

from __future__ import annotations

import argparse
import json
import sys

import pandas as pd


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="fx-inference",
        description="Run FX-AlphaLab live inference pipeline for one trading date.",
    )
    parser.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        default=None,
        help="Target date (UTC).  Defaults to yesterday.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Run full inference but skip all DB writes.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Print narrative_context as JSON instead of human-readable summary.",
        dest="output_json",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    target_date: pd.Timestamp | None = None
    if args.date:
        try:
            target_date = pd.Timestamp(args.date, tz="UTC")
        except ValueError:
            print(f"ERROR: invalid date '{args.date}' — expected YYYY-MM-DD", file=sys.stderr)
            return 2

    from src.inference.pipeline import InferencePipeline

    pipeline = InferencePipeline()
    try:
        result = pipeline.run(target_date=target_date, dry_run=args.dry_run)
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"ERROR: inference failed — {exc}", file=sys.stderr)
        return 1

    report = result.report

    if args.output_json:
        print(json.dumps(report.narrative_context, indent=2, default=str))
        return 0

    # ── Human-readable summary ────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  FX-AlphaLab Inference  |  {result.target_date.date()}")
    print(f"{'='*60}")
    print(f"  Overall action : {report.overall_action.upper()}")
    print(f"  Global regime  : {report.global_regime}")
    if report.hold_reason:
        print(f"  Hold reason    : {report.hold_reason}")
    if report.top_pick:
        print(f"  Top pick       : {report.top_pick}")

    print("\n  Per-pair signals:")
    for pa in sorted(report.pairs, key=lambda a: a.conviction_score, reverse=True):
        flag = " <-- TOP" if pa.pair == report.top_pick else ""
        print(
            f"    {pa.pair:8s}  {pa.suggested_action:5s}  "
            f"tier={pa.confidence_tier:6s}  "
            f"conviction={pa.conviction_score:.4f}  "
            f"pos={pa.position_size_pct:.1f}%  "
            f"SL={pa.sl_pct:.4f}%  TP={pa.tp_pct:.4f}%{flag}"
        )

    if result.stale_warnings:
        print(f"\n  [WARN] Stale sources ({len(result.stale_warnings)}):")
        for source, msg in result.stale_warnings.items():
            print(f"    {source}: {msg[:120]}")

    if args.dry_run:
        print("\n  [dry-run] DB writes skipped.")
    else:
        print(f"\n  DB rows written: {result.db_rows}")

    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
