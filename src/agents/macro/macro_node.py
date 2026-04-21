"""Macroeconomics node: reads pre-computed monthly signals (Node 1 of 2)."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from src.shared.utils import setup_logger

if TYPE_CHECKING:
    from src.agents.macro.agent import MacroSignal


class MacroeconomicsNode:
    """Load monthly macro signal artifact and provide lookups by pair & date."""

    DATA_PATH = (
        Path(__file__).resolve().parents[3]
        / "data"
        / "processed"
        / "macro"
        / "macro_signal.parquet"
    )

    def __init__(self, data_path: Path | None = None, log_file: Path | None = None) -> None:
        self.data_path = data_path or self.DATA_PATH
        self.logger = setup_logger(self.__class__.__name__, log_file)
        self._data: pd.DataFrame | None = None

    def load(self) -> None:
        """Load parquet and index by (pair, timestamp_utc)."""
        if not self.data_path.exists():
            raise FileNotFoundError(f"Macro signal artifact not found: {self.data_path}")

        df = pd.read_parquet(self.data_path)

        # Ensure timestamp is UTC-aware
        if df["timestamp_utc"].dtype.kind == "M":  # datetime-like
            if df["timestamp_utc"].dt.tz is None:
                df["timestamp_utc"] = df["timestamp_utc"].dt.tz_localize("UTC")
            else:
                df["timestamp_utc"] = df["timestamp_utc"].dt.tz_convert("UTC")

        # Index and sort
        df = df.set_index(["pair", "timestamp_utc"]).sort_index()

        self._data = df
        self.logger.info("Loaded macro signal data: %d rows from %s", len(df), self.data_path)

    def predict(self, pair: str, date: pd.Timestamp) -> MacroSignal:
        """
        Look up a macro signal for (pair, date).

        Temporal logic: signal timestamped 2025-01-31 was generated at end of Jan
        and is valid from Feb 1. Request for 2025-02-15 returns 2025-01-31 signal.
        Implementation: find all signals for pair where signal_timestamp <=
        month_end(date - 1 month); take the latest.

        Args:
            pair: Currency pair (e.g., "EURUSD" or "EURUSDm").
            date: Request date (UTC, naive or aware).

        Returns:
            MacroSignal with all 9 fields.

        Raises:
            ValueError: If pair not in artifact (after normalization).
            KeyError: If no signal exists for the resolved lookup date.
        """
        if self._data is None:
            self.load()

        assert self._data is not None

        # Normalize pair
        normalized_pair = pair[:-1] if pair.endswith("m") else pair

        # Check pair exists
        pair_levels = self._data.index.get_level_values("pair").unique()
        if normalized_pair not in pair_levels:
            raise ValueError(
                f"Pair '{normalized_pair}' not in macro signal artifact. "
                f"Available: {sorted(pair_levels)}"
            )

        # Convert date to UTC
        if isinstance(date, pd.Timestamp):
            lookup_date = date
        else:
            lookup_date = pd.Timestamp(date)

        if lookup_date.tzinfo is None:
            lookup_date = lookup_date.tz_localize("UTC")
        else:
            lookup_date = lookup_date.tz_convert("UTC")

        # Compute month floor: last day of month (date - 1 month)
        # If request is 2025-02-15, we want signal from 2025-01-31
        prior_month = lookup_date - pd.DateOffset(months=1)
        month_floor = prior_month.normalize() + pd.offsets.MonthEnd(0)

        # Retrieve all signals for pair
        pair_signals = self._data.loc[[normalized_pair]]

        # Filter: signal_timestamp <= month_floor
        valid_signals = pair_signals[
            pair_signals.index.get_level_values("timestamp_utc") <= month_floor
        ]

        if valid_signals.empty:
            raise KeyError(
                f"No signal found for pair '{normalized_pair}' on or before {month_floor.date()}"
            )

        # Take latest (highest timestamp)
        latest_idx = valid_signals.index.get_level_values("timestamp_utc").max()
        signal_row = valid_signals.loc[(normalized_pair, latest_idx)]

        # Import here to avoid circular dependency
        from src.agents.macro.agent import MacroSignal

        return MacroSignal(
            pair=pair,
            signal_timestamp=latest_idx,
            module_c_direction=str(signal_row["module_c_direction"]),
            macro_confidence=float(signal_row["macro_confidence"]),
            carry_signal_score=float(signal_row["carry_signal_score"]),
            regime_context_score=float(signal_row["regime_context_score"]),
            fundamental_mispricing_score=float(signal_row["fundamental_mispricing_score"]),
            macro_surprise_score=float(signal_row["macro_surprise_score"]),
            macro_bias_score=float(signal_row["macro_bias_score"]),
            node_version="macro_economics_v1",
        )
