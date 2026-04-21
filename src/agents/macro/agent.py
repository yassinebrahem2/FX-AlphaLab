"""Macro agent: orchestrates Node 1 (macroeconomics) and Node 2 (calendar, future)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.agents.macro.macro_node import MacroeconomicsNode
from src.shared.utils import setup_logger


@dataclass(frozen=True)
class MacroSignal:
    """Monthly macro signal with 9 fields."""

    pair: str
    signal_timestamp: pd.Timestamp  # month-end from parquet (e.g., 2025-01-31)
    module_c_direction: str  # "up" or "down"
    macro_confidence: float
    carry_signal_score: float
    regime_context_score: float
    fundamental_mispricing_score: float
    macro_surprise_score: float  # 0.0 until CalendarEventsNode integrated
    macro_bias_score: float
    node_version: str  # "macro_economics_v1"


class MacroAgent:
    """Orchestrator for macro analysis: delegates to nodes."""

    def __init__(self, data_path: Path | None = None, log_file: Path | None = None) -> None:
        self.macro_node = MacroeconomicsNode(data_path=data_path, log_file=log_file)
        self.calendar_node: None = None  # Placeholder for Node 2 (CalendarEventsNode)
        self.logger = setup_logger(self.__class__.__name__, log_file)

    def load(self) -> None:
        """Load all active nodes."""
        self.macro_node.load()

        # Node 2 (calendar events) — uncomment when CalendarEventsNode is implemented:
        # if self.calendar_node is not None:
        #     self.calendar_node.load()

    def predict(self, pair: str, date: pd.Timestamp) -> MacroSignal:
        """
        Predict macro signal for a pair at a date.

        Delegates to macro_node for now. Calendar events (Node 2) will fuse
        macro_surprise_score when implemented.

        Args:
            pair: Currency pair (e.g., "EURUSD").
            date: Request date (UTC, naive or aware).

        Returns:
            MacroSignal with all 9 fields.
        """
        signal = self.macro_node.predict(pair, date)

        # Node 2 fusion — uncomment when CalendarEventsNode is implemented:
        # if self.calendar_node is not None:
        #     calendar_signal = self.calendar_node.predict(pair, date)
        #     return MacroSignal(
        #         pair=signal.pair,
        #         signal_timestamp=signal.signal_timestamp,
        #         module_c_direction=signal.module_c_direction,
        #         macro_confidence=signal.macro_confidence,
        #         carry_signal_score=signal.carry_signal_score,
        #         regime_context_score=signal.regime_context_score,
        #         fundamental_mispricing_score=signal.fundamental_mispricing_score,
        #         macro_surprise_score=calendar_signal.surprise_score,
        #         macro_bias_score=signal.macro_bias_score,
        #         node_version=signal.node_version,
        #     )

        return signal
