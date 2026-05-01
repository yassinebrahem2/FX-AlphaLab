"""Macro agent: orchestrates Node 1 (macroeconomics) and Node 2 (calendar, future)."""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pandas as pd

from src.agents.macro.calendar_node import CalendarEventsNode
from src.agents.macro.macro_node import MacroeconomicsNode
from src.agents.macro.signal import MacroSignal, TopCalendarEvent
from src.shared.utils import setup_logger


class MacroAgent:
    """Orchestrator for macro analysis: delegates to nodes."""

    def __init__(
        self,
        data_path: Path | None = None,
        calendar_node: CalendarEventsNode | None = None,
        log_file: Path | None = None,
    ) -> None:
        self.macro_node = MacroeconomicsNode(data_path=data_path, log_file=log_file)
        self.calendar_node = calendar_node
        self.logger = setup_logger(self.__class__.__name__, log_file)

    def load(self) -> None:
        """Load all active nodes."""
        self.macro_node.load()
        if self.calendar_node is not None:
            self.calendar_node.load()

    def predict(self, pair: str, date: pd.Timestamp) -> MacroSignal:
        """
        Predict macro signal for a pair at a date.

        Args:
            pair: Currency pair (e.g., "EURUSD").
            date: Request date (UTC, naive or aware).

        Returns:
            MacroSignal with all fields, including dominant_driver and top_calendar_events.
        """
        signal = self.macro_node.predict(pair, date)

        top_events: list[TopCalendarEvent] | None = None
        if self.calendar_node is not None:
            try:
                surprise_score, top_events = self.calendar_node.predict_with_context(
                    pair, signal.signal_timestamp
                )
                signal = dataclasses.replace(
                    signal,
                    macro_surprise_score=surprise_score,
                    top_calendar_events=top_events,
                )
            except Exception as exc:
                self.logger.warning("CalendarEventsNode.predict_with_context failed: %s", exc)

        sub_scores = {
            "carry_signal_score": abs(signal.carry_signal_score),
            "regime_context_score": abs(signal.regime_context_score),
            "fundamental_mispricing_score": abs(signal.fundamental_mispricing_score),
            "macro_surprise_score": abs(signal.macro_surprise_score),
        }
        dominant_driver = max(sub_scores, key=sub_scores.__getitem__)
        signal = dataclasses.replace(signal, dominant_driver=dominant_driver)

        return signal
