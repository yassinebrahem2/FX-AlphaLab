"""Abstract base class for per-pair, per-day FX signal agents."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime

import pandas as pd


class BaseAgent(ABC):
    """
    Contract for agents that produce a per-pair, per-day signal.

    Agents bound to a single pair (e.g. TechnicalAgent) raise ValueError
    from predict() when called with a mismatched pair. The coordinator is
    responsible for routing calls to the correct agent instance.
    """

    logger: logging.Logger

    @abstractmethod
    def predict(self, pair: str, date: pd.Timestamp) -> object:
        """Return a signal dataclass for the given pair and UTC date."""

    def compute_batch(
        self,
        pairs: list[str],
        start_date: datetime,
        end_date: datetime,
    ) -> pd.DataFrame:
        """
        Run predict() over all business days in [start_date, end_date] for each pair.

        Returns a DataFrame with 'date', 'pair', and 'signal' columns sorted by
        (date, pair). Rows where predict() raises are logged and skipped.
        Individual agents may override for a richer (flattened) schema.
        """
        rows: list[dict] = []
        for date in pd.bdate_range(start=start_date, end=end_date, tz="UTC"):
            for pair in pairs:
                try:
                    signal = self.predict(pair, date)
                    rows.append({"date": date, "pair": pair, "signal": signal})
                except Exception as exc:
                    self.logger.warning("predict(%s, %s) failed: %s", pair, date.date(), exc)
        if not rows:
            return pd.DataFrame(columns=["date", "pair", "signal"])
        return pd.DataFrame(rows).sort_values(["date", "pair"]).reset_index(drop=True)
