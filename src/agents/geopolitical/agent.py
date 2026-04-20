"""Geopolitical agent using zone-level daily risk artifacts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.shared.utils import setup_logger


@dataclass(frozen=True)
class GeopoliticalSignal:
    """Pair-level geopolitical signal computed from zone-level risk."""

    pair: str
    timestamp_utc: pd.Timestamp
    base_ccy: str
    quote_ccy: str
    base_zone: str
    quote_zone: str
    base_zone_risk: float
    quote_zone_risk: float
    risk_diff: float
    risk_level: float
    risk_regime: str
    risk_trend: str
    top_drivers: list[str]


class GeopoliticalAgent:
    """Load geopolitical risk artifact and produce bilateral FX risk signals."""

    DATA_PATH = (
        Path(__file__).resolve().parents[3]
        / "data"
        / "processed"
        / "geopolitical"
        / "geo_risk_daily.csv"
    )

    CURRENCY_TO_ZONE: dict[str, str] = {
        "EUR": "EUR",
        "USD": "USD",
        "GBP": "GBP",
        "JPY": "JPY",
        "CHF": "CHF",
    }

    RISK_TREND_THRESHOLD: float = 2.0

    def __init__(self, data_path: Path | None = None, log_file: Path | None = None) -> None:
        self.data_path = data_path or self.DATA_PATH
        self.logger = setup_logger(self.__class__.__name__, log_file)
        self._data: pd.DataFrame | None = None

    def load(self) -> None:
        """Load and parse the geopolitical artifact into a date/zone indexed frame."""
        if not self.data_path.exists():
            raise FileNotFoundError(f"Geopolitical artifact not found: {self.data_path}")

        df = pd.read_csv(self.data_path)

        df["date"] = pd.to_datetime(df["date"], utc=True).dt.floor("D")
        df["top_drivers"] = df["top_drivers"].apply(json.loads)

        df = df.set_index(["date", "zone"]).sort_index()

        self._data = df
        self.logger.info("Loaded geopolitical risk data: %d rows from %s", len(df), self.data_path)

    def predict(self, pair: str, date: pd.Timestamp) -> GeopoliticalSignal:
        """Produce a bilateral geopolitical signal for a currency pair at a UTC day."""
        if self._data is None:
            self.load()

        assert self._data is not None

        normalized_pair = pair[:-1] if pair.endswith("m") else pair
        base_ccy = normalized_pair[:3]
        quote_ccy = normalized_pair[3:6]

        base_zone = self.CURRENCY_TO_ZONE.get(base_ccy)
        quote_zone = self.CURRENCY_TO_ZONE.get(quote_ccy)

        if base_zone is None or quote_zone is None:
            raise ValueError(
                f"No zone coverage for pair '{pair}': "
                f"{base_ccy}→{base_zone}, {quote_ccy}→{quote_zone}"
            )

        lookup_day = pd.Timestamp(date)
        if lookup_day.tzinfo is None:
            lookup_day = lookup_day.tz_localize("UTC")
        else:
            lookup_day = lookup_day.tz_convert("UTC")
        lookup_day = lookup_day.floor("D")

        try:
            base_row = self._data.loc[(lookup_day, base_zone)]
        except KeyError as exc:
            raise KeyError(
                f"Date {lookup_day.date()} not found in geo risk data for zone '{base_zone}'"
            ) from exc

        try:
            quote_row = self._data.loc[(lookup_day, quote_zone)]
        except KeyError as exc:
            raise KeyError(
                f"Date {lookup_day.date()} not found in geo risk data for zone '{quote_zone}'"
            ) from exc

        base_zone_risk = float(base_row["geo_risk_index"])
        quote_zone_risk = float(quote_row["geo_risk_index"])

        risk_diff = base_zone_risk - quote_zone_risk
        risk_level = max(base_zone_risk, quote_zone_risk)
        risk_regime = "high" if risk_level > 100.0 else "low"

        dominant_row = base_row if base_zone_risk >= quote_zone_risk else quote_row
        risk_trend = self._compute_risk_trend(
            float(dominant_row["geo_risk_ewm_7"]),
            float(dominant_row["geo_risk_ewm_30"]),
        )

        return GeopoliticalSignal(
            pair=pair,
            timestamp_utc=lookup_day,
            base_ccy=base_ccy,
            quote_ccy=quote_ccy,
            base_zone=base_zone,
            quote_zone=quote_zone,
            base_zone_risk=base_zone_risk,
            quote_zone_risk=quote_zone_risk,
            risk_diff=risk_diff,
            risk_level=risk_level,
            risk_regime=risk_regime,
            risk_trend=risk_trend,
            top_drivers=list(dominant_row["top_drivers"]),
        )

    def _compute_risk_trend(self, ewm_7: float, ewm_30: float) -> str:
        if ewm_7 > ewm_30 + self.RISK_TREND_THRESHOLD:
            return "rising"
        if ewm_7 < ewm_30 - self.RISK_TREND_THRESHOLD:
            return "falling"
        return "stable"
