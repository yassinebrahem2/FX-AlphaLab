from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

ZONE_NODES = ["USD", "EUR", "GBP", "JPY", "CHF"]
ZONE_NODE_IDX: dict[str, int] = {z: i for i, z in enumerate(ZONE_NODES)}
DIRECTED_EDGES: list[tuple[str, str]] = [
    (zi, zj) for zi in ZONE_NODES for zj in ZONE_NODES if zi != zj
]  # 20 directed zone pairs

ZONE_COUNTRIES: dict[str, frozenset[str]] = {
    "USD": frozenset({"USA"}),
    "EUR": frozenset(
        {
            "EUR",
            "AUT",
            "BEL",
            "CYP",
            "DEU",
            "ESP",
            "EST",
            "FIN",
            "FRA",
            "GRC",
            "HRV",
            "IRL",
            "ITA",
            "LTU",
            "LUX",
            "LVA",
            "MLT",
            "NLD",
            "PRT",
            "SVK",
            "SVN",
        }
    ),
    "GBP": frozenset({"GBR"}),
    "JPY": frozenset({"JPN"}),
    "CHF": frozenset({"CHE"}),
}


@dataclass(frozen=True)
class ZoneFeatureExplanation:
    """Layer 1 explainability: which features are driving this zone's risk score."""

    zone: str
    risk_score: float
    feature_zscores: dict[str, float]
    dominant_driver: str


@dataclass(frozen=True)
class TopEvent:
    """Layer 2 explainability: a specific GDELT event driving zone activity."""

    actor1_name: str | None
    actor2_name: str | None
    goldstein_scale: float
    avg_tone: float
    num_mentions: int
    quad_class: int
    source_url: str | None


@dataclass(frozen=True)
class ZoneGraphData:
    """Layer 3 explainability: full graph state for network visualization."""

    zone_risk_scores: dict[str, float]
    edge_weights: dict[str, float]


@dataclass(frozen=True)
class GeopoliticalSignal:
    """Complete geopolitical signal for one FX pair on one date."""

    pair: str
    timestamp_utc: pd.Timestamp
    base_ccy: str
    quote_ccy: str
    base_zone: str
    quote_zone: str
    bilateral_risk_score: float
    risk_regime: str
    base_zone_explanation: ZoneFeatureExplanation
    quote_zone_explanation: ZoneFeatureExplanation
    top_events: list[TopEvent]
    graph: ZoneGraphData
