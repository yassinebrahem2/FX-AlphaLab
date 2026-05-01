"""Geopolitical agent using GDELT zone features and a GAT ensemble."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.nn.functional as functional

from src.agents.geopolitical.signal import (
    DIRECTED_EDGES,
    ZONE_COUNTRIES,
    ZONE_NODE_IDX,
    ZONE_NODES,
    GeopoliticalSignal,
    TopEvent,
    ZoneFeatureExplanation,
    ZoneGraphData,
)
from src.shared.utils import setup_logger

N_NODES = 5
ADJ_FULL = torch.ones(N_NODES, N_NODES)
FEATURE_NAMES = ["log_count", "goldstein", "conflict_frac", "avg_tone", "log_mentions"]


class _GATLayerV2(nn.Module):
    def __init__(
        self,
        in_features: int,
        out_features: int,
        n_heads: int,
        dropout: float = 0.2,
        concat: bool = True,
    ) -> None:
        super().__init__()
        self.out_features = out_features
        self.n_heads = n_heads
        self.concat = concat
        self.W = nn.Linear(in_features, out_features * n_heads, bias=False)
        self.a_left = nn.Parameter(torch.empty(1, 1, n_heads, out_features))
        self.a_right = nn.Parameter(torch.empty(1, 1, n_heads, out_features))
        self.edge_scale = nn.Parameter(torch.tensor(0.05))
        self.leaky = nn.LeakyReLU(0.2)
        self.drop = nn.Dropout(dropout)
        nn.init.xavier_normal_(self.a_left)
        nn.init.xavier_normal_(self.a_right)

    def forward(
        self,
        x: torch.Tensor,
        adj_mask: torch.Tensor,
        edge_prior: torch.Tensor | None = None,
    ) -> torch.Tensor:
        batch_size, n_nodes = x.shape[0], x.shape[1]
        heads, f_out = self.n_heads, self.out_features
        h = self.W(x).view(batch_size, n_nodes, heads, f_out)
        e_left = (h * self.a_left).sum(-1)
        e_right = (h * self.a_right).sum(-1)
        e = self.leaky(e_left.unsqueeze(2) + e_right.unsqueeze(1))
        if edge_prior is not None:
            e = e + self.edge_scale * edge_prior.unsqueeze(-1)
        e = e.masked_fill((adj_mask == 0).unsqueeze(-1), float("-inf"))
        alpha = self.drop(torch.softmax(e, dim=2))
        h_new = torch.einsum("bijk,bjkf->bikf", alpha, h)
        if self.concat:
            return h_new.reshape(batch_size, n_nodes, heads * f_out)
        return h_new.mean(dim=2)


class _GATZoneRiskV2(nn.Module):
    def __init__(
        self,
        in_features: int = 5,
        hidden: int = 8,
        n_heads: int = 4,
        dropout: float = 0.2,
    ) -> None:
        super().__init__()
        self.gat1 = _GATLayerV2(in_features, hidden, n_heads, dropout, concat=True)
        self.bn1 = nn.BatchNorm1d(N_NODES)
        self.gat2 = _GATLayerV2(hidden * n_heads, hidden, n_heads, dropout, concat=False)
        self.zone_head = nn.Linear(hidden, 1)

    def forward(self, x: torch.Tensor, adj: torch.Tensor) -> torch.Tensor:
        h = functional.elu(self.gat1(x, adj))
        h = self.bn1(h)
        h = functional.elu(self.gat2(h, adj))
        return self.zone_head(h).squeeze(-1)


CURRENCY_TO_ZONE: dict[str, str] = {
    "EUR": "EUR",
    "USD": "USD",
    "GBP": "GBP",
    "JPY": "JPY",
    "CHF": "CHF",
}
PAIR_ZONES: dict[str, tuple[str, str]] = {
    "EURUSD": ("EUR", "USD"),
    "GBPUSD": ("GBP", "USD"),
    "USDJPY": ("USD", "JPY"),
    "USDCHF": ("USD", "CHF"),
}


class GeopoliticalAgent:
    """Run GAT-based geopolitical risk inference on GDELT zone features."""

    RISK_REGIME_THRESHOLD: float = 1.0
    ROLL_WIN: int = 30
    MIN_HIST: int = 10
    TOP_K_EVENTS: int = 5

    def __init__(
        self,
        zone_features_path: Path | None = None,
        clean_silver_dir: Path | None = None,
        model_states_path: Path | None = None,
        log_file: Path | None = None,
    ) -> None:
        root = Path(__file__).resolve().parents[3]
        self.zone_features_path = (
            zone_features_path
            or root / "data" / "processed" / "geopolitical" / "zone_features_daily.parquet"
        )
        self.clean_silver_dir = clean_silver_dir or root / "data" / "processed" / "gdelt_events"
        self.model_states_path = (
            model_states_path or root / "models" / "production" / "gdelt_gat_final_states.pt"
        )
        self.config_path = root / "models" / "production" / "gdelt_gat_config.json"
        self.logger = setup_logger(self.__class__.__name__, log_file)
        self._data: pd.DataFrame | None = None
        self._models: list[_GATZoneRiskV2] | None = None

    def load(self) -> None:
        """Load zone features and GAT model weights."""
        df = pd.read_parquet(self.zone_features_path)
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df.sort_values("date").reset_index(drop=True)

        config = self._load_config()
        states = torch.load(self.model_states_path, weights_only=True, map_location="cpu")
        models: list[_GATZoneRiskV2] = []
        for state in states:
            model = _GATZoneRiskV2(
                in_features=int(config.get("in_features", 5)),
                hidden=int(config.get("hidden", 8)),
                n_heads=int(config.get("n_heads", 4)),
                dropout=float(config.get("dropout", 0.2)),
            )
            model.load_state_dict(state)
            model.eval()
            models.append(model)

        self._data = df
        self._models = models
        self.logger.info("Loaded zone features: %d rows", len(df))

    def predict(self, pair: str, date: pd.Timestamp) -> GeopoliticalSignal:
        """Produce GeopoliticalSignal for a pair on a given UTC date."""
        if self._data is None or self._models is None:
            self.load()

        assert self._data is not None
        assert self._models is not None

        normalized_pair = pair[:-1] if pair.endswith("m") else pair
        base_ccy = normalized_pair[:3]
        quote_ccy = normalized_pair[3:6]

        base_zone = CURRENCY_TO_ZONE.get(base_ccy)
        quote_zone = CURRENCY_TO_ZONE.get(quote_ccy)
        if base_zone is None or quote_zone is None:
            message = (
                f"No zone coverage for pair '{pair}': "
                f"{base_ccy}→{base_zone}, {quote_ccy}→{quote_zone}"
            )
            raise ValueError(message)

        target_date = pd.Timestamp(date)
        if target_date.tzinfo is None:
            target_date = target_date.tz_localize("UTC")
        else:
            target_date = target_date.tz_convert("UTC")
        target_date = target_date.floor("D")

        gdelt_date = (target_date - pd.Timedelta(days=1)).date()
        today_row = self._lookup_date_row(gdelt_date)
        history = self._history_window(gdelt_date)

        today_features = self._row_to_matrix(today_row)
        if history.shape[0] < self.MIN_HIST:
            z_features = np.zeros_like(today_features, dtype="float64")
        else:
            mu = history.mean(axis=0)
            sigma = history.std(axis=0).clip(min=1e-8)
            z_features = (today_features - mu) / sigma

        zone_risk = self._ensemble_zone_risk(z_features)

        base_risk = zone_risk[ZONE_NODE_IDX[base_zone]]
        quote_risk = zone_risk[ZONE_NODE_IDX[quote_zone]]
        bilateral_risk_score = float(base_risk + quote_risk)
        risk_regime = "high" if bilateral_risk_score > self.RISK_REGIME_THRESHOLD else "low"

        base_explanation = self._build_zone_explanation(base_zone, z_features, zone_risk)
        quote_explanation = self._build_zone_explanation(quote_zone, z_features, zone_risk)

        dominant_zone = base_zone if abs(base_risk) >= abs(quote_risk) else quote_zone
        top_events = self._top_events(dominant_zone, gdelt_date)

        graph = self._build_graph(zone_risk, today_row)

        return GeopoliticalSignal(
            pair=normalized_pair,
            timestamp_utc=target_date,
            base_ccy=base_ccy,
            quote_ccy=quote_ccy,
            base_zone=base_zone,
            quote_zone=quote_zone,
            bilateral_risk_score=bilateral_risk_score,
            risk_regime=risk_regime,
            base_zone_explanation=base_explanation,
            quote_zone_explanation=quote_explanation,
            top_events=top_events,
            graph=graph,
        )

    def _load_config(self) -> dict[str, float | int]:
        try:
            with self.config_path.open("r", encoding="utf-8") as handle:
                return json.load(handle)
        except FileNotFoundError:
            return {}

    def _lookup_date_row(self, gdelt_date: date) -> pd.Series:
        assert self._data is not None
        match = self._data[self._data["date"] == gdelt_date]
        if match.empty:
            raise KeyError(f"Date {gdelt_date} not found in zone features")
        return match.iloc[0]

    def _history_window(self, gdelt_date: date) -> np.ndarray:
        assert self._data is not None
        history_df = self._data[self._data["date"] < gdelt_date].tail(self.ROLL_WIN)
        if history_df.empty:
            return np.zeros((0, len(ZONE_NODES), len(FEATURE_NAMES)), dtype="float64")
        return self._frame_to_matrix(history_df)

    def _row_to_matrix(self, row: pd.Series) -> np.ndarray:
        matrix = np.zeros((len(ZONE_NODES), len(FEATURE_NAMES)), dtype="float64")
        for i, zone in enumerate(ZONE_NODES):
            zone_key = zone.lower()
            for j, feature in enumerate(FEATURE_NAMES):
                matrix[i, j] = float(row[f"{zone_key}_{feature}"])
        return matrix

    def _frame_to_matrix(self, frame: pd.DataFrame) -> np.ndarray:
        matrices = [self._row_to_matrix(row) for _, row in frame.iterrows()]
        return np.stack(matrices, axis=0)

    def _ensemble_zone_risk(self, z_features: np.ndarray) -> np.ndarray:
        assert self._models is not None
        x = torch.tensor(z_features, dtype=torch.float32).unsqueeze(0)
        adj = ADJ_FULL.to(dtype=torch.float32)
        outputs = []
        with torch.no_grad():
            for model in self._models:
                outputs.append(model(x, adj).squeeze(0).cpu().numpy())
        return np.mean(np.stack(outputs, axis=0), axis=0)

    def _build_zone_explanation(
        self, zone: str, z_features: np.ndarray, zone_risk: np.ndarray
    ) -> ZoneFeatureExplanation:
        idx = ZONE_NODE_IDX[zone]
        feature_zscores = {name: float(z_features[idx, j]) for j, name in enumerate(FEATURE_NAMES)}
        dominant_driver = max(feature_zscores, key=lambda k: abs(feature_zscores[k]))
        return ZoneFeatureExplanation(
            zone=zone,
            risk_score=float(zone_risk[idx]),
            feature_zscores=feature_zscores,
            dominant_driver=dominant_driver,
        )

    def _top_events(self, zone: str, gdelt_date: date) -> list[TopEvent]:
        if self.clean_silver_dir is None:
            return []

        path = (
            self.clean_silver_dir
            / f"year={gdelt_date.year}"
            / f"month={gdelt_date.month:02d}"
            / "gdelt_events_cleaned.parquet"
        )
        if not path.exists():
            return []

        df = pd.read_parquet(path)
        df = df[pd.to_datetime(df["event_date"], utc=True).dt.date == gdelt_date].copy()
        if df.empty:
            return []
        countries = ZONE_COUNTRIES[zone]
        mask = df["actor1_country_code"].isin(countries) | df["actor2_country_code"].isin(countries)
        df = df[mask].copy()
        if df.empty:
            return []

        score = df["num_mentions"].fillna(0).astype("float64") * df["goldstein_scale"].abs().fillna(
            0
        )
        df = df.assign(_rank_score=score).sort_values("_rank_score", ascending=False)

        events: list[TopEvent] = []
        for _, row in df.head(self.TOP_K_EVENTS).iterrows():
            events.append(
                TopEvent(
                    actor1_name=row.get("actor1_name"),
                    actor2_name=row.get("actor2_name"),
                    goldstein_scale=float(row.get("goldstein_scale", 0.0) or 0.0),
                    avg_tone=float(row.get("avg_tone", 0.0) or 0.0),
                    num_mentions=int(row.get("num_mentions", 0) or 0),
                    quad_class=int(row.get("quad_class", 0) or 0),
                    source_url=row.get("source_url"),
                )
            )
        return events

    def _build_graph(self, zone_risk: np.ndarray, row: pd.Series) -> ZoneGraphData:
        zone_risk_scores = {zone: float(zone_risk[ZONE_NODE_IDX[zone]]) for zone in ZONE_NODES}
        edge_weights = {}
        for zi, zj in DIRECTED_EDGES:
            key = f"{zi}→{zj}"
            edge_weights[key] = float(row[f"edge_{zi.lower()}_{zj.lower()}"])
        return ZoneGraphData(zone_risk_scores=zone_risk_scores, edge_weights=edge_weights)
