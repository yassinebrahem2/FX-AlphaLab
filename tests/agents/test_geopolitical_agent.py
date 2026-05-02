"""Tests for the geopolitical agent and zone feature builder."""

from datetime import date, datetime
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import torch

from src.agents.geopolitical.agent import GeopoliticalAgent, _GATZoneRiskV2

_builder_path = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "ingestion"
    / "preprocessors"
    / "gdelt_zone_feature_builder.py"
)
_builder_spec = spec_from_file_location("gdelt_zone_feature_builder", _builder_path)
assert _builder_spec and _builder_spec.loader
_builder_module = module_from_spec(_builder_spec)
_builder_spec.loader.exec_module(_builder_module)

DIRECTED_EDGES = _builder_module.DIRECTED_EDGES
FEATURE_NAMES = _builder_module.FEATURE_NAMES
GDELTZoneFeatureBuilder = _builder_module.GDELTZoneFeatureBuilder
ZONE_NODES = _builder_module.ZONE_NODES


@pytest.fixture
def mock_zone_features_df() -> pd.DataFrame:
    dates = pd.date_range("2023-01-01", periods=40, freq="D").date
    rows = []
    for i, day in enumerate(dates):
        row: dict[str, float | date] = {"date": day}
        for zone in ZONE_NODES:
            zone_key = zone.lower()
            for j, feature in enumerate(FEATURE_NAMES):
                row[f"{zone_key}_{feature}"] = float(i + j + len(zone_key))
        for zi, zj in DIRECTED_EDGES:
            row[f"edge_{zi.lower()}_{zj.lower()}"] = float((i + len(zi) + len(zj)) % 7)
        rows.append(row)
    return pd.DataFrame(rows)


@pytest.fixture
def mock_clean_silver_df() -> pd.DataFrame:
    event_date = pd.Timestamp("2023-02-01", tz="UTC")
    rows = []
    for i in range(20):
        rows.append(
            {
                "event_date": event_date,
                "actor1_country_code": "USA" if i % 2 == 0 else "DEU",
                "actor2_country_code": "DEU" if i % 3 == 0 else "USA",
                "actor1_name": f"Actor1-{i}",
                "actor2_name": f"Actor2-{i}",
                "goldstein_scale": 1.0 + (i % 5) * 0.5,
                "avg_tone": -0.2 + 0.05 * i,
                "num_mentions": 5 + i,
                "quad_class": 3 if i % 4 == 0 else 1,
                "source_url": f"http://example.com/{i}",
            }
        )
    return pd.DataFrame(rows)


@pytest.fixture
def mock_model_states() -> list[dict[str, torch.Tensor]]:
    return [_GATZoneRiskV2().state_dict() for _ in range(15)]


@pytest.fixture
def agent(
    mock_zone_features_df: pd.DataFrame,
    mock_clean_silver_df: pd.DataFrame,
    mock_model_states: list[dict[str, torch.Tensor]],
    monkeypatch: pytest.MonkeyPatch,
) -> GeopoliticalAgent:
    def mock_read_parquet(path: Path, *args: object, **kwargs: object) -> pd.DataFrame:
        if "gdelt_events_cleaned" in str(path):
            return mock_clean_silver_df
        return mock_zone_features_df

    monkeypatch.setattr(pd, "read_parquet", mock_read_parquet)
    monkeypatch.setattr(torch, "load", lambda *args, **kwargs: mock_model_states)
    monkeypatch.setattr(Path, "exists", lambda self: True)

    return GeopoliticalAgent(
        zone_features_path=Path("zone_features_daily.parquet"),
        clean_silver_dir=Path("gdelt_events"),
        model_states_path=Path("gdelt_gat_final_states.pt"),
    )


def test_signal_identity_eurusd(agent: GeopoliticalAgent) -> None:
    signal = agent.predict("EURUSDm", pd.Timestamp("2023-02-02", tz="UTC"))

    assert signal.pair == "EURUSD"
    assert signal.base_ccy == "EUR"
    assert signal.quote_ccy == "USD"
    assert signal.base_zone == "EUR"
    assert signal.quote_zone == "USD"


def test_signal_identity_usdjpy(agent: GeopoliticalAgent) -> None:
    signal = agent.predict("USDJPYm", pd.Timestamp("2023-02-02", tz="UTC"))

    assert signal.base_ccy == "USD"
    assert signal.quote_ccy == "JPY"


def test_bilateral_asymmetry(agent: GeopoliticalAgent, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(agent, "_ensemble_zone_risk", lambda z: np.array([1, 2, 3, 4, 5]))
    date = pd.Timestamp("2023-02-02", tz="UTC")

    eurusd_signal = agent.predict("EURUSDm", date)
    gbpusd_signal = agent.predict("GBPUSDm", date)

    assert eurusd_signal.bilateral_risk_score != gbpusd_signal.bilateral_risk_score


def test_risk_regime_high(agent: GeopoliticalAgent, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        agent,
        "_ensemble_zone_risk",
        lambda z: np.array([2.0, 2.0, 2.0, 2.0, 2.0]),
    )
    signal = agent.predict("EURUSDm", pd.Timestamp("2023-02-02", tz="UTC"))

    assert signal.risk_regime == "high"


def test_risk_regime_low(agent: GeopoliticalAgent, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        agent,
        "_ensemble_zone_risk",
        lambda z: np.array([0.1, 0.1, 0.1, 0.1, 0.1]),
    )
    signal = agent.predict("EURUSDm", pd.Timestamp("2023-02-02", tz="UTC"))

    assert signal.risk_regime == "low"


def test_layer1_dominant_driver(agent: GeopoliticalAgent, monkeypatch: pytest.MonkeyPatch) -> None:
    def force_features(_: date) -> pd.Series:
        row = agent._data.iloc[-1].copy()  # type: ignore[union-attr]
        row["eur_log_count"] = 10.0
        row["eur_goldstein"] = 0.2
        row["eur_conflict_frac"] = 0.1
        row["eur_avg_tone"] = 0.05
        row["eur_log_mentions"] = 0.3
        return row

    monkeypatch.setattr(agent, "_lookup_date_row", force_features)
    monkeypatch.setattr(agent, "_history_window", lambda _: np.ones((15, 5, 5)))
    monkeypatch.setattr(agent, "_ensemble_zone_risk", lambda z: np.zeros(5))

    signal = agent.predict("EURUSDm", pd.Timestamp("2023-02-02", tz="UTC"))
    assert signal.base_zone_explanation.dominant_driver == "log_count"


def test_layer2_top_events_ranked(
    agent: GeopoliticalAgent, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        agent,
        "_ensemble_zone_risk",
        lambda z: np.array([5, 4, 1, 1, 1]),
    )
    signal = agent.predict("EURUSDm", pd.Timestamp("2023-02-02", tz="UTC"))

    assert signal.top_events
    scores = [e.num_mentions * abs(e.goldstein_scale) for e in signal.top_events]
    assert scores == sorted(scores, reverse=True)


def test_layer2_empty_when_no_silver_dir(
    mock_zone_features_df: pd.DataFrame,
    mock_model_states: list[dict[str, torch.Tensor]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pd, "read_parquet", lambda *args, **kwargs: mock_zone_features_df)
    monkeypatch.setattr(torch, "load", lambda *args, **kwargs: mock_model_states)
    agent = GeopoliticalAgent(
        zone_features_path=Path("zone_features_daily.parquet"),
        clean_silver_dir=None,
        model_states_path=Path("gdelt_gat_final_states.pt"),
    )
    signal = agent.predict("EURUSDm", pd.Timestamp("2023-02-02", tz="UTC"))

    assert signal.top_events == []


def test_layer3_graph_has_all_zones(agent: GeopoliticalAgent) -> None:
    signal = agent.predict("EURUSDm", pd.Timestamp("2023-02-02", tz="UTC"))

    assert set(signal.graph.zone_risk_scores) == set(ZONE_NODES)


def test_layer3_graph_has_all_edges(agent: GeopoliticalAgent) -> None:
    signal = agent.predict("EURUSDm", pd.Timestamp("2023-02-02", tz="UTC"))

    assert len(signal.graph.edge_weights) == 20
    assert all("→" in key for key in signal.graph.edge_weights)


def test_uncovered_pair_raises(agent: GeopoliticalAgent) -> None:
    with pytest.raises(ValueError):
        agent.predict("AUDNZDm", pd.Timestamp("2023-02-02", tz="UTC"))


def test_missing_date_raises(agent: GeopoliticalAgent) -> None:
    with pytest.raises(KeyError):
        agent.predict("EURUSDm", pd.Timestamp("2024-01-01", tz="UTC"))


def test_lazy_load(
    mock_zone_features_df: pd.DataFrame,
    mock_model_states: list[dict[str, torch.Tensor]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pd, "read_parquet", lambda *args, **kwargs: mock_zone_features_df)
    monkeypatch.setattr(torch, "load", lambda *args, **kwargs: mock_model_states)
    agent = GeopoliticalAgent(
        zone_features_path=Path("zone_features_daily.parquet"),
        clean_silver_dir=None,
        model_states_path=Path("gdelt_gat_final_states.pt"),
    )

    assert agent._data is None
    _ = agent.predict("EURUSDm", pd.Timestamp("2023-02-02", tz="UTC"))
    assert agent._data is not None


def test_m_suffix_stripped(agent: GeopoliticalAgent) -> None:
    signal_with_suffix = agent.predict("EURUSDm", pd.Timestamp("2023-02-02", tz="UTC"))
    signal_without_suffix = agent.predict("EURUSD", pd.Timestamp("2023-02-02", tz="UTC"))

    assert signal_with_suffix == signal_without_suffix


def test_compute_batch_returns_dataframe(agent: GeopoliticalAgent) -> None:
    """Test compute_batch() returns a DataFrame with date and pair columns."""
    df = agent.compute_batch(
        pairs=["EURUSD"],
        start_date=pd.Timestamp("2023-02-02", tz="UTC"),
        end_date=pd.Timestamp("2023-02-06", tz="UTC"),
    )

    assert isinstance(df, pd.DataFrame)
    assert "date" in df.columns
    assert "pair" in df.columns
    assert "signal" in df.columns
    assert len(df) > 0
    assert all(df["pair"] == "EURUSD")


def test_zone_feature_builder_run(
    mock_clean_silver_df: pd.DataFrame,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    builder = GDELTZoneFeatureBuilder(
        clean_silver_dir=Path("gdelt_events"),
        output_path=Path("zone_features_daily.parquet"),
    )
    monkeypatch.setattr(builder, "_discover_parquet_paths", lambda *args: [Path("mock.parquet")])
    monkeypatch.setattr(pd, "read_parquet", lambda *args, **kwargs: mock_clean_silver_df)

    captured: dict[str, pd.DataFrame] = {}

    def capture_to_parquet(self: pd.DataFrame, *args: object, **kwargs: object) -> None:
        captured["df"] = self.copy()

    monkeypatch.setattr(pd.DataFrame, "to_parquet", capture_to_parquet, raising=False)

    rows_written = builder.run(
        start_date=datetime(2023, 2, 1),
        end_date=datetime(2023, 2, 1),
        backfill=True,
    )

    output_df = captured["df"]
    assert rows_written == len(output_df)

    expected_columns = ["date"]
    for zone in ZONE_NODES:
        zone_key = zone.lower()
        for feature in FEATURE_NAMES:
            expected_columns.append(f"{zone_key}_{feature}")
    for zi, zj in DIRECTED_EDGES:
        expected_columns.append(f"edge_{zi.lower()}_{zj.lower()}")

    assert list(output_df.columns) == expected_columns
    assert all(isinstance(value, date) for value in output_df["date"])
    assert output_df.drop(columns=["date"]).dtypes.apply(lambda d: d == np.dtype("float64")).all()


def test_zone_feature_builder_empty_zone(monkeypatch: pytest.MonkeyPatch) -> None:
    event_date = pd.Timestamp("2023-02-01", tz="UTC")
    df = pd.DataFrame(
        {
            "event_date": [event_date],
            "actor1_country_code": ["USA"],
            "actor2_country_code": ["DEU"],
            "goldstein_scale": [1.0],
            "avg_tone": [0.1],
            "num_mentions": [5],
            "quad_class": [1],
        }
    )

    builder = GDELTZoneFeatureBuilder(
        clean_silver_dir=Path("gdelt_events"),
        output_path=Path("zone_features_daily.parquet"),
    )
    monkeypatch.setattr(builder, "_discover_parquet_paths", lambda *args: [Path("mock.parquet")])
    monkeypatch.setattr(pd, "read_parquet", lambda *args, **kwargs: df)

    captured: dict[str, pd.DataFrame] = {}

    def capture_to_parquet(self: pd.DataFrame, *args: object, **kwargs: object) -> None:
        captured["df"] = self.copy()

    monkeypatch.setattr(pd.DataFrame, "to_parquet", capture_to_parquet, raising=False)

    builder.run(start_date=datetime(2023, 2, 1), end_date=datetime(2023, 2, 1), backfill=True)
    output_df = captured["df"]

    for feature in FEATURE_NAMES:
        assert output_df.loc[0, f"jpy_{feature}"] == 0.0
    for zone in ZONE_NODES:
        if zone == "JPY":
            continue
        assert output_df.loc[0, f"edge_jpy_{zone.lower()}"] == 0.0


def test_zone_feature_builder_edge_directionality(monkeypatch: pytest.MonkeyPatch) -> None:
    event_date = pd.Timestamp("2023-02-01", tz="UTC")
    df = pd.DataFrame(
        {
            "event_date": [event_date],
            "actor1_country_code": ["USA"],
            "actor2_country_code": ["DEU"],
            "goldstein_scale": [1.0],
            "avg_tone": [0.1],
            "num_mentions": [5],
            "quad_class": [1],
        }
    )

    builder = GDELTZoneFeatureBuilder(
        clean_silver_dir=Path("gdelt_events"),
        output_path=Path("zone_features_daily.parquet"),
    )
    monkeypatch.setattr(builder, "_discover_parquet_paths", lambda *args: [Path("mock.parquet")])
    monkeypatch.setattr(pd, "read_parquet", lambda *args, **kwargs: df)

    captured: dict[str, pd.DataFrame] = {}

    def capture_to_parquet(self: pd.DataFrame, *args: object, **kwargs: object) -> None:
        captured["df"] = self.copy()

    monkeypatch.setattr(pd.DataFrame, "to_parquet", capture_to_parquet, raising=False)

    builder.run(start_date=datetime(2023, 2, 1), end_date=datetime(2023, 2, 1), backfill=True)
    output_df = captured["df"]

    assert output_df.loc[0, "edge_usd_eur"] > 0.0
    assert output_df.loc[0, "edge_eur_usd"] == 0.0
