from __future__ import annotations

import pandas as pd
import pytest

from src.agents.macro import CalendarEventsNode


@pytest.fixture
def base_ts() -> pd.Timestamp:
    return pd.Timestamp("2025-02-28 00:00:00+00:00")


@pytest.fixture
def mock_scores() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp_utc": [
                pd.Timestamp("2025-02-10 10:00:00+00:00"),
                pd.Timestamp("2025-02-12 11:00:00+00:00"),
                pd.Timestamp("2025-01-20 09:00:00+00:00"),
            ],
            "pair": ["EURUSD", "EURUSD", "EURUSD"],
            "event_id": ["e1", "e2", "e3"],
            "event_name": ["CPI", "NFP", "Retail"],
            "country": ["US", "US", "US"],
            "has_surprise_data": [1, 1, 1],
            "surprise_direction": [1.0, -1.0, 1.0],
            "country_sign": [-1.0, -1.0, -1.0],
            "impact_weight": [1.0, 0.5, 1.0],
            "prob": [0.8, 0.6, 0.7],
        }
    )


@pytest.fixture
def node(mock_scores: pd.DataFrame) -> CalendarEventsNode:
    n = CalendarEventsNode()
    n._event_scores = mock_scores.copy()
    return n


def test_imports() -> None:
    assert CalendarEventsNode is not None


def test_audusd_returns_zero(base_ts: pd.Timestamp) -> None:
    n = CalendarEventsNode()
    n._event_scores = pd.DataFrame(
        columns=[
            "timestamp_utc",
            "pair",
            "event_id",
            "event_name",
            "country",
            "has_surprise_data",
            "surprise_direction",
            "country_sign",
            "impact_weight",
            "prob",
        ]
    )
    assert n.predict("AUDUSD", base_ts) == 0.0


def test_predict_before_load_raises(base_ts: pd.Timestamp) -> None:
    n = CalendarEventsNode()
    with pytest.raises(RuntimeError):
        n.predict("EURUSD", base_ts)


def test_no_events_in_window_returns_zero(base_ts: pd.Timestamp) -> None:
    n = CalendarEventsNode()
    n._event_scores = pd.DataFrame(
        {
            "timestamp_utc": [pd.Timestamp("2025-01-10 00:00:00+00:00")],
            "pair": ["EURUSD"],
            "event_id": ["e1"],
            "event_name": ["CPI"],
            "country": ["US"],
            "has_surprise_data": [1],
            "surprise_direction": [1.0],
            "country_sign": [-1.0],
            "impact_weight": [1.0],
            "prob": [0.9],
        }
    )
    assert n.predict("EURUSD", base_ts) == 0.0


def test_positive_surprise_positive_score(base_ts: pd.Timestamp) -> None:
    n = CalendarEventsNode()
    n._event_scores = pd.DataFrame(
        {
            "timestamp_utc": [pd.Timestamp("2025-02-15 00:00:00+00:00")],
            "pair": ["EURUSD"],
            "event_id": ["e1"],
            "event_name": ["CPI"],
            "country": ["US"],
            "has_surprise_data": [1],
            "surprise_direction": [1.0],
            "country_sign": [-1.0],
            "impact_weight": [1.0],
            "prob": [0.8],
        }
    )
    assert n.predict("EURUSD", base_ts) == -1.0


def test_mixed_surprises(base_ts: pd.Timestamp) -> None:
    n = CalendarEventsNode()
    n._event_scores = pd.DataFrame(
        {
            "timestamp_utc": [
                pd.Timestamp("2025-02-10 00:00:00+00:00"),
                pd.Timestamp("2025-02-11 00:00:00+00:00"),
            ],
            "pair": ["USDJPY", "USDJPY"],
            "event_id": ["e1", "e2"],
            "event_name": ["GDP", "CPI"],
            "country": ["US", "JP"],
            "has_surprise_data": [1, 1],
            "surprise_direction": [1.0, -1.0],
            "country_sign": [1.0, -1.0],
            "impact_weight": [1.0, 1.0],
            "prob": [0.9, 0.4],
        }
    )
    score = n.predict("USDJPY", base_ts)
    assert -1.0 <= score <= 1.0
    assert score > 0.0


def test_score_clipped_to_bounds(base_ts: pd.Timestamp) -> None:
    n = CalendarEventsNode()
    n._event_scores = pd.DataFrame(
        {
            "timestamp_utc": [
                pd.Timestamp("2025-02-10 00:00:00+00:00"),
                pd.Timestamp("2025-02-11 00:00:00+00:00"),
            ],
            "pair": ["USDCHF", "USDCHF"],
            "event_id": ["e1", "e2"],
            "event_name": ["GDP", "CPI"],
            "country": ["US", "US"],
            "has_surprise_data": [1, 1],
            "surprise_direction": [2.0, 2.0],
            "country_sign": [1.0, 1.0],
            "impact_weight": [1.0, 1.0],
            "prob": [0.8, 0.6],
        }
    )
    assert n.predict("USDCHF", base_ts) == 1.0


def test_country_sign_applied(base_ts: pd.Timestamp) -> None:
    n = CalendarEventsNode()

    pos = pd.DataFrame(
        {
            "timestamp_utc": [pd.Timestamp("2025-02-10 00:00:00+00:00")],
            "pair": ["USDJPY"],
            "event_id": ["e1"],
            "event_name": ["GDP"],
            "country": ["US"],
            "has_surprise_data": [1],
            "surprise_direction": [1.0],
            "country_sign": [1.0],
            "impact_weight": [1.0],
            "prob": [0.9],
        }
    )
    n._event_scores = pos
    pos_score = n.predict("USDJPY", base_ts)

    neg = pos.copy()
    neg["country_sign"] = -1.0
    n._event_scores = neg
    neg_score = n.predict("USDJPY", base_ts)

    assert pos_score == -neg_score


def test_events_outside_window_excluded(base_ts: pd.Timestamp) -> None:
    n = CalendarEventsNode()
    n._event_scores = pd.DataFrame(
        {
            "timestamp_utc": [
                pd.Timestamp("2025-02-28 00:00:00+00:00"),
                pd.Timestamp("2025-03-01 00:00:00+00:00"),
            ],
            "pair": ["EURUSD", "EURUSD"],
            "event_id": ["e1", "e2"],
            "event_name": ["CPI", "NFP"],
            "country": ["US", "US"],
            "has_surprise_data": [1, 1],
            "surprise_direction": [1.0, 1.0],
            "country_sign": [-1.0, -1.0],
            "impact_weight": [1.0, 1.0],
            "prob": [0.8, 0.8],
        }
    )
    score = n.predict("EURUSD", base_ts)
    assert score == -1.0
