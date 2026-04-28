from datetime import datetime, timezone
from unittest.mock import Mock

import pandas as pd
import pytest

from src.agents.sentiment.agent import SentimentAgent, SentimentSignal


@pytest.fixture
def mock_stocktwits_node():
    """Returns (global_daily, pair_daily) with one active USDJPY row."""
    node = Mock()
    date = pd.Timestamp("2024-06-15", tz="UTC")
    global_daily = pd.DataFrame([{"date": date, "signal_post_count": 1.0, "net_sentiment": 0.2}])
    pair_daily = pd.DataFrame(
        [
            {
                "date": date,
                "symbol": "USDJPY",
                "raw_post_count": 1.0,
                "signal_post_count": 1.0,
                "bullish_score": 0.6,
                "bearish_score": 0.4,
                "net_sentiment": 0.2,
                "mean_model_confidence": 0.9,
            }
        ]
    )
    node.compute = Mock(return_value=(global_daily, pair_daily))
    return node


@pytest.fixture
def mock_reddit_node():
    """Returns (global_daily, pair_daily) with minimal Reddit data."""
    node = Mock()
    date = pd.Timestamp("2024-06-15", tz="UTC")
    global_daily = pd.DataFrame([{"date": date, "signal_post_count": 10.0}])
    pair_daily = pd.DataFrame(
        [
            {
                "date": date,
                "pair": "USDJPY",
                "signal_post_count": 2.0,
                "risk_off_score": 0.7,
                "risk_on_score": 0.3,
                "sentiment_strength_wmean": 0.5,
            }
        ]
    )
    node.compute = Mock(return_value=(global_daily, pair_daily))
    return node


@pytest.fixture
def mock_gdelt_node():
    """Returns DataFrame with tone_zscore and attention_zscore."""
    node = Mock()
    date = pd.Timestamp("2024-06-15")
    df = pd.DataFrame(
        [
            {
                "date": date,
                "tone_mean": 0.0,
                "article_count": 5,
                "tone_zscore": 0.5,
                "attention_zscore": 1.2,
            }
        ]
    )
    node.compute = Mock(return_value=df)
    return node


@pytest.fixture
def mock_gtrends_node():
    """Returns DataFrame with macro_attention_zscore."""
    node = Mock()
    date = pd.Timestamp("2024-06-15")
    df = pd.DataFrame([{"date": date, "macro_attention_zscore": 1.3}])
    node.compute = Mock(return_value=df)
    return node


@pytest.fixture
def agent(mock_stocktwits_node, mock_reddit_node, mock_gdelt_node, mock_gtrends_node):
    """SentimentAgent wired with all four mocked nodes."""
    return SentimentAgent(
        stocktwits_node=mock_stocktwits_node,
        reddit_node=mock_reddit_node,
        gdelt_node=mock_gdelt_node,
        gtrends_node=mock_gtrends_node,
    )


def test_get_signal_returns_sentiment_signal_type(agent):
    sig = agent.get_signal(datetime(2024, 6, 15, tzinfo=timezone.utc))
    assert isinstance(sig, SentimentSignal)


def test_usdjpy_active_when_signal_present(agent):
    sig = agent.get_signal(datetime(2024, 6, 15, tzinfo=timezone.utc))
    assert sig.usdjpy_stocktwits_active is True
    assert isinstance(sig.usdjpy_stocktwits_net_sentiment, float)


def test_usdjpy_null_when_no_signal(mock_reddit_node, mock_gdelt_node, mock_gtrends_node):
    # stocktwits node returns zero signal_post_count
    node = Mock()
    date = pd.Timestamp("2024-06-15", tz="UTC")
    global_daily = pd.DataFrame([{"date": date, "signal_post_count": 0.0, "net_sentiment": 0.0}])
    pair_daily = pd.DataFrame(
        [
            {
                "date": date,
                "symbol": "USDJPY",
                "raw_post_count": 0.0,
                "signal_post_count": 0.0,
                "bullish_score": 0.0,
                "bearish_score": 0.0,
                "net_sentiment": 0.0,
                "mean_model_confidence": 0.0,
            }
        ]
    )
    node.compute = Mock(return_value=(global_daily, pair_daily))

    agent_local = SentimentAgent(
        stocktwits_node=node,
        reddit_node=mock_reddit_node,
        gdelt_node=mock_gdelt_node,
        gtrends_node=mock_gtrends_node,
    )

    sig = agent_local.get_signal(datetime(2024, 6, 15, tzinfo=timezone.utc))
    assert sig.usdjpy_stocktwits_active is False
    assert sig.usdjpy_stocktwits_net_sentiment is None


def test_composite_stress_flag_fires_on_two_signals(agent):
    sig = agent.get_signal(datetime(2024, 6, 15, tzinfo=timezone.utc))
    assert sig.composite_stress_flag is True


def test_composite_stress_flag_off_on_one_signal(mock_stocktwits_node, mock_reddit_node):
    # only gdelt elevated
    gdelt = Mock()
    date = pd.Timestamp("2024-06-15")
    gdelt_df = pd.DataFrame(
        [
            {
                "date": date,
                "tone_mean": 0.0,
                "article_count": 5,
                "tone_zscore": 0.5,
                "attention_zscore": 1.2,
            }
        ]
    )
    gtrends = Mock()
    gtrends_df = pd.DataFrame([{"date": date, "macro_attention_zscore": 0.5}])

    agent_local = SentimentAgent(
        stocktwits_node=mock_stocktwits_node,
        reddit_node=mock_reddit_node,
        gdelt_node=gdelt,
        gtrends_node=gtrends,
    )
    gdelt.compute = Mock(return_value=gdelt_df)
    gtrends.compute = Mock(return_value=gtrends_df)

    sig = agent_local.get_signal(datetime(2024, 6, 15, tzinfo=timezone.utc))
    assert sig.composite_stress_flag is False


def test_composite_stress_flag_off_when_both_none(mock_stocktwits_node, mock_reddit_node):
    gdelt = Mock()
    gdelt_df = pd.DataFrame(
        [
            {
                "date": pd.Timestamp("2024-06-15"),
                "tone_mean": pd.NA,
                "article_count": 0,
                "tone_zscore": pd.NA,
                "attention_zscore": pd.NA,
            }
        ]
    )
    gtrends = Mock()
    gtrends_df = pd.DataFrame(
        [{"date": pd.Timestamp("2024-06-15"), "macro_attention_zscore": pd.NA}]
    )

    agent_local = SentimentAgent(
        stocktwits_node=mock_stocktwits_node,
        reddit_node=mock_reddit_node,
        gdelt_node=gdelt,
        gtrends_node=gtrends,
    )
    gdelt.compute = Mock(return_value=gdelt_df)
    gtrends.compute = Mock(return_value=gtrends_df)

    sig = agent_local.get_signal(datetime(2024, 6, 15, tzinfo=timezone.utc))
    assert sig.composite_stress_flag is False


def test_compute_batch_returns_dataframe(agent):
    df = agent.compute_batch(
        datetime(2024, 6, 14, tzinfo=timezone.utc), datetime(2024, 6, 16, tzinfo=timezone.utc)
    )
    expected_cols = [
        "timestamp_utc",
        "usdjpy_stocktwits_net_sentiment",
        "usdjpy_stocktwits_active",
        "gdelt_tone_zscore",
        "gdelt_attention_zscore",
        "macro_attention_zscore",
        "composite_stress_flag",
    ]
    assert list(df.columns) == expected_cols
    assert df["timestamp_utc"].is_monotonic_increasing


def test_compute_batch_shape(agent):
    df = agent.compute_batch(
        datetime(2024, 6, 14, tzinfo=timezone.utc), datetime(2024, 6, 16, tzinfo=timezone.utc)
    )
    assert len(df) == 3


def test_context_none_when_not_requested(agent):
    df = agent.compute_batch(
        datetime(2024, 6, 14, tzinfo=timezone.utc),
        datetime(2024, 6, 16, tzinfo=timezone.utc),
        include_context=False,
    )
    assert "context" not in df.columns


def test_context_populated_when_requested(agent):
    df = agent.compute_batch(
        datetime(2024, 6, 14, tzinfo=timezone.utc),
        datetime(2024, 6, 16, tzinfo=timezone.utc),
        include_context=True,
    )
    assert "context" in df.columns
    row = df.loc[df["timestamp_utc"] == pd.Timestamp("2024-06-15", tz="UTC")].iloc[0]
    assert isinstance(row["context"], dict)
    assert "reddit_global_activity_zscore" in row["context"]
    assert "reddit_pair_views" in row["context"]
    assert "stocktwits_pair_views" in row["context"]


def test_gdelt_node_failure_does_not_raise(
    mock_stocktwits_node, mock_reddit_node, mock_gtrends_node
):
    gdelt = Mock()
    gdelt.compute = Mock(side_effect=Exception("boom"))
    agent_local = SentimentAgent(
        stocktwits_node=mock_stocktwits_node,
        reddit_node=mock_reddit_node,
        gdelt_node=gdelt,
        gtrends_node=mock_gtrends_node,
    )
    sig = agent_local.get_signal(datetime(2024, 6, 15, tzinfo=timezone.utc))
    assert sig.gdelt_tone_zscore is None
    assert sig.gdelt_attention_zscore is None


def test_gtrends_node_failure_does_not_raise(
    mock_stocktwits_node, mock_reddit_node, mock_gdelt_node
):
    gtrends = Mock()
    gtrends.compute = Mock(side_effect=Exception("boom"))
    agent_local = SentimentAgent(
        stocktwits_node=mock_stocktwits_node,
        reddit_node=mock_reddit_node,
        gdelt_node=mock_gdelt_node,
        gtrends_node=gtrends,
    )
    sig = agent_local.get_signal(datetime(2024, 6, 15, tzinfo=timezone.utc))
    assert sig.macro_attention_zscore is None


def test_get_signal_raises_for_missing_date(agent):
    with pytest.raises(ValueError):
        agent.get_signal(datetime(2023, 1, 1, tzinfo=timezone.utc))
