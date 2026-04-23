import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from src.agents.sentiment.reddit_node import RedditSignalNode


def _to_unix(dt: datetime) -> int:
    return int(dt.timestamp())


def _write_jsonl(path: Path, rows: list[dict], malformed_line: str | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file_handle:
        for row in rows:
            file_handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        if malformed_line is not None:
            file_handle.write(malformed_line + "\n")


def test_compute_raises_on_missing_or_empty_checkpoint(tmp_path: Path) -> None:
    missing = tmp_path / "labels_checkpoint.jsonl"
    node = RedditSignalNode(checkpoint_path=missing)

    with pytest.raises(ValueError, match="Checkpoint is empty"):
        node.compute(
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 3, tzinfo=timezone.utc),
        )

    empty = tmp_path / "empty.jsonl"
    empty.write_text("\n", encoding="utf-8")
    node_empty = RedditSignalNode(checkpoint_path=empty)

    with pytest.raises(ValueError, match="Checkpoint is empty"):
        node_empty.compute(
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 3, tzinfo=timezone.utc),
        )


def test_compute_builds_global_and_pair_tables_with_decay_and_fallback_pair(tmp_path: Path) -> None:
    checkpoint = tmp_path / "labels_checkpoint.jsonl"

    d1 = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    d2 = datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc)

    rows = [
        {
            "id": "a",
            "content_type": "FUNDAMENTAL",
            "risk_sentiment": "RISK_OFF",
            "sentiment_strength": -2,
            "target_pair": "EUR/USD",
            "score": 100,
            "created_utc": _to_unix(d1),
            "subreddit": "Forex",
        },
        {
            "id": "b",
            "content_type": "TECHNICAL",
            "risk_sentiment": "RISK_ON",
            "sentiment_strength": 1,
            "target_pair": None,
            "title": "Watching GBP/USD breakout",
            "body": "Looking for continuation above resistance",
            "score": 10,
            "created_utc": _to_unix(d2),
            "subreddit": "Forex",
        },
        {
            "id": "c",
            "content_type": "NOISE",
            "risk_sentiment": "NEUTRAL",
            "sentiment_strength": 0,
            "target_pair": None,
            "score": 3,
            "created_utc": _to_unix(d2),
            "subreddit": "Forex",
        },
    ]
    _write_jsonl(checkpoint, rows, malformed_line="{bad-json")

    node = RedditSignalNode(checkpoint_path=checkpoint)

    global_daily, pair_daily = node.compute(
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime(2026, 1, 3, tzinfo=timezone.utc),
    )

    assert list(global_daily.columns) == [
        "date",
        "raw_post_count",
        "signal_post_count",
        "risk_off_score",
        "risk_on_score",
        "sentiment_strength_wmean",
        "fundamental_share",
        "confidence",
    ]
    assert list(pair_daily.columns) == [
        "date",
        "pair",
        "raw_post_count",
        "signal_post_count",
        "risk_off_score",
        "risk_on_score",
        "sentiment_strength_wmean",
        "fundamental_share",
        "confidence",
    ]

    assert len(global_daily) == 3
    assert len(pair_daily) == 3 * len(node.PAIR_LIST)

    day2 = pd.Timestamp("2026-01-02", tz="UTC")
    global_day2 = global_daily.loc[global_daily["date"] == day2].iloc[0]
    assert int(global_day2["raw_post_count"]) == 2

    expected_confidence = (
        float(global_day2["signal_post_count"]) / float(global_day2["raw_post_count"])
        if float(global_day2["raw_post_count"]) > 0
        else 0.0
    )
    assert float(global_day2["confidence"]) == pytest.approx(expected_confidence)

    gbpusd_day2 = pair_daily[(pair_daily["date"] == day2) & (pair_daily["pair"] == "GBPUSD")].iloc[
        0
    ]
    assert float(gbpusd_day2["signal_post_count"]) > 0.0


def test_get_signal_returns_latest_available_and_raises_without_signal(tmp_path: Path) -> None:
    checkpoint = tmp_path / "labels_checkpoint.jsonl"

    rows = [
        {
            "id": "a",
            "content_type": "NEWS_REACTION",
            "risk_sentiment": "RISK_OFF",
            "sentiment_strength": -1,
            "target_pair": "USDJPY",
            "score": 25,
            "created_utc": _to_unix(datetime(2026, 1, 2, 10, 0, tzinfo=timezone.utc)),
            "subreddit": "investing",
        }
    ]
    _write_jsonl(checkpoint, rows)

    node = RedditSignalNode(checkpoint_path=checkpoint)
    signal = node.get_signal(datetime(2026, 1, 4, tzinfo=timezone.utc))

    assert set(signal.keys()) == {
        "risk_off_score",
        "risk_on_score",
        "sentiment_strength_wmean",
        "signal_post_count",
        "confidence",
    }
    assert signal["signal_post_count"] > 0.0

    only_noise = tmp_path / "noise.jsonl"
    _write_jsonl(
        only_noise,
        [
            {
                "id": "n1",
                "content_type": "NOISE",
                "risk_sentiment": "NEUTRAL",
                "sentiment_strength": 0,
                "target_pair": None,
                "score": 1,
                "created_utc": _to_unix(datetime(2026, 1, 2, 10, 0, tzinfo=timezone.utc)),
                "subreddit": "Forex",
            }
        ],
    )
    noise_node = RedditSignalNode(checkpoint_path=only_noise)

    with pytest.raises(ValueError, match="No signal data available"):
        noise_node.get_signal(datetime(2026, 1, 4, tzinfo=timezone.utc))
