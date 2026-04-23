import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from src.agents.sentiment.stocktwits_node import StocktwitsSignalNode


def _write_jsonl(path: Path, rows: list[dict], malformed_line: str | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file_handle:
        for row in rows:
            file_handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        if malformed_line is not None:
            file_handle.write(malformed_line + "\n")


def test_compute_raises_on_missing_or_empty_checkpoint(tmp_path: Path) -> None:
    missing = tmp_path / "labels_checkpoint.jsonl"
    node = StocktwitsSignalNode(checkpoint_path=missing)

    with pytest.raises(ValueError, match="Checkpoint is empty"):
        node.compute(
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 3, tzinfo=timezone.utc),
        )

    empty = tmp_path / "empty.jsonl"
    empty.write_text("\n", encoding="utf-8")
    node_empty = StocktwitsSignalNode(checkpoint_path=empty)

    with pytest.raises(ValueError, match="Checkpoint is empty"):
        node_empty.compute(
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 3, tzinfo=timezone.utc),
        )


def test_compute_structure_shape_safe_division_and_malformed_skip(tmp_path: Path) -> None:
    checkpoint = tmp_path / "labels_checkpoint.jsonl"
    rows = [
        {
            "message_id": "a",
            "symbol": "EURUSD",
            "timestamp_published": "2026-01-01T12:00:00Z",
            "prob_bullish": 0.90,
            "prob_bearish": 0.10,
        },
        {
            "message_id": "b",
            "symbol": "GBPUSD",
            "timestamp_published": "2026-01-03T09:00:00Z",
            "prob_bullish": 0.40,
            "prob_bearish": 0.60,
        },
    ]
    _write_jsonl(checkpoint, rows, malformed_line="{bad-json")

    node = StocktwitsSignalNode(checkpoint_path=checkpoint)
    global_daily, pair_daily = node.compute(
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime(2026, 1, 4, tzinfo=timezone.utc),
    )

    assert list(global_daily.columns) == [
        "date",
        "raw_post_count",
        "signal_post_count",
        "bullish_score",
        "bearish_score",
        "net_sentiment",
        "mean_model_confidence",
    ]
    assert list(pair_daily.columns) == [
        "date",
        "symbol",
        "raw_post_count",
        "signal_post_count",
        "bullish_score",
        "bearish_score",
        "net_sentiment",
        "mean_model_confidence",
    ]

    assert len(global_daily) == 4
    assert len(pair_daily) == 4 * len(node.PAIR_LIST)

    day_without_posts = pd.Timestamp("2026-01-02", tz="UTC")
    row = global_daily.loc[global_daily["date"] == day_without_posts].iloc[0]
    assert float(row["raw_post_count"]) == 0.0
    assert float(row["signal_post_count"]) > 0.0
    assert not pd.isna(row["bullish_score"])
    assert not pd.isna(row["bearish_score"])

    assert ((global_daily["net_sentiment"] >= -1.0) & (global_daily["net_sentiment"] <= 1.0)).all()


def test_get_signal_returns_expected_keys_and_raises_when_no_data(tmp_path: Path) -> None:
    checkpoint = tmp_path / "labels_checkpoint.jsonl"
    _write_jsonl(
        checkpoint,
        [
            {
                "message_id": "x1",
                "symbol": "USDJPY",
                "timestamp_published": "2026-01-10T10:00:00Z",
                "prob_bullish": 0.72,
                "prob_bearish": 0.28,
            }
        ],
    )

    node = StocktwitsSignalNode(checkpoint_path=checkpoint)
    signal = node.get_signal(datetime(2026, 1, 12, tzinfo=timezone.utc))

    assert set(signal.keys()) == {
        "bullish_score",
        "bearish_score",
        "net_sentiment",
        "signal_post_count",
        "mean_model_confidence",
    }
    assert signal["signal_post_count"] > 0.0

    old_checkpoint = tmp_path / "old_labels_checkpoint.jsonl"
    _write_jsonl(
        old_checkpoint,
        [
            {
                "message_id": "o1",
                "symbol": "EURUSD",
                "timestamp_published": "2025-10-01T10:00:00Z",
                "prob_bullish": 0.55,
                "prob_bearish": 0.45,
            }
        ],
    )
    old_node = StocktwitsSignalNode(checkpoint_path=old_checkpoint)

    with pytest.raises(ValueError, match="No signal data available"):
        old_node.get_signal(datetime(2026, 1, 12, tzinfo=timezone.utc))
