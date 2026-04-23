import json
from pathlib import Path
from unittest.mock import Mock

import pytest
import torch

from src.ingestion.preprocessors.stocktwits_preprocessor import (
    StocktwitsPreprocessor,
    preprocess_for_model,
)


def _write_jsonl(path: Path, rows: list[dict], malformed_line: str | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file_handle:
        for row in rows:
            file_handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        if malformed_line is not None:
            file_handle.write(malformed_line + "\n")


class _DummyModelOutput:
    def __init__(self, logits: torch.Tensor) -> None:
        self.logits = logits


class _DummyModel:
    def __init__(self) -> None:
        self.eval_called = False
        self.device = None

    def eval(self):
        self.eval_called = True
        return self

    def to(self, device: str):
        self.device = device
        return self

    def __call__(self, **_: dict) -> _DummyModelOutput:
        return _DummyModelOutput(torch.tensor([[0.2, 0.8], [0.9, 0.1]], dtype=torch.float32))


@pytest.fixture
def preprocessor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> StocktwitsPreprocessor:
    raw_dir = tmp_path / "raw" / "stocktwits"
    checkpoint_path = tmp_path / "processed" / "stocktwits" / "labels_checkpoint.jsonl"
    model_dir = tmp_path / "models" / "sentiment" / "stocktwits"
    raw_dir.mkdir(parents=True, exist_ok=True)
    model_dir.mkdir(parents=True, exist_ok=True)

    tokenizer_mock = Mock(return_value={"input_ids": torch.tensor([[1, 2], [3, 4]])})
    model = _DummyModel()

    monkeypatch.setattr(
        "src.ingestion.preprocessors.stocktwits_preprocessor.AutoTokenizer.from_pretrained",
        Mock(return_value=tokenizer_mock),
    )
    monkeypatch.setattr(
        "src.ingestion.preprocessors.stocktwits_preprocessor.AutoModelForSequenceClassification.from_pretrained",
        Mock(return_value=model),
    )

    return StocktwitsPreprocessor(
        raw_dir=raw_dir,
        checkpoint_path=checkpoint_path,
        model_dir=model_dir,
        batch_size=2,
        device="cpu",
    )


def test_preprocess_for_model_normalizes_url_cashtag_and_symbols() -> None:
    text = "Bullish on $eurusd\nSee https://example.com !!!"

    out = preprocess_for_model(text)

    assert out == "bullish on eurusd see url"


def test_passes_filter_handles_empty_and_non_empty(preprocessor: StocktwitsPreprocessor) -> None:
    assert preprocessor._passes_filter({"body": ""}) is False
    assert preprocessor._passes_filter({"body": "Strong move on EURUSD"}) is True


def test_load_raw_posts_deduplicates_message_id(preprocessor: StocktwitsPreprocessor) -> None:
    f1 = preprocessor.raw_dir / "a.jsonl"
    f2 = preprocessor.raw_dir / "b.jsonl"
    _write_jsonl(
        f1,
        [
            {"message_id": "1", "body": "a"},
            {"message_id": "2", "body": "b"},
        ],
    )
    _write_jsonl(
        f2,
        [
            {"message_id": "2", "body": "dup"},
            {"message_id": "3", "body": "c"},
        ],
        malformed_line="{bad-json",
    )

    posts = preprocessor._load_raw_posts()

    assert [str(p.get("message_id")) for p in posts] == ["1", "2", "3"]


def test_load_labeled_ids_reads_checkpoint(preprocessor: StocktwitsPreprocessor) -> None:
    _write_jsonl(
        preprocessor.checkpoint_path,
        [{"message_id": "m1"}, {"message_id": "m2"}],
        malformed_line="{bad-json",
    )

    labeled_ids = preprocessor._load_labeled_ids()

    assert labeled_ids == {"m1", "m2"}


def test_run_skips_labeled_and_writes_checkpoint(preprocessor: StocktwitsPreprocessor) -> None:
    _write_jsonl(
        preprocessor.raw_dir / "stocktwits_batch.jsonl",
        [
            {
                "message_id": "1",
                "symbol": "EURUSD",
                "timestamp_published": "2026-01-01T10:00:00Z",
                "body": "Old already labeled",
            },
            {
                "message_id": "2",
                "symbol": "USDJPY",
                "timestamp_published": "2026-01-01T11:00:00Z",
                "body": "Fresh signal",
            },
            {
                "message_id": "3",
                "symbol": "GBPUSD",
                "timestamp_published": "2026-01-01T12:00:00Z",
                "body": "Another fresh signal",
            },
        ],
    )
    _write_jsonl(preprocessor.checkpoint_path, [{"message_id": "1", "existing": True}])

    preprocessor._infer_batch = Mock(
        side_effect=[
            [
                {
                    "message_id": "2",
                    "symbol": "USDJPY",
                    "timestamp_published": "2026-01-01T11:00:00Z",
                    "prob_bullish": 0.81,
                    "prob_bearish": 0.19,
                    "predicted_label": "bullish",
                    "model": preprocessor.MODEL,
                },
                {
                    "message_id": "3",
                    "symbol": "GBPUSD",
                    "timestamp_published": "2026-01-01T12:00:00Z",
                    "prob_bullish": 0.22,
                    "prob_bearish": 0.78,
                    "predicted_label": "bearish",
                    "model": preprocessor.MODEL,
                },
            ]
        ]
    )

    written = preprocessor.run()

    assert written == 2
    assert preprocessor._infer_batch.call_count == 1

    parsed = [
        json.loads(line)
        for line in preprocessor.checkpoint_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert [item["message_id"] for item in parsed] == ["1", "2", "3"]


def test_health_check_true_and_false(preprocessor: StocktwitsPreprocessor) -> None:
    assert preprocessor.health_check() is True

    preprocessor.model = Mock(side_effect=RuntimeError("boom"))
    assert preprocessor.health_check() is False
