import importlib.util
import json
import sys
import types
from pathlib import Path
from unittest.mock import Mock, patch

import pytest


def _load_reddit_module() -> types.ModuleType:
    root = Path(__file__).resolve().parents[2]
    module_path = root / "src" / "ingestion" / "preprocessors" / "reddit_preprocessor.py"

    if "groq" not in sys.modules:
        groq_stub = types.ModuleType("groq")

        class _StubRateLimitError(Exception):
            pass

        class _StubGroq:
            def __init__(self, api_key: str):
                self.api_key = api_key
                self.chat = types.SimpleNamespace(
                    completions=types.SimpleNamespace(create=lambda **_: None)
                )
                self.models = types.SimpleNamespace(list=lambda: None)

        groq_stub.Groq = _StubGroq
        groq_stub.RateLimitError = _StubRateLimitError
        sys.modules["groq"] = groq_stub

    spec = importlib.util.spec_from_file_location("reddit_preprocessor_under_test", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


reddit_module = _load_reddit_module()
RedditPreprocessor = reddit_module.RedditPreprocessor


@pytest.fixture
def preprocessor(tmp_path: Path):
    raw_dir = tmp_path / "raw" / "reddit"
    checkpoint_path = tmp_path / "processed" / "reddit" / "labels_checkpoint.jsonl"
    raw_dir.mkdir(parents=True, exist_ok=True)

    return RedditPreprocessor(
        raw_dir=raw_dir,
        checkpoint_path=checkpoint_path,
        groq_api_keys=["key-1"],
        subreddits=["Forex", "investing", "stocks"],
        max_retries=2,
        log_file=None,
    )


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file_handle:
        for row in rows:
            file_handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def test_build_user_message_uses_module_max_body_chars() -> None:
    row = {
        "subreddit": "Forex",
        "title": "A" * 20,
        "body": "B" * (reddit_module.MAX_BODY_CHARS + 50),
        "flair": "Charts",
    }

    message = reddit_module.build_user_message(row)

    assert "Subreddit: r/Forex" in message
    assert "Flair: Charts" in message
    assert f"Body: {'B' * reddit_module.MAX_BODY_CHARS}" in message
    assert f"Body: {'B' * (reddit_module.MAX_BODY_CHARS + 1)}" not in message
    assert RedditPreprocessor.MAX_BODY_CHARS == reddit_module.MAX_BODY_CHARS == 1200


def test_label_one_sleeps_on_invalid_labels(preprocessor) -> None:
    bad_payload = {"content_type": "NOISE"}
    response = types.SimpleNamespace(
        choices=[
            types.SimpleNamespace(message=types.SimpleNamespace(content=json.dumps(bad_payload)))
        ]
    )
    client = types.SimpleNamespace(
        chat=types.SimpleNamespace(
            completions=types.SimpleNamespace(create=Mock(return_value=response))
        )
    )

    preprocessor.pool = types.SimpleNamespace(
        acquire=Mock(return_value=("k1", client)),
        on_success=Mock(),
        on_rate_limit=Mock(),
    )

    row = {
        "id": "post-1",
        "subreddit": "Forex",
        "title": "EURUSD setup for London open",
        "body": "Looking for continuation.",
        "flair": "",
    }

    with patch.object(reddit_module.time, "sleep") as mock_sleep:
        result = preprocessor._label_one(row)

    assert result is None
    assert preprocessor.pool.acquire.call_count == preprocessor.max_retries
    assert client.chat.completions.create.call_count == preprocessor.max_retries
    assert mock_sleep.call_count == preprocessor.max_retries
    assert all(call.args[0] == 1 for call in mock_sleep.call_args_list)


def test_run_applies_filters_incremental_checkpoint_and_returns_written_count(
    preprocessor, tmp_path: Path
):
    raw_path = preprocessor.raw_dir / "forex_raw.jsonl"
    checkpoint = preprocessor.checkpoint_path

    valid_a = {
        "id": "7",
        "subreddit": "Forex",
        "title": "EURUSD technical setup for this session",
        "selftext": "",
        "score": 42,
        "created_utc": 1700000000,
        "author": "trader_a",
        "removed_by_category": None,
        "link_flair_text": "Charts",
    }
    valid_b = {
        "id": "8",
        "subreddit": "Forex",
        "title": "View",
        "selftext": "I am long USDJPY with a clear thesis into CPI.",
        "author": "trader_b",
        "removed_by_category": None,
        "link_flair_text": "",
    }

    rows = [
        {
            "id": "1",
            "subreddit": "Forex",
            "title": "Already labeled post with enough content",
            "selftext": "Body",
            "author": "user",
            "removed_by_category": None,
            "link_flair_text": "",
        },
        {
            "id": "2",
            "subreddit": "Forex",
            "title": "Removed",
            "selftext": "Body long enough to pass otherwise",
            "author": "user",
            "removed_by_category": "moderator",
            "link_flair_text": "",
        },
        {
            "id": "3",
            "subreddit": "Forex",
            "title": "Deleted author title long enough",
            "selftext": "Body long enough",
            "author": "[deleted]",
            "removed_by_category": None,
            "link_flair_text": "",
        },
        {
            "id": "4",
            "subreddit": "Forex",
            "title": "short",
            "selftext": "[removed]",
            "author": "user",
            "removed_by_category": None,
            "link_flair_text": "",
        },
        {
            "id": "5",
            "subreddit": "Forex",
            "title": "Valid enough title for structural stage",
            "selftext": "https://example.com",
            "author": "user",
            "removed_by_category": None,
            "link_flair_text": "",
        },
        {
            "id": "6",
            "subreddit": "Forex",
            "title": "tiny",
            "selftext": "small",
            "author": "user",
            "removed_by_category": None,
            "link_flair_text": "",
        },
        valid_a,
        valid_b,
    ]
    _write_jsonl(raw_path, rows)
    _write_jsonl(checkpoint, [{"id": "1", "existing": True}])

    calls: list[str] = []

    def _fake_label(row: dict) -> dict:
        calls.append(row["id"])
        return {
            "id": row["id"],
            "reasoning": "ok",
            "content_type": "NOISE",
            "sarcasm_irony_score": 0,
            "sentiment_strength": 0,
            "target_pair": None,
            "risk_sentiment": "NEUTRAL",
            "target_clarity": 0,
            "stance_clarity": "NONE",
        }

    preprocessor._label_one = _fake_label  # type: ignore[method-assign]

    written = preprocessor.run(backfill=False)

    assert written == 2
    assert calls == ["7", "8"]

    lines = preprocessor.checkpoint_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3
    parsed = [json.loads(line) for line in lines]
    out_ids = [row["id"] for row in parsed]
    assert out_ids == ["1", "7", "8"]
    assert parsed[1]["score"] == 42
    assert parsed[1]["created_utc"] == 1700000000
    assert parsed[1]["subreddit"] == "Forex"
    assert parsed[2]["score"] == 0
    assert parsed[2]["created_utc"] == 0
    assert parsed[2]["subreddit"] == "Forex"


def test_health_check_uses_first_key(preprocessor) -> None:
    first_client = types.SimpleNamespace(models=types.SimpleNamespace(list=Mock(return_value=None)))
    second_client = types.SimpleNamespace(
        models=types.SimpleNamespace(list=Mock(return_value=None))
    )
    preprocessor.pool = types.SimpleNamespace(
        keys=["k1", "k2"],
        clients={"k1": first_client, "k2": second_client},
    )

    ok = preprocessor.health_check()

    assert ok is True
    first_client.models.list.assert_called_once()
    second_client.models.list.assert_not_called()


def test_health_check_returns_false_on_exception(preprocessor) -> None:
    bad_client = types.SimpleNamespace(
        models=types.SimpleNamespace(list=Mock(side_effect=RuntimeError("boom")))
    )
    preprocessor.pool = types.SimpleNamespace(keys=["k1"], clients={"k1": bad_client})

    assert preprocessor.health_check() is False


def test_load_groq_api_keys_stops_at_first_missing(monkeypatch) -> None:
    from src.shared import config as config_module

    monkeypatch.setenv("GROQ_API_KEY_1", "a")
    monkeypatch.setenv("GROQ_API_KEY_2", "b")
    monkeypatch.delenv("GROQ_API_KEY_3", raising=False)
    monkeypatch.setenv("GROQ_API_KEY_4", "d")

    keys = config_module._load_groq_api_keys()

    assert keys == ["a", "b"]


def test_run_backfill_true_relabels_all_posts(preprocessor, tmp_path: Path):
    """Test that backfill=True relabels all posts, including those in checkpoint."""
    raw_path = preprocessor.raw_dir / "forex_raw.jsonl"
    checkpoint = preprocessor.checkpoint_path

    post_a = {
        "id": "7",
        "subreddit": "Forex",
        "title": "EURUSD technical setup for this session",
        "selftext": "",
        "score": 42,
        "created_utc": 1700000000,
        "author": "trader_a",
        "removed_by_category": None,
        "link_flair_text": "Charts",
    }
    post_b = {
        "id": "8",
        "subreddit": "Forex",
        "title": "View",
        "selftext": "I am long USDJPY with a clear thesis into CPI.",
        "author": "trader_b",
        "removed_by_category": None,
        "link_flair_text": "",
    }

    rows = [post_a, post_b]
    _write_jsonl(raw_path, rows)

    # Pre-populate checkpoint with both posts already labeled
    _write_jsonl(
        checkpoint,
        [
            {"id": "7", "existing": True},
            {"id": "8", "existing": True},
        ],
    )

    calls: list[str] = []

    def _fake_label(row: dict) -> dict:
        calls.append(row["id"])
        return {
            "id": row["id"],
            "reasoning": "ok",
            "content_type": "NOISE",
            "sarcasm_irony_score": 0,
            "sentiment_strength": 0,
            "target_pair": None,
            "risk_sentiment": "NEUTRAL",
            "target_clarity": 0,
            "stance_clarity": "NONE",
        }

    preprocessor._label_one = _fake_label  # type: ignore[method-assign]

    # With backfill=True, both posts should be relabeled (not skipped even though in checkpoint)
    written = preprocessor.run(backfill=True)

    assert written == 2
    assert calls == ["7", "8"]

    # Checkpoint should be overwritten (write mode), not appended to
    lines = preprocessor.checkpoint_path.read_text(encoding="utf-8").splitlines()
    # With backfill, only the new labels are written (checkpoint was overwritten)
    assert len(lines) == 2
    parsed = [json.loads(line) for line in lines]
    out_ids = [row["id"] for row in parsed]
    assert out_ids == ["7", "8"]


def test_missing_prompt_file_raises(tmp_path: Path):
    original_prompt = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "ingestion"
        / "preprocessors"
        / "prompts"
        / "reddit_labeling_system.txt"
    )
    backup = original_prompt.with_suffix(".txt.bak_test")
    original_prompt.rename(backup)

    try:
        with pytest.raises(FileNotFoundError, match="System prompt file not found"):
            RedditPreprocessor(
                raw_dir=tmp_path / "raw" / "reddit",
                checkpoint_path=tmp_path / "processed" / "reddit" / "labels_checkpoint.jsonl",
                groq_api_keys=["key-1"],
            )
    finally:
        backup.rename(original_prompt)
