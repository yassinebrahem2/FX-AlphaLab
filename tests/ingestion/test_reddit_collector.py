import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import requests

ROOT_DIR = Path(__file__).resolve().parents[2]
REDDIT_COLLECTOR_PATH = ROOT_DIR / "src" / "ingestion" / "collectors" / "reddit_collector.py"
REDDIT_COLLECTOR_SPEC = importlib.util.spec_from_file_location(
    "reddit_collector_under_test", REDDIT_COLLECTOR_PATH
)
assert REDDIT_COLLECTOR_SPEC is not None and REDDIT_COLLECTOR_SPEC.loader is not None
reddit_module = importlib.util.module_from_spec(REDDIT_COLLECTOR_SPEC)
sys.modules[REDDIT_COLLECTOR_SPEC.name] = reddit_module
REDDIT_COLLECTOR_SPEC.loader.exec_module(reddit_module)
RedditCollector = reddit_module.RedditCollector


@pytest.fixture
def collector(tmp_path: Path) -> RedditCollector:
    return RedditCollector(
        output_dir=tmp_path / "data" / "raw" / "reddit",
        subreddits=["Forex"],
        batch_size=2,
        request_delay=0.01,
        log_file=None,
    )


def _post(post_id: str, created_utc: int, **extra: object) -> dict:
    post = {"id": post_id, "created_utc": created_utc}
    post.update(extra)
    return post


def _make_response(payload: dict | None = None, json_error: Exception | None = None) -> Mock:
    response = Mock()
    response.raise_for_status = Mock()
    if json_error is not None:
        response.json.side_effect = json_error
    else:
        response.json.return_value = payload or {"data": []}
    return response


def test_load_canonical_state_reads_max_timestamp_and_ids(collector: RedditCollector) -> None:
    canonical_path = collector._canonical_path("Forex")
    canonical_path.parent.mkdir(parents=True, exist_ok=True)
    canonical_path.write_text(
        "\n".join(
            [
                json.dumps(_post("a", 1704067200), ensure_ascii=False),
                "not-json",
                json.dumps(_post("b", 1704068400), ensure_ascii=False),
                "",
            ]
        ),
        encoding="utf-8",
    )

    last_ts, seen_ids = collector._load_canonical_state(canonical_path)

    assert last_ts == datetime.fromtimestamp(1704068400, tz=timezone.utc)
    assert seen_ids == {"a", "b"}


def test_collect_one_uses_single_canonical_scan(collector: RedditCollector) -> None:
    first_batch = [
        _post("p1", 1704067200),
        _post("p2", 1704067260),
    ]
    second_batch = [_post("p3", 1704067320)]

    with patch.object(
        collector, "_last_collected_ts", side_effect=AssertionError("should not be called")
    ):
        with patch.object(
            collector,
            "_load_canonical_state",
            return_value=(
                datetime.fromtimestamp(1704067199, tz=timezone.utc),
                {"existing"},
            ),
        ) as mock_state:
            with patch.object(
                collector, "_fetch_batch", side_effect=[first_batch, second_batch]
            ) as mock_fetch:
                with patch.object(reddit_module.time, "sleep") as mock_sleep:
                    collector.collect_one(
                        "Forex",
                        end_date=datetime(2024, 1, 10, tzinfo=timezone.utc),
                    )

    assert mock_state.call_count == 1
    assert mock_fetch.call_count == 2
    assert mock_fetch.call_args_list[0].kwargs["after"] == datetime(
        2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc
    )
    assert mock_fetch.call_args_list[1].kwargs["after"] == datetime(
        2024, 1, 1, 0, 1, 1, tzinfo=timezone.utc
    )
    assert mock_sleep.call_count == 1

    canonical_path = collector._canonical_path("Forex")
    lines = canonical_path.read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["id"] for line in lines] == ["p1", "p2", "p3"]


def test_collect_returns_counts_by_subreddit(collector: RedditCollector) -> None:
    with patch.object(collector, "collect_one", side_effect=[2, 0]) as mock_collect_one:
        collector.subreddits = ["Forex", "Stocks"]
        result = collector.collect(
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 2, tzinfo=timezone.utc),
            backfill=True,
        )

    assert result == {"forex": 2, "stocks": 0}
    assert all(isinstance(value, int) and value >= 0 for value in result.values())
    assert mock_collect_one.call_count == 2


def test_fetch_batch_retries_json_decode_error(collector: RedditCollector) -> None:
    bad_response = _make_response(json_error=ValueError("bad json"))
    good_response = _make_response(payload={"data": [_post("p1", 1704067200)]})

    with patch.object(
        reddit_module.requests, "get", side_effect=[bad_response, good_response]
    ) as mock_get:
        with patch.object(reddit_module.time, "sleep") as mock_sleep:
            result = collector._fetch_batch(
                "Forex",
                after=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
                before=datetime(2024, 1, 2, 0, 0, tzinfo=timezone.utc),
            )

    assert result == [_post("p1", 1704067200)]
    assert mock_get.call_count == 2
    assert mock_sleep.call_args_list[0].args[0] == pytest.approx(1.0)


def test_fetch_batch_returns_empty_after_all_json_errors(collector: RedditCollector) -> None:
    bad_response = _make_response(json_error=ValueError("bad json"))

    with patch.object(reddit_module.requests, "get", return_value=bad_response) as mock_get:
        with patch.object(reddit_module.time, "sleep"):
            result = collector._fetch_batch(
                "Forex",
                after=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
                before=datetime(2024, 1, 2, 0, 0, tzinfo=timezone.utc),
            )

    assert result == []
    assert mock_get.call_count == collector.MAX_RETRIES


def test_write_posts_deduplicates_seen_ids(collector: RedditCollector, tmp_path: Path) -> None:
    file_path = tmp_path / "posts.jsonl"
    posts = [
        _post("a", 1704067200),
        _post("b", 1704067260),
        _post("b", 1704067300),
    ]
    seen_ids = {"a"}

    with file_path.open("w", encoding="utf-8") as file_handle:
        written = collector._write_posts(posts, file_handle, seen_ids)

    assert written == 1
    assert seen_ids == {"a", "b"}
    assert [
        json.loads(line)["id"] for line in file_path.read_text(encoding="utf-8").splitlines()
    ] == ["b"]


@pytest.mark.parametrize(
    ("status_code", "expected"),
    [
        (200, True),
        (503, False),
    ],
)
def test_health_check_returns_boolean(
    collector: RedditCollector, status_code: int, expected: bool
) -> None:
    response = Mock()
    response.status_code = status_code

    with patch.object(reddit_module.requests, "get", return_value=response):
        assert collector.health_check() is expected


def test_health_check_handles_request_exception(collector: RedditCollector) -> None:
    with patch.object(
        reddit_module.requests, "get", side_effect=requests.RequestException("timeout")
    ):
        assert collector.health_check() is False
