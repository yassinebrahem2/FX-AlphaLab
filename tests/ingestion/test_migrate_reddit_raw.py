import json
import logging
from pathlib import Path

from scripts.migrate_reddit_raw import migrate_subreddit


def _post(post_id: str | None, created_utc: int | None, **extra: object) -> dict:
    post = dict(extra)
    if post_id is not None:
        post["id"] = post_id
    if created_utc is not None:
        post["created_utc"] = created_utc
    return post


def _write_jsonl(path: Path, posts: list[dict], include_malformed: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(post, ensure_ascii=False) for post in posts]
    if include_malformed:
        lines.append("not-json")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _logger() -> logging.Logger:
    logger = logging.getLogger("reddit-migration-test")
    logger.handlers = []
    logger.addHandler(logging.NullHandler())
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


def test_migrate_subreddit_merges_canonical_first_and_is_idempotent(tmp_path: Path) -> None:
    data_dir = tmp_path / "data" / "raw" / "reddit"
    canonical_path = data_dir / "forex_raw.jsonl"
    legacy_jsonl_path = data_dir / "forex_raw_20260101.jsonl"
    legacy_json_path = data_dir / "reddit_forex_arctic_20260221_235933.json"

    _write_jsonl(
        canonical_path,
        [
            _post("keep", 100, origin="canonical"),
        ],
    )
    _write_jsonl(
        legacy_jsonl_path,
        [
            _post("keep", 200, origin="legacy-jsonl"),
            _post("legacy_jsonl", 150, origin="legacy-jsonl"),
            _post(None, 125, origin="missing-id"),
            _post("missing_created", None, origin="missing-created"),
        ],
        include_malformed=True,
    )
    legacy_json_path.parent.mkdir(parents=True, exist_ok=True)
    legacy_json_path.write_text(
        json.dumps(
            [
                _post("legacy_json", 50, origin="legacy-json"),
                _post(None, 60, origin="missing-id"),
                _post("missing_created", None, origin="missing-created"),
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    logger = _logger()

    migrate_subreddit("Forex", data_dir, logger)
    first_run = canonical_path.read_text(encoding="utf-8")

    migrate_subreddit("Forex", data_dir, logger)
    second_run = canonical_path.read_text(encoding="utf-8")

    assert first_run == second_run

    output_posts = [json.loads(line) for line in first_run.splitlines()]
    assert [post["id"] for post in output_posts] == ["legacy_json", "keep", "legacy_jsonl"]
    assert output_posts[1]["origin"] == "canonical"
    assert legacy_jsonl_path.exists()
    assert legacy_json_path.exists()
