import argparse
import json
import logging
from pathlib import Path
from time import perf_counter

from src.shared.utils import setup_logger

SUBREDDITS = ["Forex", "investing", "stocks"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate legacy Reddit raw files into canonical subreddit JSONL files."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data/raw/reddit"),
        help="Directory containing Reddit raw files (default: data/raw/reddit)",
    )
    return parser.parse_args()


def load_jsonl_posts(path: Path, logger: logging.Logger) -> list[dict]:
    posts: list[dict] = []
    with path.open("r", encoding="utf-8") as file_handle:
        for line_number, line in enumerate(file_handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                item = json.loads(stripped)
            except json.JSONDecodeError as exc:
                logger.warning("Skipping malformed JSON in %s line=%d: %s", path, line_number, exc)
                continue
            if not isinstance(item, dict):
                logger.warning("Skipping non-object JSON in %s line=%d", path, line_number)
                continue
            posts.append(item)
    return posts


def load_forex_legacy_json(path: Path, logger: logging.Logger) -> list[dict]:
    posts: list[dict] = []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        logger.warning("Skipping malformed JSON file %s: %s", path, exc)
        return posts

    candidates: list[dict] = []
    if isinstance(payload, list):
        candidates = [item for item in payload if isinstance(item, dict)]
    elif isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            candidates = [item for item in data if isinstance(item, dict)]
        else:
            candidates = [payload]

    for item in candidates:
        if "id" not in item or "created_utc" not in item:
            logger.warning("Skipping legacy Forex post missing id or created_utc in %s", path)
            continue
        posts.append(item)

    return posts


def sort_key_created_utc(post: dict) -> int:
    return int(post["created_utc"])


def migrate_subreddit(subreddit: str, data_dir: Path, logger: logging.Logger) -> None:
    sub = subreddit.lower()
    canonical_path = data_dir / f"{sub}_raw.jsonl"
    legacy_files = sorted(data_dir.glob(f"{sub}_raw_*.jsonl"))

    extra_forex_json = data_dir / "reddit_forex_arctic_20260221_235933.json"
    files_to_read: list[Path] = []

    if canonical_path.exists():
        files_to_read.append(canonical_path)
    files_to_read.extend(legacy_files)

    if sub == "forex" and extra_forex_json.exists():
        files_to_read.append(extra_forex_json)

    posts_by_id: dict[str, dict] = {}
    files_read = 0
    raw_posts = 0

    for file_path in files_to_read:
        files_read += 1
        if file_path.suffix.lower() == ".jsonl":
            posts = load_jsonl_posts(file_path, logger)
        else:
            posts = load_forex_legacy_json(file_path, logger)

        for post in posts:
            post_id = post.get("id")
            created_utc = post.get("created_utc")
            if post_id is None or created_utc is None:
                logger.warning("Skipping post missing id or created_utc in %s", file_path)
                continue
            try:
                int(created_utc)
            except (TypeError, ValueError):
                logger.warning("Skipping post with invalid created_utc in %s", file_path)
                continue

            raw_posts += 1
            key = str(post_id)
            if key in posts_by_id:
                continue
            posts_by_id[key] = post

    dedup_posts = sorted(posts_by_id.values(), key=sort_key_created_utc)
    canonical_path.parent.mkdir(parents=True, exist_ok=True)
    with canonical_path.open("w", encoding="utf-8") as file_handle:
        for post in dedup_posts:
            file_handle.write(json.dumps(post, ensure_ascii=False) + "\n")

    logger.info(
        "subreddit=%s files_read=%d raw_posts=%d dedup_posts=%d output=%s",
        subreddit,
        files_read,
        raw_posts,
        len(dedup_posts),
        canonical_path,
    )


def main() -> None:
    args = parse_args()
    logger_name = "migrate_reddit_raw"
    logger = setup_logger(logger_name)
    data_dir = args.data_dir

    started_at = perf_counter()
    logger.info("Starting Reddit raw migration in %s", data_dir)

    for subreddit in SUBREDDITS:
        migrate_subreddit(subreddit, data_dir, logger)

    elapsed = perf_counter() - started_at
    logger.info("Migration finished in %.2fs", elapsed)


if __name__ == "__main__":
    main()
