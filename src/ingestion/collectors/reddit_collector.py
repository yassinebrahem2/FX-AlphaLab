import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import IO

import requests

from src.ingestion.collectors.base_collector import BaseCollector

__all__ = ["RedditCollector"]


class RedditCollector(BaseCollector):
    SOURCE_NAME = "reddit"
    API_BASE = "https://arctic-shift.photon-reddit.com/api/posts/search"
    DEFAULT_LOOKBACK_DAYS = 30
    MAX_RETRIES = 3
    RETRY_BASE_DELAY = 1.0

    def __init__(
        self,
        output_dir: Path,
        subreddits: list[str],
        batch_size: int = 100,
        request_delay: float = 0.5,
        log_file: Path | None = None,
    ) -> None:
        super().__init__(output_dir, log_file)
        self.subreddits = subreddits
        self.batch_size = batch_size
        self.request_delay = request_delay

    def collect(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        backfill: bool = False,
    ) -> dict[str, int]:
        result: dict[str, int] = {}
        for subreddit in self.subreddits:
            result[subreddit.lower()] = self.collect_one(
                subreddit,
                start_date=start_date,
                end_date=end_date,
                backfill=backfill,
            )
        return result

    def collect_one(
        self,
        subreddit: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        backfill: bool = False,
    ) -> int:
        effective_end = (
            self._to_utc(end_date) if end_date is not None else datetime.now(timezone.utc)
        )
        effective_start = self._to_utc(start_date) if start_date is not None else None
        lookback_start = effective_end - timedelta(days=self.DEFAULT_LOOKBACK_DAYS)

        path = self._canonical_path(subreddit)
        last_ts, seen_ids = self._load_canonical_state(path)
        if last_ts is not None:
            if backfill:
                cursor = last_ts + timedelta(seconds=1)
            else:
                cursor = max(last_ts + timedelta(seconds=1), lookback_start)
        elif backfill and effective_start is not None:
            cursor = effective_start
        else:
            cursor = lookback_start

        self.logger.info(
            "Starting subreddit=%s cursor=%s end=%s seen_ids=%d",
            subreddit,
            cursor.isoformat(),
            effective_end.isoformat(),
            len(seen_ids),
        )

        total_written = 0
        with path.open("a", encoding="utf-8") as file_handle:
            while cursor < effective_end:
                posts = self._fetch_batch(subreddit=subreddit, after=cursor, before=effective_end)
                batch_count = len(posts)
                if batch_count == 0:
                    break

                written = self._write_posts(posts, file_handle, seen_ids)
                total_written += written

                max_created = self._max_created_utc(posts)
                if max_created is None:
                    self.logger.warning(
                        "No valid created_utc in batch for subreddit=%s cursor=%s; stopping.",
                        subreddit,
                        cursor.isoformat(),
                    )
                    break

                cursor = max_created + timedelta(seconds=1)
                self.logger.info(
                    "Subreddit=%s batch=%d written=%d next_cursor=%s",
                    subreddit,
                    batch_count,
                    written,
                    cursor.isoformat(),
                )

                if batch_count < self.batch_size:
                    break

                time.sleep(self.request_delay)

        self.logger.info("Finished subreddit=%s total_written=%d", subreddit, total_written)
        return total_written

    def health_check(self) -> bool:
        subreddit = self.subreddits[0] if self.subreddits else "Forex"
        params = {
            "subreddit": subreddit,
            "after": (datetime.now(timezone.utc) - timedelta(days=1)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            "before": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "limit": 1,
            "sort": "asc",
        }
        try:
            response = requests.get(self.API_BASE, params=params, timeout=30)
            ok = response.status_code == 200
            if ok:
                self.logger.info("Reddit health_check succeeded with status_code=200")
            else:
                self.logger.warning(
                    "Reddit health_check failed with status_code=%d", response.status_code
                )
            return ok
        except requests.RequestException as exc:
            self.logger.warning("Reddit health_check request failed: %s", exc)
            return False

    def _last_collected_ts(self, subreddit: str) -> datetime | None:
        last_ts, _ = self._load_canonical_state(self._canonical_path(subreddit))
        return last_ts

    def _canonical_path(self, subreddit: str) -> Path:
        return self.output_dir / f"{subreddit.lower()}_raw.jsonl"

    def _fetch_batch(self, subreddit: str, after: datetime, before: datetime) -> list[dict]:
        params = {
            "subreddit": subreddit,
            "after": after.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "before": before.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "limit": self.batch_size,
            "sort": "asc",
        }

        for attempt in range(self.MAX_RETRIES):
            try:
                response = requests.get(self.API_BASE, params=params, timeout=30)
                response.raise_for_status()
                try:
                    payload = response.json()
                except ValueError as exc:
                    if attempt == self.MAX_RETRIES - 1:
                        self.logger.error(
                            "Failed to parse response JSON for subreddit=%s after=%s before=%s error=%s",
                            subreddit,
                            params["after"],
                            params["before"],
                            exc,
                        )
                        return []
                    delay = self.RETRY_BASE_DELAY * (2**attempt)
                    self.logger.warning(
                        "Retrying subreddit=%s after=%s attempt=%d/%d in %.1fs",
                        subreddit,
                        params["after"],
                        attempt + 1,
                        self.MAX_RETRIES,
                        delay,
                    )
                    time.sleep(delay)
                    continue

                data = payload.get("data", []) if isinstance(payload, dict) else []
                if isinstance(data, list):
                    return data
                self.logger.warning(
                    "Unexpected response format for subreddit=%s after=%s; expected list in data.",
                    subreddit,
                    params["after"],
                )
                return []
            except requests.RequestException as exc:
                if attempt == self.MAX_RETRIES - 1:
                    self.logger.error(
                        "Failed to fetch subreddit=%s after=%s before=%s error=%s",
                        subreddit,
                        params["after"],
                        params["before"],
                        exc,
                    )
                    return []
                delay = self.RETRY_BASE_DELAY * (2**attempt)
                self.logger.warning(
                    "Retrying subreddit=%s after=%s attempt=%d/%d in %.1fs",
                    subreddit,
                    params["after"],
                    attempt + 1,
                    self.MAX_RETRIES,
                    delay,
                )
                time.sleep(delay)

        return []

    def _write_posts(self, posts: list[dict], file: IO[str], seen_ids: set[str]) -> int:
        written = 0
        for post in posts:
            post_id = post.get("id")
            if post_id is None:
                self.logger.warning("Skipping post without id")
                continue
            post_id_str = str(post_id)
            if post_id_str in seen_ids:
                continue
            seen_ids.add(post_id_str)
            file.write(json.dumps(post, ensure_ascii=False) + "\n")
            written += 1
        return written

    def _load_existing_ids(self, path: Path) -> set[str]:
        _, seen_ids = self._load_canonical_state(path)
        return seen_ids

    def _load_canonical_state(self, path: Path) -> tuple[datetime | None, set[str]]:
        if not path.exists():
            return None, set()

        max_ts: datetime | None = None
        seen_ids: set[str] = set()
        with path.open("r", encoding="utf-8") as file_handle:
            for line_number, line in enumerate(file_handle, start=1):
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    post = json.loads(stripped)
                except json.JSONDecodeError as exc:
                    self.logger.warning(
                        "Skipping malformed JSON line in %s line=%d: %s",
                        path,
                        line_number,
                        exc,
                    )
                    continue
                post_id = post.get("id")
                if post_id is None:
                    self.logger.warning("Skipping post without id in %s line=%d", path, line_number)
                    continue
                post_id_str = str(post_id)
                seen_ids.add(post_id_str)

                created = self._created_utc_from_post(post)
                if created is None:
                    self.logger.warning(
                        "Skipping line missing/invalid created_utc in %s line=%d",
                        path,
                        line_number,
                    )
                    continue

                if max_ts is None or created > max_ts:
                    max_ts = created

        return max_ts, seen_ids

    def _max_created_utc(self, posts: list[dict]) -> datetime | None:
        max_dt: datetime | None = None
        for post in posts:
            created = self._created_utc_from_post(post)
            if created is None:
                continue
            if max_dt is None or created > max_dt:
                max_dt = created
        return max_dt

    @staticmethod
    def _created_utc_from_post(post: dict) -> datetime | None:
        created_utc = post.get("created_utc")
        if created_utc is None:
            return None
        try:
            timestamp = int(created_utc)
        except (TypeError, ValueError):
            return None
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
