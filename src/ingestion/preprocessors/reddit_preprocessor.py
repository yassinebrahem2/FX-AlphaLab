import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

from groq import Groq, RateLimitError

from src.shared.utils import setup_logger

MAX_BODY_CHARS = 1200

REQUIRED_FIELDS = {
    "reasoning": lambda v: isinstance(v, str) and len(v) > 0,
    "content_type": lambda v: v
    in ("TECHNICAL", "FUNDAMENTAL", "NEWS_REACTION", "POSITION_DISCLOSURE", "NOISE"),
    "sarcasm_irony_score": lambda v: v in (0, 1, 2),
    "sentiment_strength": lambda v: isinstance(v, int) and -2 <= v <= 2,
    "target_pair": lambda v: v is None or isinstance(v, str),
    "risk_sentiment": lambda v: v in ("RISK_ON", "RISK_OFF", "NEUTRAL"),
    "target_clarity": lambda v: v in (0, 1, 2),
    "stance_clarity": lambda v: v in ("CLEAR", "CONDITIONAL", "QUESTION", "NONE"),
}


class KeyPool:
    MAX_BACKOFF = 300.0

    def __init__(self, api_keys: list[str], logger) -> None:
        self.keys = list(api_keys)
        self.clients = {k: Groq(api_key=k) for k in self.keys}
        self.available_after = {k: 0.0 for k in self.keys}
        self.fail_count = {k: 0 for k in self.keys}
        self.logger = logger

    def _earliest(self) -> tuple[str, float]:
        key = min(self.keys, key=lambda k: self.available_after[k])
        wait = max(0.0, self.available_after[key] - time.time())
        return key, wait

    def acquire(self) -> tuple[str, Groq]:
        key, wait = self._earliest()
        if wait > 0:
            self.logger.info("All keys in backoff; waiting %.1fs", wait)
            time.sleep(wait)
        return key, self.clients[key]

    def on_success(self, key: str) -> None:
        self.fail_count[key] = 0

    def on_rate_limit(self, key: str, retry_after: float | None = None) -> None:
        self.fail_count[key] += 1
        backoff = (
            retry_after
            if retry_after
            else min(
                5.0 * (2 ** self.fail_count[key]),
                self.MAX_BACKOFF,
            )
        )
        self.available_after[key] = time.time() + backoff
        self.logger.warning("Key ...%s rate limited; backoff %.0fs", key[-6:], backoff)


def validate_labels(labels: dict) -> tuple[bool, str]:
    for field, check in REQUIRED_FIELDS.items():
        if field not in labels:
            return False, f"missing: {field}"
        if not check(labels[field]):
            return False, f"invalid {field}={labels[field]!r}"
    return True, ""


def build_user_message(row: dict) -> str:
    parts = [f"Subreddit: r/{row['subreddit']}"]
    if row.get("flair"):
        parts.append(f"Flair: {row['flair']}")
    parts.append(f"Title: {row['title']}")
    body = str(row.get("body", "")).strip()
    if body:
        parts.append(f"Body: {body[:MAX_BODY_CHARS]}")
    return "\n".join(parts)


class RedditPreprocessor:
    SOURCE_NAME = "reddit"
    MAX_BODY_CHARS = MAX_BODY_CHARS
    MODEL = "gpt-oss-120b"

    def __init__(
        self,
        raw_dir: Path,
        checkpoint_path: Path,
        groq_api_keys: list[str],
        subreddits: list[str] = ("Forex", "investing", "stocks"),
        max_retries: int = 5,
        log_file: Path | None = None,
    ) -> None:
        self.raw_dir = raw_dir
        self.checkpoint_path = checkpoint_path
        self.subreddits = list(subreddits)
        self.max_retries = max_retries
        self.logger = setup_logger(self.__class__.__name__, log_file)

        prompt_path = Path(__file__).parent / "prompts" / "reddit_labeling_system.txt"
        if not prompt_path.exists():
            raise FileNotFoundError(f"System prompt file not found: {prompt_path}")
        self.system_prompt = prompt_path.read_text(encoding="utf-8")

        if not groq_api_keys:
            raise ValueError("At least one Groq API key is required")
        self.pool = KeyPool(groq_api_keys, self.logger)

    def run(self, backfill: bool = False) -> int:
        all_raw_posts = self._load_raw_posts()

        # If backfill=True, ignore checkpoint and relabel all posts
        if backfill:
            labeled_ids = set()
        else:
            labeled_ids = self._load_labeled_ids()

        candidates = [p for p in all_raw_posts if str(p["id"]) not in labeled_ids]
        filtered = [p for p in candidates if self._passes_filters(p)]

        self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

        written = 0
        total = len(filtered)
        # When backfill=True, open in write mode "w" (overwrite); otherwise append mode "a"
        file_mode = "w" if backfill else "a"
        with self.checkpoint_path.open(file_mode, encoding="utf-8") as file_handle:
            for idx, post in enumerate(filtered, start=1):
                body = post.get("selftext", "") or ""
                flair = post.get("link_flair_text") or ""
                row = {
                    "id": str(post["id"]),
                    "subreddit": post.get("subreddit", ""),
                    "title": str(post.get("title", "") or ""),
                    "body": str(body),
                    "flair": str(flair),
                }
                labels = self._label_one(row)
                if labels is not None:
                    labels["score"] = int(post.get("score") or 0)
                    labels["created_utc"] = int(post.get("created_utc") or 0)
                    labels["subreddit"] = post.get("subreddit", "")
                    file_handle.write(json.dumps(labels, ensure_ascii=False) + "\n")
                    file_handle.flush()
                    written += 1

                if idx % 100 == 0:
                    self.logger.info("Labeled %d/%d posts", idx, total)

        return written

    def health_check(self) -> bool:
        try:
            key = self.pool.keys[0]
            client = self.pool.clients[key]
            client.models.list()
            self.logger.info("Groq health_check succeeded")
            return True
        except Exception as exc:
            self.logger.warning("Groq health_check failed: %s", exc)
            return False

    def _label_one(self, row: dict) -> dict | None:
        user_msg = build_user_message(row)
        attempts = 0
        while attempts < self.max_retries:
            key, client = self.pool.acquire()
            try:
                resp = client.chat.completions.create(
                    model=self.MODEL,
                    messages=[
                        {"role": "system", "content": self.system_prompt},
                        {"role": "user", "content": user_msg},
                    ],
                    response_format={"type": "json_object"},
                    temperature=0.0,
                    max_tokens=512,
                )
                self.pool.on_success(key)
                labels = json.loads(resp.choices[0].message.content)
                ok, reason = validate_labels(labels)
                if not ok:
                    attempts += 1
                    self.logger.warning("%s - attempt %d", reason, attempts)
                    time.sleep(1)
                    continue

                labels["id"] = row["id"]
                labels["model"] = self.MODEL
                labels["labeled_at"] = datetime.now(timezone.utc).isoformat()
                return labels
            except RateLimitError as exc:
                retry_after = None
                if hasattr(exc, "response") and exc.response is not None:
                    header = exc.response.headers.get("retry-after") or exc.response.headers.get(
                        "x-ratelimit-reset-requests"
                    )
                    if header:
                        try:
                            retry_after = float(header)
                        except ValueError:
                            retry_after = None
                self.pool.on_rate_limit(key, retry_after)
                attempts += 1
            except json.JSONDecodeError:
                attempts += 1
                self.logger.warning("JSON parse error - attempt %d", attempts)
                time.sleep(1)
            except Exception as exc:
                attempts += 1
                self.logger.warning("%s: %s - attempt %d", type(exc).__name__, exc, attempts)
                time.sleep(2 ** min(attempts, 4))

        self.logger.error(
            "Labeling failed for post id=%s after %d attempts", row.get("id"), attempts
        )
        return None

    def _load_raw_posts(self) -> list[dict]:
        posts: list[dict] = []
        for subreddit in self.subreddits:
            path = self.raw_dir / f"{subreddit.lower()}_raw.jsonl"
            if not path.exists():
                self.logger.warning("Raw file not found: %s", path)
                continue

            with path.open("r", encoding="utf-8") as file_handle:
                for line_number, line in enumerate(file_handle, start=1):
                    stripped = line.strip()
                    if not stripped:
                        continue
                    try:
                        post = json.loads(stripped)
                    except json.JSONDecodeError as exc:
                        self.logger.warning(
                            "Skipping malformed JSON in %s line=%d: %s", path, line_number, exc
                        )
                        continue
                    posts.append(post)
        return posts

    def _load_labeled_ids(self) -> set[str]:
        if not self.checkpoint_path.exists():
            return set()

        labeled_ids: set[str] = set()
        with self.checkpoint_path.open("r", encoding="utf-8") as file_handle:
            for line_number, line in enumerate(file_handle, start=1):
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    item = json.loads(stripped)
                except json.JSONDecodeError as exc:
                    self.logger.warning(
                        "Skipping malformed checkpoint JSON in %s line=%d: %s",
                        self.checkpoint_path,
                        line_number,
                        exc,
                    )
                    continue
                if "id" not in item:
                    self.logger.warning(
                        "Skipping checkpoint line without id in %s line=%d",
                        self.checkpoint_path,
                        line_number,
                    )
                    continue
                labeled_ids.add(str(item["id"]))
        return labeled_ids

    def _passes_filters(self, post: dict) -> bool:
        if post.get("removed_by_category") is not None:
            return False

        author = str(post.get("author", "") or "")
        if author in {"[deleted]", "AutoModerator"}:
            return False

        title = str(post.get("title", "") or "")
        body = str(post.get("selftext", "") or "")

        if len(title) < 10 and body in {"[deleted]", "[removed]", ""}:
            return False

        stripped_body = body.strip()
        if re.fullmatch(r"https?://\S+", stripped_body):
            return False

        if len(title + " " + body) < 20:
            return False

        return True
