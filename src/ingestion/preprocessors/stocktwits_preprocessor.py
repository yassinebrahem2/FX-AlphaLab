import json
import math
import re
from pathlib import Path

import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

from src.shared.utils import setup_logger

URL_REGEX = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
CASHTAG_PAIR_REGEX = re.compile(r"\$([A-Za-z]{6})\b")
WHITESPACE_REGEX = re.compile(r"\s+")


def normalize_text(raw_text: object) -> str:
    if raw_text is None:
        return ""
    if isinstance(raw_text, float) and math.isnan(raw_text):
        return ""

    text = str(raw_text).replace("\n", " ").replace("\r", " ")
    text = URL_REGEX.sub(" URL ", text)
    text = CASHTAG_PAIR_REGEX.sub(lambda m: f" {m.group(1).upper()} ", text)
    text = WHITESPACE_REGEX.sub(" ", text).strip()
    return text


def preprocess_for_model(text: object) -> str:
    t = normalize_text(text)
    t = t.lower()
    t = re.sub(r"[^a-z0-9$\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


__all__ = ["StocktwitsPreprocessor"]


class StocktwitsPreprocessor:
    SOURCE_NAME = "stocktwits"
    MODEL = "FinTwitBERT-sentiment"
    TOKENIZER_NAME = "StephanAkkerman/FinTwitBERT-sentiment"
    THRESHOLD = 0.58
    LABEL2ID = {"bearish": 0, "bullish": 1}
    ID2LABEL = {0: "bearish", 1: "bullish"}
    MAX_LENGTH = 128

    def __init__(
        self,
        raw_dir: Path,
        checkpoint_path: Path,
        model_dir: Path,
        batch_size: int = 32,
        device: str = "auto",
        log_file: Path | None = None,
    ) -> None:
        self.raw_dir = raw_dir
        self.checkpoint_path = checkpoint_path
        self.model_dir = model_dir
        self.batch_size = batch_size
        self.logger = setup_logger(self.__class__.__name__, log_file)

        if not self.model_dir.exists():
            raise FileNotFoundError(f"Model directory not found: {self.model_dir}")

        resolved_device = device
        if device == "auto":
            resolved_device = "cuda" if torch.cuda.is_available() else "cpu"
        self.device = resolved_device

        self.tokenizer = AutoTokenizer.from_pretrained(self.TOKENIZER_NAME)
        self.model = AutoModelForSequenceClassification.from_pretrained(str(self.model_dir))
        self.model.eval()
        self.model.to(self.device)

    def run(self) -> int:
        all_raw_posts = self._load_raw_posts()
        labeled_ids = self._load_labeled_ids()
        candidates = [p for p in all_raw_posts if str(p.get("message_id", "")) not in labeled_ids]
        filtered = [p for p in candidates if self._passes_filter(p)]

        self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

        written = 0
        total = len(filtered)
        with self.checkpoint_path.open("a", encoding="utf-8") as file_handle:
            for start in range(0, total, self.batch_size):
                batch = filtered[start : start + self.batch_size]
                outputs = self._infer_batch(batch)
                for item in outputs:
                    file_handle.write(json.dumps(item, ensure_ascii=False) + "\n")
                    written += 1
                file_handle.flush()

                if written > 0 and written % 500 == 0:
                    self.logger.info("Labeled %d/%d stocktwits posts", written, total)

        return written

    def health_check(self) -> bool:
        try:
            encoded = self.tokenizer(
                [""],
                padding=True,
                truncation=True,
                max_length=self.MAX_LENGTH,
                return_tensors="pt",
            )
            encoded = {key: value.to(self.device) for key, value in encoded.items()}
            with torch.no_grad():
                _ = self.model(**encoded).logits
            return True
        except Exception as exc:
            self.logger.warning("Stocktwits health_check failed: %s", exc)
            return False

    def _infer_batch(self, posts: list[dict]) -> list[dict]:
        texts = [preprocess_for_model(p.get("body", "") or "") for p in posts]
        encoded = self.tokenizer(
            texts,
            padding=True,
            truncation=True,
            max_length=self.MAX_LENGTH,
            return_tensors="pt",
        )
        encoded = {key: value.to(self.device) for key, value in encoded.items()}

        with torch.no_grad():
            logits = self.model(**encoded).logits

        probs = torch.softmax(logits, dim=-1).cpu().numpy()

        results: list[dict] = []
        for post, prob in zip(posts, probs, strict=False):
            prob_bearish = float(prob[0])
            prob_bullish = float(prob[1])
            predicted_label = "bullish" if prob_bullish >= self.THRESHOLD else "bearish"
            results.append(
                {
                    "message_id": str(post.get("message_id", "")),
                    "symbol": str(post.get("symbol", "")),
                    "timestamp_published": str(post.get("timestamp_published", "")),
                    "prob_bullish": round(prob_bullish, 6),
                    "prob_bearish": round(prob_bearish, 6),
                    "predicted_label": predicted_label,
                    "model": self.MODEL,
                }
            )

        return results

    def _load_raw_posts(self) -> list[dict]:
        jsonl_files = sorted(self.raw_dir.glob("*.jsonl"))
        if not jsonl_files:
            self.logger.warning("No stocktwits raw files found in %s", self.raw_dir)
            return []

        posts: list[dict] = []
        for path in jsonl_files:
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
                    if not isinstance(post, dict):
                        self.logger.warning(
                            "Skipping non-object JSON in %s line=%d", path, line_number
                        )
                        continue
                    posts.append(post)

        deduped: list[dict] = []
        seen_ids: set[str] = set()
        for post in posts:
            message_id = str(post.get("message_id", ""))
            if message_id in seen_ids:
                continue
            seen_ids.add(message_id)
            deduped.append(post)

        return deduped

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
                if "message_id" not in item:
                    self.logger.warning(
                        "Skipping checkpoint line without message_id in %s line=%d",
                        self.checkpoint_path,
                        line_number,
                    )
                    continue
                labeled_ids.add(str(item["message_id"]))
        return labeled_ids

    def _passes_filter(self, post: dict) -> bool:
        return len(preprocess_for_model(post.get("body", "") or "")) > 0
