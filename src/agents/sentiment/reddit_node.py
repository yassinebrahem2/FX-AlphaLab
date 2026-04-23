import json
import re
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from src.shared.utils import setup_logger

__all__ = ["RedditSignalNode"]


class RedditSignalNode:
    HALF_LIFE_DAYS = 5
    DECAY_WINDOW_DAYS = 14
    CONTENT_WEIGHTS = {
        "FUNDAMENTAL": 2,
        "NEWS_REACTION": 2,
        "TECHNICAL": 1,
        "POSITION_DISCLOSURE": 1,
    }
    PAIR_LIST = [
        "EURUSD",
        "GBPUSD",
        "USDJPY",
        "XAUUSD",
        "AUDUSD",
        "USDCAD",
        "USDCHF",
        "NZDUSD",
        "EURGBP",
        "EURJPY",
        "GBPJPY",
    ]
    ALLOWED_CONTENT = {"FUNDAMENTAL", "NEWS_REACTION", "TECHNICAL", "POSITION_DISCLOSURE"}

    def __init__(
        self,
        checkpoint_path: Path,
        log_file: Path | None = None,
    ) -> None:
        self.checkpoint_path = checkpoint_path
        self.logger = setup_logger(self.__class__.__name__, log_file)
        self.PAIR_PATTERN = re.compile("|".join(self.PAIR_LIST))

    def compute(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        records: list[dict] = []
        if not self.checkpoint_path.exists():
            raise ValueError("Checkpoint is empty — run RedditPreprocessor first")

        with self.checkpoint_path.open("r", encoding="utf-8") as file_handle:
            for line_number, line in enumerate(file_handle, start=1):
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    item = json.loads(stripped)
                except json.JSONDecodeError as exc:
                    self.logger.warning(
                        "Skipping malformed JSON in %s line=%d: %s",
                        self.checkpoint_path,
                        line_number,
                        exc,
                    )
                    continue
                if not isinstance(item, dict):
                    self.logger.warning(
                        "Skipping non-object JSON in %s line=%d",
                        self.checkpoint_path,
                        line_number,
                    )
                    continue
                records.append(item)

        if not records:
            raise ValueError("Checkpoint is empty — run RedditPreprocessor first")

        df = pd.DataFrame(records)
        required_defaults: dict[str, object] = {
            "id": "",
            "content_type": "",
            "risk_sentiment": "",
            "sentiment_strength": 0,
            "target_pair": None,
            "score": 0,
            "created_utc": 0,
            "subreddit": "",
        }
        for col, default in required_defaults.items():
            if col not in df.columns:
                df[col] = default

        df["sentiment_strength"] = pd.to_numeric(df["sentiment_strength"], errors="coerce").fillna(
            0.0
        )
        df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0.0)
        df["created_utc"] = pd.to_numeric(df["created_utc"], errors="coerce").fillna(0).astype(int)

        df["date"] = pd.to_datetime(df["created_utc"], unit="s", utc=True).dt.normalize()

        start_ts = pd.Timestamp(start_date)
        end_ts = pd.Timestamp(end_date)
        if start_ts.tzinfo is None:
            start_ts = start_ts.tz_localize("UTC")
        else:
            start_ts = start_ts.tz_convert("UTC")
        if end_ts.tzinfo is None:
            end_ts = end_ts.tz_localize("UTC")
        else:
            end_ts = end_ts.tz_convert("UTC")
        start_ts = start_ts.normalize()
        end_ts = end_ts.normalize()

        window_start = start_ts - pd.Timedelta(days=self.DECAY_WINDOW_DAYS)
        df = df[(df["date"] >= window_start) & (df["date"] <= end_ts)].copy()

        raw_daily_counts = df.groupby("date").size().rename("raw_post_count").reset_index()

        s2 = df[df["content_type"].isin(self.ALLOWED_CONTENT)].copy()

        s2["pair"] = s2["target_pair"].apply(self._normalize_pair)
        if "title" in s2.columns and "body" in s2.columns:
            unattributed = s2["pair"].isna()
            s2.loc[unattributed, "pair"] = s2[unattributed].apply(
                lambda r: self._extract_pair_from_text(r.get("title"), r.get("body")), axis=1
            )
        s2["pair"] = s2["pair"].fillna("global")

        score_p99 = s2["score"].quantile(0.99) if not s2.empty else 0.0
        if pd.isna(score_p99):
            score_p99 = 0.0
        s2["score_clip"] = s2["score"].clip(upper=score_p99)
        s2["w_score"] = np.log1p(s2["score_clip"])
        s2["w_content"] = s2["content_type"].map(self.CONTENT_WEIGHTS).astype(float)
        s2["w"] = s2["w_score"] * s2["w_content"]

        s2["risk_off_ind"] = (s2["risk_sentiment"] == "RISK_OFF").astype(float)
        s2["risk_on_ind"] = (s2["risk_sentiment"] == "RISK_ON").astype(float)
        s2["fundamental_news_ind"] = (
            s2["content_type"].isin(["FUNDAMENTAL", "NEWS_REACTION"]).astype(float)
        )

        expanded = self._build_expanded(s2)

        global_agg = expanded.groupby("date", as_index=False).agg(
            signal_post_count=("effective_weight", "sum"),
            risk_off_num=("risk_off_num", "sum"),
            risk_on_num=("risk_on_num", "sum"),
            sentiment_num=("sentiment_num", "sum"),
            fundamental_num=("fundamental_num", "sum"),
        )

        calendar = pd.DataFrame({"date": pd.date_range(start_ts, end_ts, freq="D", tz="UTC")})
        raw_map = (
            raw_daily_counts.set_index("date")["raw_post_count"]
            if not raw_daily_counts.empty
            else pd.Series(dtype="int64")
        )
        calendar["raw_post_count"] = calendar["date"].map(raw_map).fillna(0).astype(int)

        global_daily = calendar.merge(global_agg, on="date", how="left")
        fill_cols = [
            "signal_post_count",
            "risk_off_num",
            "risk_on_num",
            "sentiment_num",
            "fundamental_num",
        ]
        global_daily[fill_cols] = global_daily[fill_cols].fillna(0.0)

        ew = global_daily["signal_post_count"].values
        global_daily["risk_off_score"] = np.divide(
            global_daily["risk_off_num"],
            ew,
            where=ew > 0,
            out=np.zeros_like(ew),
        )
        global_daily["risk_on_score"] = np.divide(
            global_daily["risk_on_num"],
            ew,
            where=ew > 0,
            out=np.zeros_like(ew),
        )
        global_daily["sentiment_strength_wmean"] = np.divide(
            global_daily["sentiment_num"],
            ew,
            where=ew > 0,
            out=np.zeros_like(ew),
        )
        global_daily["fundamental_share"] = np.divide(
            global_daily["fundamental_num"],
            ew,
            where=ew > 0,
            out=np.zeros_like(ew),
        )
        global_daily["confidence"] = np.divide(
            global_daily["signal_post_count"],
            global_daily["raw_post_count"],
            where=global_daily["raw_post_count"] > 0,
            out=np.zeros(len(global_daily)),
        )

        pair_posts = s2[s2["pair"] != "global"].copy()
        pair_expanded = self._build_expanded(pair_posts)
        pair_agg = pair_expanded.groupby(["date", "pair"], as_index=False).agg(
            signal_post_count=("effective_weight", "sum"),
            risk_off_num=("risk_off_num", "sum"),
            risk_on_num=("risk_on_num", "sum"),
            sentiment_num=("sentiment_num", "sum"),
            fundamental_num=("fundamental_num", "sum"),
        )

        pair_grid = (
            calendar[["date"]]
            .assign(_key=1)
            .merge(pd.DataFrame({"pair": self.PAIR_LIST, "_key": 1}), on="_key")
            .drop(columns="_key")
        )
        pair_grid["raw_post_count"] = pair_grid["date"].map(raw_map).fillna(0).astype(int)

        pair_daily = pair_grid.merge(pair_agg, on=["date", "pair"], how="left")
        pair_daily[fill_cols] = pair_daily[fill_cols].fillna(0.0)

        pair_ew = pair_daily["signal_post_count"].values
        pair_daily["risk_off_score"] = np.divide(
            pair_daily["risk_off_num"],
            pair_ew,
            where=pair_ew > 0,
            out=np.zeros_like(pair_ew),
        )
        pair_daily["risk_on_score"] = np.divide(
            pair_daily["risk_on_num"],
            pair_ew,
            where=pair_ew > 0,
            out=np.zeros_like(pair_ew),
        )
        pair_daily["sentiment_strength_wmean"] = np.divide(
            pair_daily["sentiment_num"],
            pair_ew,
            where=pair_ew > 0,
            out=np.zeros_like(pair_ew),
        )
        pair_daily["fundamental_share"] = np.divide(
            pair_daily["fundamental_num"],
            pair_ew,
            where=pair_ew > 0,
            out=np.zeros_like(pair_ew),
        )
        pair_daily["confidence"] = np.divide(
            pair_daily["signal_post_count"],
            pair_daily["raw_post_count"],
            where=pair_daily["raw_post_count"] > 0,
            out=np.zeros(len(pair_daily)),
        )

        global_daily = global_daily[
            [
                "date",
                "raw_post_count",
                "signal_post_count",
                "risk_off_score",
                "risk_on_score",
                "sentiment_strength_wmean",
                "fundamental_share",
                "confidence",
            ]
        ]

        pair_daily = pair_daily[
            [
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
        ]

        return global_daily, pair_daily

    def get_signal(self, target_date: datetime) -> dict:
        window_start = target_date - pd.Timedelta(days=self.DECAY_WINDOW_DAYS + 5)
        global_df, _ = self.compute(window_start, target_date)
        available = global_df[global_df["signal_post_count"] > 0]
        if available.empty:
            raise ValueError(f"No signal data available on or before {target_date}")
        row = available.iloc[-1]
        return {
            "risk_off_score": float(row["risk_off_score"]),
            "risk_on_score": float(row["risk_on_score"]),
            "sentiment_strength_wmean": float(row["sentiment_strength_wmean"]),
            "signal_post_count": float(row["signal_post_count"]),
            "confidence": float(row["confidence"]),
        }

    def _normalize_pair(self, x: object) -> str | None:
        if x is None or pd.isna(x):
            return None
        txt = str(x).upper()
        txt = re.sub(r"[^A-Z]", "", txt)
        m = self.PAIR_PATTERN.search(txt)
        return m.group(0) if m else None

    def _extract_pair_from_text(self, title: object, body: object) -> str | None:
        txt = f"{title or ''} {body or ''}".upper()
        txt = re.sub(r"[^A-Z]", "", txt)
        m = self.PAIR_PATTERN.search(txt)
        return m.group(0) if m else None

    def _build_expanded(self, posts: pd.DataFrame) -> pd.DataFrame:
        lambda_ = np.log(2) / self.HALF_LIFE_DAYS
        decay_offsets = np.arange(self.DECAY_WINDOW_DAYS)

        expanded_frames = []
        for offset in decay_offsets:
            temp = posts[
                [
                    "date",
                    "pair",
                    "w",
                    "risk_off_ind",
                    "risk_on_ind",
                    "sentiment_strength",
                    "fundamental_news_ind",
                ]
            ].copy()
            temp["date"] = temp["date"] + pd.Timedelta(days=int(offset))
            decay = float(np.exp(-lambda_ * offset))
            temp["effective_weight"] = temp["w"] * decay
            temp["risk_off_num"] = temp["effective_weight"] * temp["risk_off_ind"]
            temp["risk_on_num"] = temp["effective_weight"] * temp["risk_on_ind"]
            temp["sentiment_num"] = temp["effective_weight"] * temp["sentiment_strength"]
            temp["fundamental_num"] = temp["effective_weight"] * temp["fundamental_news_ind"]
            expanded_frames.append(
                temp[
                    [
                        "date",
                        "pair",
                        "effective_weight",
                        "risk_off_num",
                        "risk_on_num",
                        "sentiment_num",
                        "fundamental_num",
                    ]
                ]
            )

        return pd.concat(expanded_frames, ignore_index=True)
