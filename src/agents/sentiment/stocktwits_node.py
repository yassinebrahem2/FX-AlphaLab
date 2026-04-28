import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from src.shared.utils import setup_logger

__all__ = ["StocktwitsSignalNode"]


class StocktwitsSignalNode:
    HALF_LIFE_DAYS = 5
    DECAY_WINDOW_DAYS = 14
    PAIR_LIST = ["EURUSD", "GBPUSD", "USDCHF", "USDJPY"]

    def __init__(self, checkpoint_path: Path, log_file: Path | None = None) -> None:
        self.checkpoint_path = checkpoint_path
        self.logger = setup_logger(self.__class__.__name__, log_file)

    def compute(
        self, start_date: datetime, end_date: datetime
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        records: list[dict] = []
        if not self.checkpoint_path.exists():
            raise ValueError("Checkpoint is empty — run StocktwitsPreprocessor first")

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
            raise ValueError("Checkpoint is empty — run StocktwitsPreprocessor first")

        df = pd.DataFrame(records)
        required_defaults: dict[str, object] = {
            "message_id": "",
            "symbol": "",
            "timestamp_published": "",
            "prob_bullish": 0.0,
            "prob_bearish": 0.0,
        }
        for col, default in required_defaults.items():
            if col not in df.columns:
                df[col] = default

        df["prob_bullish"] = pd.to_numeric(df["prob_bullish"], errors="coerce").fillna(0.0)
        df["prob_bearish"] = pd.to_numeric(df["prob_bearish"], errors="coerce").fillna(0.0)

        df["date"] = pd.to_datetime(
            df["timestamp_published"], utc=True, errors="coerce"
        ).dt.normalize()
        df = df[df["date"].notna()].copy()

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

        df["w"] = 1.0
        df["bullish_ind"] = df["prob_bullish"]
        df["bearish_ind"] = df["prob_bearish"]
        df["confidence_ind"] = df[["prob_bullish", "prob_bearish"]].max(axis=1)

        expanded = self._build_expanded(df)
        pair_expanded = self._build_expanded(df[df["symbol"].isin(self.PAIR_LIST)])

        global_agg = expanded.groupby("date", as_index=False).agg(
            signal_post_count=("effective_weight", "sum"),
            bullish_num=("bullish_num", "sum"),
            bearish_num=("bearish_num", "sum"),
            confidence_num=("confidence_num", "sum"),
        )

        calendar = pd.DataFrame({"date": pd.date_range(start_ts, end_ts, freq="D", tz="UTC")})
        raw_map = (
            raw_daily_counts.set_index("date")["raw_post_count"]
            if not raw_daily_counts.empty
            else pd.Series(dtype="int64")
        )
        calendar["raw_post_count"] = calendar["date"].map(raw_map).fillna(0).astype(int)

        global_daily = calendar.merge(global_agg, on="date", how="left")
        fill_cols = ["signal_post_count", "bullish_num", "bearish_num", "confidence_num"]
        global_daily[fill_cols] = global_daily[fill_cols].fillna(0.0)

        global_ew = global_daily["signal_post_count"].values
        global_daily["bullish_score"] = np.divide(
            global_daily["bullish_num"],
            global_ew,
            where=global_ew > 0,
            out=np.zeros_like(global_ew),
        )
        global_daily["bearish_score"] = np.divide(
            global_daily["bearish_num"],
            global_ew,
            where=global_ew > 0,
            out=np.zeros_like(global_ew),
        )
        global_daily["net_sentiment"] = (
            global_daily["bullish_score"] - global_daily["bearish_score"]
        )
        global_daily["mean_model_confidence"] = np.divide(
            global_daily["confidence_num"],
            global_ew,
            where=global_ew > 0,
            out=np.zeros_like(global_ew),
        )

        pair_agg = pair_expanded.groupby(["date", "symbol"], as_index=False).agg(
            signal_post_count=("effective_weight", "sum"),
            bullish_num=("bullish_num", "sum"),
            bearish_num=("bearish_num", "sum"),
            confidence_num=("confidence_num", "sum"),
        )

        pair_grid = (
            calendar[["date"]]
            .assign(_key=1)
            .merge(pd.DataFrame({"symbol": self.PAIR_LIST, "_key": 1}), on="_key")
            .drop(columns="_key")
        )
        pair_grid["raw_post_count"] = pair_grid["date"].map(raw_map).fillna(0).astype(int)

        pair_daily = pair_grid.merge(pair_agg, on=["date", "symbol"], how="left")
        pair_daily[fill_cols] = pair_daily[fill_cols].fillna(0.0)

        pair_ew = pair_daily["signal_post_count"].values
        pair_daily["bullish_score"] = np.divide(
            pair_daily["bullish_num"],
            pair_ew,
            where=pair_ew > 0,
            out=np.zeros_like(pair_ew),
        )
        pair_daily["bearish_score"] = np.divide(
            pair_daily["bearish_num"],
            pair_ew,
            where=pair_ew > 0,
            out=np.zeros_like(pair_ew),
        )
        pair_daily["net_sentiment"] = pair_daily["bullish_score"] - pair_daily["bearish_score"]
        pair_daily["mean_model_confidence"] = np.divide(
            pair_daily["confidence_num"],
            pair_ew,
            where=pair_ew > 0,
            out=np.zeros_like(pair_ew),
        )

        global_daily = global_daily[
            [
                "date",
                "raw_post_count",
                "signal_post_count",
                "bullish_score",
                "bearish_score",
                "net_sentiment",
                "mean_model_confidence",
            ]
        ]

        pair_daily = pair_daily[
            [
                "date",
                "symbol",
                "raw_post_count",
                "signal_post_count",
                "bullish_score",
                "bearish_score",
                "net_sentiment",
                "mean_model_confidence",
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
            "bullish_score": float(row["bullish_score"]),
            "bearish_score": float(row["bearish_score"]),
            "net_sentiment": float(row["net_sentiment"]),
            "signal_post_count": float(row["signal_post_count"]),
            "mean_model_confidence": float(row["mean_model_confidence"]),
        }

    def _build_expanded(self, posts: pd.DataFrame) -> pd.DataFrame:
        lambda_ = np.log(2) / self.HALF_LIFE_DAYS
        decay_offsets = np.arange(self.DECAY_WINDOW_DAYS)

        expanded_frames = []
        for offset in decay_offsets:
            temp = posts[
                ["date", "symbol", "w", "bullish_ind", "bearish_ind", "confidence_ind"]
            ].copy()
            temp["date"] = temp["date"] + pd.Timedelta(days=int(offset))
            decay = float(np.exp(-lambda_ * offset))
            temp["effective_weight"] = temp["w"] * decay
            temp["bullish_num"] = temp["effective_weight"] * temp["bullish_ind"]
            temp["bearish_num"] = temp["effective_weight"] * temp["bearish_ind"]
            temp["confidence_num"] = temp["effective_weight"] * temp["confidence_ind"]
            expanded_frames.append(
                temp[
                    [
                        "date",
                        "symbol",
                        "effective_weight",
                        "bullish_num",
                        "bearish_num",
                        "confidence_num",
                    ]
                ]
            )

        if not expanded_frames:
            return pd.DataFrame(
                columns=[
                    "date",
                    "symbol",
                    "effective_weight",
                    "bullish_num",
                    "bearish_num",
                    "confidence_num",
                ]
            )

        return pd.concat(expanded_frames, ignore_index=True)
