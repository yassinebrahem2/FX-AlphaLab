"""Sentiment aggregator and signal dataclass for FX coordinator."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from src.agents.sentiment.gdelt_node import GDELTSignalNode
from src.agents.sentiment.google_trends_node import GoogleTrendsSignalNode
from src.agents.sentiment.reddit_node import RedditSignalNode
from src.agents.sentiment.stocktwits_node import StocktwitsSignalNode
from src.shared.utils import setup_logger


@dataclass(frozen=True)
class SentimentSignal:
    """Signal payload produced by SentimentAgent for a single UTC day."""

    timestamp_utc: pd.Timestamp

    usdjpy_stocktwits_vol_signal: float | None
    usdjpy_stocktwits_active: bool

    gdelt_tone_zscore: float | None
    gdelt_attention_zscore: float | None
    macro_attention_zscore: float | None
    composite_stress_flag: bool

    context: dict | None


class SentimentAgent:
    """Aggregate sentiment and attention signals into a daily SentimentSignal."""

    STRESS_ZSCORE_THRESHOLD: float = 1.0
    GDELT_WARMUP_DAYS: int = 30
    REDDIT_ACTIVITY_ROLLING_WINDOW: int = 30

    def __init__(
        self,
        stocktwits_node: StocktwitsSignalNode,
        reddit_node: RedditSignalNode,
        gdelt_node: GDELTSignalNode,
        gtrends_node: GoogleTrendsSignalNode,
        log_file: Path | None = None,
    ) -> None:
        """Initialize SentimentAgent with node instances and logger."""

        self.stocktwits_node = stocktwits_node
        self.reddit_node = reddit_node
        self.gdelt_node = gdelt_node
        self.gtrends_node = gtrends_node
        self.logger = setup_logger(self.__class__.__name__, log_file)

    def compute_batch(
        self, start_date: datetime, end_date: datetime, include_context: bool = False
    ) -> pd.DataFrame:
        """Return daily dataframe of sentiment/attention signals between two dates inclusive."""

        # Normalize inputs to UTC date boundaries
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

        grid = pd.DataFrame(
            {"timestamp_utc": pd.date_range(start=start_ts, end=end_ts, freq="D", tz="UTC")}
        )

        # StockTwits
        try:
            _, pair_daily = self.stocktwits_node.compute(start_date, end_date)
            st_pair = pair_daily[pair_daily["symbol"] == "USDJPY"].copy()
            # normalize keys to date objects for robust mapping
            st_pair_dates = {
                pd.Timestamp(d).normalize().to_pydatetime().date(): float(v)
                for d, v in st_pair.set_index("date")["signal_post_count"].to_dict().items()
            }
            st_pair_net = {
                pd.Timestamp(d).normalize().to_pydatetime().date(): float(v)
                for d, v in st_pair.set_index("date")["net_sentiment"].to_dict().items()
            }

            def _date_key(ts):
                return pd.Timestamp(ts).normalize().to_pydatetime().date()

            grid["usdjpy_stocktwits_active"] = grid["timestamp_utc"].map(
                lambda d: bool(st_pair_dates.get(_date_key(d), 0))
            )

            def _net_map(d):
                key = _date_key(d)
                if st_pair_dates.get(key, 0) > 0:
                    return st_pair_net.get(key, np.nan)
                return pd.NA

            grid["usdjpy_stocktwits_vol_signal"] = grid["timestamp_utc"].map(_net_map)
        except Exception as exc:  # pragma: no cover - defensive
            self.logger.exception("Stocktwits node failed: %s", exc)
            grid["usdjpy_stocktwits_active"] = False
            grid["usdjpy_stocktwits_vol_signal"] = pd.NA

        # GDELT
        try:
            gdelt_df = self.gdelt_node.compute(
                start_date - timedelta(days=self.GDELT_WARMUP_DAYS), end_date
            )
            # ensure date column is datetime (date or timestamp)
            gdelt_df = gdelt_df.copy()
            gdelt_df["date"] = (
                pd.to_datetime(gdelt_df["date"])
                if not pd.api.types.is_datetime64_any_dtype(gdelt_df["date"])
                else gdelt_df["date"]
            )
            # Convert to UTC-naive date (handle both aware and naive)
            if isinstance(gdelt_df["date"].dtype, pd.DatetimeTZDtype):
                gdelt_df["date"] = (
                    gdelt_df["date"].dt.tz_convert("UTC").dt.tz_localize(None).dt.normalize()
                )
            else:
                gdelt_df["date"] = gdelt_df["date"].dt.normalize()
            gdelt_tone_series = (
                gdelt_df.set_index("date")["tone_zscore"]
                if "tone_zscore" in gdelt_df.columns
                else pd.Series()
            )
            gdelt_att_series = (
                gdelt_df.set_index("date")["attention_zscore"]
                if "attention_zscore" in gdelt_df.columns
                else pd.Series()
            )

            gdelt_tone = {
                pd.Timestamp(d).normalize().to_pydatetime().date(): float(v)
                for d, v in gdelt_tone_series.to_dict().items()
            }
            gdelt_att = {
                pd.Timestamp(d).normalize().to_pydatetime().date(): float(v)
                for d, v in gdelt_att_series.to_dict().items()
            }

            grid["gdelt_tone_zscore"] = (
                grid["timestamp_utc"]
                .map(
                    lambda d: gdelt_tone.get(
                        pd.Timestamp(d).normalize().to_pydatetime().date(), np.nan
                    )
                )
                .astype("float64")
            )
            grid["gdelt_attention_zscore"] = (
                grid["timestamp_utc"]
                .map(
                    lambda d: gdelt_att.get(
                        pd.Timestamp(d).normalize().to_pydatetime().date(), np.nan
                    )
                )
                .astype("float64")
            )
        except Exception as exc:
            self.logger.exception("GDELT node failed: %s", exc)
            grid["gdelt_tone_zscore"] = np.nan
            grid["gdelt_attention_zscore"] = np.nan

        # Google Trends
        try:
            gtrends_df = self.gtrends_node.compute(
                start_date - timedelta(days=self.GDELT_WARMUP_DAYS), end_date
            )
            gtrends_df = gtrends_df.copy()
            gtrends_df["date"] = (
                pd.to_datetime(gtrends_df["date"])
                if not pd.api.types.is_datetime64_any_dtype(gtrends_df["date"])
                else gtrends_df["date"]
            )
            # Convert to UTC-naive date (handle both aware and naive)
            if isinstance(gtrends_df["date"].dtype, pd.DatetimeTZDtype):
                gtrends_df["date"] = (
                    gtrends_df["date"].dt.tz_convert("UTC").dt.tz_localize(None).dt.normalize()
                )
            else:
                gtrends_df["date"] = gtrends_df["date"].dt.normalize()
            macro_series = (
                gtrends_df.set_index("date")["macro_attention_zscore"]
                if "macro_attention_zscore" in gtrends_df.columns
                else pd.Series()
            )
            macro_map = {
                pd.Timestamp(d).normalize().to_pydatetime().date(): float(v)
                for d, v in macro_series.to_dict().items()
            }
            grid["macro_attention_zscore"] = (
                grid["timestamp_utc"]
                .map(
                    lambda d: macro_map.get(
                        pd.Timestamp(d).normalize().to_pydatetime().date(), np.nan
                    )
                )
                .astype("float64")
            )
        except Exception as exc:
            self.logger.exception("Google Trends node failed: %s", exc)
            grid["macro_attention_zscore"] = np.nan

        # composite_stress_flag
        def _is_elevated(x):
            try:
                return x is not None and not pd.isna(x) and float(x) > self.STRESS_ZSCORE_THRESHOLD
            except Exception:
                return False

        grid["composite_stress_flag"] = (
            grid["gdelt_attention_zscore"].apply(_is_elevated).astype(int)
            + grid["macro_attention_zscore"].apply(_is_elevated).astype(int)
        ) >= 2

        # Context (optional)
        if include_context:
            try:
                reddit_global, reddit_pair = self.reddit_node.compute(
                    start_date - timedelta(days=self.REDDIT_ACTIVITY_ROLLING_WINDOW), end_date
                )

                # compute reddit rolling zscore for global activity
                rg = reddit_global.copy()
                rg["signal_post_count"] = pd.to_numeric(
                    rg["signal_post_count"], errors="coerce"
                ).fillna(0.0)
                rolling_mean = (
                    rg["signal_post_count"]
                    .rolling(window=self.REDDIT_ACTIVITY_ROLLING_WINDOW, min_periods=10)
                    .mean()
                )
                rolling_std = (
                    rg["signal_post_count"]
                    .rolling(window=self.REDDIT_ACTIVITY_ROLLING_WINDOW, min_periods=10)
                    .std()
                )
                rolling_std[rolling_std == 0] = pd.NA
                rg["reddit_global_activity_zscore"] = (
                    rg["signal_post_count"] - rolling_mean
                ) / rolling_std
                rg["date"] = pd.to_datetime(rg["date"]).dt.tz_localize(None).dt.normalize()
                reddit_z_map = rg.set_index("date")["reddit_global_activity_zscore"].to_dict()

                # prepare pair mappings
                reddit_pair["date"] = (
                    pd.to_datetime(reddit_pair["date"]).dt.tz_localize(None).dt.normalize()
                )
                st_pair = pair_daily if "pair_daily" in locals() else pd.DataFrame()
                st_pair = st_pair.copy()
                st_pair["date"] = pd.to_datetime(st_pair["date"]) if not st_pair.empty else st_pair
                if not st_pair.empty:
                    st_pair["date"] = st_pair["date"].dt.tz_localize(None).dt.normalize()

                def _build_context_row(ts):
                    key = pd.Timestamp(ts).normalize().to_pydatetime().date()
                    reddit_z = reddit_z_map.get(key, pd.NA)
                    # reddit_pair_views
                    rp_rows = (
                        reddit_pair[reddit_pair["date"] == pd.Timestamp(key)]
                        if not reddit_pair.empty
                        else pd.DataFrame()
                    )
                    reddit_pair_views = {}
                    for _, r in rp_rows.iterrows():
                        reddit_pair_views[r["pair"]] = {
                            "signal_post_count": float(r.get("signal_post_count", 0.0)),
                            "risk_off_score": float(r.get("risk_off_score", 0.0)),
                            "risk_on_score": float(r.get("risk_on_score", 0.0)),
                            "sentiment_strength_wmean": float(
                                r.get("sentiment_strength_wmean", 0.0)
                            ),
                        }

                    sp_rows = (
                        st_pair[st_pair["date"] == pd.Timestamp(key)]
                        if not st_pair.empty
                        else pd.DataFrame()
                    )
                    stocktwits_pair_views = {}
                    for _, s in sp_rows.iterrows():
                        stocktwits_pair_views[s["symbol"]] = {
                            "signal_post_count": float(s.get("signal_post_count", 0.0)),
                            "bullish_score": float(s.get("bullish_score", 0.0)),
                            "bearish_score": float(s.get("bearish_score", 0.0)),
                            "net_sentiment": float(s.get("net_sentiment", 0.0)),
                        }

                    return {
                        "reddit_global_activity_zscore": (
                            float(reddit_z) if not pd.isna(reddit_z) else None
                        ),
                        "reddit_pair_views": reddit_pair_views,
                        "stocktwits_pair_views": stocktwits_pair_views,
                    }

                grid["context"] = grid["timestamp_utc"].map(_build_context_row)
            except Exception as exc:
                self.logger.exception("Reddit/context computation failed: %s", exc)
                grid["context"] = [None] * len(grid)

        # final column ordering
        cols = [
            "timestamp_utc",
            "usdjpy_stocktwits_vol_signal",
            "usdjpy_stocktwits_active",
            "gdelt_tone_zscore",
            "gdelt_attention_zscore",
            "macro_attention_zscore",
            "composite_stress_flag",
        ]
        if include_context:
            cols.append("context")

        result = grid[cols].copy()
        # Ensure timestamp_utc dtype and sort
        result["timestamp_utc"] = pd.to_datetime(result["timestamp_utc"])
        result = result.sort_values("timestamp_utc").reset_index(drop=True)
        return result

    def get_signal(self, target_date: datetime, include_context: bool = False) -> SentimentSignal:
        """Return a SentimentSignal for the given target_date (UTC day)."""

        decay_window_days = 14
        start = target_date - timedelta(days=self.GDELT_WARMUP_DAYS + decay_window_days)
        df = self.compute_batch(start, target_date, include_context=include_context)

        # normalize target_date to UTC midnight
        ts = pd.Timestamp(target_date)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        ts = ts.normalize()

        row = df.loc[df["timestamp_utc"] == ts]
        if row.empty:
            raise ValueError(f"No sentiment data available for {target_date}")
        r = row.iloc[0]

        def _nan_to_none(x):
            if x is None:
                return None
            try:
                if pd.isna(x):
                    return None
            except Exception:
                pass
            return float(x)

        # Verify at least one upstream node has data by checking if row has any non-NaN signal values.
        # No need to re-invoke compute_batch(); already executed above.
        signal_cols = [
            "usdjpy_stocktwits_vol_signal",
            "gdelt_tone_zscore",
            "gdelt_attention_zscore",
            "macro_attention_zscore",
        ]
        has_source = any(not pd.isna(r.get(col)) for col in signal_cols)
        if not has_source and not r["usdjpy_stocktwits_active"]:
            raise ValueError(f"No sentiment data available for {target_date}")

        return SentimentSignal(
            timestamp_utc=pd.Timestamp(r["timestamp_utc"]).tz_convert("UTC"),
            usdjpy_stocktwits_vol_signal=_nan_to_none(r["usdjpy_stocktwits_vol_signal"]),
            usdjpy_stocktwits_active=bool(r["usdjpy_stocktwits_active"]),
            gdelt_tone_zscore=_nan_to_none(r["gdelt_tone_zscore"]),
            gdelt_attention_zscore=_nan_to_none(r["gdelt_attention_zscore"]),
            macro_attention_zscore=_nan_to_none(r["macro_attention_zscore"]),
            composite_stress_flag=bool(r["composite_stress_flag"]),
            context=(r["context"] if "context" in r.index and not pd.isna(r["context"]) else None),
        )
