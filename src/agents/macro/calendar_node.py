from __future__ import annotations

from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer

from src.shared.utils import setup_logger


class CalendarEventsNode:
    COVERED_PAIRS = {"EURUSD", "GBPUSD", "USDCHF", "USDJPY"}
    PAIR_COUNTRY_SIGN = {
        "EURUSD": {"US": -1},
        "GBPUSD": {"GB": +1, "US": -1},
        "USDCHF": {"US": +1, "CH": -1},
        "USDJPY": {"US": +1, "JP": -1},
    }
    IMPACT_WEIGHT = {"high": 1.0, "medium": 0.5, "low": 0.25, "non-economic": 0.0}
    LGB_PAIRS = {"EURUSD", "USDCHF"}
    CB_PAIRS = {"GBPUSD", "USDJPY"}

    ROOT = Path(__file__).resolve().parents[3]
    ARTIFACTS_DIR = ROOT / "models" / "production" / "macro"
    EVENTS_PATH = ROOT / "data" / "processed" / "events" / "events_2021-01-01_2025-12-31.csv"
    PRICE_DIR = ROOT / "data" / "processed" / "ohlcv"
    VIX_PATH = ROOT / "data" / "processed" / "macro" / "macro_VIXCLS_2021-01-01_2026-02-21.csv"

    def __init__(
        self,
        artifacts_dir: Path | None = None,
        events_path: Path | None = None,
        price_dir: Path | None = None,
        vix_path: Path | None = None,
        log_file: Path | None = None,
    ) -> None:
        self.artifacts_dir = artifacts_dir or self.ARTIFACTS_DIR
        self.events_path = events_path or self.EVENTS_PATH
        self.price_dir = price_dir or self.PRICE_DIR
        self.vix_path = vix_path or self.VIX_PATH
        self.logger = setup_logger(self.__class__.__name__, log_file)
        self._event_scores: pd.DataFrame | None = None

    def load(self) -> None:
        lgb_path = self.artifacts_dir / "lgb_models.joblib"
        cb_path = self.artifacts_dir / "catboost_models.joblib"

        lgb_models: dict[str, dict] = joblib.load(lgb_path)
        cb_models: dict[str, dict] = {}

        catboost_available = True
        try:
            import catboost  # noqa: F401

            cb_models = joblib.load(cb_path)
        except Exception as exc:
            catboost_available = False
            self.logger.warning(
                "CatBoost unavailable or artifact load failed (%s). GBPUSD/USDJPY score as 0.0.",
                exc,
            )

        events = pd.read_csv(self.events_path)
        events["timestamp_utc"] = pd.to_datetime(events["timestamp_utc"], utc=True, errors="coerce")
        events = events.dropna(subset=["timestamp_utc"]).copy()
        events["country"] = events["country"].astype(str).str.upper()
        events["impact"] = events["impact"].astype(str).str.lower()

        expanded: list[pd.DataFrame] = []
        for pair in sorted(self.COVERED_PAIRS):
            country_map = self.PAIR_COUNTRY_SIGN[pair]
            part = events[events["country"].isin(country_map.keys())].copy()
            part["pair"] = pair
            part["country_sign"] = part["country"].map(country_map).astype(float)
            expanded.append(part)

        ev = pd.concat(expanded, ignore_index=True)
        ev = ev[ev["impact"] != "non-economic"].copy()
        ev["impact_weight"] = ev["impact"].map(self.IMPACT_WEIGHT).fillna(0.0).astype(float)

        pair_prices: dict[str, pd.DataFrame] = {}
        vol_thresholds: dict[str, tuple[float, float]] = {}
        price_feature_cols = [
            "return_1d_before",
            "return_3d_before",
            "rolling_volatility_3d",
            "rolling_volatility_7d",
            "rolling_vol_20d",
            "atr_5d",
            "atr_20d",
            "atr_ratio",
            "bb_w5",
            "bb_w20",
            "bb_squeeze",
            "body_ratio_3d_mean",
        ]

        for pair in sorted(self.COVERED_PAIRS):
            files = sorted(self.price_dir.glob(f"ohlcv_{pair}*_D1_*.parquet"))
            if not files:
                self.logger.warning(
                    "No price files for %s in %s; using default price features.",
                    pair,
                    self.price_dir,
                )
                vol_thresholds[pair] = (0.0, 0.0)
                continue

            frames = [pd.read_parquet(p) for p in files]
            px = pd.concat(frames, ignore_index=True)
            if "timestamp_utc" not in px.columns and "timestamp" in px.columns:
                px = px.rename(columns={"timestamp": "timestamp_utc"})
            px["timestamp_utc"] = pd.to_datetime(px["timestamp_utc"], utc=True, errors="coerce")
            px = px.dropna(subset=["timestamp_utc"]).copy()
            px = (
                px.sort_values("timestamp_utc")
                .drop_duplicates("timestamp_utc")
                .reset_index(drop=True)
            )

            px["daily_return"] = px["close"].pct_change()
            px["return_1d_before"] = px["daily_return"]
            px["return_3d_before"] = px["close"].pct_change(3)
            px["rolling_volatility_3d"] = px["daily_return"].rolling(3).std()
            px["rolling_volatility_7d"] = px["daily_return"].rolling(7).std()
            px["rolling_vol_20d"] = px["daily_return"].rolling(20).std()
            px["atr"] = (px["high"] - px["low"]).abs()
            px["atr_5d"] = px["atr"].rolling(5, min_periods=2).mean()
            px["atr_20d"] = px["atr"].rolling(20, min_periods=10).mean()
            px["atr_ratio"] = px["atr_5d"] / (px["atr_20d"] + 1e-9)
            px["bb_w5"] = px["close"].rolling(5, min_periods=2).std() / (
                px["close"].rolling(5, min_periods=2).mean() + 1e-9
            )
            px["bb_w20"] = px["close"].rolling(20, min_periods=10).std() / (
                px["close"].rolling(20, min_periods=10).mean() + 1e-9
            )
            px["bb_squeeze"] = px["bb_w5"] / (px["bb_w20"] + 1e-9)
            px["body_ratio"] = (px["close"] - px["open"]).abs() / (
                (px["high"] - px["low"]).abs() + 1e-9
            )
            px["body_ratio_3d_mean"] = px["body_ratio"].rolling(3, min_periods=1).mean()

            vol = px["rolling_vol_20d"].dropna()
            q33 = float(vol.quantile(0.33)) if not vol.empty else 0.0
            q67 = float(vol.quantile(0.67)) if not vol.empty else 0.0
            vol_thresholds[pair] = (q33, q67)

            shifted = px[["timestamp_utc", *price_feature_cols]].copy()
            shifted[price_feature_cols] = shifted[price_feature_cols].shift(1)
            pair_prices[pair] = shifted

        merged_parts: list[pd.DataFrame] = []
        for pair in sorted(self.COVERED_PAIRS):
            ep = ev[ev["pair"] == pair].copy()
            ep = ep.sort_values("timestamp_utc")
            ep["event_timestamp_utc"] = ep["timestamp_utc"]
            ep["anchor_day"] = ep["timestamp_utc"].dt.floor("D")

            if pair not in pair_prices:
                mp = ep.copy()
                for col in price_feature_cols:
                    mp[col] = 0.0
                mp["month"] = mp["timestamp_utc"].dt.month
                mp["day_of_week"] = mp["timestamp_utc"].dt.dayofweek
                mp["hour"] = mp["timestamp_utc"].dt.hour
                mp = mp.sort_values("timestamp_utc")
                mp["hours_since_last_event"] = (
                    mp["timestamp_utc"].diff().dt.total_seconds().div(3600).fillna(24.0)
                )

                actual = pd.to_numeric(mp["actual"], errors="coerce")
                forecast = pd.to_numeric(mp["forecast"], errors="coerce")
                mp["surprise_raw"] = actual - forecast
                mp["has_surprise_data"] = (actual.notna() & forecast.notna()).astype(int)
                mp["surprise_direction"] = np.sign(mp["surprise_raw"]).fillna(0.0)
                mp["surprise_beat"] = (mp["surprise_raw"] > 0).astype(float)
                mp.loc[mp["surprise_raw"].isna(), "surprise_beat"] = 0.0
                mp["surprise_miss"] = (mp["surprise_raw"] < 0).astype(float)
                mp.loc[mp["surprise_raw"].isna(), "surprise_miss"] = 0.0
                mp["surprise_abs_clipped"] = mp["surprise_raw"].abs().clip(0, 10)
                mp["surprise_z"] = mp["surprise_raw"]
                mp["surprise_z_abs"] = mp["surprise_z"].abs()
                mp["surprise_z_direction"] = np.sign(mp["surprise_z"]).fillna(0.0)
                mp["vol_regime"] = "normal"
                merged_parts.append(mp)
                continue

            pp = pair_prices[pair].copy().sort_values("timestamp_utc")
            mp = pd.merge_asof(
                ep.sort_values("anchor_day"),
                pp,
                left_on="anchor_day",
                right_on="timestamp_utc",
                direction="backward",
            )

            mp = mp.rename(
                columns={"timestamp_utc_x": "timestamp_utc", "timestamp_utc_y": "price_ts"}
            )
            mp["timestamp_utc"] = mp["event_timestamp_utc"]

            mp["month"] = mp["timestamp_utc"].dt.month
            mp["day_of_week"] = mp["timestamp_utc"].dt.dayofweek
            mp["hour"] = mp["timestamp_utc"].dt.hour
            mp = mp.sort_values("timestamp_utc")
            mp["hours_since_last_event"] = (
                mp["timestamp_utc"].diff().dt.total_seconds().div(3600).fillna(24.0)
            )

            actual = pd.to_numeric(mp["actual"], errors="coerce")
            forecast = pd.to_numeric(mp["forecast"], errors="coerce")
            mp["surprise_raw"] = actual - forecast
            mp["has_surprise_data"] = (actual.notna() & forecast.notna()).astype(int)
            mp["surprise_direction"] = np.sign(mp["surprise_raw"]).fillna(0.0)
            mp["surprise_beat"] = (mp["surprise_raw"] > 0).astype(float)
            mp.loc[mp["surprise_raw"].isna(), "surprise_beat"] = 0.0
            mp["surprise_miss"] = (mp["surprise_raw"] < 0).astype(float)
            mp.loc[mp["surprise_raw"].isna(), "surprise_miss"] = 0.0
            mp["surprise_abs_clipped"] = mp["surprise_raw"].abs().clip(0, 10)
            mp["surprise_z"] = mp["surprise_raw"]
            mp["surprise_z_abs"] = mp["surprise_z"].abs()
            mp["surprise_z_direction"] = np.sign(mp["surprise_z"]).fillna(0.0)

            q33, q67 = vol_thresholds[pair]
            mp["vol_regime"] = pd.cut(
                mp["rolling_vol_20d"],
                bins=[-np.inf, q33, q67, np.inf],
                labels=["low", "normal", "high"],
            ).astype(str)
            mp["vol_regime"] = mp["vol_regime"].replace("nan", "normal").fillna("normal")
            merged_parts.append(mp)

        scored = pd.concat(merged_parts, ignore_index=True)

        vix = pd.read_csv(self.vix_path)
        vix = vix[vix["series_id"] == "VIXCLS"].copy()
        vix["timestamp_utc"] = pd.to_datetime(vix["timestamp_utc"], utc=True, errors="coerce")
        vix = vix.dropna(subset=["timestamp_utc"]).copy()
        vix["timestamp_utc"] = vix["timestamp_utc"].dt.floor("D")
        vix["value"] = pd.to_numeric(vix["value"], errors="coerce")
        vix = vix.sort_values("timestamp_utc")
        vix["vix_20d_rank"] = vix["value"].rolling(20, min_periods=5).rank(pct=True)
        vix["vix_5d_mean"] = vix["value"].rolling(5, min_periods=2).mean()
        vix_20m = vix["value"].rolling(20).mean()
        vix_20s = vix["value"].rolling(20).std()
        vix["vix_spike"] = (vix["value"] > (vix_20m + 1.5 * vix_20s)).astype(float)
        vix["vix_change_1d"] = vix["value"].diff()
        vix["vix_regime"] = pd.cut(
            vix["value"], bins=[-np.inf, 15, 25, np.inf], labels=["calm", "normal", "stress"]
        ).astype(str)
        vix["vix_regime"] = vix["vix_regime"].replace("nan", "normal")

        scored = scored.sort_values("timestamp_utc")
        scored = pd.merge_asof(
            scored,
            vix[
                [
                    "timestamp_utc",
                    "value",
                    "vix_20d_rank",
                    "vix_5d_mean",
                    "vix_spike",
                    "vix_change_1d",
                    "vix_regime",
                ]
            ],
            on="timestamp_utc",
            direction="backward",
            tolerance=pd.Timedelta(days=7),
        )
        scored = scored.rename(columns={"value": "vix_value"})

        defaults_num = [
            "cot_net",
            "cot_net_pct_oi",
            "cot_change_4w",
            "cot_pct_rank",
            "gold_return_1d",
            "eurchf_level",
            "event_frequency",
            "event_avg_abs_return_train",
            "country_avg_abs_return_train",
            "pair_event_beta",
        ]
        for col in defaults_num:
            if col not in scored.columns:
                scored[col] = 0.0
            else:
                scored[col] = pd.to_numeric(scored[col], errors="coerce").fillna(0.0)

        for i in range(12):
            col = f"emb_{i}"
            if col not in scored.columns:
                scored[col] = 0.0
            else:
                scored[col] = pd.to_numeric(scored[col], errors="coerce").fillna(0.0)

        scored["source"] = scored.get("source", pd.Series(index=scored.index, dtype=object))
        scored["source"] = scored["source"].fillna("unknown").astype(str)
        scored["event_category"] = scored.get(
            "event_category", pd.Series(index=scored.index, dtype=object)
        )
        scored["event_category"] = scored["event_category"].fillna("unknown").astype(str)

        surp = scored["surprise_raw"].fillna(0.0)
        surp_abs = surp.abs()
        surp_z = scored["surprise_z"].fillna(0.0)

        def _pair_q67(p: str) -> float:
            return vol_thresholds.get(p, (0.0, 0.0))[1]

        def _pair_q33(p: str) -> float:
            return vol_thresholds.get(p, (0.0, 0.0))[0]

        vol_high = (
            scored["rolling_vol_20d"].fillna(0.0)
            > scored["pair"].map(lambda p: _pair_q67(str(p))).fillna(0.0)
        ).astype(float)
        vol_low = (
            scored["rolling_vol_20d"].fillna(0.0)
            < scored["pair"].map(lambda p: _pair_q33(str(p))).fillna(0.0)
        ).astype(float)

        scored["ia_surprise_x_vol_high"] = surp_abs * vol_high
        scored["ia_surprise_x_vol_low"] = surp_abs * vol_low
        scored["ia_surprise_x_pair_beta"] = surp * scored["pair_event_beta"]
        scored["ia_pair_beta_x_vix_rank"] = scored["pair_event_beta"] * scored["vix_20d_rank"]
        scored["ia_surprise_x_vix_rank"] = surp * scored["vix_20d_rank"]
        scored["ia_vol20d_x_surprise"] = scored["rolling_vol_20d"] * surp
        scored["ia_vix_stress_x_surp"] = (scored["vix_regime"] == "stress").astype(float) * surp_abs
        scored["ia_pair_beta_x_vol_high"] = scored["pair_event_beta"] * vol_high
        scored["ia_beat_x_vix_rank"] = scored["surprise_beat"] * scored["vix_20d_rank"]
        scored["ia_miss_x_vix_rank"] = scored["surprise_miss"] * scored["vix_20d_rank"]
        scored["ia_cot_x_surprise"] = scored["cot_net"] * surp
        scored["ia_cot_chg_x_vix"] = scored["cot_change_4w"] * scored["vix_20d_rank"]
        scored["ia_surprisez_x_vol_high"] = surp_z * vol_high
        scored["ia_surprisez_x_vix_rank"] = surp_z * scored["vix_20d_rank"]
        scored["ia_atr_ratio_x_surp"] = scored["atr_ratio"] * surp_z

        num_cols_all = scored.select_dtypes(include=[np.number]).columns
        scored[num_cols_all] = scored[num_cols_all].fillna(0.0)

        scored["prob"] = 0.0
        for pair in sorted(self.COVERED_PAIRS):
            pair_mask = scored["pair"] == pair
            pair_df = scored.loc[pair_mask].copy()

            if pair in self.LGB_PAIRS:
                artifact = lgb_models.get(pair)
                if artifact is None:
                    continue
                cols = list(artifact["num_cols"]) + list(artifact["cat_cols"])
                frame = self._build_frame(pair_df, cols, artifact["cat_cols"])
                probs = artifact["pipeline"].predict_proba(frame)[:, 1]
                scored.loc[pair_mask, "prob"] = probs
                continue

            if pair in self.CB_PAIRS and catboost_available:
                artifact = cb_models.get(pair)
                if artifact is None:
                    continue
                cols = list(artifact["num_cols"]) + list(artifact["cat_cols"])
                frame = self._build_frame(pair_df, cols, artifact["cat_cols"])
                num_cols = list(artifact["num_cols"])
                cat_cols = list(artifact["cat_cols"])
                if num_cols:
                    imputer = SimpleImputer(strategy="median")
                    frame[num_cols] = imputer.fit_transform(frame[num_cols])
                for col in cat_cols:
                    frame[col] = frame[col].fillna("missing").astype(str)
                model_cat_idx = artifact["model"].get_cat_feature_indices()
                for idx in model_cat_idx:
                    if 0 <= idx < len(frame.columns):
                        col_name = frame.columns[idx]
                        frame[col_name] = frame[col_name].fillna("missing").astype(str)
                try:
                    probs = artifact["model"].predict_proba(frame)[:, 1]
                    scored.loc[pair_mask, "prob"] = probs
                except Exception as exc:
                    self.logger.warning("CatBoost scoring failed for %s: %s", pair, exc)
                    scored.loc[pair_mask, "prob"] = 0.0

        self._event_scores = scored[
            [
                "timestamp_utc",
                "pair",
                "event_id",
                "event_name",
                "country",
                "has_surprise_data",
                "surprise_direction",
                "country_sign",
                "impact_weight",
                "prob",
            ]
        ].copy()

        counts = self._event_scores.groupby("pair").size().to_dict()
        for pair in sorted(self.COVERED_PAIRS):
            self.logger.info("Calendar events scored for %s: %d", pair, int(counts.get(pair, 0)))

    def predict(self, pair: str, signal_month_end: pd.Timestamp) -> float:
        if pair not in self.COVERED_PAIRS:
            return 0.0

        if self._event_scores is None:
            raise RuntimeError("CalendarEventsNode.load() must be called before predict().")

        ts = pd.Timestamp(signal_month_end)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")

        month_start = ts.replace(day=1)
        month_end = ts

        df = self._event_scores
        window = df[
            (df["pair"] == pair)
            & (df["timestamp_utc"] >= month_start)
            & (df["timestamp_utc"] <= month_end)
            & (df["has_surprise_data"] == 1)
        ]

        if window.empty:
            return 0.0

        score_i = (
            window["prob"]
            * window["surprise_direction"]
            * window["country_sign"]
            * window["impact_weight"]
        )
        total_weight = (window["prob"] * window["impact_weight"]).sum()
        if float(total_weight) <= 1e-9:
            return 0.0

        result = float(score_i.sum() / total_weight)
        return float(np.clip(result, -1.0, 1.0))

    @staticmethod
    def _build_frame(df: pd.DataFrame, cols: list[str], cat_cols: list[str]) -> pd.DataFrame:
        frame = pd.DataFrame(index=df.index)
        for col in cols:
            if col in df.columns:
                frame[col] = df[col]
            else:
                frame[col] = 0.0

        for col in cols:
            if col in cat_cols:
                frame[col] = frame[col].astype(object)
            else:
                frame[col] = pd.to_numeric(frame[col], errors="coerce")

        return frame[cols]
