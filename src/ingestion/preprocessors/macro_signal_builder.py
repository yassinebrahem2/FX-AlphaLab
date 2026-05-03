"""MacroSignalBuilder — inference-only Silver macro → macro_signal.parquet.

Loads pre-trained VAR+LSTM artifacts saved by notebook 05, runs forward-pass
inference on current Silver macro data, and writes macro_signal.parquet.

Artifact file (macro_hybrid_models.pkl) must exist in models/production/macro/.
Run notebook 05 to produce it — training is notebook territory, not here.
"""

from __future__ import annotations

import pickle
import warnings
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from sklearn.preprocessing import StandardScaler

from src.shared.utils import setup_logger

warnings.filterwarnings("ignore")

# ── Constants ──────────────────────────────────────────────────────────────

PAIRS = ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD"]
LOOKBACK_MONTHS = 12

_HAWK_WORDS = ["inflation", "tighten", "hike", "restrictive", "hawkish", "higher for longer"]
_DOVE_WORDS = ["cut", "easing", "accommodative", "recession", "slowdown", "dovish"]
_NEWS_SOURCES = ["fed", "ecb", "boe"]

ARTIFACTS_FILE = "macro_hybrid_models.pkl"


# ── Artifact schema (also imported by notebook 05 when saving) ─────────────


@dataclass
class MacroHybridArtifact:
    """Serialisable inference bundle produced by notebook 05 per pair."""

    pair: str
    feature_cols: list[str]
    var_top_indices: list[int]
    best_lag: int
    var_fit: object  # statsmodels VARResultsWrapper
    lstm_state_dict: dict  # torch state_dict for _ResidualLSTM(hidden_size=32)
    scaler: StandardScaler
    var_history_tail: np.ndarray  # last best_lag rows of var training matrix
    resid_tail: np.ndarray  # last LOOKBACK_MONTHS residuals for LSTM seed
    train_cutoff: pd.Timestamp  # last timestamp in training set


# ── Private helpers ────────────────────────────────────────────────────────


def _kalman_smooth(series: np.ndarray) -> np.ndarray:
    """RTS smoother — fills NaN gaps in a 1-D series."""
    x = np.asarray(series, dtype=float).copy()
    n = len(x)
    if n == 0:
        return x

    F = np.array([[1.0, 1.0], [0.0, 1.0]])
    H = np.array([[1.0, 0.0]])
    Q = np.array([[1e-4, 0.0], [0.0, 1e-3]])
    R = np.array([[1e-2]])

    valid = np.isfinite(x)
    if not valid.any():
        return np.zeros_like(x)

    first = int(np.flatnonzero(valid)[0])
    state = np.array([x[first], 0.0])
    P = np.eye(2)

    pred_s = np.zeros((n, 2))
    pred_c = np.zeros((n, 2, 2))
    filt_s = np.zeros((n, 2))
    filt_c = np.zeros((n, 2, 2))

    for t in range(n):
        state = F @ state
        P = F @ P @ F.T + Q
        pred_s[t], pred_c[t] = state, P
        if np.isfinite(x[t]):
            y_obs = np.array([x[t]]) - H @ state
            S = H @ P @ H.T + R
            K = P @ H.T @ np.linalg.inv(S)
            state = state + (K @ y_obs).ravel()
            P = (np.eye(2) - K @ H) @ P
        filt_s[t], filt_c[t] = state, P

    sm_s = filt_s.copy()
    sm_c = filt_c.copy()
    for t in range(n - 2, -1, -1):
        C = filt_c[t] @ F.T @ np.linalg.inv(pred_c[t + 1])
        sm_s[t] = filt_s[t] + C @ (sm_s[t + 1] - pred_s[t + 1])
        sm_c[t] = filt_c[t] + C @ (sm_c[t + 1] - pred_c[t + 1]) @ C.T

    out = x.copy()
    missing = ~np.isfinite(out)
    out[missing] = sm_s[missing, 0]
    return out


def _rolling_zscore(s: pd.Series, window: int = 24) -> pd.Series:
    mu = s.rolling(window=window, min_periods=6).mean()
    sd = s.rolling(window=window, min_periods=6).std(ddof=0)
    return ((s - mu) / sd.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)


class _ResidualLSTM(nn.Module):
    def __init__(self, hidden_size: int = 32) -> None:
        super().__init__()
        self.lstm = nn.LSTM(input_size=1, hidden_size=hidden_size, num_layers=1, batch_first=True)
        self.fc = nn.Linear(hidden_size, 1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.lstm(x)
        return self.fc(out[:, -1, :])


# ── Main class ─────────────────────────────────────────────────────────────


class MacroSignalBuilder:
    """Inference-only preprocessor: VAR+LSTM forward pass → macro_signal.parquet.

    Requires macro_hybrid_models.pkl in model_dir, produced by notebook 05.

    Usage::

        builder = MacroSignalBuilder()
        path = builder.run()
    """

    CATEGORY = "macro"
    OUTPUT_NAME = "macro_signal"

    def __init__(
        self,
        macro_dir: Path | None = None,
        ohlcv_dir: Path | None = None,
        news_dir: Path | None = None,
        output_dir: Path | None = None,
        model_dir: Path | None = None,
        log_file: Path | None = None,
    ) -> None:
        from src.shared.config import Config

        root = Config.ROOT_DIR
        self.macro_dir = macro_dir or root / "data" / "processed" / "macro"
        self.ohlcv_dir = ohlcv_dir or root / "data" / "processed" / "ohlcv"
        self.news_dir = news_dir or self.macro_dir / "news"
        self.output_dir = output_dir or self.macro_dir
        self.model_dir = model_dir or root / "models" / "production" / "macro"
        self.logger = setup_logger(self.__class__.__name__, log_file)

        self.output_dir.mkdir(parents=True, exist_ok=True)

    # ── Public API ─────────────────────────────────────────────────────────

    def run(self) -> Path:
        """Load artifacts → infer → write macro_signal.parquet."""
        artifacts = self._load_artifacts()

        macro_monthly = self._load_macro_monthly()
        news_monthly = self._load_news_monthly()
        feature_matrix = self._build_feature_matrix(macro_monthly, news_monthly)

        rows = []
        for artifact in artifacts:
            pair_df = self._build_pair_dataset(artifact.pair, feature_matrix)
            if pair_df is None or len(pair_df) == 0:
                self.logger.warning("%s: no data for inference, skipped", artifact.pair)
                continue
            try:
                signals = self._infer_pair(artifact, pair_df, feature_matrix)
                rows.append(signals)
                self.logger.info("%s: %d signal rows", artifact.pair, len(signals))
            except Exception as e:
                self.logger.error("%s: inference failed — %s", artifact.pair, e)

        if not rows:
            raise RuntimeError("All pairs failed inference — cannot write signal artifact.")

        signal_df = (
            pd.concat(rows, ignore_index=True)
            .sort_values(["pair", "timestamp_utc"])
            .reset_index(drop=True)
        )
        self._validate(signal_df)

        path = self.output_dir / f"{self.OUTPUT_NAME}.parquet"
        signal_df.to_parquet(path, engine="pyarrow", index=False)
        self.logger.info("Wrote %s (%d rows)", path, len(signal_df))
        return path

    # ── Artifact loading ───────────────────────────────────────────────────

    def _load_artifacts(self) -> list[MacroHybridArtifact]:
        path = self.model_dir / ARTIFACTS_FILE
        if not path.exists():
            raise FileNotFoundError(
                f"No macro inference artifacts at {path}.\n"
                "Run notebook 05 (macro_hybrid_var_lstm) with the updated save cell "
                "to produce macro_hybrid_models.pkl."
            )
        with open(path, "rb") as f:
            artifacts = pickle.load(f)
        if not isinstance(artifacts, list) or not artifacts:
            raise ValueError(f"Expected a non-empty list of MacroHybridArtifact in {path}")
        self.logger.info("Loaded %d pair artifacts from %s", len(artifacts), path)
        return artifacts

    # ── Data loading ───────────────────────────────────────────────────────

    def _load_macro_monthly(self) -> pd.DataFrame:
        csv_files = sorted(self.macro_dir.glob("macro_*.csv"))
        individual = [p for p in csv_files if not p.name.startswith("macro_all_")]
        all_files = [p for p in csv_files if p.name.startswith("macro_all_")]

        if not individual and not all_files:
            raise FileNotFoundError(f"No macro CSV files in {self.macro_dir}")

        frames = []
        for path in individual:
            try:
                frames.append(self._read_macro_csv(path))
            except Exception as e:
                self.logger.warning("Skipped %s: %s", path.name, e)

        if all_files:
            largest = max(all_files, key=lambda p: p.stat().st_size)
            try:
                frames.append(self._read_macro_csv(largest))
            except Exception as e:
                self.logger.warning("Skipped %s: %s", largest.name, e)

        if not frames:
            raise RuntimeError("All macro CSV reads failed")

        long = pd.concat(frames, ignore_index=True)
        long = long.drop_duplicates(subset=["series_id", "timestamp_utc"]).sort_values(
            ["series_id", "timestamp_utc"]
        )
        long["month_end"] = (
            long["timestamp_utc"].dt.to_period("M").dt.to_timestamp("M").dt.tz_localize("UTC")
        )
        monthly = (
            long.pivot_table(index="month_end", columns="series_id", values="value", aggfunc="last")
            .sort_index()
            .ffill(limit=6)
        )
        monthly.index.name = "timestamp_utc"
        return monthly.reset_index()

    @staticmethod
    def _read_macro_csv(path: Path) -> pd.DataFrame:
        df = pd.read_csv(path)
        col_map = {c.lower(): c for c in df.columns}

        ts_col = next(
            (col_map[c] for c in ["timestamp_utc", "timestamp", "date"] if c in col_map), None
        )
        sr_col = next(
            (col_map[c] for c in ["series_id", "series", "indicator"] if c in col_map), None
        )
        vl_col = next((col_map[c] for c in ["value", "close", "last"] if c in col_map), None)

        if ts_col is None or sr_col is None or vl_col is None:
            raise ValueError(f"Cannot infer columns from {path.name}: {list(df.columns)}")

        out = df[[ts_col, sr_col, vl_col]].rename(
            columns={ts_col: "timestamp_utc", sr_col: "series_id", vl_col: "value"}
        )
        out["timestamp_utc"] = pd.to_datetime(out["timestamp_utc"], utc=True, errors="coerce")
        out["series_id"] = out["series_id"].astype(str).str.strip()
        out["value"] = pd.to_numeric(out["value"], errors="coerce")
        return out.dropna(subset=["timestamp_utc", "series_id"])

    def _load_news_monthly(self) -> pd.DataFrame:
        if not self.news_dir.exists():
            self.logger.warning("News dir missing; news features will be zero")
            return pd.DataFrame(columns=["timestamp_utc"])

        frames = []
        for src in _NEWS_SOURCES:
            for path in sorted((self.news_dir / src).glob("year=*/month=*/news_cleaned.parquet")):
                try:
                    ndf = pd.read_parquet(path)
                    if ndf.empty:
                        continue
                    text_col = next(
                        (c for c in ["headline", "title", "text"] if c in ndf.columns), None
                    )
                    ts_col = next(
                        (
                            c
                            for c in ["timestamp_utc", "timestamp", "published_at", "date"]
                            if c in ndf.columns
                        ),
                        None,
                    )
                    if text_col is None or ts_col is None:
                        continue
                    ndf = ndf[[ts_col, text_col]].rename(
                        columns={ts_col: "timestamp_utc", text_col: "text"}
                    )
                    ndf["timestamp_utc"] = pd.to_datetime(
                        ndf["timestamp_utc"], utc=True, errors="coerce"
                    )
                    ndf = ndf.dropna(subset=["timestamp_utc"])
                    ndf["text"] = ndf["text"].fillna("").astype(str).str.lower()
                    ndf["month_end"] = (
                        ndf["timestamp_utc"]
                        .dt.to_period("M")
                        .dt.to_timestamp("M")
                        .dt.tz_localize("UTC")
                    )
                    ndf["hawk"] = ndf["text"].apply(lambda s: sum(w in s for w in _HAWK_WORDS))
                    ndf["dove"] = ndf["text"].apply(lambda s: sum(w in s for w in _DOVE_WORDS))
                    agg = ndf.groupby("month_end").agg(
                        doc_count=("text", "count"), hawk=("hawk", "sum"), dove=("dove", "sum")
                    )
                    agg["tone"] = (agg["hawk"] - agg["dove"]) / (1.0 + agg["hawk"] + agg["dove"])
                    agg = agg.reset_index().rename(columns={"month_end": "timestamp_utc"})
                    agg["source"] = src.upper()
                    frames.append(agg)
                except Exception as e:
                    self.logger.warning("Skipped %s: %s", path, e)

        if not frames:
            self.logger.warning("No news parquets readable; news features will be zero")
            return pd.DataFrame(columns=["timestamp_utc"])

        news = pd.concat(frames, ignore_index=True)
        wide = news.pivot_table(
            index="timestamp_utc", columns="source", values="tone", aggfunc="mean"
        )
        wide.columns = [f"NEWS_{c}_TONE" for c in wide.columns]
        wide = wide.reset_index().sort_values("timestamp_utc")
        for col in ["NEWS_FED_TONE", "NEWS_ECB_TONE", "NEWS_BOE_TONE"]:
            if col not in wide.columns:
                wide[col] = 0.0
        wide["NEWS_ECB_MINUS_FED"] = wide["NEWS_ECB_TONE"] - wide["NEWS_FED_TONE"]
        wide["NEWS_BOE_MINUS_FED"] = wide["NEWS_BOE_TONE"] - wide["NEWS_FED_TONE"]
        return wide.reset_index(drop=True)

    def _load_ohlcv_monthly(self, pair: str) -> pd.DataFrame | None:
        candidates = sorted(self.ohlcv_dir.glob(f"ohlcv_{pair}*_D1_*.parquet"))
        if not candidates:
            base = pair[:-1] if pair.endswith("m") else pair
            candidates = sorted(self.ohlcv_dir.glob(f"ohlcv_{base}*_D1_*.parquet"))
        if not candidates:
            return None

        scored = []
        for p in candidates:
            try:
                tmp = pd.read_parquet(p, columns=["timestamp_utc"])
                ts = pd.to_datetime(tmp["timestamp_utc"], utc=True, errors="coerce").dropna()
                scored.append(((ts.max() - ts.min()).days, len(ts), p))
            except Exception:
                continue

        if not scored:
            return None

        best = max(scored, key=lambda x: (x[0], x[1]))[2]
        pdf = pd.read_parquet(best)
        ts_col = "timestamp_utc" if "timestamp_utc" in pdf.columns else "timestamp"
        pdf["timestamp_utc"] = pd.to_datetime(pdf[ts_col], utc=True, errors="coerce")
        return (
            pdf.dropna(subset=["timestamp_utc"]).sort_values("timestamp_utc").reset_index(drop=True)
        )

    # ── Feature engineering ────────────────────────────────────────────────

    def _build_feature_matrix(
        self, macro_monthly: pd.DataFrame, news_monthly: pd.DataFrame
    ) -> pd.DataFrame:
        base = macro_monthly.copy().sort_values("timestamp_utc").reset_index(drop=True)
        avail = set(c for c in base.columns if c != "timestamp_utc")
        derived = pd.DataFrame({"timestamp_utc": base["timestamp_utc"]})

        def _need(cols: list[str]) -> bool:
            return all(c in avail for c in cols)

        for s in ["DFF", "ECB_DFR", "ECB_MRR", "BOEBANKRATE"]:
            if s in avail:
                derived[f"{s}_LVL"] = base[s]
                derived[f"{s}_LVL_L3"] = base[s].shift(3)
                derived[f"{s}_LVL_L6"] = base[s].shift(6)

        for s in ["DFF", "ECB_DFR", "BOEBANKRATE"]:
            if s in avail:
                d1, d3 = base[s].diff(1), base[s].diff(3)
                derived[f"{s}_MOM_1M"] = d1
                derived[f"{s}_MOM_3M"] = d3
                derived[f"{s}_MOM_6M"] = base[s].diff(6)
                derived[f"{s}_ACC"] = d1 - d3 / 3.0

        if _need(["ECB_DFR", "DFF"]):
            rd = base["ECB_DFR"] - base["DFF"]
            for lag in [0, 1, 3, 6]:
                suffix = f"_L{lag}" if lag else ""
                derived[f"RATEDIFF_EUR_USD{suffix}"] = rd.shift(lag)
            derived["RATEDIFF_EUR_USD_D3"] = rd.diff(3)
            derived["RATEDIFF_EUR_USD_D6"] = rd.diff(6)

        if _need(["BOEBANKRATE", "DFF"]):
            derived["RATEDIFF_GBP_USD"] = base["BOEBANKRATE"] - base["DFF"]

        if _need(["DFF", "CPIAUCSL"]):
            cpi_yoy = base["CPIAUCSL"].pct_change(12) * 100.0
            derived["CPI_US_YOY"] = cpi_yoy
            derived["REALRATE_USD"] = base["DFF"] - cpi_yoy

        if _need(["ECB_DFR", "HICP_EA"]):
            cpi_eur_yoy = base["HICP_EA"].pct_change(12) * 100.0
            derived["REALRATE_EUR"] = base["ECB_DFR"] - cpi_eur_yoy

        for s in [c for c in avail if "CPI" in c or "PCE" in c]:
            derived[f"{s}_YOY"] = base[s].pct_change(12) * 100.0

        for s in ["UNRATE", "BOPGSTB", "NETFI", "PAYEMS", "GDPC1"]:
            if s in avail:
                derived[f"{s}_YOY"] = base[s].pct_change(12) * 100.0
                derived[f"{s}_D1"] = base[s].diff(1)

        for s in ["STLFSI4", "VIXCLS", "BAMLH0A0HYM2"]:
            if s in avail:
                derived[f"{s}_LVL"] = base[s]
                derived[f"{s}_L1"] = base[s].shift(1)
                derived[f"{s}_L3"] = base[s].shift(3)

        if "timestamp_utc" in news_monthly.columns and len(news_monthly) > 0:
            matrix = derived.merge(news_monthly, on="timestamp_utc", how="left")
        else:
            matrix = derived.copy()

        for col in [
            "NEWS_FED_TONE",
            "NEWS_ECB_TONE",
            "NEWS_BOE_TONE",
            "NEWS_ECB_MINUS_FED",
            "NEWS_BOE_MINUS_FED",
        ]:
            if col not in matrix.columns:
                matrix[col] = 0.0

        return matrix.sort_values("timestamp_utc").reset_index(drop=True)

    def _build_pair_dataset(self, pair: str, feature_matrix: pd.DataFrame) -> pd.DataFrame | None:
        ohlcv = self._load_ohlcv_monthly(pair)
        if ohlcv is None:
            return None

        close_col = next(
            (c for c in ["close", "adj_close", "last"] if c in ohlcv.columns),
            next((c for c in ohlcv.columns if pd.api.types.is_numeric_dtype(ohlcv[c])), None),
        )
        if close_col is None:
            return None

        monthly_close = (
            ohlcv.assign(
                month_end=ohlcv["timestamp_utc"]
                .dt.to_period("M")
                .dt.to_timestamp("M")
                .dt.tz_localize("UTC")
            )
            .groupby("month_end")[close_col]
            .last()
            .rename("monthly_close")
            .reset_index()
            .rename(columns={"month_end": "timestamp_utc"})
            .sort_values("timestamp_utc")
        )

        all_feat = [c for c in feature_matrix.columns if c != "timestamp_utc"]
        if pair == "EURUSD":
            selected = [
                c
                for c in all_feat
                if any(
                    k in c
                    for k in [
                        "EUR",
                        "ECB",
                        "DFF",
                        "REALRATE_USD",
                        "CPI_US",
                        "STLFSI4",
                        "VIX",
                        "BAML",
                        "NEWS_",
                    ]
                )
            ]
        elif pair == "GBPUSD":
            selected = [
                c
                for c in all_feat
                if any(
                    k in c
                    for k in [
                        "GBP",
                        "BOE",
                        "DFF",
                        "REALRATE_USD",
                        "CPI_US",
                        "STLFSI4",
                        "VIX",
                        "BAML",
                        "NEWS_",
                    ]
                )
            ]
        else:
            selected = [
                c
                for c in all_feat
                if any(
                    k in c
                    for k in [
                        "DFF",
                        "REALRATE_USD",
                        "CPI_US",
                        "RATEDIFF_",
                        "STLFSI4",
                        "VIX",
                        "BAML",
                        "NEWS_",
                    ]
                )
            ]
        selected = sorted(set(selected))

        monthly_close["log_return"] = np.log(
            monthly_close["monthly_close"] / monthly_close["monthly_close"].shift(1)
        )

        df = feature_matrix[["timestamp_utc", *selected]].merge(
            monthly_close[["timestamp_utc", "monthly_close", "log_return"]],
            on="timestamp_utc",
            how="inner",
        )
        df["pair"] = pair
        return df.sort_values("timestamp_utc").reset_index(drop=True)

    # ── Inference ──────────────────────────────────────────────────────────

    def _infer_pair(
        self,
        artifact: MacroHybridArtifact,
        pair_df: pd.DataFrame,
        feature_matrix: pd.DataFrame,
    ) -> pd.DataFrame:
        """Rolling 1-step-ahead VAR+LSTM pass over the full history.

        Uses actual log_return as VAR feedback (not predicted y), giving the
        standard backtestable 'what would the model have said' signal series.
        LSTM is seeded on the first LOOKBACK_MONTHS actual VAR residuals, then
        rolls forward using its own predictions. Months before the LSTM warmup
        window carry VAR-only hybrid (LSTM component = 0).
        """
        df = (
            pair_df.dropna(subset=["log_return"])
            .sort_values("timestamp_utc")
            .reset_index(drop=True)
        )
        if len(df) < artifact.best_lag + 2:
            raise ValueError(f"Insufficient history for {artifact.pair}: {len(df)} rows")

        X_raw = df.reindex(columns=artifact.feature_cols).fillna(0.0)
        for col in artifact.feature_cols:
            X_raw[col] = _kalman_smooth(
                pd.to_numeric(X_raw[col], errors="coerce").to_numpy(dtype=float)
            )

        X_scaled = artifact.scaler.transform(X_raw)
        top_k = artifact.var_top_indices
        y = df["log_return"].to_numpy(dtype=float)
        timestamps = pd.to_datetime(df["timestamp_utc"], utc=True)

        # Build full VAR input matrix: [actual_y, top-k scaled features]
        var_input = np.column_stack([y, X_scaled[:, top_k]])

        # Rolling 1-step-ahead VAR forecast — actual y used as feedback each step
        var_forecast = []
        for i in range(artifact.best_lag, len(df)):
            history = var_input[i - artifact.best_lag : i]
            yhat = float(artifact.var_fit.forecast(y=history, steps=1)[0][0])
            var_forecast.append(yhat)

        var_forecast = np.asarray(var_forecast, dtype=float)
        forecast_ts = timestamps.iloc[artifact.best_lag :]
        actual_y = y[artifact.best_lag :]

        # VAR residuals (actual − predicted) used to seed and warm the LSTM
        var_residuals = (actual_y - var_forecast).astype(np.float32)

        lstm = _ResidualLSTM(hidden_size=32)
        lstm.load_state_dict(artifact.lstm_state_dict)
        lstm.eval()

        lstm_forecast = np.zeros(len(var_forecast), dtype=float)
        n = len(var_residuals)
        if n > LOOKBACK_MONTHS:
            rolling = var_residuals[:LOOKBACK_MONTHS].copy()
            with torch.no_grad():
                for i in range(LOOKBACK_MONTHS, n):
                    xin = torch.tensor(rolling).view(1, LOOKBACK_MONTHS, 1)
                    pred = float(lstm(xin).item())
                    lstm_forecast[i] = pred
                    # teacher-force with actual residual while history is available
                    rolling = np.concatenate(
                        [rolling[1:], np.array([var_residuals[i]], dtype=np.float32)]
                    )

        hybrid = var_forecast + lstm_forecast
        score = pd.Series(hybrid, index=forecast_ts)

        return self._assemble_pair_signals(artifact.pair, score, feature_matrix)

    def _assemble_pair_signals(
        self,
        pair: str,
        score: pd.Series,
        feature_matrix: pd.DataFrame,
    ) -> pd.DataFrame:
        feat_ts = feature_matrix.set_index("timestamp_utc").sort_index()
        test_idx = score.index

        roll_max = score.abs().rolling(window=24, min_periods=1).max().replace(0.0, np.nan)
        macro_conf = (score.abs() / roll_max).clip(0.0, 1.0).fillna(0.0)

        if pair == "EURUSD" and "RATEDIFF_EUR_USD" in feat_ts.columns:
            carry_raw = feat_ts["RATEDIFF_EUR_USD"]
        elif pair == "GBPUSD" and "RATEDIFF_GBP_USD" in feat_ts.columns:
            carry_raw = feat_ts["RATEDIFF_GBP_USD"]
        elif "RATEDIFF_EUR_USD" in feat_ts.columns:
            carry_raw = feat_ts["RATEDIFF_EUR_USD"]
        else:
            carry_raw = pd.Series(0.0, index=feat_ts.index)

        carry_z = _rolling_zscore(carry_raw, 24).reindex(test_idx).fillna(0.0)

        dff_vel = (
            feat_ts["DFF_MOM_3M"]
            if "DFF_MOM_3M" in feat_ts.columns
            else pd.Series(0.0, index=feat_ts.index)
        )
        stlfsi = (
            feat_ts["STLFSI4_LVL"]
            if "STLFSI4_LVL" in feat_ts.columns
            else pd.Series(0.0, index=feat_ts.index)
        )
        regime = 0.5 * (
            _rolling_zscore(dff_vel, 24).reindex(test_idx).fillna(0.0)
            + _rolling_zscore(stlfsi, 24).reindex(test_idx).fillna(0.0)
        )

        real_usd = (
            feat_ts["REALRATE_USD"]
            if "REALRATE_USD" in feat_ts.columns
            else pd.Series(0.0, index=feat_ts.index)
        )
        real_dev = real_usd - real_usd.rolling(24, min_periods=6).mean()
        fundamental = _rolling_zscore(real_dev, 24).reindex(test_idx).fillna(0.0)

        macro_bias = np.clip(
            0.5 * macro_conf.values
            + 0.25 * np.abs(carry_z.values)
            + 0.25 * np.abs(fundamental.values),
            0.0,
            1.0,
        )

        return pd.DataFrame(
            {
                "timestamp_utc": test_idx,
                "pair": pair,
                "module_c_direction": np.where(score.values >= 0.0, "up", "down"),
                "macro_confidence": macro_conf.values,
                "carry_signal_score": carry_z.values,
                "regime_context_score": regime.values,
                "fundamental_mispricing_score": fundamental.values,
                "macro_surprise_score": np.zeros(len(test_idx)),
                "macro_bias_score": macro_bias,
            }
        )

    # ── Validation ─────────────────────────────────────────────────────────

    def _validate(self, df: pd.DataFrame) -> None:
        required = [
            "timestamp_utc",
            "pair",
            "module_c_direction",
            "macro_confidence",
            "carry_signal_score",
            "regime_context_score",
            "fundamental_mispricing_score",
            "macro_surprise_score",
            "macro_bias_score",
        ]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"MacroSignal missing columns: {missing}")
        if df["module_c_direction"].isna().any():
            raise ValueError("NaN direction found")
        if df["macro_confidence"].isna().any():
            raise ValueError("NaN confidence found")
        if df.empty:
            raise ValueError("Signal DataFrame is empty")
