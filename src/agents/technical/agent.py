"""Production LSTM technical agent for FX directional signals.

This module stays faithful to the notebook Section 3 LSTM setup while exposing
simple train/save/load/predict APIs for production integration.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
from sklearn.preprocessing import MinMaxScaler
from torch.utils.data import DataLoader, Dataset

from src.agents.base import BaseAgent
from src.agents.technical.features import add_features, get_feature_names, volatility_regime_label
from src.shared.utils import setup_logger


def _normalize_pair(pair: str) -> str:
    return pair[:-1] if pair.endswith("m") else pair


@dataclass(frozen=True)
class TrainingMetrics:
    """Compact training/evaluation metrics payload."""

    train_loss: float
    val_loss: float
    test_loss: float
    hit_ratio: float
    f1: float
    auc: float


@dataclass(frozen=True)
class IndicatorSnapshot:
    """Key technical indicator values at inference time — used for explainability."""

    rsi: float
    macd_hist: float
    bb_pct: float
    above_ema200: bool
    atr_pct_rank: float


@dataclass(frozen=True)
class TechnicalSignal:
    """Single-model directional signal for the coordinator."""

    pair: str
    timeframe: str
    timestamp_utc: pd.Timestamp
    direction: int
    prob_up: float
    prob_down: float
    confidence: float
    volatility_regime: str
    threshold_used: float
    model_version: str
    indicator_snapshot: IndicatorSnapshot | None = None
    timeframe_votes: dict[str, int] | None = None


def fuse_timeframe_signals(
    signals: dict[str, TechnicalSignal],
    weights: dict[str, float] | None = None,
) -> TechnicalSignal:
    if "D1" not in signals:
        raise ValueError("D1 signal is required for multi-timeframe fusion.")

    if weights is None:
        weights = {"D1": 1.0 / 3.0, "H4": 1.0 / 3.0, "H1": 1.0 / 3.0}

    missing_weights = [tf for tf in signals if tf not in weights]
    if missing_weights:
        raise ValueError(f"Missing weights for timeframes: {missing_weights}")

    fused_score = sum(weights[tf] * (signal.prob_up - 0.5) for tf, signal in signals.items())

    direction = int(fused_score >= 0.0)
    prob_up = float(np.clip(fused_score + 0.5, 0.0, 1.0))
    confidence = float(np.clip(abs(fused_score) * 2.0, 0.0, 1.0))

    d1_signal = signals["D1"]
    timeframe_votes = {tf: signal.direction for tf, signal in signals.items()}
    return TechnicalSignal(
        pair=d1_signal.pair,
        timeframe="MTF",
        timestamp_utc=d1_signal.timestamp_utc,
        direction=direction,
        prob_up=prob_up,
        prob_down=1.0 - prob_up,
        confidence=confidence,
        volatility_regime=d1_signal.volatility_regime,
        threshold_used=d1_signal.threshold_used,
        model_version=d1_signal.model_version,
        indicator_snapshot=d1_signal.indicator_snapshot,
        timeframe_votes=timeframe_votes,
    )


class FXDataset(Dataset):
    """Torch dataset wrapper for sequence tensors."""

    def __init__(self, x: np.ndarray, y: np.ndarray) -> None:
        self.x = torch.tensor(x, dtype=torch.float32)
        self.y = torch.tensor(y, dtype=torch.float32)

    def __len__(self) -> int:
        return len(self.y)

    def __getitem__(self, idx: int) -> tuple[torch.Tensor, torch.Tensor]:
        return self.x[idx], self.y[idx]


class LSTMModel(nn.Module):
    """Baseline Section 3 LSTM architecture."""

    def __init__(
        self, input_size: int, hidden_size: int = 128, num_layers: int = 2, dropout: float = 0.3
    ):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )
        self.dropout = nn.Dropout(dropout)
        self.fc = nn.Linear(hidden_size, 1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.lstm(x)
        out = out[:, -1, :]
        out = self.dropout(out)
        out = self.fc(out)
        return out.squeeze(-1)


class TechnicalAgent(BaseAgent):
    """Train/infer LSTM directional model using features from features.py."""

    def __init__(
        self,
        pair: str,
        timeframe: str,
        seq_len: int = 60,
        batch_size: int = 32,
        learning_rate: float = 1e-5,
        epochs: int = 100,
        patience: int = 10,
        threshold: float = 0.5,
        train_end: str = "2023-12-31",
        val_end: str = "2024-06-30",
        hidden_size: int = 128,
        num_layers: int = 2,
        dropout: float = 0.3,
        model_version: str = "lstm_v1",
        data_dir: Path | None = None,
    ) -> None:
        self.pair = pair
        self.timeframe = timeframe
        self.seq_len = seq_len
        self.batch_size = batch_size
        self.learning_rate = learning_rate
        self.epochs = epochs
        self.patience = patience
        self.threshold = threshold
        self.train_end = train_end
        self.val_end = val_end
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        self.dropout = dropout
        self.model_version = model_version

        _root = Path(__file__).resolve().parents[3]
        self.data_dir = data_dir or (_root / "data" / "processed" / "ohlcv")

        self.feature_cols = get_feature_names()
        self.target_col = "target"
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        self.model: LSTMModel | None = None
        self.scaler: MinMaxScaler | None = None
        self.history: dict[str, list[float]] = {"train_loss": [], "val_loss": [], "val_hit": []}

        self.logger = setup_logger(f"TechnicalAgent[{pair}-{timeframe}]")

    def _chronological_split(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)

        train = df[df.index <= self.train_end].copy()
        val = df[(df.index > self.train_end) & (df.index <= self.val_end)].copy()
        test = df[df.index > self.val_end].copy()
        return train, val, test

    def _scale_features(
        self,
        train: pd.DataFrame,
        val: pd.DataFrame,
        test: pd.DataFrame,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, MinMaxScaler]:
        scaler = MinMaxScaler()
        train[self.feature_cols] = scaler.fit_transform(train[self.feature_cols])
        val[self.feature_cols] = scaler.transform(val[self.feature_cols])
        test[self.feature_cols] = scaler.transform(test[self.feature_cols])
        return train, val, test, scaler

    def _make_sequences(self, df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        x_vals = df[self.feature_cols].values
        y_vals = df[self.target_col].values

        x_data: list[np.ndarray] = []
        y_data: list[float] = []

        for i in range(self.seq_len, len(x_vals)):
            x_data.append(x_vals[i - self.seq_len : i])
            y_data.append(y_vals[i])

        return np.array(x_data, dtype=np.float32), np.array(y_data, dtype=np.float32)

    def _build_loaders(
        self, df: pd.DataFrame
    ) -> tuple[DataLoader, DataLoader, DataLoader, MinMaxScaler, tuple[np.ndarray, np.ndarray]]:
        train_df, val_df, test_df = self._chronological_split(df)

        for split_name, split_df in (("train", train_df), ("val", val_df), ("test", test_df)):
            if len(split_df) == 0:
                raise ValueError(
                    f"Split '{split_name}' is empty. Index range: {df.index.min()} -> {df.index.max()}"
                )
            if len(split_df) <= self.seq_len:
                raise ValueError(
                    f"Split '{split_name}' has {len(split_df)} rows but seq_len={self.seq_len}."
                )

        train_df, val_df, test_df, scaler = self._scale_features(
            train_df.copy(), val_df.copy(), test_df.copy()
        )

        x_tr, y_tr = self._make_sequences(train_df)
        x_va, y_va = self._make_sequences(val_df)
        x_te, y_te = self._make_sequences(test_df)

        train_loader = DataLoader(FXDataset(x_tr, y_tr), batch_size=self.batch_size, shuffle=False)
        val_loader = DataLoader(FXDataset(x_va, y_va), batch_size=self.batch_size, shuffle=False)
        test_loader = DataLoader(FXDataset(x_te, y_te), batch_size=self.batch_size, shuffle=False)

        return train_loader, val_loader, test_loader, scaler, (x_te, y_te)

    @staticmethod
    def _evaluate(
        model: nn.Module,
        loader: DataLoader,
        criterion: nn.Module,
        device: torch.device,
    ) -> tuple[float, float, float, float]:
        model.eval()
        total_loss = 0.0
        all_preds: list[int] = []
        all_labels: list[int] = []

        with torch.no_grad():
            for x_batch, y_batch in loader:
                x_batch, y_batch = x_batch.to(device), y_batch.to(device)
                logits = model(x_batch)
                loss = criterion(logits, y_batch)
                total_loss += loss.item()

                probs = torch.sigmoid(logits).cpu().numpy()
                preds = (probs >= 0.5).astype(int)
                all_preds.extend(preds.tolist())
                all_labels.extend(y_batch.cpu().numpy().astype(int).tolist())

        avg_loss = total_loss / len(loader)
        hit_ratio = accuracy_score(all_labels, all_preds)
        f1 = f1_score(all_labels, all_preds, zero_division=0)
        try:
            auc = roc_auc_score(all_labels, all_preds)
        except ValueError:
            auc = float("nan")
        return avg_loss, float(hit_ratio), float(f1), float(auc)

    @staticmethod
    def _train_one_epoch(
        model: nn.Module,
        loader: DataLoader,
        optimizer: torch.optim.Optimizer,
        criterion: nn.Module,
        device: torch.device,
    ) -> float:
        model.train()
        total_loss = 0.0

        for x_batch, y_batch in loader:
            x_batch, y_batch = x_batch.to(device), y_batch.to(device)
            optimizer.zero_grad()
            logits = model(x_batch)
            loss = criterion(logits, y_batch)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            optimizer.step()
            total_loss += loss.item()

        return total_loss / len(loader)

    def fit(self, raw_df: pd.DataFrame) -> TrainingMetrics:
        """Train the model on a single pair/timeframe DataFrame."""
        df = raw_df.copy()
        if self.target_col not in df.columns:
            df = add_features(df)
        else:
            df = df.dropna().copy()

        if len(df) == 0:
            raise ValueError("Input dataframe is empty after feature processing.")

        train_loader, val_loader, test_loader, scaler, _ = self._build_loaders(df)

        model = LSTMModel(
            input_size=len(self.feature_cols),
            hidden_size=self.hidden_size,
            num_layers=self.num_layers,
            dropout=self.dropout,
        ).to(self.device)

        optimizer = torch.optim.Adam(model.parameters(), lr=self.learning_rate)
        criterion = nn.BCEWithLogitsLoss()

        best_val_loss = float("inf")
        best_state: dict[str, torch.Tensor] | None = None
        best_train_loss = float("inf")
        patience_counter = 0

        for epoch in range(1, self.epochs + 1):
            train_loss = self._train_one_epoch(
                model, train_loader, optimizer, criterion, self.device
            )
            val_loss, val_hit, _, _ = self._evaluate(model, val_loader, criterion, self.device)

            self.history["train_loss"].append(train_loss)
            self.history["val_loss"].append(val_loss)
            self.history["val_hit"].append(val_hit)

            if val_loss < best_val_loss:
                best_val_loss = val_loss
                best_train_loss = train_loss
                best_state = {
                    key: value.detach().cpu().clone() for key, value in model.state_dict().items()
                }
                patience_counter = 0
            else:
                patience_counter += 1

            if epoch % 10 == 0 or patience_counter == 0:
                self.logger.info(
                    "Epoch %03d | train_loss=%.4f | val_loss=%.4f | val_hit=%.4f | patience=%d/%d",
                    epoch,
                    train_loss,
                    val_loss,
                    val_hit,
                    patience_counter,
                    self.patience,
                )

            if patience_counter >= self.patience:
                self.logger.info("Early stopping at epoch %d", epoch)
                break

        if best_state is None:
            raise RuntimeError("Training failed to produce a best model state.")

        model.load_state_dict(best_state)
        test_loss, hit_ratio, f1, auc = self._evaluate(model, test_loader, criterion, self.device)

        self.model = model
        self.scaler = scaler

        return TrainingMetrics(
            train_loss=float(best_train_loss),
            val_loss=float(best_val_loss),
            test_loss=float(test_loss),
            hit_ratio=float(hit_ratio),
            f1=float(f1),
            auc=float(auc),
        )

    def save(self, output_path: str | Path) -> Path:
        """Save full model artifact to .pkl as requested for production."""
        if self.model is None or self.scaler is None:
            raise ValueError("Model is not trained yet. Call fit() before save().")

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        artifact = {
            "model_state_dict": {
                key: value.detach().cpu().clone() for key, value in self.model.state_dict().items()
            },
            "model_config": {
                "input_size": len(self.feature_cols),
                "hidden_size": self.hidden_size,
                "num_layers": self.num_layers,
                "dropout": self.dropout,
            },
            "feature_cols": self.feature_cols,
            "target_col": self.target_col,
            "seq_len": self.seq_len,
            "pair": self.pair,
            "timeframe": self.timeframe,
            "threshold": self.threshold,
            "model_version": self.model_version,
            "train_end": self.train_end,
            "val_end": self.val_end,
            "scaler": self.scaler,
            "history": self.history,
        }

        joblib.dump(artifact, output_path)
        self.logger.info("Saved technical LSTM artifact to %s", output_path)
        return output_path

    @classmethod
    def load(
        cls, artifact_path: str | Path, device: str | torch.device | None = None
    ) -> TechnicalAgent:
        """Load agent from .pkl artifact."""
        artifact = joblib.load(artifact_path)

        agent = cls(
            pair=artifact["pair"],
            timeframe=artifact["timeframe"],
            seq_len=artifact["seq_len"],
            threshold=artifact["threshold"],
            train_end=artifact["train_end"],
            val_end=artifact["val_end"],
            hidden_size=artifact["model_config"]["hidden_size"],
            num_layers=artifact["model_config"]["num_layers"],
            dropout=artifact["model_config"]["dropout"],
            model_version=artifact["model_version"],
        )

        if device is not None:
            agent.device = torch.device(device)

        model = LSTMModel(**artifact["model_config"]).to(agent.device)
        model.load_state_dict(artifact["model_state_dict"])
        model.eval()

        agent.model = model
        agent.scaler = artifact["scaler"]
        agent.history = artifact.get("history", {"train_loss": [], "val_loss": [], "val_hit": []})
        agent.feature_cols = artifact["feature_cols"]
        return agent

    def predict(self, pair: str, date: pd.Timestamp) -> TechnicalSignal:
        """Load Silver OHLCV data for pair/date and run inference."""
        if _normalize_pair(pair) != _normalize_pair(self.pair):
            raise ValueError(f"This agent is bound to '{self.pair}'; cannot predict for '{pair}'.")

        _base = self.pair[:-1] if self.pair.endswith("m") else self.pair
        parquets = sorted(self.data_dir.glob(f"ohlcv_{_base}_{self.timeframe}_*.parquet"))
        if not parquets:
            parquets = sorted(self.data_dir.glob(f"ohlcv_{_base}m_{self.timeframe}_*.parquet"))
        if not parquets:
            raise FileNotFoundError(
                f"No OHLCV parquet for {self.pair}/{self.timeframe} in {self.data_dir}"
            )

        frames = [pd.read_parquet(p) for p in parquets]
        df = pd.concat(frames)
        # timestamp_utc is stored as a column, not the index — promote it
        if "timestamp_utc" in df.columns:
            df = df.set_index("timestamp_utc")
        df.index = pd.to_datetime(df.index)
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        df.columns = df.columns.str.lower()
        df = df.sort_index()

        # Strip timezone from cutoff so comparison is always tz-naive
        ts = pd.Timestamp(date)
        if ts.tzinfo is not None:
            ts = ts.tz_convert("UTC").tz_localize(None)

        # 252 (atr_pct_rank rolling) + seq_len + buffer = ~400 bars minimum
        df = df[df.index <= ts].tail(400)

        return self._predict_from_df(df)

    def _predict_from_df(self, raw_df: pd.DataFrame) -> TechnicalSignal:
        """Generate one technical signal from an already-loaded OHLCV DataFrame."""
        if self.model is None or self.scaler is None:
            raise ValueError(
                "Model artifact is not loaded. Train or load a model before predict()."
            )

        df = add_features(raw_df)
        if len(df) <= self.seq_len:
            raise ValueError(
                f"Not enough rows for inference. Need > {self.seq_len} rows after feature engineering."
            )

        if not isinstance(df.index, pd.DatetimeIndex):
            raise TypeError("Input dataframe must have a DatetimeIndex.")

        last_row = df.iloc[-1]
        snapshot = IndicatorSnapshot(
            rsi=float(last_row["rsi"]),
            macd_hist=float(last_row["macd_hist"]),
            bb_pct=float(last_row["bb_pct"]),
            above_ema200=bool(last_row["close"] > last_row["ema_200"]),
            atr_pct_rank=float(last_row["atr_pct_rank"]),
        )

        x_live = df[self.feature_cols].copy()
        x_live[self.feature_cols] = self.scaler.transform(x_live[self.feature_cols])
        seq = x_live.values[-self.seq_len :]
        x_tensor = torch.tensor(seq[np.newaxis, :, :], dtype=torch.float32).to(self.device)

        self.model.eval()
        with torch.no_grad():
            logit = self.model(x_tensor)
            prob_up = float(torch.sigmoid(logit).cpu().item())

        direction = int(prob_up >= self.threshold)
        prob_down = 1.0 - prob_up
        confidence = abs(prob_up - 0.5) * 2.0
        atr_pct_rank = float(last_row["atr_pct_rank"])
        volatility_regime = volatility_regime_label(atr_pct_rank=atr_pct_rank)

        return TechnicalSignal(
            pair=self.pair,
            timeframe=self.timeframe,
            timestamp_utc=pd.Timestamp(df.index[-1]),
            direction=direction,
            prob_up=prob_up,
            prob_down=prob_down,
            confidence=confidence,
            volatility_regime=volatility_regime,
            threshold_used=self.threshold,
            model_version=self.model_version,
            indicator_snapshot=snapshot,
            timeframe_votes=None,
        )
