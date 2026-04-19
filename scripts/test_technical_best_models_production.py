"""Production smoke test for LSTM technical best-model artifacts.

Loads each *_best.pth + *_scaler.pkl pair from models/best_models/content/best_models,
rebuilds features via features.py, runs live-style inference, and writes a report.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import torch

from src.agents.technical.features import add_features, get_feature_names, load_pair
from src.agents.technical.lstm_agent import LSTMModel

BEST_MODELS_DIR = Path("models/best_models/content/best_models")
REPORT_PATH = Path("outputs/signals/technical_production_smoke_test.csv")
MODEL_PATTERN = re.compile(
    r"^lstm_(?P<pair>[A-Za-z0-9]+)_(?P<tf>D1|H4|H1)_seq(?P<seq>\d+)_best\.pth$"
)


def _parse_model_name(file_name: str) -> tuple[str, str, int]:
    match = MODEL_PATTERN.match(file_name)
    if not match:
        raise ValueError(f"Unexpected model filename format: {file_name}")
    pair = match.group("pair")
    timeframe = match.group("tf")
    seq_len = int(match.group("seq"))
    return pair, timeframe, seq_len


def _run_single_model(model_path: Path, device: torch.device) -> dict[str, object]:
    pair, timeframe, seq_len = _parse_model_name(model_path.name)
    scaler_path = model_path.with_name(model_path.name.replace("_best.pth", "_scaler.pkl"))

    result: dict[str, object] = {
        "model_file": model_path.name,
        "pair": pair,
        "timeframe": timeframe,
        "seq_len": seq_len,
        "status": "PASS",
        "error": "",
        "prob_up": np.nan,
        "direction": np.nan,
        "last_timestamp_utc": "",
        "n_feature_rows": 0,
    }

    try:
        if not scaler_path.exists():
            raise FileNotFoundError(f"Missing scaler for model: {scaler_path.name}")

        df_raw = load_pair(pair, timeframe)
        df_feat = add_features(df_raw)
        result["n_feature_rows"] = int(len(df_feat))

        if len(df_feat) <= seq_len:
            raise ValueError(
                f"Not enough rows after feature engineering ({len(df_feat)}) for seq_len={seq_len}"
            )

        feature_cols = get_feature_names()
        scaler = joblib.load(scaler_path)

        x_df = df_feat[feature_cols].copy()
        x_df[feature_cols] = scaler.transform(x_df[feature_cols])

        seq = x_df.values[-seq_len:]
        x_tensor = torch.tensor(seq[np.newaxis, :, :], dtype=torch.float32).to(device)

        checkpoint = torch.load(model_path, map_location=device, weights_only=False)
        if isinstance(checkpoint, dict) and "model_state_dict" in checkpoint:
            input_size = int(checkpoint.get("input_size", len(feature_cols)))
            hidden_size = int(checkpoint.get("hidden_size", 128))
            num_layers = int(checkpoint.get("num_layers", 2))
            dropout = float(checkpoint.get("dropout", 0.3))
            state_dict = checkpoint["model_state_dict"]
        else:
            input_size = len(feature_cols)
            hidden_size = 128
            num_layers = 2
            dropout = 0.3
            state_dict = checkpoint

        model = LSTMModel(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            dropout=dropout,
        ).to(device)
        model.load_state_dict(state_dict)
        model.eval()

        with torch.no_grad():
            logits = model(x_tensor)
            prob_up = float(torch.sigmoid(logits).cpu().item())

        result["prob_up"] = prob_up
        result["direction"] = int(prob_up >= 0.5)
        result["last_timestamp_utc"] = str(pd.Timestamp(df_feat.index[-1]))

    except Exception as exc:  # noqa: BLE001
        result["status"] = "FAIL"
        result["error"] = str(exc)

    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Smoke test LSTM best-model artifacts for production"
    )
    parser.add_argument(
        "--models-dir",
        type=Path,
        default=BEST_MODELS_DIR,
        help="Directory containing *_best.pth and *_scaler.pkl files",
    )
    parser.add_argument(
        "--report-path",
        type=Path,
        default=REPORT_PATH,
        help="CSV output path for smoke-test results",
    )
    args = parser.parse_args()

    models_dir = args.models_dir
    if not models_dir.exists():
        raise FileNotFoundError(f"Models directory not found: {models_dir}")

    model_paths = sorted(models_dir.glob("lstm_*_best.pth"))
    if not model_paths:
        raise FileNotFoundError(f"No LSTM model files found in: {models_dir}")

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    results = [_run_single_model(path, device) for path in model_paths]
    report_df = pd.DataFrame(results)

    args.report_path.parent.mkdir(parents=True, exist_ok=True)
    report_df.to_csv(args.report_path, index=False)

    pass_count = int((report_df["status"] == "PASS").sum())
    fail_count = int((report_df["status"] == "FAIL").sum())

    print(f"Models tested: {len(report_df)}")
    print(f"PASS: {pass_count}")
    print(f"FAIL: {fail_count}")
    print(f"Report: {args.report_path}")

    if fail_count > 0:
        failed = report_df.loc[report_df["status"] == "FAIL", ["model_file", "error"]]
        print("\nFailed models:")
        print(failed.to_string(index=False))
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
