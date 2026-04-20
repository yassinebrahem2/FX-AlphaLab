"""Re-serialize technical model artifacts into bundled production .pkl files."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import joblib
import torch

from src.agents.technical.agent import LSTMModel, TechnicalAgent

SOURCE_MODELS_DIR = Path("models/best_models/content/best_models")
OUTPUT_MODELS_DIR = Path("models/technical")
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


def _load_checkpoint_config(
    checkpoint: dict[str, object], fallback_input_size: int
) -> tuple[int, int, int, float]:
    hidden_size = int(checkpoint.get("hidden_size", 128))
    num_layers = int(checkpoint.get("num_layers", 2))
    dropout = float(checkpoint.get("dropout", 0.3))
    checkpoint_input_size = int(checkpoint.get("input_size", fallback_input_size))
    return checkpoint_input_size, hidden_size, num_layers, dropout


def _resave_single(model_path: Path, output_dir: Path) -> tuple[str, str, str, str]:
    pair, timeframe, seq_len = _parse_model_name(model_path.name)
    scaler_path = model_path.with_name(model_path.name.replace("_best.pth", "_scaler.pkl"))
    output_path = output_dir / f"{pair}_{timeframe}.pkl"

    try:
        if not scaler_path.exists():
            raise FileNotFoundError(f"Missing scaler for model: {scaler_path.name}")

        scaler = joblib.load(scaler_path)
        checkpoint = torch.load(model_path, map_location="cpu", weights_only=False)

        default_input_size = len(
            TechnicalAgent(pair=pair, timeframe=timeframe, seq_len=seq_len).feature_cols
        )
        if isinstance(checkpoint, dict) and "model_state_dict" in checkpoint:
            input_size, hidden_size, num_layers, dropout = _load_checkpoint_config(
                checkpoint, default_input_size
            )
            state_dict = checkpoint["model_state_dict"]
        else:
            input_size = default_input_size
            hidden_size, num_layers, dropout = 128, 2, 0.3
            state_dict = checkpoint

        agent = TechnicalAgent(
            pair=pair,
            timeframe=timeframe,
            seq_len=seq_len,
            hidden_size=hidden_size,
            num_layers=num_layers,
            dropout=dropout,
        )

        model = LSTMModel(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            dropout=dropout,
        )
        model.load_state_dict(state_dict)
        model.eval()

        agent.model = model
        agent.scaler = scaler

        joblib.dump(scaler, scaler_path)
        saved_path = agent.save(output_path)

        return model_path.name, "PASS", str(saved_path), ""
    except Exception as exc:  # noqa: BLE001
        return model_path.name, "FAIL", str(output_path), str(exc)


def _print_summary(rows: list[tuple[str, str, str, str]]) -> None:
    name_w = max(len("model_name"), *(len(row[0]) for row in rows)) if rows else len("model_name")
    status_w = len("status")
    output_w = (
        max(len("output_path"), *(len(row[2]) for row in rows)) if rows else len("output_path")
    )

    header = f"{'model_name':<{name_w}}  {'status':<{status_w}}  {'output_path':<{output_w}}"
    print(header)
    print("-" * len(header))
    for model_name, status, output_path, _ in rows:
        print(f"{model_name:<{name_w}}  {status:<{status_w}}  {output_path:<{output_w}}")

    failed = [row for row in rows if row[1] == "FAIL"]
    if failed:
        print("\nErrors:")
        for model_name, _, _, error in failed:
            print(f"- {model_name}: {error}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Re-serialize technical LSTM artifacts for production"
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=SOURCE_MODELS_DIR,
        help="Directory containing *_best.pth and *_scaler.pkl files",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUT_MODELS_DIR,
        help="Directory to write bundled production .pkl artifacts",
    )
    args = parser.parse_args()

    source_dir = args.source_dir
    output_dir = args.output_dir

    if not source_dir.exists():
        raise FileNotFoundError(f"Source directory not found: {source_dir}")

    model_paths = sorted(source_dir.glob("lstm_*_best.pth"))
    if not model_paths:
        raise FileNotFoundError(f"No LSTM model files found in: {source_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)

    rows = [_resave_single(model_path, output_dir) for model_path in model_paths]
    _print_summary(rows)

    has_failures = any(status == "FAIL" for _, status, _, _ in rows)
    return 1 if has_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
