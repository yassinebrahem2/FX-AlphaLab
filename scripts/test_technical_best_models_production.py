"""Production smoke test for bundled technical model artifacts."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import numpy as np
import pandas as pd

from src.agents.technical.agent import TechnicalAgent
from src.agents.technical.features import load_pair

MODELS_DIR = Path("models/technical")
REPORT_PATH = Path("outputs/signals/technical_production_smoke_test.csv")
MODEL_PATTERN = re.compile(r"^(?P<pair>[A-Za-z0-9]+)_(?P<tf>D1|H4|H1)\.pkl$")


def _parse_model_name(file_name: str) -> tuple[str, str]:
    match = MODEL_PATTERN.match(file_name)
    if not match:
        raise ValueError(f"Unexpected model filename format: {file_name}")
    pair = match.group("pair")
    timeframe = match.group("tf")
    return pair, timeframe


def _run_single_model(model_path: Path) -> dict[str, object]:
    pair, timeframe = _parse_model_name(model_path.name)

    result: dict[str, object] = {
        "model_file": model_path.name,
        "pair": pair,
        "timeframe": timeframe,
        "seq_len": np.nan,
        "status": "PASS",
        "error": "",
        "prob_up": np.nan,
        "prob_down": np.nan,
        "direction": np.nan,
        "confidence": np.nan,
        "volatility_regime": "",
        "threshold_used": np.nan,
        "model_version": "",
        "last_timestamp_utc": "",
        "n_feature_rows": 0,
    }

    try:
        df_raw = load_pair(pair, timeframe)
        agent = TechnicalAgent.load(model_path)
        signal = agent.predict(df_raw)

        result["seq_len"] = int(agent.seq_len)
        result["prob_up"] = float(signal.prob_up)
        result["prob_down"] = float(signal.prob_down)
        result["direction"] = int(signal.direction)
        result["confidence"] = float(signal.confidence)
        result["volatility_regime"] = signal.volatility_regime
        result["threshold_used"] = float(signal.threshold_used)
        result["model_version"] = signal.model_version
        result["last_timestamp_utc"] = str(pd.Timestamp(signal.timestamp_utc))
        result["n_feature_rows"] = int(len(df_raw))

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
        default=MODELS_DIR,
        help="Directory containing bundled technical model artifacts (*.pkl)",
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

    model_paths = sorted(models_dir.glob("*_D1.pkl"))
    model_paths += sorted(models_dir.glob("*_H4.pkl"))
    model_paths += sorted(models_dir.glob("*_H1.pkl"))
    model_paths = sorted(set(model_paths))
    if not model_paths:
        raise FileNotFoundError(f"No bundled model files found in: {models_dir}")

    results = [_run_single_model(path) for path in model_paths]
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
