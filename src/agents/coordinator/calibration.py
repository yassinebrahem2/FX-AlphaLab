from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

_DEFAULT_PATH = (
    Path(__file__).resolve().parents[3] / "models" / "production" / "coordinator_calibration.json"
)


@dataclass(frozen=True)
class CalibrationParams:
    usdjpy_conf_p75: float  # USDJPY tech-gate confidence percentile-75 threshold
    geo_slope: float  # OLS slope: vol_signal_norm (geo) → fwd_vol_3d
    geo_intercept: float
    st_slope: float  # OLS slope: vol_signal_norm (stocktwits) → fwd_vol_3d
    st_intercept: float
    baseline_vol: float  # fallback when vol_signal is unavailable


def load_calibration(path: Path = _DEFAULT_PATH) -> CalibrationParams:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"Coordinator calibration not found at {path}. "
            "Run calibration.refit() to generate it."
        ) from exc
    return CalibrationParams(
        usdjpy_conf_p75=data["usdjpy_conf_p75"],
        geo_slope=data["geo_slope"],
        geo_intercept=data["geo_intercept"],
        st_slope=data["st_slope"],
        st_intercept=data["st_intercept"],
        baseline_vol=data["baseline_vol"],
    )


def refit(
    signals_df: pd.DataFrame,
    train_cutoff: str = "2023-12-31",
    out_path: Path = _DEFAULT_PATH,
) -> CalibrationParams:
    """Refit vol calibration constants from signals_aligned.parquet.

    Recommended cadence: quarterly expanding-window refit.
    Seed values (from NB04/NB05, 2022-2023 training window) are committed in
    models/production/coordinator_calibration.json.
    """
    from sklearn.linear_model import LinearRegression  # noqa: PLC0415

    cutoff = pd.Timestamp(train_cutoff)
    fwd_col = "fwd_vol_3d"

    usdjpy_train = signals_df[(signals_df["pair"] == "USDJPY") & (signals_df["date"] <= cutoff)]
    p75 = float(usdjpy_train["tech_confidence"].quantile(0.75))

    train = signals_df[signals_df["date"] <= cutoff].copy()
    geo_rows = []
    for _, row in train.iterrows():
        if pd.isna(row.get(fwd_col)):
            continue
        geo_val = row.get("geo_bilateral_risk")
        if pd.notna(geo_val):
            geo_rows.append({"vol_signal_norm": float(geo_val), "fwd_vol_3d": float(row[fwd_col])})
    geo_df = pd.DataFrame(geo_rows).dropna()
    lr_geo = LinearRegression().fit(geo_df[["vol_signal_norm"]], geo_df["fwd_vol_3d"])

    # StockTwits: invert signal (IC is negative → higher raw = lower vol)
    st_rows = signals_df[
        (signals_df["pair"] == "USDJPY")
        & signals_df["usdjpy_stocktwits_vol_signal"].notna()
        & signals_df[fwd_col].notna()
        & (signals_df["date"] <= pd.Timestamp("2024-12-31"))
    ].copy()
    st_vol_norm = -st_rows["usdjpy_stocktwits_vol_signal"].values
    lr_st = LinearRegression().fit(st_vol_norm.reshape(-1, 1), st_rows[fwd_col].values)

    baseline = float(geo_df["fwd_vol_3d"].mean())

    params = CalibrationParams(
        usdjpy_conf_p75=p75,
        geo_slope=float(lr_geo.coef_[0]),
        geo_intercept=float(lr_geo.intercept_),
        st_slope=float(lr_st.coef_[0]),
        st_intercept=float(lr_st.intercept_),
        baseline_vol=baseline,
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(
            {
                "usdjpy_conf_p75": params.usdjpy_conf_p75,
                "geo_slope": params.geo_slope,
                "geo_intercept": params.geo_intercept,
                "st_slope": params.st_slope,
                "st_intercept": params.st_intercept,
                "baseline_vol": params.baseline_vol,
                "train_cutoff": train_cutoff,
                "notebook": "coordinator_04_report + coordinator_05_backtest",
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return params
