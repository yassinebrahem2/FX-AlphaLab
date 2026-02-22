"""
Rerun pipeline with REAL MT5 data — prints RMSE leaderboard, loss matrix,
DM sign convention, RC/SPA p-values.
"""
import pandas as pd
import numpy as np
from pathlib import Path
from scipy import stats
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error
import warnings
warnings.filterwarnings("ignore")
pd.set_option("display.max_columns", None)
pd.set_option("display.precision", 5)
pd.set_option("display.width", 160)

# ============================================================================
# CONFIG
# ============================================================================
# ── Resolve repo root (works from scripts/ or repo root) ─────────────────
BASE = Path(__file__).resolve().parent.parent
if not (BASE / "src").exists():
    BASE = BASE.parent

GT_RAW_DIR = BASE / "data" / "raw" / "attention" / "google_trends"
GT_CSV     = GT_RAW_DIR / "google_trends_FULL_MERGED.csv"
CALIB_CSV  = BASE / "data" / "processed" / "macro" / "google_trends_calibrated.csv"
OUTPUTS    = BASE / "outputs"
MT5_DIR    = BASE / "data" / "raw" / "mt5"
PAIR = "GBPUSD"
TIMEFRAME = "H1"
TARGET = "ret_1"
GEO_LIST = ["GLOBAL", "GB", "US"]
GPROP = "web"
N_LAGS = [1, 2, 4]
MIN_TRAIN = 52
RIDGE_ALPHA = 1.0

KEYWORD_CATEGORIES = {
    "FX Pairs": ["EURUSD", "GBPUSD", "USDJPY", "euro dollar", "pound dollar",
                 "Japanese yen", "GBP exchange rate", "USDCHF", "Swiss franc"],
    "Central Banks": ["ECB", "European Central Bank", "Christine Lagarde",
                      "Federal Reserve", "FOMC",
                      "Bank of England", "BoE",
                      "Bank of Japan", "BOJ"],
    "Macro Indicators": ["CPI", "inflation", "eurozone inflation", "PMI", "bond yields",
                         "rate hike", "rate cut", "recession", "UK inflation"],
    "Risk Sentiment": ["VIX", "volatility", "safe haven", "market crash",
                       "DXY", "dollar index", "forex"],
}
KW_TO_CAT = {kw: cat for cat, kws in KEYWORD_CATEGORIES.items() for kw in kws}


# ============================================================================
# HELPERS
# ============================================================================
def parse_pull_id(sf):
    parts = Path(sf).stem.split("_")
    nums = [p for p in parts if p.isdigit() and len(p) == 6]
    return nums[-2] if len(nums) >= 2 else (nums[0] if nums else "pull_0")


# ============================================================================
# LOAD GT
# ============================================================================
gt = pd.read_csv(GT_CSV)
gt["date"] = pd.to_datetime(gt["date"], utc=True)
gt = gt[~gt["is_partial"]].copy()
gt["pull_id"] = gt["source_file"].apply(parse_pull_id)
gt["category"] = gt["keyword"].map(KW_TO_CAT).fillna("Other")
gt = gt.sort_values(["keyword", "geo", "gprop", "date"]).reset_index(drop=True)
df = gt  # alias used downstream


# ============================================================================
# LOAD REAL MT5 FX DATA — NO SYNTHETIC
# ============================================================================
def load_mt5_pair_timeframe(mt5_dir, pair, timeframe):
    files = sorted(mt5_dir.glob(f"mt5_{pair}_{timeframe}_*.csv"))
    if not files:
        raise FileNotFoundError(
            f"No MT5 files for {pair}/{timeframe} under {mt5_dir}")
    f = files[-1]
    d = pd.read_csv(f)
    d["date"] = pd.to_datetime(d["time"], utc=True, errors="coerce")
    d["price"] = pd.to_numeric(d["close"], errors="coerce")
    d = d.dropna(subset=["date", "price"]).sort_values("date").reset_index(drop=True)
    d["log_price"] = np.log(d["price"])
    d["ret_1"] = d["log_price"].diff()
    d["rv_14"] = d["ret_1"].rolling(14).std()
    out = d[["date", "price", "log_price", "ret_1", "rv_14"]].copy()
    out.attrs["source_file"] = str(f)
    return out


fx_raw = load_mt5_pair_timeframe(MT5_DIR, PAIR, TIMEFRAME)
print("=" * 80)
print("FX DATA LOAD")
print("=" * 80)
print(f"Source file : {fx_raw.attrs['source_file']}")
print(f"Raw shape   : {fx_raw.shape}")
print(f"Raw range   : {fx_raw['date'].min()} → {fx_raw['date'].max()}")
print(f"Raw ret_1   : mean={fx_raw['ret_1'].mean():.6f}, std={fx_raw['ret_1'].std():.6f}")
print()
print("FX data source = REAL MT5 CSV ✅ (no synthetic fallback)")

# Downsample to weekly
df_fx = (fx_raw.set_index("date")[["price"]]
         .resample("W-FRI").last()
         .dropna(subset=["price"]))
df_fx = df_fx.rename(columns={"price": "close"})
df_fx["log_price"] = np.log(df_fx["close"])
df_fx["ret_1"] = df_fx["log_price"].diff()
df_fx["rv_14"] = df_fx["ret_1"].rolling(4, min_periods=3).std()
df_fx = df_fx.dropna(subset=["ret_1"])
print(f"Weekly bars : {len(df_fx)}")
print(f"Weekly range: {df_fx.index.min().date()} → {df_fx.index.max().date()}")
print(f"ret_1 stats : mean={df_fx['ret_1'].mean():.5f}, std={df_fx['ret_1'].std():.5f}")
print(f"rv_14 stats : mean={df_fx['rv_14'].mean():.5f}, std={df_fx['rv_14'].std():.5f}")


# ============================================================================
# FEATURE ENGINEERING (4 pipelines)
# ============================================================================
df_f = gt[gt["gprop"] == GPROP].copy()

def build_feature_matrix(df_src, kws, method="raw", anchor_kw="forex"):
    series = {}
    for kw in kws:
        sub = df_src[df_src["keyword"] == kw]
        if sub.empty:
            continue
        avail_geos = [g for g in (["GLOBAL"] + GEO_LIST) if g in sub["geo"].unique()]
        if not avail_geos:
            continue
        if method in ("raw", "calibrated", "cal_detrend"):
            geo = avail_geos[0]
            s = sub[sub["geo"] == geo].set_index("date")["value"]
        elif method == "agg_geo_median":
            # Pipeline B: median across ALL available geos (not just GEO_LIST)
            s = sub.groupby("date")["value"].median()
        else:
            continue
        if s.empty or len(s) < 10:
            continue
        if method in ("calibrated", "cal_detrend"):
            anchor_sub = df_src[df_src["keyword"] == anchor_kw]
            if not anchor_sub.empty:
                a_geo = "GLOBAL" if "GLOBAL" in anchor_sub["geo"].unique() else anchor_sub["geo"].iloc[0]
                anchor_s = anchor_sub[anchor_sub["geo"] == a_geo].set_index("date")["value"]
                common = s.index.intersection(anchor_s.index)
                if len(common) >= 10:
                    scale = float(anchor_s.loc[common].mean()) / max(float(s.loc[common].mean()), 1e-6)
                    s = s * scale
        if method == "cal_detrend":
            trend = s.rolling(8, min_periods=4).mean()
            s = s - trend
        denom = s.std()
        if denom > 1e-6:
            s = (s - s.mean()) / denom
        series[kw] = s.rename(kw)
    if not series:
        return pd.DataFrame()
    return pd.DataFrame(series).dropna(how="all")


ALL_KWS = sorted(gt["keyword"].unique())
feats = {
    "A_raw": build_feature_matrix(df_f, ALL_KWS, "raw"),
    "B_agg_geo": build_feature_matrix(df_f, ALL_KWS, "agg_geo_median"),
    "C_anchor_rescale": build_feature_matrix(df_f, ALL_KWS, "calibrated"),
    "D_cal_det": build_feature_matrix(df_f, ALL_KWS, "cal_detrend"),
}

if CALIB_CSV.exists():
    df_calib_ext = pd.read_csv(CALIB_CSV, parse_dates=["date"])
    df_calib_ext["date"] = pd.to_datetime(df_calib_ext["date"], utc=True)
    if "keyword" in df_calib_ext.columns and "value" in df_calib_ext.columns:
        df_calib_ext["geo"] = "GLOBAL"
        df_calib_ext["gprop"] = GPROP
        feats["C_anchor_rescale"] = build_feature_matrix(df_calib_ext, ALL_KWS, "raw")
        feats["D_cal_det"] = build_feature_matrix(df_calib_ext, ALL_KWS, "cal_detrend")


def add_lags(feat_df, lags):
    if feat_df.empty:
        return feat_df
    return pd.concat([feat_df.shift(l).add_suffix(f"_L{l}") for l in lags], axis=1)


feat_lags = {name: add_lags(feat, N_LAGS) for name, feat in feats.items()}


def merge_with_fx(feat_lag, df_fx, target_col):
    feat_resampled = feat_lag.resample("W-FRI").mean()
    combined = pd.concat([feat_resampled, df_fx[[target_col]]], axis=1).dropna()
    return combined.drop(columns=[target_col]), combined[target_col]


# ============================================================================
# WALK-FORWARD
# ============================================================================
def walk_forward(X, y, y_lag=None, y_lags_multi=None, alpha=1.0, min_train=52):
    T = len(y)
    preds = {"RW": [], "AR1": [], "FXRidge": [], "GTRidge": [], "GT_FX": []}
    for t in range(min_train, T):
        y_tr = y[:t]
        X_tr = X[:t]
        preds["RW"].append(float(y_tr[-1]))
        if y_lag is not None:
            yL_tr = y_lag[:t].reshape(-1, 1)
            yL_te = y_lag[t:t + 1].reshape(-1, 1)
            ok_ar = ~np.isnan(yL_tr.flatten())
            if ok_ar.sum() >= 10 and not np.isnan(yL_te).any():
                ar_m = Ridge(alpha=0.01).fit(yL_tr[ok_ar], y_tr[ok_ar])
                preds["AR1"].append(float(ar_m.predict(yL_te)[0]))
            else:
                preds["AR1"].append(float(y_tr.mean()))
        else:
            preds["AR1"].append(float(y_tr.mean()))
        if y_lags_multi is not None:
            yM_tr = y_lags_multi[:t, :]
            yM_te = y_lags_multi[t:t + 1, :]
            ok_row = ~np.isnan(yM_tr).any(axis=1)
            if ok_row.sum() >= 10 and not np.isnan(yM_te).any():
                sc = StandardScaler()
                fx_m = Ridge(alpha=alpha).fit(sc.fit_transform(yM_tr[ok_row]), y_tr[ok_row])
                preds["FXRidge"].append(float(fx_m.predict(sc.transform(yM_te))[0]))
            else:
                preds["FXRidge"].append(float(y_tr.mean()))
        else:
            preds["FXRidge"].append(preds["AR1"][-1])
        ok_gt = ~np.isnan(X_tr).any(axis=1)
        if ok_gt.sum() >= 10:
            sc_gt = StandardScaler()
            gt_m = Ridge(alpha=alpha).fit(sc_gt.fit_transform(X_tr[ok_gt]), y_tr[ok_gt])
            preds["GTRidge"].append(float(gt_m.predict(sc_gt.transform(X[t:t + 1]))[0]))
        else:
            preds["GTRidge"].append(float(y_tr.mean()))
        if y_lag is not None:
            X_gf_tr = np.hstack([X_tr, y_lag[:t].reshape(-1, 1)])
            X_gf_te = np.hstack([X[t:t + 1], y_lag[t:t + 1].reshape(-1, 1)])
            ok_gf = ~np.isnan(X_gf_tr).any(axis=1)
            if ok_gf.sum() >= 10 and not np.isnan(X_gf_te).any():
                sc_gf = StandardScaler()
                gf_m = Ridge(alpha=alpha).fit(sc_gf.fit_transform(X_gf_tr[ok_gf]), y_tr[ok_gf])
                preds["GT_FX"].append(float(gf_m.predict(sc_gf.transform(X_gf_te))[0]))
            else:
                preds["GT_FX"].append(preds["GTRidge"][-1])
        else:
            preds["GT_FX"].append(preds["GTRidge"][-1])
    return preds, y[min_train:], list(range(min_train, T))


def eval_preds(actuals, preds_dict):
    rows = []
    rw_rmse = np.sqrt(mean_squared_error(actuals, preds_dict["RW"]))
    for name, p in preds_dict.items():
        p = np.array(p)
        rmse = np.sqrt(mean_squared_error(actuals, p))
        rows.append({"model": name,
                     "RMSE": rmse,
                     "MAE": mean_absolute_error(actuals, p),
                     "skill_vs_RW": 1 - rmse / rw_rmse,
                     "DA_%": np.mean(np.sign(p) == np.sign(actuals)) * 100})
    return pd.DataFrame(rows).round(6)


pipeline_results = {}
pipeline_preds = {}

print()
print("=" * 80)
print("WALK-FORWARD EVALUATION (REAL MT5 DATA)")
print("=" * 80)

for pipe_name, feat_lag in feat_lags.items():
    if feat_lag.empty:
        print(f"Pipeline {pipe_name}: empty -- skip")
        continue
    X_feat, y_pa = merge_with_fx(feat_lag, df_fx, TARGET)
    if len(y_pa) < MIN_TRAIN + 20:
        print(f"Pipeline {pipe_name}: insufficient overlap ({len(y_pa)}) -- skip")
        continue
    y_arr = y_pa.values
    X_arr = X_feat.values
    y_lag_arr = y_pa.shift(1).reindex(y_pa.index).values
    y_lags_multi = np.column_stack([y_pa.shift(l).reindex(y_pa.index).values for l in N_LAGS])
    preds, acts, test_idx = walk_forward(
        X_arr, y_arr, y_lag=y_lag_arr, y_lags_multi=y_lags_multi,
        alpha=RIDGE_ALPHA, min_train=MIN_TRAIN)
    pipeline_results[pipe_name] = eval_preds(acts, preds)
    pipeline_preds[pipe_name] = {"preds": preds, "actuals": acts,
                                  "index": y_pa.index[test_idx]}
    print(f"Pipeline {pipe_name}: {len(acts)} OOS predictions")


# ============================================================================
# RMSE LEADERBOARD
# ============================================================================
print()
print("=" * 80)
print(f"RMSE LEADERBOARD  (pair={PAIR}, target={TARGET}, FX=REAL MT5)")
print("=" * 80)

if pipeline_results:
    rows_lb = []
    for pipe, df_r in pipeline_results.items():
        r = df_r.set_index("model")
        get_r = lambda m: float(r.loc[m, "RMSE"]) if m in r.index else np.nan
        get_m = lambda m: float(r.loc[m, "MAE"]) if m in r.index else np.nan
        da_f = lambda m: float(r.loc[m, "DA_%"]) if m in r.index else np.nan
        rw = get_r("RW")
        ar1 = get_r("AR1")
        fxr = get_r("FXRidge")
        gtr = get_r("GTRidge")
        gtfx = get_r("GT_FX")
        rows_lb.append({
            "pipeline": pipe,
            "RW_RMSE": rw,
            "AR1_RMSE": ar1,
            "FXRidge_RMSE": fxr,
            "GTRidge_RMSE": gtr,
            "GT_FX_RMSE": gtfx,
            "RW_MAE": get_m("RW"),
            "AR1_MAE": get_m("AR1"),
            "FXRidge_MAE": get_m("FXRidge"),
            "GTRidge_MAE": get_m("GTRidge"),
            "GT_FX_MAE": get_m("GT_FX"),
            "D_GTFX_vs_RW": round(gtfx - rw, 6),
            "D_GTFX_vs_FXonly": round(gtfx - fxr, 6),
        })
    df_lb = pd.DataFrame(rows_lb).round(6)
    rmse_cols = ["pipeline", "RW_RMSE", "AR1_RMSE", "FXRidge_RMSE", "GTRidge_RMSE", "GT_FX_RMSE",
                 "D_GTFX_vs_RW", "D_GTFX_vs_FXonly"]
    print(df_lb[rmse_cols].to_string(index=False))
    print()
    mae_cols = ["pipeline", "RW_MAE", "AR1_MAE", "FXRidge_MAE", "GTRidge_MAE", "GT_FX_MAE"]
    print("MAE:")
    print(df_lb[mae_cols].to_string(index=False))


# ============================================================================
# LOSS MATRIX
# ============================================================================
print()
print("=" * 80)
print("LOSS MATRIX + MEAN LOSS PER MODEL")
print("=" * 80)

all_errors = {}
for pipe, data in pipeline_preds.items():
    _preds_d = data["preds"]
    _acts = np.array(data["actuals"])
    _idx = pd.DatetimeIndex(data["index"])
    for mname in _preds_d:
        key = f"{pipe}__{mname}"
        e = _acts - np.array(_preds_d[mname])
        all_errors[key] = pd.Series(e ** 2, index=_idx, name=key)

loss_df = pd.DataFrame(all_errors).dropna()
bm_cols = [c for c in loss_df.columns if c.endswith("__RW")]
bm_col = bm_cols[0]
cand_cols = [c for c in loss_df.columns if c != bm_col]
loss_bm = loss_df[bm_col].values
loss_mat = loss_df[cand_cols].values

print(f"loss_matrix.shape = {loss_df.shape}")
print(f"date min = {loss_df.index.min().date()}")
print(f"date max = {loss_df.index.max().date()}")
print()

# Mean loss per model (should equal RMSE^2)
print("Mean squared loss per model (= RMSE^2):")
for col in loss_df.columns:
    ml = loss_df[col].mean()
    rmse_from_loss = np.sqrt(ml)
    print(f"  {col:30s}  mean_loss={ml:.8f}  sqrt={rmse_from_loss:.6f}")

# Save updated loss matrix
lm_path = OUTPUTS / f"loss_matrix_{PAIR}_{TARGET}.csv"
loss_df.to_csv(lm_path)
print(f"\nSaved: {lm_path}")


# ============================================================================
# DM / HLN
# ============================================================================
print()
print("=" * 80)
print("DM / HLN — SIGN CONVENTION CHECK")
print("=" * 80)


def dm_test(e1, e2, h=1):
    d = e1 ** 2 - e2 ** 2
    T = len(d)
    d_bar = d.mean()
    g0 = np.var(d, ddof=0)
    gk = [np.cov(d[k:], d[:-k], ddof=0)[0, 1] for k in range(1, h)]
    var_d = (g0 + 2 * sum(gk)) / T
    stat = d_bar / np.sqrt(max(var_d, 1e-12))
    return float(stat), float(2 * (1 - stats.norm.cdf(abs(stat))))


def hln(dm_stat, T, h=1):
    factor = np.sqrt((T + 1 - 2 * h + h * (h - 1) / T) / T)
    hs = dm_stat * factor
    return float(hs), float(2 * (1 - stats.t.cdf(abs(hs), df=T - 1)))


print()
print("DM sign convention:")
print("  d_t = loss_benchmark_t - loss_candidate_t  (= e_RW^2 - e_model^2)")
print("  DM stat > 0 => candidate has LOWER loss than benchmark => candidate BEATS RW")
print("  DM stat < 0 => candidate has HIGHER loss => candidate WORSE than RW")
print()

best_pipe = list(pipeline_preds.keys())[0]
_res = pipeline_preds[best_pipe]
_acts = np.array(_res["actuals"])
_preds = _res["preds"]
e_bm = _acts - np.array(_preds["RW"])

print(f"DM + HLN  (benchmark=RW, pipeline={best_pipe}, T={len(_acts)})")
print("=" * 80)
print(f"{'Model':<12} {'mean(loss)':>12} {'RMSE':>10} {'DM stat':>9} {'DM p':>8} {'HLN stat':>10} {'HLN p':>8} {'sig':>5}")
print("-" * 80)

dm_summary = []
for mname in [m for m in _preds if m != "RW"]:
    e_m = _acts - np.array(_preds[mname])
    ml = (e_m ** 2).mean()
    rmse_m = np.sqrt(ml)
    dm_s, dm_p = dm_test(e_bm, e_m, h=1)
    hln_s, hln_p = hln(dm_s, T=len(_acts))
    sig = "***" if hln_p < 0.01 else ("**" if hln_p < 0.05 else ("*" if hln_p < 0.10 else "n.s."))
    print(f"{mname:<12} {ml:>12.8f} {rmse_m:>10.6f} {dm_s:>+9.3f} {dm_p:>8.4f} {hln_s:>+10.3f} {hln_p:>8.4f} {sig:>5}")
    dm_summary.append({"model": mname, "dm_stat": dm_s, "hln_p": hln_p, "sig": sig})

print()
print("Consistency check: RMSE = sqrt(mean_loss)")
print("  Positive DM => candidate beats RW;  Negative DM => candidate worse than RW")


# ============================================================================
# DM/HLN ACROSS ALL PIPELINES
# ============================================================================
print()
print("=" * 80)
print("FULL DM/HLN TABLE — all pipelines x models vs RW")
print("=" * 80)
print(f"{'Pipeline':<14} {'Model':<10} {'T':>5} {'DM stat':>9} {'DM p':>8} {'HLN p':>8} {'sig':>5}")
print("-" * 62)

all_dm = []
for pipe, data in pipeline_preds.items():
    acts_p = np.array(data["actuals"])
    preds_p = data["preds"]
    e_rw = acts_p - np.array(preds_p["RW"])
    for mname in ["AR1", "FXRidge", "GTRidge", "GT_FX"]:
        if mname not in preds_p:
            continue
        e_m = acts_p - np.array(preds_p[mname])
        dm_s, dm_p = dm_test(e_rw, e_m, h=1)
        hln_s, hln_p = hln(dm_s, T=len(acts_p))
        sig = "***" if hln_p < 0.01 else "**" if hln_p < 0.05 else "*" if hln_p < 0.10 else "n.s."
        print(f"{pipe:<14} {mname:<10} {len(acts_p):>5} {dm_s:>+9.3f} {dm_p:>8.4f} {hln_p:>8.4f} {sig:>5}")
        all_dm.append({"pipeline": pipe, "model": mname, "dm_stat": dm_s, "hln_p": hln_p, "sig": sig})


# ============================================================================
# RC + SPA
# ============================================================================
print()
print("=" * 80)
print("WHITE'S RC + HANSEN'S SPA")
print("=" * 80)


def whites_rc(loss_bm, loss_mat, B=2000, seed=42):
    rng = np.random.default_rng(seed)
    T, K = loss_mat.shape
    d = loss_bm[:, None] - loss_mat
    d_bar = d.mean(0)
    stat = float(d_bar.max())
    boots = np.array([
        (d[rng.integers(0, T, T)] - d_bar[None, :]).mean(0).max()
        for _ in range(B)])
    return stat, float((boots >= stat).mean())


def spa_test(loss_bm, loss_mat, B=2000, seed=42):
    rng = np.random.default_rng(seed)
    T, K = loss_mat.shape
    d = loss_bm[:, None] - loss_mat
    d_bar = d.mean(0)
    d_var = d.var(0, ddof=0)
    lam = np.sqrt(2 * np.log(np.log(max(T, 3))) * d_var / T)
    d_c = np.where(d_bar >= -lam, d_bar, 0.0)
    t_c = float(max(0, d_c.max()))
    boots_c = np.array([
        (d[rng.integers(0, T, T)] - d_c[None, :]).mean(0).max()
        for _ in range(B)])
    return {"spa_c": float((boots_c >= t_c).mean()), "t_c": t_c}


wrc_stat, wrc_p = whites_rc(loss_bm, loss_mat, B=2000)
spa_r = spa_test(loss_bm, loss_mat, B=2000)

print(f"White's Reality Check : stat={wrc_stat:.6f}  p-value={wrc_p:.4f}  "
      f"{'[REJECT H0]' if wrc_p < 0.05 else '[fail to reject]'}")
print(f"Hansen SPA_c          : stat={spa_r['t_c']:.6f}  p-value={spa_r['spa_c']:.4f}  "
      f"{'[REJECT H0]' if spa_r['spa_c'] < 0.05 else '[fail to reject]'}")
print()
print("H0: no candidate beats RW after accounting for model search.")

# Save updated outputs
rc_path = OUTPUTS / f"reality_check_pvalue_{PAIR}_{TARGET}.txt"
spa_path = OUTPUTS / f"spa_pvalue_{PAIR}_{TARGET}.txt"
rc_path.write_text(f"wrc_stat={wrc_stat:.6f}\nwrc_p={wrc_p:.4f}\n")
spa_path.write_text(f"spa_t_c={spa_r['t_c']:.6f}\nspa_p={spa_r['spa_c']:.4f}\n")

sig_path = OUTPUTS / f"significance_summary_{PAIR}_{TARGET}.txt"
lines = [f"pair={PAIR}\ntarget={TARGET}\nfx_source=REAL_MT5\n",
         f"wrc_stat={wrc_stat:.6f}\nwrc_p={wrc_p:.4f}\n",
         f"spa_t_c={spa_r['t_c']:.6f}\nspa_p={spa_r['spa_c']:.4f}\n"]
for row in all_dm:
    lines.append(f"DM_{row['pipeline']}_{row['model']}_"
                 f"stat={row['dm_stat']:.4f}_hln_p={row['hln_p']:.4f}_sig={row['sig']}\n")
sig_path.write_text("".join(lines))

# GT helps table
hurt_rows = []
for pipe_name, res in pipeline_results.items():
    r = res.set_index("model")
    ar_rmse = float(r.loc["AR1", "RMSE"]) if "AR1" in r.index else np.nan
    gt_rmse = float(r.loc["GTRidge", "RMSE"]) if "GTRidge" in r.index else np.nan
    gtfx_rmse = float(r.loc["GT_FX", "RMSE"]) if "GT_FX" in r.index else np.nan
    raw_delta = gt_rmse - ar_rmse
    hurt = "hurts" if raw_delta > 0 else "helps"
    hurt_rows.append({
        "pipeline": pipe_name,
        "FX-only(AR1)_RMSE": round(ar_rmse, 5),
        "GT-Ridge_RMSE": round(gt_rmse, 5),
        "GT+FX_RMSE": round(gtfx_rmse, 5),
        "GT_delta_vs_AR1": round(raw_delta, 5),
        "verdict": hurt,
    })
df_hurt = pd.DataFrame(hurt_rows)
hurt_path = OUTPUTS / f"gt_helps_table_{PAIR}_{TARGET}.csv"
df_hurt.to_csv(hurt_path, index=False)

print()
print("=" * 80)
print("GT HELPS/HURTS TABLE")
print("=" * 80)
print(df_hurt.to_string(index=False))
hc = (df_hurt["verdict"] == "hurts").sum()
hp = (df_hurt["verdict"] == "helps").sum()
print(f"\nGT hurts: {hc}/{len(df_hurt)},  GT helps: {hp}/{len(df_hurt)}")

print(f"\nSaved: {rc_path}")
print(f"Saved: {spa_path}")
print(f"Saved: {sig_path}")
print(f"Saved: {hurt_path}")
print(f"Saved: {lm_path}")
print()
print("END — all results use REAL MT5 data.")
