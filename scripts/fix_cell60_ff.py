"""Fix cell 60 (§10 ForexFactory loader) to match actual CSV schema."""

import json
from pathlib import Path

path = Path(__file__).parent.parent / "notebooks" / "03macroanalysis2.1.ipynb"

new_source = """\
# ── §10 — ECONOMIC CALENDAR DATA LOADING & EDA (ForexFactory Silver) ──────────
# Loads ForexFactory events from data/processed/events/
# Actual schema: timestamp_utc, event_id, country, event_name, impact,
#                actual, forecast, previous, source
# country codes: US, EU, GB, JP, AU, CA, CH, NZ, CN  (ISO-2)
# impact values: high, medium, low, non-economic  (lowercase in CSV)

print("\\u00a710 \\u2014 Economic Calendar Events (ForexFactory)")
print("=" * 60)

# ── Country-code → ISO currency mapping ──────────────────────────────────────
COUNTRY_TO_CURRENCY = {
    "US": "USD", "EU": "EUR", "GB": "GBP", "JP": "JPY",
    "AU": "AUD", "CA": "CAD", "CH": "CHF", "NZ": "NZD",
    "CN": "CNY", "HK": "HKD", "SG": "SGD", "MX": "MXN",
    "SE": "SEK", "NO": "NOK", "DK": "DKK", "TR": "TRY",
    "ZA": "ZAR", "BR": "BRL", "IN": "INR",
}

ff_files = (list(FF_DIR.glob("*.parquet")) + list(FF_DIR.glob("*.csv"))) \\
           if FF_DIR.exists() else []

if not ff_files:
    print("\\n  \\u26a0  No ForexFactory Silver files found.")
    print(f"     Expected: {FF_DIR}/events_*.csv  or  ff_events_*.parquet")
    df_ff = pd.DataFrame()
else:
    dfs_ff = []
    for f in sorted(ff_files):
        try:
            df_ = pd.read_parquet(f) if str(f).endswith(".parquet") else pd.read_csv(f)
            dfs_ff.append(df_)
        except Exception as exc:
            print(f"  \\u2717 Could not read {f.name}: {exc}")

    if not dfs_ff:
        print("  \\u2717  All ForexFactory files failed to load.")
        df_ff = pd.DataFrame()
    else:
        df_ff = pd.concat(dfs_ff, ignore_index=True)
        df_ff.columns = [c.lower().strip() for c in df_ff.columns]

        # ── Timestamp ────────────────────────────────────────────────────────
        ts_col = next((c for c in df_ff.columns if "time" in c or "date" in c), df_ff.columns[0])
        df_ff = df_ff.rename(columns={ts_col: "timestamp_utc"})
        df_ff["timestamp_utc"] = pd.to_datetime(df_ff["timestamp_utc"], utc=True, errors="coerce")
        df_ff = df_ff.dropna(subset=["timestamp_utc"]).sort_values("timestamp_utc").reset_index(drop=True)

        # ── Country → Currency ───────────────────────────────────────────────
        # CSV has \'country\' column with ISO-2 country codes (US, EU, GB ...)
        if "country" in df_ff.columns:
            df_ff["currency"] = df_ff["country"].str.upper().map(COUNTRY_TO_CURRENCY).fillna(df_ff["country"])

        # ── Impact: strip non-economic rows, then title-case ─────────────────
        if "impact" in df_ff.columns:
            df_ff["impact"] = df_ff["impact"].astype(str).str.strip()
            df_ff = df_ff[
                ~df_ff["impact"].str.lower().isin(["non-economic", "holiday", "none", "nan"])
            ].copy()
            df_ff["impact"] = df_ff["impact"].str.title()   # high->High, medium->Medium

        # ── Filter to major currencies ────────────────────────────────────────
        if "currency" in df_ff.columns:
            df_ff = df_ff[df_ff["currency"].isin(FF_CURRENCIES)].copy()

        # ── Numeric columns & surprise ────────────────────────────────────────
        for col in ["actual", "forecast", "previous"]:
            if col in df_ff.columns:
                df_ff[col] = pd.to_numeric(df_ff[col], errors="coerce")
        if "actual" in df_ff.columns and "forecast" in df_ff.columns:
            df_ff["surprise"] = df_ff["actual"] - df_ff["forecast"]

        # ── Summary ──────────────────────────────────────────────────────────
        print(f"  \\u2713 Loaded {len(df_ff):,} events  "
              f"({df_ff[\'timestamp_utc\'].min().date()} \\u2192 {df_ff[\'timestamp_utc\'].max().date()})")
        if "currency" in df_ff.columns:
            print(f"  Currencies : {dict(df_ff[\'currency\'].value_counts())}")
        if "impact" in df_ff.columns:
            print(f"  Impact levels: {dict(df_ff[\'impact\'].value_counts())}")
        surp_n = df_ff["surprise"].notna().sum() if "surprise" in df_ff.columns else 0
        print(f"  Events with surprise data: {surp_n:,}")

        # ── FIGURE A: Event frequency by currency x impact level ──────────────
        if "currency" in df_ff.columns and "impact" in df_ff.columns:
            IMPACT_ORDER  = ["High", "Medium", "Low"]
            IMPACT_COLORS = {"High": "#e74c3c", "Medium": "#f39c12", "Low": "#3498db"}
            CURR_ORDER    = [c for c in ["USD", "EUR", "GBP"] if c in df_ff["currency"].unique()]

            pivot_ci = (df_ff.groupby(["currency", "impact"])
                        .size()
                        .unstack(fill_value=0)
                        .reindex(CURR_ORDER))

            levels = [l for l in IMPACT_ORDER if l in pivot_ci.columns]
            pivot_ci = pivot_ci[levels]

            fig, axes = plt.subplots(1, 2, figsize=(15, 6))
            x = np.arange(len(pivot_ci))
            bar_width = 0.25

            for i, lev in enumerate(levels):
                offset = (i - len(levels) / 2 + 0.5) * bar_width
                axes[0].bar(x + offset, pivot_ci[lev], bar_width,
                            label=lev, color=IMPACT_COLORS[lev], alpha=0.85, edgecolor="white")
            axes[0].set_xticks(x)
            axes[0].set_xticklabels(pivot_ci.index, fontsize=12)
            axes[0].set_title("\\u00a710.A \\u2014 Event Count by Currency & Impact Level", fontweight="bold")
            axes[0].set_ylabel("Number of events")
            axes[0].legend(title="Impact", fontsize=9)
            axes[0].grid(True, alpha=0.3, axis="y")

            totals = pivot_ci.sum(axis=1)
            bottom = np.zeros(len(pivot_ci))
            for lev in levels:
                pct = pivot_ci[lev] / totals * 100
                axes[1].bar(x, pct, bar_width * 3, bottom=bottom,
                            label=lev, color=IMPACT_COLORS[lev], alpha=0.85, edgecolor="white")
                for j, (p, b) in enumerate(zip(pct, bottom)):
                    if p > 8:
                        axes[1].text(x[j], b + p / 2, f"{p:.0f}%",
                                     ha="center", va="center", fontsize=9,
                                     color="white", fontweight="bold")
                bottom += pct.values
            axes[1].set_xticks(x)
            axes[1].set_xticklabels(pivot_ci.index, fontsize=12)
            axes[1].set_title("\\u00a710.A \\u2014 Impact Level Mix by Currency (%)", fontweight="bold")
            axes[1].set_ylabel("Share (%)")
            axes[1].set_ylim(0, 105)
            axes[1].legend(title="Impact", fontsize=9)
            axes[1].grid(True, alpha=0.3, axis="y")
            plt.tight_layout()
            plt.show()

        # ── FIGURE B: Surprise distribution for high-impact USD events ─────────
        if "surprise" in df_ff.columns and "currency" in df_ff.columns and "impact" in df_ff.columns:
            hi_usd = df_ff[
                (df_ff["currency"] == "USD") &
                (df_ff["impact"] == "High") &
                df_ff["surprise"].notna()
            ].copy()

            top_events = hi_usd["event_name"].value_counts().head(6).index.tolist()
            df_top = hi_usd[hi_usd["event_name"].isin(top_events)]

            if len(df_top) > 20:
                fig, axes = plt.subplots(2, 3, figsize=(16, 9))
                axes = axes.flatten()
                palette = ["#e74c3c", "#3498db", "#2ecc71", "#f39c12", "#9b59b6", "#1abc9c"]

                for ax, (event, color) in zip(axes, zip(top_events, palette)):
                    subset = df_top[df_top["event_name"] == event]["surprise"].dropna()
                    if len(subset) < 3:
                        ax.axis("off")
                        continue
                    ax.hist(subset, bins=min(15, len(subset)),
                            color=color, alpha=0.8, edgecolor="white")
                    ax.axvline(0, color="black", linewidth=1.5, linestyle="--")
                    ax.axvline(subset.mean(), color="red", linewidth=1.5, linestyle=":",
                               label=f"Mean: {subset.mean():.2f}")
                    ax.set_title(f"{event}\\n(n={len(subset)}  \\u03c3={subset.std():.2f})",
                                 fontweight="bold", fontsize=9)
                    ax.set_xlabel("Surprise (actual \\u2212 forecast)")
                    ax.legend(fontsize=8)
                    ax.grid(True, alpha=0.3)

                plt.suptitle("\\u00a710.B \\u2014 Surprise Distribution for Top High-Impact USD Events\\n"
                             "Asymmetric distributions reveal analyst forecast bias",
                             fontsize=13, fontweight="bold")
                plt.tight_layout()
                plt.show()
            else:
                print("  \\u26a0  Insufficient high-impact USD surprise data for \\u00a710.B histogram")

        # ── FIGURE C: Monthly event density heatmap ────────────────────────────
        df_ff["year"]  = df_ff["timestamp_utc"].dt.year
        df_ff["month"] = df_ff["timestamp_utc"].dt.month

        _hi_med = df_ff[df_ff["impact"].isin(["High", "Medium"])] if "impact" in df_ff.columns else df_ff
        monthly_counts = (_hi_med.groupby(["year", "month"])
                          .size()
                          .unstack(level="month", fill_value=0))

        if not monthly_counts.empty:
            fig, ax = plt.subplots(figsize=(14, max(4, len(monthly_counts) * 0.8)))
            im = ax.imshow(monthly_counts.values, aspect="auto",
                           cmap="YlOrRd", interpolation="nearest")
            ax.set_xticks(range(12))
            ax.set_xticklabels(["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                 "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], fontsize=10)
            ax.set_yticks(range(len(monthly_counts)))
            ax.set_yticklabels(monthly_counts.index, fontsize=10)
            for row in range(monthly_counts.shape[0]):
                for col in range(monthly_counts.shape[1]):
                    val = monthly_counts.values[row, col]
                    text_color = "white" if val > monthly_counts.values.max() * 0.6 else "black"
                    ax.text(col, row, f"{int(val)}", ha="center", va="center",
                            fontsize=9, color=text_color, fontweight="bold")
            plt.colorbar(im, ax=ax, label="Number of High+Medium impact events")
            ax.set_title("\\u00a710.C \\u2014 Monthly Economic Calendar Event Density (High + Medium Impact)\\n"
                         "Sparse months may create data gaps in macro analysis",
                         fontweight="bold", fontsize=12)
            plt.tight_layout()
            plt.show()

        # ── FIGURE D: High-impact USD events timeline vs DFF ──────────────────
        hi_usd_all = df_ff[
            (df_ff["currency"] == "USD") & (df_ff["impact"] == "High")
        ] if "currency" in df_ff.columns and "impact" in df_ff.columns else pd.DataFrame()

        if not hi_usd_all.empty:
            _dff_rows = df_macro[df_macro["series_id"] == "DFF"].copy()
            dff_series = _dff_rows.sort_values("timestamp_utc").set_index("timestamp_utc")["value"]

            if len(dff_series) > 10:
                fig, axes = plt.subplots(2, 1, figsize=(16, 8), sharex=True)
                axes[0].plot(dff_series.index, dff_series.values,
                             color="#2c3e50", linewidth=1.8, label="Fed Funds Rate (DFF)")
                axes[0].set_ylabel("Rate (%)")
                axes[0].set_title("\\u00a710.D \\u2014 Fed Funds Rate + High-Impact USD Event Timeline",
                                  fontweight="bold")
                axes[0].legend(fontsize=9)
                axes[0].grid(True, alpha=0.3)

                FOMC_KEYWORDS = ["fed", "fomc", "rate decision", "nfp", "non-farm", "cpi", "gdp"]
                for _, row in hi_usd_all.iterrows():
                    is_fomc = any(kw in str(row.get("event_name", "")).lower() for kw in FOMC_KEYWORDS)
                    axes[1].axvline(row["timestamp_utc"],
                                   color="#e74c3c" if is_fomc else "#f39c12",
                                   alpha=0.4, linewidth=1.2)

                from matplotlib.lines import Line2D
                axes[1].legend(handles=[
                    Line2D([0], [0], color="#e74c3c", linewidth=2, label="NFP / FOMC / CPI / GDP"),
                    Line2D([0], [0], color="#f39c12", linewidth=2, label="Other high-impact USD"),
                ], fontsize=9, loc="upper right")
                axes[1].set_ylabel("Event marker")
                axes[1].set_title("High-Impact USD Events (rug)", fontweight="bold")
                axes[1].set_yticks([])
                axes[1].grid(True, alpha=0.3, axis="x")
                plt.tight_layout()
                plt.show()
            else:
                print("  \\u26a0  \\u00a710.D skipped \\u2014 DFF data not found in df_macro")
        else:
            print("  \\u26a0  \\u00a710.D skipped \\u2014 no high-impact USD events found")

        print("\\n\\u2713 \\u00a710 ForexFactory calendar analysis complete")
        print(f"  Total events analysed: {len(df_ff):,}")
        if "impact" in df_ff.columns:
            print(f"  High-impact events: {(df_ff[\'impact\'] == \'High\').sum():,}")
        if "surprise" in df_ff.columns:
            print(f"  Events with surprise data: {df_ff[\'surprise\'].notna().sum():,}")
"""

with open(path, encoding="utf-8") as f:
    nb = json.load(f)

target_idx = None
for i, cell in enumerate(nb["cells"]):
    src = "".join(cell["source"])
    if "ECONOMIC CALENDAR DATA LOADING" in src and cell["cell_type"] == "code":
        target_idx = i
        break

if target_idx is None:
    print("ERROR: target cell not found")
else:
    nb["cells"][target_idx]["source"] = new_source
    nb["cells"][target_idx]["outputs"] = []
    nb["cells"][target_idx]["execution_count"] = None
    with open(path, "w", encoding="utf-8") as f:
        json.dump(nb, f, ensure_ascii=False, indent=1)
    print(f"OK: cell {target_idx + 1} updated (id={nb['cells'][target_idx]['id']})")
