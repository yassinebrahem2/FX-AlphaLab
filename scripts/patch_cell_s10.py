"""Patch §10 ForexFactory cell to handle actual CSV schema (country col, lowercase impact)."""

import json
import pathlib

NB_PATH = pathlib.Path(__file__).parent.parent / "notebooks" / "03macroanalysis2.1.ipynb"
TARGET_ID = "d5fcce1f"

NEW_SOURCE = """\
# ── §10 — ECONOMIC CALENDAR DATA LOADING & EDA (ForexFactory Silver) ──────────
# Loads ForexFactory events from data/processed/events/
# Graceful fallback if data has not been collected yet.

print("§10 — Economic Calendar Events (ForexFactory)")
print("=" * 60)

# Country code → currency code mapping (ForexFactory convention)
COUNTRY_TO_CURRENCY = {
    "US": "USD", "EU": "EUR", "GB": "GBP", "JP": "JPY",
    "AU": "AUD", "CA": "CAD", "CH": "CHF", "NZ": "NZD", "CN": "CNY",
}

ff_files = (list(FF_DIR.glob("*.parquet")) + list(FF_DIR.glob("*.csv"))) \\
           if FF_DIR.exists() else []

if not ff_files:
    print("\\n  ⚠  No ForexFactory Silver files found.")
    print(f"     Expected: {FF_DIR}/ff_events_*.parquet or *.csv")
    print("\\n  To collect and preprocess ForexFactory data, run:")
    print("     python scripts/collect_forexfactory_data.py --preprocess")
    df_ff = pd.DataFrame()
else:
    # ── Load & concatenate all shards ──────────────────────────────────────────
    dfs_ff = []
    for f in sorted(ff_files):
        try:
            df_ = pd.read_parquet(f) if str(f).endswith(".parquet") else pd.read_csv(f)
            dfs_ff.append(df_)
        except Exception as exc:
            print(f"  ✗ Could not read {f.name}: {exc}")

    if not dfs_ff:
        print("  ✗  All ForexFactory files failed to load. Re-run the collector.")
        df_ff = pd.DataFrame()
    else:
        df_ff = pd.concat(dfs_ff, ignore_index=True)

        # ── Normalise column names ─────────────────────────────────────────────
        df_ff.columns = [c.lower() for c in df_ff.columns]

        # Timestamp
        ts_col = next(
            (c for c in df_ff.columns if "time" in c or "date" in c),
            df_ff.columns[0],
        )
        df_ff = df_ff.rename(columns={ts_col: "timestamp_utc"})
        df_ff["timestamp_utc"] = pd.to_datetime(
            df_ff["timestamp_utc"], utc=True, errors="coerce"
        )
        df_ff = df_ff.dropna(subset=["timestamp_utc"]).sort_values("timestamp_utc")

        # Country → currency  (handles "country" col used by ForexFactory Silver)
        if "country" in df_ff.columns and "currency" not in df_ff.columns:
            df_ff["currency"] = df_ff["country"].map(COUNTRY_TO_CURRENCY)
        elif "currency" not in df_ff.columns:
            curr_col = next((c for c in df_ff.columns if "curr" in c), None)
            if curr_col:
                df_ff = df_ff.rename(columns={curr_col: "currency"})

        # Filter to target currencies
        if "currency" in df_ff.columns:
            df_ff = df_ff[df_ff["currency"].isin(FF_CURRENCIES)].copy()

        # Impact: normalise to Title-case, drop non-economic / holiday rows
        if "impact" in df_ff.columns:
            df_ff["impact"] = df_ff["impact"].astype(str).str.strip().str.title()
            df_ff = df_ff[
                ~df_ff["impact"].isin(["Non-Economic", "Holiday", "None", "Nan"])
            ].copy()

        # ── Summary ───────────────────────────────────────────────────────────
        print(f"  ✔ Loaded {len(df_ff):,} events  "
              f"({df_ff['timestamp_utc'].min().date()} → {df_ff['timestamp_utc'].max().date()})")
        if "currency" in df_ff.columns:
            print(f"  Currencies : {dict(df_ff['currency'].value_counts())}")
        if "impact" in df_ff.columns:
            print(f"  Impact lvls: {dict(df_ff['impact'].value_counts())}")

        # ── FIGURE A: Event frequency by currency × impact level ───────────────
        if "currency" in df_ff.columns and "impact" in df_ff.columns:
            IMPACT_ORDER  = ["High", "Medium", "Low"]
            IMPACT_COLORS = {"High": "#e74c3c", "Medium": "#f39c12", "Low": "#3498db"}
            CURR_ORDER    = [c for c in ["USD", "EUR", "GBP"] if c in df_ff["currency"].unique()]

            pivot_ci = (
                df_ff.groupby(["currency", "impact"])
                .size()
                .unstack(fill_value=0)
                .reindex(CURR_ORDER)
            )
            levels = [l for l in IMPACT_ORDER if l in pivot_ci.columns]
            pivot_ci = pivot_ci[levels]

            fig, axes = plt.subplots(1, 2, figsize=(15, 6))
            x = np.arange(len(pivot_ci))
            bar_width = 0.25

            for i, lev in enumerate(levels):
                offset = (i - len(levels) / 2 + 0.5) * bar_width
                axes[0].bar(
                    x + offset, pivot_ci[lev], bar_width,
                    label=lev, color=IMPACT_COLORS[lev], alpha=0.85, edgecolor="white",
                )
            axes[0].set_xticks(x)
            axes[0].set_xticklabels(pivot_ci.index, fontsize=12)
            axes[0].set_title("§10.A — Event Count by Currency & Impact Level", fontweight="bold")
            axes[0].set_ylabel("Number of events")
            axes[0].legend(title="Impact", fontsize=9)
            axes[0].grid(True, alpha=0.3, axis="y")

            totals = pivot_ci.sum(axis=1)
            bottom = np.zeros(len(pivot_ci))
            for lev in levels:
                pct = pivot_ci[lev] / totals * 100
                axes[1].bar(
                    x, pct, bar_width * 3, bottom=bottom,
                    label=lev, color=IMPACT_COLORS[lev], alpha=0.85, edgecolor="white",
                )
                for j, (p, b) in enumerate(zip(pct, bottom)):
                    if p > 8:
                        axes[1].text(
                            x[j], b + p / 2, f"{p:.0f}%",
                            ha="center", va="center", fontsize=9,
                            color="white", fontweight="bold",
                        )
                bottom += pct.values
            axes[1].set_xticks(x)
            axes[1].set_xticklabels(pivot_ci.index, fontsize=12)
            axes[1].set_title("§10.A — Impact Level Mix by Currency (%)", fontweight="bold")
            axes[1].set_ylabel("Share (%)")
            axes[1].set_ylim(0, 105)
            axes[1].legend(title="Impact", fontsize=9)
            axes[1].grid(True, alpha=0.3, axis="y")

            plt.tight_layout()
            plt.show()

        # ── FIGURE B: Surprise distribution for high-impact USD events ─────────
        actual_col   = next((c for c in df_ff.columns if c == "actual"),   None)
        forecast_col = next((c for c in df_ff.columns if c == "forecast"), None)
        event_col    = next(
            (c for c in df_ff.columns if c in ("event_name", "event", "name", "title")),
            None,
        )

        if actual_col and forecast_col and event_col and "impact" in df_ff.columns:
            df_ff = df_ff.rename(columns={event_col: "event_name"})
            df_ff["actual"]   = pd.to_numeric(df_ff["actual"],   errors="coerce")
            df_ff["forecast"] = pd.to_numeric(df_ff["forecast"], errors="coerce")
            df_ff["surprise"] = df_ff["actual"] - df_ff["forecast"]

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
                    ax.set_title(
                        f"{event}\\n(n={len(subset)}  σ={subset.std():.2f})",
                        fontweight="bold", fontsize=9,
                    )
                    ax.set_xlabel("Surprise (actual − forecast)")
                    ax.legend(fontsize=8)
                    ax.grid(True, alpha=0.3)

                plt.suptitle(
                    "§10.B — Surprise Distribution for Top High-Impact USD Events\\n"
                    "Asymmetric distributions reveal analyst forecast bias",
                    fontsize=13, fontweight="bold",
                )
                plt.tight_layout()
                plt.show()
            else:
                print("  ⚠  Insufficient high-impact USD surprise data for §10.B histogram")

        # ── FIGURE C: Monthly event density heatmap ────────────────────────────
        df_ff["year"]  = df_ff["timestamp_utc"].dt.year
        df_ff["month"] = df_ff["timestamp_utc"].dt.month

        impact_mask = (
            df_ff["impact"].isin(["High", "Medium"])
            if "impact" in df_ff.columns
            else pd.Series(True, index=df_ff.index)
        )
        monthly_counts = (
            df_ff[impact_mask]
            .groupby(["year", "month"])
            .size()
            .unstack(level="month", fill_value=0)
        )

        if not monthly_counts.empty:
            fig, ax = plt.subplots(figsize=(14, max(4, len(monthly_counts) * 0.8)))
            im = ax.imshow(monthly_counts.values, aspect="auto",
                           cmap="YlOrRd", interpolation="nearest")

            ax.set_xticks(range(12))
            ax.set_xticklabels(
                ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                 "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
                fontsize=10,
            )
            ax.set_yticks(range(len(monthly_counts)))
            ax.set_yticklabels(monthly_counts.index, fontsize=10)

            for row in range(monthly_counts.shape[0]):
                for col in range(monthly_counts.shape[1]):
                    val = monthly_counts.values[row, col]
                    text_color = "white" if val > monthly_counts.values.max() * 0.6 else "black"
                    ax.text(col, row, str(val), ha="center", va="center",
                            fontsize=8, color=text_color)

            plt.colorbar(im, ax=ax, label="Event count")
            ax.set_title(
                "§10.C — Monthly High+Medium Impact Event Density",
                fontweight="bold", fontsize=13,
            )
            plt.tight_layout()
            plt.show()
"""

nb = json.loads(NB_PATH.read_text(encoding="utf-8"))

found = False
for cell in nb["cells"]:
    if cell.get("id", "") == TARGET_ID:
        cell["source"] = NEW_SOURCE
        cell["outputs"] = []
        cell["execution_count"] = None
        found = True
        break

if not found:
    raise RuntimeError(f"Cell {TARGET_ID} not found in notebook")

NB_PATH.write_text(json.dumps(nb, ensure_ascii=False, indent=1), encoding="utf-8")
print(f"✔ Patched cell {TARGET_ID} in {NB_PATH.name}")
