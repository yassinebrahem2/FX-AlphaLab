"""Fix the ForexFactory loading cell in 03macroanalysis2.1.ipynb.

Changes:
1. Fix operator-precedence bug in ff_files glob line
2. Map 'country' ISO codes (US/EU/GB) to currency codes (USD/EUR/GBP)
3. Normalise impact to title-case and strip 'non-economic' rows
4. Fix 'High' filter in Figure B surprise section
"""

import json
from pathlib import Path

NB_PATH = Path(__file__).parents[1] / "notebooks" / "03macroanalysis2.1.ipynb"
CELL_ID = "eaec896f"

NEW_SOURCE = """\n# \u2500\u2500 \u00a710 \u2014 ECONOMIC CALENDAR DATA LOADING & EDA (ForexFactory Silver) \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
# Loads ForexFactory events from data/processed/events/
# Graceful fallback if data has not been collected yet.

print("\u00a710 \u2014 Economic Calendar Events (ForexFactory)")
print("=" * 60)

ff_files = (list(FF_DIR.glob("*.parquet")) + list(FF_DIR.glob("*.csv"))) \\
           if FF_DIR.exists() else []

if not ff_files:
    print("\\n  \u26a0  No ForexFactory Silver files found.")
    print(f"     Expected: {FF_DIR}/events_*.csv  or  ff_events_*.parquet")
    print("\\n  To collect and preprocess ForexFactory data, run:")
    print("     python scripts/collect_forexfactory_data.py --preprocess")
    df_ff = pd.DataFrame()   # ensure downstream cells don't NameError
else:
    # \u2500\u2500 Load & concatenate all files \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    dfs_ff = []
    for f in sorted(ff_files):
        try:
            df_ = pd.read_parquet(f) if str(f).endswith('.parquet') else pd.read_csv(f)
            dfs_ff.append(df_)
        except Exception as exc:
            print(f"  \u2717 Could not read {f.name}: {exc}")

    if not dfs_ff:
        print("  \u2717  All ForexFactory files failed to load. Re-run the collector.")
        df_ff = pd.DataFrame()
    else:
        df_ff = pd.concat(dfs_ff, ignore_index=True)

        # \u2500\u2500 Normalise column names \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
        df_ff.columns = [c.lower() for c in df_ff.columns]

        # Timestamp
        ts_col = next((c for c in df_ff.columns
                       if 'time' in c.lower() or 'date' in c.lower()), df_ff.columns[0])
        df_ff = df_ff.rename(columns={ts_col: 'timestamp_utc'})
        df_ff['timestamp_utc'] = pd.to_datetime(df_ff['timestamp_utc'], utc=True, errors='coerce')
        df_ff = df_ff.dropna(subset=['timestamp_utc']).sort_values('timestamp_utc')

        # \u2500\u2500 Country \u2192 Currency mapping \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
        # Silver file uses ISO country codes; map to FX currency codes.
        COUNTRY_TO_CURRENCY = {
            'US': 'USD', 'EU': 'EUR', 'GB': 'GBP',
            'JP': 'JPY', 'CA': 'CAD', 'AU': 'AUD', 'NZ': 'NZD',
            'CH': 'CHF', 'CN': 'CNY',
        }
        curr_col    = next((c for c in df_ff.columns if 'curr' in c.lower()), None)
        country_col = next((c for c in df_ff.columns if c == 'country' or 'countr' in c.lower()), None)
        if curr_col:
            df_ff = df_ff.rename(columns={curr_col: 'currency'})
        elif country_col:
            df_ff['currency'] = df_ff[country_col].map(COUNTRY_TO_CURRENCY)
        if 'currency' in df_ff.columns:
            df_ff = df_ff[df_ff['currency'].isin(FF_CURRENCIES)].copy()

        # \u2500\u2500 Normalise impact to title-case \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
        # Raw data ships lowercase ('high', 'medium', 'low', 'non-economic')
        if 'impact' in df_ff.columns:
            df_ff['impact'] = df_ff['impact'].astype(str).str.title()
            df_ff = df_ff[~df_ff['impact'].isin(['Non-Economic', 'Holiday', 'None', 'Nan'])]

        # \u2500\u2500 Summary \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
        print(f"  \u2714 Loaded {len(df_ff):,} events  "
              f"({df_ff['timestamp_utc'].min().date()} \u2192 {df_ff['timestamp_utc'].max().date()})")
        if 'currency' in df_ff.columns:
            print(f"  Currencies: {dict(df_ff['currency'].value_counts())}")
        if 'impact' in df_ff.columns:
            print(f"  Impact levels: {dict(df_ff['impact'].value_counts())}")

        # \u2500\u2500 FIGURE A: Event frequency by currency \u00d7 impact level \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
        if 'currency' in df_ff.columns and 'impact' in df_ff.columns:
            IMPACT_ORDER  = ['High', 'Medium', 'Low']
            IMPACT_COLORS = {'High': '#e74c3c', 'Medium': '#f39c12', 'Low': '#3498db'}
            CURR_ORDER    = [c for c in ['USD', 'EUR', 'GBP'] if c in df_ff['currency'].unique()]

            pivot_ci = (df_ff.groupby(['currency', 'impact'])
                        .size()
                        .unstack(fill_value=0)
                        .reindex(CURR_ORDER))

            # Keep only impact levels that exist in data
            levels = [l for l in IMPACT_ORDER if l in pivot_ci.columns]
            pivot_ci = pivot_ci[levels]

            fig, axes = plt.subplots(1, 2, figsize=(15, 6))

            # Grouped bar
            x = np.arange(len(pivot_ci))
            bar_width = 0.25
            for i, lev in enumerate(levels):
                offset = (i - len(levels) / 2 + 0.5) * bar_width
                axes[0].bar(x + offset, pivot_ci[lev], bar_width,
                            label=lev, color=IMPACT_COLORS[lev], alpha=0.85, edgecolor='white')
            axes[0].set_xticks(x)
            axes[0].set_xticklabels(pivot_ci.index, fontsize=12)
            axes[0].set_title('\u00a710.A \u2014 Event Count by Currency & Impact Level', fontweight='bold')
            axes[0].set_ylabel('Number of events')
            axes[0].legend(title='Impact', fontsize=9)
            axes[0].grid(True, alpha=0.3, axis='y')

            # Stacked 100% bar
            totals = pivot_ci.sum(axis=1)
            bottom = np.zeros(len(pivot_ci))
            for lev in levels:
                pct = pivot_ci[lev] / totals * 100
                axes[1].bar(x, pct, bar_width * 3, bottom=bottom,
                            label=lev, color=IMPACT_COLORS[lev], alpha=0.85, edgecolor='white')
                # Label segments > 8%
                for j, (p, b) in enumerate(zip(pct, bottom)):
                    if p > 8:
                        axes[1].text(x[j], b + p / 2, f'{p:.0f}%',
                                     ha='center', va='center', fontsize=9, color='white', fontweight='bold')
                bottom += pct.values
            axes[1].set_xticks(x)
            axes[1].set_xticklabels(pivot_ci.index, fontsize=12)
            axes[1].set_title('\u00a710.A \u2014 Impact Level Mix by Currency (%)', fontweight='bold')
            axes[1].set_ylabel('Share (%)')
            axes[1].set_ylim(0, 105)
            axes[1].legend(title='Impact', fontsize=9)
            axes[1].grid(True, alpha=0.3, axis='y')

            plt.tight_layout()
            plt.show()

        # \u2500\u2500 FIGURE B: Surprise distribution for high-impact USD events \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
        actual_col   = next((c for c in df_ff.columns if 'actual'   in c.lower()), None)
        forecast_col = next((c for c in df_ff.columns if 'forecast' in c.lower()), None)
        event_col    = next((c for c in df_ff.columns
                             if 'event' in c.lower() or 'name' in c.lower() or 'title' in c.lower()), None)

        if actual_col and forecast_col and event_col and 'impact' in df_ff.columns:
            df_ff = df_ff.rename(columns={
                actual_col: 'actual', forecast_col: 'forecast', event_col: 'event_name'
            })
            df_ff['actual']   = pd.to_numeric(df_ff['actual'],   errors='coerce')
            df_ff['forecast'] = pd.to_numeric(df_ff['forecast'], errors='coerce')
            df_ff['surprise'] = df_ff['actual'] - df_ff['forecast']

            # Select top high-impact USD events by frequency
            _currency_series = df_ff['currency'] if 'currency' in df_ff.columns else pd.Series(dtype=str)
            hi_usd = df_ff[
                (_currency_series == 'USD') &
                (df_ff['impact'] == 'High') &
                df_ff['surprise'].notna()
            ].copy()

            top_events = hi_usd['event_name'].value_counts().head(6).index.tolist()
            df_top = hi_usd[hi_usd['event_name'].isin(top_events)]

            if len(df_top) > 20:
                fig, axes = plt.subplots(2, 3, figsize=(16, 9))
                axes = axes.flatten()
                palette = ['#e74c3c', '#3498db', '#2ecc71', '#f39c12', '#9b59b6', '#1abc9c']

                for ax, (event, color) in zip(axes, zip(top_events, palette)):
                    subset = df_top[df_top['event_name'] == event]['surprise'].dropna()
                    if len(subset) < 3:
                        ax.axis('off')
                        continue
                    ax.hist(subset, bins=min(15, len(subset)),
                            color=color, alpha=0.8, edgecolor='white')
                    ax.axvline(0, color='black', linewidth=1.5, linestyle='--')
                    ax.axvline(subset.mean(), color='red', linewidth=1.5, linestyle=':',
                               label=f'mean={subset.mean():.2f}')
                    ax.set_title(event, fontsize=9, fontweight='bold')
                    ax.set_xlabel('Surprise (actual \u2212 forecast)')
                    ax.legend(fontsize=7)
                    ax.grid(True, alpha=0.3)

                plt.suptitle('\u00a710.B \u2014 Surprise Distribution: Top High-Impact USD Events',
                             fontsize=13, fontweight='bold', y=1.01)
                plt.tight_layout()
                plt.show()
            else:
                print("  \u26a0  Insufficient high-impact USD surprises for distribution plot.")
        else:
            print("  \u26a0  Missing actual/forecast/event columns \u2014 skipping surprise analysis.")
"""


def main():
    with open(NB_PATH, encoding="utf-8") as f:
        nb = json.load(f)

    target = next((c for c in nb["cells"] if c.get("id") == CELL_ID), None)
    if target is None:
        print(f"ERROR: cell {CELL_ID} not found")
        return

    # Convert the multi-line string into the list-of-strings format
    # that Jupyter notebooks use internally.
    lines = NEW_SOURCE.splitlines(keepends=True)
    target["source"] = lines
    target["outputs"] = []
    target["execution_count"] = None

    with open(NB_PATH, "w", encoding="utf-8") as f:
        json.dump(nb, f, ensure_ascii=False, indent=1)

    print(f"SUCCESS: cell {CELL_ID} updated and notebook saved.")


if __name__ == "__main__":
    main()
