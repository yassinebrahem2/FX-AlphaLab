"""Fix the OHLCV glob pattern in the notebook to match MT5 Silver file naming convention."""

from pathlib import Path

NOTEBOOK = Path("notebooks/03macroanalysis2.1.ipynb")

content = NOTEBOOK.read_text(encoding="utf-8")

# ── Target: the two-line glob that only matches legacy names ──────────────────
OLD = (
    '"    candidates = list(OHLCV_DIR.glob(f\\"{pair}_D1*.parquet\\")) + \\\\\\n",\n'
    '    "                 list(OHLCV_DIR.glob(f\\"{pair}_D1*.csv\\"))\\n",'
)

# ── Replacement: also match MT5 ohlcv_{PAIR}m_D1_* naming ───────────────────
NEW = (
    '"    # MT5 Silver naming: ohlcv_{PAIR}m_D1_{START}_{END}.parquet\\n",\n'
    '    "    candidates = (\\n",\n'
    '    "        list(OHLCV_DIR.glob(f\\"ohlcv_{pair}m_D1*.parquet\\")) +\\n",\n'
    '    "        list(OHLCV_DIR.glob(f\\"ohlcv_{pair}_D1*.parquet\\")) +\\n",\n'
    '    "        list(OHLCV_DIR.glob(f\\"{pair}_D1*.parquet\\")) +\\n",\n'
    '    "        list(OHLCV_DIR.glob(f\\"{pair}_D1*.csv\\"))\\n",\n'
    '    "    )\\n",'
)

print(f"OLD found: {content.count(OLD)}")
assert content.count(OLD) == 1, "Expected exactly 1 occurrence"

content = content.replace(OLD, NEW, 1)
NOTEBOOK.write_text(content, encoding="utf-8")
print("✓ Glob pattern fixed in cell 57")

# ── Also fix the TARGET_PAIR fallback cell (direct path construction) ─────────
OLD2 = (
    "\"    _fx_path = OHLCV_DIR / f'{TARGET_PAIR}_D1.parquet'\\n\",\n"
    "    \"    _fx_csv  = OHLCV_DIR / f'{TARGET_PAIR}_D1.csv'\\n\",\n"
    '    "    _fx_file = _fx_path if _fx_path.exists() else (_fx_csv if _fx_csv.exists() else None)\\n",'
)

NEW2 = (
    '"    # MT5 Silver naming: ohlcv_{PAIR}m_D1_{START}_{END}.parquet\\n",\n'
    '    "_fx_candidates = (\\n",\n'
    "    \"    list(OHLCV_DIR.glob(f'ohlcv_{TARGET_PAIR}m_D1*.parquet')) +\\n\",\n"
    "    \"    list(OHLCV_DIR.glob(f'ohlcv_{TARGET_PAIR}_D1*.parquet')) +\\n\",\n"
    "    \"    list(OHLCV_DIR.glob(f'{TARGET_PAIR}_D1*.parquet')) +\\n\",\n"
    "    \"    list(OHLCV_DIR.glob(f'{TARGET_PAIR}_D1*.csv'))\\n\",\n"
    '    ")\\n",\n'
    '    "_fx_file = sorted(_fx_candidates)[-1] if _fx_candidates else None\\n",'
)

count2 = content.count(OLD2)
print(f"OLD2 found: {count2}")
if count2 == 1:
    content = content.replace(OLD2, NEW2, 1)
    NOTEBOOK.write_text(content, encoding="utf-8")
    print("✓ TARGET_PAIR fallback path also fixed")
else:
    print("  (skipping TARGET_PAIR fix - pattern not found or count mismatch)")

print("Done.")
