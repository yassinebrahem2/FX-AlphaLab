"""Fix indentation of _fx_candidates block in the TARGET_PAIR fallback cell."""

from pathlib import Path

NOTEBOOK = Path("notebooks/03macroanalysis2.1.ipynb")
content = NOTEBOOK.read_text(encoding="utf-8")

# These are the _exact_ characters as they appear in the JSON file
OLD = (
    '    "_fx_candidates = (\\n",\n'
    "    \"    list(OHLCV_DIR.glob(f'ohlcv_{TARGET_PAIR}m_D1*.parquet')) +\\n\",\n"
    "    \"    list(OHLCV_DIR.glob(f'ohlcv_{TARGET_PAIR}_D1*.parquet')) +\\n\",\n"
    "    \"    list(OHLCV_DIR.glob(f'{TARGET_PAIR}_D1*.parquet')) +\\n\",\n"
    "    \"    list(OHLCV_DIR.glob(f'{TARGET_PAIR}_D1*.csv'))\\n\",\n"
    '    ")\\n",\n'
    '    "_fx_file = sorted(_fx_candidates)[-1] if _fx_candidates else None\\n",'
)

NEW = (
    '    "    _fx_candidates = (\\n",\n'
    "    \"        list(OHLCV_DIR.glob(f'ohlcv_{TARGET_PAIR}m_D1*.parquet')) +\\n\",\n"
    "    \"        list(OHLCV_DIR.glob(f'ohlcv_{TARGET_PAIR}_D1*.parquet')) +\\n\",\n"
    "    \"        list(OHLCV_DIR.glob(f'{TARGET_PAIR}_D1*.parquet')) +\\n\",\n"
    "    \"        list(OHLCV_DIR.glob(f'{TARGET_PAIR}_D1*.csv'))\\n\",\n"
    '    "    )\\n",\n'
    '    "    _fx_file = sorted(_fx_candidates)[-1] if _fx_candidates else None\\n",'
)

count = content.count(OLD)
print(f"OLD pattern found: {count} time(s)")
if count == 1:
    content = content.replace(OLD, NEW, 1)
    NOTEBOOK.write_text(content, encoding="utf-8")
    print("✓ Indentation fixed in TARGET_PAIR fallback cell")
else:
    idx = content.find("_fx_candidates = (")
    if idx >= 0:
        print(f"Found '_fx_candidates' at index {idx}, context:")
        print(repr(content[idx - 10 : idx + 300]))
    else:
        print("No '_fx_candidates' block found — already fixed or pattern changed")
