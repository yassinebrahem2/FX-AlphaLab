"""Fix cell 57 in 03macroanalysis2.1.ipynb."""

import json
from pathlib import Path

NB_PATH = Path(__file__).parent.parent / "notebooks" / "03macroanalysis2.1.ipynb"

with open(NB_PATH, encoding="utf-8") as f:
    nb = json.load(f)

for cell in nb["cells"]:
    if cell.get("id") != "5fe9c83f":
        continue

    lines = cell["source"]  # list of strings, each ends with \n except possibly the last

    # ── locate the IMPORT_ERRORS block start / end ────────────────────────────
    block_start = None
    block_end = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped == "if IMPORT_ERRORS:" and block_start is None:
            block_start = i
        if block_start is not None and i > block_start:
            # block ends when we hit a non-indented line or blank line followed by non-blank
            if not line.startswith(" ") and line.strip() != "":
                block_end = i
                break

    if block_start is None:
        print("ERROR: could not find 'if IMPORT_ERRORS:' in source lines.")
        raise SystemExit(1)

    print(f"IMPORT_ERRORS block: lines {block_start}–{block_end}")
    print("Lines to remove:")
    for l in lines[block_start:block_end]:
        print("  ", repr(l))

    # Remove blank line(s) just before if IMPORT_ERRORS: (between the loop and the if)
    # Find any trailing blank line right before block_start
    while block_start > 0 and lines[block_start - 1].strip() == "":
        block_start -= 1

    # Locate the 'if not ohlcv_data:' line index (block_end is pointing there)
    if_not_ohlcv_idx = block_end

    # ── Build new lines: remove the IMPORT_ERRORS block, inject info inside if not ohlcv_data ──
    new_lines = lines[:block_start]  # everything before the block (keep indentation)

    # Now inject the informational message inside if not ohlcv_data:
    # Find the FRED fallback comment after if not ohlcv_data:
    inject_after = None
    for j in range(if_not_ohlcv_idx, len(lines)):
        if "FRED fallback: build synthetic" in lines[j]:
            inject_after = j
            break

    if inject_after is None:
        print("ERROR: could not find FRED fallback comment.")
        raise SystemExit(1)

    # Lines from if_not_ohlcv_idx up to and including inject_after
    new_lines += lines[if_not_ohlcv_idx : inject_after + 1]

    # Inject info block
    new_lines += [
        "    if IMPORT_ERRORS:\n",
        '        print(f"\\n  \u2139\ufe0f  MT5 Silver files not found for: {IMPORT_ERRORS}")\n',
        '        print("     Using FRED spot-rate fallback. To collect MT5 data run:")\n',
        '        print(f"     python scripts/collect_mt5_data.py "\n',
        "              f\"--pairs {','.join(IMPORT_ERRORS)} --timeframes D1 --preprocess\\n\")\n",
    ]

    # Rest of the lines after inject_after
    new_lines += lines[inject_after + 1 :]

    cell["source"] = new_lines
    cell["outputs"] = []
    cell["execution_count"] = None
    print("Cell 57 patched. Stale outputs cleared.")
    break
else:
    print("ERROR: cell 5fe9c83f not found.")
    raise SystemExit(1)

with open(NB_PATH, "w", encoding="utf-8") as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)

print("Notebook saved.")
