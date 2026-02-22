"""Fix notebook: remove duplicate VADER cells and lint violations."""

import json
import re

path = r"c:\Users\Moataz\FX-AlphaLab\notebooks\04d_reddit_sentiment.ipynb"
with open(path, encoding="utf-8") as f:
    nb = json.load(f)

original_count = len(nb["cells"])
# Keep only first 38 cells (FinBERT version) — drop duplicate VADER half
nb["cells"] = nb["cells"][:38]
print(f"Cells: {original_count} → {len(nb['cells'])} (removed duplicate VADER half)")


def get_src(cell: dict) -> str:
    return "".join(cell.get("source", []))


def set_src(cell: dict, src: str) -> None:
    lines = src.split("\n")
    cell["source"] = [line + "\n" for line in lines]
    if cell["source"]:
        cell["source"][-1] = cell["source"][-1].rstrip("\n")


# Fix 1: Cell index 1 (cell 2) — move torch/transformers imports to top (E402)
cell2 = nb["cells"][1]
src = get_src(cell2)
# Remove the FinBERT comment + imports from their current mid-cell position
src = re.sub(
    r"\n# FinBERT for financial-domain sentiment analysis \(matches project NewsPreprocessor\)\nimport torch\nfrom transformers import pipeline as hf_pipeline\n",
    "\n",
    src,
)
# Insert torch/transformers right after the seaborn import (before warnings.filterwarnings)
src = src.replace(
    "import seaborn as sns\n\nwarnings.filterwarnings",
    "import seaborn as sns\nimport torch\nfrom transformers import pipeline as hf_pipeline\n\n"
    "# FinBERT for financial-domain sentiment analysis (matches project NewsPreprocessor)\n"
    "warnings.filterwarnings",
)
set_src(cell2, src)
print("Fixed E402: torch/transformers imports moved to top of cell 2")

# Fix 2: Find day-of-week cell — move Patch import to top (E402)
for i, cell in enumerate(nb["cells"]):
    src = get_src(cell)
    if "Day-of-week and hour patterns" in src and "from matplotlib.patches import Patch" in src:
        # Remove inline import after the comment
        src = src.replace(
            "\n# Add session legend\nfrom matplotlib.patches import Patch\n",
            "\n# Add session legend\n",
        )
        # Prepend at top of cell
        src = "from matplotlib.patches import Patch\n" + src
        set_src(cell, src)
        print(f"Fixed E402: Patch import moved to top of cell {i + 1}")

# Fix 3: Find sentiment distribution cell — rename l → lbl (E741)
for i, cell in enumerate(nb["cells"]):
    src = get_src(cell)
    if "colors_pie.get(l," in src:
        src = src.replace(
            "colors_pie.get(l, '#9E9E9E') for l in label_counts.index",
            "colors_pie.get(lbl, '#9E9E9E') for lbl in label_counts.index",
        )
        set_src(cell, src)
        print(f"Fixed E741: renamed l → lbl in cell {i + 1}")

with open(path, "w", encoding="utf-8") as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)
print("Notebook saved.")
