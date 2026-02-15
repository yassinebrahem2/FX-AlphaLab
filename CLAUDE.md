# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FX-AlphaLab is a multi-agent AI framework for FX market analysis. The project is in **W4 (Data Acquisition & Understanding phase)** following the CRISP-DM methodology with a planned **Week 5 architecture refactoring** to migrate from current structure to production-grade `src/` structure.

**Current Status**:
- Branch: `dev` (all PRs target here, never `main` directly)
- Phase: W3-W6 Data Acquisition & Understanding
- Collectors implemented:
  - Tabular: MT5, FRED, ECB, ForexFactory Calendar
  - Document: Fed, ECB News, GDELT, BoE
- Next milestone: W6 presentation + report + notebooks + datasets

## Core Principles

**CRITICAL - Read this first:**

1. **Be lean and concise**
   - NO verbose explanations unless explicitly asked.
   - Show code changes, not essays about them.

2. **Production-quality code only**
   - NO workarounds, hacks, or compatibility shims.
   - Strict separation of concerns. Never mix presentation, logic, or data access.
   - Follow existing patterns in the codebase. Read before writing.
   - No backward compatibility. Break cleanly and fix all callers.

## Architecture Plan

**Critical**: The project has an evolving architecture plan at `C:\Users\yassi\.claude\plans\lovely-giggling-wave.md` that defines:
- Medallion Architecture (Bronze → Silver → Gold)
- Data contracts and schemas
- Week 5 refactoring plan (current `data/ingestion/` → future `src/ingestion/`)
- Complete file structure and module organization
- Milestone deliverables aligned with supervisor's roadmap

**Always consult the plan before making architectural decisions.**

## Current vs Future Structure

**CURRENT (W4)**: Code lives in src/ structure
```
src/ingestion/          # Data collectors (MT5, FRED, ECB, Calendar)
src/shared/             # Config, utils, logging
src/agents/             # Placeholder for future agents
backend/                # Placeholder for future API
tests/                  # Test suite
```

**FUTURE (W5+)**: Production-grade structure
```
src/                     # All application code
  ├── shared/           # Cross-cutting (config, models, db, utils)
  ├── ingestion/        # Collectors, preprocessors, repositories
  ├── agents/           # Technical, macro, sentiment agents (W7+)
  ├── alpha/            # Signal aggregation (W9+)
  ├── explain/          # LLM explanations (W12+)
  └── backend/          # FastAPI routers (W12+)
data/                    # Data files only (Bronze/Silver/Gold)
outputs/                 # Agent outputs, reports
```

**Import Pattern Changes**:
- Current: `from src.ingestion.collectors.fred_collector import FREDCollector`
- Future: `from src.ingestion.collectors.fred_collector import FREDCollector`

## Data Architecture (Medallion Pattern)

### Bronze (Raw) - data/raw/
Immutable source data preserving all fields:
- **Location**: `data/raw/{source}/`
- **Format**: `{source}_{dataset}_{YYYYMMDD}.csv`
- **Contract**: Add `source` column, snake_case, UTF-8, preserve all fields
- **Example**: `data/raw/mt5/EURUSD_H1_20260210.csv`

### Silver (Processed) - data/processed/
Normalized, validated data with standardized schemas:
- **OHLCV**: `data/processed/ohlcv/ohlcv_{PAIR}_{TIMEFRAME}_{START}_{END}.parquet`
  - Schema: `[timestamp_utc, pair, timeframe, open, high, low, close, volume, source]`
- **Macro**: `data/processed/macro/macro_{SERIES_ID}_{START}_{END}.csv`
  - Schema: `[timestamp_utc, series_id, value, source, frequency, units]`
- **Events**: `data/processed/events/events_{START}_{END}.csv`
  - Schema: `[timestamp_utc, event_id, country, event_name, impact, actual, forecast, previous, source]`
- **Sentiment** (Partitioned Parquet): `data/processed/sentiment/source={SOURCE}/year={YYYY}/month={MM}/sentiment_cleaned.parquet`
  - Schema: `[timestamp_utc, article_id, pair, headline, sentiment_score, sentiment_label, document_type, speaker, source, url]`
  - Partitioned by source (fed, ecb, boe, gdelt), year, and month for efficient querying

### Gold (Outputs) - outputs/
Business-ready analysis results (W7+):
- `outputs/signals/` - Agent signals (W7+)
- `outputs/alpha/` - Alpha recommendations (W9+)
- `outputs/reports/` - LLM-generated explanations (W12+)

## Collector Patterns

The project uses two collector types based on data characteristics:

### BaseCollector (Tabular Data)

For numeric/structured data sources (FRED, ECB, MT5, economic calendars):

```python
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
import pandas as pd

class BaseCollector(ABC):
    SOURCE_NAME: str  # e.g. "fred", "ecb", "mt5"

    def __init__(self, output_dir: Path, log_file: Path | None = None):
        self.output_dir = output_dir
        self.logger = setup_logger(self.__class__.__name__, log_file)

    @abstractmethod
    def collect(self, start_date: datetime | None, end_date: datetime | None) -> dict[str, pd.DataFrame]:
        """Fetch data and return {dataset_name: DataFrame}."""
        pass

    @abstractmethod
    def health_check(self) -> bool:
        """Verify source is reachable."""
        pass

    def export_csv(self, df: pd.DataFrame, dataset_name: str) -> Path:
        """Export with §3.1 naming: {SOURCE_NAME}_{dataset_name}_{YYYYMMDD}.csv"""
        pass
```

**When implementing tabular collectors**:
1. Inherit from `BaseCollector`
2. Set `SOURCE_NAME` class attribute
3. Implement `collect()` returning dict of DataFrames
4. Implement `health_check()` for connectivity verification
5. Use `export_csv()` for consistent file naming
6. Add comprehensive tests (see existing test_*_collector.py files)

## Development Commands

### Setup
```bash
# Clone and setup
git clone https://github.com/yassinebrahem2/FX-AlphaLab.git
cd FX-AlphaLab
python -m venv .venv
.venv\Scripts\activate  # Windows
pip install -e ".[dev]"
pre-commit install

# Copy and configure environment
cp .env.example .env
# Edit .env with API keys (FRED_API_KEY, DB credentials, MT5 settings)
```

### Testing
```bash
# Run all tests with coverage
pytest

# Run specific test file
pytest tests/test_fred_collector.py

# Run specific test function
pytest tests/test_fred_collector.py::test_fred_collector_initialization

# Run with verbose output and coverage report
pytest -v --cov=. --cov-report=term-missing

# Coverage must be >60% for src/ingestion/
```

### Code Quality
```bash
# Format code (automatic via pre-commit)
black .

# Lint (automatic via pre-commit)
ruff check .
ruff check . --fix  # Auto-fix issues

# Type checking
mypy shared/ data/ agents/

# Run all quality checks
pre-commit run --all-files
```

### Git Workflow
```bash
# Always work from dev branch
git checkout dev
git pull origin dev

# Create feature branch
git checkout -b feature/your-feature-name

# Make changes, then commit (pre-commit runs automatically)
git add .
git commit -m "feat: add new collector"

# Push and create PR targeting dev
git push origin feature/your-feature-name
```

## Testing Patterns

### Collector Tests
All collector tests follow similar patterns (see tests/test_*_collector.py):

```python
import pytest
from unittest.mock import Mock, patch
from src.ingestion.collectors.your_collector import YourCollector

class TestYourCollector:
    @pytest.fixture
    def collector(self, tmp_path):
        """Fixture provides collector with temp output dir."""
        return YourCollector(output_dir=tmp_path)

    def test_initialization(self, collector):
        """Test collector initializes correctly."""
        assert collector.SOURCE_NAME == "your_source"
        assert collector.output_dir.exists()

    @patch('src.ingestion.collectors.your_collector.external_api_call')
    def test_collect(self, mock_api, collector):
        """Test data collection with mocked API."""
        mock_api.return_value = mock_data
        result = collector.collect(start_date=..., end_date=...)
        assert "dataset_name" in result
        assert isinstance(result["dataset_name"], pd.DataFrame)

    def test_health_check(self, collector):
        """Test connectivity check."""
        assert collector.health_check() is True

    def test_export_csv(self, collector, tmp_path):
        """Test CSV export with correct naming."""
        df = pd.DataFrame({"col": [1, 2, 3]})
        path = collector.export_csv(df, "test_dataset")
        assert path.exists()
        assert "your_source_test_dataset" in path.name
```

### Mock External APIs
Always mock external API calls in tests to avoid:
- Network dependencies
- Rate limits
- API costs
- CI failures

Use `@patch` decorator or `unittest.mock.Mock` objects.

## Configuration Management

Config lives in `src/shared/config.py`:

```python
from src.shared.config import Config

# Access paths
Config.ROOT_DIR          # Project root
Config.DATA_DIR          # data/ directory
Config.LOGS_DIR          # logs/ directory

# Access credentials
Config.FRED_API_KEY      # FRED API key
Config.DB_HOST           # PostgreSQL host
Config.database_url      # Full database connection string

# Validate config
Config.validate()        # Raises ValueError if required keys missing
```

**Environment Variables** (in .env):
- `FRED_API_KEY` - Required for FRED collector
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` - Database connection
- `MT5_LOGIN`, `MT5_PASSWORD`, `MT5_SERVER` - MT5 integration (Windows only)
- `SCRAPING_DELAY`, `REQUEST_TIMEOUT` - Collection settings

## Type Hints (Python 3.10+)

**Always use modern union syntax**:
```python
# ✅ Good
def func(value: str | None) -> dict[str, int]:
    pass

# ❌ Bad (deprecated)
from typing import Optional, Dict
def func(value: Optional[str]) -> Dict[str, int]:
    pass
```

## Code Style Standards

- **Line length**: 100 characters (black and ruff configured)
- **Import order**: Automatic via ruff (stdlib, third-party, local)
- **Docstrings**: Use for all public functions/classes
- **Type hints**: Required for function signatures
- **Pre-commit hooks**: Run automatically on commit (black, ruff, mypy, trailing whitespace, yaml/json validation)

## Commit Message Convention

Follow [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation only
- `test:` - Adding/updating tests
- `refactor:` - Code refactoring
- `chore:` - Maintenance tasks
- `ci:` - CI/CD changes

Examples:
```
feat: add ECB exchange rate collector
fix: resolve timezone handling in FRED collector
test: add comprehensive tests for calendar scraper
docs: update README with setup instructions
refactor: extract common retry logic to BaseCollector
```

## Project Phases & Deliverables

Consult the architecture plan for detailed week-by-week breakdown. Key phases:

- **W3-W6 (Current)**: Data Acquisition & Understanding
  - Deliverable: Presentation + Report + Notebooks + Datasets
  - Infrastructure: Collectors, preprocessors, PostgreSQL, Bronze→Silver pipeline

- **W6-W11**: Modeling & Business Accepting
  - Deliverable: Presentation + Report + Notebooks + Signal Outputs
  - Infrastructure: Technical/macro/sentiment agents, alpha generator, backtester

- **W11-W14**: Deployment
  - Deliverable: Presentation + Live Demo + Report
  - Infrastructure: FastAPI backend, React frontend, ChromaDB, LLM integration

- **W15**: Final Evaluation

## Important Notes

1. **Never commit to `main` directly** - Always PR to `dev`
2. **Week 5 migration pending** - Code will move to `src/` structure
3. **All data gitignored** - Only .gitkeep files tracked in data/
4. **Windows-only dependency** - MT5 collector requires Windows + MetaTrader5
5. **Coverage requirements** - Maintain >60% for ingestion modules
6. **Pre-commit mandatory** - Cannot commit without passing hooks
7. **Bronze layer is immutable** - Never modify raw data after collection
8. **Consult architecture plan** - Always check C:\Users\yassi\.claude\plans\lovely-giggling-wave.md before architectural decisions

## Resources

- **Architecture Plan**: `C:\Users\yassi\.claude\plans\lovely-giggling-wave.md`
- **Supervisor's Roadmap**: `.claude/615071072_900434176065622_8366952998458839016_n.jpeg`
- **Documentation**: `docs/` directory (ingestion docs exist for MT5, FRED, ECB, Calendar)
- **CI Pipeline**: `.github/workflows/ci.yml`
- **Issues**: GitHub Issues
- **Coverage Reports**: `htmlcov/index.html` after running pytest with `--cov-report=html`
