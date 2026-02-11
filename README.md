# FX-AlphaLab

[![CI](https://github.com/yassinebrahem2/FX-AlphaLab/actions/workflows/ci.yml/badge.svg)](https://github.com/yassinebrahem2/FX-AlphaLab/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

Multi-Agent AI Framework for FX Market Analysis

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Data Sources](#data-sources)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Data Pipeline](#data-pipeline)
- [Development Workflow](#development-workflow)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)

## Overview

FX-AlphaLab is a sophisticated quantitative trading system designed for foreign exchange market analysis. The platform implements a modern data lake architecture with multi-source ingestion, intelligent preprocessing, and a multi-agent AI framework for generating actionable trading insights.

The system follows a layered data architecture:
- **Bronze Layer**: Raw data ingestion with comprehensive caching
- **Silver Layer**: Validated, normalized, and enriched data
- **Gold Layer**: Production-ready models, predictions, and signals

Built with production-grade code quality, comprehensive testing, and continuous integration to ensure reliability and maintainability.

## Features

- **Multi-Source Data Ingestion**: MT5 integration, economic calendars, FRED API, ECB exchange rates
- **Data Lake Architecture**: Bronze → Silver → Gold layered data pipeline
- **Intelligent Caching**: Rate-limited API calls with smart caching and retry mechanisms
- **Real-time Data Preprocessing**: Automated normalization, validation, and transformation
- **Multi-Agent System**: Specialized agents for macro, sentiment, and technical analysis
- **Alpha Generation**: Production-ready alpha model pipeline
- **Explainability**: Built-in model interpretation and transparency tools
- **RESTful Backend**: FastAPI-based backend with WebSocket support
- **Comprehensive Testing**: Full test coverage with pytest
- **CI/CD Pipeline**: Automated testing, linting, and type checking

## Data Sources

### Current Integrations

1. **MetaTrader 5 (MT5)** - Windows Only
   - Real-time OHLCV price data
   - Tick data and spreads
   - Direct broker connectivity
   - Scripts: [scripts/collect_mt5_data.py](scripts/collect_mt5_data.py)

2. **FRED (Federal Reserve Economic Data)**
   - Macroeconomic indicators (CPI, unemployment, interest rates)
   - Financial stress indices
   - Managed rate limiting and caching
   - Scripts: [scripts/collect_fred_data.py](scripts/collect_fred_data.py)

3. **ECB (European Central Bank)**
   - Official EUR exchange rates
   - Historical FX reference rates
   - Daily data updates
   - Scripts: [scripts/collect_ecb_data.py](scripts/collect_ecb_data.py)

4. **Economic Calendar**
   - High-impact economic events
   - Event timestamps and expectations
   - Country-specific event filtering
   - Scripts: [scripts/collect_calendar_data.py](scripts/collect_calendar_data.py)

For detailed documentation on each data source, see [docs/ingestion/](docs/ingestion/).

## Project Structure

```
fx-alphalab/
├── .github/
│   └── workflows/          # CI/CD pipelines
├── src/                    # Source code
│   ├── ingestion/          # Data ingestion pipeline
│   │   ├── collectors/     # Data collectors (MT5, FRED, ECB, Calendar)
│   │   ├── preprocessors/  # Data transformation & normalization
│   │   └── repositories/   # Data persistence layer
│   ├── agents/             # AI agent implementations
│   │   ├── macro/          # Macroeconomic analysis agents
│   │   ├── sentiment/      # Sentiment analysis agents
│   │   └── technical/      # Technical analysis agents
│   ├── alpha/              # Alpha generation models
│   ├── backend/            # Backend services
│   │   ├── routers/        # API route handlers
│   │   ├── schemas/        # Data validation schemas
│   │   └── websocket/      # Real-time communication
│   ├── explain/            # Model explainability modules
│   └── shared/             # Shared utilities
│       ├── config.py       # Configuration management
│       ├── utils.py        # Common utilities
│       ├── db/             # Database interfaces
│       └── models/         # Shared data models
├── data/                   # Data storage
│   ├── cache/              # Cached API responses
│   │   ├── calendar/       # Economic event cache
│   │   ├── ecb/            # ECB data cache
│   │   ├── fred/           # FRED macro indicators cache
│   │   ├── mt5/            # MT5 price data cache
│   │   └── news/           # News data cache
│   ├── processed/          # Processed & normalized data
│   │   ├── events/         # Economic events (silver layer)
│   │   ├── macro/          # Macro indicators (silver layer)
│   │   ├── ohlcv/          # Price data (silver layer)
│   │   └── sentiment/      # Sentiment scores
│   └── raw/                # Raw ingested data (bronze layer)
│       ├── calendar/       # Raw economic calendar
│       ├── ecb/            # Raw ECB exchange rates
│       ├── fred/           # Raw FRED data
│       ├── mt5/            # Raw MT5 data
│       └── news/           # Raw news articles
├── scripts/                # Executable scripts
│   ├── collect_*.py        # Data collection scripts
│   ├── process_*.py        # Data processing scripts
│   └── test_*.py           # Integration test scripts
├── tests/                  # Test suite
│   ├── ingestion/          # Ingestion pipeline tests
│   ├── agents/             # Agent tests
│   ├── alpha/              # Alpha model tests
│   ├── backend/            # Backend tests
│   ├── explain/            # Explainability tests
│   └── shared/             # Shared utilities tests
├── docs/                   # Documentation
│   └── ingestion/          # Data ingestion docs
├── models/                 # Trained model artifacts
│   ├── alpha/              # Alpha models
│   └── sentiment/          # Sentiment models
├── outputs/                # Generated outputs
│   ├── alpha/              # Alpha predictions
│   ├── reports/            # Analysis reports
│   └── signals/            # Trading signals
├── logs/                   # Application logs
│   ├── agents/             # Agent execution logs
│   ├── backend/            # Backend service logs
│   └── collectors/         # Data collection logs
├── config/                 # Configuration files
│   └── collectors/         # Collector configurations
├── notebooks/              # Jupyter notebooks for exploration
└── frontend/               # Frontend (placeholder)
```

### Architecture Layers

- **Bronze Layer** (`data/raw/`): Raw data as ingested from sources
- **Silver Layer** (`data/processed/`): Cleaned, normalized, validated data
- **Gold Layer** (`models/`, `outputs/`): ML models and production-ready outputs

## Getting Started

### Prerequisites

- **Python 3.10+** (required)
- **PostgreSQL** (for database functionality)
- **MetaTrader5** (Windows only, optional - for MT5 data)
- **Git**

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yassinebrahem2/FX-AlphaLab.git
   cd FX-AlphaLab
   ```

2. **Create and activate virtual environment**
   ```bash
   # Windows
   python -m venv .venv
   .venv\Scripts\activate

   # Linux/Mac
   python -m venv .venv
   source .venv/bin/activate
   ```

3. **Install the package in editable mode**
   ```bash
   # Install with development dependencies
   pip install -e ".[dev]"

   # For notebook development, also install:
   pip install -e ".[notebook]"

   # On Windows, for MT5 integration:
   pip install -e ".[mt5]"
   ```

4. **Set up environment variables**
   ```bash
   # Copy the example env file
   cp .env.example .env

   # Edit .env with your credentials
   # Add your API keys and database credentials
   ```

5. **Install pre-commit hooks**
   ```bash
   pre-commit install
   ```

   This ensures code quality checks run automatically before each commit.

### Configuration

Edit the `.env` file with your settings:

```env
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=fx_alphalab
DB_USER=postgres
DB_PASSWORD=your_password

# API Keys
FRED_API_KEY=your_fred_api_key

# MT5 (Windows only)
MT5_LOGIN=your_login
MT5_PASSWORD=your_password
MT5_SERVER=your_broker_server

# Data Collection
SCRAPING_DELAY=3.0
REQUEST_TIMEOUT=30
```

## Data Pipeline

### Collection Scripts

The system includes automated data collection scripts in the [scripts/](scripts/) directory:

```bash
# Collect FRED macroeconomic data (CPI, unemployment, interest rates)
python scripts/collect_fred_data.py

# Collect ECB exchange rates
python scripts/collect_ecb_data.py

# Collect economic calendar events
python scripts/collect_calendar_data.py

# Collect MT5 price data (Windows only)
python scripts/collect_mt5_data.py

# Process calendar data to silver layer
python scripts/process_calendar_silver.py
```

### Data Flow

1. **Bronze Layer (Raw)**: Scripts collect data from external sources and save to `data/raw/`
   - Raw API responses cached to `data/cache/` for efficiency
   - Rate limiting and retry logic prevent API throttling
   - Original data format preserved

2. **Silver Layer (Processed)**: Data is cleaned, validated, and normalized
   - [src/ingestion/preprocessors/](src/ingestion/preprocessors/) handle transformations
   - Standardized timestamps and data types
   - Quality checks and validation
   - Output saved to `data/processed/`

3. **Gold Layer (Production)**: Ready for model consumption
   - Feature engineering and aggregations
   - Model predictions saved to `outputs/`
   - Trained models persisted to `models/`

### Storage Structure

- **Cache**: Immutable API responses with timestamps
- **Raw**: Original ingested data (bronze)
- **Processed**: Cleaned and validated data (silver)
- **Models**: Trained ML artifacts
- **Outputs**: Predictions, signals, and reports

## Development Workflow

### Branching Strategy

This project follows a professional Git workflow:
- **main**: Stable production-ready code (protected)
- **dev**: Active development and integration branch
- **feature/**: Feature branches created from `dev`

### Before You Start Coding

1. **Ensure you're on the latest dev branch**
   ```bash
   git checkout dev
   git pull origin dev
   ```

2. **Create a new branch from dev**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**

3. **Run quality checks locally** (same as CI)
   ```bash
   # Format code
   black .

   # Check linting
   ruff check .

   # Fix auto-fixable issues
   ruff check . --fix

   # Type checking
   mypy src/

   # Run tests
   pytest tests/ --cov=. --cov-report=term
   ```

4. **Or use pre-commit to run everything**
   ```bash
   pre-commit run --all-files
   ```

### Making a Commit

When you commit, pre-commit hooks automatically run:
- Code formatting (black)
- Linting (ruff)
- Type checking (mypy)
- Trailing whitespace removal
- YAML/JSON validation

```bash
git add .
git commit -m "feat: add new feature"  # Hooks run automatically
git push origin feature/your-feature-name
```

### Pull Request Process

1. Push your branch to GitHub
2. Open a Pull Request **targeting the `dev` branch**
3. CI pipeline runs automatically (must pass)
4. Request review from team members
5. Address feedback
6. Merge to `dev` when approved

**Note**: Only maintainers merge `dev` → `main` for releases.

## Testing

### Test Structure

Tests are organized to mirror the source code structure:

```
tests/
├── ingestion/          # Tests for data collectors and preprocessors
├── agents/             # Tests for AI agents
├── alpha/              # Tests for alpha models
├── backend/            # Tests for API and backend services
├── explain/            # Tests for explainability modules
└── shared/             # Tests for shared utilities
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov=. --cov-report=term-missing

# Run specific test file
pytest tests/test_config.py

# Run specific test function
pytest tests/test_config.py::test_config_paths_exist

# Run with verbose output
pytest -v
```

### Writing Tests

- Place tests in `tests/` directory
- Name test files as `test_*.py`
- Name test functions as `test_*`
- Use descriptive names
- Follow existing test patterns

Example:
```python
def test_feature_behavior():
    """Test that feature works as expected."""
    result = your_function()
    assert result == expected_value
```

## Code Quality Standards

This project enforces strict code quality:

- **Black**: Code formatting (line length: 100)
- **Ruff**: Fast linting (replaces flake8, isort, etc.)
- **Mypy**: Static type checking
- **Pytest**: Testing with coverage requirements
- **Pre-commit**: Automated checks before commit

### Type Hints

Always use modern type hints (Python 3.10+):

```python
# Good
def process_data(value: str | None) -> dict[str, int]:
    ...

# Bad (deprecated)
from typing import Optional, Dict
def process_data(value: Optional[str]) -> Dict[str, int]:
    ...
```

## Contributing

### Workflow Summary

1. **Fork** the repository (external contributors)
2. **Clone** your fork / the repo
3. **Switch to dev** (`git checkout dev && git pull origin dev`)
4. **Create a branch from dev** (`git checkout -b feature/amazing-feature`)
5. **Make changes** with tests
6. **Run quality checks** locally
7. **Commit** (`git commit -m 'feat: add amazing feature'`)
8. **Push** (`git push origin feature/amazing-feature`)
9. **Open a Pull Request to `dev` branch**

### Commit Message Convention

Follow [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation changes
- `test:` Adding/updating tests
- `refactor:` Code refactoring
- `chore:` Maintenance tasks
- `ci:` CI/CD changes

Examples:
```
feat: add FRED API data ingestion
fix: resolve timezone issue in utils
docs: update README with setup instructions
test: add tests for config validation
```

### Review Guidelines

- Code must pass all CI checks
- Maintain or improve test coverage
- Update documentation as needed
- Follow existing code style
- Keep PRs focused and reasonably sized

## Useful Commands

### Development

```bash
# Install dependencies
pip install -e ".[dev,notebook,mt5]"

# Run pre-commit on all files
pre-commit run --all-files

# Update pre-commit hooks
pre-commit autoupdate

# Format code
black .

# Lint code
ruff check .

# Type check
mypy src/

# Run tests with coverage
pytest --cov=. --cov-report=html
# Open htmlcov/index.html to view detailed coverage

# Clean up cache files
find . -type d -name "__pycache__" -exec rm -rf {} +
find . -type d -name ".pytest_cache" -exec rm -rf {} +
find . -type d -name ".ruff_cache" -exec rm -rf {} +
```

### Data Collection

```bash
# Collect all data sources
python scripts/collect_fred_data.py
python scripts/collect_ecb_data.py
python scripts/collect_calendar_data.py
python scripts/collect_mt5_data.py    # Windows only

# Process data to silver layer
python scripts/process_calendar_silver.py

# Test collectors
python scripts/test_collectors.py
```

## Resources

- **Documentation**: See `docs/` directory
- **Issues**: [GitHub Issues](https://github.com/yassinebrahem2/FX-AlphaLab/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yassinebrahem2/FX-AlphaLab/discussions)
- **CI/CD**: [GitHub Actions](https://github.com/yassinebrahem2/FX-AlphaLab/actions)

## Troubleshooting

### Common Issues

**Pre-commit hooks failing**
```bash
# Update hooks
pre-commit autoupdate
pre-commit install --install-hooks
```

**Import errors**
```bash
# Reinstall in editable mode
pip install -e ".[dev]"
```

**Database connection issues**
- Verify PostgreSQL is running
- Check `.env` credentials
- Ensure database exists

**MT5 installation (Windows only)**
```bash
pip install -e ".[mt5]"
```

## License

This project is proprietary and confidential. All rights reserved.

---

**Maintainers**: FX-AlphaLab Team
**Questions?** Open an issue or reach out to the team.
