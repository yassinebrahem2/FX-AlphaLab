# FX-AlphaLab

[![CI](https://github.com/yassinebrahem2/FX-AlphaLab/actions/workflows/ci.yml/badge.svg)](https://github.com/yassinebrahem2/FX-AlphaLab/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

Multi-Agent AI Framework for FX Market Analysis

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)

## Overview

FX-AlphaLab is a sophisticated multi-agent AI framework designed for foreign exchange market analysis. The platform combines real-time data ingestion, advanced preprocessing, and intelligent agent orchestration to provide comprehensive market insights.

## Features

- **Multi-Source Data Ingestion**: MT5 integration, economic calendars, FRED API, ECB data
- **Real-time Data Synchronization**: Automated data preprocessing and normalization
- **AI-Powered Agents**: Specialized agents for different aspects of FX analysis
- **Scalable Backend**: Built with modern Python best practices
- **Comprehensive Testing**: Full test coverage with pytest
- **CI/CD Pipeline**: Automated testing, linting, and type checking

## Project Structure

```
fx-alphalab/
├── .github/
│   └── workflows/          # CI/CD pipelines
├── data/
│   ├── ingestion/          # Data collection modules
│   ├── preprocessing/      # Data transformation
│   └── storage/            # Database interfaces
├── agents/                 # AI agent implementations
├── backend/                # Backend services
├── frontend/               # Frontend (placeholder)
├── shared/
│   ├── config.py           # Configuration management
│   └── utils.py            # Shared utilities
├── tests/                  # Test suite
├── notebooks/              # Jupyter notebooks for exploration
├── docs/                   # Documentation
└── datasets/               # Sample/output datasets
```

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
   mypy shared/ data/ agents/

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
mypy shared/ data/ agents/

# Run tests with coverage
pytest --cov=. --cov-report=html
# Open htmlcov/index.html to view detailed coverage

# Clean up cache files
find . -type d -name "__pycache__" -exec rm -rf {} +
find . -type d -name ".pytest_cache" -exec rm -rf {} +
find . -type d -name ".ruff_cache" -exec rm -rf {} +
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
