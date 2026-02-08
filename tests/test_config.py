"""Tests for configuration module."""
import os
from pathlib import Path

import pytest

from shared.config import Config


def test_config_paths_exist():
    """Test that config paths are properly initialized."""
    assert isinstance(Config.ROOT_DIR, Path)
    assert isinstance(Config.DATA_DIR, Path)
    assert isinstance(Config.LOGS_DIR, Path)


def test_config_default_values():
    """Test default configuration values."""
    assert Config.DB_HOST == os.getenv("DB_HOST", "localhost")
    assert Config.DB_PORT == int(os.getenv("DB_PORT", "5432"))
    assert Config.DB_NAME == os.getenv("DB_NAME", "fx_alphalab")
    assert Config.SCRAPING_DELAY == float(os.getenv("SCRAPING_DELAY", "3.0"))
    assert Config.REQUEST_TIMEOUT == int(os.getenv("REQUEST_TIMEOUT", "30"))


def test_config_database_url():
    """Test database URL construction."""
    config = Config()
    db_url = config.database_url
    assert "postgresql://" in db_url
    assert Config.DB_HOST in db_url
    assert str(Config.DB_PORT) in db_url
    assert Config.DB_NAME in db_url


def test_config_validation_missing_fred_key():
    """Test configuration validation fails without FRED API key."""
    original_key = os.environ.get("FRED_API_KEY")
    
    try:
        if "FRED_API_KEY" in os.environ:
            del os.environ["FRED_API_KEY"]
        Config.FRED_API_KEY = None
        
        with pytest.raises(ValueError, match="FRED_API_KEY not set"):
            Config.validate()
    finally:
        if original_key:
            os.environ["FRED_API_KEY"] = original_key
