"""Tests for sources configuration loading and validation."""

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
import yaml

from src.shared.config.sources import (
    InferenceConfig,
    SourceConfig,
    SourcesConfig,
    load_sources_config,
)


class TestSourceConfigValidation:
    """Test SourceConfig validation."""

    def test_valid_source_config(self):
        """Test creation of valid SourceConfig."""
        config = SourceConfig(
            enabled=True,
            interval_hours=24,
            min_silver_days=400,
            fetch_from=None,
            description="Test source",
        )
        assert config.enabled is True
        assert config.interval_hours == 24
        assert config.min_silver_days == 400

    def test_invalid_interval_negative(self):
        """Test that negative interval raises ValueError."""
        with pytest.raises(ValueError, match="interval_hours must be positive"):
            SourceConfig(
                enabled=True,
                interval_hours=-1,
                min_silver_days=400,
                fetch_from=None,
                description="Test",
            )

    def test_invalid_interval_zero(self):
        """Test that zero interval raises ValueError."""
        with pytest.raises(ValueError, match="interval_hours must be positive"):
            SourceConfig(
                enabled=True,
                interval_hours=0,
                min_silver_days=400,
                fetch_from=None,
                description="Test",
            )

    def test_invalid_min_silver_days_negative(self):
        """Test that negative min_silver_days raises ValueError."""
        with pytest.raises(ValueError, match="min_silver_days must be non-negative"):
            SourceConfig(
                enabled=True,
                interval_hours=24,
                min_silver_days=-1,
                fetch_from=None,
                description="Test",
            )

    def test_invalid_fetch_from_date(self):
        """Test that invalid date format in fetch_from raises ValueError."""
        with pytest.raises(ValueError, match="fetch_from must be ISO date string"):
            SourceConfig(
                enabled=True,
                interval_hours=24,
                min_silver_days=400,
                fetch_from="invalid-date",
                description="Test",
            )

    def test_valid_fetch_from_date(self):
        """Test that valid ISO date in fetch_from is accepted."""
        config = SourceConfig(
            enabled=True,
            interval_hours=24,
            min_silver_days=None,
            fetch_from="2021-01-01",
            description="Test",
        )
        assert config.fetch_from == "2021-01-01"

    def test_frozen_model(self):
        """Test that SourceConfig is immutable (frozen)."""
        config = SourceConfig(
            enabled=True,
            interval_hours=24,
            min_silver_days=400,
            fetch_from=None,
            description="Test",
        )
        with pytest.raises(Exception):  # FrozenInstanceError
            config.enabled = False


class TestInferenceConfigValidation:
    """Test InferenceConfig validation."""

    def test_valid_inference_config(self):
        """Test creation of valid InferenceConfig."""
        config = InferenceConfig(
            schedule_cron="30 22 * * *",
            dry_run=False,
            description="Daily inference at 22:30 UTC",
        )
        assert config.schedule_cron == "30 22 * * *"
        assert config.dry_run is False

    def test_invalid_cron_expression_too_short(self):
        """Test that cron expression with too few fields raises ValueError."""
        with pytest.raises(ValueError, match="must be 5-field cron expression"):
            InferenceConfig(
                schedule_cron="30 22 * *",
                dry_run=False,
            )

    def test_invalid_cron_expression_too_long(self):
        """Test that cron expression with too many fields raises ValueError."""
        with pytest.raises(ValueError, match="must be 5-field cron expression"):
            InferenceConfig(
                schedule_cron="30 22 * * * UTC",
                dry_run=False,
            )


class TestSourcesConfigValidation:
    """Test SourcesConfig validation."""

    def test_valid_sources_config(self):
        """Test creation of valid SourcesConfig."""
        config = SourcesConfig(
            sources={
                "test_source": SourceConfig(
                    enabled=True,
                    interval_hours=24,
                    min_silver_days=100,
                    fetch_from=None,
                    description="Test",
                )
            },
            inference=InferenceConfig(
                schedule_cron="30 22 * * *",
                dry_run=False,
            ),
        )
        assert "test_source" in config.sources
        assert config.inference.schedule_cron == "30 22 * * *"

    def test_empty_sources_raises_error(self):
        """Test that empty sources dict raises ValueError."""
        with pytest.raises(ValueError, match="At least one source must be configured"):
            SourcesConfig(
                sources={},
                inference=InferenceConfig(
                    schedule_cron="30 22 * * *",
                    dry_run=False,
                ),
            )


class TestLoadSourcesConfig:
    """Test load_sources_config function."""

    def test_load_valid_config(self):
        """Test loading a valid sources.yaml file."""
        with TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "sources.yaml"
            config_data = {
                "sources": {
                    "fred_macro": {
                        "enabled": True,
                        "interval_hours": 24,
                        "min_silver_days": 730,
                        "fetch_from": None,
                        "description": "FRED macro data",
                    }
                },
                "inference": {
                    "schedule_cron": "30 22 * * *",
                    "dry_run": False,
                    "description": "Daily inference",
                },
            }
            with open(config_path, "w") as f:
                yaml.dump(config_data, f)

            config = load_sources_config(config_path)
            assert "fred_macro" in config.sources
            assert config.sources["fred_macro"].interval_hours == 24
            assert config.inference.schedule_cron == "30 22 * * *"

    def test_load_nonexistent_file(self):
        """Test that loading nonexistent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="Config file not found"):
            load_sources_config(Path("/nonexistent/path/sources.yaml"))

    def test_load_empty_file(self):
        """Test that loading empty YAML raises ValueError."""
        with TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "sources.yaml"
            with open(config_path, "w") as f:
                f.write("")

            with pytest.raises(ValueError, match="Config file is empty"):
                load_sources_config(config_path)

    def test_load_missing_sources_section(self):
        """Test that missing 'sources' section raises ValueError."""
        with TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "sources.yaml"
            config_data = {
                "inference": {
                    "schedule_cron": "30 22 * * *",
                    "dry_run": False,
                }
            }
            with open(config_path, "w") as f:
                yaml.dump(config_data, f)

            with pytest.raises(ValueError, match="missing required 'sources' section"):
                load_sources_config(config_path)

    def test_load_missing_inference_section(self):
        """Test that missing 'inference' section raises ValueError."""
        with TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "sources.yaml"
            config_data = {
                "sources": {
                    "test_source": {
                        "enabled": True,
                        "interval_hours": 24,
                        "min_silver_days": 100,
                        "fetch_from": None,
                        "description": "Test",
                    }
                }
            }
            with open(config_path, "w") as f:
                yaml.dump(config_data, f)

            with pytest.raises(ValueError, match="missing required 'inference' section"):
                load_sources_config(config_path)

    def test_load_invalid_yaml(self):
        """Test that invalid YAML syntax raises ValueError."""
        with TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "sources.yaml"
            with open(config_path, "w") as f:
                f.write("{ invalid yaml: [unclosed")

            with pytest.raises(ValueError, match="Failed to parse YAML config"):
                load_sources_config(config_path)

    def test_load_invalid_source_config(self):
        """Test that invalid source config raises ValueError."""
        with TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "sources.yaml"
            config_data = {
                "sources": {
                    "bad_source": {
                        "enabled": True,
                        "interval_hours": -1,  # Invalid: must be positive
                        "min_silver_days": 100,
                        "fetch_from": None,
                        "description": "Bad",
                    }
                },
                "inference": {
                    "schedule_cron": "30 22 * * *",
                    "dry_run": False,
                },
            }
            with open(config_path, "w") as f:
                yaml.dump(config_data, f)

            with pytest.raises(ValueError, match="Invalid sources config"):
                load_sources_config(config_path)
