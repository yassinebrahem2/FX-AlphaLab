"""Sources configuration schema and loader for FX-AlphaLab."""

from datetime import datetime
from pathlib import Path

import yaml
from pydantic import BaseModel, field_validator, model_validator


class SourceConfig(BaseModel):
    """Configuration for a single data source."""

    enabled: bool
    interval_hours: float
    min_silver_days: int | None
    fetch_from: str | None
    description: str

    model_config = {"frozen": True}

    @field_validator("interval_hours")
    @classmethod
    def interval_must_be_positive(cls, v: float) -> float:
        """Validate interval is positive."""
        if v <= 0:
            raise ValueError("interval_hours must be positive")
        return v

    @field_validator("min_silver_days")
    @classmethod
    def min_silver_days_must_be_non_negative(cls, v: int | None) -> int | None:
        """Validate min_silver_days is non-negative if set."""
        if v is not None and v < 0:
            raise ValueError("min_silver_days must be non-negative")
        return v

    @field_validator("fetch_from")
    @classmethod
    def fetch_from_must_be_valid_date(cls, v: str | None) -> str | None:
        """Validate fetch_from is valid ISO date if set."""
        if v is not None:
            try:
                datetime.fromisoformat(v)
            except ValueError:
                raise ValueError(f"fetch_from must be ISO date string, got {v}")
        return v


class InferenceConfig(BaseModel):
    """Configuration for inference scheduling."""

    schedule_cron: str
    dry_run: bool
    description: str = ""

    model_config = {"frozen": True}

    @field_validator("schedule_cron")
    @classmethod
    def schedule_cron_must_be_valid(cls, v: str) -> str:
        """Validate cron expression has 5 fields."""
        parts = v.split()
        if len(parts) != 5:
            raise ValueError(
                f"schedule_cron must be 5-field cron expression (min hour day month dow), got: {v}"
            )
        return v


class SourcesConfig(BaseModel):
    """Root configuration for all sources and inference."""

    sources: dict[str, SourceConfig]
    inference: InferenceConfig

    model_config = {"frozen": True}

    @model_validator(mode="after")
    def validate_no_empty_sources(self) -> "SourcesConfig":
        """Ensure at least one source is configured."""
        if not self.sources:
            raise ValueError("At least one source must be configured in sources.yaml")
        return self


def load_sources_config(path: Path) -> SourcesConfig:
    """Load and validate sources configuration from YAML file.

    Args:
        path: Path to sources.yaml file.

    Returns:
        SourcesConfig instance with all sources and inference settings.

    Raises:
        FileNotFoundError: If config file does not exist.
        ValueError: If config is invalid or missing required fields.
        yaml.YAMLError: If YAML parsing fails.
    """
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    try:
        with open(path) as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ValueError(f"Failed to parse YAML config: {e}") from e

    if data is None:
        raise ValueError("Config file is empty")

    if "sources" not in data:
        raise ValueError("Config missing required 'sources' section")

    if "inference" not in data:
        raise ValueError("Config missing required 'inference' section")

    try:
        return SourcesConfig(**data)
    except Exception as e:
        raise ValueError(f"Invalid sources config: {e}") from e
