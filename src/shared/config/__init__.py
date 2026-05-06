"""Configuration management for FX-AlphaLab."""

import os
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def _load_groq_api_keys() -> list[str]:
    keys: list[str] = []
    idx = 1
    while True:
        env_key = f"GROQ_API_KEY_{idx}"
        value = os.getenv(env_key)
        if value is None:
            break
        keys.append(value)
        idx += 1
    return keys


class Config:
    """Application configuration."""

    # Project paths (4 parents: config/__init__.py -> config -> shared -> src -> project_root)
    ROOT_DIR = Path(__file__).parent.parent.parent.parent
    DATA_DIR = ROOT_DIR / "data"
    LOGS_DIR = ROOT_DIR / "logs"

    # Database
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
    DB_NAME: str = os.getenv("DB_NAME", "fx_alphalab")
    DB_USER: str = os.getenv("DB_USER", "postgres")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "")

    # API Keys
    FRED_API_KEY: str | None = os.getenv("FRED_API_KEY")
    GROQ_API_KEYS: list[str] = _load_groq_api_keys()

    # Google Cloud (BigQuery for GDELT)
    GOOGLE_APPLICATION_CREDENTIALS: str | None = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    # MT5 Configuration
    MT5_LOGIN: int | None = (
        int(os.getenv("MT5_LOGIN", "0"))
        if os.getenv("MT5_LOGIN") and os.getenv("MT5_LOGIN").isdigit()
        else None
    )
    MT5_PASSWORD: str | None = os.getenv("MT5_PASSWORD")
    MT5_SERVER: str | None = os.getenv("MT5_SERVER")

    # Data collection settings
    SCRAPING_DELAY: float = float(os.getenv("SCRAPING_DELAY", "3.0"))
    REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "30"))
    REDDIT_LABELS_PATH: Path = DATA_DIR / "processed" / "reddit" / "labels_checkpoint.jsonl"
    STOCKTWITS_LABELS_PATH: Path = (
        DATA_DIR / "processed" / "sentiment" / "source=stockwits" / "labels_checkpoint.jsonl"
    )
    STOCKTWITS_MODEL_DIR: Path = ROOT_DIR / "models" / "sentiment" / "stocktwits"

    # Auth settings
    AUTH_JWT_SECRET: str | None = os.getenv("AUTH_JWT_SECRET")
    AUTH_JWT_ALGORITHM: str = os.getenv("AUTH_JWT_ALGORITHM", "HS256")
    AUTH_ACCESS_TOKEN_MINUTES: int = int(os.getenv("AUTH_ACCESS_TOKEN_MINUTES", "1440"))
    AUTH_REFRESH_TOKEN_DAYS: int = int(os.getenv("AUTH_REFRESH_TOKEN_DAYS", "365"))

    # SMTP / Email settings
    SMTP_HOST: str = os.getenv("SMTP_HOST", "smtp.gmail.com")
    SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USER: str | None = os.getenv("SMTP_USER")
    SMTP_PASSWORD: str | None = os.getenv("SMTP_PASSWORD")
    SMTP_FROM_NAME: str = os.getenv("SMTP_FROM_NAME", "FX-AlphaLab")

    @classmethod
    def validate(cls) -> None:
        """Validate required configuration."""
        if not cls.FRED_API_KEY:
            raise ValueError("FRED_API_KEY not set in environment")

    @property
    def database_url(self) -> str:
        """Construct database URL."""
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"


config = Config()
