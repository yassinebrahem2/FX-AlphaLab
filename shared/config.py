"""Configuration management for FX-AlphaLab."""
import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class Config:
    """Application configuration."""

    # Project paths
    ROOT_DIR = Path(__file__).parent.parent
    DATA_DIR = ROOT_DIR / "data"
    LOGS_DIR = ROOT_DIR / "logs"

    # Database
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
    DB_NAME: str = os.getenv("DB_NAME", "fx_alphalab")
    DB_USER: str = os.getenv("DB_USER", "postgres")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "")

    # API Keys
    FRED_API_KEY: Optional[str] = os.getenv("FRED_API_KEY")

    # MT5 Configuration
    MT5_LOGIN: Optional[int] = int(os.getenv("MT5_LOGIN", "0")) if os.getenv("MT5_LOGIN") else None
    MT5_PASSWORD: Optional[str] = os.getenv("MT5_PASSWORD")
    MT5_SERVER: Optional[str] = os.getenv("MT5_SERVER")

    # Data collection settings
    SCRAPING_DELAY: float = float(os.getenv("SCRAPING_DELAY", "3.0"))
    REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "30"))

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
