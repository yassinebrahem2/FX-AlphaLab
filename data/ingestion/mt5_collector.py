"""
MT5 Connector

Infrastructure-only component.
- No preprocessing
- No feature engineering
- No intelligence or trading logic
"""

import time
from typing import Dict

import MetaTrader5 as mt5
import pandas as pd


class MT5Connector:
    """
    Thin wrapper around the MetaTrader5 Python API.

    Responsibilities:
    - Manage MT5 connection lifecycle
    - Fetch raw OHLC data from MT5

    Out of scope:
    - Data cleaning
    - Feature engineering
    - Indicators
    - Trading logic
    """

    TIMEFRAMES: Dict[str, int] = {
        "H1": mt5.TIMEFRAME_H1,
        "H4": mt5.TIMEFRAME_H4,
        "D1": mt5.TIMEFRAME_D1,
    }

    def __init__(self, max_retries: int = 3, retry_delay_sec: int = 2) -> None:
        self.connected: bool = False
        self.max_retries = max_retries
        self.retry_delay_sec = retry_delay_sec

    def connect(self) -> None:
        """
        Initialize MT5 connection with retries.
        """
        if self.connected:
            return

        for _ in range(self.max_retries):
            if mt5.initialize():
                self.connected = True
                return
            time.sleep(self.retry_delay_sec)

        raise RuntimeError(
            f"MT5 initialization failed after {self.max_retries} attempts"
        )

    def shutdown(self) -> None:
        """
        Shutdown MT5 connection safely.
        """
        if self.connected:
            mt5.shutdown()
            self.connected = False

    def _ensure_symbol(self, symbol: str) -> None:
        """
        Ensure symbol is available in MT5.
        """
        if not mt5.symbol_select(symbol, True):
            raise RuntimeError(f"Symbol not available in MT5: {symbol}")

    def fetch_ohlc(
        self,
        symbol: str,
        timeframe: str,
        n_bars: int = 5000,
    ) -> pd.DataFrame:
        """
        Fetch raw OHLC data from MT5.

        Parameters:
            symbol: FX symbol (e.g. 'EURUSD')
            timeframe: One of ['H1', 'H4', 'D1']
            n_bars: Number of bars to retrieve

        Returns:
            pandas.DataFrame with raw MT5 OHLC fields
        """
        if timeframe not in self.TIMEFRAMES:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        if not self.connected:
            self.connect()

        self._ensure_symbol(symbol)

        rates = mt5.copy_rates_from_pos(
            symbol,
            self.TIMEFRAMES[timeframe],
            0,
            n_bars,
        )

        if rates is None or len(rates) == 0:
            raise RuntimeError(
                f"No data returned for {symbol} ({timeframe})"
            )

        return pd.DataFrame(rates)
