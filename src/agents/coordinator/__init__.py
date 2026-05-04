"""Coordinator agent: aggregates agent signals into deterministic trade recommendations."""

from src.agents.coordinator.backtest import Backtester, BacktestResult
from src.agents.coordinator.calibration import CalibrationParams, load_calibration, refit
from src.agents.coordinator.predict import Coordinator
from src.agents.coordinator.prompt import build_llm_prompt
from src.agents.coordinator.signal import (
    ClosedTrade,
    CoordinatorReport,
    CoordinatorSignal,
    OpenTrade,
    PairAnalysis,
)

__all__ = [
    "Backtester",
    "BacktestResult",
    "CalibrationParams",
    "ClosedTrade",
    "Coordinator",
    "CoordinatorReport",
    "CoordinatorSignal",
    "OpenTrade",
    "PairAnalysis",
    "build_llm_prompt",
    "load_calibration",
    "refit",
]
