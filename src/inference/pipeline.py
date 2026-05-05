"""Live inference pipeline: runs all 4 agents, assembles signals, writes Gold DB.

Typical usage:
    from src.inference.pipeline import InferencePipeline
    result = InferencePipeline().run()          # target = yesterday
    result = InferencePipeline().run(dry_run=True)   # skip DB writes

T-1 data lag contract
---------------------
"Today's signal" uses yesterday's close as the target_date.  GDELT publishes
T+1, macro signals are monthly, so requesting T-1 is the correct production
anchor.  Pass an explicit `target_date` to override (e.g. for historical
replay or testing).

Error-handling policy
---------------------
- Technical model pkl or OHLCV Parquet missing → hard error (FileNotFoundError propagates).
- Any other per-source exception → logged as a stale warning; corresponding
  columns in the assembled row are set to None; coordinator applies its fallback
  logic (FLAT / regime-only).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.agents.coordinator.calibration import load_calibration
from src.agents.coordinator.predict import Coordinator
from src.agents.coordinator.signal import CoordinatorReport
from src.agents.geopolitical.agent import GeopoliticalAgent
from src.agents.geopolitical.signal import GeopoliticalSignal
from src.agents.macro.agent import MacroAgent
from src.agents.macro.signal import MacroSignal
from src.agents.sentiment.agent import SentimentAgent
from src.agents.sentiment.gdelt_node import GDELTSignalNode
from src.agents.sentiment.google_trends_node import GoogleTrendsSignalNode
from src.agents.sentiment.reddit_node import RedditSignalNode
from src.agents.sentiment.signal import SentimentSignal
from src.agents.sentiment.stocktwits_node import StocktwitsSignalNode
from src.agents.technical.agent import TechnicalAgent, TechnicalSignal, fuse_timeframe_signals
from src.inference.assembler import assemble_row
from src.shared.utils import setup_logger

logger = logging.getLogger(__name__)

_PAIRS = ["EURUSD", "GBPUSD", "USDCHF", "USDJPY"]
_TIMEFRAMES = ["D1", "H4", "H1"]


@dataclass
class PipelineResult:
    target_date: pd.Timestamp
    report: CoordinatorReport
    stale_warnings: dict[str, str]
    db_rows: dict[str, int]


class InferencePipeline:
    """Orchestrates all 4 agents for a single target date and writes Gold DB.

    All file paths default to the production layout under `root_dir`.
    Pass explicit paths to override for testing.
    """

    def __init__(
        self,
        root_dir: Path | None = None,
        calibration_path: Path | None = None,
    ) -> None:
        self._root = root_dir or Path(__file__).resolve().parents[2]
        self._cal_path = calibration_path or (
            self._root / "models" / "production" / "coordinator_calibration.json"
        )
        self._log = setup_logger(self.__class__.__name__)

    # ── Public API ────────────────────────────────────────────────────────────

    def run(
        self,
        target_date: pd.Timestamp | None = None,
        dry_run: bool = False,
    ) -> PipelineResult:
        """Run full inference for target_date (defaults to yesterday UTC).

        Args:
            target_date: UTC date to run inference for.  Defaults to yesterday.
            dry_run: When True, skip all DB writes.

        Returns:
            PipelineResult with the CoordinatorReport and metadata.

        Raises:
            FileNotFoundError: If a technical model pkl or OHLCV Parquet is missing for any pair.
        """
        if target_date is None:
            target_date = pd.Timestamp.utcnow().normalize() - pd.Timedelta(days=1)

        target_date = pd.Timestamp(target_date)
        if target_date.tzinfo is None:
            target_date = target_date.tz_localize("UTC")

        self._log.info("Running inference for %s (dry_run=%s)", target_date.date(), dry_run)

        stale: dict[str, str] = {}

        # ── 0. Rebuild macro_signal.parquet (ensures tone features are fresh) ──
        try:
            self._rebuild_macro_signal()
        except Exception as exc:
            macro_path = self._root / "data" / "processed" / "macro" / "macro_signal.parquet"
            if macro_path.exists():
                self._log.warning(
                    "macro_signal rebuild failed (%s); using stale file from %s. "
                    "Fix: run DukascopyPreprocessor to create Silver OHLCV D1 parquets.",
                    exc,
                    pd.Timestamp(macro_path.stat().st_mtime, unit="s").date(),
                )
                stale["macro_signal_rebuild"] = str(exc)
            else:
                self._log.error("Failed to rebuild macro_signal and no stale file exists: %s", exc)
                raise

        # ── 1. Technical signals (per pair, multi-timeframe fused) ────────────
        tech_signals: dict[str, TechnicalSignal | None] = {}
        for pair in _PAIRS:
            try:
                tech_signals[pair] = self._run_technical(pair, target_date)
            except FileNotFoundError as exc:
                raise FileNotFoundError(
                    f"Technical artifact missing for {pair} — cannot run inference: {exc}"
                ) from exc
            except Exception as exc:
                self._log.warning("TechnicalAgent failed for %s: %s", pair, exc)
                stale[f"technical_{pair}"] = str(exc)
                tech_signals[pair] = None

        # ── 2. Macro signals (per pair) ───────────────────────────────────────
        macro_agent = self._build_macro_agent()
        macro_signals: dict[str, MacroSignal | None] = {}
        for pair in _PAIRS:
            try:
                macro_signals[pair] = macro_agent.predict(pair, target_date)
            except Exception as exc:
                self._log.warning("MacroAgent failed for %s: %s", pair, exc)
                stale[f"macro_{pair}"] = str(exc)
                macro_signals[pair] = None

        # ── 3. Geopolitical signals (per pair) ───────────────────────────────
        geo_agent = self._build_geo_agent()
        geo_signals: dict[str, GeopoliticalSignal | None] = {}
        for pair in _PAIRS:
            try:
                geo_signals[pair] = geo_agent.predict(pair, target_date)
            except Exception as exc:
                self._log.warning("GeopoliticalAgent failed for %s: %s", pair, exc)
                stale[f"geo_{pair}"] = str(exc)
                geo_signals[pair] = None

        # ── 4. Sentiment signal (global, one call) ───────────────────────────
        sentiment_signal: SentimentSignal | None = None
        try:
            sentiment_agent = self._build_sentiment_agent()
            sentiment_signal = sentiment_agent.get_signal(target_date.to_pydatetime())
            # Surface partial-availability warnings: macro_attention relies on Google
            # Trends which may be stale; gdelt_tone may be missing for recent dates.
            if sentiment_signal.macro_attention_zscore is None:
                stale["sentiment_gtrends"] = (
                    "macro_attention_zscore unavailable (Google Trends stale)"
                )
            if sentiment_signal.gdelt_tone_zscore is None:
                stale["sentiment_gdelt"] = (
                    "gdelt_tone_zscore unavailable (GDELT stale or out of range)"
                )
        except Exception as exc:
            self._log.warning("SentimentAgent failed: %s", exc)
            stale["sentiment"] = str(exc)

        # ── 5. Assemble signals_aligned rows ──────────────────────────────────
        rows = [
            assemble_row(
                pair=pair,
                date=target_date,
                technical=tech_signals[pair],
                macro=macro_signals[pair],
                geopolitical=geo_signals[pair],
                sentiment=sentiment_signal,
            )
            for pair in _PAIRS
        ]
        signals_df = pd.DataFrame(rows)
        signals_df["date"] = pd.to_datetime(signals_df["date"])

        # ── 6. Coordinator ────────────────────────────────────────────────────
        cal = load_calibration(self._cal_path)
        coordinator = Coordinator(calibration=cal)
        report = coordinator.build_report(signals_df)

        if stale:
            self._log.warning("Stale sources for %s: %s", target_date.date(), list(stale.keys()))

        # ── 7. Write to Gold DB ───────────────────────────────────────────────
        db_rows: dict[str, int] = {}
        if not dry_run:
            db_rows = self._write_to_db(signals_df, report)

        return PipelineResult(
            target_date=target_date,
            report=report,
            stale_warnings=stale,
            db_rows=db_rows,
        )

    # ── Agent builders ────────────────────────────────────────────────────────

    def _rebuild_macro_signal(self) -> None:
        """Rebuild macro_signal.parquet to ensure tone features are fresh.

        Requires Silver OHLCV D1 parquets to exist (DukascopyPreprocessor must have run).
        If rebuild fails and a stale macro_signal.parquet exists, the caller falls back to
        the stale file and logs a warning rather than aborting inference.
        """
        from src.ingestion.preprocessors.macro_signal_builder import MacroSignalBuilder

        builder = MacroSignalBuilder()
        builder.run(backfill=True)
        self._log.info("Rebuilt macro_signal.parquet successfully")

    # ── Agent builders ────────────────────────────────────────────────────────

    def _run_technical(self, pair: str, date: pd.Timestamp) -> TechnicalSignal:
        """Load 3 LSTM artifacts for pair and return fused multi-timeframe signal."""
        model_dir = self._root / "models" / "technical"
        # Model files use 'm' suffix (e.g. EURUSDm_D1.pkl)
        suffix = "m"
        agents: dict[str, TechnicalAgent] = {}
        for tf in _TIMEFRAMES:
            pkl = model_dir / f"{pair}{suffix}_{tf}.pkl"
            if not pkl.exists():
                raise FileNotFoundError(f"Technical model not found: {pkl}")
            agents[tf] = TechnicalAgent.load(pkl)

        signals = {tf: agent.predict(pair, date) for tf, agent in agents.items()}
        return fuse_timeframe_signals(signals)

    def _build_macro_agent(self) -> MacroAgent:
        macro_path = self._root / "data" / "processed" / "macro" / "macro_signal.parquet"
        agent = MacroAgent(data_path=macro_path)
        agent.load()
        return agent

    def _build_geo_agent(self) -> GeopoliticalAgent:
        agent = GeopoliticalAgent(
            zone_features_path=(
                self._root / "data" / "processed" / "geopolitical" / "zone_features_daily.parquet"
            ),
            clean_silver_dir=self._root / "data" / "processed" / "gdelt_events",
            model_states_path=self._root / "models" / "production" / "gdelt_gat_final_states.pt",
        )
        agent.load()
        return agent

    def _build_sentiment_agent(self) -> SentimentAgent:
        data_dir = self._root / "data" / "processed"
        sentiment_dir = data_dir / "sentiment"

        stocktwits_node = StocktwitsSignalNode(
            checkpoint_path=(sentiment_dir / "source=stockwits" / "labels_checkpoint.jsonl")
        )
        reddit_node = RedditSignalNode(
            checkpoint_path=self._root / "data" / "processed" / "reddit" / "labels_checkpoint.jsonl"
        )
        gdelt_node = GDELTSignalNode(silver_dir=sentiment_dir / "source=gdelt")

        from src.shared.config.sources import load_sources_config

        sources_cfg = load_sources_config(self._root / "config" / "sources.yaml")
        gtrends_cfg = sources_cfg.sources.get("google_trends")
        gtrends_node = (
            GoogleTrendsSignalNode(silver_dir=sentiment_dir)
            if gtrends_cfg and gtrends_cfg.enabled
            else None
        )

        return SentimentAgent(
            stocktwits_node=stocktwits_node,
            reddit_node=reddit_node,
            gdelt_node=gdelt_node,
            gtrends_node=gtrends_node,
        )

    # ── DB writer ─────────────────────────────────────────────────────────────

    def _write_to_db(self, signals_df: pd.DataFrame, report: CoordinatorReport) -> dict[str, int]:
        from src.shared.db.storage import (
            insert_agent_signals,
            insert_coordinator_report,
            insert_coordinator_signals,
        )

        agent_rows_written = insert_agent_signals(signals_df.to_dict("records"))

        coord_rows = self._report_to_coordinator_rows(report)
        coord_rows_written = insert_coordinator_signals(coord_rows)

        report_dict = {
            "date": report.date.date(),
            "top_pick": report.top_pick,
            "overall_action": report.overall_action,
            "hold_reason": report.hold_reason,
            "global_regime": report.global_regime,
            "narrative_context": report.narrative_context,
        }
        report_written = insert_coordinator_report(report_dict)

        self._log.info(
            "DB write: agent_signals=%d coordinator_signals=%d coordinator_report=%s",
            agent_rows_written,
            coord_rows_written,
            report_written,
        )
        return {
            "agent_signals": agent_rows_written,
            "coordinator_signals": coord_rows_written,
            "coordinator_reports": int(report_written),
        }

    @staticmethod
    def _report_to_coordinator_rows(report: CoordinatorReport) -> list[dict]:
        rows = []
        top_pairs = {report.top_pick} if report.top_pick else set()
        for pa in report.pairs:
            rows.append(
                {
                    "date": report.date.date(),
                    "pair": pa.pair,
                    "vol_signal": pa.vol_signal_norm,
                    "vol_source": pa.vol_source,
                    "direction": pa.direction,
                    "direction_source": pa.direction_source,
                    "direction_horizon": pa.direction_horizon,
                    "direction_ic": pa.direction_ic,
                    "confidence_tier": pa.confidence_tier,
                    "flat_reason": pa.flat_reason,
                    "regime": pa.regime,
                    "suggested_action": pa.suggested_action,
                    "conviction_score": pa.conviction_score,
                    "position_size_pct": pa.position_size_pct,
                    "sl_pct": pa.sl_pct,
                    "tp_pct": pa.tp_pct,
                    "risk_reward_ratio": pa.risk_reward_ratio,
                    "estimated_vol_3d": pa.estimated_vol_3d,
                    "is_top_pick": pa.pair in top_pairs,
                    "overall_action": report.overall_action,
                }
            )
        return rows
