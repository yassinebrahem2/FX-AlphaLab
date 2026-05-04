"""Bronze → Silver preprocessor for Dukascopy 1-minute OHLCV data.

Reads per-day Bronze Parquet files produced by DukascopyCollector and
resamples to H1 / H4 / D1 Silver files consumed by TechnicalAgent and
MacroAgent.

Silver output:
    data/processed/ohlcv/ohlcv_{INSTRUMENT}_{TF}_latest.parquet

The ``_latest`` suffix ensures a single, consistently-named file per
instrument+timeframe that satisfies every glob pattern in the codebase:
    ohlcv_{base}_{tf}_*.parquet  (features.py, agent.py)
    ohlcv_*_D1_*.parquet         (calendar_node.py, macro_signal_builder.py)

Anchor:
    All three timeframes use midnight UTC — confirmed empirically against
    MT5 Silver data in NB20 (ohlcv_provider_comparison).  Resample rule:
        H1  → "h"   (pandas 2.x)
        H4  → "4h"  (pandas 2.x)
        D1  → "D"
    closed="left", label="left" throughout.

Collision safety:
    Before writing a new _latest file, all pre-existing files matching
    ohlcv_{instrument}_{tf}_*.parquet in the output directory are removed.
    This prevents find_parquet() from raising ValueError("Multiple parquets
    found") if notebook artifacts from older runs coexist.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.ingestion.preprocessors.base_preprocessor import BasePreprocessor
from src.shared.config import Config

_TF_RULE: dict[str, str] = {
    "H1": "h",
    "H4": "4h",
    "D1": "D",
}


class DukascopyPreprocessor(BasePreprocessor):
    """Resample Dukascopy 1-min Bronze bars to H1 / H4 / D1 Silver Parquet files."""

    CATEGORY = "ohlcv"

    DEFAULT_INSTRUMENTS: list[str] = ["EURUSD", "GBPUSD", "USDJPY", "USDCHF"]
    TIMEFRAMES: list[str] = ["H1", "H4", "D1"]

    def __init__(
        self,
        input_dir: Path | None = None,
        output_dir: Path | None = None,
        instruments: list[str] | None = None,
        log_file: Path | None = None,
    ) -> None:
        super().__init__(
            input_dir=input_dir or Config.DATA_DIR / "raw" / "dukascopy",
            output_dir=output_dir or Config.DATA_DIR / "processed" / "ohlcv",
            log_file=log_file or Config.LOGS_DIR / "preprocessors" / "dukascopy_preprocessor.log",
        )
        self.instruments = instruments or list(self.DEFAULT_INSTRUMENTS)

    # ── BasePreprocessor interface ─────────────────────────────────────────

    def preprocess(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        backfill: bool = False,
    ) -> dict[str, pd.DataFrame]:
        """Resample Bronze 1-min bars to H1 / H4 / D1 Silver files.

        Args:
            start_date: Only load Bronze files from this date onward.
            end_date: Only load Bronze files up to this date.
            backfill: When False, skip any instrument+timeframe whose Silver
                file already exists. When True, reprocess all.

        Returns:
            Mapping of "{instrument}_{tf}" → resampled DataFrame for each
            combination that was actually processed (skipped ones omitted).
        """
        results: dict[str, pd.DataFrame] = {}

        for instrument in self.instruments:
            df_1min = self._load_bronze_1min(instrument, start_date, end_date)

            for tf in self.TIMEFRAMES:
                silver_path = self._silver_path(instrument, tf)

                if silver_path.exists() and not backfill:
                    self.logger.debug("Skipping %s_%s — Silver exists", instrument, tf)
                    continue

                if df_1min.empty:
                    self.logger.warning("No Bronze 1-min data for %s — skipping %s", instrument, tf)
                    continue

                df_tf = _resample_ohlcv(df_1min, tf)
                df_tf["pair"] = instrument
                df_tf["timeframe"] = tf
                df_tf["source"] = "dukascopy"

                self._write_silver(df_tf, silver_path, instrument, tf)
                self.logger.info(
                    "%s_%s: wrote %d bars to %s", instrument, tf, len(df_tf), silver_path.name
                )
                results[f"{instrument}_{tf}"] = df_tf

        return results

    def validate(self, df: pd.DataFrame) -> bool:
        required = {"timestamp_utc", "pair", "timeframe", "open", "high", "low", "close", "volume"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns: {missing}")
        if df.empty:
            raise ValueError("DataFrame is empty")
        high_ok = (
            (df["high"] >= df["open"]) & (df["high"] >= df["close"]) & (df["high"] >= df["low"])
        )
        low_ok = (df["low"] <= df["open"]) & (df["low"] <= df["close"]) & (df["low"] <= df["high"])
        if not high_ok.all():
            raise ValueError(f"OHLC high violation: {(~high_ok).sum()} rows")
        if not low_ok.all():
            raise ValueError(f"OHLC low violation: {(~low_ok).sum()} rows")
        return True

    def health_check(self) -> bool:
        instr_dir = self.input_dir / self.instruments[0]
        return instr_dir.exists() and any(instr_dir.glob("**/*.parquet"))

    # ── Private helpers ────────────────────────────────────────────────────

    def _load_bronze_1min(
        self,
        instrument: str,
        start_date: datetime | None,
        end_date: datetime | None,
    ) -> pd.DataFrame:
        """Concatenate all per-day Bronze Parquet files for one instrument."""
        instr_dir = self.input_dir / instrument
        if not instr_dir.exists():
            self.logger.warning("Bronze dir not found: %s", instr_dir)
            return pd.DataFrame(columns=["timestamp_utc", "open", "high", "low", "close", "volume"])

        frames: list[pd.DataFrame] = []
        for path in sorted(instr_dir.glob("**/*.parquet")):
            df = pd.read_parquet(path)
            if not df.empty:
                frames.append(df)

        if not frames:
            return pd.DataFrame(columns=["timestamp_utc", "open", "high", "low", "close", "volume"])

        combined = pd.concat(frames, ignore_index=True)
        combined["timestamp_utc"] = pd.to_datetime(combined["timestamp_utc"], utc=True)
        combined = (
            combined.sort_values("timestamp_utc")
            .drop_duplicates(subset=["timestamp_utc"])
            .reset_index(drop=True)
        )

        if start_date is not None:
            ts_start = (
                pd.Timestamp(start_date, tz="UTC")
                if start_date.tzinfo is None
                else pd.Timestamp(start_date).tz_convert("UTC")
            )
            combined = combined[combined["timestamp_utc"] >= ts_start]
        if end_date is not None:
            ts_end = (
                pd.Timestamp(end_date, tz="UTC")
                if end_date.tzinfo is None
                else pd.Timestamp(end_date).tz_convert("UTC")
            )
            combined = combined[combined["timestamp_utc"] <= ts_end]

        return combined.reset_index(drop=True)

    def _silver_path(self, instrument: str, tf: str) -> Path:
        return self.output_dir / f"ohlcv_{instrument}_{tf}_latest.parquet"

    def _write_silver(self, df: pd.DataFrame, path: Path, instrument: str, tf: str) -> None:
        """Atomically write Silver Parquet, removing any old files for the same slot."""
        # Remove all pre-existing files matching the same instrument+tf to prevent
        # find_parquet() from raising ValueError("Multiple parquets found").
        for stale in self.output_dir.glob(f"ohlcv_{instrument}_{tf}_*.parquet"):
            stale.unlink()

        tmp = path.with_suffix(".tmp")
        try:
            # Write timestamp_utc as the index so load_pair()'s
            # ``df.index = pd.to_datetime(df.index)`` restores real timestamps.
            # No explicit schema: PyArrow infers types from pandas dtypes, which
            # correctly preserves the DatetimeIndex and float32 volume.
            df_out = df.set_index("timestamp_utc")
            table = pa.Table.from_pandas(df_out, preserve_index=True)
            pq.write_table(table, str(tmp), compression="zstd", compression_level=12)
            tmp.rename(path)
        except Exception:
            tmp.unlink(missing_ok=True)
            raise


def _resample_ohlcv(df_1min: pd.DataFrame, tf: str) -> pd.DataFrame:
    """Resample 1-min OHLCV to a coarser timeframe.

    Uses midnight UTC anchor for all timeframes (confirmed against MT5 Silver
    in NB20). Bars are labeled by their open timestamp (closed="left",
    label="left").

    Args:
        df_1min: DataFrame with timestamp_utc (tz-aware UTC) and OHLCV columns.
        tf: One of "H1", "H4", "D1".

    Returns:
        Resampled DataFrame with timestamp_utc reset as column.
    """
    rule = _TF_RULE[tf]
    resampled = (
        df_1min.set_index("timestamp_utc")
        .resample(rule, closed="left", label="left")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna(subset=["open"])
        .reset_index()
    )
    return resampled
