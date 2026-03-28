"""GDELT GKG preprocessor: Bronze JSONL → Silver partitioned Parquet.

GDELT Global Knowledge Graph (GKG) records do not contain article titles.
Sentiment is derived directly from the V2Tone field (pre-computed by GDELT)
rather than FinBERT. V2Tone[0] is the overall tone score, typically in the
range [-10, +10] (theoretically unbounded, but this covers ~99.9% of records).

Sentiment derivation:
    sentiment_score  = clip(v2tone / 10, -1.0, 1.0)
    sentiment_label  = "positive"  if score >  0.05
                     | "negative"  if score < -0.05
                     | "neutral"   otherwise

Probability proxy (NOT real model probabilities — documented intentionally):
    sentiment_prob_*: 0/1 proxy values.
    Assigned 1.0 to the winning class, 0.0 to all others.
    Downstream consumers must not treat these as calibrated probabilities.

Silver Schema (identical to NewsPreprocessor output):
    timestamp_utc, article_id, pair, direction_multiplier,
    headline, text_input_type, sentiment_score, sentiment_label,
    sentiment_prob_pos, sentiment_prob_neg, sentiment_prob_neutral,
    document_type, speaker, source, url
"""

from datetime import datetime
from pathlib import Path

import pandas as pd

from src.ingestion.preprocessors.document_preprocessor import DocumentPreprocessor


class GDELTPreprocessor(DocumentPreprocessor):
    """Preprocessor for GDELT GKG aggregated news data.

    Reads Bronze JSONL produced by GDELTCollector, applies V2Tone-based
    sentiment scoring (no FinBERT), fans out each record to major FX pairs,
    and exports Silver Parquet partitioned by source/year/month.

    V2Tone is GDELT's pre-computed tone score for the entire article.
    GDELT is global financial/political news — it is not currency-specific,
    so all affected pairs use direction_multiplier=+1 (positive tone → risk-on).
    """

    SOURCE_NAME = "gdelt"

    # GDELT is global news; map to major FX pairs with direction_multiplier=+1
    # (positive tone = risk-on environment, +1 for all pairs by convention)
    PAIRS: list[tuple[str, int]] = [
        ("EURUSD", +1),
        ("GBPUSD", +1),
        ("USDJPY", +1),
    ]

    # Neutral threshold: scores within ±0.05 are labelled "neutral"
    NEUTRAL_THRESHOLD: float = 0.05

    def __init__(
        self,
        input_dir: Path,
        output_dir: Path,
        log_file: Path | None = None,
    ) -> None:
        """Initialise GDELTPreprocessor.

        Args:
            input_dir: Directory containing Bronze JSONL files
                       (typically data/raw/news/gdelt/).
            output_dir: Root directory for Silver Parquet output
                        (typically data/processed/sentiment/).
            log_file: Optional path for file-based logging.
        """
        super().__init__(input_dir, output_dir, log_file)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def preprocess(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        source: str | None = None,
    ) -> pd.DataFrame:
        """Transform Bronze GDELT JSONL to Silver sentiment DataFrame.

        Args:
            start_date: Optional lower bound for timestamp_published filtering.
            end_date: Optional upper bound for timestamp_published filtering.
            source: Unused (GDELT preprocessor always targets 'gdelt').
                    Accepted for interface compatibility with DocumentPreprocessor.

        Returns:
            DataFrame containing Silver sentiment records with pair fan-out
            (one row per document per pair in PAIRS).

        Raises:
            ValueError: If no JSONL files are found or all records are invalid.
        """
        self.logger.info(
            "Starting GDELT preprocessing: start=%s, end=%s",
            start_date,
            end_date,
        )

        jsonl_files = self.find_jsonl_files()
        if not jsonl_files:
            raise ValueError(f"No JSONL files found in {self.input_dir}")

        all_records: list[dict] = []
        for file_path in jsonl_files:
            try:
                documents = self.read_jsonl(file_path)
                records = self._process_documents(documents)
                all_records.extend(records)
            except Exception as exc:
                self.logger.error("Failed to process %s: %s", file_path, exc)
                continue

        if not all_records:
            raise ValueError("No valid records extracted from GDELT JSONL files")

        df = pd.DataFrame(all_records)

        # Optional date filtering
        if start_date or end_date:
            df["_ts"] = pd.to_datetime(df["timestamp_utc"], utc=True)
            if start_date:
                df = df[df["_ts"] >= pd.Timestamp(start_date, tz="UTC")]
            if end_date:
                df = df[df["_ts"] <= pd.Timestamp(end_date, tz="UTC")]
            df = df.drop(columns=["_ts"])

        # Deduplicate on composite key (article_id, pair)
        initial = len(df)
        df = df.drop_duplicates(subset=["article_id", "pair"], keep="first")
        if len(df) < initial:
            self.logger.info("Removed %d duplicate (article_id, pair) records", initial - len(df))

        df = df.sort_values(["timestamp_utc", "pair"]).reset_index(drop=True)

        self.validate(df)

        self.logger.info("Preprocessed %d GDELT sentiment records (after pair fan-out)", len(df))
        return df

    def validate(self, df: pd.DataFrame) -> bool:
        """Validate Silver sentiment schema for GDELT records.

        Same column requirements as NewsPreprocessor with the following
        GDELT-specific allowances:
          - text_input_type may be "v2tone" (in addition to standard values)
          - probability columns are 0/1 proxy values (sum still ≈ 1.0)

        Args:
            df: DataFrame to validate.

        Returns:
            True if validation passes.

        Raises:
            ValueError: If any validation check fails.
        """
        required_columns = [
            "timestamp_utc",
            "article_id",
            "pair",
            "direction_multiplier",
            "headline",
            "text_input_type",
            "sentiment_score",
            "sentiment_label",
            "sentiment_prob_pos",
            "sentiment_prob_neg",
            "sentiment_prob_neutral",
            "document_type",
            "speaker",
            "source",
            "url",
        ]

        missing = set(required_columns) - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        critical_non_null = [
            "timestamp_utc",
            "article_id",
            "pair",
            "direction_multiplier",
            "headline",
            "text_input_type",
            "sentiment_score",
            "sentiment_label",
            "sentiment_prob_pos",
            "sentiment_prob_neg",
            "sentiment_prob_neutral",
            "document_type",
            "source",
        ]
        for field in critical_non_null:
            if df[field].isna().any():
                null_count = df[field].isna().sum()
                raise ValueError(f"Null values in critical field '{field}': {null_count}")

        if not df["sentiment_score"].between(-1.0, 1.0).all():
            invalid = (~df["sentiment_score"].between(-1.0, 1.0)).sum()
            raise ValueError(f"sentiment_score outside [-1.0, 1.0]: {invalid} records")

        valid_labels = {"positive", "neutral", "negative"}
        invalid_labels = set(df["sentiment_label"].unique()) - valid_labels
        if invalid_labels:
            raise ValueError(f"Invalid sentiment_label values: {invalid_labels}")

        valid_multipliers = {-1, 1}
        invalid_multipliers = set(df["direction_multiplier"].unique()) - valid_multipliers
        if invalid_multipliers:
            raise ValueError(f"Invalid direction_multiplier values: {invalid_multipliers}")

        invalid_pairs = df[df["pair"].str.len() != 6]
        if len(invalid_pairs) > 0:
            raise ValueError(f"Invalid pair format (must be 6 chars): {len(invalid_pairs)} records")

        # Allow "v2tone" in addition to standard NewsPreprocessor types
        valid_types = {"headline_body", "headline_only", "v2tone"}
        invalid_types = set(df["text_input_type"].unique()) - valid_types
        if invalid_types:
            raise ValueError(f"Invalid text_input_type values: {invalid_types}")

        # Probability proxy values must sum to 1.0 (tolerance 0.01)
        prob_sums = (
            df["sentiment_prob_pos"] + df["sentiment_prob_neg"] + df["sentiment_prob_neutral"]
        )
        invalid_sums = (~prob_sums.between(0.99, 1.01)).sum()
        if invalid_sums > 0:
            raise ValueError(f"Probability sum not ≈ 1.0 (tolerance 0.01): {invalid_sums} records")

        duplicates = df.duplicated(subset=["article_id", "pair"]).sum()
        if duplicates > 0:
            raise ValueError(f"Duplicate (article_id, pair) found: {duplicates} duplicates")

        self.logger.info("Schema validation passed for %d records", len(df))
        return True

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _process_documents(self, documents: list[dict]) -> list[dict]:
        """Convert list of Bronze GDELT documents to Silver records with fan-out.

        For each document:
          1. Parse V2Tone → sentiment score
          2. Derive label and probability proxies
          3. Fan out to each pair in PAIRS

        Args:
            documents: Bronze document dicts from JSONL.

        Returns:
            List of Silver records (len(documents) × len(PAIRS)).
        """
        records: list[dict] = []
        for doc in documents:
            try:
                row = self._build_record(doc)
            except Exception as exc:
                self.logger.warning(
                    "Skipping GDELT record (url=%s): %s",
                    doc.get("url", "unknown"),
                    exc,
                )
                continue

            for pair, direction_multiplier in self.PAIRS:
                fanned = row.copy()
                fanned["pair"] = pair
                fanned["direction_multiplier"] = direction_multiplier
                records.append(fanned)

        return records

    def _build_record(self, doc: dict) -> dict:
        """Build a single (pre-fan-out) Silver record from a Bronze document.

        Args:
            doc: Bronze GDELT document dict.

        Returns:
            Partial Silver record (without 'pair' and 'direction_multiplier').

        Raises:
            KeyError: If required Bronze fields are missing.
            ValueError: If timestamp cannot be parsed.
        """
        url = doc.get("url") or ""
        if not url:
            raise ValueError("Document has no URL — cannot generate article_id")

        timestamp_raw = doc.get("timestamp_published") or doc["timestamp_collected"]
        timestamp_utc = pd.to_datetime(timestamp_raw, utc=True).strftime("%Y-%m-%dT%H:%M:%SZ")

        article_id = self.generate_article_id(
            url=url,
            title="",
            timestamp=timestamp_utc,
            source=self.SOURCE_NAME,
        )

        # headline: use URL as substitute for missing article title
        headline = self.clean_text(url)

        v2tone = self._parse_v2tone(doc.get("tone"))
        score = round(max(-1.0, min(1.0, v2tone / 10.0)), 4)
        label = self._score_to_label(score)
        prob_pos, prob_neg, prob_neutral = self._score_to_probs(label)

        return {
            "timestamp_utc": timestamp_utc,
            "article_id": article_id,
            "headline": headline,
            "text_input_type": "v2tone",
            "sentiment_score": score,
            "sentiment_label": label,
            "sentiment_prob_pos": prob_pos,
            "sentiment_prob_neg": prob_neg,
            "sentiment_prob_neutral": prob_neutral,
            "document_type": "article",
            "speaker": None,
            "source": self.SOURCE_NAME,
            "url": url,
        }

    def _parse_v2tone(self, tone_raw) -> float:
        """Parse Bronze 'tone' field to an overall tone float.

        Existing Bronze JSONL may store V2Tone as a raw comma-separated string
        (e.g. "-2.42,2.34,...") from before the collector fix.  After the fix,
        the collector writes a float directly.  Both forms are handled here.

        Args:
            tone_raw: Raw tone value (float, int, str, or None).

        Returns:
            Overall tone score as float, or 0.0 on any parse failure.
        """
        if tone_raw is None:
            return 0.0
        if isinstance(tone_raw, (int, float)):
            return float(tone_raw)
        try:
            return float(str(tone_raw).split(",")[0])
        except (ValueError, IndexError):
            self.logger.warning("Cannot parse tone value: %r — defaulting to 0.0", tone_raw)
            return 0.0

    def _score_to_label(self, score: float) -> str:
        """Map a sentiment score to a categorical label.

        Args:
            score: Sentiment score in [-1.0, 1.0].

        Returns:
            "positive", "negative", or "neutral".
        """
        if score > self.NEUTRAL_THRESHOLD:
            return "positive"
        if score < -self.NEUTRAL_THRESHOLD:
            return "negative"
        return "neutral"

    def _score_to_probs(self, label: str) -> tuple[float, float, float]:
        """Return 0/1 probability proxy values for a sentiment label.

        These are NOT calibrated model probabilities.  They are deterministic
        proxies that assign 1.0 to the winning class and 0.0 to the others,
        satisfying the Silver schema's probability sum ≈ 1.0 constraint.

        Args:
            label: One of "positive", "negative", "neutral".

        Returns:
            (prob_pos, prob_neg, prob_neutral) tuple of floats.
        """
        if label == "positive":
            return (1.0, 0.0, 0.0)
        if label == "negative":
            return (0.0, 1.0, 0.0)
        return (0.0, 0.0, 1.0)
