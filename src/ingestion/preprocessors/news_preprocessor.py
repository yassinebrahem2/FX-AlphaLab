"""News preprocessor for transforming Bronze news documents to Silver sentiment data.

Handles news from all DocumentCollector sources (Fed, ECB News, BoE):
- Read JSONL from data/raw/news/{source}/
- Apply sentiment analysis (FinBERT - BERT fine-tuned on financial text)
- Fan out each document to affected FX pairs with direction multipliers
- Generate article IDs
- Map to Silver sentiment schema
- Export to data/processed/sentiment/ (partitioned by source/year/month)

Silver Schema (14 fields):
    - timestamp_utc: Publication timestamp in UTC (ISO 8601)
    - article_id: Unique 16-char hash of URL or title+timestamp
    - pair: FX pair affected (e.g., "EURUSD", "GBPJPY")
    - direction_multiplier: +1 if source currency is base, -1 if quote
    - headline: Article title
    - text_input_type: "headline_body" or "headline_only"
    - sentiment_score: Float [-1.0, 1.0] = P(positive) - P(negative)
    - sentiment_label: Categorical ["positive", "neutral", "negative"]
    - sentiment_prob_pos: P(positive) from FinBERT [0.0, 1.0]
    - sentiment_prob_neg: P(negative) from FinBERT [0.0, 1.0]
    - sentiment_prob_neutral: P(neutral) from FinBERT [0.0, 1.0]
    - document_type: Type of document (e.g., "statement", "speech", "press_release")
    - speaker: Speaker/author if applicable (e.g., "Jerome Powell", "Christine Lagarde")
    - source: Data source identifier (e.g., "fed", "ecb", "boe")
    - url: Original article URL
"""

from datetime import datetime
from pathlib import Path

import pandas as pd
import torch
from transformers import pipeline

from src.ingestion.preprocessors.document_preprocessor import DocumentPreprocessor


class NewsPreprocessor(DocumentPreprocessor):
    """Preprocessor for news articles from all document sources.

    Uses FinBERT for sentiment analysis (BERT fine-tuned on financial text).
    Maps each institutional source to affected FX pairs with direction multipliers.
    Fans out each document to multiple rows (one per affected pair).
    """

    # Source → list of (pair, direction_multiplier) tuples
    # direction_multiplier: +1 if source currency is base, -1 if quote
    SOURCE_PAIRS_MAP: dict[str, list[tuple[str, int]]] = {
        "fed": [  # USD
            ("EURUSD", -1),  # USD is quote
            ("GBPUSD", -1),  # USD is quote
            ("USDJPY", +1),  # USD is base
            ("USDCHF", +1),  # USD is base
            ("USDCAD", +1),  # USD is base
            ("AUDUSD", -1),  # USD is quote
            ("NZDUSD", -1),  # USD is quote
        ],
        "ecb": [  # EUR
            ("EURUSD", +1),  # EUR is base
            ("EURGBP", +1),  # EUR is base
            ("EURJPY", +1),  # EUR is base
        ],
        "boe": [  # GBP
            ("GBPUSD", +1),  # GBP is base
            ("EURGBP", -1),  # GBP is quote
            ("GBPJPY", +1),  # GBP is base
        ],
    }

    def __init__(
        self,
        input_dir: Path,
        output_dir: Path,
        log_file: Path | None = None,
    ) -> None:
        """Initialize NewsPreprocessor with FinBERT sentiment analyzer.

        Args:
            input_dir: Directory containing Bronze JSONL files (data/raw/news/).
            output_dir: Directory for Silver Parquet output (data/processed/sentiment/).
            log_file: Optional path for file-based logging.
        """
        super().__init__(input_dir, output_dir, log_file)

        # Auto-detect GPU availability
        device = 0 if torch.cuda.is_available() else -1
        device_name = "GPU" if device == 0 else "CPU"
        self.logger.info("Loading FinBERT sentiment model on %s...", device_name)

        self.sentiment_model = pipeline(
            "sentiment-analysis",
            model="ProsusAI/finbert",
            tokenizer="ProsusAI/finbert",
            device=device,
        )
        self.logger.info("FinBERT model loaded successfully on %s", device_name)

    def preprocess(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        source: str | None = None,
    ) -> pd.DataFrame:
        """Transform Bronze news documents to Silver sentiment DataFrame.

        Args:
            start_date: Start date for filtering (optional).
            end_date: End date for filtering (optional).
            source: Specific source to process (e.g., "fed", "ecb"). If None, all sources.

        Returns:
            DataFrame with processed sentiment records (with pair fan-out).

        Raises:
            ValueError: If no JSONL files found or all files are invalid.
        """
        self.logger.info(
            "Starting preprocessing: source=%s, start=%s, end=%s",
            source or "all",
            start_date,
            end_date,
        )

        # Find all JSONL files
        jsonl_files = self.find_jsonl_files(source=source)
        if not jsonl_files:
            raise ValueError(f"No JSONL files found in {self.input_dir}")

        # Process all documents
        all_records = []
        for file_path in jsonl_files:
            try:
                documents = self.read_jsonl(file_path)
                records = self._process_documents(documents)
                all_records.extend(records)
            except Exception as e:
                self.logger.error("Failed to process %s: %s", file_path, e)
                continue

        if not all_records:
            raise ValueError("No valid records extracted from JSONL files")

        # Create DataFrame
        df = pd.DataFrame(all_records)

        # Filter by date if provided
        if start_date or end_date:
            df["_timestamp"] = pd.to_datetime(df["timestamp_utc"])
            if start_date:
                df = df[df["_timestamp"] >= start_date]
            if end_date:
                df = df[df["_timestamp"] <= end_date]
            df = df.drop(columns=["_timestamp"])

        # Remove duplicates by (article_id, pair) composite key
        initial_count = len(df)
        df = df.drop_duplicates(subset=["article_id", "pair"], keep="first")
        if len(df) < initial_count:
            self.logger.info(
                "Removed %d duplicate (article_id, pair) records", initial_count - len(df)
            )

        # Sort by timestamp, then pair
        df = df.sort_values(["timestamp_utc", "pair"]).reset_index(drop=True)

        # Validate schema
        self.validate(df)

        self.logger.info("Preprocessed %d sentiment records (after pair fan-out)", len(df))
        return df

    def _process_documents(self, documents: list[dict]) -> list[dict]:
        """Process list of Bronze documents to Silver records with batch sentiment.

        Extracts metadata first, builds combined text (headline + body[:400]),
        scores all texts in a single batch, then fans out to multiple pairs.

        Args:
            documents: List of document dictionaries from JSONL.

        Returns:
            List of Silver sentiment records (one per document per affected pair).
        """
        # Phase 1: Extract metadata (partial records without pair info)
        prepared = []
        for doc in documents:
            try:
                prepared.append(self._extract_metadata(doc))
            except Exception as e:
                self.logger.warning(
                    "Failed to extract metadata from '%s': %s",
                    doc.get("title", "unknown"),
                    e,
                )
                continue

        if not prepared:
            return []

        # Phase 2: Build scoring texts (headline + body[:400] when available)
        texts = []
        for rec in prepared:
            content = rec.get("_content", "")
            if content:
                text = rec["headline"] + ". " + content[:400]
                rec["text_input_type"] = "headline_body"
            else:
                text = rec["headline"]
                rec["text_input_type"] = "headline_only"
            texts.append(text)

        # Phase 3: Batch sentiment scoring
        scores, labels, prob_pos, prob_neg, prob_neutral = self._analyze_sentiment_batch(texts)

        # Phase 4: Merge sentiment results and remove temporary _content field
        for rec, score, label, p_pos, p_neg, p_neut in zip(
            prepared, scores, labels, prob_pos, prob_neg, prob_neutral
        ):
            rec["sentiment_score"] = score
            rec["sentiment_label"] = label
            rec["sentiment_prob_pos"] = p_pos
            rec["sentiment_prob_neg"] = p_neg
            rec["sentiment_prob_neutral"] = p_neut
            rec.pop("_content", None)  # Remove temporary field

        # Phase 5: Fan out to pairs
        fanned_out = []
        for rec in prepared:
            source = rec["source"]
            pairs = self.SOURCE_PAIRS_MAP.get(source, [])
            if not pairs:
                self.logger.warning("No pairs mapped for source '%s', skipping fan-out", source)
                continue

            for pair, direction_multiplier in pairs:
                row = rec.copy()
                row["pair"] = pair
                row["direction_multiplier"] = direction_multiplier
                fanned_out.append(row)

        return fanned_out

    def _extract_metadata(self, doc: dict) -> dict:
        """Extract and normalize metadata from a Bronze document.

        Args:
            doc: Bronze document dictionary.

        Returns:
            Partial Silver record (without sentiment fields or pair info).
            Includes temporary _content field for later use in text building.

        Raises:
            KeyError: If required Bronze fields are missing.
        """
        title = doc["title"]
        timestamp_raw = doc.get("timestamp_published") or doc["timestamp_collected"]
        source = doc["source"].lower()  # Normalize to lowercase (BoE → boe)
        url = doc.get("url", "")
        document_type = doc.get("document_type", "article")
        speaker = doc.get("speaker") or doc.get("metadata", {}).get("author")
        content = doc.get("content") or doc.get("body") or ""

        # Normalize timestamp to UTC ISO 8601 (Silver contract requirement)
        timestamp = pd.to_datetime(timestamp_raw, utc=True).strftime("%Y-%m-%dT%H:%M:%SZ")

        # Clean text
        title_clean = self.clean_text(title)

        # Generate article ID
        article_id = self.generate_article_id(url, title_clean, timestamp, source)

        return {
            "timestamp_utc": timestamp,
            "article_id": article_id,
            "headline": title_clean,
            "document_type": document_type,
            "speaker": speaker if speaker else None,
            "source": source,
            "url": url if url else None,
            "_content": content,  # Temporary field for text building
        }

    def _analyze_sentiment_batch(
        self, texts: list[str]
    ) -> tuple[list[float], list[str], list[float], list[float], list[float]]:
        """Analyze sentiment for a batch of texts using FinBERT.

        Processes all texts in a single model call for efficient GPU/CPU utilization.
        Extracts all three probability outputs (positive, neutral, negative).

        Args:
            texts: List of text strings to analyze.

        Returns:
            Tuple of (scores, labels, prob_pos, prob_neg, prob_neutral) lists.
            - scores: Float in [-1.0, 1.0] = P(pos) - P(neg)
            - labels: "positive", "neutral", or "negative"
            - prob_pos: P(positive) from FinBERT [0.0, 1.0]
            - prob_neg: P(negative) from FinBERT [0.0, 1.0]
            - prob_neutral: P(neutral) from FinBERT [0.0, 1.0]
        """
        scores = []
        labels = []
        prob_pos_list = []
        prob_neg_list = []
        prob_neutral_list = []

        # Separate empty texts (skip model) from non-empty (batch score)
        non_empty_indices = [i for i, t in enumerate(texts) if t]
        non_empty_texts = [texts[i] for i in non_empty_indices]

        # Initialize all as neutral with zero probabilities
        for _ in texts:
            scores.append(0.0)
            labels.append("neutral")
            prob_pos_list.append(0.0)
            prob_neg_list.append(1.0)  # Default neutral has P(neutral)=1.0
            prob_neutral_list.append(0.0)

        if not non_empty_texts:
            return scores, labels, prob_pos_list, prob_neg_list, prob_neutral_list

        try:
            # Request top_k=3 to get all three label probabilities
            results = self.sentiment_model(
                non_empty_texts, truncation=True, max_length=512, batch_size=32, top_k=3
            )

            for idx, result_list in zip(non_empty_indices, results):
                # result_list is a list of 3 dicts: [{"label": "positive", "score": X}, ...]
                # Build probability dict
                probs = {item["label"].lower(): item["score"] for item in result_list}

                p_pos = probs.get("positive", 0.0)
                p_neg = probs.get("negative", 0.0)
                p_neut = probs.get("neutral", 0.0)

                # Determine top label
                top_label = max(probs, key=probs.get)

                # Score = P(pos) - P(neg)
                score = p_pos - p_neg

                scores[idx] = round(score, 4)
                labels[idx] = top_label
                prob_pos_list[idx] = round(p_pos, 4)
                prob_neg_list[idx] = round(p_neg, 4)
                prob_neutral_list[idx] = round(p_neut, 4)

        except Exception as e:
            self.logger.warning("Batch sentiment analysis failed: %s", e)

        return scores, labels, prob_pos_list, prob_neg_list, prob_neutral_list

    def validate(self, df: pd.DataFrame) -> bool:
        """Validate Silver sentiment schema.

        Args:
            df: DataFrame to validate.

        Returns:
            True if valid.

        Raises:
            ValueError: If schema validation fails with details.
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

        # Check all columns present
        missing = set(required_columns) - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        # Check no nulls in critical fields
        critical_fields = [
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
        for field in critical_fields:
            if df[field].isna().any():
                null_count = df[field].isna().sum()
                raise ValueError(f"Null values in critical field '{field}': {null_count}")

        # Check sentiment_score range
        if not df["sentiment_score"].between(-1.0, 1.0).all():
            invalid = df[~df["sentiment_score"].between(-1.0, 1.0)]
            raise ValueError(f"sentiment_score outside [-1.0, 1.0]: {len(invalid)} records")

        # Check sentiment_label valid
        valid_labels = {"positive", "neutral", "negative"}
        invalid_labels = set(df["sentiment_label"].unique()) - valid_labels
        if invalid_labels:
            raise ValueError(f"Invalid sentiment_label values: {invalid_labels}")

        # Check direction_multiplier valid
        valid_multipliers = {-1, 1}
        invalid_multipliers = set(df["direction_multiplier"].unique()) - valid_multipliers
        if invalid_multipliers:
            raise ValueError(f"Invalid direction_multiplier values: {invalid_multipliers}")

        # Check pair format (6-character string)
        invalid_pairs = df[df["pair"].str.len() != 6]
        if len(invalid_pairs) > 0:
            raise ValueError(f"Invalid pair format (must be 6 chars): {len(invalid_pairs)} records")

        # Check text_input_type valid
        valid_types = {"headline_body", "headline_only"}
        invalid_types = set(df["text_input_type"].unique()) - valid_types
        if invalid_types:
            raise ValueError(f"Invalid text_input_type values: {invalid_types}")

        # Check probability sum ≈ 1.0 (within 0.01 tolerance)
        prob_sums = (
            df["sentiment_prob_pos"] + df["sentiment_prob_neg"] + df["sentiment_prob_neutral"]
        )
        invalid_sums = df[~prob_sums.between(0.99, 1.01)]
        if len(invalid_sums) > 0:
            raise ValueError(
                f"Probability sum not ≈ 1.0 (tolerance 0.01): {len(invalid_sums)} records"
            )

        # Check (article_id, pair) composite uniqueness
        duplicates = df.duplicated(subset=["article_id", "pair"]).sum()
        if duplicates > 0:
            raise ValueError(f"Duplicate (article_id, pair) found: {duplicates} duplicates")

        self.logger.info("Schema validation passed for %d records", len(df))
        return True
