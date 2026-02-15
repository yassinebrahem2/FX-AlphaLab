"""News preprocessor for transforming Bronze news documents to Silver sentiment data.

Handles news from all DocumentCollector sources (Fed, ECB News, BoE):
- Read JSONL from data/raw/news/{source}/
- Apply sentiment analysis (FinBERT - BERT fine-tuned on financial text)
- Map source to primary currency affected
- Generate article IDs
- Map to Silver sentiment schema
- Export to data/processed/sentiment/ (partitioned by source/year/month)

Silver Schema (10 fields):
    - timestamp_utc: Publication timestamp in UTC (ISO 8601)
    - article_id: Unique 16-char hash of URL or title+timestamp
    - currency: Primary currency affected (e.g., "USD", "EUR", "GBP")
    - headline: Article title
    - sentiment_score: Float [-1.0, 1.0] from FinBERT confidence (positive=+, negative=-)
    - sentiment_label: Categorical ["positive", "neutral", "negative"]
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
    Maps source to primary currency affected (fed→USD, ecb→EUR, boe→GBP).
    Maps to standardized Silver sentiment schema.
    """

    # Source → primary currency mapping
    SOURCE_CURRENCY_MAP: dict[str, str] = {
        "fed": "USD",
        "ecb": "EUR",
        "boe": "GBP",
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
            DataFrame with processed sentiment records.

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

        # Remove duplicates by article_id
        initial_count = len(df)
        df = df.drop_duplicates(subset=["article_id"], keep="first")
        if len(df) < initial_count:
            self.logger.info("Removed %d duplicate articles", initial_count - len(df))

        # Sort by timestamp
        df = df.sort_values("timestamp_utc").reset_index(drop=True)

        # Validate schema
        self.validate(df)

        self.logger.info("Preprocessed %d news articles", len(df))
        return df

    def _process_documents(self, documents: list[dict]) -> list[dict]:
        """Process list of Bronze documents to Silver records with batch sentiment.

        Extracts metadata first, then scores all headlines in a single batch
        for efficient GPU/CPU utilization.

        Args:
            documents: List of document dictionaries from JSONL.

        Returns:
            List of Silver sentiment records.
        """
        # Phase 1: Extract metadata and clean headlines
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

        # Phase 2: Batch sentiment scoring on all headlines
        headlines = [rec["headline"] for rec in prepared]
        scores, labels = self._analyze_sentiment_batch(headlines)

        # Phase 3: Merge sentiment results into records
        for rec, score, label in zip(prepared, scores, labels):
            rec["sentiment_score"] = score
            rec["sentiment_label"] = label

        return prepared

    def _extract_metadata(self, doc: dict) -> dict:
        """Extract and normalize metadata from a Bronze document.

        Args:
            doc: Bronze document dictionary.

        Returns:
            Partial Silver record (without sentiment fields).

        Raises:
            KeyError: If required Bronze fields are missing.
        """
        title = doc["title"]
        timestamp_raw = doc.get("timestamp_published") or doc["timestamp_collected"]
        source = doc["source"]
        url = doc.get("url", "")
        document_type = doc.get("document_type", "article")
        speaker = doc.get("speaker") or doc.get("metadata", {}).get("author")

        # Normalize timestamp to UTC ISO 8601 (Silver contract requirement)
        timestamp = pd.to_datetime(timestamp_raw, utc=True).strftime("%Y-%m-%dT%H:%M:%SZ")

        # Clean text
        title_clean = self.clean_text(title)

        # Generate article ID
        article_id = self.generate_article_id(url, title_clean, timestamp, source)

        # Map source to primary currency affected
        currency = self.SOURCE_CURRENCY_MAP.get(source, "OTHER")

        return {
            "timestamp_utc": timestamp,
            "article_id": article_id,
            "currency": currency,
            "headline": title_clean,
            "document_type": document_type,
            "speaker": speaker if speaker else None,
            "source": source,
            "url": url if url else None,
        }

    def _analyze_sentiment_batch(self, texts: list[str]) -> tuple[list[float], list[str]]:
        """Analyze sentiment for a batch of texts using FinBERT.

        Processes all texts in a single model call for efficient GPU/CPU utilization.

        Args:
            texts: List of headline strings to analyze.

        Returns:
            Tuple of (scores, labels) lists, parallel to input texts.
            - scores: Float in [-1.0, 1.0] (normalized from FinBERT confidence)
            - labels: "positive", "neutral", or "negative"
        """
        scores = []
        labels = []

        # Separate empty texts (skip model) from non-empty (batch score)
        non_empty_indices = [i for i, t in enumerate(texts) if t]
        non_empty_texts = [texts[i] for i in non_empty_indices]

        # Initialize all as neutral
        for _ in texts:
            scores.append(0.0)
            labels.append("neutral")

        if not non_empty_texts:
            return scores, labels

        try:
            results = self.sentiment_model(
                non_empty_texts, truncation=True, max_length=512, batch_size=32
            )

            for idx, result in zip(non_empty_indices, results):
                label = result["label"].lower()
                confidence = result["score"]

                if label == "positive":
                    score = confidence
                elif label == "negative":
                    score = -confidence
                else:
                    score = 0.0

                scores[idx] = round(score, 4)
                labels[idx] = label

        except Exception as e:
            self.logger.warning("Batch sentiment analysis failed: %s", e)

        return scores, labels

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
            "currency",
            "headline",
            "sentiment_score",
            "sentiment_label",
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
            "currency",
            "headline",
            "sentiment_score",
            "sentiment_label",
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

        # Check article_id uniqueness
        duplicates = df["article_id"].duplicated().sum()
        if duplicates > 0:
            raise ValueError(f"Duplicate article_id found: {duplicates} duplicates")

        self.logger.info("Schema validation passed for %d records", len(df))
        return True
