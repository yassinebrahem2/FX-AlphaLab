"""Tests for NewsPreprocessor."""

import json
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from src.ingestion.preprocessors.news_preprocessor import NewsPreprocessor


class TestNewsPreprocessor:
    """Test suite for NewsPreprocessor."""

    @pytest.fixture
    def sample_documents(self) -> list[dict]:
        """Sample Bronze documents for testing."""
        return [
            {
                "source": "fed",
                "timestamp_collected": "2026-02-12T10:00:00Z",
                "timestamp_published": "2026-02-12T09:00:00Z",
                "url": "https://www.federalreserve.gov/newsevents/pressreleases/statement.htm",
                "title": "Federal Reserve announces rate decision",
                "content": "The Federal Reserve decided to maintain interest rates.",
                "document_type": "statement",
                "speaker": "Jerome Powell",
                "metadata": {"category": "monetary_policy"},
            },
            {
                "source": "ecb",
                "timestamp_collected": "2026-02-12T14:00:00Z",
                "timestamp_published": "2026-02-12T13:00:00Z",
                "url": "https://www.ecb.europa.eu/press/pr/date/2026/html/speech.en.html",
                "title": "ECB President discusses monetary policy outlook",
                "content": "Christine Lagarde addressed concerns about policy.",
                "document_type": "speech",
                "speaker": "Christine Lagarde",
                "metadata": {"author": "Christine Lagarde"},
            },
            {
                "source": "boe",
                "timestamp_collected": "2026-02-12T16:00:00Z",
                "timestamp_published": "2026-02-12T15:30:00Z",
                "url": "https://www.bankofengland.co.uk/news/2026/statement",
                "title": "Sterling rallies on positive GDP data",
                "content": "The British pound rose sharply.",
                "document_type": "article",
                "speaker": None,
                "metadata": {},
            },
        ]

    @pytest.fixture
    def mock_sentiment_results(self) -> list[dict]:
        """Default mock sentiment results for 3 documents."""
        return [
            {"label": "neutral", "score": 0.82},
            {"label": "positive", "score": 0.75},
            {"label": "positive", "score": 0.91},
        ]

    @pytest.fixture
    def preprocessor(self, tmp_path: Path) -> NewsPreprocessor:
        """NewsPreprocessor instance with temp directories."""
        input_dir = tmp_path / "raw" / "news"
        output_dir = tmp_path / "processed" / "sentiment"
        input_dir.mkdir(parents=True)
        output_dir.mkdir(parents=True)

        with (
            patch("src.ingestion.preprocessors.news_preprocessor.pipeline") as mock_pipeline,
            patch("src.ingestion.preprocessors.news_preprocessor.torch") as mock_torch,
        ):
            mock_torch.cuda.is_available.return_value = False
            mock_model = Mock()
            mock_pipeline.return_value = mock_model
            preprocessor = NewsPreprocessor(input_dir=input_dir, output_dir=output_dir)
            preprocessor._mock_model = mock_model
            return preprocessor

    @pytest.fixture
    def setup_bronze_data(
        self, preprocessor: NewsPreprocessor, sample_documents: list[dict]
    ) -> Path:
        """Create sample Bronze JSONL file."""
        bronze_dir = preprocessor.input_dir / "fed"
        bronze_dir.mkdir(parents=True, exist_ok=True)
        jsonl_path = bronze_dir / "statements_20260212.jsonl"

        with open(jsonl_path, "w", encoding="utf-8") as f:
            for doc in sample_documents:
                f.write(json.dumps(doc) + "\n")

        return jsonl_path

    def test_initialization(self, preprocessor: NewsPreprocessor):
        """Test NewsPreprocessor initializes correctly."""
        assert preprocessor.SOURCE_CURRENCY_MAP == {"fed": "USD", "ecb": "EUR", "boe": "GBP"}
        assert preprocessor.sentiment_model is not None
        assert preprocessor.input_dir.exists()
        assert preprocessor.output_dir.exists()

    def test_read_jsonl(self, preprocessor: NewsPreprocessor, setup_bronze_data: Path):
        """Test reading JSONL file."""
        documents = preprocessor.read_jsonl(setup_bronze_data)
        assert len(documents) == 3
        assert all(isinstance(doc, dict) for doc in documents)
        assert documents[0]["source"] == "fed"

    def test_read_jsonl_missing_file(self, preprocessor: NewsPreprocessor, tmp_path: Path):
        """Test reading nonexistent JSONL file raises error."""
        with pytest.raises(ValueError, match="JSONL file not found"):
            preprocessor.read_jsonl(tmp_path / "nonexistent.jsonl")

    def test_clean_text(self, preprocessor: NewsPreprocessor):
        """Test text cleaning."""
        dirty_text = "  Multiple   spaces\xa0 and\u200b artifacts  "
        clean = preprocessor.clean_text(dirty_text)
        assert clean == "Multiple spaces and artifacts"

    def test_generate_article_id(self, preprocessor: NewsPreprocessor):
        """Test article ID generation."""
        # With URL
        id1 = preprocessor.generate_article_id(
            url="https://example.com/article1",
            title="Title",
            timestamp="2026-02-12T10:00:00Z",
            source="fed",
        )
        assert len(id1) == 16
        assert isinstance(id1, str)

        # Without URL
        id2 = preprocessor.generate_article_id(
            url=None,
            title="Title",
            timestamp="2026-02-12T10:00:00Z",
            source="fed",
        )
        assert len(id2) == 16

        # Same URL should produce same ID
        id3 = preprocessor.generate_article_id(
            url="https://example.com/article1",
            title="Different Title",
            timestamp="2026-02-12T11:00:00Z",
            source="ecb",
        )
        assert id1 == id3

    def test_analyze_sentiment_batch(self, preprocessor: NewsPreprocessor):
        """Test FinBERT batch sentiment analysis."""
        # Configure mock to return batch results
        preprocessor._mock_model.return_value = [
            {"label": "positive", "score": 0.95},
            {"label": "negative", "score": 0.88},
            {"label": "neutral", "score": 0.75},
        ]

        texts = [
            "The economy is performing fantastically well!",
            "Recession risks are increasing sharply.",
            "The central bank announced a meeting date.",
        ]
        scores, labels = preprocessor._analyze_sentiment_batch(texts)

        assert len(scores) == 3
        assert len(labels) == 3
        assert scores[0] == 0.95
        assert labels[0] == "positive"
        assert scores[1] == -0.88
        assert labels[1] == "negative"
        assert scores[2] == 0.0
        assert labels[2] == "neutral"

    def test_analyze_sentiment_batch_empty_texts(self, preprocessor: NewsPreprocessor):
        """Test batch sentiment handles empty strings."""
        preprocessor._mock_model.return_value = [
            {"label": "positive", "score": 0.9},
        ]

        scores, labels = preprocessor._analyze_sentiment_batch(["", "Some headline", ""])

        assert scores[0] == 0.0
        assert labels[0] == "neutral"
        assert scores[1] == 0.9
        assert labels[1] == "positive"
        assert scores[2] == 0.0
        assert labels[2] == "neutral"

    def test_extract_metadata(self, preprocessor: NewsPreprocessor, sample_documents: list[dict]):
        """Test extracting metadata from a Bronze document."""
        doc = sample_documents[0]
        record = preprocessor._extract_metadata(doc)

        assert record["timestamp_utc"] == "2026-02-12T09:00:00Z"
        assert isinstance(record["article_id"], str)
        assert len(record["article_id"]) == 16
        assert record["headline"] == "Federal Reserve announces rate decision"
        assert record["currency"] == "USD"
        assert record["document_type"] == "statement"
        assert record["speaker"] == "Jerome Powell"
        assert record["source"] == "fed"
        assert "sentiment_score" not in record

    def test_currency_mapping(self, preprocessor: NewsPreprocessor, sample_documents: list[dict]):
        """Test source → currency mapping for all sources."""
        for doc in sample_documents:
            record = preprocessor._extract_metadata(doc)
            expected = preprocessor.SOURCE_CURRENCY_MAP.get(doc["source"].lower(), "OTHER")
            assert record["currency"] == expected

    def test_source_case_normalization(self, preprocessor: NewsPreprocessor):
        """Test that mixed-case source names are normalized to lowercase."""
        doc = {
            "source": "BoE",
            "timestamp_collected": "2026-02-12T10:00:00Z",
            "title": "Bank of England statement",
        }
        record = preprocessor._extract_metadata(doc)
        assert record["source"] == "boe"
        assert record["currency"] == "GBP"

    def test_preprocess_full_pipeline(
        self,
        preprocessor: NewsPreprocessor,
        setup_bronze_data: Path,
        mock_sentiment_results: list[dict],
    ):
        """Test full preprocessing pipeline."""
        preprocessor._mock_model.return_value = mock_sentiment_results

        df = preprocessor.preprocess()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3

        # Check all required columns present
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
        assert all(col in df.columns for col in required_columns)

        # Check data types
        assert df["sentiment_score"].between(-1.0, 1.0).all()
        assert df["sentiment_label"].isin(["positive", "neutral", "negative"]).all()

    def test_validate_schema(self, preprocessor: NewsPreprocessor):
        """Test schema validation."""
        df = pd.DataFrame(
            [
                {
                    "timestamp_utc": "2026-02-12T10:00:00Z",
                    "article_id": "abc123def4567890",
                    "currency": "USD",
                    "headline": "Test headline",
                    "sentiment_score": 0.5,
                    "sentiment_label": "positive",
                    "document_type": "statement",
                    "speaker": "Jerome Powell",
                    "source": "fed",
                    "url": "https://example.com",
                }
            ]
        )
        assert preprocessor.validate(df) is True

    def test_validate_schema_missing_columns(self, preprocessor: NewsPreprocessor):
        """Test validation fails with missing columns."""
        df = pd.DataFrame([{"timestamp_utc": "2026-02-12T10:00:00Z"}])
        with pytest.raises(ValueError, match="Missing required columns"):
            preprocessor.validate(df)

    def test_validate_schema_null_values(self, preprocessor: NewsPreprocessor):
        """Test validation fails with null critical fields."""
        df = pd.DataFrame(
            [
                {
                    "timestamp_utc": None,
                    "article_id": "abc123def4567890",
                    "currency": "USD",
                    "headline": "Test",
                    "sentiment_score": 0.5,
                    "sentiment_label": "positive",
                    "document_type": "statement",
                    "speaker": None,
                    "source": "fed",
                    "url": "https://example.com",
                }
            ]
        )
        with pytest.raises(ValueError, match="Null values in critical field"):
            preprocessor.validate(df)

    def test_validate_schema_invalid_sentiment_score(self, preprocessor: NewsPreprocessor):
        """Test validation fails with invalid sentiment score."""
        df = pd.DataFrame(
            [
                {
                    "timestamp_utc": "2026-02-12T10:00:00Z",
                    "article_id": "abc123def4567890",
                    "currency": "USD",
                    "headline": "Test",
                    "sentiment_score": 1.5,
                    "sentiment_label": "positive",
                    "document_type": "statement",
                    "speaker": "Jerome Powell",
                    "source": "fed",
                    "url": "https://example.com",
                }
            ]
        )
        with pytest.raises(ValueError, match="sentiment_score outside"):
            preprocessor.validate(df)

    def test_validate_schema_invalid_sentiment_label(self, preprocessor: NewsPreprocessor):
        """Test validation fails with invalid sentiment label."""
        df = pd.DataFrame(
            [
                {
                    "timestamp_utc": "2026-02-12T10:00:00Z",
                    "article_id": "abc123def4567890",
                    "currency": "USD",
                    "headline": "Test",
                    "sentiment_score": 0.5,
                    "sentiment_label": "very_positive",
                    "document_type": "statement",
                    "speaker": "Jerome Powell",
                    "source": "fed",
                    "url": "https://example.com",
                }
            ]
        )
        with pytest.raises(ValueError, match="Invalid sentiment_label"):
            preprocessor.validate(df)

    def test_validate_schema_duplicate_article_ids(self, preprocessor: NewsPreprocessor):
        """Test validation fails with duplicate article IDs."""
        df = pd.DataFrame(
            [
                {
                    "timestamp_utc": "2026-02-12T10:00:00Z",
                    "article_id": "abc123def4567890",
                    "currency": "USD",
                    "headline": "Test 1",
                    "sentiment_score": 0.5,
                    "sentiment_label": "positive",
                    "document_type": "statement",
                    "speaker": "Jerome Powell",
                    "source": "fed",
                    "url": "https://example.com/1",
                },
                {
                    "timestamp_utc": "2026-02-12T11:00:00Z",
                    "article_id": "abc123def4567890",
                    "currency": "GBP",
                    "headline": "Test 2",
                    "sentiment_score": -0.3,
                    "sentiment_label": "negative",
                    "document_type": "speech",
                    "speaker": "Andrew Bailey",
                    "source": "boe",
                    "url": "https://example.com/2",
                },
            ]
        )
        with pytest.raises(ValueError, match="Duplicate article_id"):
            preprocessor.validate(df)

    def test_export_partitioned(self, preprocessor: NewsPreprocessor):
        """Test exporting to partitioned Parquet."""
        df = pd.DataFrame(
            [
                {
                    "timestamp_utc": "2026-02-12T10:00:00Z",
                    "article_id": "abc123def4567890",
                    "currency": "USD",
                    "headline": "Test headline",
                    "sentiment_score": 0.5,
                    "sentiment_label": "positive",
                    "document_type": "statement",
                    "speaker": "Jerome Powell",
                    "source": "fed",
                    "url": "https://example.com",
                }
            ]
        )

        output_paths = preprocessor.export_partitioned(df)

        assert len(output_paths) == 1
        partition_key = list(output_paths.keys())[0]
        assert "source=fed" in partition_key
        assert "year=2026" in partition_key
        assert "month=02" in partition_key

        path = output_paths[partition_key]
        assert path.exists()
        assert path.name == "sentiment_cleaned.parquet"

        # Read back and verify
        df_read = pd.read_parquet(path)
        assert len(df_read) == 1
        assert df_read["headline"].iloc[0] == "Test headline"
