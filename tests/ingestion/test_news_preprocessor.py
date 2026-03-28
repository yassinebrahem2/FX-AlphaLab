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
    def mock_sentiment_results_top_k(self) -> list[list[dict]]:
        """Mock sentiment results in top_k=3 format."""
        return [
            [
                {"label": "neutral", "score": 0.82},
                {"label": "positive", "score": 0.10},
                {"label": "negative", "score": 0.08},
            ],
            [
                {"label": "positive", "score": 0.75},
                {"label": "neutral", "score": 0.15},
                {"label": "negative", "score": 0.10},
            ],
            [
                {"label": "positive", "score": 0.91},
                {"label": "neutral", "score": 0.05},
                {"label": "negative", "score": 0.04},
            ],
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
        assert hasattr(preprocessor, "SOURCE_PAIRS_MAP")
        assert preprocessor.SOURCE_PAIRS_MAP == {
            "fed": [
                ("EURUSD", -1),
                ("GBPUSD", -1),
                ("USDJPY", +1),
                ("USDCHF", +1),
                ("USDCAD", +1),
                ("AUDUSD", -1),
                ("NZDUSD", -1),
            ],
            "ecb": [("EURUSD", +1), ("EURGBP", +1), ("EURJPY", +1)],
            "boe": [("GBPUSD", +1), ("EURGBP", -1), ("GBPJPY", +1)],
        }
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
        """Test FinBERT batch sentiment analysis with top_k=3."""
        # Configure mock to return top_k=3 format
        preprocessor._mock_model.return_value = [
            [
                {"label": "positive", "score": 0.70},
                {"label": "neutral", "score": 0.20},
                {"label": "negative", "score": 0.10},
            ],
            [
                {"label": "negative", "score": 0.60},
                {"label": "neutral", "score": 0.30},
                {"label": "positive", "score": 0.10},
            ],
            [
                {"label": "neutral", "score": 0.80},
                {"label": "positive", "score": 0.12},
                {"label": "negative", "score": 0.08},
            ],
        ]

        texts = [
            "The economy is performing fantastically well!",
            "Recession risks are increasing sharply.",
            "The central bank announced a meeting date.",
        ]
        scores, labels, prob_pos, prob_neg, prob_neutral = preprocessor._analyze_sentiment_batch(
            texts
        )

        assert len(scores) == 3
        assert len(labels) == 3
        assert len(prob_pos) == 3
        assert len(prob_neg) == 3
        assert len(prob_neutral) == 3

        # First text: P(pos)=0.70, P(neg)=0.10, score=0.70-0.10=0.60
        assert scores[0] == 0.60
        assert labels[0] == "positive"
        assert prob_pos[0] == 0.70
        assert prob_neg[0] == 0.10
        assert prob_neutral[0] == 0.20

        # Second text: P(pos)=0.10, P(neg)=0.60, score=0.10-0.60=-0.50
        assert scores[1] == -0.50
        assert labels[1] == "negative"

        # Third text: P(pos)=0.12, P(neg)=0.08, score=0.12-0.08=0.04
        assert scores[2] == 0.04
        assert labels[2] == "neutral"

    def test_analyze_sentiment_batch_empty_texts(self, preprocessor: NewsPreprocessor):
        """Test batch sentiment handles empty strings."""
        preprocessor._mock_model.return_value = [
            [
                {"label": "positive", "score": 0.80},
                {"label": "neutral", "score": 0.15},
                {"label": "negative", "score": 0.05},
            ],
        ]

        scores, labels, prob_pos, prob_neg, prob_neutral = preprocessor._analyze_sentiment_batch(
            ["", "Some headline", ""]
        )

        # Empty strings default to neutral
        assert scores[0] == 0.0
        assert labels[0] == "neutral"
        assert scores[1] == 0.75  # 0.80 - 0.05
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
        assert record["document_type"] == "statement"
        assert record["speaker"] == "Jerome Powell"
        assert record["source"] == "fed"
        assert record["_content"] == "The Federal Reserve decided to maintain interest rates."
        assert "sentiment_score" not in record
        assert "pair" not in record  # pair is added during fan-out, not extraction

    def test_content_extraction(self, preprocessor: NewsPreprocessor):
        """Test that _content field is extracted from Bronze document."""
        doc = {
            "source": "fed",
            "timestamp_collected": "2026-02-12T10:00:00Z",
            "title": "Test Title",
            "content": "Full document body text here.",
        }
        record = preprocessor._extract_metadata(doc)
        assert record["_content"] == "Full document body text here."

        # No content field
        doc_no_content = {
            "source": "fed",
            "timestamp_collected": "2026-02-12T10:00:00Z",
            "title": "Test Title",
        }
        record_no_content = preprocessor._extract_metadata(doc_no_content)
        assert record_no_content["_content"] == ""

    def test_source_case_normalization(self, preprocessor: NewsPreprocessor):
        """Test that mixed-case source names are normalized to lowercase."""
        doc = {
            "source": "BoE",
            "timestamp_collected": "2026-02-12T10:00:00Z",
            "title": "Bank of England statement",
        }
        record = preprocessor._extract_metadata(doc)
        assert record["source"] == "boe"

    def test_pair_fanout(self, preprocessor: NewsPreprocessor, sample_documents: list[dict]):
        """Test that one document fans out to multiple rows (one per affected pair)."""
        doc = sample_documents[0]  # Fed document

        # Mock sentiment to return top_k=3 format
        preprocessor._mock_model.return_value = [
            [
                {"label": "positive", "score": 0.70},
                {"label": "neutral", "score": 0.20},
                {"label": "negative", "score": 0.10},
            ],
        ]

        records = preprocessor._process_documents([doc])

        # Fed affects 7 pairs
        assert len(records) == 7

        # Check that all records have same article_id and sentiment
        article_ids = {rec["article_id"] for rec in records}
        assert len(article_ids) == 1

        # Check pairs
        pairs = [rec["pair"] for rec in records]
        assert set(pairs) == {"EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD", "NZDUSD"}

        # All records should have sentiment fields
        for rec in records:
            assert "sentiment_score" in rec
            assert "sentiment_label" in rec
            assert "sentiment_prob_pos" in rec
            assert "sentiment_prob_neg" in rec
            assert "sentiment_prob_neutral" in rec

    def test_direction_multiplier(self, preprocessor: NewsPreprocessor):
        """Test that direction_multiplier is correct for each pair."""
        doc = {
            "source": "fed",
            "timestamp_collected": "2026-02-12T10:00:00Z",
            "title": "Fed statement",
            "content": "Interest rates unchanged.",
        }

        preprocessor._mock_model.return_value = [
            [
                {"label": "neutral", "score": 0.80},
                {"label": "positive", "score": 0.10},
                {"label": "negative", "score": 0.10},
            ],
        ]

        records = preprocessor._process_documents([doc])

        # Check specific pairs
        eurusd = [r for r in records if r["pair"] == "EURUSD"][0]
        assert eurusd["direction_multiplier"] == -1  # USD is quote

        usdjpy = [r for r in records if r["pair"] == "USDJPY"][0]
        assert usdjpy["direction_multiplier"] == +1  # USD is base

    def test_confidence_scores(self, preprocessor: NewsPreprocessor):
        """Test that all three FinBERT probabilities are stored."""
        doc = {
            "source": "fed",
            "timestamp_collected": "2026-02-12T10:00:00Z",
            "title": "Test",
        }

        preprocessor._mock_model.return_value = [
            [
                {"label": "positive", "score": 0.60},
                {"label": "neutral", "score": 0.25},
                {"label": "negative", "score": 0.15},
            ],
        ]

        records = preprocessor._process_documents([doc])

        for rec in records:
            assert rec["sentiment_prob_pos"] == 0.60
            assert rec["sentiment_prob_neg"] == 0.15
            assert rec["sentiment_prob_neutral"] == 0.25
            # Check sum ≈ 1.0
            prob_sum = (
                rec["sentiment_prob_pos"]
                + rec["sentiment_prob_neg"]
                + rec["sentiment_prob_neutral"]
            )
            assert 0.99 <= prob_sum <= 1.01

    def test_score_formula(self, preprocessor: NewsPreprocessor):
        """Test that sentiment_score = P(pos) - P(neg), not confidence × sign(label)."""
        doc = {
            "source": "fed",
            "timestamp_collected": "2026-02-12T10:00:00Z",
            "title": "Test",
        }

        preprocessor._mock_model.return_value = [
            [
                {"label": "positive", "score": 0.70},
                {"label": "neutral", "score": 0.20},
                {"label": "negative", "score": 0.10},
            ],
        ]

        records = preprocessor._process_documents([doc])

        for rec in records:
            # score = P(pos) - P(neg) = 0.70 - 0.10 = 0.60
            assert rec["sentiment_score"] == 0.60

    def test_text_input_type(self, preprocessor: NewsPreprocessor):
        """Test text_input_type field based on content availability."""
        # Document with content
        doc_with_content = {
            "source": "fed",
            "timestamp_collected": "2026-02-12T10:00:00Z",
            "title": "Test Title",
            "content": "Full body text here.",
        }

        # Document without content
        doc_no_content = {
            "source": "ecb",
            "timestamp_collected": "2026-02-12T10:00:00Z",
            "title": "Test Title",
        }

        preprocessor._mock_model.return_value = [
            [
                {"label": "neutral", "score": 0.80},
                {"label": "positive", "score": 0.10},
                {"label": "negative", "score": 0.10},
            ],
        ] * 2

        records = preprocessor._process_documents([doc_with_content, doc_no_content])

        # Fed records should have "headline_body"
        fed_records = [r for r in records if r["source"] == "fed"]
        for rec in fed_records:
            assert rec["text_input_type"] == "headline_body"

        # ECB records should have "headline_only"
        ecb_records = [r for r in records if r["source"] == "ecb"]
        for rec in ecb_records:
            assert rec["text_input_type"] == "headline_only"

    def test_preprocess_full_pipeline(
        self,
        preprocessor: NewsPreprocessor,
        setup_bronze_data: Path,
        mock_sentiment_results_top_k: list[list[dict]],
    ):
        """Test full preprocessing pipeline with pair fan-out."""
        preprocessor._mock_model.return_value = mock_sentiment_results_top_k

        df = preprocessor.preprocess()

        assert isinstance(df, pd.DataFrame)
        # 3 documents: Fed (7 pairs) + ECB (3 pairs) + BoE (3 pairs) = 13 rows
        assert len(df) == 13

        # Check all required columns present
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
        assert all(col in df.columns for col in required_columns)

        # Check data types
        assert df["sentiment_score"].between(-1.0, 1.0).all()
        assert df["sentiment_label"].isin(["positive", "neutral", "negative"]).all()
        assert df["direction_multiplier"].isin([-1, 1]).all()
        assert df["pair"].str.len().eq(6).all()

    def test_validate_schema(self, preprocessor: NewsPreprocessor):
        """Test schema validation with new fields."""
        df = pd.DataFrame(
            [
                {
                    "timestamp_utc": "2026-02-12T10:00:00Z",
                    "article_id": "abc123def4567890",
                    "pair": "EURUSD",
                    "direction_multiplier": -1,
                    "headline": "Test headline",
                    "text_input_type": "headline_body",
                    "sentiment_score": 0.5,
                    "sentiment_label": "positive",
                    "sentiment_prob_pos": 0.70,
                    "sentiment_prob_neg": 0.20,
                    "sentiment_prob_neutral": 0.10,
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
                    "pair": "EURUSD",
                    "direction_multiplier": -1,
                    "headline": "Test",
                    "text_input_type": "headline_only",
                    "sentiment_score": 0.5,
                    "sentiment_label": "positive",
                    "sentiment_prob_pos": 0.70,
                    "sentiment_prob_neg": 0.20,
                    "sentiment_prob_neutral": 0.10,
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
                    "pair": "EURUSD",
                    "direction_multiplier": -1,
                    "headline": "Test",
                    "text_input_type": "headline_only",
                    "sentiment_score": 1.5,
                    "sentiment_label": "positive",
                    "sentiment_prob_pos": 0.70,
                    "sentiment_prob_neg": 0.20,
                    "sentiment_prob_neutral": 0.10,
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
                    "pair": "EURUSD",
                    "direction_multiplier": -1,
                    "headline": "Test",
                    "text_input_type": "headline_only",
                    "sentiment_score": 0.5,
                    "sentiment_label": "very_positive",
                    "sentiment_prob_pos": 0.70,
                    "sentiment_prob_neg": 0.20,
                    "sentiment_prob_neutral": 0.10,
                    "document_type": "statement",
                    "speaker": "Jerome Powell",
                    "source": "fed",
                    "url": "https://example.com",
                }
            ]
        )
        with pytest.raises(ValueError, match="Invalid sentiment_label"):
            preprocessor.validate(df)

    def test_validate_schema_invalid_direction_multiplier(self, preprocessor: NewsPreprocessor):
        """Test validation fails with invalid direction_multiplier."""
        df = pd.DataFrame(
            [
                {
                    "timestamp_utc": "2026-02-12T10:00:00Z",
                    "article_id": "abc123def4567890",
                    "pair": "EURUSD",
                    "direction_multiplier": 0,  # Invalid
                    "headline": "Test",
                    "text_input_type": "headline_only",
                    "sentiment_score": 0.5,
                    "sentiment_label": "positive",
                    "sentiment_prob_pos": 0.70,
                    "sentiment_prob_neg": 0.20,
                    "sentiment_prob_neutral": 0.10,
                    "document_type": "statement",
                    "speaker": "Jerome Powell",
                    "source": "fed",
                    "url": "https://example.com",
                }
            ]
        )
        with pytest.raises(ValueError, match="Invalid direction_multiplier"):
            preprocessor.validate(df)

    def test_validate_schema_invalid_pair_format(self, preprocessor: NewsPreprocessor):
        """Test validation fails with invalid pair format."""
        df = pd.DataFrame(
            [
                {
                    "timestamp_utc": "2026-02-12T10:00:00Z",
                    "article_id": "abc123def4567890",
                    "pair": "EUR",  # Too short
                    "direction_multiplier": -1,
                    "headline": "Test",
                    "text_input_type": "headline_only",
                    "sentiment_score": 0.5,
                    "sentiment_label": "positive",
                    "sentiment_prob_pos": 0.70,
                    "sentiment_prob_neg": 0.20,
                    "sentiment_prob_neutral": 0.10,
                    "document_type": "statement",
                    "speaker": "Jerome Powell",
                    "source": "fed",
                    "url": "https://example.com",
                }
            ]
        )
        with pytest.raises(ValueError, match="Invalid pair format"):
            preprocessor.validate(df)

    def test_validate_schema_invalid_probability_sum(self, preprocessor: NewsPreprocessor):
        """Test validation fails when probabilities don't sum to ~1.0."""
        df = pd.DataFrame(
            [
                {
                    "timestamp_utc": "2026-02-12T10:00:00Z",
                    "article_id": "abc123def4567890",
                    "pair": "EURUSD",
                    "direction_multiplier": -1,
                    "headline": "Test",
                    "text_input_type": "headline_only",
                    "sentiment_score": 0.5,
                    "sentiment_label": "positive",
                    "sentiment_prob_pos": 0.50,
                    "sentiment_prob_neg": 0.20,
                    "sentiment_prob_neutral": 0.10,  # Sum = 0.80, not ~1.0
                    "document_type": "statement",
                    "speaker": "Jerome Powell",
                    "source": "fed",
                    "url": "https://example.com",
                }
            ]
        )
        with pytest.raises(ValueError, match="Probability sum not"):
            preprocessor.validate(df)

    def test_validate_schema_composite_uniqueness(self, preprocessor: NewsPreprocessor):
        """Test validation fails with duplicate (article_id, pair) composite key."""
        df = pd.DataFrame(
            [
                {
                    "timestamp_utc": "2026-02-12T10:00:00Z",
                    "article_id": "abc123def4567890",
                    "pair": "EURUSD",
                    "direction_multiplier": -1,
                    "headline": "Test 1",
                    "text_input_type": "headline_only",
                    "sentiment_score": 0.5,
                    "sentiment_label": "positive",
                    "sentiment_prob_pos": 0.70,
                    "sentiment_prob_neg": 0.20,
                    "sentiment_prob_neutral": 0.10,
                    "document_type": "statement",
                    "speaker": "Jerome Powell",
                    "source": "fed",
                    "url": "https://example.com/1",
                },
                {
                    "timestamp_utc": "2026-02-12T11:00:00Z",
                    "article_id": "abc123def4567890",  # Same article_id
                    "pair": "EURUSD",  # Same pair
                    "direction_multiplier": -1,
                    "headline": "Test 2",
                    "text_input_type": "headline_only",
                    "sentiment_score": -0.3,
                    "sentiment_label": "negative",
                    "sentiment_prob_pos": 0.10,
                    "sentiment_prob_neg": 0.70,
                    "sentiment_prob_neutral": 0.20,
                    "document_type": "speech",
                    "speaker": "Jerome Powell",
                    "source": "fed",
                    "url": "https://example.com/2",
                },
            ]
        )
        with pytest.raises(ValueError, match="Duplicate \\(article_id, pair\\)"):
            preprocessor.validate(df)

    def test_export_partitioned(self, preprocessor: NewsPreprocessor):
        """Test exporting to partitioned Parquet."""
        df = pd.DataFrame(
            [
                {
                    "timestamp_utc": "2026-02-12T10:00:00Z",
                    "article_id": "abc123def4567890",
                    "pair": "EURUSD",
                    "direction_multiplier": -1,
                    "headline": "Test headline",
                    "text_input_type": "headline_body",
                    "sentiment_score": 0.5,
                    "sentiment_label": "positive",
                    "sentiment_prob_pos": 0.70,
                    "sentiment_prob_neg": 0.20,
                    "sentiment_prob_neutral": 0.10,
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
        assert df_read["pair"].iloc[0] == "EURUSD"
        assert df_read["direction_multiplier"].iloc[0] == -1
