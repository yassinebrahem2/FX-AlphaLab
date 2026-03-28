"""Tests for GDELTPreprocessor."""

import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from src.ingestion.preprocessors.gdelt_preprocessor import GDELTPreprocessor


class TestGDELTPreprocessor:
    """Test suite for GDELTPreprocessor."""

    # ------------------------------------------------------------------
    # Fixtures
    # ------------------------------------------------------------------

    @pytest.fixture
    def sample_documents(self) -> list[dict]:
        """Sample Bronze GDELT documents as produced by GDELTCollector."""
        return [
            {
                "source": "gdelt",
                "timestamp_collected": "2026-02-14T23:19:58",
                "timestamp_published": "2026-02-13T00:15:00Z",
                "url": "https://www.cnbc.com/2026/02/13/safe-haven-currency.html",
                "source_domain": "cnbc.com",
                "tone": -2.42290748898678,  # float (post-fix collector output)
                "themes": ["ECON_CURRENCY%EUR", "ECON_CENTRAL_BANK"],
                "locations": [],
                "organizations": ["Federal Reserve"],
                "metadata": {"credibility_tier": 2, "url_hash": "abc123"},
            },
            {
                "source": "gdelt",
                "timestamp_collected": "2026-02-14T23:20:00",
                "timestamp_published": "2026-02-13T06:00:00Z",
                "url": "https://www.reuters.com/2026/02/13/ecb-policy.html",
                "source_domain": "reuters.com",
                "tone": 3.5,  # positive tone
                "themes": ["ECON_CENTRAL_BANK", "EUR"],
                "locations": ["Germany"],
                "organizations": ["ECB"],
                "metadata": {"credibility_tier": 1, "url_hash": "def456"},
            },
            {
                "source": "gdelt",
                "timestamp_collected": "2026-02-14T23:21:00",
                "timestamp_published": "2026-02-13T12:00:00Z",
                "url": "https://www.ft.com/2026/02/13/gbp-rally.html",
                "source_domain": "ft.com",
                "tone": "0.3,1.2,0.9,0,0,5",  # legacy string format (pre-fix)
                "themes": ["ECON_CURRENCY%GBP"],
                "locations": ["United Kingdom"],
                "organizations": ["Bank of England"],
                "metadata": {"credibility_tier": 1, "url_hash": "ghi789"},
            },
        ]

    @pytest.fixture
    def preprocessor(self, tmp_path: Path) -> GDELTPreprocessor:
        """GDELTPreprocessor instance with temporary directories."""
        input_dir = tmp_path / "raw" / "news" / "gdelt"
        output_dir = tmp_path / "processed" / "sentiment"
        input_dir.mkdir(parents=True)
        output_dir.mkdir(parents=True)
        return GDELTPreprocessor(input_dir=input_dir, output_dir=output_dir)

    @pytest.fixture
    def bronze_jsonl(self, preprocessor: GDELTPreprocessor, sample_documents: list[dict]) -> Path:
        """Write sample documents to a Bronze JSONL file."""
        path = preprocessor.input_dir / "aggregated_20260213.jsonl"
        with open(path, "w", encoding="utf-8") as f:
            for doc in sample_documents:
                f.write(json.dumps(doc) + "\n")
        return path

    @pytest.fixture
    def valid_silver_df(self) -> pd.DataFrame:
        """Minimal valid Silver DataFrame for validate() tests."""
        rows = []
        for pair in ["EURUSD", "GBPUSD", "USDJPY"]:
            rows.append(
                {
                    "timestamp_utc": "2026-02-13T00:15:00Z",
                    "article_id": "abcd1234567890ab",
                    "pair": pair,
                    "direction_multiplier": 1,
                    "headline": "https://www.example.com/article",
                    "text_input_type": "v2tone",
                    "sentiment_score": -0.24,
                    "sentiment_label": "negative",
                    "sentiment_prob_pos": 0.0,
                    "sentiment_prob_neg": 1.0,
                    "sentiment_prob_neutral": 0.0,
                    "document_type": "article",
                    "speaker": None,
                    "source": "gdelt",
                    "url": "https://www.example.com/article",
                }
            )
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    def test_initialization(self, preprocessor: GDELTPreprocessor) -> None:
        """Preprocessor initialises with correct class attributes."""
        assert preprocessor.SOURCE_NAME == "gdelt"
        assert preprocessor.PAIRS == [
            ("EURUSD", +1),
            ("GBPUSD", +1),
            ("USDJPY", +1),
        ]
        assert preprocessor.NEUTRAL_THRESHOLD == 0.05
        assert preprocessor.input_dir.exists()
        assert preprocessor.output_dir.exists()

    # ------------------------------------------------------------------
    # _parse_v2tone
    # ------------------------------------------------------------------

    def test_parse_v2tone_float(self, preprocessor: GDELTPreprocessor) -> None:
        """Float tone values (post-fix collector) are returned as-is."""
        assert preprocessor._parse_v2tone(-2.4229) == -2.4229
        assert preprocessor._parse_v2tone(3.5) == 3.5
        assert preprocessor._parse_v2tone(0.0) == 0.0

    def test_parse_v2tone_string_comma_separated(self, preprocessor: GDELTPreprocessor) -> None:
        """Legacy comma-separated string is parsed to first element."""
        assert preprocessor._parse_v2tone("-2.42,2.34,4.77,0,0,7") == pytest.approx(-2.42)
        assert preprocessor._parse_v2tone("3.5,1.2,2.3") == pytest.approx(3.5)
        assert preprocessor._parse_v2tone("0.3,1.2,0.9,0,0,5") == pytest.approx(0.3)

    def test_parse_v2tone_none_returns_zero(self, preprocessor: GDELTPreprocessor) -> None:
        """None tone defaults to 0.0."""
        assert preprocessor._parse_v2tone(None) == 0.0

    def test_parse_v2tone_invalid_string_returns_zero(
        self, preprocessor: GDELTPreprocessor
    ) -> None:
        """Unparseable string defaults to 0.0 without raising."""
        assert preprocessor._parse_v2tone("N/A") == 0.0
        assert preprocessor._parse_v2tone("") == 0.0

    def test_parse_v2tone_integer(self, preprocessor: GDELTPreprocessor) -> None:
        """Integer values are accepted and converted to float."""
        result = preprocessor._parse_v2tone(5)
        assert isinstance(result, float)
        assert result == 5.0

    # ------------------------------------------------------------------
    # _score_to_label
    # ------------------------------------------------------------------

    def test_score_to_label_positive(self, preprocessor: GDELTPreprocessor) -> None:
        assert preprocessor._score_to_label(0.06) == "positive"
        assert preprocessor._score_to_label(1.0) == "positive"

    def test_score_to_label_negative(self, preprocessor: GDELTPreprocessor) -> None:
        assert preprocessor._score_to_label(-0.06) == "negative"
        assert preprocessor._score_to_label(-1.0) == "negative"

    def test_score_to_label_neutral_boundary(self, preprocessor: GDELTPreprocessor) -> None:
        assert preprocessor._score_to_label(0.0) == "neutral"
        assert preprocessor._score_to_label(0.05) == "neutral"
        assert preprocessor._score_to_label(-0.05) == "neutral"

    # ------------------------------------------------------------------
    # _score_to_probs
    # ------------------------------------------------------------------

    def test_score_to_probs_positive(self, preprocessor: GDELTPreprocessor) -> None:
        p_pos, p_neg, p_neutral = preprocessor._score_to_probs("positive")
        assert p_pos == 1.0
        assert p_neg == 0.0
        assert p_neutral == 0.0
        assert p_pos + p_neg + p_neutral == pytest.approx(1.0)

    def test_score_to_probs_negative(self, preprocessor: GDELTPreprocessor) -> None:
        p_pos, p_neg, p_neutral = preprocessor._score_to_probs("negative")
        assert p_pos == 0.0
        assert p_neg == 1.0
        assert p_neutral == 0.0
        assert p_pos + p_neg + p_neutral == pytest.approx(1.0)

    def test_score_to_probs_neutral(self, preprocessor: GDELTPreprocessor) -> None:
        p_pos, p_neg, p_neutral = preprocessor._score_to_probs("neutral")
        assert p_pos == 0.0
        assert p_neg == 0.0
        assert p_neutral == 1.0
        assert p_pos + p_neg + p_neutral == pytest.approx(1.0)

    # ------------------------------------------------------------------
    # _build_record
    # ------------------------------------------------------------------

    def test_build_record_post_fix_tone(
        self, preprocessor: GDELTPreprocessor, sample_documents: list[dict]
    ) -> None:
        """Post-fix doc (tone is float) produces correct Silver record."""
        doc = sample_documents[0]  # tone=-2.4229
        record = preprocessor._build_record(doc)

        assert record["timestamp_utc"] == "2026-02-13T00:15:00Z"
        assert len(record["article_id"]) == 16
        assert record["headline"] == doc["url"]
        assert record["text_input_type"] == "v2tone"
        assert record["sentiment_score"] == pytest.approx(-0.2423, abs=1e-3)
        assert record["sentiment_label"] == "negative"
        assert record["sentiment_prob_pos"] == 0.0
        assert record["sentiment_prob_neg"] == 1.0
        assert record["sentiment_prob_neutral"] == 0.0
        assert record["document_type"] == "article"
        assert record["speaker"] is None
        assert record["source"] == "gdelt"
        assert record["url"] == doc["url"]
        assert "pair" not in record
        assert "direction_multiplier" not in record

    def test_build_record_legacy_string_tone(
        self, preprocessor: GDELTPreprocessor, sample_documents: list[dict]
    ) -> None:
        """Pre-fix doc (tone is comma-separated string) is handled correctly."""
        doc = sample_documents[2]  # tone="0.3,1.2,0.9,0,0,5"
        record = preprocessor._build_record(doc)

        # score = clip(0.3 / 10, -1, 1) = 0.03 → neutral
        assert record["sentiment_score"] == pytest.approx(0.03, abs=1e-4)
        assert record["sentiment_label"] == "neutral"
        assert record["sentiment_prob_neutral"] == 1.0

    def test_build_record_missing_url_raises(self, preprocessor: GDELTPreprocessor) -> None:
        """Document without URL raises ValueError."""
        doc = {
            "source": "gdelt",
            "timestamp_collected": "2026-02-13T00:00:00",
            "tone": 1.0,
        }
        with pytest.raises(ValueError, match="no URL"):
            preprocessor._build_record(doc)

    def test_build_record_score_clipped_to_range(self, preprocessor: GDELTPreprocessor) -> None:
        """Extreme V2Tone values are clipped to [-1.0, 1.0]."""
        doc = {
            "source": "gdelt",
            "timestamp_collected": "2026-02-13T00:00:00",
            "timestamp_published": "2026-02-13T00:00:00Z",
            "url": "https://example.com/extreme",
            "tone": 99.9,  # far above 10
        }
        record = preprocessor._build_record(doc)
        assert record["sentiment_score"] == 1.0

        doc_neg = dict(doc, url="https://example.com/extreme-neg", tone=-99.9)
        record_neg = preprocessor._build_record(doc_neg)
        assert record_neg["sentiment_score"] == -1.0

    # ------------------------------------------------------------------
    # _process_documents
    # ------------------------------------------------------------------

    def test_process_documents_fan_out(
        self, preprocessor: GDELTPreprocessor, sample_documents: list[dict]
    ) -> None:
        """Each document fans out to one row per pair in PAIRS."""
        records = preprocessor._process_documents([sample_documents[0]])
        assert len(records) == len(preprocessor.PAIRS)

        pairs_in_records = [r["pair"] for r in records]
        assert set(pairs_in_records) == {"EURUSD", "GBPUSD", "USDJPY"}

        for rec in records:
            assert rec["direction_multiplier"] == +1
            assert rec["source"] == "gdelt"

    def test_process_documents_all_same_article_id(
        self, preprocessor: GDELTPreprocessor, sample_documents: list[dict]
    ) -> None:
        """Fan-out records for the same document share an article_id."""
        records = preprocessor._process_documents([sample_documents[1]])
        article_ids = {r["article_id"] for r in records}
        assert len(article_ids) == 1

    def test_process_documents_skips_invalid(self, preprocessor: GDELTPreprocessor) -> None:
        """Documents missing required fields are skipped without raising."""
        bad_doc = {"source": "gdelt", "timestamp_collected": "2026-02-13T00:00:00"}
        good_doc = {
            "source": "gdelt",
            "timestamp_collected": "2026-02-13T00:00:00",
            "timestamp_published": "2026-02-13T00:00:00Z",
            "url": "https://example.com/good",
            "tone": 1.0,
        }
        records = preprocessor._process_documents([bad_doc, good_doc])
        # bad_doc skipped; good_doc fans out to 3 pairs
        assert len(records) == len(preprocessor.PAIRS)

    def test_process_documents_multiple_docs(
        self, preprocessor: GDELTPreprocessor, sample_documents: list[dict]
    ) -> None:
        """Multiple documents produce N_docs × N_pairs rows."""
        records = preprocessor._process_documents(sample_documents)
        assert len(records) == len(sample_documents) * len(preprocessor.PAIRS)

    # ------------------------------------------------------------------
    # preprocess (end-to-end)
    # ------------------------------------------------------------------

    def test_preprocess_returns_correct_shape(
        self,
        preprocessor: GDELTPreprocessor,
        bronze_jsonl: Path,
        sample_documents: list[dict],
    ) -> None:
        """preprocess() returns N_docs × N_pairs rows with correct columns."""
        df = preprocessor.preprocess()
        expected_rows = len(sample_documents) * len(preprocessor.PAIRS)
        assert len(df) == expected_rows

        required_cols = [
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
        for col in required_cols:
            assert col in df.columns, f"Missing column: {col}"

    def test_preprocess_source_column(
        self, preprocessor: GDELTPreprocessor, bronze_jsonl: Path
    ) -> None:
        """All output rows have source='gdelt'."""
        df = preprocessor.preprocess()
        assert (df["source"] == "gdelt").all()

    def test_preprocess_text_input_type(
        self, preprocessor: GDELTPreprocessor, bronze_jsonl: Path
    ) -> None:
        """All rows use 'v2tone' as text_input_type."""
        df = preprocessor.preprocess()
        assert (df["text_input_type"] == "v2tone").all()

    def test_preprocess_score_range(
        self, preprocessor: GDELTPreprocessor, bronze_jsonl: Path
    ) -> None:
        """All sentiment scores are within [-1.0, 1.0]."""
        df = preprocessor.preprocess()
        assert df["sentiment_score"].between(-1.0, 1.0).all()

    def test_preprocess_probability_sums(
        self, preprocessor: GDELTPreprocessor, bronze_jsonl: Path
    ) -> None:
        """Probability proxy values sum to 1.0 for every row."""
        df = preprocessor.preprocess()
        prob_sums = (
            df["sentiment_prob_pos"] + df["sentiment_prob_neg"] + df["sentiment_prob_neutral"]
        )
        assert (prob_sums.round(4) == 1.0).all()

    def test_preprocess_date_filter_start(
        self,
        preprocessor: GDELTPreprocessor,
        bronze_jsonl: Path,
        sample_documents: list[dict],
    ) -> None:
        """start_date filter excludes records before cutoff."""
        # All docs have timestamp_published on 2026-02-13; filtering to after 06:00
        # should exclude sample_documents[0] (00:15) and keep [1] (06:00) + [2] (12:00)
        cutoff = datetime(2026, 2, 13, 6, 0, 0)
        df = preprocessor.preprocess(start_date=cutoff)
        n_pairs = len(preprocessor.PAIRS)
        # 2 documents × n_pairs rows
        assert len(df) == 2 * n_pairs

    def test_preprocess_date_filter_end(
        self,
        preprocessor: GDELTPreprocessor,
        bronze_jsonl: Path,
    ) -> None:
        """end_date filter excludes records after cutoff."""
        cutoff = datetime(2026, 2, 13, 1, 0, 0)  # 01:00 — only doc[0] passes (00:15)
        df = preprocessor.preprocess(end_date=cutoff)
        n_pairs = len(preprocessor.PAIRS)
        assert len(df) == 1 * n_pairs

    def test_preprocess_deduplication(
        self,
        preprocessor: GDELTPreprocessor,
        sample_documents: list[dict],
    ) -> None:
        """Duplicate (article_id, pair) records are dropped."""
        path = preprocessor.input_dir / "aggregated_20260213.jsonl"
        # Write same document twice
        with open(path, "w", encoding="utf-8") as f:
            for doc in [sample_documents[0], sample_documents[0]]:
                f.write(json.dumps(doc) + "\n")

        df = preprocessor.preprocess()
        # 1 unique doc × n_pairs
        assert len(df) == len(preprocessor.PAIRS)

    def test_preprocess_no_jsonl_raises(self, preprocessor: GDELTPreprocessor) -> None:
        """preprocess() raises ValueError when no JSONL files exist."""
        with pytest.raises(ValueError, match="No JSONL files found"):
            preprocessor.preprocess()

    def test_preprocess_all_invalid_docs_raises(self, preprocessor: GDELTPreprocessor) -> None:
        """preprocess() raises ValueError when all docs are invalid."""
        path = preprocessor.input_dir / "aggregated_20260213.jsonl"
        # Write docs with no URL — will all be skipped
        with open(path, "w", encoding="utf-8") as f:
            f.write(
                json.dumps({"source": "gdelt", "timestamp_collected": "2026-02-13T00:00:00"}) + "\n"
            )

        with pytest.raises(ValueError, match="No valid records"):
            preprocessor.preprocess()

    def test_preprocess_sorted_by_timestamp_pair(
        self, preprocessor: GDELTPreprocessor, bronze_jsonl: Path
    ) -> None:
        """Output is sorted by (timestamp_utc, pair) ascending."""
        df = preprocessor.preprocess()
        timestamps = df["timestamp_utc"].tolist()
        pairs = df["pair"].tolist()
        # Check the combination is non-decreasing
        combined = list(zip(timestamps, pairs))
        assert combined == sorted(combined)

    # ------------------------------------------------------------------
    # validate
    # ------------------------------------------------------------------

    def test_validate_passes_on_valid_df(
        self, preprocessor: GDELTPreprocessor, valid_silver_df: pd.DataFrame
    ) -> None:
        """validate() returns True for a well-formed Silver DataFrame."""
        assert preprocessor.validate(valid_silver_df) is True

    def test_validate_missing_column_raises(
        self, preprocessor: GDELTPreprocessor, valid_silver_df: pd.DataFrame
    ) -> None:
        """validate() raises ValueError when a required column is missing."""
        df = valid_silver_df.drop(columns=["url"])
        with pytest.raises(ValueError, match="Missing required columns"):
            preprocessor.validate(df)

    def test_validate_null_critical_field_raises(
        self, preprocessor: GDELTPreprocessor, valid_silver_df: pd.DataFrame
    ) -> None:
        """validate() raises ValueError when a critical field contains nulls."""
        df = valid_silver_df.copy()
        df.loc[0, "article_id"] = None
        with pytest.raises(ValueError, match="Null values in critical field 'article_id'"):
            preprocessor.validate(df)

    def test_validate_score_out_of_range_raises(
        self, preprocessor: GDELTPreprocessor, valid_silver_df: pd.DataFrame
    ) -> None:
        """validate() raises ValueError when sentiment_score is outside [-1, 1]."""
        df = valid_silver_df.copy()
        df.loc[0, "sentiment_score"] = 1.5
        with pytest.raises(ValueError, match="sentiment_score outside"):
            preprocessor.validate(df)

    def test_validate_invalid_label_raises(
        self, preprocessor: GDELTPreprocessor, valid_silver_df: pd.DataFrame
    ) -> None:
        """validate() raises ValueError for unrecognised sentiment_label."""
        df = valid_silver_df.copy()
        df.loc[0, "sentiment_label"] = "bullish"
        with pytest.raises(ValueError, match="Invalid sentiment_label values"):
            preprocessor.validate(df)

    def test_validate_invalid_text_input_type_raises(
        self, preprocessor: GDELTPreprocessor, valid_silver_df: pd.DataFrame
    ) -> None:
        """validate() raises ValueError for unknown text_input_type."""
        df = valid_silver_df.copy()
        df["text_input_type"] = "finbert"
        with pytest.raises(ValueError, match="Invalid text_input_type values"):
            preprocessor.validate(df)

    def test_validate_v2tone_text_input_type_accepted(
        self, preprocessor: GDELTPreprocessor, valid_silver_df: pd.DataFrame
    ) -> None:
        """validate() accepts 'v2tone' as a valid text_input_type."""
        df = valid_silver_df.copy()
        df["text_input_type"] = "v2tone"
        assert preprocessor.validate(df) is True

    def test_validate_headline_only_accepted(
        self, preprocessor: GDELTPreprocessor, valid_silver_df: pd.DataFrame
    ) -> None:
        """validate() accepts inherited text_input_type values ('headline_only')."""
        df = valid_silver_df.copy()
        df["text_input_type"] = "headline_only"
        assert preprocessor.validate(df) is True

    def test_validate_prob_sum_not_one_raises(
        self, preprocessor: GDELTPreprocessor, valid_silver_df: pd.DataFrame
    ) -> None:
        """validate() raises ValueError when probability values do not sum to 1.0."""
        df = valid_silver_df.copy()
        df.loc[0, "sentiment_prob_neg"] = 0.5  # sum now 1.5 for that row
        with pytest.raises(ValueError, match="Probability sum not"):
            preprocessor.validate(df)

    def test_validate_invalid_direction_multiplier_raises(
        self, preprocessor: GDELTPreprocessor, valid_silver_df: pd.DataFrame
    ) -> None:
        """validate() raises ValueError for direction_multiplier outside {-1, +1}."""
        df = valid_silver_df.copy()
        df.loc[0, "direction_multiplier"] = 0
        with pytest.raises(ValueError, match="Invalid direction_multiplier"):
            preprocessor.validate(df)

    def test_validate_duplicate_article_pair_raises(
        self, preprocessor: GDELTPreprocessor, valid_silver_df: pd.DataFrame
    ) -> None:
        """validate() raises ValueError on duplicate (article_id, pair) rows."""
        df = pd.concat([valid_silver_df, valid_silver_df], ignore_index=True)
        with pytest.raises(ValueError, match="Duplicate"):
            preprocessor.validate(df)

    # ------------------------------------------------------------------
    # export_partitioned (integration)
    # ------------------------------------------------------------------

    def test_export_partitioned_creates_parquet(
        self,
        preprocessor: GDELTPreprocessor,
        bronze_jsonl: Path,
    ) -> None:
        """export_partitioned() writes Silver Parquet files partitioned by source/year/month."""
        df = preprocessor.preprocess()
        output_paths = preprocessor.export_partitioned(df)

        assert len(output_paths) > 0
        for partition_key, file_path in output_paths.items():
            assert file_path.exists()
            assert file_path.name == "sentiment_cleaned.parquet"
            assert "source=gdelt" in str(file_path)
            assert "year=" in str(file_path)
            assert "month=" in str(file_path)

            # Verify the written Parquet is readable and schema is correct
            loaded = pd.read_parquet(file_path)
            assert "timestamp_utc" in loaded.columns
            assert "sentiment_score" in loaded.columns
            assert (loaded["source"] == "gdelt").all()
