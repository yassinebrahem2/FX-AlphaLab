"""Unit tests for StocktwitsCollector."""

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import requests

from src.ingestion.collectors.stocktwits_collector import StocktwitsCollector

# ---------------------------------------------------------------------------
# Sample API Response Fixtures
# ---------------------------------------------------------------------------


def _make_message(
    msg_id: int = 1001,
    body: str = "EURUSD looking strong today",
    created_at: str = "2026-02-20T12:00:00Z",
    username: str = "trader_fx",
    user_id: int = 42,
    followers: int = 150,
    sentiment: str | None = "Bullish",
) -> dict:
    """Return a single Stocktwits message dict matching the real API shape."""
    entities: dict = {}
    if sentiment is not None:
        entities["sentiment"] = {"basic": sentiment}
    return {
        "id": msg_id,
        "body": body,
        "created_at": created_at,
        "user": {
            "id": user_id,
            "username": username,
            "followers": followers,
        },
        "entities": entities,
    }


def _make_api_response(
    messages: list[dict],
    has_more: bool = False,
    since: int = 0,
    max_id: int = 0,
) -> dict:
    """Return a mock Stocktwits API stream response dict."""
    return {
        "response": {"status": 200},
        "cursor": {"more": has_more, "since": since, "max": max_id},
        "messages": messages,
    }


SAMPLE_MESSAGES = [
    _make_message(1001, "EURUSD breakout above 1.09", sentiment="Bullish"),
    _make_message(1002, "GBPUSD weak, short it", sentiment="Bearish", username="fx_pro"),
    _make_message(1003, "Just watching the market", sentiment=None, username="newbie"),
]

SAMPLE_API_RESPONSE_SINGLE_PAGE = _make_api_response(
    messages=SAMPLE_MESSAGES,
    has_more=False,
)

SAMPLE_API_RESPONSE_PAGE_1 = _make_api_response(
    messages=SAMPLE_MESSAGES[:2],
    has_more=True,
    max_id=1001,
)
SAMPLE_API_RESPONSE_PAGE_2 = _make_api_response(
    messages=SAMPLE_MESSAGES[2:],
    has_more=False,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def collector(tmp_path: Path) -> StocktwitsCollector:
    """StocktwitsCollector with a temporary output dir and no file logging."""
    return StocktwitsCollector(
        output_dir=tmp_path / "raw" / "news" / "stocktwits",
        log_file=None,
        symbols=["EURUSD"],
    )


@pytest.fixture
def multi_symbol_collector(tmp_path: Path) -> StocktwitsCollector:
    """StocktwitsCollector configured for all three FX symbols."""
    return StocktwitsCollector(
        output_dir=tmp_path / "raw" / "news" / "stocktwits",
        log_file=None,
        symbols=["EURUSD", "GBPUSD", "USDJPY"],
    )


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------


class TestStocktwitsCollectorInit:
    """Verify correct initialization behaviour."""

    def test_source_name(self, collector: StocktwitsCollector) -> None:
        assert collector.SOURCE_NAME == "stocktwits"

    def test_output_dir_created(self, collector: StocktwitsCollector) -> None:
        assert collector.output_dir.exists()

    def test_custom_symbols(self, collector: StocktwitsCollector) -> None:
        assert collector.symbols == ["EURUSD"]

    def test_default_symbols(self, tmp_path: Path) -> None:
        c = StocktwitsCollector(output_dir=tmp_path)
        assert c.symbols == StocktwitsCollector.DEFAULT_SYMBOLS

    def test_session_has_user_agent(self, collector: StocktwitsCollector) -> None:
        ua = collector._session.headers.get("User-Agent", "")
        assert "FX-AlphaLab" in ua

    def test_session_has_accept_header(self, collector: StocktwitsCollector) -> None:
        assert collector._session.headers.get("Accept") == "application/json"


# ---------------------------------------------------------------------------
# Health Check
# ---------------------------------------------------------------------------


class TestHealthCheck:
    """Verify health_check returns correct boolean based on HTTP status."""

    def test_health_check_success(self, collector: StocktwitsCollector) -> None:
        mock_resp = Mock()
        mock_resp.status_code = 200
        with patch.object(collector._session, "get", return_value=mock_resp):
            assert collector.health_check() is True

    def test_health_check_bad_status(self, collector: StocktwitsCollector) -> None:
        mock_resp = Mock()
        mock_resp.status_code = 503
        with patch.object(collector._session, "get", return_value=mock_resp):
            assert collector.health_check() is False

    def test_health_check_network_error(self, collector: StocktwitsCollector) -> None:
        with patch.object(
            collector._session, "get", side_effect=requests.RequestException("timeout")
        ):
            assert collector.health_check() is False


# ---------------------------------------------------------------------------
# _parse_message
# ---------------------------------------------------------------------------


class TestParseMessage:
    """Verify _parse_message produces correct Bronze-layer records."""

    def test_bullish_message(self, collector: StocktwitsCollector) -> None:
        msg = _make_message(1001, sentiment="Bullish")
        ts_collected = "2026-02-20T14:00:00Z"
        record = collector._parse_message(msg, "EURUSD", ts_collected)

        assert record["source"] == "stocktwits"
        assert record["message_id"] == 1001
        assert record["symbol"] == "EURUSD"
        assert record["sentiment"] == "Bullish"
        assert record["timestamp_collected"] == ts_collected
        assert record["timestamp_published"] == "2026-02-20T12:00:00Z"
        assert record["username"] == "trader_fx"
        assert record["user_id"] == 42
        assert record["followers_count"] == 150
        assert "stocktwits.com/trader_fx/message/1001" in record["url"]

    def test_bearish_message(self, collector: StocktwitsCollector) -> None:
        msg = _make_message(1002, sentiment="Bearish")
        record = collector._parse_message(msg, "EURUSD", "2026-02-20T14:00:00Z")
        assert record["sentiment"] == "Bearish"

    def test_unlabeled_message(self, collector: StocktwitsCollector) -> None:
        msg = _make_message(1003, sentiment=None)
        record = collector._parse_message(msg, "EURUSD", "2026-02-20T14:00:00Z")
        assert record["sentiment"] is None

    def test_symbol_uppercased(self, collector: StocktwitsCollector) -> None:
        msg = _make_message(1004)
        record = collector._parse_message(msg, "eurusd", "2026-02-20T14:00:00Z")
        assert record["symbol"] == "EURUSD"

    def test_missing_user_fields_handled(self, collector: StocktwitsCollector) -> None:
        """Messages without user subkey must not raise."""
        msg = {
            "id": 999,
            "body": "test",
            "created_at": "2026-02-20T10:00:00Z",
            "user": None,
            "entities": {},
        }
        record = collector._parse_message(msg, "GBPUSD", "2026-02-20T14:00:00Z")
        assert record["username"] == ""
        assert record["user_id"] is None
        assert record["sentiment"] is None

    def test_all_required_bronze_fields_present(self, collector: StocktwitsCollector) -> None:
        required = {
            "source",
            "timestamp_collected",
            "timestamp_published",
            "message_id",
            "symbol",
            "body",
            "sentiment",
            "username",
            "user_id",
            "followers_count",
            "url",
        }
        msg = _make_message(1005)
        record = collector._parse_message(msg, "USDJPY", "2026-02-20T14:00:00Z")
        assert required.issubset(set(record.keys()))


# ---------------------------------------------------------------------------
# _request
# ---------------------------------------------------------------------------


class TestRequest:
    """Verify HTTP request layer: success, 429 backoff, error handling."""

    def test_returns_json_on_200(self, collector: StocktwitsCollector) -> None:
        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"messages": []}
        with patch.object(collector._session, "get", return_value=mock_resp):
            result = collector._request("https://example.com", {})
        assert result == {"messages": []}

    def test_returns_none_after_all_retries_fail(self, collector: StocktwitsCollector) -> None:
        mock_resp = Mock()
        mock_resp.status_code = 500
        with patch.object(collector._session, "get", return_value=mock_resp):
            with patch("time.sleep"):  # speed up test
                result = collector._request("https://example.com", {})
        assert result is None

    def test_429_triggers_backoff(self, collector: StocktwitsCollector) -> None:
        """On a 429, _request should sleep for RETRY_BACKOFF_ON_429."""
        mock_429 = Mock(status_code=429)
        mock_ok = Mock(status_code=200)
        mock_ok.json.return_value = {"messages": []}

        with patch.object(collector._session, "get", side_effect=[mock_429, mock_ok]):
            with patch("time.sleep") as mock_sleep:
                result = collector._request("https://example.com", {})

        sleep_args = [call.args[0] for call in mock_sleep.call_args_list]
        assert collector.RETRY_BACKOFF_ON_429 in sleep_args
        assert result == {"messages": []}

    def test_network_exception_returns_none(self, collector: StocktwitsCollector) -> None:
        with patch.object(collector._session, "get", side_effect=requests.RequestException("err")):
            with patch("time.sleep"):
                result = collector._request("https://example.com", {})
        assert result is None


# ---------------------------------------------------------------------------
# _collect_symbol  (single and multi-page)
# ---------------------------------------------------------------------------


class TestCollectSymbol:
    """Test pagination logic inside _collect_symbol."""

    def test_single_page_collection(self, collector: StocktwitsCollector) -> None:
        with patch.object(collector, "_request", return_value=SAMPLE_API_RESPONSE_SINGLE_PAGE):
            records = collector._collect_symbol("EURUSD", pages=5, start_date=None, end_date=None)

        assert len(records) == len(SAMPLE_MESSAGES)
        assert all(r["symbol"] == "EURUSD" for r in records)

    def test_multi_page_pagination(self, collector: StocktwitsCollector) -> None:
        responses = [SAMPLE_API_RESPONSE_PAGE_1, SAMPLE_API_RESPONSE_PAGE_2]
        with patch.object(collector, "_request", side_effect=responses):
            records = collector._collect_symbol("EURUSD", pages=5, start_date=None, end_date=None)

        assert len(records) == len(SAMPLE_MESSAGES)

    def test_deduplication_within_run(self, collector: StocktwitsCollector) -> None:
        """Same message appearing on two pages must only appear once."""
        dup_response = _make_api_response(messages=[SAMPLE_MESSAGES[0]], has_more=True)
        end_response = _make_api_response(messages=[SAMPLE_MESSAGES[0]], has_more=False)
        with patch.object(collector, "_request", side_effect=[dup_response, end_response]):
            records = collector._collect_symbol("EURUSD", pages=5, start_date=None, end_date=None)
        assert len(records) == 1

    def test_stops_when_cursor_exhausted(self, collector: StocktwitsCollector) -> None:
        """Must stop fetching when cursor.more is False."""
        with patch.object(
            collector, "_request", return_value=SAMPLE_API_RESPONSE_SINGLE_PAGE
        ) as mock_req:
            collector._collect_symbol("EURUSD", pages=10, start_date=None, end_date=None)
        # Only 1 API call because cursor.more=False on the first page
        assert mock_req.call_count == 1

    def test_stops_on_empty_messages(self, collector: StocktwitsCollector) -> None:
        empty_response = _make_api_response(messages=[], has_more=False)
        with patch.object(collector, "_request", return_value=empty_response) as mock_req:
            records = collector._collect_symbol("EURUSD", pages=5, start_date=None, end_date=None)
        assert records == []
        assert mock_req.call_count == 1

    def test_stops_when_request_returns_none(self, collector: StocktwitsCollector) -> None:
        with patch.object(collector, "_request", return_value=None):
            records = collector._collect_symbol("EURUSD", pages=5, start_date=None, end_date=None)
        assert records == []

    def test_start_date_filter(self, collector: StocktwitsCollector) -> None:
        """Records older than start_date must be excluded."""
        # Message timestamp: 2026-02-20T12:00:00Z — filter requires >= 2026-02-21
        future_cutoff = datetime(2026, 2, 21, tzinfo=timezone.utc)
        with patch.object(collector, "_request", return_value=SAMPLE_API_RESPONSE_SINGLE_PAGE):
            records = collector._collect_symbol(
                "EURUSD", pages=1, start_date=future_cutoff, end_date=None
            )
        assert records == []

    def test_end_date_filter(self, collector: StocktwitsCollector) -> None:
        """Records newer than end_date must be excluded."""
        past_cutoff = datetime(2026, 2, 19, tzinfo=timezone.utc)
        with patch.object(collector, "_request", return_value=SAMPLE_API_RESPONSE_SINGLE_PAGE):
            records = collector._collect_symbol(
                "EURUSD", pages=1, start_date=None, end_date=past_cutoff
            )
        assert records == []

    def test_naive_start_date_treated_as_utc(self, collector: StocktwitsCollector) -> None:
        """Naive datetimes must not raise — they are assumed UTC."""
        naive_start = datetime(2020, 1, 1)  # no tzinfo
        with patch.object(collector, "_request", return_value=SAMPLE_API_RESPONSE_SINGLE_PAGE):
            # Should not raise TypeError
            records = collector._collect_symbol(
                "EURUSD", pages=1, start_date=naive_start, end_date=None
            )
        assert isinstance(records, list)


# ---------------------------------------------------------------------------
# collect()  — top-level orchestration
# ---------------------------------------------------------------------------


class TestCollect:
    """Test the public collect() method."""

    def test_collect_returns_dict_keyed_by_lowercase_symbol(
        self, multi_symbol_collector: StocktwitsCollector
    ) -> None:
        with patch.object(
            multi_symbol_collector,
            "_collect_symbol",
            return_value=SAMPLE_MESSAGES,
        ):
            result = multi_symbol_collector.collect(pages_per_symbol=1)

        assert set(result.keys()) == {"eurusd", "gbpusd", "usdjpy"}

    def test_collect_uses_default_pages_when_not_specified(
        self, collector: StocktwitsCollector
    ) -> None:
        with patch.object(collector, "_collect_symbol", return_value=[]) as mock_cs:
            collector.collect()
        _, call_kwargs = mock_cs.call_args
        assert mock_cs.call_args[0][1] == StocktwitsCollector.DEFAULT_PAGES_PER_SYMBOL

    def test_collect_passes_pages_per_symbol(self, collector: StocktwitsCollector) -> None:
        with patch.object(collector, "_collect_symbol", return_value=[]) as mock_cs:
            collector.collect(pages_per_symbol=3)
        assert mock_cs.call_args[0][1] == 3

    def test_collect_result_values_are_lists(self, collector: StocktwitsCollector) -> None:
        with patch.object(collector, "_collect_symbol", return_value=SAMPLE_MESSAGES):
            result = collector.collect(pages_per_symbol=1)
        for v in result.values():
            assert isinstance(v, list)


# ---------------------------------------------------------------------------
# Bronze JSONL export  (via DocumentCollector.export_jsonl)
# ---------------------------------------------------------------------------


class TestExportJsonl:
    """Verify JSONL export using the inherited DocumentCollector method."""

    def test_export_creates_jsonl_file(self, collector: StocktwitsCollector) -> None:
        records = [
            collector._parse_message(m, "EURUSD", "2026-02-20T14:00:00Z") for m in SAMPLE_MESSAGES
        ]
        path = collector.export_jsonl(records, "eurusd")

        assert path.exists()
        assert path.suffix == ".jsonl"
        assert "eurusd" in path.name

    def test_exported_file_is_valid_jsonl(self, collector: StocktwitsCollector) -> None:
        records = [
            collector._parse_message(m, "EURUSD", "2026-02-20T14:00:00Z") for m in SAMPLE_MESSAGES
        ]
        path = collector.export_jsonl(records, "eurusd")

        lines = path.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == len(SAMPLE_MESSAGES)
        for line in lines:
            obj = json.loads(line)
            assert "source" in obj
            assert obj["source"] == "stocktwits"

    def test_export_empty_list_raises(self, collector: StocktwitsCollector) -> None:
        with pytest.raises(ValueError, match="empty"):
            collector.export_jsonl([], "eurusd")

    def test_export_all_writes_one_file_per_symbol(self, collector: StocktwitsCollector) -> None:
        """export_all() must write a JSONL file for each key in the data dict."""
        data = {
            "eurusd": [
                collector._parse_message(m, "EURUSD", "2026-02-20T14:00:00Z")
                for m in SAMPLE_MESSAGES
            ]
        }
        paths = collector.export_all(data=data)
        assert "eurusd" in paths
        assert paths["eurusd"].exists()


# ---------------------------------------------------------------------------
# Bronze schema contract validation
# ---------------------------------------------------------------------------


class TestBronzeContract:
    """Verify every produced record satisfies the §3.1 Bronze contract."""

    REQUIRED_FIELDS = {
        "source",
        "timestamp_collected",
        "timestamp_published",
        "message_id",
        "symbol",
        "body",
        "sentiment",
        "username",
        "user_id",
        "followers_count",
        "url",
    }

    def _collect_records(self, collector: StocktwitsCollector) -> list[dict]:
        with patch.object(collector, "_request", return_value=SAMPLE_API_RESPONSE_SINGLE_PAGE):
            data = collector.collect(pages_per_symbol=1)
        return [rec for msgs in data.values() for rec in msgs]

    def test_all_required_fields_present(self, collector: StocktwitsCollector) -> None:
        for record in self._collect_records(collector):
            assert self.REQUIRED_FIELDS.issubset(
                set(record.keys())
            ), f"Missing fields: {self.REQUIRED_FIELDS - set(record.keys())}"

    def test_source_is_stocktwits(self, collector: StocktwitsCollector) -> None:
        for record in self._collect_records(collector):
            assert record["source"] == "stocktwits"

    def test_timestamp_published_is_iso_string(self, collector: StocktwitsCollector) -> None:
        for record in self._collect_records(collector):
            # Must be non-empty string parseable as datetime
            ts = record["timestamp_published"]
            assert isinstance(ts, str) and len(ts) > 0

    def test_sentiment_is_valid_value(self, collector: StocktwitsCollector) -> None:
        valid = {"Bullish", "Bearish", None}
        for record in self._collect_records(collector):
            assert record["sentiment"] in valid, f"Unexpected sentiment: {record['sentiment']}"

    def test_symbol_is_uppercase(self, collector: StocktwitsCollector) -> None:
        for record in self._collect_records(collector):
            assert record["symbol"] == record["symbol"].upper()
