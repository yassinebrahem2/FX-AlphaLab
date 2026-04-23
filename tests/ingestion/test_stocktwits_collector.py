"""Unit tests for StocktwitsCollector."""

import importlib.util
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import requests

MODULE_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "ingestion"
    / "collectors"
    / "stocktwits_collector.py"
)
MODULE_SPEC = importlib.util.spec_from_file_location(
    "stocktwits_collector_test_module", MODULE_PATH
)
assert MODULE_SPEC is not None and MODULE_SPEC.loader is not None
MODULE = importlib.util.module_from_spec(MODULE_SPEC)
MODULE_SPEC.loader.exec_module(MODULE)
StocktwitsCollector = MODULE.StocktwitsCollector


def _api_response(messages: list[dict], more: bool = False) -> dict:
    return {"messages": messages, "cursor": {"more": more}}


def _make_msg(msg_id: int, symbol: str, timestamp_iso: str) -> dict:
    return {
        "id": msg_id,
        "body": f"{symbol} message {msg_id}",
        "created_at": timestamp_iso,
        "user": {
            "id": 100 + msg_id,
            "username": f"user_{msg_id}",
            "followers": msg_id,
        },
        "entities": {"sentiment": {"basic": "Bullish"}},
    }


def _iso_days_ago(days: int) -> str:
    ts = datetime.now(timezone.utc) - timedelta(days=days)
    return ts.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


@pytest.fixture
def collector(tmp_path: Path) -> StocktwitsCollector:
    return StocktwitsCollector(
        output_dir=tmp_path / "raw" / "news" / "stocktwits",
        log_file=None,
        symbols=["EURUSD"],
    )


@pytest.fixture
def collector_two_symbols(tmp_path: Path) -> StocktwitsCollector:
    return StocktwitsCollector(
        output_dir=tmp_path / "raw" / "news" / "stocktwits",
        log_file=None,
        symbols=["EURUSD", "GBPUSD"],
    )


class TestInitialization:
    def test_default_symbols_include_usdchf(self, tmp_path: Path) -> None:
        c = StocktwitsCollector(output_dir=tmp_path, log_file=None)
        assert c.DEFAULT_SYMBOLS == ["EURUSD", "GBPUSD", "USDCHF", "USDJPY"]


class TestHealthCheck:
    def test_health_check_returns_true_and_false(self, collector: StocktwitsCollector) -> None:
        ok = Mock()
        ok.status_code = 200
        with patch.object(collector._session, "get", return_value=ok):
            assert collector.health_check() is True

        with patch.object(
            collector._session,
            "get",
            side_effect=requests.RequestException("boom"),
        ):
            assert collector.health_check() is False


class TestParseMessage:
    def test_parse_message_bronze_schema(self, collector: StocktwitsCollector) -> None:
        raw_msg = {
            "id": 1234,
            "body": "EURUSD looks strong",
            "created_at": "2026-02-20T12:00:00Z",
            "user": {"id": 99, "username": "alice", "followers": 321},
            "entities": {"sentiment": {"basic": "Bullish"}},
        }
        record = collector._parse_message(raw_msg, "eurusd", "2026-02-20T13:00:00Z")

        assert record == {
            "source": "stocktwits",
            "timestamp_collected": "2026-02-20T13:00:00Z",
            "timestamp_published": "2026-02-20T12:00:00Z",
            "message_id": 1234,
            "symbol": "EURUSD",
            "body": "EURUSD looks strong",
            "sentiment": "Bullish",
            "username": "alice",
            "user_id": 99,
            "followers_count": 321,
            "url": "https://stocktwits.com/alice/message/1234",
        }


class TestRequest:
    def test_429_backoff_is_exponential(self, collector: StocktwitsCollector) -> None:
        resp_429 = Mock()
        resp_429.status_code = 429
        resp_ok = Mock()
        resp_ok.status_code = 200
        resp_ok.json.return_value = {"messages": []}

        with patch.object(collector._session, "get", side_effect=[resp_429, resp_429, resp_ok]):
            with patch("time.sleep") as mock_sleep:
                result = collector._request("https://example.com", {})

        assert result == {"messages": []}
        assert [call.args[0] for call in mock_sleep.call_args_list] == [60.0, 120.0]


class TestCollectOne:
    def test_collect_one_writes_to_correct_canonical_path(
        self, collector: StocktwitsCollector
    ) -> None:
        with patch.object(
            collector,
            "_request",
            return_value=_api_response([_make_msg(1, "EURUSD", _iso_days_ago(1))], more=False),
        ):
            count = collector.collect_one("EURUSD")

        expected_path = collector.output_dir / "eurusd_raw.jsonl"
        assert count == 1
        assert expected_path.exists()
        assert [path.name for path in collector.output_dir.glob("*.jsonl")] == ["eurusd_raw.jsonl"]


class TestCollect:
    def test_default_mode_collects_last_30_days(self, collector: StocktwitsCollector) -> None:
        responses = [
            _api_response(
                [
                    _make_msg(101, "EURUSD", _iso_days_ago(1)),
                    _make_msg(102, "EURUSD", _iso_days_ago(5)),
                    _make_msg(103, "EURUSD", _iso_days_ago(35)),
                ],
                more=False,
            )
        ]
        with patch.object(collector, "_request", side_effect=responses):
            result = collector.collect()

        canonical_path = collector.output_dir / "eurusd_raw.jsonl"
        rows = _load_jsonl(canonical_path)

        assert result == {"eurusd": 2}
        assert [row["message_id"] for row in rows] == [101, 102]

    def test_default_mode_creates_canonical_file(self, collector: StocktwitsCollector) -> None:
        with patch.object(
            collector,
            "_request",
            return_value=_api_response([_make_msg(1, "EURUSD", _iso_days_ago(1))], more=False),
        ):
            collector.collect()

        assert (collector.output_dir / "eurusd_raw.jsonl").exists()

    def test_explicit_range_respects_start_and_end_date(
        self, collector: StocktwitsCollector
    ) -> None:
        responses = [
            _api_response(
                [
                    _make_msg(401, "EURUSD", "2023-06-01T00:00:00Z"),
                    _make_msg(402, "EURUSD", "2022-06-01T00:00:00Z"),
                ],
                more=True,
            ),
            _api_response(
                [
                    _make_msg(403, "EURUSD", "2021-06-01T00:00:00Z"),
                    _make_msg(404, "EURUSD", "2020-06-01T00:00:00Z"),
                ],
                more=True,
            ),
        ]
        with patch.object(collector, "_request", side_effect=responses) as mock_request:
            result = collector.collect(
                start_date=datetime(2021, 1, 1),
                end_date=datetime(2022, 12, 31),
            )

        rows = _load_jsonl(collector.output_dir / "eurusd_raw.jsonl")
        assert mock_request.call_count == 2
        assert result == {"eurusd": 2}
        assert [row["message_id"] for row in rows] == [402, 403]

    def test_explicit_range_stops_at_start_date(self, collector: StocktwitsCollector) -> None:
        responses = [
            _api_response(
                [
                    _make_msg(501, "EURUSD", "2022-06-01T00:00:00Z"),
                    _make_msg(502, "EURUSD", "2021-06-01T00:00:00Z"),
                ],
                more=True,
            ),
            _api_response(
                [
                    _make_msg(503, "EURUSD", "2020-06-01T00:00:00Z"),
                    _make_msg(504, "EURUSD", "2019-06-01T00:00:00Z"),
                ],
                more=True,
            ),
        ]
        with patch.object(collector, "_request", side_effect=responses) as mock_request:
            collector.collect(start_date=datetime(2021, 1, 1), end_date=datetime(2022, 12, 31))

        assert mock_request.call_count == 2

    def test_backfill_stops_at_max_id_in_canonical_file(
        self, collector: StocktwitsCollector
    ) -> None:
        canonical_path = collector.output_dir / "eurusd_raw.jsonl"
        canonical_path.parent.mkdir(parents=True, exist_ok=True)
        canonical_record = collector._parse_message(
            _make_msg(1000, "EURUSD", "2022-01-01T00:00:00Z"),
            "EURUSD",
            "2022-01-01T01:00:00Z",
        )
        canonical_path.write_text(json.dumps(canonical_record) + "\n", encoding="utf-8")
        responses = [
            _api_response(
                [
                    _make_msg(1003, "EURUSD", "2022-01-03T00:00:00Z"),
                    _make_msg(1001, "EURUSD", "2022-01-02T00:00:00Z"),
                ],
                more=True,
            ),
            _api_response(
                [
                    _make_msg(999, "EURUSD", "2021-12-31T00:00:00Z"),
                ],
                more=False,
            ),
        ]
        with patch.object(collector, "_request", side_effect=responses):
            count = collector.collect(backfill=True)

        rows = _load_jsonl(canonical_path)
        assert count == {"eurusd": 2}
        assert [row["message_id"] for row in rows] == [1000, 1003, 1001]

    def test_backfill_with_no_canonical_file_behaves_as_default(self, tmp_path: Path) -> None:
        default_collector = StocktwitsCollector(
            output_dir=tmp_path / "default" / "raw" / "news" / "stocktwits",
            log_file=None,
            symbols=["EURUSD"],
        )
        backfill_collector = StocktwitsCollector(
            output_dir=tmp_path / "backfill" / "raw" / "news" / "stocktwits",
            log_file=None,
            symbols=["EURUSD"],
        )
        response = _api_response(
            [
                _make_msg(201, "EURUSD", _iso_days_ago(1)),
                _make_msg(202, "EURUSD", _iso_days_ago(5)),
                _make_msg(203, "EURUSD", _iso_days_ago(35)),
            ],
            more=False,
        )
        with patch.object(default_collector, "_request", return_value=response):
            default_result = default_collector.collect()
        with patch.object(backfill_collector, "_request", return_value=response):
            backfill_result = backfill_collector.collect(backfill=True)

        default_rows = _load_jsonl(default_collector.output_dir / "eurusd_raw.jsonl")
        backfill_rows = _load_jsonl(backfill_collector.output_dir / "eurusd_raw.jsonl")
        assert default_result == backfill_result == {"eurusd": 2}
        assert [row["message_id"] for row in default_rows] == [201, 202]
        assert [row["message_id"] for row in backfill_rows] == [201, 202]

    def test_canonical_file_is_append_only(self, collector: StocktwitsCollector) -> None:
        first_batch = _api_response(
            [
                _make_msg(301, "EURUSD", _iso_days_ago(1)),
                _make_msg(302, "EURUSD", _iso_days_ago(2)),
            ],
            more=False,
        )
        second_batch = _api_response(
            [
                _make_msg(401, "EURUSD", _iso_days_ago(3)),
                _make_msg(402, "EURUSD", _iso_days_ago(4)),
            ],
            more=False,
        )

        with patch.object(collector, "_request", return_value=first_batch):
            first_result = collector.collect()
        with patch.object(collector, "_request", return_value=second_batch):
            second_result = collector.collect()

        rows = _load_jsonl(collector.output_dir / "eurusd_raw.jsonl")
        assert first_result == {"eurusd": 2}
        assert second_result == {"eurusd": 2}
        assert [row["message_id"] for row in rows] == [301, 302, 401, 402]

    def test_collect_returns_counts_per_symbol(
        self, collector_two_symbols: StocktwitsCollector
    ) -> None:
        response = _api_response(
            [
                _make_msg(601, "EURUSD", _iso_days_ago(1)),
            ],
            more=False,
        )
        with patch.object(collector_two_symbols, "_request", return_value=response):
            result = collector_two_symbols.collect()

        assert result == {"eurusd": 1, "gbpusd": 1}
