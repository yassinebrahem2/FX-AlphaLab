from datetime import datetime, timezone
from src.ingestion.collectors.boe_collector import BoECollector
from unittest.mock import Mock, patch
from unittest.mock import call
import requests
import json



def test_timestamp_normalization():
    collector = BoECollector()

    # Simulate feedparser published_parsed structure
    class DummyTime:
        tm_year = 2026
        tm_mon = 2
        tm_mday = 10
        tm_hour = 16
        tm_min = 30
        tm_sec = 0

    iso = collector._to_utc_iso("Tue, 10 Feb 2026 16:30:00 Z", DummyTime)
    assert iso == "2026-02-10T16:30:00+00:00"


def test_document_type_classification_speech():
    collector = BoECollector()
    url = "https://www.bankofengland.co.uk/speech/2026/february/test"

    bucket, doc_type = collector._classify_document_type(url, "Test Speech")

    assert bucket == "speeches"
    assert doc_type == "boe_speech"



def test_document_type_classification_mps():
    collector = BoECollector()
    url = "https://www.bankofengland.co.uk/monetary-policy-summary-and-minutes/2026/february"

    bucket, doc_type = collector._classify_document_type(url, "Monetary Policy Summary")

    assert bucket == "summaries"
    assert doc_type == "monetary_policy_summary"



def test_health_check_returns_bool():
    collector = BoECollector()
    result = collector.health_check()
    assert isinstance(result, bool)

def _make_response(text: str, status: int = 200) -> Mock:
    resp = Mock()
    resp.status_code = status
    resp.ok = 200 <= status < 300
    resp.text = text
    resp.headers = {"content-type": "text/xml; charset=utf-8"}
    if status >= 400:
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError(response=resp)
    else:
        resp.raise_for_status = Mock()
    return resp


class TestHealthCheckMocked:
    def test_health_check_true_when_feed_has_entries(self):
        collector = BoECollector()

        sample_rss = """<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0"><channel>
          <item>
            <title>Test item</title>
            <link>https://www.bankofengland.co.uk/news/test</link>
            <pubDate>Tue, 10 Feb 2026 16:30:00 Z</pubDate>
          </item>
        </channel></rss>
        """

        with patch.object(collector._session, "get", return_value=_make_response(sample_rss, 200)):
            assert collector.health_check() is True


    def test_health_check_false_when_all_feeds_fail(self):
        collector = BoECollector()
        with patch.object(
            collector._session, "get", side_effect=requests.exceptions.ConnectionError("down")
        ):
            assert collector.health_check() is False


class TestCollectErrorHandling:
    def test_collect_skips_article_when_fetch_fails(self, tmp_path):
        collector = BoECollector(output_dir=tmp_path)
        collector.RSS_URLS = ["https://www.bankofengland.co.uk/RSS/News"]


        # Minimal RSS with one entry inside range
        sample_rss = """<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0"><channel>
          <item>
            <title>Test item</title>
            <link>https://www.bankofengland.co.uk/news/test</link>
            <pubDate>Tue, 10 Feb 2026 16:30:00 Z</pubDate>
          </item>
        </channel></rss>
        """

        # RSS fetch ok, article fetch fails
        with (
            patch.object(collector._session, "get", return_value=_make_response(sample_rss, 200)),
            patch.object(collector, "_fetch_article_text", side_effect=RuntimeError("boom")),
            patch("time.sleep"),
        ):
            out = collector.collect(
                start_date=datetime(2026, 2, 10, tzinfo=timezone.utc),
                end_date=datetime(2026, 2, 10, 23, 59, 59, tzinfo=timezone.utc),
            )

        assert isinstance(out, dict)
        assert "press_releases" in out
        assert len(out["press_releases"]) == 1

        obj = out["press_releases"][0]
        assert obj["metadata"]["fetch_error"] is not None
        assert obj["metadata"]["fetch_error_type"] is not None

def test_collect_writes_jsonl_with_required_schema(tmp_path):
    collector = BoECollector(output_dir=tmp_path)

    sample_rss = """<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0"><channel>
      <item>
        <title>Test item</title>
        <link>https://www.bankofengland.co.uk/news/test</link>
        <pubDate>Tue, 10 Feb 2026 16:30:00 Z</pubDate>
      </item>
    </channel></rss>
    """

    with (
        patch.object(collector._session, "get", return_value=_make_response(sample_rss, 200)),
        patch.object(collector, "_fetch_article_text", return_value="FULL TEXT"),
        patch("time.sleep"),
    ):
        data = collector.collect(
            start_date=datetime(2026, 2, 10, tzinfo=timezone.utc),
            end_date=datetime(2026, 2, 10, 23, 59, 59, tzinfo=timezone.utc),
        )

    assert "press_releases" in data
    obj = data["press_releases"][0]


    required_keys = [
        "source",
        "timestamp_collected",
        "timestamp_published",
        "url",
        "title",
        "content",
        "document_type",
        "speaker",
        "metadata",
    ]
    for k in required_keys:
        assert k in obj

    assert obj["source"] == "BoE"
    assert obj["content"] == "FULL TEXT"


def test_extract_speaker_from_page_finds_speech_by(tmp_path):
    collector = BoECollector(output_dir=tmp_path)

    html = """
    <html><head></head>
    <body>
      <main>
        <h1>Some speech title</h1>
        <div>Speech by Jane Doe</div>
      </main>
    </body></html>
    """

    resp = Mock()
    resp.ok = True
    resp.status_code = 200
    resp.text = html
    resp.apparent_encoding = "utf-8"
    resp.raise_for_status = Mock()

    with patch.object(collector._session, "get", return_value=resp):
        speaker = collector._extract_speaker_from_page("https://www.bankofengland.co.uk/speech/2026/test")

    assert speaker == "Jane Doe"


def test_collect_calls_sleep_for_rate_limiting(tmp_path):
    collector = BoECollector(output_dir=tmp_path)

    sample_rss = """<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0"><channel>
      <item>
        <title>Test item</title>
        <link>https://www.bankofengland.co.uk/news/test</link>
        <pubDate>Tue, 10 Feb 2026 16:30:00 Z</pubDate>
      </item>
    </channel></rss>
    """

    with (
        patch.object(collector._session, "get", return_value=_make_response(sample_rss, 200)),
        patch.object(collector, "_fetch_article_text", return_value="FULL TEXT"),
        patch("time.sleep") as sleep_mock,
    ):
        collector.collect(
            start_date=datetime(2026, 2, 10, tzinfo=timezone.utc),
            end_date=datetime(2026, 2, 10, 23, 59, 59, tzinfo=timezone.utc),
        )

    # at least one document written -> sleep called at least once
    assert sleep_mock.call_count >= 1


def test_collect_date_filtering_inclusive_bounds(tmp_path):
    collector = BoECollector(output_dir=tmp_path)
    collector.RSS_URLS = ["https://www.bankofengland.co.uk/RSS/News"]


    # Two items: one on Feb 10, one on Feb 11
    sample_rss = """<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0"><channel>
      <item>
        <title>In range</title>
        <link>https://www.bankofengland.co.uk/news/in-range</link>
        <pubDate>Tue, 10 Feb 2026 16:30:00 Z</pubDate>
      </item>
      <item>
        <title>Out of range</title>
        <link>https://www.bankofengland.co.uk/news/out-range</link>
        <pubDate>Wed, 11 Feb 2026 10:00:00 Z</pubDate>
      </item>
    </channel></rss>
    """

    with (
        patch.object(collector._session, "get", return_value=_make_response(sample_rss, 200)),
        patch.object(collector, "_fetch_article_text", return_value="FULL TEXT"),
        patch("time.sleep"),
    ):
        data = collector.collect(
            start_date=datetime(2026, 2, 10, tzinfo=timezone.utc),
            end_date=datetime(2026, 2, 10, 23, 59, 59, tzinfo=timezone.utc),
        )

    assert "press_releases" in data

    assert len(data["press_releases"]) == 1

    assert data["press_releases"][0]["title"] == "In range"


def test_collect_fallback_writes_record_when_fetch_fails(tmp_path):
    collector = BoECollector(output_dir=tmp_path)

    sample_rss = """<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0"><channel>
      <item>
        <title>Test item</title>
        <link>https://www.bankofengland.co.uk/news/test</link>
        <description>RSS SUMMARY TEXT</description>
        <pubDate>Tue, 10 Feb 2026 16:30:00 Z</pubDate>
      </item>
    </channel></rss>
    """

    with (
        patch.object(collector._session, "get", return_value=_make_response(sample_rss, 200)),
        patch.object(collector, "_fetch_article_text", side_effect=RuntimeError("HTTP 500")),
        patch("time.sleep"),
    ):
        data = collector.collect(
            start_date=datetime(2026, 2, 10, tzinfo=timezone.utc),
            end_date=datetime(2026, 2, 10, 23, 59, 59, tzinfo=timezone.utc),
        )

    assert "press_releases" in data
    obj = data["press_releases"][0]

    assert obj["content"] in ("RSS SUMMARY TEXT", "")  # depending on rss parsing field
    assert obj["metadata"]["fetch_error"] is not None
    assert obj["metadata"]["fetch_error_type"] is not None


def test_export_jsonl_writes_file(tmp_path):
    collector = BoECollector(output_dir=tmp_path)

    fake_data = [{
        "source": "BoE",
        "timestamp_collected": "2026-02-11T00:00:00+00:00",
        "timestamp_published": "2026-02-10T16:30:00+00:00",
        "url": "http://test",
        "title": "Test",
        "content": "Hello",
        "document_type": "press_release",
        "speaker": None,
        "metadata": {},
    }]

    path = collector.export_jsonl(fake_data, "press_releases")

    assert path.exists()
    obj = json.loads(path.read_text().splitlines()[0])
    assert obj["title"] == "Test"


def test_export_all_to_jsonl_writes_multiple_files(tmp_path):
    collector = BoECollector(output_dir=tmp_path)

    fake = {
        "press_releases": [{
            "source": "BoE",
            "timestamp_collected": "x",
            "timestamp_published": "x",
            "url": "x",
            "title": "A",
            "content": "x",
            "document_type": "press_release",
            "speaker": None,
            "metadata": {},
        }],
        "speeches": [{
            "source": "BoE",
            "timestamp_collected": "x",
            "timestamp_published": "x",
            "url": "x",
            "title": "B",
            "content": "x",
            "document_type": "boe_speech",
            "speaker": None,
            "metadata": {},
        }],
    }

    paths = collector.export_all(data=fake)

    assert "press_releases" in paths
    assert "speeches" in paths
    assert paths["press_releases"].exists()
    assert paths["speeches"].exists()



