"""Unit tests for ECB data collector."""

from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import requests

from data.ingestion.base_collector import BaseCollector
from data.ingestion.ecb_collector import ECBCollector

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_response(content: str | bytes, status: int = 200) -> Mock:
    """Build a mock requests.Response."""
    resp = Mock()
    resp.status_code = status
    resp.content = content.encode() if isinstance(content, str) else content
    resp.ok = 200 <= status < 300
    if status >= 400:
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError(response=resp)
    else:
        resp.raise_for_status = Mock()
    return resp


SAMPLE_POLICY_CSV = (
    "KEY,FREQ,REF_AREA,CURRENCY,PROVIDER_FM,INSTRUMENT_FM,PROVIDER_FM_ID,"
    "DATA_TYPE_FM,TIME_PERIOD,OBS_VALUE\n"
    "FM.B.U2.EUR.4F.KR.DFR.LEV,B,U2,EUR,4F,KR,DFR,LEV,2023-02-08,2.5\n"
    "FM.B.U2.EUR.4F.KR.MRR_FR.LEV,B,U2,EUR,4F,KR,MRR_FR,LEV,2023-02-08,3.0\n"
)

SAMPLE_EXCHANGE_CSV = (
    "KEY,FREQ,CURRENCY,CURRENCY_DENOM,EXR_TYPE,EXR_SUFFIX,TIME_PERIOD,OBS_VALUE\n"
    "EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2023-01-02,1.0683\n"
    "EXR.D.GBP.EUR.SP00.A,D,GBP,EUR,SP00,A,2023-01-02,0.8863\n"
    "EXR.D.JPY.EUR.SP00.A,D,JPY,EUR,SP00,A,2023-01-02,139.62\n"
    "EXR.D.CHF.EUR.SP00.A,D,CHF,EUR,SP00,A,2023-01-02,0.9873\n"
)


# ---------------------------------------------------------------------------
# BaseCollector
# ---------------------------------------------------------------------------


class TestBaseCollector:
    """Verify the abstract base class contract."""

    def test_cannot_instantiate_directly(self):
        with pytest.raises(TypeError):
            BaseCollector(output_dir=Path("/tmp"))  # type: ignore[abstract]

    def test_export_csv_writes_file(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        df = pd.DataFrame({"date": ["2023-01-01"], "rate": [1.05], "source": ["ecb"]})

        path = collector.export_csv(df, "test_dataset")

        assert path.exists()
        assert path.name.startswith("ecb_test_dataset_")
        assert path.suffix == ".csv"
        assert len(pd.read_csv(path)) == 1

    def test_export_csv_naming_convention(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        df = pd.DataFrame({"a": [1]})
        date_str = datetime.now().strftime("%Y%m%d")

        path = collector.export_csv(df, "my_data")

        assert path.name == f"ecb_my_data_{date_str}.csv"

    def test_export_csv_raises_on_empty(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        with pytest.raises(ValueError, match="Cannot export empty DataFrame"):
            collector.export_csv(pd.DataFrame(), "empty")


# ---------------------------------------------------------------------------
# ECBDataset
# ---------------------------------------------------------------------------


class TestECBDataset:
    def test_frozen(self):
        ds = ECBCollector.POLICY_RATES
        with pytest.raises(Exception):
            ds.name = "changed"  # type: ignore[misc]

    def test_policy_rates_definition(self):
        ds = ECBCollector.POLICY_RATES
        assert ds.dataflow == "FM"
        assert "MRR_FR" in ds.key
        assert "DFR" in ds.key
        assert ds.frequency == "B"

    def test_exchange_rates_definition(self):
        ds = ECBCollector.EXCHANGE_RATES
        assert ds.dataflow == "EXR"
        assert all(c in ds.key for c in ["USD", "GBP", "JPY", "CHF"])
        assert ds.frequency == "D"


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------


class TestECBCollectorInit:
    def test_default_output_dir(self, tmp_path, monkeypatch):
        monkeypatch.setattr("shared.config.Config.DATA_DIR", tmp_path)
        collector = ECBCollector()
        assert collector.output_dir == tmp_path / "datasets" / "ecb" / "raw"
        assert collector.output_dir.exists()

    def test_custom_output_dir(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        assert collector.output_dir == tmp_path

    def test_session_has_retry_logic(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        adapter = collector._session.get_adapter("https://")
        # Verify HTTPAdapter with retry is configured (duck-typed check)
        assert adapter is not None
        assert hasattr(adapter, "max_retries")


# ---------------------------------------------------------------------------
# URL building
# ---------------------------------------------------------------------------


class TestBuildUrl:
    def test_basic_structure(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        url = collector._build_url(ECBCollector.EXCHANGE_RATES)
        assert url.startswith(f"{ECBCollector.BASE_URL}/data/EXR/")
        assert "format=csvdata" in url

    def test_date_range_params(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        url = collector._build_url(
            ECBCollector.EXCHANGE_RATES,
            start_period="2023-01-01",
            end_period="2023-12-31",
        )
        assert "startPeriod=2023-01-01" in url
        assert "endPeriod=2023-12-31" in url

    def test_updated_after_url_encoded(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        url = collector._build_url(
            ECBCollector.EXCHANGE_RATES,
            updated_after=datetime(2024, 1, 1, 12, 0, 0),
        )
        assert "updatedAfter=" in url
        assert "%3A" in url  # encoded colon


# ---------------------------------------------------------------------------
# _fetch
# ---------------------------------------------------------------------------


class TestFetch:
    def test_success(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        with patch.object(
            collector._session, "get", return_value=_make_response(SAMPLE_EXCHANGE_CSV)
        ):
            df = collector._fetch(ECBCollector.EXCHANGE_RATES)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 4

    def test_empty_response_returns_empty_df(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        with patch.object(collector._session, "get", return_value=_make_response(b"")):
            df = collector._fetch(ECBCollector.EXCHANGE_RATES)
        assert df.empty

    def test_404_raises_value_error(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        with patch.object(collector._session, "get", return_value=_make_response("", 404)):
            with pytest.raises(ValueError, match="Invalid ECB dataset"):
                collector._fetch(ECBCollector.EXCHANGE_RATES)

    def test_timeout_propagates(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        with patch.object(collector._session, "get", side_effect=requests.exceptions.Timeout()):
            with pytest.raises(requests.exceptions.Timeout):
                collector._fetch(ECBCollector.EXCHANGE_RATES)


# ---------------------------------------------------------------------------
# Processing — policy rates
# ---------------------------------------------------------------------------


class TestProcessPolicyRates:
    def test_happy_path(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        raw = pd.DataFrame(
            {
                "TIME_PERIOD": ["2023-02-08", "2023-02-08"],
                "OBS_VALUE": ["2.5", "3.0"],
                "PROVIDER_FM_ID": ["DFR", "MRR_FR"],
                "FREQ": ["B", "B"],
            }
        )
        result = collector._process_policy_rates(raw)

        assert list(result.columns) == ["date", "rate_type", "rate", "frequency", "unit", "source"]
        assert len(result) == 2
        assert result["rate"].dtype == float
        assert set(result["rate_type"]) == {
            "Deposit Facility Rate",
            "Main Refinancing Operations Rate",
        }
        assert (result["source"] == "ECB").all()
        assert (result["unit"] == "Percent").all()

    def test_empty_input(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        result = collector._process_policy_rates(pd.DataFrame())
        assert result.empty
        assert "date" in result.columns

    def test_unknown_rate_codes_filtered(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        raw = pd.DataFrame(
            {
                "TIME_PERIOD": ["2023-02-08"],
                "OBS_VALUE": ["2.5"],
                "PROVIDER_FM_ID": ["UNKNOWN"],
                "FREQ": ["B"],
            }
        )
        assert collector._process_policy_rates(raw).empty

    def test_invalid_obs_value_dropped(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        raw = pd.DataFrame(
            {
                "TIME_PERIOD": ["2023-02-08", "2023-02-08"],
                "OBS_VALUE": ["2.5", "N/A"],
                "PROVIDER_FM_ID": ["DFR", "MRR_FR"],
                "FREQ": ["B", "B"],
            }
        )
        result = collector._process_policy_rates(raw)
        assert len(result) == 1
        assert result.iloc[0]["rate"] == 2.5

    def test_sorted_by_date(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        raw = pd.DataFrame(
            {
                "TIME_PERIOD": ["2023-06-01", "2022-01-01"],
                "OBS_VALUE": ["3.5", "0.0"],
                "PROVIDER_FM_ID": ["DFR", "DFR"],
                "FREQ": ["B", "B"],
            }
        )
        result = collector._process_policy_rates(raw)
        assert result.iloc[0]["rate"] == 0.0


# ---------------------------------------------------------------------------
# Processing — exchange rates
# ---------------------------------------------------------------------------


class TestProcessExchangeRates:
    def test_happy_path(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        raw = pd.DataFrame(
            {
                "TIME_PERIOD": ["2023-01-02", "2023-01-02"],
                "OBS_VALUE": ["1.0683", "0.8863"],
                "CURRENCY": ["USD", "GBP"],
                "FREQ": ["D", "D"],
            }
        )
        result = collector._process_exchange_rates(raw)

        assert list(result.columns) == ["date", "currency_pair", "rate", "frequency", "source"]
        assert set(result["currency_pair"]) == {"EUR/USD", "EUR/GBP"}
        assert result["rate"].dtype == float

    def test_empty_input(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        assert collector._process_exchange_rates(pd.DataFrame()).empty

    def test_unsupported_currencies_filtered(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        raw = pd.DataFrame(
            {
                "TIME_PERIOD": ["2023-01-02"],
                "OBS_VALUE": ["1.5"],
                "CURRENCY": ["XXX"],
                "FREQ": ["D"],
            }
        )
        assert collector._process_exchange_rates(raw).empty

    def test_sorted_by_date_then_pair(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        raw = pd.DataFrame(
            {
                "TIME_PERIOD": ["2023-01-03", "2023-01-02", "2023-01-02"],
                "OBS_VALUE": ["1.05", "1.07", "0.88"],
                "CURRENCY": ["USD", "USD", "GBP"],
                "FREQ": ["D", "D", "D"],
            }
        )
        result = collector._process_exchange_rates(raw)
        assert result.iloc[0]["currency_pair"] == "EUR/GBP"
        assert result.iloc[2]["currency_pair"] == "EUR/USD"


# ---------------------------------------------------------------------------
# collect() and collect_policy_rates() / collect_exchange_rates()
# ---------------------------------------------------------------------------


class TestCollect:
    def test_collect_returns_both_datasets(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        with (
            patch.object(collector, "collect_policy_rates", return_value=pd.DataFrame({"a": [1]})),
            patch.object(
                collector, "collect_exchange_rates", return_value=pd.DataFrame({"b": [2]})
            ),
            patch("time.sleep"),
        ):
            result = collector.collect()

        assert set(result.keys()) == {"policy_rates", "exchange_rates"}

    def test_collect_default_date_range(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        with (
            patch.object(collector, "_fetch", return_value=pd.DataFrame()) as mock_fetch,
            patch("time.sleep"),
        ):
            collector.collect()

        calls = mock_fetch.call_args_list
        assert len(calls) == 2
        start_period = calls[0][1]["start_period"]
        end_period = calls[0][1]["end_period"]
        days = (
            datetime.strptime(end_period, "%Y-%m-%d") - datetime.strptime(start_period, "%Y-%m-%d")
        ).days
        assert 720 <= days <= 740

    def test_collect_policy_rates_delegates_to_fetch(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        with patch.object(collector, "_fetch", return_value=pd.DataFrame()) as mock_fetch:
            collector.collect_policy_rates(
                start_date=datetime(2023, 1, 1), end_date=datetime(2023, 12, 31)
            )
        mock_fetch.assert_called_once_with(
            ECBCollector.POLICY_RATES,
            start_period="2023-01-01",
            end_period="2023-12-31",
        )

    def test_collect_exchange_rates_passes_updated_after(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        ts = datetime(2024, 6, 1, 12, 0, 0)
        with patch.object(collector, "_fetch", return_value=pd.DataFrame()) as mock_fetch:
            collector.collect_exchange_rates(updated_after=ts)
        assert mock_fetch.call_args[1]["updated_after"] == ts


# ---------------------------------------------------------------------------
# incremental_update
# ---------------------------------------------------------------------------


class TestIncrementalUpdate:
    def test_policy_rates_always_full(self, tmp_path):
        """Policy rates must be fetched without updatedAfter (FM limitation)."""
        collector = ECBCollector(output_dir=tmp_path)
        last_update = datetime.now() - timedelta(days=7)

        with (
            patch.object(collector, "_fetch", return_value=pd.DataFrame()) as mock_fetch,
            patch("time.sleep"),
        ):
            collector.incremental_update(last_update)

        policy_call = mock_fetch.call_args_list[0]
        assert policy_call[1].get("updated_after") is None

    def test_exchange_rates_use_updated_after(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        last_update = datetime(2024, 1, 1, 12, 0, 0)

        with (
            patch.object(collector, "_fetch", return_value=pd.DataFrame()) as mock_fetch,
            patch("time.sleep"),
        ):
            collector.incremental_update(last_update)

        exr_call = mock_fetch.call_args_list[1]
        assert exr_call[1]["updated_after"] == last_update


# ---------------------------------------------------------------------------
# health_check
# ---------------------------------------------------------------------------


class TestHealthCheck:
    def test_returns_true_on_ok_response(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        with patch.object(collector._session, "get", return_value=_make_response("OK", 200)):
            assert collector.health_check() is True

    def test_returns_false_on_error_response(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        with patch.object(collector._session, "get", return_value=_make_response("", 503)):
            assert collector.health_check() is False

    def test_returns_false_on_network_error(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        with patch.object(
            collector._session, "get", side_effect=requests.exceptions.ConnectionError()
        ):
            assert collector.health_check() is False


# ---------------------------------------------------------------------------
# Integration: _fetch → _process pipeline with sample CSV fixtures
# ---------------------------------------------------------------------------


class TestFetchProcessPipeline:
    def test_policy_rates_end_to_end(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        with patch.object(
            collector._session, "get", return_value=_make_response(SAMPLE_POLICY_CSV)
        ):
            result = collector.collect_policy_rates(
                start_date=datetime(2023, 1, 1), end_date=datetime(2023, 12, 31)
            )

        assert len(result) == 2
        assert set(result["rate_type"]) == {
            "Deposit Facility Rate",
            "Main Refinancing Operations Rate",
        }

    def test_exchange_rates_end_to_end(self, tmp_path):
        collector = ECBCollector(output_dir=tmp_path)
        with patch.object(
            collector._session, "get", return_value=_make_response(SAMPLE_EXCHANGE_CSV)
        ):
            result = collector.collect_exchange_rates(
                start_date=datetime(2023, 1, 1), end_date=datetime(2023, 12, 31)
            )

        assert len(result) == 4
        assert set(result["currency_pair"]) == {"EUR/USD", "EUR/GBP", "EUR/JPY", "EUR/CHF"}
