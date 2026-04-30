import csv
import io
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from src.ingestion.collectors.base_collector import BaseCollector

ALL_COLUMNS = [
    "GLOBALEVENTID",
    "SQLDATE",
    "MonthYear",
    "Year",
    "FractionDate",
    "Actor1Code",
    "Actor1Name",
    "Actor1CountryCode",
    "Actor1KnownGroupCode",
    "Actor1EthnicCode",
    "Actor1Religion1Code",
    "Actor1Religion2Code",
    "Actor1Type1Code",
    "Actor1Type2Code",
    "Actor1Type3Code",
    "Actor2Code",
    "Actor2Name",
    "Actor2CountryCode",
    "Actor2KnownGroupCode",
    "Actor2EthnicCode",
    "Actor2Religion1Code",
    "Actor2Religion2Code",
    "Actor2Type1Code",
    "Actor2Type2Code",
    "Actor2Type3Code",
    "IsRootEvent",
    "EventCode",
    "EventBaseCode",
    "EventRootCode",
    "QuadClass",
    "GoldsteinScale",
    "NumMentions",
    "NumSources",
    "NumArticles",
    "AvgTone",
    "Actor1Geo_Type",
    "Actor1Geo_FullName",
    "Actor1Geo_CountryCode",
    "Actor1Geo_ADM1Code",
    "Actor1Geo_Lat",
    "Actor1Geo_Long",
    "Actor1Geo_FeatureID",
    "Actor2Geo_Type",
    "Actor2Geo_FullName",
    "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code",
    "Actor2Geo_Lat",
    "Actor2Geo_Long",
    "Actor2Geo_FeatureID",
    "ActionGeo_Type",
    "ActionGeo_FullName",
    "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code",
    "ActionGeo_Lat",
    "ActionGeo_Long",
    "ActionGeo_FeatureID",
    "DATEADDED",
    "SOURCEURL",
]

KEEP_COLUMNS = [
    "GLOBALEVENTID",
    "SQLDATE",
    "Actor1Code",
    "Actor1Name",
    "Actor1CountryCode",
    "Actor1Type1Code",
    "Actor2Code",
    "Actor2Name",
    "Actor2CountryCode",
    "Actor2Type1Code",
    "EventCode",
    "EventBaseCode",
    "EventRootCode",
    "QuadClass",
    "GoldsteinScale",
    "NumMentions",
    "NumSources",
    "NumArticles",
    "AvgTone",
    "Actor1Geo_FullName",
    "Actor1Geo_CountryCode",
    "Actor2Geo_FullName",
    "Actor2Geo_CountryCode",
    "ActionGeo_FullName",
    "ActionGeo_CountryCode",
    "SOURCEURL",
]


class GDELTEventsCollector(BaseCollector):
    """Collector for GDELT Events data (daily Bronze parquet files)."""

    SOURCE_NAME = "gdelt_events"
    DEFAULT_LOOKBACK_DAYS: int = 30

    ZONE_COUNTRY_CODES = {
        "USD": ["USA"],
        "EUR": [
            "EUR",
            "AUT",
            "BEL",
            "CYP",
            "DEU",
            "ESP",
            "EST",
            "FIN",
            "FRA",
            "GRC",
            "HRV",
            "IRL",
            "ITA",
            "LTU",
            "LUX",
            "LVA",
            "MLT",
            "NLD",
            "PRT",
            "SVK",
            "SVN",
        ],
        "GBP": ["GBR"],
        "JPY": ["JPN"],
        "CHF": ["CHE"],
    }

    ALL_ZONE_CODES = [code for codes in ZONE_COUNTRY_CODES.values() for code in codes]

    def __init__(
        self,
        output_dir: Path,
        log_file: Path | None = None,
        sleep_between: float = 1.5,
    ) -> None:
        super().__init__(output_dir=output_dir, log_file=log_file)
        self.sleep_between = float(sleep_between)

    def _day_path(self, d: date) -> Path:
        return self.output_dir / f"{d.year}" / f"{d.month:02d}" / f"{d.strftime('%Y%m%d')}.parquet"

    def _download_zip(self, d: date) -> bytes | None:
        url = f"http://data.gdeltproject.org/events/{d.strftime('%Y%m%d')}.export.CSV.zip"
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                return resp.content
            if resp.status_code == 404:
                self.logger.warning("GDELT file not found: %s", url)
                return None
            self.logger.warning("GDELT unexpected status %s for %s", resp.status_code, url)
            return None
        except requests.RequestException as exc:
            self.logger.warning("GDELT download error for %s: %s", url, exc)
            return None

    def _parse_and_filter(self, content: bytes) -> pd.DataFrame:
        try:
            with ZipFile(io.BytesIO(content)) as zf:
                name = zf.namelist()[0]
                with zf.open(name) as member:
                    text_stream = io.TextIOWrapper(member, encoding="utf-8", errors="replace")
                    df = pd.read_csv(
                        text_stream,
                        sep="\t",
                        header=None,
                        names=ALL_COLUMNS,
                        dtype=str,
                        quoting=csv.QUOTE_NONE,
                        on_bad_lines="skip",
                        usecols=KEEP_COLUMNS,
                    )
        except Exception as exc:
            self.logger.warning("Failed to parse zip content: %s", exc)
            return pd.DataFrame(columns=self._normalized_columns())

        if df.empty:
            return pd.DataFrame(columns=self._normalized_columns())

        mask = df["Actor1CountryCode"].isin(self.ALL_ZONE_CODES) | df["Actor2CountryCode"].isin(
            self.ALL_ZONE_CODES
        )
        df = df[mask].copy()
        if df.empty:
            return pd.DataFrame(columns=self._normalized_columns())

        records = []
        for row in df.to_dict("records"):
            records.append(self._normalize_row(row))

        return pd.DataFrame.from_records(records, columns=self._normalized_columns())

    def _normalized_columns(self) -> list[str]:
        return [
            "source",
            "timestamp_collected",
            "event_id",
            "event_date",
            "actor1_name",
            "actor1_code",
            "actor1_country_code",
            "actor1_type1_code",
            "actor2_name",
            "actor2_code",
            "actor2_country_code",
            "actor2_type1_code",
            "event_code",
            "event_base_code",
            "event_root_code",
            "quad_class",
            "goldstein_scale",
            "num_mentions",
            "num_sources",
            "num_articles",
            "avg_tone",
            "actor1_geo_country_code",
            "actor1_geo_full_name",
            "actor2_geo_country_code",
            "actor2_geo_full_name",
            "action_geo_country_code",
            "action_geo_full_name",
            "source_url",
        ]

    def _normalize_row(self, row: dict) -> dict:
        def clean(value: object) -> str | None:
            if value is None:
                return None
            text = str(value).strip()
            return text or None

        gid = clean(row.get("GLOBALEVENTID"))
        if gid and gid.endswith(".0"):
            try:
                gid = str(int(float(gid)))
            except Exception:
                pass

        sql = clean(row.get("SQLDATE"))
        event_date = None
        if sql:
            try:
                event_date = datetime.strptime(sql, "%Y%m%d").strftime("%Y%m%d")
            except Exception:
                event_date = sql

        return {
            "source": self.SOURCE_NAME,
            "timestamp_collected": datetime.now(timezone.utc).isoformat(),
            "event_id": gid,
            "event_date": event_date,
            "actor1_name": clean(row.get("Actor1Name")),
            "actor1_code": clean(row.get("Actor1Code")),
            "actor1_country_code": clean(row.get("Actor1CountryCode")),
            "actor1_type1_code": clean(row.get("Actor1Type1Code")),
            "actor2_name": clean(row.get("Actor2Name")),
            "actor2_code": clean(row.get("Actor2Code")),
            "actor2_country_code": clean(row.get("Actor2CountryCode")),
            "actor2_type1_code": clean(row.get("Actor2Type1Code")),
            "event_code": clean(row.get("EventCode")),
            "event_base_code": clean(row.get("EventBaseCode")),
            "event_root_code": clean(row.get("EventRootCode")),
            "quad_class": row.get("QuadClass"),
            "goldstein_scale": row.get("GoldsteinScale"),
            "num_mentions": row.get("NumMentions"),
            "num_sources": row.get("NumSources"),
            "num_articles": row.get("NumArticles"),
            "avg_tone": row.get("AvgTone"),
            "actor1_geo_country_code": clean(row.get("Actor1Geo_CountryCode")),
            "actor1_geo_full_name": clean(row.get("Actor1Geo_FullName")),
            "actor2_geo_country_code": clean(row.get("Actor2Geo_CountryCode")),
            "actor2_geo_full_name": clean(row.get("Actor2Geo_FullName")),
            "action_geo_country_code": clean(row.get("ActionGeo_CountryCode")),
            "action_geo_full_name": clean(row.get("ActionGeo_FullName")),
            "source_url": clean(row.get("SOURCEURL")),
        }

    def _write_parquet(self, df: pd.DataFrame, path: Path) -> None:
        cols = self._normalized_columns()
        for c in cols:
            if c not in df.columns:
                df[c] = pd.Series(dtype="string")

        df = df[cols]
        table = pa.Table.from_pandas(df, preserve_index=False)
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            table,
            str(path),
            compression="zstd",
            compression_level=12,
            use_dictionary=True,
            write_statistics=False,
        )

    def collect(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        backfill: bool = False,
    ) -> dict[str, int]:
        if not start_date or not end_date:
            raise ValueError("start_date and end_date must be provided.")

        current_day = start_date.date()
        end_day = end_date.date()
        results: dict[str, int] = {}

        while current_day <= end_day:
            path = self._day_path(current_day)
            date_key = current_day.strftime("%Y%m%d")
            if path.exists() and not backfill:
                self.logger.info("Skipping existing day %s", date_key)
                current_day += timedelta(days=1)
                continue

            content = self._download_zip(current_day)
            if content is None:
                # write empty sentinel with normalized columns
                empty = pd.DataFrame(columns=self._normalized_columns())
                self._write_parquet(empty, path)
                results[date_key] = 0
                current_day += timedelta(days=1)
                time.sleep(self.sleep_between)
                continue

            df = self._parse_and_filter(content)
            row_count = 0 if df is None else len(df)
            if df is None or df.empty:
                df = pd.DataFrame(columns=self._normalized_columns())

            self._write_parquet(df, path)
            results[date_key] = row_count

            current_day += timedelta(days=1)
            time.sleep(self.sleep_between)

        return results

    def health_check(self) -> bool:
        # check yesterday's file exists or is reachable
        yesterday = datetime.utcnow().date() - timedelta(days=1)
        url = f"http://data.gdeltproject.org/events/{yesterday.strftime('%Y%m%d')}.export.CSV.zip"
        try:
            resp = requests.head(url, timeout=10)
            return resp.status_code < 500
        except requests.RequestException:
            return False
