from __future__ import annotations

import asyncio
import json
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import pandas as pd

from app.models.schemas import AnalysisPlan, Holding, QuestionType
from app.database import Database
from app.services.analytics import AnalyticsService
from app.services.dynamic_eda import DynamicEDAService
from app.services.feature_store import FeatureStore
from app.services.ingestion.fred import FredIngestionService
from app.services.ingestion.sec_bulk import SecBulkIngestionService


def _write_zip(path: Path, members: dict[str, dict]) -> None:
    with ZipFile(path, "w") as archive:
        for name, payload in members.items():
            archive.writestr(name, json.dumps(payload))


def test_sec_bulk_loads_company_facts_and_submissions(tmp_path: Path) -> None:
    database = Database(tmp_path / "app.db")
    database.initialize()
    companyfacts_zip = tmp_path / "companyfacts.zip"
    submissions_zip = tmp_path / "submissions.zip"
    _write_zip(
        companyfacts_zip,
        {
            "CIK0000320193.json": {
                "cik": 320193,
                "entityName": "Apple Inc.",
                "tickers": ["AAPL"],
                "facts": {
                    "us-gaap": {
                        "Revenues": {
                            "units": {
                                "USD": [
                                    {
                                        "end": "2024-09-28",
                                        "val": 391035000000,
                                        "fy": 2024,
                                        "fp": "FY",
                                        "form": "10-K",
                                        "filed": "2024-11-01",
                                        "frame": "CY2024",
                                        "accn": "0000320193-24-000123",
                                    }
                                ]
                            }
                        },
                        "NetIncomeLoss": {
                            "units": {
                                "USD": [
                                    {
                                        "end": "2024-09-28",
                                        "val": 93736000000,
                                        "fy": 2024,
                                        "fp": "FY",
                                        "form": "10-K",
                                        "filed": "2024-11-01",
                                        "frame": "CY2024",
                                        "accn": "0000320193-24-000123",
                                    }
                                ]
                            }
                        },
                    }
                },
            }
        },
    )
    _write_zip(
        submissions_zip,
        {
            "CIK0000320193.json": {
                "cik": "0000320193",
                "name": "Apple Inc.",
                "tickers": ["AAPL"],
                "filings": {
                    "recent": {
                        "accessionNumber": ["0000320193-24-000123"],
                        "form": ["10-K"],
                        "filingDate": ["2024-11-01"],
                        "acceptanceDateTime": ["2024-11-01T16:30:00"],
                        "primaryDocument": ["aapl-20240928x10k.htm"],
                        "isXBRL": [1],
                        "isInlineXBRL": [1],
                    }
                },
            }
        },
    )

    service = SecBulkIngestionService(database, user_agent="test@example.com")
    result = service.load_from_directory(
        companyfacts_path=companyfacts_zip,
        submissions_path=submissions_zip,
    )

    assert result.companies == 1
    assert result.fundamentals >= 2
    assert result.filings == 1

    store = FeatureStore(database)
    latest = store.latest_company_fundamentals("AAPL")
    revenue_trend = store.trailing_fundamental_trend("AAPL", "Revenues")

    assert any(item["metric"] == "Revenues" for item in latest)
    assert revenue_trend[0]["value"] == 391035000000


class StubFredService(FredIngestionService):
    def _fetch_series_observations(self, *, series_id, start_date, end_date):  # noqa: ANN001, ARG002
        return [
            {
                "date": "2024-01-02",
                "value": 4.25,
                "vintage_date": "2024-01-02",
                "metadata": {"realtime_start": "2024-01-02", "realtime_end": "2024-01-02"},
            },
            {
                "date": "2024-01-03",
                "value": 4.3,
                "vintage_date": "2024-01-03",
                "metadata": {"realtime_start": "2024-01-03", "realtime_end": "2024-01-03"},
            },
        ]


class StubSecSampleIngestionService(SecBulkIngestionService):
    def __init__(self, database: Database, responses: dict[str, dict]) -> None:
        super().__init__(database, user_agent="test@example.com")
        self.responses = responses

    def _request_json(self, url: str, *, client=None) -> dict:  # noqa: ANN001, ARG002
        return self.responses[url]


def test_sec_sample_bootstrap_loads_from_dim_company(tmp_path: Path) -> None:
    database = Database(tmp_path / "app.db")
    database.initialize()
    with database.connect() as connection:
        connection.execute(
            """
            INSERT INTO dim_company(ticker, cik, company_name, sector, industry, exchange, updated_at)
            VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            ("AAPL", "0000320193", "Apple Inc.", "Technology", "Consumer Electronics", "NASDAQ", "2026-01-01T00:00:00+00:00"),
        )
    responses = {
        "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json": {
            "cik": 320193,
            "entityName": "Apple Inc.",
            "tickers": ["AAPL"],
            "facts": {
                "us-gaap": {
                    "Revenues": {
                        "units": {
                            "USD": [
                                {
                                    "end": "2024-09-28",
                                    "val": 391035000000,
                                    "fy": 2024,
                                    "fp": "FY",
                                    "form": "10-K",
                                    "filed": "2024-11-01",
                                    "frame": "CY2024",
                                    "accn": "0000320193-24-000123",
                                }
                            ]
                        }
                    }
                }
            },
        },
        "https://data.sec.gov/submissions/CIK0000320193.json": {
            "cik": "0000320193",
            "name": "Apple Inc.",
            "tickers": ["AAPL"],
            "filings": {
                "recent": {
                    "accessionNumber": ["0000320193-24-000123"],
                    "form": ["10-K"],
                    "filingDate": ["2024-11-01"],
                    "acceptanceDateTime": ["2024-11-01T16:30:00"],
                    "primaryDocument": ["aapl-20240928x10k.htm"],
                    "isXBRL": [1],
                    "isInlineXBRL": [1],
                }
            },
        },
    }

    service = StubSecSampleIngestionService(database, responses)
    result = service.bootstrap_sample_from_dim_company(max_companies=1)

    assert result.companies == 1
    assert result.fundamentals == 1
    assert result.filings == 1

    store = FeatureStore(database)
    latest = store.latest_company_fundamentals("AAPL")
    assert latest[0]["metric"] == "Revenues"
    with database.connect() as connection:
        run = connection.execute(
            "SELECT source, status, row_count, details_json FROM ingestion_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
    assert run["source"] == "sec_sample"
    assert run["status"] == "success"
    assert run["row_count"] == 3


def test_sec_sample_bootstrap_records_resume_metadata(tmp_path: Path) -> None:
    database = Database(tmp_path / "app.db")
    database.initialize()
    with database.connect() as connection:
        connection.execute(
            """
            INSERT INTO dim_company(ticker, cik, company_name, sector, industry, exchange, updated_at)
            VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            ("AAPL", "CIK0000320193", "Apple Inc.", "Technology", "Consumer Electronics", "NASDAQ", "2026-01-01T00:00:00+00:00"),
        )
    responses = {
        "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json": {
            "cik": 320193,
            "entityName": "Apple Inc.",
            "tickers": ["AAPL"],
            "facts": {},
        },
        "https://data.sec.gov/submissions/CIK0000320193.json": {
            "cik": "0000320193",
            "name": "Apple Inc.",
            "tickers": ["AAPL"],
            "filings": {"recent": {"accessionNumber": [], "form": [], "filingDate": [], "acceptanceDateTime": [], "primaryDocument": [], "isXBRL": [], "isInlineXBRL": []}},
        },
    }

    service = StubSecSampleIngestionService(database, responses)
    result = service.bootstrap_sample_from_dim_company(max_companies=1, offset=0, source="sec_refresh")

    assert result.requested_companies == 1
    assert result.requested_unique_ciks == 1
    assert result.failed_companies == 0
    assert result.next_offset == 1

    with database.connect() as connection:
        run = connection.execute(
            "SELECT details_json FROM ingestion_runs WHERE source = 'sec_refresh' ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
    assert run is not None
    details = json.loads(run["details_json"])
    assert details["offset"] == 0
    assert details["requested_companies"] == 1
    assert details["requested_unique_ciks"] == 1
    assert details["failed_companies"] == 0
    assert details["next_offset"] == 1


def test_sec_refresh_cleanup_and_resume_offset(tmp_path: Path) -> None:
    database = Database(tmp_path / "app.db")
    database.initialize()
    service = SecBulkIngestionService(database, user_agent="test@example.com")
    with database.connect() as connection:
        connection.execute(
            """
            INSERT INTO ingestion_runs(run_id, source, started_at, status, row_count, details_json)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            ("sec-bulk-stale", "sec_bulk", "2026-04-15T04:17:54+00:00", "running", 0, None),
        )
        connection.execute(
            """
            INSERT INTO ingestion_runs(run_id, source, started_at, status, row_count, details_json)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (
                "sec-refresh-done",
                "sec_refresh",
                "2026-04-15T05:28:20+00:00",
                "partial_success",
                10,
                json.dumps({"offset": 200, "requested_companies": 50, "next_offset": 250}),
            ),
        )
        connection.execute(
            """
            INSERT INTO ingestion_runs(run_id, source, started_at, status, row_count, details_json)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            ("sec-refresh-stale", "sec_refresh", "2026-04-15T05:30:34+00:00", "running", 0, None),
        )

    cleaned = service.cleanup_stale_runs(
        sources=["sec_bulk", "sec_refresh"],
        reason="Superseded by refresh_sec_company_data resume workflow.",
    )

    assert cleaned == 2
    assert service.resume_offset_for_refresh(source="sec_refresh", fallback_offset=0) == 250

    with database.connect() as connection:
        rows = connection.execute(
            "SELECT run_id, status, completed_at, details_json FROM ingestion_runs WHERE status = 'failed' ORDER BY run_id"
        ).fetchall()
    assert {row["run_id"] for row in rows} == {"sec-bulk-stale", "sec-refresh-stale"}
    assert all(row["completed_at"] for row in rows)
    assert all("cleanup_reason" in json.loads(row["details_json"]) for row in rows)


class StubAlphaVantage:
    async def get_treasury_yield(self, maturity: str = "10year") -> pd.DataFrame:  # noqa: ARG002
        index = pd.date_range("2024-01-02", periods=80, freq="B")
        return pd.DataFrame({"value": [4.0 + (idx * 0.01) for idx in range(80)]}, index=index)

    async def get_federal_funds_rate(self) -> pd.DataFrame:
        index = pd.date_range("2024-01-02", periods=80, freq="B")
        return pd.DataFrame({"value": [5.25 for _idx in range(80)]}, index=index)

    async def get_inflation_expectation(self) -> pd.DataFrame:
        index = pd.date_range("2023-01-01", periods=24, freq="MS")
        return pd.DataFrame({"value": [2.1 + (idx * 0.02) for idx in range(24)]}, index=index)

    async def get_cpi(self) -> pd.DataFrame:
        index = pd.date_range("2023-01-01", periods=24, freq="MS")
        return pd.DataFrame({"value": [300 + idx for idx in range(24)]}, index=index)


def test_fred_sync_populates_series_and_feature_snapshot(tmp_path: Path) -> None:
    database = Database(tmp_path / "app.db")
    database.initialize()
    service = StubFredService(database, api_key="fred-key")

    row_count = service.sync_curated_series(series_ids=["DGS10", "UNRATE"])

    assert row_count == 4
    store = FeatureStore(database)
    snapshot = store.macro_snapshot(["DGS10", "UNRATE"])
    assert len(snapshot) == 2
    assert {item["series_id"] for item in snapshot} == {"DGS10", "UNRATE"}


def test_dynamic_eda_uses_local_feature_store_tables(tmp_path: Path) -> None:
    database = Database(tmp_path / "app.db")
    database.initialize()
    fred = StubFredService(database, api_key="fred-key")
    fred.sync_curated_series(series_ids=["DGS10", "DGS2", "FEDFUNDS", "CPIAUCSL", "UNRATE", "VIXCLS"])
    companyfacts_zip = tmp_path / "companyfacts.zip"
    submissions_zip = tmp_path / "submissions.zip"
    _write_zip(
        companyfacts_zip,
        {
            "CIK0000320193.json": {
                "cik": 320193,
                "entityName": "Apple Inc.",
                "tickers": ["AAPL"],
                "facts": {
                    "us-gaap": {
                        "Revenues": {
                            "units": {
                                "USD": [
                                    {"end": "2024-09-28", "val": 100.0, "fy": 2024, "fp": "FY", "form": "10-K", "filed": "2024-11-01", "frame": "CY2024", "accn": "accn-1"}
                                ]
                            }
                        }
                    }
                },
            }
        },
    )
    _write_zip(
        submissions_zip,
        {
            "CIK0000320193.json": {
                "name": "Apple Inc.",
                "tickers": ["AAPL"],
                "filings": {"recent": {"accessionNumber": [], "form": [], "filingDate": [], "acceptanceDateTime": [], "primaryDocument": [], "isXBRL": [], "isInlineXBRL": []}},
            }
        },
    )
    sec = SecBulkIngestionService(database, user_agent="test@example.com")
    sec.load_from_directory(companyfacts_path=companyfacts_zip, submissions_path=submissions_zip)

    analytics = AnalyticsService()
    index = pd.date_range("2024-01-02", periods=80, freq="B")
    price_history = {"AAPL": pd.DataFrame({"adjusted_close": [100 + idx for idx in range(80)]}, index=index)}
    benchmark_history = pd.DataFrame({"adjusted_close": [400 + idx for idx in range(80)]}, index=index)
    bundle = analytics.compute_baseline(
        holdings=[Holding(ticker="AAPL", shares=10, company_name="Apple Inc", sector="Technology")],
        benchmark_symbol="SPY",
        price_history=price_history,
        benchmark_history=benchmark_history,
        risk_free_rate=0.02,
    )
    service = DynamicEDAService(
        alpha_vantage=StubAlphaVantage(),  # type: ignore[arg-type]
        feature_store=FeatureStore(database),
    )
    service.analyze_rates_regimes = lambda baseline_bundle: asyncio.sleep(0, result={  # type: ignore[method-assign]
        "sample_days": 80,
        "sample_start": "2024-01-02",
        "sample_end": "2024-04-22",
        "yield_up": None,
        "yield_down": None,
        "yield_up_attribution": [],
        "yield_down_attribution": [],
        "recent_events": [],
    })
    result = asyncio.run(
        service.execute(
            plan=AnalysisPlan(
                question_type=QuestionType.rates_macro,
                objective="reduce_macro_sensitivity",
                explanation="test",
                dynamic_workflow="rates_macro",
            ),
            question="How exposed is this portfolio to rates?",
            baseline_bundle=bundle,
        )
    )

    table_names = {table.name for table in result.tables}
    assert "Local Macro Feature Snapshot" in table_names
