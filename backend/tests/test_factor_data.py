from __future__ import annotations

import asyncio
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import pandas as pd

from app.database import Database
from app.models.schemas import AnalysisPlan, Holding, QuestionType, TickerMetadata
from app.services.analytics import AnalyticsService
from app.services.dynamic_eda import DynamicEDAService
from app.services.feature_store import FeatureStore
from app.services.ingestion.factor_returns import FactorReturnsIngestionService
from app.services.stock_dataset_builder import StockDatasetBuilder


def _zip_payload(member_name: str, content: str) -> bytes:
    buffer = BytesIO()
    with ZipFile(buffer, "w") as archive:
        archive.writestr(member_name, content)
    return buffer.getvalue()


class StubFactorIngestionService(FactorReturnsIngestionService):
    def __init__(self, database: Database, payloads: dict[str, bytes]) -> None:
        super().__init__(database)
        self.payloads = payloads

    def _fetch_dataset_rows(self, *, dataset, start_date, end_date):  # noqa: ANN001
        return self._parse_zipped_csv(
            payload=self.payloads[str(dataset["dataset_id"])],
            dataset=dataset,
            start_date=start_date,
            end_date=end_date,
        )


class DummyAlphaVantage:
    def __init__(self, price_history: dict[str, pd.DataFrame]) -> None:
        self.price_history = price_history

    async def get_daily_adjusted(self, symbol: str, *, outputsize: str = "compact") -> pd.DataFrame:  # noqa: ARG002
        return self.price_history[symbol]


class StubTickerMetadataService:
    def __init__(self, rows: list[TickerMetadata]) -> None:
        self.rows = rows

    def all(self) -> list[TickerMetadata]:
        return self.rows

    def get(self, ticker: str) -> TickerMetadata | None:
        ticker = ticker.upper().strip()
        for row in self.rows:
            if row.ticker == ticker:
                return row
        return None


def _seed_factor_store(database: Database) -> FeatureStore:
    dates = pd.date_range("2024-01-02", periods=60, freq="B")
    ff5_rows = [
        f"{item.strftime('%Y%m%d')},{0.10 + (idx % 5) * 0.01:.2f},{0.02 - (idx % 3) * 0.01:.2f},{-0.08 + (idx % 4) * 0.01:.2f},{0.03 + (idx % 2) * 0.01:.2f},{0.01:.2f},{0.01:.2f}"
        for idx, item in enumerate(dates)
    ]
    momentum_rows = [
        f"{item.strftime('%Y%m%d')},{0.12 - (idx % 4) * 0.01:.2f}"
        for idx, item in enumerate(dates)
    ]
    ff5_daily = "This file was created for testing.\n,Mkt-RF,SMB,HML,RMW,CMA,RF\n" + "\n".join(ff5_rows) + "\n\nAnnual Factors: January-December\n"
    momentum_daily = "This file was created for testing.\n,Mom\n" + "\n".join(momentum_rows) + "\n\nAnnual Factors: January-December\n"
    payloads = {
        "KEN_FRENCH_FF5_DAILY": _zip_payload("ff5_daily.csv", ff5_daily),
        "KEN_FRENCH_MOMENTUM_DAILY": _zip_payload("mom_daily.csv", momentum_daily),
    }
    service = StubFactorIngestionService(database, payloads)
    service.sync_datasets(
        dataset_ids=["KEN_FRENCH_FF5_DAILY", "KEN_FRENCH_MOMENTUM_DAILY"],
    )
    return FeatureStore(database)


def test_factor_sync_populates_local_factor_frame(tmp_path: Path) -> None:
    database = Database(tmp_path / "app.db")
    database.initialize()

    store = _seed_factor_store(database)
    frame = store.factor_model_frame(frequency="daily")

    assert list(frame.columns) == ["CMA", "HML", "MKT_RF", "MOM", "RF", "RMW", "SMB"]
    assert frame.loc[pd.Timestamp("2024-01-02"), "MKT_RF"] == 0.001
    assert frame.loc[pd.Timestamp("2024-01-03"), "MOM"] == 0.0011

    with database.connect() as connection:
        run = connection.execute(
            "SELECT source, status, row_count FROM ingestion_runs WHERE source = 'factor_returns' ORDER BY started_at DESC LIMIT 1"
        ).fetchone()

    assert run is not None
    assert run["status"] == "success"
    assert run["row_count"] == 420


def test_stock_dataset_builder_adds_factor_exposure_columns_from_local_store(tmp_path: Path) -> None:
    database = Database(tmp_path / "app.db")
    database.initialize()
    store = _seed_factor_store(database)
    index = pd.date_range("2024-01-02", periods=55, freq="B")
    returns = pd.Series(
        [0.0012 + (idx * 0.00002) for idx in range(len(index) - 1)],
        index=index[1:],
    )
    prices = pd.Series(100.0, index=index)
    for idx, daily_return in enumerate(returns, start=1):
        prices.iloc[idx] = prices.iloc[idx - 1] * (1 + daily_return)
    benchmark = pd.Series(400.0, index=index)
    for idx in range(1, len(index)):
        benchmark.iloc[idx] = benchmark.iloc[idx - 1] * (1 + 0.0009)
    alpha_vantage = DummyAlphaVantage(
        {
            "AAPL": pd.DataFrame({"adjusted_close": prices}, index=index),
            "SPY": pd.DataFrame({"adjusted_close": benchmark}, index=index),
        }
    )
    metadata = StubTickerMetadataService(
        [
            TickerMetadata(ticker="AAPL", company_name="Apple", cik="1", sector="Technology", exchange="NASDAQ"),
        ]
    )
    builder = StockDatasetBuilder(
        alpha_vantage=alpha_vantage,  # type: ignore[arg-type]
        ticker_metadata_service=metadata,
        feature_store=store,
    )

    frame = asyncio.run(
        builder.build_cross_section(
            tickers=["AAPL"],
            benchmark_symbol="SPY",
            lookback_days=55,
            comparison_universe="portfolio_only",
        )
    )

    assert "factor_market_beta" in frame.columns
    assert "factor_growth_tilt_beta" in frame.columns
    assert "factor_momentum_beta" in frame.columns
    assert frame.iloc[0]["factor_primary_exposure"]


def test_dynamic_eda_performance_uses_local_factor_attribution(tmp_path: Path) -> None:
    database = Database(tmp_path / "app.db")
    database.initialize()
    store = _seed_factor_store(database)

    analytics = AnalyticsService()
    index = pd.date_range("2024-01-02", periods=80, freq="B")
    factor_cycle = [0.0015, 0.0012, 0.0009, 0.0011, 0.0013]
    price_history: dict[str, pd.DataFrame] = {}
    for ticker, base in {"AAPL": 100.0, "MSFT": 150.0}.items():
        series = pd.Series(base, index=index)
        for idx in range(1, len(index)):
            market = factor_cycle[idx % len(factor_cycle)]
            hml = -0.0008 + (idx % 3) * 0.0001
            mom = 0.001 + (idx % 4) * 0.00005
            daily_return = 0.0001 + (1.0 * market) - (0.6 * hml) + (0.8 * mom)
            series.iloc[idx] = series.iloc[idx - 1] * (1 + daily_return)
        price_history[ticker] = pd.DataFrame({"adjusted_close": series}, index=index)
    benchmark = pd.Series(400.0, index=index)
    for idx in range(1, len(index)):
        benchmark.iloc[idx] = benchmark.iloc[idx - 1] * (1 + 0.0009)
    benchmark_history = pd.DataFrame({"adjusted_close": benchmark}, index=index)

    bundle = analytics.compute_baseline(
        holdings=[
            Holding(ticker="AAPL", shares=10, company_name="Apple", sector="Technology"),
            Holding(ticker="MSFT", shares=8, company_name="Microsoft", sector="Technology"),
        ],
        benchmark_symbol="SPY",
        price_history=price_history,
        benchmark_history=benchmark_history,
        risk_free_rate=0.02,
    )
    service = DynamicEDAService(
        alpha_vantage=DummyAlphaVantage(price_history | {"SPY": benchmark_history}),  # type: ignore[arg-type]
        feature_store=store,
    )
    result = asyncio.run(
        service.execute(
            plan=AnalysisPlan(
                question_type=QuestionType.performance_drivers,
                objective="performance",
                explanation="test",
                dynamic_workflow="performance_drivers",
            ),
            question="What is driving portfolio performance?",
            baseline_bundle=bundle,
        )
    )

    table_names = {table.name for table in result.tables}
    assert "Portfolio Factor Attribution" in table_names
    assert "Holding Factor Exposures" in table_names
    assert any("Local factor regression" in finding.headline for finding in result.findings)
