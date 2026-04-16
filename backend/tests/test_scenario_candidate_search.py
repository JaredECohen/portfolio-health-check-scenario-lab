from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from app.models.schemas import Holding, OptimizationPreference, ScenarioDelta, TickerMetadata
from app.services.analytics import AnalyticsService
from app.services.scenario import ScenarioService


class StubTickerMetadataService:
    def __init__(self, rows: list[TickerMetadata]) -> None:
        self.rows = rows

    def all(self) -> list[TickerMetadata]:
        return self.rows


class StubFeatureStore:
    def __init__(self, fundamentals_by_ticker: dict[str, dict[str, float]]) -> None:
        self.fundamentals_by_ticker = fundamentals_by_ticker

    def latest_company_fundamentals_panel(
        self,
        tickers: list[str],
        metrics: list[str] | None = None,
        *,
        chunk_size: int = 250,  # noqa: ARG002
    ) -> list[dict[str, object]]:
        selected_metrics = set(metrics or [])
        rows: list[dict[str, object]] = []
        for ticker in tickers:
            for metric, value in self.fundamentals_by_ticker.get(ticker, {}).items():
                if selected_metrics and metric not in selected_metrics:
                    continue
                rows.append(
                    {
                        "ticker": ticker,
                        "metric": metric,
                        "value": value,
                        "period_end": "2024-12-31",
                        "fiscal_period": "FY",
                        "fiscal_year": 2024,
                        "unit": "USD",
                        "form_type": "10-K",
                        "filed_at": "2025-02-01",
                    }
                )
        return rows

    def factor_model_frame(self, **_: object) -> pd.DataFrame:
        return pd.DataFrame()


class StubStockDatasetBuilder:
    def __init__(self, frame: pd.DataFrame) -> None:
        self.frame = frame

    async def build_cross_section(self, *, tickers: list[str], **_: object) -> pd.DataFrame:
        return self.frame[self.frame["ticker"].isin(tickers)].reset_index(drop=True)


class StubAlphaVantage:
    def __init__(self, histories: dict[str, pd.DataFrame]) -> None:
        self.histories = histories

    async def get_daily_adjusted(self, symbol: str, *, outputsize: str = "compact") -> pd.DataFrame:  # noqa: ARG002
        return self.histories[symbol]


def _history(prices: list[float], index: pd.DatetimeIndex) -> pd.DataFrame:
    return pd.DataFrame({"adjusted_close": prices}, index=index)


def _build_baseline_bundle():
    index = pd.date_range("2024-01-02", periods=80, freq="B")
    baseline_history = {
        "AAPL": _history(
            [
                100.0,
                102.0,
                101.0,
                104.0,
                103.0,
                107.0,
                106.0,
                110.0,
            ]
            * 10,
            index,
        )
    }
    benchmark_history = _history(
        [
            400.0,
            401.0,
            402.0,
            403.0,
            404.0,
            405.0,
            406.0,
            407.0,
        ]
        * 10,
        index,
    )
    analytics = AnalyticsService()
    bundle = analytics.compute_baseline(
        holdings=[
            Holding(
                ticker="AAPL",
                shares=10,
                company_name="Apple",
                sector="Technology",
                cik="1",
                exchange="NASDAQ",
            )
        ],
        benchmark_symbol="SPY",
        price_history=baseline_history,
        benchmark_history=benchmark_history,
        risk_free_rate=0.02,
    )
    return bundle, index


@pytest.mark.anyio
async def test_rank_candidates_filters_low_quality_names_before_final_ranking() -> None:
    baseline_bundle, index = _build_baseline_bundle()
    metadata = StubTickerMetadataService(
        [
            TickerMetadata(ticker="AAPL", company_name="Apple", cik="1", sector="Technology", exchange="NASDAQ"),
            TickerMetadata(ticker="JNJ", company_name="Johnson & Johnson", cik="2", sector="Healthcare", exchange="NYSE"),
            TickerMetadata(ticker="DUK", company_name="Duke Energy", cik="3", sector="Utilities", exchange="NYSE"),
            TickerMetadata(ticker="BAD", company_name="Bad Co", cik="4", sector="Technology", exchange="NASDAQ"),
        ]
    )
    fundamentals = {
        "JNJ": {
            "Revenues": 1000.0,
            "GrossProfit": 690.0,
            "OperatingIncomeLoss": 220.0,
            "NetIncomeLoss": 165.0,
            "AssetsCurrent": 1200.0,
            "LiabilitiesCurrent": 800.0,
            "LongTermDebtNoncurrent": 320.0,
        },
        "DUK": {
            "Revenues": 1000.0,
            "GrossProfit": 430.0,
            "OperatingIncomeLoss": 130.0,
            "NetIncomeLoss": 95.0,
            "AssetsCurrent": 950.0,
            "LiabilitiesCurrent": 850.0,
            "LongTermDebtNoncurrent": 420.0,
        },
        "BAD": {
            "Revenues": 1000.0,
            "GrossProfit": 80.0,
            "OperatingIncomeLoss": -60.0,
            "NetIncomeLoss": -90.0,
            "AssetsCurrent": 700.0,
            "LiabilitiesCurrent": 1100.0,
            "LongTermDebtNoncurrent": 1600.0,
        },
    }
    cross_section = pd.DataFrame(
        [
            {
                "ticker": "JNJ",
                "sector": "Healthcare",
                "company_name": "Johnson & Johnson",
                "trailing_return": 0.18,
                "return_63d": 0.09,
                "return_21d": 0.04,
                "annualized_volatility": 0.12,
                "beta_vs_benchmark": 0.72,
                "correlation_vs_benchmark": 0.48,
                "operating_margin": 0.22,
                "net_margin": 0.165,
                "gross_margin": 0.69,
                "current_ratio": 1.5,
                "debt_to_revenue": 0.32,
            },
            {
                "ticker": "DUK",
                "sector": "Utilities",
                "company_name": "Duke Energy",
                "trailing_return": 0.11,
                "return_63d": 0.05,
                "return_21d": 0.02,
                "annualized_volatility": 0.09,
                "beta_vs_benchmark": 0.58,
                "correlation_vs_benchmark": 0.41,
                "operating_margin": 0.13,
                "net_margin": 0.095,
                "gross_margin": 0.43,
                "current_ratio": 1.12,
                "debt_to_revenue": 0.42,
            },
            {
                "ticker": "BAD",
                "sector": "Technology",
                "company_name": "Bad Co",
                "trailing_return": -0.14,
                "return_63d": -0.08,
                "return_21d": -0.03,
                "annualized_volatility": 0.42,
                "beta_vs_benchmark": 1.8,
                "correlation_vs_benchmark": 0.92,
                "operating_margin": -0.06,
                "net_margin": -0.09,
                "gross_margin": 0.08,
                "current_ratio": 0.64,
                "debt_to_revenue": 1.6,
            },
        ]
    )
    histories = {
        "JNJ": _history([100 + i * 0.35 for i in range(len(index))], index),
        "DUK": _history([100 + i * 0.22 for i in range(len(index))], index),
    }
    service = ScenarioService(
        analytics_service=AnalyticsService(),
        alpha_vantage=StubAlphaVantage(histories),  # type: ignore[arg-type]
        ticker_metadata=metadata,  # type: ignore[arg-type]
        candidate_universe_path=Path("unused.json"),
        feature_store=StubFeatureStore(fundamentals),  # type: ignore[arg-type]
        stock_dataset_builder=StubStockDatasetBuilder(cross_section),  # type: ignore[arg-type]
    )

    result = await service.rank_candidates(
        baseline_bundle=baseline_bundle,
        benchmark_symbol="SPY",
        objective="performance",
        lookback_days=80,
        max_candidates=3,
    )

    returned_tickers = [candidate.ticker for candidate in result.candidates]
    assert "BAD" not in returned_tickers
    assert returned_tickers[:2] == ["JNJ", "DUK"]
    assert "local fundamental quality filter" in result.method
    assert any("operating margin" in line.lower() for line in result.candidates[0].rationale)
    assert any("Started from 3 common-equity candidates" in line for line in result.screening_summary)


@pytest.mark.anyio
async def test_shortlist_universe_returns_screen_metrics_and_summary() -> None:
    baseline_bundle, _index = _build_baseline_bundle()
    metadata = StubTickerMetadataService(
        [
            TickerMetadata(ticker="AAPL", company_name="Apple", cik="1", sector="Technology", exchange="NASDAQ"),
            TickerMetadata(ticker="JNJ", company_name="Johnson & Johnson", cik="2", sector="Healthcare", exchange="NYSE"),
            TickerMetadata(ticker="DUK", company_name="Duke Energy", cik="3", sector="Utilities", exchange="NYSE"),
            TickerMetadata(ticker="BAD", company_name="Bad Co", cik="4", sector="Technology", exchange="NASDAQ"),
        ]
    )
    fundamentals = {
        "JNJ": {
            "Revenues": 1000.0,
            "GrossProfit": 690.0,
            "OperatingIncomeLoss": 220.0,
            "NetIncomeLoss": 165.0,
            "AssetsCurrent": 1200.0,
            "LiabilitiesCurrent": 800.0,
            "LongTermDebtNoncurrent": 320.0,
        },
        "DUK": {
            "Revenues": 1000.0,
            "GrossProfit": 430.0,
            "OperatingIncomeLoss": 130.0,
            "NetIncomeLoss": 95.0,
            "AssetsCurrent": 950.0,
            "LiabilitiesCurrent": 850.0,
            "LongTermDebtNoncurrent": 420.0,
        },
        "BAD": {
            "Revenues": 1000.0,
            "GrossProfit": 80.0,
            "OperatingIncomeLoss": -60.0,
            "NetIncomeLoss": -90.0,
            "AssetsCurrent": 700.0,
            "LiabilitiesCurrent": 1100.0,
            "LongTermDebtNoncurrent": 1600.0,
        },
    }
    cross_section = pd.DataFrame(
        [
            {
                "ticker": "JNJ",
                "sector": "Healthcare",
                "company_name": "Johnson & Johnson",
                "trailing_return": 0.18,
                "return_63d": 0.09,
                "return_21d": 0.04,
                "annualized_volatility": 0.12,
                "beta_vs_benchmark": 0.72,
                "correlation_vs_benchmark": 0.48,
                "operating_margin": 0.22,
                "net_margin": 0.165,
                "gross_margin": 0.69,
                "current_ratio": 1.5,
                "debt_to_revenue": 0.32,
            },
            {
                "ticker": "DUK",
                "sector": "Utilities",
                "company_name": "Duke Energy",
                "trailing_return": 0.11,
                "return_63d": 0.05,
                "return_21d": 0.02,
                "annualized_volatility": 0.09,
                "beta_vs_benchmark": 0.58,
                "correlation_vs_benchmark": 0.41,
                "operating_margin": 0.13,
                "net_margin": 0.095,
                "gross_margin": 0.43,
                "current_ratio": 1.12,
                "debt_to_revenue": 0.42,
            },
            {
                "ticker": "BAD",
                "sector": "Technology",
                "company_name": "Bad Co",
                "trailing_return": -0.14,
                "return_63d": -0.08,
                "return_21d": -0.03,
                "annualized_volatility": 0.42,
                "beta_vs_benchmark": 1.8,
                "correlation_vs_benchmark": 0.92,
                "operating_margin": -0.06,
                "net_margin": -0.09,
                "gross_margin": 0.08,
                "current_ratio": 0.64,
                "debt_to_revenue": 1.6,
            },
        ]
    )
    service = ScenarioService(
        analytics_service=AnalyticsService(),
        alpha_vantage=StubAlphaVantage({}),  # type: ignore[arg-type]
        ticker_metadata=metadata,  # type: ignore[arg-type]
        candidate_universe_path=Path("unused.json"),
        feature_store=StubFeatureStore(fundamentals),  # type: ignore[arg-type]
        stock_dataset_builder=StubStockDatasetBuilder(cross_section),  # type: ignore[arg-type]
    )

    shortlist = await service.shortlist_universe(
        baseline_bundle=baseline_bundle,
        objective="performance",
        lookback_days=80,
        max_candidates=2,
    )

    assert {item["ticker"] for item in shortlist["candidates"]} == {"JNJ", "DUK"}
    assert shortlist["candidates"][0]["screen_score"] is not None
    assert any(item["operating_margin"] == pytest.approx(0.22) for item in shortlist["candidates"])
    assert any("price-strength" in line for line in shortlist["screening_summary"])


@pytest.mark.anyio
async def test_shortlist_universe_uses_portfolio_correlation_for_metric_optimization() -> None:
    baseline_bundle, _index = _build_baseline_bundle()
    metadata = StubTickerMetadataService(
        [
            TickerMetadata(ticker="AAPL", company_name="Apple", cik="1", sector="Technology", exchange="NASDAQ"),
            TickerMetadata(ticker="LOWC", company_name="Low Correlation Co", cik="2", sector="Healthcare", exchange="NYSE"),
            TickerMetadata(ticker="HIGH", company_name="High Correlation Co", cik="3", sector="Healthcare", exchange="NYSE"),
        ]
    )
    fundamentals = {
        "LOWC": {
            "Revenues": 1000.0,
            "GrossProfit": 600.0,
            "OperatingIncomeLoss": 170.0,
            "NetIncomeLoss": 130.0,
            "AssetsCurrent": 1200.0,
            "LiabilitiesCurrent": 800.0,
            "LongTermDebtNoncurrent": 300.0,
        },
        "HIGH": {
            "Revenues": 1000.0,
            "GrossProfit": 640.0,
            "OperatingIncomeLoss": 190.0,
            "NetIncomeLoss": 145.0,
            "AssetsCurrent": 1200.0,
            "LiabilitiesCurrent": 800.0,
            "LongTermDebtNoncurrent": 300.0,
        },
    }
    cross_section = pd.DataFrame(
        [
            {
                "ticker": "LOWC",
                "sector": "Healthcare",
                "company_name": "Low Correlation Co",
                "trailing_return": 0.12,
                "return_vs_benchmark": 0.04,
                "return_63d": 0.06,
                "return_21d": 0.03,
                "annualized_volatility": 0.14,
                "beta_vs_benchmark": 0.78,
                "correlation_vs_benchmark": 0.55,
                "correlation_vs_portfolio": 0.18,
                "operating_margin": 0.17,
                "net_margin": 0.13,
                "gross_margin": 0.6,
                "current_ratio": 1.5,
                "debt_to_revenue": 0.3,
            },
            {
                "ticker": "HIGH",
                "sector": "Healthcare",
                "company_name": "High Correlation Co",
                "trailing_return": 0.16,
                "return_vs_benchmark": 0.08,
                "return_63d": 0.09,
                "return_21d": 0.04,
                "annualized_volatility": 0.13,
                "beta_vs_benchmark": 0.82,
                "correlation_vs_benchmark": 0.66,
                "correlation_vs_portfolio": 0.84,
                "operating_margin": 0.19,
                "net_margin": 0.145,
                "gross_margin": 0.64,
                "current_ratio": 1.45,
                "debt_to_revenue": 0.32,
            },
        ]
    )
    service = ScenarioService(
        analytics_service=AnalyticsService(),
        alpha_vantage=StubAlphaVantage({}),  # type: ignore[arg-type]
        ticker_metadata=metadata,  # type: ignore[arg-type]
        candidate_universe_path=Path("unused.json"),
        feature_store=StubFeatureStore(fundamentals),  # type: ignore[arg-type]
        stock_dataset_builder=StubStockDatasetBuilder(cross_section),  # type: ignore[arg-type]
    )

    shortlist = await service.shortlist_universe(
        baseline_bundle=baseline_bundle,
        objective="diversify",
        optimization_preferences=[
            OptimizationPreference(metric="average_pairwise_correlation", direction="minimize"),
        ],
        lookback_days=80,
        max_candidates=2,
    )

    assert [item["ticker"] for item in shortlist["candidates"]] == ["LOWC", "HIGH"]
    assert shortlist["candidates"][0]["correlation_vs_portfolio"] == pytest.approx(0.18)


def test_candidate_constraints_enforce_hard_return_preservation() -> None:
    deltas = [
        ScenarioDelta(metric="trailing_return", before=0.12, after=0.1, delta=-0.02),
        ScenarioDelta(metric="beta_vs_benchmark", before=1.05, after=0.92, delta=-0.13),
    ]

    assert not ScenarioService._meets_candidate_constraints(
        deltas,
        objective="reduce_macro_sensitivity",
        optimization_preferences=[
            OptimizationPreference(metric="beta_vs_benchmark", direction="minimize"),
            OptimizationPreference(metric="trailing_return", direction="maximize", hard_constraint=True),
        ],
    )
