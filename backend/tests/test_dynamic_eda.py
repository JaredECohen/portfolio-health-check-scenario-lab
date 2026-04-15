from __future__ import annotations

import asyncio

import pandas as pd

from app.models.schemas import AnalysisPlan, DynamicEDAResult, EDAFinding, Holding, HypotheticalPosition, QuestionType, ScenarioAnalytics, ScenarioDelta
from app.services.analytics import AnalyticsService
from app.services.dynamic_eda import DynamicEDAService


def _price_frame(start: str, periods: int, base: float, step: float) -> pd.DataFrame:
    index = pd.date_range(start=start, periods=periods, freq="B")
    return pd.DataFrame(
        {"adjusted_close": [base + (value * step) for value in range(periods)]},
        index=index,
    )


def _series_frame(start: str, periods: int, values: list[float], freq: str = "B") -> pd.DataFrame:
    index = pd.date_range(start=start, periods=periods, freq=freq)
    return pd.DataFrame({"value": values[:periods]}, index=index)


class StubAlphaVantage:
    async def get_treasury_yield(self, maturity: str = "10year") -> pd.DataFrame:
        if maturity == "2year":
            values = [4.6 + ((idx % 6) * 0.025) - ((idx % 4) * 0.01) for idx in range(80)]
        else:
            values = [4.0 + ((idx % 5) * 0.02) - ((idx % 3) * 0.008) for idx in range(80)]
        return _series_frame("2024-01-02", 80, values)

    async def get_federal_funds_rate(self) -> pd.DataFrame:
        return _series_frame("2024-01-02", 80, [5.25 for _idx in range(80)])

    async def get_inflation_expectation(self) -> pd.DataFrame:
        return _series_frame("2023-01-01", 24, [2.1 + (idx * 0.02) for idx in range(24)], freq="MS")

    async def get_cpi(self) -> pd.DataFrame:
        return _series_frame("2023-01-01", 24, [300 + idx for idx in range(24)], freq="MS")

    async def get_wti(self) -> pd.DataFrame:
        return _series_frame("2024-01-02", 80, [70 + (idx * 0.4) for idx in range(80)])

    async def get_brent(self) -> pd.DataFrame:
        return _series_frame("2024-01-02", 80, [75 + (idx * 0.3) for idx in range(80)])

    async def get_natural_gas(self) -> pd.DataFrame:
        return _series_frame("2024-01-02", 80, [2.5 + (idx * 0.02) for idx in range(80)])


class StubEIAService:
    async def get_petroleum_storage_snapshot(self) -> dict:
        return {
            "report_date": "2025-08-22",
            "commercial_crude": {
                "level_million_bbl": 426.708,
                "previous_million_bbl": 422.458,
                "weekly_change_million_bbl": 4.25,
            },
            "total_ex_spr": {
                "level_million_bbl": 1608.159,
                "previous_million_bbl": 1602.587,
                "weekly_change_million_bbl": 5.572,
            },
        }

    async def get_natgas_storage_snapshot(self) -> dict:
        return {
            "report_date": "2025-08-22",
            "total_lower_48": {
                "working_gas_bcf": 3555.0,
                "net_change_bcf": -43.0,
                "year_ago_bcf": 3342.0,
                "five_year_avg_bcf": 3443.0,
                "vs_5y_pct": 3.3,
            },
        }


class StubNewsIntelService:
    async def collect(self, *, question: str, tickers: list[str], topics: list[str], limit_per_source: int = 12):  # noqa: ARG002
        from app.models.schemas import NewsArticle, NewsIntelResult, NewsSourceStats

        return NewsIntelResult(
            query=question,
            retrieval_sources=["Alpha Vantage NEWS_SENTIMENT", "GDELT DOC 2.0"],
            articles=[
                NewsArticle(
                    source="Alpha Vantage NEWS_SENTIMENT",
                    source_type="news",
                    title="Fed and inflation pressures hit tech",
                    url="https://example.com/fed-tech",
                    published_at="2025-08-22T12:00:00+00:00",
                    domain="example.com",
                    tickers=tickers[:2],
                    topics=topics[:2],
                    sentiment=-0.2,
                    relevance=0.9,
                )
            ],
            source_stats=[
                NewsSourceStats(
                    source="Alpha Vantage NEWS_SENTIMENT",
                    article_count=1,
                    avg_sentiment=-0.2,
                    latest_published_at="2025-08-22T12:00:00+00:00",
                )
            ],
            dominant_topics=topics[:2],
            caveats=[],
        )

    async def get_natgas_storage_snapshot(self) -> dict:
        return {
            "report_date": "2025-08-22",
            "total_lower_48": {
                "working_gas_bcf": 3555.0,
                "net_change_bcf": -43.0,
                "year_ago_bcf": 3342.0,
                "five_year_avg_bcf": 3443.0,
                "vs_5y_pct": 3.3,
            },
        }


class StubStockDatasetBuilder:
    async def build_cross_section(self, **_: object) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "ticker": "AAPL",
                    "sector": "Technology",
                    "company_name": "Apple Inc",
                    "effective_observations": 80,
                    "trailing_return": 0.32,
                    "return_63d": 0.15,
                    "return_21d": 0.05,
                    "forward_21d_return": 0.04,
                    "annualized_volatility": 0.22,
                    "beta_vs_benchmark": 1.05,
                    "correlation_vs_benchmark": 0.88,
                    "net_margin": 0.24,
                    "operating_margin": 0.31,
                    "gross_margin": 0.45,
                    "current_ratio": 1.2,
                    "debt_to_revenue": 0.45,
                },
                {
                    "ticker": "MSFT",
                    "sector": "Technology",
                    "company_name": "Microsoft Corp",
                    "effective_observations": 80,
                    "trailing_return": 0.28,
                    "return_63d": 0.12,
                    "return_21d": 0.03,
                    "forward_21d_return": 0.03,
                    "annualized_volatility": 0.18,
                    "beta_vs_benchmark": 0.98,
                    "correlation_vs_benchmark": 0.85,
                    "net_margin": 0.33,
                    "operating_margin": 0.42,
                    "gross_margin": 0.68,
                    "current_ratio": 1.4,
                    "debt_to_revenue": 0.18,
                },
                {
                    "ticker": "JPM",
                    "sector": "Financials",
                    "company_name": "JPMorgan Chase",
                    "effective_observations": 80,
                    "trailing_return": 0.14,
                    "return_63d": 0.08,
                    "return_21d": 0.02,
                    "forward_21d_return": 0.01,
                    "annualized_volatility": 0.16,
                    "beta_vs_benchmark": 0.83,
                    "correlation_vs_benchmark": 0.72,
                    "net_margin": 0.29,
                    "operating_margin": 0.36,
                    "gross_margin": 0.52,
                    "current_ratio": 0.95,
                    "debt_to_revenue": 0.62,
                },
                {
                    "ticker": "XOM",
                    "sector": "Energy",
                    "company_name": "Exxon Mobil",
                    "effective_observations": 80,
                    "trailing_return": 0.09,
                    "return_63d": 0.03,
                    "return_21d": -0.01,
                    "forward_21d_return": -0.02,
                    "annualized_volatility": 0.27,
                    "beta_vs_benchmark": 1.18,
                    "correlation_vs_benchmark": 0.66,
                    "net_margin": 0.11,
                    "operating_margin": 0.14,
                    "gross_margin": 0.24,
                    "current_ratio": 1.05,
                    "debt_to_revenue": 0.74,
                },
                {
                    "ticker": "PEP",
                    "sector": "Consumer Staples",
                    "company_name": "PepsiCo",
                    "effective_observations": 80,
                    "trailing_return": 0.06,
                    "return_63d": 0.01,
                    "return_21d": 0.0,
                    "forward_21d_return": 0.01,
                    "annualized_volatility": 0.12,
                    "beta_vs_benchmark": 0.62,
                    "correlation_vs_benchmark": 0.54,
                    "net_margin": 0.09,
                    "operating_margin": 0.13,
                    "gross_margin": 0.38,
                    "current_ratio": 0.88,
                    "debt_to_revenue": 0.57,
                },
            ]
        )


def test_enrich_with_scenario_adds_findings_and_tables() -> None:
    analytics = AnalyticsService()
    holdings = [
        Holding(ticker="AAPL", shares=10, company_name="Apple Inc", sector="Technology"),
        Holding(ticker="MSFT", shares=7, company_name="Microsoft Corp", sector="Technology"),
    ]
    after_holdings = [
        *holdings,
        Holding(ticker="JPM", shares=9, company_name="JPMorgan Chase", sector="Financial Services"),
    ]
    price_history = {
        "AAPL": _price_frame("2024-01-02", 80, 100, 1.4),
        "MSFT": _price_frame("2024-01-02", 80, 180, 1.0),
        "JPM": _price_frame("2024-01-02", 80, 140, 0.6),
    }
    benchmark_history = _price_frame("2024-01-02", 80, 400, 0.9)

    baseline_bundle = analytics.compute_baseline(
        holdings=holdings,
        benchmark_symbol="SPY",
        price_history={ticker: price_history[ticker] for ticker in ("AAPL", "MSFT")},
        benchmark_history=benchmark_history,
        risk_free_rate=0.02,
    )
    after_bundle = analytics.compute_baseline(
        holdings=after_holdings,
        benchmark_symbol="SPY",
        price_history=price_history,
        benchmark_history=benchmark_history,
        risk_free_rate=0.02,
    )
    scenario = ScenarioAnalytics(
        label="Add JPM",
        hypothetical_position=HypotheticalPosition(ticker="JPM", target_weight=0.05),
        before_metrics=baseline_bundle.baseline.metrics,
        after_metrics=after_bundle.baseline.metrics,
        deltas=[
            ScenarioDelta(
                metric=key,
                before=baseline_bundle.metrics_map.get(key),
                after=after_bundle.metrics_map.get(key),
                delta=(
                    None
                    if baseline_bundle.metrics_map.get(key) is None or after_bundle.metrics_map.get(key) is None
                    else after_bundle.metrics_map[key] - baseline_bundle.metrics_map[key]
                ),
            )
            for key in baseline_bundle.metrics_map
        ],
        before_sector_exposures=baseline_bundle.baseline.sector_exposures,
        after_sector_exposures=after_bundle.baseline.sector_exposures,
        before_positions=baseline_bundle.baseline.positions,
        after_positions=after_bundle.baseline.positions,
    )
    base_dynamic_eda = DynamicEDAResult(
        workflow="what_if",
        question_type=QuestionType.what_if_addition,
        findings=[
            EDAFinding(
                headline="Base what-if finding",
                evidence=["Baseline evidence only."],
                metrics={},
            )
        ],
        tables=[],
        data_sources=[],
    )

    service = DynamicEDAService(alpha_vantage=None)  # type: ignore[arg-type]
    enriched = service.enrich_with_scenario(
        dynamic_eda=base_dynamic_eda,
        scenario=scenario,
        baseline_bundle=baseline_bundle,
        after_bundle=after_bundle,
        question="What happens if I add JPM?",
    )

    table_names = {table.name for table in enriched.tables}
    assert len(enriched.findings) >= 4
    assert "Scenario Metric Comparison" in table_names
    assert "Sector Exposure Shifts" in table_names
    assert "Largest Holding Weight Changes" in table_names
    assert enriched.scenario_analysis is not None


def test_rates_workflow_routes_multiple_macro_sources() -> None:
    analytics = AnalyticsService()
    holdings = [
        Holding(ticker="AAPL", shares=10, company_name="Apple Inc", sector="Technology"),
        Holding(ticker="XOM", shares=8, company_name="Exxon Mobil", sector="Energy"),
    ]
    price_history = {
        "AAPL": _price_frame("2024-01-02", 80, 100, 1.0),
        "XOM": _price_frame("2024-01-02", 80, 90, 0.7),
    }
    benchmark_history = _price_frame("2024-01-02", 80, 400, 0.9)
    baseline_bundle = analytics.compute_baseline(
        holdings=holdings,
        benchmark_symbol="SPY",
        price_history=price_history,
        benchmark_history=benchmark_history,
        risk_free_rate=0.02,
    )
    dynamic = DynamicEDAService(
        StubAlphaVantage(),
        eia_service=StubEIAService(),
        news_intel_service=StubNewsIntelService(),
    )
    plan = AnalysisPlan(
        question_type=QuestionType.rates_macro,
        objective="reduce_macro_sensitivity",
        explanation="Rates test",
        dynamic_workflow="rates_macro_regime",
        macro_themes=["rates", "inflation"],
        preferred_data_sources=["TREASURY_YIELD_10Y", "CPI"],
    )

    result = asyncio.run(
        dynamic.execute(
            plan=plan,
            question="How exposed is this portfolio to rates, inflation, and Fed pressure?",
            baseline_bundle=baseline_bundle,
        )
    )

    routed = {item.series for item in result.data_sources}
    table_names = {table.name for table in result.tables}
    assert "TREASURY_YIELD_10Y" in routed
    assert "FEDERAL_FUNDS_RATE" in routed
    assert "INFLATION_EXPECTATION" in routed
    assert "CPI" in routed
    assert len(result.findings) >= 4
    assert "Rates Sensitivity Screen" in table_names
    assert "Rates Macro Context" in table_names
    assert "Normalized News Feed" in table_names
    assert any("screens multiple rates lenses" in finding.headline for finding in result.findings)


def test_geopolitical_workflow_routes_eia_context() -> None:
    analytics = AnalyticsService()
    holdings = [
        Holding(ticker="AAPL", shares=10, company_name="Apple Inc", sector="Technology"),
        Holding(ticker="XOM", shares=8, company_name="Exxon Mobil", sector="Energy"),
    ]
    price_history = {
        "AAPL": _price_frame("2024-01-02", 80, 100, 1.0),
        "XOM": _price_frame("2024-01-02", 80, 90, 1.6),
    }
    benchmark_history = _price_frame("2024-01-02", 80, 400, 0.2)
    baseline_bundle = analytics.compute_baseline(
        holdings=holdings,
        benchmark_symbol="SPY",
        price_history=price_history,
        benchmark_history=benchmark_history,
        risk_free_rate=0.02,
    )
    dynamic = DynamicEDAService(
        StubAlphaVantage(),
        eia_service=StubEIAService(),
        news_intel_service=StubNewsIntelService(),
    )
    plan = AnalysisPlan(
        question_type=QuestionType.geopolitical_war,
        objective="reduce_macro_sensitivity",
        explanation="War test",
        dynamic_workflow="geopolitical_stress",
        macro_themes=["energy"],
    )

    result = asyncio.run(
        dynamic.execute(
            plan=plan,
            question="What happens if war drives oil and natural gas higher?",
            baseline_bundle=baseline_bundle,
        )
    )

    routed = {item.series for item in result.data_sources}
    assert {"WTI", "BRENT", "NATURAL_GAS", "EIA_PETROLEUM_STATUS", "EIA_NATGAS_STORAGE"}.issubset(routed)
    assert any(table.name == "EIA Energy Inventory Context" for table in result.tables)
    assert result.news_intel is not None


def test_question_text_alone_does_not_expand_routed_data_sources() -> None:
    analytics = AnalyticsService()
    holdings = [
        Holding(ticker="AAPL", shares=10, company_name="Apple Inc", sector="Technology"),
    ]
    price_history = {
        "AAPL": _price_frame("2024-01-02", 80, 100, 1.0),
    }
    benchmark_history = _price_frame("2024-01-02", 80, 400, 0.9)
    baseline_bundle = analytics.compute_baseline(
        holdings=holdings,
        benchmark_symbol="SPY",
        price_history=price_history,
        benchmark_history=benchmark_history,
        risk_free_rate=0.02,
    )
    dynamic = DynamicEDAService(StubAlphaVantage())
    plan = AnalysisPlan(
        question_type=QuestionType.rates_macro,
        objective="reduce_macro_sensitivity",
        explanation="Sparse planner output",
        dynamic_workflow="rates_macro_regime",
    )

    result = asyncio.run(
        dynamic.execute(
            plan=plan,
            question="How exposed is this portfolio to rates, inflation, and Fed pressure?",
            baseline_bundle=baseline_bundle,
        )
    )

    assert result.data_sources == []


def test_factor_cross_section_builds_cross_section_tables() -> None:
    analytics = AnalyticsService()
    holdings = [
        Holding(ticker="AAPL", shares=10, company_name="Apple Inc", sector="Technology"),
        Holding(ticker="MSFT", shares=8, company_name="Microsoft Corp", sector="Technology"),
        Holding(ticker="JPM", shares=7, company_name="JPMorgan Chase", sector="Financials"),
    ]
    price_history = {
        "AAPL": _price_frame("2024-01-02", 80, 100, 1.0),
        "MSFT": _price_frame("2024-01-02", 80, 180, 0.9),
        "JPM": _price_frame("2024-01-02", 80, 140, 0.5),
    }
    benchmark_history = _price_frame("2024-01-02", 80, 400, 0.7)
    baseline_bundle = analytics.compute_baseline(
        holdings=holdings,
        benchmark_symbol="SPY",
        price_history=price_history,
        benchmark_history=benchmark_history,
        risk_free_rate=0.02,
    )
    dynamic = DynamicEDAService(
        StubAlphaVantage(),
        news_intel_service=StubNewsIntelService(),
        stock_dataset_builder=StubStockDatasetBuilder(),
    )
    plan = AnalysisPlan(
        question_type=QuestionType.factor_cross_section,
        objective="compare_factor_exposures",
        explanation="Cross-sectional factor test",
        dynamic_workflow="factor_cross_section",
        relevant_tickers=["XOM", "PEP"],
        preferred_data_sources=["SEC_FILINGS", "ALPHA_VANTAGE_NEWS_SENTIMENT"],
    )

    result = asyncio.run(
        dynamic.execute(
            plan=plan,
            question="Compare sector returns and whether profitability or leverage correlates with returns.",
            baseline_bundle=baseline_bundle,
        )
    )

    table_names = {table.name for table in result.tables}
    assert "Stock Factor Comparison" in table_names
    assert "Sector Return Comparison" in table_names
    assert "Metric Correlations vs Returns" in table_names
    assert "Metric Quantile Buckets" in table_names
    assert "Regression Diagnostics" in table_names
    assert any("cross-sectional dataframe" in finding.headline.lower() or "builds a stock-level dataframe" in finding.headline.lower() for finding in result.findings)
    correlation_table = next(table for table in result.tables if table.name == "Metric Correlations vs Returns")
    assert any(row["metric"] == "net_margin" for row in correlation_table.rows)
