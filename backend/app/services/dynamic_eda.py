from __future__ import annotations

import asyncio
import re
from typing import Any

import numpy as np
import pandas as pd

from app.models.schemas import (
    AnalysisPlan,
    AnalysisTable,
    DataSourceReference,
    DynamicEDAResult,
    EDAFinding,
    NewsIntelResult,
    PositionSnapshot,
    QuestionType,
    ScenarioAnalytics,
    SectorExposure,
)
from app.services.analytics import AnalyticsBundle
from app.services.alpha_vantage import AlphaVantageService
from app.services.eia import EIAService, EIAServiceError
from app.services.feature_store import FeatureStore
from app.services.news_intel import NewsIntelService
from app.services.sec_edgar import SecEdgarService
from app.services.stock_dataset_builder import StockDatasetBuilder
from app.services.ticker_metadata import TickerMetadataService


DATASET_CATALOG: dict[str, dict[str, Any]] = {
    "TREASURY_YIELD_10Y": {
        "source": "Alpha Vantage",
        "series": "TREASURY_YIELD_10Y",
        "category": "rates",
        "description": "U.S. 10Y Treasury yield, daily.",
        "url": "https://www.alphavantage.co/documentation/",
        "requires_api_key": True,
    },
    "TREASURY_YIELD_2Y": {
        "source": "Alpha Vantage",
        "series": "TREASURY_YIELD_2Y",
        "category": "rates",
        "description": "U.S. 2Y Treasury yield, daily.",
        "url": "https://www.alphavantage.co/documentation/",
        "requires_api_key": True,
    },
    "FEDERAL_FUNDS_RATE": {
        "source": "Alpha Vantage",
        "series": "FEDERAL_FUNDS_RATE",
        "category": "rates",
        "description": "Federal funds rate history.",
        "url": "https://www.alphavantage.co/documentation/",
        "requires_api_key": True,
    },
    "CPI": {
        "source": "Alpha Vantage",
        "series": "CPI",
        "category": "inflation",
        "description": "Consumer Price Index history.",
        "url": "https://www.alphavantage.co/documentation/",
        "requires_api_key": True,
    },
    "INFLATION_EXPECTATION": {
        "source": "Alpha Vantage",
        "series": "INFLATION_EXPECTATION",
        "category": "inflation",
        "description": "U.S. inflation expectations history.",
        "url": "https://www.alphavantage.co/documentation/",
        "requires_api_key": True,
    },
    "UNEMPLOYMENT": {
        "source": "Alpha Vantage",
        "series": "UNEMPLOYMENT",
        "category": "labor",
        "description": "U.S. unemployment rate history.",
        "url": "https://www.alphavantage.co/documentation/",
        "requires_api_key": True,
    },
    "RETAIL_SALES": {
        "source": "Alpha Vantage",
        "series": "RETAIL_SALES",
        "category": "growth",
        "description": "U.S. retail sales history.",
        "url": "https://www.alphavantage.co/documentation/",
        "requires_api_key": True,
    },
    "CONSUMER_SENTIMENT": {
        "source": "Alpha Vantage",
        "series": "CONSUMER_SENTIMENT",
        "category": "growth",
        "description": "University of Michigan consumer sentiment history.",
        "url": "https://www.alphavantage.co/documentation/",
        "requires_api_key": True,
    },
    "REAL_GDP": {
        "source": "Alpha Vantage",
        "series": "REAL_GDP",
        "category": "growth",
        "description": "U.S. real GDP history.",
        "url": "https://www.alphavantage.co/documentation/",
        "requires_api_key": True,
    },
    "WTI": {
        "source": "Alpha Vantage",
        "series": "WTI",
        "category": "energy",
        "description": "WTI crude oil prices.",
        "url": "https://www.alphavantage.co/documentation/",
        "requires_api_key": True,
    },
    "BRENT": {
        "source": "Alpha Vantage",
        "series": "BRENT",
        "category": "energy",
        "description": "Brent crude oil prices.",
        "url": "https://www.alphavantage.co/documentation/",
        "requires_api_key": True,
    },
    "NATURAL_GAS": {
        "source": "Alpha Vantage",
        "series": "NATURAL_GAS",
        "category": "energy",
        "description": "Natural gas prices.",
        "url": "https://www.alphavantage.co/documentation/",
        "requires_api_key": True,
    },
    "SEC_FILINGS": {
        "source": "SEC EDGAR",
        "series": "SEC_FILINGS",
        "category": "fundamental",
        "description": "Recent corporate filings for covered holdings.",
        "url": "https://www.sec.gov/edgar/search/",
        "requires_api_key": False,
    },
    "EARNINGS_TRANSCRIPTS": {
        "source": "Alpha Vantage",
        "series": "EARNINGS_TRANSCRIPTS",
        "category": "fundamental",
        "description": "Recent earnings call transcript metadata.",
        "url": "https://www.alphavantage.co/documentation/",
        "requires_api_key": True,
    },
    "ALPHA_VANTAGE_NEWS_SENTIMENT": {
        "source": "Alpha Vantage",
        "series": "ALPHA_VANTAGE_NEWS_SENTIMENT",
        "category": "news",
        "description": "Ticker- and topic-aware news feed with sentiment metadata.",
        "url": "https://www.alphavantage.co/documentation/",
        "requires_api_key": True,
    },
    "GDELT_DOC_2": {
        "source": "GDELT",
        "series": "GDELT_DOC_2",
        "category": "news",
        "description": "Free article discovery and topic monitoring feed.",
        "url": "https://api.gdeltproject.org/api/v2/doc/doc",
        "requires_api_key": False,
    },
    "EIA_PETROLEUM_STATUS": {
        "source": "U.S. EIA",
        "series": "EIA_PETROLEUM_STATUS",
        "category": "energy",
        "description": "U.S. petroleum inventories from the Weekly Petroleum Status Report.",
        "url": "https://www.eia.gov/opendata/",
        "requires_api_key": False,
    },
    "EIA_NATGAS_STORAGE": {
        "source": "U.S. EIA",
        "series": "EIA_NATGAS_STORAGE",
        "category": "energy",
        "description": "U.S. natural gas storage balances from the Weekly Natural Gas Storage Report.",
        "url": "https://www.eia.gov/opendata/",
        "requires_api_key": False,
    },
}

TOPIC_SERIES_MAP: dict[str, list[str]] = {
    "rates": ["TREASURY_YIELD_10Y", "TREASURY_YIELD_2Y", "FEDERAL_FUNDS_RATE", "INFLATION_EXPECTATION"],
    "inflation": ["CPI", "INFLATION_EXPECTATION", "WTI", "BRENT"],
    "credit": ["TREASURY_YIELD_10Y", "FEDERAL_FUNDS_RATE"],
    "oil": ["WTI", "BRENT", "EIA_PETROLEUM_STATUS"],
    "energy": ["WTI", "BRENT", "NATURAL_GAS", "EIA_PETROLEUM_STATUS", "EIA_NATGAS_STORAGE"],
    "natural_gas": ["NATURAL_GAS", "EIA_NATGAS_STORAGE"],
    "growth": ["RETAIL_SALES", "CONSUMER_SENTIMENT", "REAL_GDP"],
    "labor": ["UNEMPLOYMENT"],
    "fundamental": ["SEC_FILINGS", "EARNINGS_TRANSCRIPTS"],
    "news": ["ALPHA_VANTAGE_NEWS_SENTIMENT", "GDELT_DOC_2"],
}


class DynamicEDAService:
    def __init__(
        self,
        alpha_vantage: AlphaVantageService,
        *,
        eia_service: EIAService | None = None,
        news_intel_service: NewsIntelService | None = None,
        feature_store: FeatureStore | None = None,
        sec_edgar_service: SecEdgarService | None = None,
        ticker_metadata_service: TickerMetadataService | None = None,
        stock_dataset_builder: StockDatasetBuilder | None = None,
    ) -> None:
        self.alpha_vantage = alpha_vantage
        self.eia_service = eia_service
        self.news_intel_service = news_intel_service
        self.feature_store = feature_store
        self.sec_edgar_service = sec_edgar_service
        self.ticker_metadata_service = ticker_metadata_service
        self.stock_dataset_builder = stock_dataset_builder or StockDatasetBuilder(
            alpha_vantage=alpha_vantage,
            ticker_metadata_service=ticker_metadata_service,
            feature_store=feature_store,
            sec_edgar_service=sec_edgar_service,
        )

    async def execute(
        self,
        *,
        plan: AnalysisPlan,
        question: str,
        baseline_bundle: AnalyticsBundle,
    ) -> DynamicEDAResult:
        routed_sources = self._resolve_data_sources(plan=plan, question=question)
        news_intel = await self._collect_news_intel(
            plan=plan,
            question=question,
            baseline_bundle=baseline_bundle,
        )
        if plan.question_type == QuestionType.general_health:
            return self._general_health(plan, baseline_bundle, routed_sources, news_intel)
        if plan.question_type == QuestionType.concentration_diversification:
            return self._concentration(plan, baseline_bundle, routed_sources, news_intel)
        if plan.question_type == QuestionType.performance_drivers:
            return self._performance(plan, baseline_bundle, routed_sources, news_intel)
        if plan.question_type == QuestionType.rates_macro:
            return await self._rates(plan, baseline_bundle, routed_sources, news_intel)
        if plan.question_type == QuestionType.geopolitical_war:
            return await self._war(plan, baseline_bundle, question, routed_sources, news_intel)
        if plan.question_type == QuestionType.factor_cross_section:
            return await self._factor_cross_section(plan, baseline_bundle, question, routed_sources, news_intel)
        return await self._what_if(plan, baseline_bundle, question, routed_sources, news_intel)

    async def _collect_news_intel(
        self,
        *,
        plan: AnalysisPlan,
        question: str,
        baseline_bundle: AnalyticsBundle,
    ):
        if self.news_intel_service is None:
            return None
        top_tickers = [
            item.ticker
            for item in baseline_bundle.baseline.positions[: min(4, len(baseline_bundle.baseline.positions))]
        ]
        topics = list(dict.fromkeys([*plan.macro_themes, plan.question_type.value]))
        return await self.news_intel_service.collect(
            question=question,
            tickers=[ticker for ticker in [*plan.relevant_tickers, *top_tickers] if ticker],
            topics=topics,
        )

    def _resolve_data_sources(self, *, plan: AnalysisPlan, question: str) -> list[DataSourceReference]:
        preferred = [series.strip().upper() for series in plan.preferred_data_sources if series.strip()]
        theme_expansions: list[str] = []
        for theme in plan.macro_themes:
            normalized_theme = theme.strip().lower()
            if not normalized_theme:
                continue
            theme_expansions.extend(TOPIC_SERIES_MAP.get(normalized_theme, []))
        unique_series = list(dict.fromkeys([*preferred, *theme_expansions]))
        references: list[DataSourceReference] = []
        for series in unique_series:
            metadata = DATASET_CATALOG.get(series)
            if metadata is None:
                continue
            references.append(
                DataSourceReference(
                    source=metadata["source"],
                    series=metadata["series"],
                    category=metadata["category"],
                    description=metadata["description"],
                    access="free",
                    requires_api_key=bool(metadata.get("requires_api_key", False)),
                    status=self._series_status(series, metadata),
                    url=metadata.get("url"),
                    rationale=self._series_rationale(series, question),
                )
            )
        return references

    def _series_status(self, series: str, metadata: dict[str, Any]) -> str:
        if metadata.get("source") == "Alpha Vantage" and metadata.get("requires_api_key"):
            return "available" if getattr(self.alpha_vantage, "api_key", None) else "not_configured"
        if series.startswith("EIA_"):
            return "available" if self.eia_service is not None else "not_configured"
        return str(metadata.get("status", "available"))

    @staticmethod
    def _series_rationale(series: str, question: str) -> str | None:
        lowered = question.lower()
        if series.startswith("TREASURY_YIELD") or series == "FEDERAL_FUNDS_RATE":
            return "Used when the question is sensitive to discount rates, financing costs, or duration."
        if series in {"CPI", "INFLATION_EXPECTATION"}:
            return "Used when inflation pass-through or real-rate pressure may matter."
        if series in {"WTI", "BRENT", "NATURAL_GAS"}:
            return "Used when energy shocks or commodity-driven geopolitical stress may matter."
        if series in {"RETAIL_SALES", "CONSUMER_SENTIMENT", "REAL_GDP", "UNEMPLOYMENT"}:
            return "Used when the question references recession, growth, or consumer demand."
        if series in {"SEC_FILINGS", "EARNINGS_TRANSCRIPTS"} and any(
            token in lowered for token in ("company", "earnings", "filing", "add", "addition")
        ):
            return "Used to ground company-specific what-if or risk questions in primary source disclosures."
        return None

    async def analyze_rates_regimes(
        self,
        baseline_bundle: AnalyticsBundle,
    ) -> dict[str, Any] | None:
        try:
            treasury_10y, treasury_2y = await asyncio.gather(
                self.alpha_vantage.get_treasury_yield(),
                self.alpha_vantage.get_treasury_yield(maturity="2year"),
            )
        except Exception:  # noqa: BLE001
            return None

        series_candidates = [
            ("10Y yield change", treasury_10y["value"].diff() if not treasury_10y.empty else pd.Series(dtype=float)),
            ("2Y yield change", treasury_2y["value"].diff() if not treasury_2y.empty else pd.Series(dtype=float)),
        ]
        if not treasury_10y.empty and not treasury_2y.empty:
            curve_change = (treasury_10y["value"] - treasury_2y["value"]).diff()
            series_candidates.append(("10Y-2Y curve change", curve_change))

        analyzed_series = [
            self._analyze_single_rate_series(
                series_name=series_name,
                macro_series=series,
                baseline_bundle=baseline_bundle,
            )
            for series_name, series in series_candidates
        ]
        analyzed_series = [item for item in analyzed_series if item is not None]
        if not analyzed_series:
            return None

        analyzed_series.sort(key=self._rate_series_materiality, reverse=True)
        primary = max(
            analyzed_series,
            key=lambda item: max(
                abs(float(item.get("yield_up", {}).get("avg_same_day_excess", 0.0))) if item.get("yield_up") else 0.0,
                abs(float(item.get("yield_down", {}).get("avg_same_day_excess", 0.0))) if item.get("yield_down") else 0.0,
            ),
        )
        return {
            **primary,
            "series_screen": [
                {
                    "series": item["series_name"],
                    "sample_days": item["sample_days"],
                    "yield_up_days": item["yield_up"]["days"] if item.get("yield_up") else 0,
                    "yield_up_avg_same_day_excess": (
                        round(float(item["yield_up"]["avg_same_day_excess"]), 6) if item.get("yield_up") else None
                    ),
                    "yield_up_forward_5d_excess": (
                        round(float(item["yield_up"]["avg_forward_5d_excess"]), 6) if item.get("yield_up") and item["yield_up"]["avg_forward_5d_excess"] is not None else None
                    ),
                    "yield_down_days": item["yield_down"]["days"] if item.get("yield_down") else 0,
                    "yield_down_avg_same_day_excess": (
                        round(float(item["yield_down"]["avg_same_day_excess"]), 6) if item.get("yield_down") else None
                    ),
                    "yield_down_forward_5d_excess": (
                        round(float(item["yield_down"]["avg_forward_5d_excess"]), 6) if item.get("yield_down") and item["yield_down"]["avg_forward_5d_excess"] is not None else None
                    ),
                    "portfolio_corr": round(float(item["yield_change_corr"]["portfolio"]), 4),
                    "benchmark_corr": round(float(item["yield_change_corr"]["benchmark"]), 4),
                }
                for item in analyzed_series
            ],
        }

    @staticmethod
    def _rate_series_materiality(item: dict[str, Any]) -> float:
        up_stats = item.get("yield_up") or {}
        down_stats = item.get("yield_down") or {}
        candidates = [
            up_stats.get("avg_same_day_excess"),
            down_stats.get("avg_same_day_excess"),
            up_stats.get("avg_forward_5d_excess"),
            down_stats.get("avg_forward_5d_excess"),
            up_stats.get("avg_forward_10d_excess"),
            down_stats.get("avg_forward_10d_excess"),
        ]
        return max(abs(float(value)) for value in candidates if value is not None)

    def _analyze_single_rate_series(
        self,
        *,
        series_name: str,
        macro_series: pd.Series,
        baseline_bundle: AnalyticsBundle,
    ) -> dict[str, Any] | None:
        aligned = pd.concat(
            [
                baseline_bundle.portfolio_returns.rename("portfolio"),
                baseline_bundle.benchmark_returns.rename("benchmark"),
                macro_series.rename("yield_change"),
            ],
            axis=1,
            sort=False,
        ).dropna()
        if aligned.empty or len(aligned) < 40:
            return None
        if np.isclose(float(aligned["yield_change"].std(ddof=0)), 0.0):
            return None

        aligned["excess"] = aligned["portfolio"] - aligned["benchmark"]
        positive_threshold = float(aligned["yield_change"].quantile(0.9))
        negative_threshold = float(aligned["yield_change"].quantile(0.1))
        yield_up = aligned[aligned["yield_change"] >= positive_threshold].copy()
        yield_down = aligned[aligned["yield_change"] <= negative_threshold].copy()
        if yield_up.empty and yield_down.empty:
            return None

        portfolio_index = baseline_bundle.portfolio_value_series / baseline_bundle.portfolio_value_series.iloc[0]
        benchmark_index = baseline_bundle.benchmark_prices / baseline_bundle.benchmark_prices.iloc[0]
        holding_returns = baseline_bundle.holding_returns.copy()
        weight_map = {item.ticker: item.weight for item in baseline_bundle.baseline.positions}
        company_map = {item.ticker: item.company_name for item in baseline_bundle.baseline.positions}

        def forward_return(series: pd.Series, event_date: pd.Timestamp, horizon: int) -> float | None:
            if event_date not in series.index:
                return None
            start_idx = series.index.get_loc(event_date)
            if isinstance(start_idx, slice):
                return None
            end_idx = start_idx + horizon
            if end_idx >= len(series.index):
                return None
            start_value = float(series.iloc[start_idx])
            end_value = float(series.iloc[end_idx])
            if start_value == 0:
                return None
            return (end_value / start_value) - 1

        def regime_stats(frame: pd.DataFrame, label: str, threshold: float) -> dict[str, Any]:
            event_dates = list(frame.index)
            forward_1d = [value for value in (forward_return(portfolio_index, d, 1) for d in event_dates) if value is not None]
            forward_5d = [value for value in (forward_return(portfolio_index, d, 5) for d in event_dates) if value is not None]
            forward_10d = [value for value in (forward_return(portfolio_index, d, 10) for d in event_dates) if value is not None]
            benchmark_forward_1d = [value for value in (forward_return(benchmark_index, d, 1) for d in event_dates) if value is not None]
            benchmark_forward_5d = [value for value in (forward_return(benchmark_index, d, 5) for d in event_dates) if value is not None]
            benchmark_forward_10d = [value for value in (forward_return(benchmark_index, d, 10) for d in event_dates) if value is not None]
            return {
                "regime": label,
                "days": int(len(frame)),
                "threshold_bps": threshold * 100,
                "avg_same_day_return": float(frame["portfolio"].mean()),
                "avg_same_day_benchmark": float(frame["benchmark"].mean()),
                "avg_same_day_excess": float(frame["excess"].mean()),
                "same_day_hit_rate": float((frame["excess"] > 0).mean()),
                "avg_forward_1d_return": float(np.mean(forward_1d)) if forward_1d else None,
                "avg_forward_1d_excess": (
                    float(np.mean([a - b for a, b in zip(forward_1d, benchmark_forward_1d, strict=False)]))
                    if forward_1d and benchmark_forward_1d
                    else None
                ),
                "avg_forward_5d_return": float(np.mean(forward_5d)) if forward_5d else None,
                "avg_forward_5d_excess": (
                    float(np.mean([a - b for a, b in zip(forward_5d, benchmark_forward_5d, strict=False)]))
                    if forward_5d and benchmark_forward_5d
                    else None
                ),
                "avg_forward_10d_return": float(np.mean(forward_10d)) if forward_10d else None,
                "avg_forward_10d_excess": (
                    float(np.mean([a - b for a, b in zip(forward_10d, benchmark_forward_10d, strict=False)]))
                    if forward_10d and benchmark_forward_10d
                    else None
                ),
            }

        def attribution_rows(frame: pd.DataFrame) -> list[dict[str, Any]]:
            subset = holding_returns.reindex(frame.index).dropna(how="all")
            if subset.empty:
                return []
            average_returns = subset.mean()
            rows = []
            for ticker, average_return in average_returns.items():
                weight = float(weight_map.get(ticker, 0.0))
                contribution = float(weight * float(average_return))
                rows.append(
                    {
                        "ticker": ticker,
                        "company_name": company_map.get(ticker, ticker),
                        "avg_return": round(float(average_return), 6),
                        "weight": round(weight, 6),
                        "weighted_contribution": round(contribution, 6),
                    }
                )
            rows.sort(key=lambda item: abs(item["weighted_contribution"]), reverse=True)
            return rows[:5]

        up_stats = regime_stats(yield_up, "yield_up_shock", positive_threshold) if not yield_up.empty else None
        down_stats = regime_stats(yield_down, "yield_down_shock", negative_threshold) if not yield_down.empty else None
        combined = pd.concat([yield_up.assign(regime="yield_up"), yield_down.assign(regime="yield_down")], sort=False)
        recent_rows = []
        if not combined.empty:
            combined = combined.sort_index(ascending=False).head(10)
            recent_rows = [
                {
                    "date": index.strftime("%Y-%m-%d"),
                    "regime": row["regime"],
                    "yield_change_bps": round(float(row["yield_change"] * 100), 2),
                    "portfolio_return": round(float(row["portfolio"]), 6),
                    "benchmark_return": round(float(row["benchmark"]), 6),
                    "excess_return": round(float(row["excess"]), 6),
                }
                for index, row in combined.iterrows()
            ]
        return {
            "series_name": series_name,
            "sample_days": int(len(aligned)),
            "sample_start": aligned.index.min().strftime("%Y-%m-%d"),
            "sample_end": aligned.index.max().strftime("%Y-%m-%d"),
            "yield_up": up_stats,
            "yield_down": down_stats,
            "yield_up_attribution": attribution_rows(yield_up),
            "yield_down_attribution": attribution_rows(yield_down),
            "recent_events": recent_rows,
            "yield_change_corr": {
                "portfolio": self._safe_corr(aligned["portfolio"], aligned["yield_change"]),
                "benchmark": self._safe_corr(aligned["benchmark"], aligned["yield_change"]),
            },
        }

    @staticmethod
    def _safe_corr(left: pd.Series, right: pd.Series) -> float:
        value = left.corr(right)
        if value is None or not np.isfinite(value):
            return 0.0
        return float(value)

    def _general_health(
        self,
        plan: AnalysisPlan,
        baseline_bundle: AnalyticsBundle,
        data_sources: list[DataSourceReference],
        news_intel: NewsIntelResult | None,
    ) -> DynamicEDAResult:
        top_sector = baseline_bundle.baseline.sector_exposures[0]
        findings = [
            EDAFinding(
                headline="Concentration and volatility are the first-order health signals.",
                evidence=[
                    f"Top 3 holdings account for {baseline_bundle.metrics_map['top3_share'] * 100:.2f}% of capital.",
                    f"Herfindahl concentration index is {baseline_bundle.metrics_map['herfindahl_index']:.2f}.",
                    f"Annualized volatility is {baseline_bundle.metrics_map['annualized_volatility'] * 100:.2f}%.",
                ],
                metrics={
                    "top3_share": baseline_bundle.metrics_map["top3_share"],
                    "herfindahl_index": baseline_bundle.metrics_map["herfindahl_index"],
                    "annualized_volatility": baseline_bundle.metrics_map["annualized_volatility"],
                },
            ),
            EDAFinding(
                headline="Sector skew is visible in the baseline exposure table.",
                evidence=[
                    f"The largest sector is {top_sector.sector} at {top_sector.weight * 100:.2f}% weight.",
                    f"Average pairwise correlation across holdings is {baseline_bundle.metrics_map['average_pairwise_correlation']:.2f}.",
                    f"Beta vs benchmark is {baseline_bundle.metrics_map['beta_vs_benchmark']:.2f}.",
                ],
                metrics={
                    "largest_sector_weight": top_sector.weight,
                    "average_pairwise_correlation": baseline_bundle.metrics_map["average_pairwise_correlation"],
                    "beta_vs_benchmark": baseline_bundle.metrics_map["beta_vs_benchmark"],
                },
            ),
        ]
        return DynamicEDAResult(
            workflow=plan.dynamic_workflow,
            question_type=plan.question_type,
            findings=findings,
            data_sources=data_sources,
            news_intel=news_intel,
            tables=[
                AnalysisTable(
                    name="Top Contributors",
                    columns=["ticker", "contribution_pct", "weight", "return_pct"],
                    rows=[item.model_dump() for item in baseline_bundle.baseline.contributors[:5]],
                )
            ],
        )

    def _concentration(
        self,
        plan: AnalysisPlan,
        baseline_bundle: AnalyticsBundle,
        data_sources: list[DataSourceReference],
        news_intel: NewsIntelResult | None,
    ) -> DynamicEDAResult:
        correlation_pairs = []
        matrix = baseline_bundle.baseline.correlation_matrix
        tickers = list(matrix.keys())
        for index, ticker in enumerate(tickers):
            for other in tickers[index + 1 :]:
                correlation_pairs.append((ticker, other, matrix[ticker][other]))
        correlation_pairs.sort(key=lambda item: item[2], reverse=True)
        top_pair = correlation_pairs[0] if correlation_pairs else ("", "", 0.0)
        sector_exposures = baseline_bundle.baseline.sector_exposures
        top_sector = sector_exposures[0]
        second_sector = sector_exposures[1] if len(sector_exposures) > 1 else None
        top_two_sector_share = float(sum(item.weight for item in sector_exposures[:2]))
        findings = [
            EDAFinding(
                headline="The portfolio's diversification constraint is concentrated in a few names.",
                evidence=[
                    f"Top 3 holdings account for {baseline_bundle.metrics_map['top3_share'] * 100:.2f}% of value.",
                    f"Average pairwise correlation is {baseline_bundle.metrics_map['average_pairwise_correlation']:.2f}.",
                    f"Most correlated pair is {top_pair[0]} / {top_pair[1]} at {top_pair[2]:.2f}.",
                ],
                metrics={
                    "top3_share": baseline_bundle.metrics_map["top3_share"],
                    "average_pairwise_correlation": baseline_bundle.metrics_map["average_pairwise_correlation"],
                    "top_pair_correlation": top_pair[2],
                },
            ),
            EDAFinding(
                headline="Sector concentration adds a second layer of clustering.",
                evidence=[
                    f"Largest sector weight is {top_sector.weight * 100:.2f}% in {top_sector.sector}.",
                    (
                        f"Top two sectors account for {top_two_sector_share * 100:.2f}% of capital, with {second_sector.sector} as the second-largest bucket at {second_sector.weight * 100:.2f}%."
                        if second_sector is not None
                        else "Only one sector bucket is populated in the current portfolio view."
                    ),
                    f"Herfindahl index is {baseline_bundle.metrics_map['herfindahl_index']:.2f}.",
                    f"Sharpe is {baseline_bundle.metrics_map['sharpe_ratio']:.2f}, which frames the risk-adjusted tradeoff.",
                ],
                metrics={
                    "largest_sector_weight": top_sector.weight,
                    "top_two_sector_share": top_two_sector_share,
                    "herfindahl_index": baseline_bundle.metrics_map["herfindahl_index"],
                    "sharpe_ratio": baseline_bundle.metrics_map["sharpe_ratio"],
                },
            ),
        ]
        return DynamicEDAResult(
            workflow=plan.dynamic_workflow,
            question_type=plan.question_type,
            findings=findings,
            data_sources=data_sources,
            news_intel=news_intel,
            tables=[
                AnalysisTable(
                    name="Correlation Hotspots",
                    columns=["ticker_a", "ticker_b", "correlation"],
                    rows=[
                        {
                            "ticker_a": a,
                            "ticker_b": b,
                            "correlation": round(correlation, 4),
                        }
                        for a, b, correlation in correlation_pairs[:5]
                    ],
                ),
                AnalysisTable(
                    name="Sector Concentration",
                    columns=["sector", "weight", "market_value"],
                    rows=[
                        {
                            "sector": item.sector,
                            "weight": round(item.weight, 6),
                            "market_value": round(item.market_value, 2),
                        }
                        for item in sector_exposures[:8]
                    ],
                ),
            ],
        )

    def _performance(
        self,
        plan: AnalysisPlan,
        baseline_bundle: AnalyticsBundle,
        data_sources: list[DataSourceReference],
        news_intel: NewsIntelResult | None,
    ) -> DynamicEDAResult:
        contributors = baseline_bundle.baseline.contributors
        top = contributors[:3]
        bottom = sorted(contributors, key=lambda item: item.contribution_pct)[:3]
        findings = [
            EDAFinding(
                headline="Performance is being driven by a small subset of names.",
                evidence=[
                    f"Top contributor is {top[0].ticker} at {top[0].contribution_pct * 100:.2f}% contribution.",
                    f"Trailing return vs benchmark is {baseline_bundle.metrics_map['return_vs_benchmark'] * 100:.2f} percentage points.",
                    f"Worst detractor is {bottom[0].ticker} at {bottom[0].contribution_pct * 100:.2f}% contribution.",
                ],
                metrics={
                    "top_contribution": top[0].contribution_pct,
                    "return_vs_benchmark": baseline_bundle.metrics_map["return_vs_benchmark"],
                    "worst_contribution": bottom[0].contribution_pct,
                },
            ),
            EDAFinding(
                headline="Relative performance combines stock selection with portfolio risk posture.",
                evidence=[
                    f"Portfolio beta is {baseline_bundle.metrics_map['beta_vs_benchmark']:.2f}.",
                    f"Sharpe ratio is {baseline_bundle.metrics_map['sharpe_ratio']:.2f}.",
                    f"Max drawdown reached {baseline_bundle.metrics_map['max_drawdown'] * 100:.2f}%.",
                ],
                metrics={
                    "beta_vs_benchmark": baseline_bundle.metrics_map["beta_vs_benchmark"],
                    "sharpe_ratio": baseline_bundle.metrics_map["sharpe_ratio"],
                    "max_drawdown": baseline_bundle.metrics_map["max_drawdown"],
                },
            ),
        ]
        tables = [
            AnalysisTable(
                name="Contribution Decomposition",
                columns=["ticker", "contribution_pct", "return_pct", "weight"],
                rows=[item.model_dump() for item in contributors[:8]],
            )
        ]
        tables.extend(self._fundamental_feature_tables([item.ticker for item in contributors[:2]]))
        if news_intel and news_intel.articles:
            findings.append(
                EDAFinding(
                    headline="External news flow is now available as a routed evidence layer for performance questions.",
                    evidence=[
                        f"Normalized external records retained: {len(news_intel.articles)}.",
                        f"Dominant topics: {', '.join(news_intel.dominant_topics[:4]) or 'none identified'}.",
                        f"Largest retrieval source by count: {news_intel.source_stats[0].source}."
                        if news_intel.source_stats
                        else "No source-level counts were available.",
                    ],
                    metrics={"news_article_count": float(len(news_intel.articles))},
                )
            )
            tables.extend(self._news_tables(news_intel))
        return DynamicEDAResult(
            workflow=plan.dynamic_workflow,
            question_type=plan.question_type,
            findings=findings,
            data_sources=data_sources,
            news_intel=news_intel,
            tables=tables,
        )

    async def _rates(
        self,
        plan: AnalysisPlan,
        baseline_bundle: AnalyticsBundle,
        data_sources: list[DataSourceReference],
        news_intel: NewsIntelResult | None,
    ) -> DynamicEDAResult:
        regime_analysis = await self.analyze_rates_regimes(baseline_bundle)
        if regime_analysis is None:
            findings = [
                EDAFinding(
                    headline="Rates workflow was selected, but the macro series did not return a usable aligned sample.",
                    evidence=[
                        "The app kept the baseline portfolio analytics intact.",
                        "A macro overlay caveat should be applied before drawing rates conclusions.",
                    ],
                    metrics={},
                    severity="warning",
                )
            ]
            return DynamicEDAResult(
                workflow=plan.dynamic_workflow,
                question_type=plan.question_type,
                findings=findings,
                data_sources=data_sources,
                news_intel=news_intel,
                tables=[],
            )

        series_screen = regime_analysis.get("series_screen", [])
        up_stats = regime_analysis["yield_up"]
        down_stats = regime_analysis["yield_down"]
        up_attr = regime_analysis["yield_up_attribution"]
        down_attr = regime_analysis["yield_down_attribution"]
        context_rows = await self._macro_snapshot_rows(question="rates inflation fed yield")
        findings = []
        if series_screen:
            primary_row = series_screen[0]
            secondary_row = series_screen[1] if len(series_screen) > 1 else None
            primary_signal = max(
                abs(float(primary_row.get("yield_up_avg_same_day_excess") or 0.0)),
                abs(float(primary_row.get("yield_down_avg_same_day_excess") or 0.0)),
                abs(float(primary_row.get("yield_up_forward_5d_excess") or 0.0)),
                abs(float(primary_row.get("yield_down_forward_5d_excess") or 0.0)),
            )
            comparison_text = (
                f"The next-strongest tested series was {secondary_row['series']} with weaker excess-return moves than {primary_row['series']}."
                if secondary_row is not None
                else "Only one rates lens produced a usable aligned shock sample."
            )
            findings.append(
                EDAFinding(
                    headline="The app now screens multiple rates lenses before deciding which regime behavior matters most.",
                    evidence=[
                        f"Tested series included {', '.join(row['series'] for row in series_screen)} over the aligned sample window.",
                        f"The strongest relative signal came from {primary_row['series']}, where the largest observed excess-return response was {primary_signal * 100:.2f} percentage points.",
                        comparison_text,
                    ],
                    metrics={
                        "primary_rates_signal_strength": primary_signal,
                        "tested_rates_series_count": float(len(series_screen)),
                    },
                )
            )
        if up_stats is not None:
            findings.append(
                EDAFinding(
                    headline="The rates workflow treats rate moves as shock regimes, not broad long-run correlation.",
                    evidence=[
                        f"Primary shock lens was {regime_analysis['series_name']} across {regime_analysis['sample_days']} daily observations from {regime_analysis['sample_start']} to {regime_analysis['sample_end']}.",
                        f"Yield-up shocks are defined as {regime_analysis['series_name']} daily changes of at least {up_stats['threshold_bps']:.2f} bps.",
                        f"On those {up_stats['days']} shock days, the portfolio averaged {up_stats['avg_same_day_return'] * 100:.2f}% versus {up_stats['avg_same_day_benchmark'] * 100:.2f}% for SPY, or {up_stats['avg_same_day_excess'] * 100:.2f}% excess return.",
                    ],
                    metrics={
                        "yield_up_days": float(up_stats["days"]),
                        "yield_up_threshold_bps": float(up_stats["threshold_bps"]),
                        "yield_up_avg_same_day_excess": float(up_stats["avg_same_day_excess"]),
                    },
                )
            )
            findings.append(
                EDAFinding(
                    headline="Forward performance after rate-up shocks is more informative than unconditional correlation.",
                    evidence=[
                        f"Average 1-day forward return after yield-up shocks was {(up_stats['avg_forward_1d_return'] or 0.0) * 100:.2f}%.",
                        f"Average 5-day forward excess return versus SPY after yield-up shocks was {(up_stats['avg_forward_5d_excess'] or 0.0) * 100:.2f}%.",
                        f"The portfolio outperformed SPY on {up_stats['same_day_hit_rate'] * 100:.2f}% of yield-up shock days.",
                    ],
                    metrics={
                        "yield_up_forward_1d_return": float(up_stats["avg_forward_1d_return"] or 0.0),
                        "yield_up_forward_5d_excess": float(up_stats["avg_forward_5d_excess"] or 0.0),
                        "yield_up_hit_rate": float(up_stats["same_day_hit_rate"]),
                    },
                )
            )
        if down_stats is not None:
            findings.append(
                EDAFinding(
                    headline="Rate-down shocks form a separate regime and should not be blended with rate-up days.",
                    evidence=[
                        f"Yield-down shocks are defined as {regime_analysis['series_name']} daily changes of at most {down_stats['threshold_bps']:.2f} bps.",
                        f"On those {down_stats['days']} days, the portfolio averaged {down_stats['avg_same_day_return'] * 100:.2f}% versus {down_stats['avg_same_day_benchmark'] * 100:.2f}% for SPY.",
                        f"Average 10-day forward excess return after yield-down shocks was {(down_stats['avg_forward_10d_excess'] or 0.0) * 100:.2f}%.",
                    ],
                    metrics={
                        "yield_down_days": float(down_stats["days"]),
                        "yield_down_threshold_bps": float(down_stats["threshold_bps"]),
                        "yield_down_forward_10d_excess": float(down_stats["avg_forward_10d_excess"] or 0.0),
                    },
                )
            )

        if context_rows:
            context_summary = context_rows[:3]
            evidence = [
                f"{row['series']} most recently sat at {row['latest']:.2f} with a {row['change_1p']:+.2f} one-period move."
                for row in context_summary
                if row.get("latest") is not None and row.get("change_1p") is not None
            ]
            if evidence:
                findings.append(
                    EDAFinding(
                        headline="Current macro context helps distinguish policy shocks from longer-duration yield moves.",
                        evidence=evidence,
                        metrics={},
                    )
                )

        if up_attr:
            findings.append(
                EDAFinding(
                    headline="Shock-day attribution shows which holdings actually drive the portfolio's rates behavior.",
                    evidence=[
                        f"Largest weighted contributor on yield-up shock days was {up_attr[0]['ticker']} at {up_attr[0]['weighted_contribution'] * 100:.2f}% average weighted contribution.",
                        f"Second largest shock-day driver was {up_attr[1]['ticker']} at {up_attr[1]['weighted_contribution'] * 100:.2f}%."
                        if len(up_attr) > 1
                        else "Only one clear driver emerged in the available shock-day sample.",
                        "This is more decision-useful than trusting a single unconditional portfolio-to-yield correlation.",
                    ],
                    metrics={
                        "top_yield_up_driver_contribution": float(up_attr[0]["weighted_contribution"]),
                    },
                )
            )
        return DynamicEDAResult(
            workflow=plan.dynamic_workflow,
            question_type=plan.question_type,
            findings=findings,
            data_sources=data_sources,
            news_intel=news_intel,
            tables=self._rates_tables(regime_analysis, up_attr, down_attr, context_rows)
            + self._macro_feature_tables()
            + self._news_tables(news_intel),
        )

    async def _war(
        self,
        plan: AnalysisPlan,
        baseline_bundle: AnalyticsBundle,
        question: str,
        data_sources: list[DataSourceReference],
        news_intel: NewsIntelResult | None,
    ) -> DynamicEDAResult:
        war_analysis = await self._analyze_geopolitical_stress(question=question, baseline_bundle=baseline_bundle)
        energy_rows = await self._energy_inventory_rows()
        if war_analysis is None:
            findings = [
                EDAFinding(
                    headline="Geopolitical stress workflow ran, but the oil-shock proxy series was unavailable or too sparse.",
                    evidence=[
                        "The app preserved the baseline portfolio evidence.",
                        "War-scenario interpretation should be treated as incomplete until macro proxy data is available.",
                    ],
                    metrics={},
                    severity="warning",
                )
            ]
            if energy_rows:
                findings.append(
                    EDAFinding(
                        headline="EIA inventory context is still available even when the historical shock screen is sparse.",
                        evidence=[
                            f"Energy inventory snapshots captured {len(energy_rows)} current EIA series.",
                            "These series can still help frame whether the current backdrop looks inventory-tight or inventory-loose.",
                        ],
                        metrics={"eia_snapshot_count": float(len(energy_rows))},
                    )
                )
            return DynamicEDAResult(
                workflow=plan.dynamic_workflow,
                question_type=plan.question_type,
                findings=findings,
                data_sources=data_sources,
                news_intel=news_intel,
                tables=[
                    AnalysisTable(
                        name="EIA Energy Inventory Context",
                        columns=["series", "report_date", "level", "weekly_change", "reference_gap"],
                        rows=energy_rows,
                    )
                ]
                if energy_rows
                else [],
            )
        regime_rows = war_analysis["regime_rows"]
        top_drivers = war_analysis["top_drivers"]
        findings = [
            EDAFinding(
                headline="Geopolitical stress is now evaluated through multiple commodity shock proxies rather than only one oil series.",
                evidence=[
                    f"The strongest war-like proxy was {war_analysis['primary_series']} with {war_analysis['primary_shock_days']} stress days in sample.",
                    f"Across the screened commodity proxies, the worst average same-day excess return was {war_analysis['worst_same_day_excess'] * 100:.2f}%.",
                    f"The best average 5-day excess response across those proxies was {(war_analysis['best_forward_5d_excess'] or 0.0) * 100:.2f}%.",
                ],
                metrics={
                    "primary_shock_days": float(war_analysis["primary_shock_days"]),
                    "worst_same_day_excess": float(war_analysis["worst_same_day_excess"]),
                    "best_forward_5d_excess": float(war_analysis["best_forward_5d_excess"] or 0.0),
                },
            ),
            EDAFinding(
                headline="Stress attribution stays tied to actual portfolio holdings during those macro shock dates.",
                evidence=[
                    f"Top shock-day driver was {top_drivers[0]['ticker']} with {top_drivers[0]['weighted_contribution'] * 100:.2f}% weighted contribution."
                    if top_drivers
                    else "No stable holding-level attribution was available for the stress subset.",
                    f"Commodity proxies screened: {', '.join(war_analysis['screened_series'])}.",
                    "This makes the geopolitical workflow more robust to whether the current stress transmits through oil, gas, or cross-commodity spikes.",
                ],
                metrics={
                    "screened_proxy_count": float(len(war_analysis["screened_series"])),
                },
            ),
        ]
        if energy_rows:
            findings.append(
                EDAFinding(
                    headline="EIA inventory balances add a physical-market context layer to the geopolitical workflow.",
                    evidence=[
                        f"Latest petroleum snapshot level is {energy_rows[0]['level']:.3f} with weekly change {energy_rows[0]['weekly_change']:.3f}."
                        if energy_rows
                        else "No petroleum inventory snapshot was available.",
                        (
                            f"Latest natural gas storage snapshot level is {energy_rows[1]['level']:.3f} with weekly change {energy_rows[1]['weekly_change']:.3f}."
                            if len(energy_rows) > 1
                            else "No natural gas storage snapshot was available."
                        ),
                        "These series are useful when energy stress is transmitting through inventories rather than only front-month prices.",
                    ],
                    metrics={
                        "eia_snapshot_count": float(len(energy_rows)),
                    },
                )
            )
        tables = [
            AnalysisTable(
                name="Geopolitical Proxy Regimes",
                columns=[
                    "series",
                    "shock_days",
                    "avg_same_day_return",
                    "avg_same_day_excess",
                    "avg_forward_5d_excess",
                ],
                rows=regime_rows,
            ),
            AnalysisTable(
                name="Geopolitical Shock Attribution",
                columns=["ticker", "company_name", "avg_return", "weight", "weighted_contribution"],
                rows=top_drivers,
            ),
        ]
        if energy_rows:
            tables.append(
                AnalysisTable(
                    name="EIA Energy Inventory Context",
                    columns=["series", "report_date", "level", "weekly_change", "reference_gap"],
                    rows=energy_rows,
                )
            )
        return DynamicEDAResult(
            workflow=plan.dynamic_workflow,
            question_type=plan.question_type,
            findings=findings,
            data_sources=data_sources,
            news_intel=news_intel,
            tables=tables + self._news_tables(news_intel),
        )

    async def _factor_cross_section(
        self,
        plan: AnalysisPlan,
        baseline_bundle: AnalyticsBundle,
        question: str,
        data_sources: list[DataSourceReference],
        news_intel: NewsIntelResult | None,
    ) -> DynamicEDAResult:
        factor_frame = await self.build_factor_cross_section_dataset(
            plan=plan,
            baseline_bundle=baseline_bundle,
        )
        tickers = list(factor_frame["ticker"]) if not factor_frame.empty and "ticker" in factor_frame else []
        if not tickers:
            return DynamicEDAResult(
                workflow=plan.dynamic_workflow,
                question_type=plan.question_type,
                findings=[
                    EDAFinding(
                        headline="Factor cross-section routing selected no usable stock universe.",
                        evidence=[
                            "The workflow needs at least a few tickers to build a cross-sectional dataframe.",
                            "No holdings or planner-selected tickers were available for comparison.",
                        ],
                        metrics={},
                        severity="warning",
                    )
                ],
                data_sources=data_sources,
                news_intel=news_intel,
                tables=[],
            )
        if factor_frame.empty or len(factor_frame) < 3:
            return DynamicEDAResult(
                workflow=plan.dynamic_workflow,
                question_type=plan.question_type,
                findings=[
                    EDAFinding(
                        headline="Cross-sectional factor EDA could not build a large enough dataset.",
                        evidence=[
                            f"Requested universe size was {len(tickers)} tickers, but only {len(factor_frame)} had usable aligned history.",
                            "The workflow needs enough observations to compare sectors and estimate factor relationships.",
                        ],
                        metrics={
                            "requested_tickers": float(len(tickers)),
                            "usable_tickers": float(len(factor_frame)),
                        },
                        severity="warning",
                    )
                ],
                data_sources=data_sources,
                news_intel=news_intel,
                tables=[],
            )

        sector_rows = self._sector_return_rows(factor_frame)
        correlation_rows = self._metric_correlation_rows(factor_frame)
        rank_ic_rows = self._rank_ic_rows(factor_frame)
        regression_rows = self._regression_rows(factor_frame)
        bucket_rows = self._bucketed_return_rows(factor_frame, correlation_rows)
        bucket_summary_rows = self._bucket_summary_rows(bucket_rows)
        comparison_rows = self._stock_comparison_rows(factor_frame, plan.relevant_tickers)

        metric_focus = correlation_rows[0] if correlation_rows else None
        rank_ic_focus = rank_ic_rows[0] if rank_ic_rows else None
        sector_focus = sector_rows[0] if sector_rows else None
        regression_focus = regression_rows[0] if regression_rows else None
        findings = [
            EDAFinding(
                headline="The factor workflow builds a stock-level dataframe first, then compares sectors and metrics against realized returns.",
                evidence=[
                    f"The routed universe included {len(factor_frame)} stocks with aligned price history and available metadata.",
                    f"Sectors represented: {', '.join(sorted(str(item) for item in factor_frame['sector'].dropna().unique()[:6])) or 'Unknown'}.",
                    f"Usable numeric predictors screened: {len(self._factor_metric_columns(factor_frame))}.",
                ],
                metrics={
                    "cross_section_size": float(len(factor_frame)),
                    "sector_count": float(factor_frame['sector'].fillna('Unknown').nunique()),
                    "predictor_count": float(len(self._factor_metric_columns(factor_frame))),
                },
            )
        ]
        if sector_focus is not None:
            findings.append(
                EDAFinding(
                    headline="Sector-level return dispersion shows whether the signal is broad or concentrated in one industry bucket.",
                    evidence=[
                        f"Best average trailing return came from {sector_focus['sector']} at {sector_focus['avg_trailing_return'] * 100:.2f}% across {sector_focus['stock_count']} stocks.",
                        f"Average forward 21-day return for that sector was {(sector_focus['avg_forward_21d_return'] or 0.0) * 100:.2f}%.",
                        "This sector table helps separate industry effects from stock-specific metric effects.",
                    ],
                    metrics={
                        "best_sector_avg_trailing_return": float(sector_focus["avg_trailing_return"]),
                        "best_sector_stock_count": float(sector_focus["stock_count"]),
                    },
                )
            )
        if metric_focus is not None:
            findings.append(
                EDAFinding(
                    headline="The workflow ranks financial metrics by how strongly they line up with trailing and forward returns.",
                    evidence=[
                        f"Strongest screened relationship was {metric_focus['metric']} versus {metric_focus['target']} with correlation {metric_focus['correlation']:.2f}.",
                        f"That estimate used {metric_focus['observations']} stocks after dropping missing values.",
                        "These are cross-sectional associations, not proof of causality.",
                    ],
                    metrics={
                        "top_metric_correlation": float(metric_focus["correlation"]),
                        "top_metric_observations": float(metric_focus["observations"]),
                    },
                )
            )
        if rank_ic_focus is not None:
            findings.append(
                EDAFinding(
                    headline="Rank-based and sector-neutral diagnostics help distinguish broad factor ordering from simple level effects.",
                    evidence=[
                        f"Top Spearman rank relationship was {rank_ic_focus['metric']} versus {rank_ic_focus['target']} at {rank_ic_focus['spearman_correlation']:.2f}.",
                        f"Sector-neutral Spearman for that pair was {rank_ic_focus['sector_neutral_spearman']:.2f}.",
                        "If the sector-neutral signal collapses, the apparent factor may mostly be a sector effect.",
                    ],
                    metrics={
                        "top_rank_ic": float(rank_ic_focus["spearman_correlation"]),
                        "top_sector_neutral_rank_ic": float(rank_ic_focus["sector_neutral_spearman"]),
                    },
                )
            )
        if regression_focus is not None:
            findings.append(
                EDAFinding(
                    headline="Simple single-factor regressions provide a fast sanity check on sign, magnitude, and explanatory power.",
                    evidence=[
                        f"Top regression was {regression_focus['metric']} against {regression_focus['target']} with slope {regression_focus['slope']:.4f}.",
                        f"Estimated R-squared was {regression_focus['r_squared']:.2f} across {regression_focus['observations']} stocks.",
                        "This is intentionally lightweight diagnostics, not a production factor model.",
                    ],
                    metrics={
                        "top_regression_slope": float(regression_focus["slope"]),
                        "top_regression_r_squared": float(regression_focus["r_squared"]),
                    },
                )
            )
        if bucket_summary_rows:
            monotonic_count = sum(1 for row in bucket_summary_rows if row["monotonic"])
            findings.append(
                EDAFinding(
                    headline="Quantile bucket spreads show whether the factor sort is monotonic or noisy.",
                    evidence=[
                        f"{monotonic_count} of the top {len(bucket_summary_rows)} screened metrics showed monotonic average trailing returns across quantiles.",
                        f"Largest Q4-Q1 spread came from {bucket_summary_rows[0]['metric']} at {bucket_summary_rows[0]['spread_q4_q1'] * 100:.2f} percentage points.",
                        "Monotonic buckets are stronger evidence than a single noisy correlation estimate.",
                    ],
                    metrics={
                        "monotonic_metric_count": float(monotonic_count),
                        "largest_quantile_spread": float(bucket_summary_rows[0]["spread_q4_q1"]),
                    },
                )
            )
        if news_intel and news_intel.articles:
            findings.append(
                EDAFinding(
                    headline="Narrative context can be compared against the factor tables instead of treated as a substitute for them.",
                    evidence=[
                        f"Normalized news records retained: {len(news_intel.articles)}.",
                        f"Dominant topics: {', '.join(news_intel.dominant_topics[:4]) or 'none identified'}.",
                        "Use this to pressure-test whether return dispersion is being driven by fundamentals, macro narrative, or both.",
                    ],
                    metrics={"news_article_count": float(len(news_intel.articles))},
                )
            )
        return DynamicEDAResult(
            workflow=plan.dynamic_workflow,
            question_type=plan.question_type,
            findings=findings,
            data_sources=data_sources,
            news_intel=news_intel,
            tables=[
                AnalysisTable(
                    name="Stock Factor Comparison",
                    columns=[
                        "ticker",
                        "sector",
                        "trailing_return",
                        "forward_21d_return",
                        "forward_21d_median",
                        "annualized_volatility",
                        "beta_vs_benchmark",
                        "net_margin",
                        "operating_margin",
                        "gross_margin",
                        "current_ratio",
                        "debt_to_revenue",
                        "focus",
                    ],
                    rows=comparison_rows,
                ),
                AnalysisTable(
                    name="Sector Return Comparison",
                    columns=[
                        "sector",
                        "stock_count",
                        "avg_trailing_return",
                        "median_trailing_return",
                        "avg_forward_21d_return",
                        "avg_annualized_volatility",
                    ],
                    rows=sector_rows,
                ),
                AnalysisTable(
                    name="Metric Correlations vs Returns",
                    columns=["metric", "target", "correlation", "sector_neutral_correlation", "observations"],
                    rows=correlation_rows,
                ),
                AnalysisTable(
                    name="Rank IC Diagnostics",
                    columns=["metric", "target", "spearman_correlation", "sector_neutral_spearman", "observations"],
                    rows=rank_ic_rows,
                ),
                AnalysisTable(
                    name="Metric Quantile Buckets",
                    columns=["metric", "bucket", "stock_count", "avg_trailing_return", "avg_forward_21d_return"],
                    rows=bucket_rows,
                ),
                AnalysisTable(
                    name="Quantile Bucket Diagnostics",
                    columns=["metric", "q1_avg", "q4_avg", "spread_q4_q1", "monotonic"],
                    rows=bucket_summary_rows,
                ),
                AnalysisTable(
                    name="Regression Diagnostics",
                    columns=["metric", "target", "slope", "intercept", "r_squared", "observations"],
                    rows=regression_rows,
                ),
            ]
            + self._fundamental_feature_tables(plan.relevant_tickers or tickers[:3])
            + self._news_tables(news_intel),
        )

    async def build_factor_cross_section_dataset(
        self,
        *,
        plan: AnalysisPlan,
        baseline_bundle: AnalyticsBundle,
    ) -> pd.DataFrame:
        tickers = list(
            dict.fromkeys(
                [
                    *[position.ticker for position in baseline_bundle.baseline.positions],
                    *plan.relevant_tickers,
                ]
            )
        )
        start_date = pd.Timestamp(baseline_bundle.baseline.effective_start_date).date()
        end_date = pd.Timestamp(baseline_bundle.baseline.effective_end_date).date()
        return await self.stock_dataset_builder.build_cross_section(
            tickers=tickers,
            benchmark_symbol=baseline_bundle.baseline.benchmark_symbol,
            lookback_days=baseline_bundle.baseline.effective_observations,
            start_date=start_date,
            end_date=end_date,
            comparison_universe=plan.comparison_universe,
            comparison_sector_filters=plan.comparison_sector_filters,
            comparison_ticker_limit=plan.comparison_ticker_limit,
            portfolio_tickers=[position.ticker for position in baseline_bundle.baseline.positions],
        )

    async def _what_if(
        self,
        plan: AnalysisPlan,
        baseline_bundle: AnalyticsBundle,
        question: str,
        data_sources: list[DataSourceReference],
        news_intel: NewsIntelResult | None,
    ) -> DynamicEDAResult:
        contextual_tables = await self._what_if_context_tables(question=question, baseline_bundle=baseline_bundle)
        findings = [
            EDAFinding(
                headline="What-if analysis is evaluated as a deterministic before/after comparison with dynamic context from routed macro and company datasets.",
                evidence=[
                    f"Baseline top 3 weight is {baseline_bundle.metrics_map['top3_share'] * 100:.2f}%.",
                    f"Baseline Sharpe ratio is {baseline_bundle.metrics_map['sharpe_ratio']:.2f}.",
                    f"Routed supporting datasets: {', '.join(source.series for source in data_sources[:5]) or 'baseline analytics only'}.",
                ],
                metrics={
                    "top3_share": baseline_bundle.metrics_map["top3_share"],
                    "sharpe_ratio": baseline_bundle.metrics_map["sharpe_ratio"],
                },
            ),
            EDAFinding(
                headline="The routing layer now chooses additional data sources based on the question before scenario math runs.",
                evidence=[
                    "Company-specific questions can lean on SEC filings and earnings transcripts.",
                    "Macro-sensitive addition questions can bring in rates, inflation, or energy series without hard-coding one path.",
                    f"Question received: {question}",
                ],
                metrics={},
            ),
        ]
        return DynamicEDAResult(
            workflow=plan.dynamic_workflow,
            question_type=plan.question_type,
            findings=findings,
            data_sources=data_sources,
            news_intel=news_intel,
            tables=contextual_tables
            + self._fundamental_feature_tables(plan.relevant_tickers or [item.ticker for item in baseline_bundle.baseline.positions[:2]])
            + self._news_tables(news_intel),
        )

    def enrich_with_scenario(
        self,
        *,
        dynamic_eda: DynamicEDAResult,
        scenario: ScenarioAnalytics,
        baseline_bundle: AnalyticsBundle,
        after_bundle: AnalyticsBundle,
        question: str,
    ) -> DynamicEDAResult:
        if dynamic_eda.question_type != QuestionType.what_if_addition:
            return dynamic_eda

        delta_map = {item.metric: item for item in scenario.deltas}
        before_positions = self._aggregate_positions(scenario.before_positions)
        after_positions = self._aggregate_positions(scenario.after_positions)
        before_sectors = self._aggregate_sectors(scenario.before_sector_exposures)
        after_sectors = self._aggregate_sectors(scenario.after_sector_exposures)
        holding_shift_rows = self._holding_shift_rows(before_positions, after_positions)
        sector_shift_rows = self._sector_shift_rows(before_sectors, after_sectors)
        metric_rows = self._scenario_metric_rows(delta_map)

        volatility_delta = self._delta_value(delta_map, "annualized_volatility")
        beta_delta = self._delta_value(delta_map, "beta_vs_benchmark")
        sharpe_delta = self._delta_value(delta_map, "sharpe_ratio")
        return_delta = self._delta_value(delta_map, "trailing_return")
        top3_delta = self._delta_value(delta_map, "top3_share")
        herfindahl_delta = self._delta_value(delta_map, "herfindahl_index")
        corr_delta = self._delta_value(delta_map, "average_pairwise_correlation")
        drawdown_delta = self._delta_value(delta_map, "max_drawdown")

        hypothetical_ticker = scenario.hypothetical_position.ticker
        existing_weight_before = before_positions.get(hypothetical_ticker, {}).get("weight", 0.0)
        biggest_holding_shift = holding_shift_rows[0] if holding_shift_rows else None
        biggest_sector_shift = sector_shift_rows[0] if sector_shift_rows else None

        scenario_findings = [
            EDAFinding(
                headline="The scenario output now shows the actual risk/return tradeoff of the proposed addition.",
                evidence=[
                    f"Trailing return changes by {self._format_pct_delta(return_delta)} and Sharpe changes by {self._format_decimal_delta(sharpe_delta)}.",
                    f"Annualized volatility changes by {self._format_pct_delta(volatility_delta)} while beta changes by {self._format_decimal_delta(beta_delta)}.",
                    f"Max drawdown changes by {self._format_pct_delta(drawdown_delta)} over the same historical window.",
                ],
                metrics={
                    "trailing_return_delta": return_delta,
                    "sharpe_ratio_delta": sharpe_delta,
                    "annualized_volatility_delta": volatility_delta,
                    "beta_vs_benchmark_delta": beta_delta,
                    "max_drawdown_delta": drawdown_delta,
                },
            ),
            EDAFinding(
                headline="The what-if workflow also measures whether the addition improves concentration and diversification.",
                evidence=[
                    f"Top 3 concentration changes by {self._format_pct_delta(top3_delta)} and Herfindahl changes by {self._format_decimal_delta(herfindahl_delta)}.",
                    f"Average pairwise correlation changes by {self._format_decimal_delta(corr_delta)}.",
                    (
                        f"The hypothetical {hypothetical_ticker} position increases an existing exposure that was already {existing_weight_before * 100:.2f}% of the portfolio."
                        if existing_weight_before > 0
                        else f"The hypothetical {hypothetical_ticker} position introduces a new ticker exposure to the portfolio."
                    ),
                ],
                metrics={
                    "top3_share_delta": top3_delta,
                    "herfindahl_delta": herfindahl_delta,
                    "average_pairwise_correlation_delta": corr_delta,
                    "existing_weight_before": float(existing_weight_before),
                },
            ),
        ]

        if biggest_holding_shift or biggest_sector_shift:
            holding_sentence = (
                f"The largest holding weight shift is {biggest_holding_shift['ticker']} at {biggest_holding_shift['weight_delta'] * 100:.2f} percentage points."
                if biggest_holding_shift
                else "No single holding weight shift dominated the scenario."
            )
            sector_sentence = (
                f"The largest sector shift is {biggest_sector_shift['sector']} at {biggest_sector_shift['weight_delta'] * 100:.2f} percentage points."
                if biggest_sector_shift
                else "No single sector shift dominated the scenario."
            )
            scenario_findings.append(
                EDAFinding(
                    headline="Exposure shifts explain where the scenario impact is actually coming from.",
                    evidence=[
                        holding_sentence,
                        sector_sentence,
                        f"The deterministic before/after tables below answer the question '{question}' using the scenario math instead of only a narrative summary.",
                    ],
                    metrics={
                        "largest_holding_weight_delta": (
                            float(biggest_holding_shift["weight_delta"]) if biggest_holding_shift else 0.0
                        ),
                        "largest_sector_weight_delta": (
                            float(biggest_sector_shift["weight_delta"]) if biggest_sector_shift else 0.0
                        ),
                    },
                )
            )

        scenario_findings.append(
            EDAFinding(
                headline="The scenario interpretation is grounded in the same aligned sample window as the baseline analytics.",
                evidence=[
                    f"Baseline effective window is {baseline_bundle.baseline.effective_start_date} to {baseline_bundle.baseline.effective_end_date} across {baseline_bundle.baseline.effective_observations} observations.",
                    f"Post-addition effective window is {after_bundle.baseline.effective_start_date} to {after_bundle.baseline.effective_end_date} across {after_bundle.baseline.effective_observations} observations.",
                    "This keeps the before/after comparison tied to a common historical sample rather than mixing different periods.",
                ],
                metrics={
                    "baseline_effective_observations": float(baseline_bundle.baseline.effective_observations),
                    "after_effective_observations": float(after_bundle.baseline.effective_observations),
                },
            )
        )

        tables = list(dynamic_eda.tables)
        if metric_rows:
            tables.append(
                AnalysisTable(
                    name="Scenario Metric Comparison",
                    columns=["metric", "before", "after", "delta"],
                    rows=metric_rows,
                )
            )
        if sector_shift_rows:
            tables.append(
                AnalysisTable(
                    name="Sector Exposure Shifts",
                    columns=[
                        "sector",
                        "before_weight",
                        "after_weight",
                        "weight_delta",
                        "before_market_value",
                        "after_market_value",
                    ],
                    rows=sector_shift_rows,
                )
            )
        if holding_shift_rows:
            tables.append(
                AnalysisTable(
                    name="Largest Holding Weight Changes",
                    columns=[
                        "ticker",
                        "company_name",
                        "before_weight",
                        "after_weight",
                        "weight_delta",
                        "before_market_value",
                        "after_market_value",
                    ],
                    rows=holding_shift_rows,
                )
            )

        return dynamic_eda.model_copy(
            update={
                "findings": [*dynamic_eda.findings, *scenario_findings],
                "tables": tables,
                "scenario_analysis": scenario,
            }
        )

    async def _analyze_geopolitical_stress(
        self,
        *,
        question: str,
        baseline_bundle: AnalyticsBundle,
    ) -> dict[str, Any] | None:
        series_specs = [
            ("WTI", self.alpha_vantage.get_wti),
            ("BRENT", self.alpha_vantage.get_brent),
            ("NATURAL_GAS", self.alpha_vantage.get_natural_gas),
        ]
        fetch_results = await asyncio.gather(
            *(loader() for _series, loader in series_specs),
            return_exceptions=True,
        )
        regime_rows: list[dict[str, Any]] = []
        attribution_pool: list[pd.Timestamp] = []
        screened_series: list[str] = []
        for (series_name, _loader), frame in zip(series_specs, fetch_results, strict=True):
            if isinstance(frame, Exception) or frame.empty:
                continue
            screened_series.append(series_name)
            aligned = pd.concat(
                [
                    baseline_bundle.portfolio_returns.rename("portfolio"),
                    baseline_bundle.benchmark_returns.rename("benchmark"),
                    frame["value"].pct_change().rename("proxy_change"),
                ],
                axis=1,
                sort=False,
            ).dropna()
            if len(aligned) < 40:
                continue
            threshold = float(aligned["proxy_change"].quantile(0.9))
            shock_days = aligned[
                (aligned["proxy_change"] >= threshold) & (aligned["benchmark"] <= aligned["benchmark"].quantile(0.25))
            ].copy()
            if shock_days.empty:
                continue
            shock_days["excess"] = shock_days["portfolio"] - shock_days["benchmark"]
            attribution_pool.extend(list(shock_days.index))
            forward_excess = self._forward_window_excess(
                baseline_bundle.portfolio_value_series,
                baseline_bundle.benchmark_prices,
                list(shock_days.index),
                5,
            )
            regime_rows.append(
                {
                    "series": series_name,
                    "shock_days": int(len(shock_days)),
                    "avg_same_day_return": round(float(shock_days["portfolio"].mean()), 6),
                    "avg_same_day_excess": round(float(shock_days["excess"].mean()), 6),
                    "avg_forward_5d_excess": round(float(np.mean(forward_excess)), 6) if forward_excess else None,
                }
            )
        if not regime_rows:
            return None
        regime_rows.sort(key=lambda item: item["avg_same_day_excess"])
        top_drivers = self._attribution_for_dates(
            baseline_bundle=baseline_bundle,
            dates=sorted(set(attribution_pool)),
        )
        best_forward_5d_excess = max(
            (row["avg_forward_5d_excess"] for row in regime_rows if row["avg_forward_5d_excess"] is not None),
            default=None,
        )
        return {
            "question": question,
            "regime_rows": regime_rows,
            "top_drivers": top_drivers,
            "screened_series": screened_series,
            "primary_series": regime_rows[0]["series"],
            "primary_shock_days": regime_rows[0]["shock_days"],
            "worst_same_day_excess": float(regime_rows[0]["avg_same_day_excess"]),
            "best_forward_5d_excess": best_forward_5d_excess,
        }

    async def _energy_inventory_rows(self) -> list[dict[str, Any]]:
        if self.eia_service is None:
            return []
        results = await asyncio.gather(
            self.eia_service.get_petroleum_storage_snapshot(),
            self.eia_service.get_natgas_storage_snapshot(),
            return_exceptions=True,
        )
        rows: list[dict[str, Any]] = []
        petroleum, natgas = results
        if not isinstance(petroleum, Exception):
            commercial = petroleum["commercial_crude"]
            rows.append(
                {
                    "series": "EIA_PETROLEUM_STATUS",
                    "report_date": petroleum.get("report_date"),
                    "level": round(float(commercial["level_million_bbl"]), 3),
                    "weekly_change": round(float(commercial["weekly_change_million_bbl"]), 3),
                    "reference_gap": round(
                        float((petroleum.get("total_ex_spr") or {}).get("weekly_change_million_bbl") or 0.0),
                        3,
                    ),
                }
            )
        if not isinstance(natgas, Exception):
            total = natgas["total_lower_48"]
            rows.append(
                {
                    "series": "EIA_NATGAS_STORAGE",
                    "report_date": natgas.get("report_date"),
                    "level": round(float(total["working_gas_bcf"]), 3),
                    "weekly_change": round(float(total["net_change_bcf"]), 3),
                    "reference_gap": round(float(total["vs_5y_pct"] or 0.0), 3),
                }
            )
        return rows

    async def _what_if_context_tables(
        self,
        *,
        question: str,
        baseline_bundle: AnalyticsBundle,
    ) -> list[AnalysisTable]:
        question_lower = question.lower()
        tables = [
            AnalysisTable(
                name="Current Portfolio Decision Frame",
                columns=["metric", "value"],
                rows=[
                    {"metric": "top3_share", "value": round(float(baseline_bundle.metrics_map["top3_share"]), 6)},
                    {"metric": "sharpe_ratio", "value": round(float(baseline_bundle.metrics_map["sharpe_ratio"]), 6)},
                    {
                        "metric": "beta_vs_benchmark",
                        "value": round(float(baseline_bundle.metrics_map["beta_vs_benchmark"]), 6),
                    },
                    {
                        "metric": "average_pairwise_correlation",
                        "value": round(float(baseline_bundle.metrics_map["average_pairwise_correlation"]), 6),
                    },
                ],
            )
        ]
        if any(token in question_lower for token in ("rate", "yield", "fed", "macro", "inflation", "war", "oil", "gas")):
            macro_rows = await self._macro_snapshot_rows(question=question)
            if macro_rows:
                tables.append(
                    AnalysisTable(
                        name="Question-Routed Macro Context",
                        columns=["series", "latest", "change_1p", "change_12p"],
                        rows=macro_rows,
                    )
                )
        if any(token in question_lower for token in ("war", "oil", "energy", "gas", "commodity")):
            energy_rows = await self._energy_inventory_rows()
            if energy_rows:
                tables.append(
                    AnalysisTable(
                        name="EIA Energy Inventory Context",
                        columns=["series", "report_date", "level", "weekly_change", "reference_gap"],
                        rows=energy_rows,
                    )
                )
        return tables

    async def _macro_snapshot_rows(self, *, question: str) -> list[dict[str, Any]]:
        lower = question.lower()
        requests: list[tuple[str, Any]] = []
        if any(token in lower for token in ("rate", "yield", "fed", "duration")):
            requests.extend(
                [
                    ("TREASURY_YIELD_10Y", self.alpha_vantage.get_treasury_yield()),
                    ("FEDERAL_FUNDS_RATE", self.alpha_vantage.get_federal_funds_rate()),
                ]
            )
        if any(token in lower for token in ("inflation", "pricing", "cpi")):
            requests.extend(
                [
                    ("CPI", self.alpha_vantage.get_cpi()),
                    ("INFLATION_EXPECTATION", self.alpha_vantage.get_inflation_expectation()),
                ]
            )
        if any(token in lower for token in ("war", "oil", "energy", "gas", "commodity")):
            requests.extend(
                [
                    ("WTI", self.alpha_vantage.get_wti()),
                    ("BRENT", self.alpha_vantage.get_brent()),
                    ("NATURAL_GAS", self.alpha_vantage.get_natural_gas()),
                ]
            )
        if not requests:
            return []
        fetched = await asyncio.gather(*(task for _name, task in requests), return_exceptions=True)
        rows: list[dict[str, Any]] = []
        for (series_name, _task), frame in zip(requests, fetched, strict=True):
            if isinstance(frame, Exception) or frame.empty:
                continue
            latest = float(frame["value"].iloc[-1])
            change_1p = float(frame["value"].iloc[-1] - frame["value"].iloc[-2]) if len(frame) > 1 else None
            change_12p = float(frame["value"].iloc[-1] - frame["value"].iloc[-13]) if len(frame) > 12 else None
            rows.append(
                {
                    "series": series_name,
                    "latest": round(latest, 4),
                    "change_1p": round(change_1p, 4) if change_1p is not None else None,
                    "change_12p": round(change_12p, 4) if change_12p is not None else None,
                }
            )
        return rows

    @staticmethod
    def _forward_window_excess(
        portfolio_series: pd.Series,
        benchmark_series: pd.Series,
        dates: list[pd.Timestamp],
        horizon: int,
    ) -> list[float]:
        excess_values: list[float] = []
        for date in dates:
            if date not in portfolio_series.index or date not in benchmark_series.index:
                continue
            start_idx = portfolio_series.index.get_loc(date)
            benchmark_idx = benchmark_series.index.get_loc(date)
            if isinstance(start_idx, slice) or isinstance(benchmark_idx, slice):
                continue
            if start_idx + horizon >= len(portfolio_series) or benchmark_idx + horizon >= len(benchmark_series):
                continue
            portfolio_return = (portfolio_series.iloc[start_idx + horizon] / portfolio_series.iloc[start_idx]) - 1
            benchmark_return = (benchmark_series.iloc[benchmark_idx + horizon] / benchmark_series.iloc[benchmark_idx]) - 1
            excess_values.append(float(portfolio_return - benchmark_return))
        return excess_values

    @staticmethod
    def _attribution_for_dates(
        *,
        baseline_bundle: AnalyticsBundle,
        dates: list[pd.Timestamp],
    ) -> list[dict[str, Any]]:
        subset = baseline_bundle.holding_returns.reindex(dates).dropna(how="all")
        if subset.empty:
            return []
        weight_map = {item.ticker: item.weight for item in baseline_bundle.baseline.positions}
        company_map = {item.ticker: item.company_name for item in baseline_bundle.baseline.positions}
        rows: list[dict[str, Any]] = []
        average_returns = subset.mean()
        for ticker, average_return in average_returns.items():
            weight = float(weight_map.get(ticker, 0.0))
            rows.append(
                {
                    "ticker": ticker,
                    "company_name": company_map.get(ticker, ticker),
                    "avg_return": round(float(average_return), 6),
                    "weight": round(weight, 6),
                    "weighted_contribution": round(float(weight * float(average_return)), 6),
                }
            )
        rows.sort(key=lambda item: abs(item["weighted_contribution"]), reverse=True)
        return rows[:5]

    @staticmethod
    def _rates_tables(
        regime_analysis: dict[str, Any],
        up_attr: list[dict[str, Any]],
        down_attr: list[dict[str, Any]],
        context_rows: list[dict[str, Any]],
    ) -> list[AnalysisTable]:
        tables = []
        regime_rows = []
        for stats in (regime_analysis.get("yield_up"), regime_analysis.get("yield_down")):
            if stats is None:
                continue
            regime_rows.append(
                {
                    "regime": stats["regime"],
                    "days": stats["days"],
                    "threshold_bps": round(float(stats["threshold_bps"]), 2),
                    "avg_same_day_return": round(float(stats["avg_same_day_return"]), 6),
                    "avg_same_day_excess": round(float(stats["avg_same_day_excess"]), 6),
                    "same_day_hit_rate": round(float(stats["same_day_hit_rate"]), 4),
                    "avg_forward_1d_excess": (
                        round(float(stats["avg_forward_1d_excess"]), 6)
                        if stats["avg_forward_1d_excess"] is not None
                        else None
                    ),
                    "avg_forward_5d_excess": (
                        round(float(stats["avg_forward_5d_excess"]), 6)
                        if stats["avg_forward_5d_excess"] is not None
                        else None
                    ),
                    "avg_forward_10d_excess": (
                        round(float(stats["avg_forward_10d_excess"]), 6)
                        if stats["avg_forward_10d_excess"] is not None
                        else None
                    ),
                }
            )
        if regime_rows:
            tables.append(
                AnalysisTable(
                    name="Rates Shock Regimes",
                    columns=[
                        "regime",
                        "days",
                        "threshold_bps",
                        "avg_same_day_return",
                        "avg_same_day_excess",
                        "same_day_hit_rate",
                        "avg_forward_1d_excess",
                        "avg_forward_5d_excess",
                        "avg_forward_10d_excess",
                    ],
                    rows=regime_rows,
                )
            )
        series_screen = regime_analysis.get("series_screen", [])
        if series_screen:
            tables.append(
                AnalysisTable(
                    name="Rates Sensitivity Screen",
                    columns=[
                        "series",
                        "sample_days",
                        "yield_up_days",
                        "yield_up_avg_same_day_excess",
                        "yield_up_forward_5d_excess",
                        "yield_down_days",
                        "yield_down_avg_same_day_excess",
                        "yield_down_forward_5d_excess",
                        "portfolio_corr",
                        "benchmark_corr",
                    ],
                    rows=series_screen,
                )
            )
        if up_attr:
            tables.append(
                AnalysisTable(
                    name="Yield-Up Shock Attribution",
                    columns=["ticker", "company_name", "avg_return", "weight", "weighted_contribution"],
                    rows=up_attr,
                )
            )
        if down_attr:
            tables.append(
                AnalysisTable(
                    name="Yield-Down Shock Attribution",
                    columns=["ticker", "company_name", "avg_return", "weight", "weighted_contribution"],
                    rows=down_attr,
                )
            )
        recent_events = regime_analysis.get("recent_events", [])
        if recent_events:
            tables.append(
                AnalysisTable(
                    name="Recent Rate Shock Days",
                    columns=[
                        "date",
                        "regime",
                        "yield_change_bps",
                        "portfolio_return",
                        "benchmark_return",
                        "excess_return",
                    ],
                    rows=recent_events,
                )
            )
        if context_rows:
            tables.append(
                AnalysisTable(
                    name="Rates Macro Context",
                    columns=["series", "latest", "change_1p", "change_12p"],
                    rows=context_rows,
                )
            )
        return tables

    @staticmethod
    def _news_tables(news_intel: NewsIntelResult | None) -> list[AnalysisTable]:
        if news_intel is None or not news_intel.articles:
            return []
        tables = [
            AnalysisTable(
                name="Normalized News Feed",
                columns=["source", "source_type", "published_at", "title", "domain", "tickers", "topics", "url"],
                rows=[
                    {
                        "source": item.source,
                        "source_type": item.source_type,
                        "published_at": item.published_at,
                        "title": item.title,
                        "domain": item.domain,
                        "tickers": ", ".join(item.tickers),
                        "topics": ", ".join(item.topics),
                        "url": item.url,
                    }
                    for item in news_intel.articles[:10]
                ],
            ),
            AnalysisTable(
                name="News Source Mix",
                columns=["source", "article_count", "avg_sentiment", "latest_published_at"],
                rows=[item.model_dump() for item in news_intel.source_stats],
            ),
        ]
        return tables

    @staticmethod
    def _factor_metric_columns(frame: pd.DataFrame) -> list[str]:
        excluded = {"trailing_return", "forward_21d_return", "return_21d", "return_63d", "effective_observations"}
        numeric_columns = [
            column
            for column in frame.columns
            if pd.api.types.is_numeric_dtype(frame[column]) and column not in excluded
        ]
        return [
            column
            for column in numeric_columns
            if frame[column].dropna().shape[0] >= 4 and not np.isclose(float(frame[column].dropna().std(ddof=0)), 0.0)
        ]

    @staticmethod
    def _sector_return_rows(frame: pd.DataFrame) -> list[dict[str, Any]]:
        grouped = (
            frame.fillna({"sector": "Unknown"})
            .groupby("sector", dropna=False)
            .agg(
                stock_count=("ticker", "count"),
                avg_trailing_return=("trailing_return", "mean"),
                median_trailing_return=("trailing_return", "median"),
                avg_forward_21d_return=("forward_21d_return", "mean"),
                avg_annualized_volatility=("annualized_volatility", "mean"),
            )
            .reset_index()
        )
        grouped = grouped.sort_values("avg_trailing_return", ascending=False)
        return [
            {
                "sector": str(row["sector"]),
                "stock_count": int(row["stock_count"]),
                "avg_trailing_return": round(float(row["avg_trailing_return"]), 6),
                "median_trailing_return": round(float(row["median_trailing_return"]), 6),
                "avg_forward_21d_return": (
                    round(float(row["avg_forward_21d_return"]), 6)
                    if pd.notna(row["avg_forward_21d_return"])
                    else None
                ),
                "avg_annualized_volatility": round(float(row["avg_annualized_volatility"]), 6),
            }
            for _, row in grouped.iterrows()
        ]

    def _metric_correlation_rows(self, frame: pd.DataFrame) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for metric in self._factor_metric_columns(frame):
            for target in ("trailing_return", "forward_21d_return"):
                subset = frame[[metric, target]].dropna()
                if len(subset) < 4:
                    continue
                correlation = subset[metric].corr(subset[target])
                sector_neutral_corr = self._sector_neutral_corr(frame, metric, target, method="pearson")
                if correlation is None or not np.isfinite(correlation):
                    continue
                rows.append(
                    {
                        "metric": metric,
                        "target": target,
                        "correlation": round(float(correlation), 6),
                        "sector_neutral_correlation": round(float(sector_neutral_corr), 6),
                        "observations": int(len(subset)),
                    }
                )
        rows.sort(key=lambda item: abs(float(item["correlation"])), reverse=True)
        return rows[:20]

    def _rank_ic_rows(self, frame: pd.DataFrame) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for metric in self._factor_metric_columns(frame):
            for target in ("trailing_return", "forward_21d_return"):
                subset = frame[[metric, target]].dropna()
                if len(subset) < 4:
                    continue
                spearman = subset[metric].corr(subset[target], method="spearman")
                if spearman is None or not np.isfinite(spearman):
                    continue
                rows.append(
                    {
                        "metric": metric,
                        "target": target,
                        "spearman_correlation": round(float(spearman), 6),
                        "sector_neutral_spearman": round(
                            float(self._sector_neutral_corr(frame, metric, target, method="spearman")),
                            6,
                        ),
                        "observations": int(len(subset)),
                    }
                )
        rows.sort(key=lambda item: abs(float(item["spearman_correlation"])), reverse=True)
        return rows[:20]

    def _bucketed_return_rows(
        self,
        frame: pd.DataFrame,
        correlation_rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        ranked_metrics = list(dict.fromkeys(row["metric"] for row in correlation_rows[:3]))
        rows: list[dict[str, Any]] = []
        for metric in ranked_metrics:
            subset = frame[[metric, "trailing_return", "forward_21d_return"]].dropna(subset=[metric, "trailing_return"])
            if len(subset) < 4 or subset[metric].nunique() < 4:
                continue
            try:
                buckets = pd.qcut(subset[metric], 4, labels=["Q1", "Q2", "Q3", "Q4"], duplicates="drop")
            except ValueError:
                continue
            bucketed = subset.assign(bucket=buckets).groupby("bucket", observed=False).agg(
                stock_count=(metric, "count"),
                avg_trailing_return=("trailing_return", "mean"),
                avg_forward_21d_return=("forward_21d_return", "mean"),
            )
            for bucket, values in bucketed.iterrows():
                rows.append(
                    {
                        "metric": metric,
                        "bucket": str(bucket),
                        "stock_count": int(values["stock_count"]),
                        "avg_trailing_return": round(float(values["avg_trailing_return"]), 6),
                        "avg_forward_21d_return": (
                            round(float(values["avg_forward_21d_return"]), 6)
                            if pd.notna(values["avg_forward_21d_return"])
                            else None
                        ),
                    }
                )
        return rows[:24]

    @staticmethod
    def _bucket_summary_rows(bucket_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        by_metric: dict[str, list[dict[str, Any]]] = {}
        for row in bucket_rows:
            by_metric.setdefault(str(row["metric"]), []).append(row)
        summary_rows: list[dict[str, Any]] = []
        ordered_buckets = ["Q1", "Q2", "Q3", "Q4"]
        for metric, rows in by_metric.items():
            bucket_map = {str(row["bucket"]): row for row in rows}
            if not all(bucket in bucket_map for bucket in ordered_buckets):
                continue
            trailing_values = [float(bucket_map[bucket]["avg_trailing_return"]) for bucket in ordered_buckets]
            monotonic = trailing_values == sorted(trailing_values) or trailing_values == sorted(trailing_values, reverse=True)
            q1_avg = trailing_values[0]
            q4_avg = trailing_values[-1]
            summary_rows.append(
                {
                    "metric": metric,
                    "q1_avg": round(q1_avg, 6),
                    "q4_avg": round(q4_avg, 6),
                    "spread_q4_q1": round(q4_avg - q1_avg, 6),
                    "monotonic": monotonic,
                }
            )
        summary_rows.sort(key=lambda item: abs(float(item["spread_q4_q1"])), reverse=True)
        return summary_rows[:12]

    def _regression_rows(self, frame: pd.DataFrame) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for metric in self._factor_metric_columns(frame):
            for target in ("trailing_return", "forward_21d_return"):
                subset = frame[[metric, target]].dropna()
                if len(subset) < 5:
                    continue
                x = subset[metric].to_numpy(dtype=float)
                y = subset[target].to_numpy(dtype=float)
                design = np.column_stack([np.ones(len(x)), x])
                try:
                    coefficients, *_ = np.linalg.lstsq(design, y, rcond=None)
                except np.linalg.LinAlgError:
                    continue
                fitted = design @ coefficients
                residual = float(np.sum((y - fitted) ** 2))
                total = float(np.sum((y - y.mean()) ** 2))
                r_squared = 1.0 - (residual / total) if total else 0.0
                rows.append(
                    {
                        "metric": metric,
                        "target": target,
                        "intercept": round(float(coefficients[0]), 6),
                        "slope": round(float(coefficients[1]), 6),
                        "r_squared": round(float(r_squared), 6),
                        "observations": int(len(subset)),
                    }
                )
        rows.sort(key=lambda item: abs(float(item["r_squared"])), reverse=True)
        return rows[:20]

    @staticmethod
    def _stock_comparison_rows(frame: pd.DataFrame, focus_tickers: list[str]) -> list[dict[str, Any]]:
        focus_set = {ticker.upper() for ticker in focus_tickers}
        ordered = frame.sort_values(["trailing_return", "forward_21d_return"], ascending=False)
        rows: list[dict[str, Any]] = []
        for _, row in ordered.iterrows():
            rows.append(
                {
                    "ticker": row["ticker"],
                    "sector": row.get("sector"),
                    "trailing_return": round(float(row["trailing_return"]), 6),
                    "forward_21d_return": (
                        round(float(row["forward_21d_return"]), 6) if pd.notna(row.get("forward_21d_return")) else None
                    ),
                    "forward_21d_median": (
                        round(float(row["forward_21d_median"]), 6) if pd.notna(row.get("forward_21d_median")) else None
                    ),
                    "annualized_volatility": round(float(row["annualized_volatility"]), 6),
                    "beta_vs_benchmark": round(float(row["beta_vs_benchmark"]), 6),
                    "net_margin": round(float(row["net_margin"]), 6) if pd.notna(row.get("net_margin")) else None,
                    "operating_margin": (
                        round(float(row["operating_margin"]), 6) if pd.notna(row.get("operating_margin")) else None
                    ),
                    "gross_margin": round(float(row["gross_margin"]), 6) if pd.notna(row.get("gross_margin")) else None,
                    "current_ratio": round(float(row["current_ratio"]), 6) if pd.notna(row.get("current_ratio")) else None,
                    "debt_to_revenue": (
                        round(float(row["debt_to_revenue"]), 6) if pd.notna(row.get("debt_to_revenue")) else None
                    ),
                    "focus": row["ticker"] in focus_set,
                }
            )
        return rows[:20]

    @staticmethod
    def _sector_neutral_corr(frame: pd.DataFrame, metric: str, target: str, *, method: str) -> float:
        subset = frame[["sector", metric, target]].dropna()
        if len(subset) < 4:
            return 0.0
        adjusted = subset.copy()
        adjusted[metric] = adjusted.groupby("sector")[metric].transform(lambda values: values - values.mean())
        adjusted[target] = adjusted.groupby("sector")[target].transform(lambda values: values - values.mean())
        correlation = adjusted[metric].corr(adjusted[target], method=method)
        if correlation is None or not np.isfinite(correlation):
            return 0.0
        return float(correlation)

    def _fundamental_feature_tables(self, tickers: list[str]) -> list[AnalysisTable]:
        if self.feature_store is None:
            return []
        unique_tickers = [ticker for ticker in dict.fromkeys(tickers) if ticker]
        latest_rows: list[dict[str, Any]] = []
        trend_rows: list[dict[str, Any]] = []
        for ticker in unique_tickers[:3]:
            latest_rows.extend(self.feature_store.latest_company_fundamentals(ticker, metrics=["Revenues", "NetIncomeLoss"]))
            trend_rows.extend(self.feature_store.trailing_fundamental_trend(ticker, "Revenues", limit=4))
        tables: list[AnalysisTable] = []
        if latest_rows:
            tables.append(
                AnalysisTable(
                    name="Local Fundamental Snapshot",
                    columns=["ticker", "metric", "period_end", "fiscal_period", "fiscal_year", "value", "unit", "form_type", "filed_at"],
                    rows=latest_rows,
                )
            )
        if trend_rows:
            tables.append(
                AnalysisTable(
                    name="Revenue Trend From Local SEC Store",
                    columns=["ticker", "metric", "period_end", "fiscal_period", "fiscal_year", "value", "unit", "form_type", "filed_at"],
                    rows=trend_rows,
                )
            )
        return tables

    def _macro_feature_tables(self) -> list[AnalysisTable]:
        if self.feature_store is None:
            return []
        rows = self.feature_store.macro_snapshot(["DGS10", "DGS2", "FEDFUNDS", "CPIAUCSL", "UNRATE", "VIXCLS"])
        if not rows:
            return []
        return [
            AnalysisTable(
                name="Local Macro Feature Snapshot",
                columns=["series_id", "date", "value", "source", "category", "title", "unit"],
                rows=[
                    {
                        "series_id": item["series_id"],
                        "date": item["date"],
                        "value": item["value"],
                        "source": item["source"],
                        "category": item["category"],
                        "title": item["title"],
                        "unit": item["unit"],
                    }
                    for item in rows
                ],
            )
        ]

    @staticmethod
    def _delta_value(delta_map: dict[str, Any], key: str) -> float:
        item = delta_map.get(key)
        if item is None or item.delta is None:
            return 0.0
        return float(item.delta)

    @staticmethod
    def _format_pct_delta(value: float) -> str:
        return f"{value * 100:+.2f} percentage points"

    @staticmethod
    def _format_decimal_delta(value: float) -> str:
        return f"{value:+.4f}"

    @staticmethod
    def _aggregate_positions(positions: list[PositionSnapshot]) -> dict[str, dict[str, Any]]:
        aggregated: dict[str, dict[str, Any]] = {}
        for item in positions:
            current = aggregated.setdefault(
                item.ticker,
                {
                    "ticker": item.ticker,
                    "company_name": item.company_name,
                    "weight": 0.0,
                    "market_value": 0.0,
                },
            )
            current["weight"] += float(item.weight)
            current["market_value"] += float(item.market_value)
        return aggregated

    @staticmethod
    def _aggregate_sectors(sectors: list[SectorExposure]) -> dict[str, dict[str, Any]]:
        aggregated: dict[str, dict[str, Any]] = {}
        for item in sectors:
            current = aggregated.setdefault(
                item.sector,
                {
                    "sector": item.sector,
                    "weight": 0.0,
                    "market_value": 0.0,
                },
            )
            current["weight"] += float(item.weight)
            current["market_value"] += float(item.market_value)
        return aggregated

    @staticmethod
    def _holding_shift_rows(
        before_positions: dict[str, dict[str, Any]],
        after_positions: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        rows = []
        for ticker in sorted(set(before_positions) | set(after_positions)):
            before = before_positions.get(ticker, {})
            after = after_positions.get(ticker, {})
            rows.append(
                {
                    "ticker": ticker,
                    "company_name": after.get("company_name") or before.get("company_name") or ticker,
                    "before_weight": round(float(before.get("weight", 0.0)), 6),
                    "after_weight": round(float(after.get("weight", 0.0)), 6),
                    "weight_delta": round(float(after.get("weight", 0.0) - before.get("weight", 0.0)), 6),
                    "before_market_value": round(float(before.get("market_value", 0.0)), 2),
                    "after_market_value": round(float(after.get("market_value", 0.0)), 2),
                }
            )
        rows.sort(key=lambda item: abs(item["weight_delta"]), reverse=True)
        return rows[:8]

    @staticmethod
    def _sector_shift_rows(
        before_sectors: dict[str, dict[str, Any]],
        after_sectors: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        rows = []
        for sector in sorted(set(before_sectors) | set(after_sectors)):
            before = before_sectors.get(sector, {})
            after = after_sectors.get(sector, {})
            rows.append(
                {
                    "sector": sector,
                    "before_weight": round(float(before.get("weight", 0.0)), 6),
                    "after_weight": round(float(after.get("weight", 0.0)), 6),
                    "weight_delta": round(float(after.get("weight", 0.0) - before.get("weight", 0.0)), 6),
                    "before_market_value": round(float(before.get("market_value", 0.0)), 2),
                    "after_market_value": round(float(after.get("market_value", 0.0)), 2),
                }
            )
        rows.sort(key=lambda item: abs(item["weight_delta"]), reverse=True)
        return rows[:8]

    @staticmethod
    def _scenario_metric_rows(delta_map: dict[str, Any]) -> list[dict[str, Any]]:
        labels = {
            "trailing_return": "Trailing Return",
            "return_vs_benchmark": "Return vs SPY",
            "annualized_volatility": "Annualized Volatility",
            "beta_vs_benchmark": "Beta vs SPY",
            "sharpe_ratio": "Sharpe Ratio",
            "max_drawdown": "Max Drawdown",
            "average_pairwise_correlation": "Avg Pairwise Corr",
            "herfindahl_index": "Herfindahl Index",
            "top3_share": "Top 3 Weight",
        }
        rows = []
        for key, label in labels.items():
            delta = delta_map.get(key)
            if delta is None:
                continue
            rows.append(
                {
                    "metric": label,
                    "before": round(float(delta.before), 6) if delta.before is not None else None,
                    "after": round(float(delta.after), 6) if delta.after is not None else None,
                    "delta": round(float(delta.delta), 6) if delta.delta is not None else None,
                }
            )
        return rows
