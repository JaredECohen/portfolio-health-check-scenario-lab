from __future__ import annotations

import asyncio
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from app.database import Database
from app.models.schemas import (
    AnalysisPlan,
    AnalysisTable,
    ArtifactRecord,
    CandidateSearchResult,
    CriticResult,
    DynamicEDAResult,
    EarningsOverlayResult,
    FinalMemo,
    Holding,
    NewsArticle,
    NewsIntelResult,
    NewsSourceStats,
    PortfolioInput,
    QuestionType,
)
from app.services.analytics import AnalyticsService
from app.services.artifacts import ArtifactService
from app.services.orchestration import PortfolioAnalysisOrchestrator


class PassThroughIntake:
    async def normalize(self, payload: PortfolioInput) -> PortfolioInput:
        return payload


class FakeMarketData:
    async def fetch_price_history(self, *, tickers, benchmark_symbol, lookback_days, start_date=None, end_date=None):  # noqa: ANN001, ARG002
        index = pd.date_range("2024-01-02", periods=55, freq="B")
        price_history = {
            ticker: pd.DataFrame({"adjusted_close": [100 + idx for idx in range(55)]}, index=index)
            for ticker in tickers
        }
        benchmark = pd.DataFrame({"adjusted_close": [400 + idx for idx in range(55)]}, index=index)
        return price_history, benchmark

    async def get_risk_free_rate(self, fallback_rate: float) -> float:
        return fallback_rate


class FakeDynamicEDAService:
    async def execute(self, *, plan, question, baseline_bundle):  # noqa: ANN001, ARG002
        return DynamicEDAResult(
            workflow=plan.dynamic_workflow,
            question_type=plan.question_type,
            findings=[],
            tables=[],
        )

    async def build_factor_cross_section_dataset(self, *, plan, baseline_bundle):  # noqa: ANN001, ARG002
        return pd.DataFrame(
            [
                {
                    "ticker": "AAPL",
                    "sector": "Technology",
                    "trailing_return": 0.2,
                    "forward_21d_return": 0.03,
                }
            ]
        )


class FakeScenarioService:
    async def simulate_addition(self, **kwargs):  # noqa: ANN003
        raise AssertionError("Scenario should not run in this test")


class FakeArtifactService:
    def generate_baseline_charts(self, **kwargs):  # noqa: ANN003
        return []

    def generate_scenario_chart(self, **kwargs):  # noqa: ANN003
        raise AssertionError("Scenario chart should not run")

    def save_json_artifact(self, **kwargs):  # noqa: ANN003
        return ArtifactRecord(
            artifact_id="json-artifact",
            kind="analysis_response",
            title="Analysis response JSON",
            path="/tmp/analysis_response.json",
            url="/artifacts/session/analysis_response.json",
        )

    def save_markdown_memo(self, **kwargs):  # noqa: ANN003
        return ArtifactRecord(
            artifact_id="memo-artifact",
            kind="final_memo",
            title="Final memo",
            path="/tmp/final_memo.md",
            url="/artifacts/session/final_memo.md",
        )

    def save_dataframe_artifact(self, **kwargs):  # noqa: ANN003
        return [
            ArtifactRecord(
                artifact_id="factor-dataset-artifact",
                kind="factor_cross_section_dataset",
                title="Factor cross-section dataset",
                path="/tmp/factor_cross_section_dataset.csv",
                url="/artifacts/session/factor_cross_section_dataset.csv",
            )
        ]

    def save_session_result(self, **kwargs):  # noqa: ANN003
        return None


class FakeSecEdgar:
    pass


class FakeAgentRuntime:
    def __init__(self) -> None:
        self.writer_prompts: list[str] = []
        self.critic_prompts: list[str] = []

    async def run_planner(self, prompt: str) -> AnalysisPlan:  # noqa: ARG002
        return AnalysisPlan(
            question_type=QuestionType.general_health,
            objective="performance",
            explanation="Test plan",
            dynamic_workflow="general_health",
            macro_overlay_needed=True,
            earnings_overlay_needed=True,
            relevant_tickers=["AAPL"],
        )

    async def run_dynamic_eda(self, prompt: str, *, context: object) -> DynamicEDAResult:  # noqa: ARG002
        return DynamicEDAResult(
            workflow="general_health",
            question_type=QuestionType.general_health,
            findings=[],
            tables=[],
            news_intel=NewsIntelResult(
                query="How healthy is this portfolio?",
                retrieval_sources=["Alpha Vantage NEWS_SENTIMENT", "GDELT DOC 2.0"],
                articles=[
                    NewsArticle(
                        source="Alpha Vantage NEWS_SENTIMENT",
                        source_type="news",
                        title="Tech sentiment cools",
                        url="https://example.com/tech-sentiment",
                        published_at="2025-08-22T12:00:00+00:00",
                        domain="example.com",
                        tickers=["AAPL"],
                        topics=["technology", "rates"],
                        sentiment=-0.1,
                        relevance=0.8,
                    )
                ],
                source_stats=[
                    NewsSourceStats(
                        source="Alpha Vantage NEWS_SENTIMENT",
                        article_count=1,
                        avg_sentiment=-0.1,
                        latest_published_at="2025-08-22T12:00:00+00:00",
                    )
                ],
                dominant_topics=["technology", "rates"],
                caveats=[],
            ),
        )


class FactorRuntime(FakeAgentRuntime):
    async def run_planner(self, prompt: str) -> AnalysisPlan:  # noqa: ARG002
        return AnalysisPlan(
            question_type=QuestionType.factor_cross_section,
            objective="performance",
            explanation="Factor plan",
            dynamic_workflow="factor_cross_section",
            comparison_universe="sector_peers",
            comparison_sector_filters=["Technology"],
            comparison_ticker_limit=25,
            relevant_tickers=["AAPL", "MSFT"],
        )

    async def run_dynamic_eda(self, prompt: str, *, context: object) -> DynamicEDAResult:  # noqa: ARG002
        return DynamicEDAResult(
            workflow="factor_cross_section",
            question_type=QuestionType.factor_cross_section,
            findings=[],
            tables=[
                AnalysisTable(
                    name="Sector Return Comparison",
                    columns=["sector", "stock_count", "avg_trailing_return"],
                    rows=[
                        {"sector": "Technology", "stock_count": 5, "avg_trailing_return": 0.22},
                        {"sector": "Utilities", "stock_count": 4, "avg_trailing_return": 0.04},
                    ],
                ),
                AnalysisTable(
                    name="Metric Correlations vs Returns",
                    columns=["metric", "target", "correlation", "sector_neutral_correlation", "observations"],
                    rows=[
                        {
                            "metric": "net_margin",
                            "target": "forward_21d_return",
                            "correlation": 0.41,
                            "sector_neutral_correlation": 0.28,
                            "observations": 20,
                        }
                    ],
                ),
                AnalysisTable(
                    name="Rank IC Diagnostics",
                    columns=["metric", "target", "spearman_correlation", "sector_neutral_spearman", "observations"],
                    rows=[
                        {
                            "metric": "net_margin",
                            "target": "forward_21d_return",
                            "spearman_correlation": 0.37,
                            "sector_neutral_spearman": 0.21,
                            "observations": 20,
                        }
                    ],
                ),
                AnalysisTable(
                    name="Quantile Bucket Diagnostics",
                    columns=["metric", "q1_avg", "q4_avg", "spread_q4_q1", "monotonic"],
                    rows=[
                        {
                            "metric": "net_margin",
                            "q1_avg": 0.01,
                            "q4_avg": 0.06,
                            "spread_q4_q1": 0.05,
                            "monotonic": True,
                        }
                    ],
                ),
                AnalysisTable(
                    name="Regression Diagnostics",
                    columns=["metric", "target", "slope", "intercept", "r_squared", "observations"],
                    rows=[
                        {
                            "metric": "net_margin",
                            "target": "forward_21d_return",
                            "slope": 0.12,
                            "intercept": 0.0,
                            "r_squared": 0.18,
                            "observations": 20,
                        }
                    ],
                ),
            ],
        )


def test_overlay_failures_become_warnings() -> None:
    orchestrator = PortfolioAnalysisOrchestrator(
        intake_service=PassThroughIntake(),
        market_data_service=FakeMarketData(),
        analytics_service=AnalyticsService(),
        dynamic_eda_service=FakeDynamicEDAService(),
        scenario_service=FakeScenarioService(),
        sec_edgar_service=FakeSecEdgar(),
        artifact_service=FakeArtifactService(),
        agent_runtime=FakeAgentRuntime(),
        risk_free_fallback=0.02,
    )
    payload = PortfolioInput(
        holdings=[Holding(ticker="AAPL", shares=10, company_name="Apple Inc", sector="Technology")],
        question="How healthy is this portfolio?",
    )

    result = asyncio.run(orchestrator.analyze(payload))

    assert result.overlays.earnings is not None
    assert any(
        warning.code == "overlay_unavailable" and warning.source == "research_overlay"
        for warning in result.warnings
    )


def test_writer_and_critic_receive_news_intel_in_evidence_pack() -> None:
    runtime = FakeAgentRuntime()
    orchestrator = PortfolioAnalysisOrchestrator(
        intake_service=PassThroughIntake(),
        market_data_service=FakeMarketData(),
        analytics_service=AnalyticsService(),
        dynamic_eda_service=FakeDynamicEDAService(),
        scenario_service=FakeScenarioService(),
        sec_edgar_service=FakeSecEdgar(),
        artifact_service=FakeArtifactService(),
        agent_runtime=runtime,
        risk_free_fallback=0.02,
    )
    payload = PortfolioInput(
        holdings=[Holding(ticker="AAPL", shares=10, company_name="Apple Inc", sector="Technology")],
        question="How healthy is this portfolio?",
    )

    asyncio.run(orchestrator.analyze(payload))

    assert runtime.writer_prompts
    assert runtime.critic_prompts
    assert "news_intel" in runtime.writer_prompts[0]
    assert "Alpha Vantage NEWS_SENTIMENT" in runtime.writer_prompts[0]
    assert "dominant_topics" in runtime.critic_prompts[0]


def test_writer_and_critic_receive_factor_cross_section_summary() -> None:
    runtime = FactorRuntime()
    orchestrator = PortfolioAnalysisOrchestrator(
        intake_service=PassThroughIntake(),
        market_data_service=FakeMarketData(),
        analytics_service=AnalyticsService(),
        dynamic_eda_service=FakeDynamicEDAService(),
        scenario_service=FakeScenarioService(),
        sec_edgar_service=FakeSecEdgar(),
        artifact_service=FakeArtifactService(),
        agent_runtime=runtime,
        risk_free_fallback=0.02,
    )
    payload = PortfolioInput(
        holdings=[Holding(ticker="AAPL", shares=10, company_name="Apple Inc", sector="Technology")],
        question="Compare which metrics correlate with returns across sector peers.",
    )

    asyncio.run(orchestrator.analyze(payload))

    assert runtime.writer_prompts
    assert runtime.critic_prompts
    assert "factor_cross_section_summary" in runtime.writer_prompts[0]
    assert "net_margin" in runtime.writer_prompts[0]
    assert "monotonic" in runtime.critic_prompts[0]


def test_windowed_analysis_persists_session_result(tmp_path: Path) -> None:
    database = Database(tmp_path / "app.db")
    database.initialize()
    artifact_service = ArtifactService(
        database=database,
        artifacts_dir=tmp_path / "artifacts",
    )
    orchestrator = PortfolioAnalysisOrchestrator(
        intake_service=PassThroughIntake(),
        market_data_service=FakeMarketData(),
        analytics_service=AnalyticsService(),
        dynamic_eda_service=FakeDynamicEDAService(),
        scenario_service=FakeScenarioService(),
        sec_edgar_service=FakeSecEdgar(),
        artifact_service=artifact_service,
        agent_runtime=FakeAgentRuntime(),
        risk_free_fallback=0.02,
    )
    payload = PortfolioInput(
        holdings=[Holding(ticker="AAPL", shares=10, company_name="Apple Inc", sector="Technology")],
        question="What drove performance during 2024 only?",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 12, 31),
    )

    result = asyncio.run(orchestrator.analyze(payload))

    assert result.normalized_portfolio.start_date == date(2024, 1, 1)
    with database.connect() as connection:
        row = connection.execute(
            "SELECT portfolio_json FROM analysis_sessions WHERE session_id = ?",
            (result.session_id,),
        ).fetchone()

    assert row is not None
    assert '"start_date": "2024-01-01"' in row["portfolio_json"]
    assert '"end_date": "2024-12-31"' in row["portfolio_json"]
    assert any(
        warning.source == "sample_window" and warning.code == "effective_end_shifted"
        for warning in result.warnings
    )


class WrongRiskAdjustedPlannerRuntime(FakeAgentRuntime):
    async def run_planner(self, prompt: str) -> AnalysisPlan:  # noqa: ARG002
        return AnalysisPlan(
            question_type=QuestionType.performance_drivers,
            objective="performance",
            explanation="Misclassified test plan",
            dynamic_workflow="performance_drivers",
            candidate_search_needed=False,
            relevant_tickers=["AAPL"],
        )

    async def run_candidate_search(self, prompt: str, *, context: object) -> CandidateSearchResult:  # noqa: ARG002
        return CandidateSearchResult(
            objective="performance",
            method="test",
            candidates=[],
        )


class BrokenDynamicEDARuntime(FakeAgentRuntime):
    async def run_dynamic_eda(self, prompt: str, *, context: object) -> DynamicEDAResult:  # noqa: ARG002
        raise RuntimeError("invalid dynamic eda output")


@pytest.mark.parametrize(
    "question",
    [
        "What should I add to improve risk-adjusted returns?",
        "What stock could improve Sharpe without killing returns?",
        "Find me an addition that improves Sharpe and reduces beta.",
        "How can I lower beta while preserving return potential?",
        "Which candidate lowers volatility and beta but does not hurt return in the lookback?",
    ],
)
def test_risk_adjusted_addition_questions_are_forced_into_candidate_search(
    tmp_path: Path,
    question: str,
) -> None:
    database = Database(tmp_path / "app.db")
    database.initialize()
    artifact_service = ArtifactService(
        database=database,
        artifacts_dir=tmp_path / "artifacts",
    )
    orchestrator = PortfolioAnalysisOrchestrator(
        intake_service=PassThroughIntake(),
        market_data_service=FakeMarketData(),
        analytics_service=AnalyticsService(),
        dynamic_eda_service=FakeDynamicEDAService(),
        scenario_service=FakeScenarioService(),
        sec_edgar_service=FakeSecEdgar(),
        artifact_service=artifact_service,
        agent_runtime=WrongRiskAdjustedPlannerRuntime(),
        risk_free_fallback=0.02,
    )
    payload = PortfolioInput(
        holdings=[
            Holding(ticker="AAPL", shares=10, company_name="Apple Inc", sector="Technology"),
            Holding(ticker="MSFT", shares=8, company_name="Microsoft", sector="Technology"),
        ],
        question=question,
    )

    result = asyncio.run(orchestrator.analyze(payload))

    assert result.plan.question_type == QuestionType.concentration_diversification
    assert result.plan.candidate_search_needed is True
    assert result.plan.scenario_needed is False
    assert result.dynamic_eda.candidate_search is not None
    assert result.plan.question_type == QuestionType.concentration_diversification


class WrongDiagnosticCandidatePlannerRuntime(FakeAgentRuntime):
    async def run_planner(self, prompt: str) -> AnalysisPlan:  # noqa: ARG002
        return AnalysisPlan(
            question_type=QuestionType.concentration_diversification,
            objective="diversify",
            explanation="Misclassified diagnostic plan",
            dynamic_workflow="correlation_cluster",
            candidate_search_needed=True,
            relevant_tickers=["AAPL", "MSFT"],
        )


def test_diagnostic_cluster_questions_do_not_force_candidate_search(tmp_path: Path) -> None:
    database = Database(tmp_path / "app.db")
    database.initialize()
    artifact_service = ArtifactService(
        database=database,
        artifacts_dir=tmp_path / "artifacts",
    )
    orchestrator = PortfolioAnalysisOrchestrator(
        intake_service=PassThroughIntake(),
        market_data_service=FakeMarketData(),
        analytics_service=AnalyticsService(),
        dynamic_eda_service=FakeDynamicEDAService(),
        scenario_service=FakeScenarioService(),
        sec_edgar_service=FakeSecEdgar(),
        artifact_service=artifact_service,
        agent_runtime=WrongDiagnosticCandidatePlannerRuntime(),
        risk_free_fallback=0.02,
    )
    payload = PortfolioInput(
        holdings=[
            Holding(ticker="AAPL", shares=10, company_name="Apple Inc", sector="Technology"),
            Holding(ticker="MSFT", shares=8, company_name="Microsoft", sector="Technology"),
        ],
        question="What is the most correlated cluster in this portfolio?",
    )

    result = asyncio.run(orchestrator.analyze(payload))

    assert result.plan.candidate_search_needed is False
    assert result.dynamic_eda.candidate_search is None


def test_dynamic_eda_agent_failure_falls_back_to_deterministic_service(tmp_path: Path) -> None:
    database = Database(tmp_path / "app.db")
    database.initialize()
    artifact_service = ArtifactService(
        database=database,
        artifacts_dir=tmp_path / "artifacts",
    )
    orchestrator = PortfolioAnalysisOrchestrator(
        intake_service=PassThroughIntake(),
        market_data_service=FakeMarketData(),
        analytics_service=AnalyticsService(),
        dynamic_eda_service=FakeDynamicEDAService(),
        scenario_service=FakeScenarioService(),
        sec_edgar_service=FakeSecEdgar(),
        artifact_service=artifact_service,
        agent_runtime=BrokenDynamicEDARuntime(),
        risk_free_fallback=0.02,
    )
    payload = PortfolioInput(
        holdings=[Holding(ticker="AAPL", shares=10, company_name="Apple Inc", sector="Technology")],
        question="How will a move in rates affect my portfolio?",
    )

    result = asyncio.run(orchestrator.analyze(payload))

    assert result.dynamic_eda.workflow == "general_health"
    assert any(
        warning.code == "dynamic_eda_agent_fallback" and warning.source == "dynamic_eda"
        for warning in result.warnings
    )
