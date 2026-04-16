from __future__ import annotations

import asyncio
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from app.database import Database
from app.models.schemas import (
    EDAFinding,
    AnalysisPlan,
    AnalysisTable,
    ArtifactRecord,
    CandidateSearchResult,
    CriticResult,
    DynamicEDAResult,
    EarningsOverlayResult,
    FinalMemo,
    Holding,
    MacroOverlayResult,
    NewsArticle,
    NewsIntelResult,
    NewsSourceStats,
    PortfolioInput,
    QuestionType,
    ResearchAgenda,
    ResearchSynthesis,
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


class WindowFallbackMarketData(FakeMarketData):
    async def fetch_price_history(self, *, tickers, benchmark_symbol, lookback_days, start_date=None, end_date=None):  # noqa: ANN001, ARG002
        if start_date is not None or end_date is not None:
            empty_index = pd.DatetimeIndex([], name="date")
            price_history = {
                ticker: pd.DataFrame({"adjusted_close": pd.Series(dtype=float)}, index=empty_index)
                for ticker in tickers
            }
            benchmark = pd.DataFrame({"adjusted_close": pd.Series(dtype=float)}, index=empty_index)
            return price_history, benchmark
        return await super().fetch_price_history(
            tickers=tickers,
            benchmark_symbol=benchmark_symbol,
            lookback_days=lookback_days,
            start_date=start_date,
            end_date=end_date,
        )


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

    def save_factor_cross_section_run(self, **kwargs):  # noqa: ANN003
        return None


class FakeSecEdgar:
    pass


class FakeAgentRuntime:
    def __init__(self) -> None:
        self.research_director_prompts: list[str] = []
        self.research_synthesis_prompts: list[str] = []
        self.deep_research_prompts: list[str] = []
        self.writer_prompts: list[str] = []
        self.critic_prompts: list[str] = []
        self.latest_dynamic_eda_result: DynamicEDAResult | None = None

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
        self.latest_dynamic_eda_result = DynamicEDAResult(
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
        return self.latest_dynamic_eda_result

    async def run_research_director(self, prompt: str) -> ResearchAgenda:
        self.research_director_prompts.append(prompt)
        return ResearchAgenda(
            focus_areas=["Check whether news and macro evidence change the baseline interpretation."],
            analysis_ideas=["Tie the dominant external topic back to observed portfolio beta and concentration."],
            follow_up_questions=["Does the rates narrative reinforce the quantitative EDA?"],
            overlay_requests=["Macro overlay should focus on whether rates stress matches the beta profile."],
            memo_watchouts=["Do not overstate narrative evidence without connecting it to portfolio metrics."],
        )

    async def run_research_synthesis(self, prompt: str) -> ResearchSynthesis:
        self.research_synthesis_prompts.append(prompt)
        return ResearchSynthesis(
            integrated_insights=["Rates-oriented narrative should be checked against macro overlay evidence."],
            confirmations=["News and macro context both point back to rates sensitivity."],
            eda_implications=["Deep research should explicitly connect narrative evidence to beta and sector mix."],
            memo_implications=["Frame rates sensitivity conservatively and evidence-first."],
        )

    async def run_deep_research(self, prompt: str) -> DynamicEDAResult:
        self.deep_research_prompts.append(prompt)
        assert self.latest_dynamic_eda_result is not None
        return self.latest_dynamic_eda_result.model_copy(
            update={
                "findings": [
                    *self.latest_dynamic_eda_result.findings,
                    EDAFinding(
                        headline="Deep research connected first-pass EDA to the cross-agent handoff.",
                        evidence=["Rates narrative was carried into the second-pass analysis."],
                        metrics={"handoff_count": 1.0},
                    ),
                ]
            }
        )

    async def run_macro_overlay(self, prompt: str, *, context: object) -> MacroOverlayResult:  # noqa: ARG002
        raise RuntimeError("macro overlay unavailable")

    async def run_earnings_overlay(self, prompt: str, *, context: object) -> EarningsOverlayResult:  # noqa: ARG002
        return EarningsOverlayResult(companies=[])

    async def run_writer(self, prompt: str) -> FinalMemo:
        self.writer_prompts.append(prompt)
        return FinalMemo(
            title="Test memo",
            thesis="Test thesis",
            executive_summary=["Baseline summary."],
            evidence=["Baseline evidence."],
            risks_and_caveats=["Test caveat."],
            next_steps=["Test next step."],
        )

    async def run_critic(self, prompt: str) -> CriticResult:
        self.critic_prompts.append(prompt)
        return CriticResult(
            approved_claims=["Grounded claim."],
            flagged_claims=[],
            revised_memo=FinalMemo(
                title="Reviewed memo",
                thesis="Reviewed thesis",
                executive_summary=["Reviewed summary."],
                evidence=["Reviewed evidence."],
                risks_and_caveats=["Reviewed caveat."],
                next_steps=["Reviewed next step."],
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
        self.latest_dynamic_eda_result = DynamicEDAResult(
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
        return self.latest_dynamic_eda_result


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
    assert result.overlays.macro is not None
    assert any(
        warning.code == "macro_overlay_agent_fallback" and warning.source == "research_overlay"
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


def test_multi_agent_collaboration_loop_enriches_analysis_and_writer_context() -> None:
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
        question="How healthy is this portfolio given rates pressure and recent news?",
    )

    result = asyncio.run(orchestrator.analyze(payload))

    assert runtime.research_director_prompts
    assert runtime.research_synthesis_prompts
    assert runtime.deep_research_prompts
    assert "Initial dynamic EDA summary" in runtime.research_director_prompts[0]
    assert "Overlay summary" in runtime.research_synthesis_prompts[0]
    assert "Research synthesis" in runtime.deep_research_prompts[0]
    assert result.agent_collaboration is not None
    assert result.agent_collaboration.research_agenda is not None
    assert result.agent_collaboration.research_synthesis is not None
    assert any(table.name == "Research Agenda" for table in result.dynamic_eda.tables)
    assert any(table.name == "Cross-Agent Synthesis" for table in result.dynamic_eda.tables)
    assert any("Cross-agent synthesis" in finding.headline for finding in result.dynamic_eda.findings)
    assert "agent_collaboration" in runtime.writer_prompts[0]
    assert "agent_collaboration" in runtime.critic_prompts[0]


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
    assert "overall_assessment" in runtime.writer_prompts[0]
    assert "meaningful" in runtime.critic_prompts[0]


def test_final_memo_explicitly_summarizes_factor_cross_section_evidence() -> None:
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

    result = asyncio.run(orchestrator.analyze(payload))

    memo_text = " ".join([*result.final_memo.executive_summary, *result.final_memo.evidence])
    assert "Technology led" in memo_text
    assert "net_margin" in memo_text
    assert "Quantile monotonicity looked monotonic" in memo_text
    assert "Regression / rank-IC evidence looked meaningful" in memo_text


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


def test_windowed_requests_fall_back_when_requested_window_has_no_aligned_history(tmp_path: Path) -> None:
    database = Database(tmp_path / "app.db")
    database.initialize()
    artifact_service = ArtifactService(
        database=database,
        artifacts_dir=tmp_path / "artifacts",
    )
    orchestrator = PortfolioAnalysisOrchestrator(
        intake_service=PassThroughIntake(),
        market_data_service=WindowFallbackMarketData(),
        analytics_service=AnalyticsService(),
        dynamic_eda_service=FakeDynamicEDAService(),
        scenario_service=FakeScenarioService(),
        sec_edgar_service=FakeSecEdgar(),
        artifact_service=artifact_service,
        agent_runtime=BrokenPlannerWriterCriticRuntime(),
        risk_free_fallback=0.02,
    )
    payload = PortfolioInput(
        holdings=[Holding(ticker="AAPL", shares=10, company_name="Apple Inc", sector="Technology")],
        question="What drove performance during 2023 only?",
        start_date=date(2023, 1, 1),
        end_date=date(2023, 12, 31),
    )

    result = asyncio.run(orchestrator.analyze(payload))

    assert result.baseline.effective_observations > 0
    assert any(
        warning.code == "requested_window_unavailable_fallback" and warning.source == "sample_window"
        for warning in result.warnings
    )


def test_factor_cross_section_persists_structured_run_metadata(tmp_path: Path) -> None:
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
        agent_runtime=FactorRuntime(),
        risk_free_fallback=0.02,
    )
    payload = PortfolioInput(
        holdings=[Holding(ticker="AAPL", shares=10, company_name="Apple Inc", sector="Technology")],
        question="Compare which metrics correlate with returns across sector peers.",
    )

    result = asyncio.run(orchestrator.analyze(payload))

    with database.connect() as connection:
        row = connection.execute(
            """
            SELECT universe_mode, sector_filters_json, routed_tickers_json, effective_start_date,
                   effective_end_date, metric_columns_json, row_count
            FROM factor_cross_section_runs
            WHERE session_id = ?
            """,
            (result.session_id,),
        ).fetchone()

    assert row is not None
    assert row["universe_mode"] == "sector_peers"
    assert row["sector_filters_json"] == '["Technology"]'
    assert row["routed_tickers_json"] == '["AAPL", "MSFT"]'
    assert row["effective_start_date"] == result.baseline.effective_start_date
    assert row["effective_end_date"] == result.baseline.effective_end_date
    assert row["metric_columns_json"] == '["trailing_return", "forward_21d_return"]'
    assert row["row_count"] == 1


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


class BrokenPlannerWriterCriticRuntime(FakeAgentRuntime):
    async def run_planner(self, prompt: str) -> AnalysisPlan:  # noqa: ARG002
        raise RuntimeError("planner unavailable")

    async def run_writer(self, prompt: str) -> FinalMemo:  # noqa: ARG002
        raise RuntimeError("writer unavailable")

    async def run_critic(self, prompt: str) -> CriticResult:  # noqa: ARG002
        raise RuntimeError("critic unavailable")


class CandidateSearchFailureRuntime(FakeAgentRuntime):
    async def run_planner(self, prompt: str) -> AnalysisPlan:  # noqa: ARG002
        return AnalysisPlan(
            question_type=QuestionType.concentration_diversification,
            objective="diversify",
            explanation="Candidate search fallback plan",
            dynamic_workflow="concentration_diversification",
            candidate_search_needed=True,
            relevant_tickers=["AAPL", "MSFT"],
        )

    async def run_candidate_search(self, prompt: str, *, context: object) -> CandidateSearchResult:  # noqa: ARG002
        raise RuntimeError("candidate search unavailable")


class CandidateSearchFallbackScenarioService(FakeScenarioService):
    async def rank_candidates(self, **kwargs):  # noqa: ANN003
        return CandidateSearchResult(
            objective="diversify",
            method="deterministic fallback",
            candidates=[],
        )


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


@pytest.mark.parametrize(
    ("question", "expected_objective", "expected_preferences"),
    [
        (
            "What should I add to maximize Sharpe ratio?",
            "performance",
            [("sharpe_ratio", "maximize", False)],
        ),
        (
            "What should I add to minimize average pairwise correlation?",
            "diversify",
            [("average_pairwise_correlation", "minimize", False)],
        ),
        (
            "What should I add to minimize beta without degrading return?",
            "reduce_macro_sensitivity",
            [
                ("beta_vs_benchmark", "minimize", False),
                ("trailing_return", "maximize", True),
            ],
        ),
    ],
)
def test_metric_optimization_questions_map_to_candidate_search_preferences(
    question: str,
    expected_objective: str,
    expected_preferences: list[tuple[str, str, bool]],
) -> None:
    preferences = PortfolioAnalysisOrchestrator._candidate_search_optimization_preferences(question)

    assert [
        (item.metric, item.direction, item.hard_constraint)
        for item in preferences
    ] == expected_preferences
    assert (
        PortfolioAnalysisOrchestrator._candidate_search_objective(
            question,
            optimization_preferences=preferences,
        )
        == expected_objective
    )


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


def test_planner_writer_and_critic_failures_fall_back_to_deterministic_outputs(tmp_path: Path) -> None:
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
        agent_runtime=BrokenPlannerWriterCriticRuntime(),
        risk_free_fallback=0.02,
    )
    payload = PortfolioInput(
        holdings=[Holding(ticker="AAPL", shares=10, company_name="Apple Inc", sector="Technology")],
        question="How healthy is this portfolio?",
    )

    result = asyncio.run(orchestrator.analyze(payload))

    assert result.plan.question_type == QuestionType.general_health
    assert result.final_memo.title == "Portfolio Health Check"
    assert any(char.isdigit() for char in " ".join(result.final_memo.executive_summary + result.final_memo.evidence))
    assert result.critic.approved_claims
    assert any(
        warning.code == "planner_agent_fallback" and warning.source == "planner"
        for warning in result.warnings
    )
    assert any(
        warning.code == "writer_agent_fallback" and warning.source == "memo"
        for warning in result.warnings
    )
    assert any(
        warning.code == "critic_agent_fallback" and warning.source == "critic"
        for warning in result.warnings
    )


@pytest.mark.parametrize(
    ("question", "expected_type"),
    [
        ("Am I too concentrated?", QuestionType.concentration_diversification),
        ("Which names are doing the heavy lifting here?", QuestionType.performance_drivers),
        ("If the 10Y jumps again, what happens to this portfolio?", QuestionType.rates_macro),
    ],
)
def test_deterministic_planner_fallback_routes_common_phrasings(
    tmp_path: Path,
    question: str,
    expected_type: QuestionType,
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
        agent_runtime=BrokenPlannerWriterCriticRuntime(),
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

    assert result.plan.question_type == expected_type


def test_candidate_search_failure_falls_back_to_deterministic_ranking(tmp_path: Path) -> None:
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
        scenario_service=CandidateSearchFallbackScenarioService(),
        sec_edgar_service=FakeSecEdgar(),
        artifact_service=artifact_service,
        agent_runtime=CandidateSearchFailureRuntime(),
        risk_free_fallback=0.02,
    )
    payload = PortfolioInput(
        holdings=[
            Holding(ticker="AAPL", shares=10, company_name="Apple Inc", sector="Technology"),
            Holding(ticker="MSFT", shares=8, company_name="Microsoft", sector="Technology"),
        ],
        question="What should I add to diversify this portfolio?",
    )

    result = asyncio.run(orchestrator.analyze(payload))

    assert result.dynamic_eda.candidate_search is not None
    assert result.dynamic_eda.candidate_search.method == "deterministic fallback"
    assert any(
        warning.code == "candidate_search_agent_fallback" and warning.source == "candidate_search"
        for warning in result.warnings
    )
