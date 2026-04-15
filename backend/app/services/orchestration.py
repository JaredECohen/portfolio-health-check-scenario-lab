from __future__ import annotations

import asyncio
import logging
from uuid import uuid4

from app.agents.runtime import AgentRuntime
from app.models.schemas import (
    AnalysisResponse,
    AnalysisPlan,
    AnalysisWarning,
    EarningsOverlayResult,
    FilingsOverlayResult,
    HypotheticalPosition,
    MacroOverlayResult,
    OverlayBundle,
    PortfolioInput,
    QuestionType,
)
from app.tools.agent_tools import AnalysisRunContext
from app.services.analytics import AnalyticsService
from app.services.artifacts import ArtifactService
from app.services.dynamic_eda import DynamicEDAService
from app.services.market_data import MarketDataService
from app.services.portfolio_intake import PortfolioIntakeService
from app.services.scenario import ScenarioService
from app.services.sec_edgar import SecEdgarService

logger = logging.getLogger(__name__)


class PortfolioAnalysisOrchestrator:
    def __init__(
        self,
        *,
        intake_service: PortfolioIntakeService,
        market_data_service: MarketDataService,
        analytics_service: AnalyticsService,
        dynamic_eda_service: DynamicEDAService,
        scenario_service: ScenarioService,
        sec_edgar_service: SecEdgarService,
        artifact_service: ArtifactService,
        agent_runtime: AgentRuntime,
        risk_free_fallback: float,
    ) -> None:
        self.intake_service = intake_service
        self.market_data_service = market_data_service
        self.analytics_service = analytics_service
        self.dynamic_eda_service = dynamic_eda_service
        self.scenario_service = scenario_service
        self.sec_edgar_service = sec_edgar_service
        self.artifact_service = artifact_service
        self.agent_runtime = agent_runtime
        self.risk_free_fallback = risk_free_fallback

    async def analyze(self, payload: PortfolioInput) -> AnalysisResponse:
        session_id = uuid4().hex
        warnings: list[AnalysisWarning] = []
        normalized = await self.intake_service.normalize(payload)
        tickers = [holding.ticker for holding in normalized.holdings]
        price_history, benchmark_history = await self.market_data_service.fetch_price_history(
            tickers=tickers,
            benchmark_symbol=normalized.benchmark,
            lookback_days=normalized.lookback_days,
            start_date=normalized.start_date,
            end_date=normalized.end_date,
        )
        risk_free_rate = await self.market_data_service.get_risk_free_rate(self.risk_free_fallback)
        baseline_bundle = self.analytics_service.compute_baseline(
            holdings=normalized.holdings,
            benchmark_symbol=normalized.benchmark,
            price_history=price_history,
            benchmark_history=benchmark_history,
            risk_free_rate=risk_free_rate,
        )
        warnings.extend(self._sample_window_warnings(payload=normalized, baseline=baseline_bundle.baseline))
        baseline_summary = self._baseline_summary(normalized)
        plan = await self.agent_runtime.run_planner(
            f"""
            User question: {normalized.question}
            Benchmark: {normalized.benchmark}
            Hypothetical addition present: {bool(normalized.hypothetical_position)}
            Holdings:
            {baseline_summary}
            Baseline metrics:
            {baseline_bundle.metrics_map}
            """
        )
        plan = self._stabilize_plan(
            payload=normalized,
            plan=plan,
            baseline=baseline_bundle.baseline,
        )
        context = AnalysisRunContext(
            question=normalized.question,
            plan=plan,
            baseline_bundle=baseline_bundle,
            dynamic_eda_service=self.dynamic_eda_service,
            scenario_service=self.scenario_service,
            sec_edgar_service=self.sec_edgar_service,
            lookback_days=normalized.lookback_days,
            benchmark_symbol=normalized.benchmark,
            hypothetical_present=normalized.hypothetical_position is not None,
            start_date=normalized.start_date,
            end_date=normalized.end_date,
        )
        overlay_tickers = self._overlay_tickers(
            payload=normalized,
            plan=plan,
        )
        try:
            dynamic_eda = await self.agent_runtime.run_dynamic_eda(
                f"Run the {plan.dynamic_workflow} workflow for this question: {normalized.question}",
                context=context,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Dynamic EDA agent failed; falling back to deterministic service", exc_info=exc)
            warnings.append(
                AnalysisWarning(
                    code="dynamic_eda_agent_fallback",
                    source="dynamic_eda",
                    severity="warning",
                    message="The dynamic EDA agent response was invalid for this run, so the app fell back to deterministic analysis output.",
                )
            )
            dynamic_eda = await self.dynamic_eda_service.execute(
                plan=plan,
                question=normalized.question,
                baseline_bundle=baseline_bundle,
            )

        overlays = OverlayBundle()
        overlay_tasks = []
        if plan.macro_overlay_needed:
            overlay_tasks.append(
                self.agent_runtime.run_macro_overlay(
                    f"Interpret the macro sensitivity for: {normalized.question}",
                    context=context,
                )
            )
        if plan.earnings_overlay_needed and overlay_tickers:
            overlay_tasks.append(
                self.agent_runtime.run_earnings_overlay(
                    f"Analyze recent earnings transcript signals for {overlay_tickers}",
                    context=context,
                )
            )
        if plan.filings_overlay_needed and overlay_tickers:
            overlay_tasks.append(
                self.agent_runtime.run_filings_overlay(
                    f"Analyze recent filings for {overlay_tickers}",
                    context=context,
                )
            )
        if overlay_tasks:
            overlay_results = await asyncio.gather(*overlay_tasks, return_exceptions=True)
            for item in overlay_results:
                if isinstance(item, Exception):
                    logger.warning("Overlay execution failed", exc_info=item)
                    warnings.append(
                        AnalysisWarning(
                            code="overlay_unavailable",
                            source="research_overlay",
                            severity="warning",
                            message="One or more research overlays could not be completed. Core analytics remain available.",
                        )
                    )
                    continue
                if isinstance(item, MacroOverlayResult):
                    overlays.macro = item
                if isinstance(item, EarningsOverlayResult):
                    overlays.earnings = item
                if isinstance(item, FilingsOverlayResult):
                    overlays.filings = item

        if plan.candidate_search_needed:
            try:
                dynamic_eda.candidate_search = await self.agent_runtime.run_candidate_search(
                    "Rank curated candidate additions for this portfolio objective.",
                    context=context,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Candidate search failed", exc_info=exc)
                warnings.append(
                    AnalysisWarning(
                        code="candidate_search_unavailable",
                        source="candidate_search",
                        severity="warning",
                        message="Candidate search could not be completed for this run. Baseline analytics and EDA are still valid.",
                    )
                )

        after_bundle = None
        should_run_scenario = normalized.hypothetical_position is not None or (
            plan.scenario_needed and getattr(plan.question_type, "value", str(plan.question_type)) == "what_if_addition"
        )
        if should_run_scenario:
            hypothetical = normalized.hypothetical_position or self._default_hypothetical_from_plan(
                payload=normalized,
                plan=plan,
            )
            if hypothetical is not None:
                scenario, after_bundle = await self.scenario_service.simulate_addition(
                    baseline_bundle=baseline_bundle,
                    hypothetical_position=hypothetical,
                    benchmark_symbol=normalized.benchmark,
                    lookback_days=normalized.lookback_days,
                    start_date=normalized.start_date,
                    end_date=normalized.end_date,
                )
                dynamic_eda = self.dynamic_eda_service.enrich_with_scenario(
                    dynamic_eda=dynamic_eda,
                    scenario=scenario,
                    baseline_bundle=baseline_bundle,
                    after_bundle=after_bundle,
                    question=normalized.question,
                )

        evidence_pack = {
            "baseline_metrics": baseline_bundle.metrics_map,
            "eda_findings": [item.model_dump() for item in dynamic_eda.findings],
            "factor_cross_section_summary": self._factor_cross_section_summary(dynamic_eda),
            "news_intel": dynamic_eda.news_intel.model_dump() if dynamic_eda.news_intel else None,
            "plan": plan.model_dump(),
            "overlays": overlays.model_dump(),
            "candidate_search": dynamic_eda.candidate_search.model_dump() if dynamic_eda.candidate_search else None,
            "scenario": dynamic_eda.scenario_analysis.model_dump() if dynamic_eda.scenario_analysis else None,
        }
        draft_memo = await self.agent_runtime.run_writer(
            f"""
            User question: {normalized.question}
            Portfolio summary:
            {baseline_summary}

            Evidence pack:
            {evidence_pack}
            """
        )
        critic = await self.agent_runtime.run_critic(
            f"""
            Review this draft memo against the evidence pack.

            Draft memo:
            {draft_memo.model_dump()}

            Evidence pack:
            {evidence_pack}
            """
        )
        artifacts = []
        artifacts.extend(
            self.artifact_service.generate_baseline_charts(
                session_id=session_id,
                baseline=baseline_bundle.baseline,
            )
        )
        if plan.question_type == QuestionType.factor_cross_section:
            factor_frame = await self.dynamic_eda_service.build_factor_cross_section_dataset(
                plan=plan,
                baseline_bundle=baseline_bundle,
            )
            if not factor_frame.empty:
                artifacts.extend(
                    self.artifact_service.save_dataframe_artifact(
                        session_id=session_id,
                        kind="factor_cross_section_dataset",
                        title="Factor cross-section dataset",
                        frame=factor_frame,
                        metadata={
                            "question_type": plan.question_type.value,
                            "comparison_universe": plan.comparison_universe,
                            "comparison_sector_filters": plan.comparison_sector_filters,
                            "comparison_ticker_limit": plan.comparison_ticker_limit,
                            "effective_start_date": baseline_bundle.baseline.effective_start_date,
                            "effective_end_date": baseline_bundle.baseline.effective_end_date,
                            "row_count": int(len(factor_frame)),
                            "columns": list(factor_frame.columns),
                            "routed_tickers": plan.relevant_tickers,
                        },
                    )
                )
        if dynamic_eda.scenario_analysis:
            artifacts.append(
                self.artifact_service.generate_scenario_chart(
                    session_id=session_id,
                    scenario=dynamic_eda.scenario_analysis,
                )
            )
        artifacts.append(
            self.artifact_service.save_json_artifact(
                session_id=session_id,
                kind="analysis_response",
                title="Analysis response JSON",
                payload={
                    "baseline": baseline_bundle.baseline.model_dump(),
                    "plan": plan.model_dump(),
                    "dynamic_eda": dynamic_eda.model_dump(),
                    "overlays": overlays.model_dump(),
                    "critic": critic.model_dump(),
                },
            )
        )
        artifacts.append(
            self.artifact_service.save_markdown_memo(
                session_id=session_id,
                memo=critic.revised_memo,
            )
        )
        response = AnalysisResponse(
            session_id=session_id,
            normalized_portfolio=normalized,
            baseline=baseline_bundle.baseline,
            plan=plan,
            dynamic_eda=dynamic_eda,
            overlays=overlays,
            final_memo=critic.revised_memo,
            critic=critic,
            warnings=self._dedupe_warnings(warnings),
            artifacts=artifacts,
        )
        self.artifact_service.save_session_result(
            session_id=session_id,
            question=normalized.question,
            portfolio_json=normalized.model_dump(),
            plan_json=plan.model_dump(),
            result_json=response.model_dump(),
        )
        del after_bundle
        return response

    @staticmethod
    def _sample_window_warnings(*, payload: PortfolioInput, baseline) -> list[AnalysisWarning]:
        warnings: list[AnalysisWarning] = []
        requested_start = payload.start_date.isoformat() if payload.start_date else None
        requested_end = payload.end_date.isoformat() if payload.end_date else None
        if requested_start and baseline.effective_start_date > requested_start:
            warnings.append(
                AnalysisWarning(
                    code="effective_start_shifted",
                    source="sample_window",
                    severity="info",
                    message=(
                        f"Effective analysis start date shifted to {baseline.effective_start_date} because full price history was not available for all holdings from {requested_start}."
                    ),
                )
            )
        if requested_end and baseline.effective_end_date < requested_end:
            warnings.append(
                AnalysisWarning(
                    code="effective_end_shifted",
                    source="sample_window",
                    severity="info",
                    message=(
                        f"Effective analysis end date shifted to {baseline.effective_end_date} because full price history was not available for all holdings through {requested_end}."
                    ),
                )
            )
        if not requested_start and not requested_end and baseline.effective_observations < payload.lookback_days:
            warnings.append(
                AnalysisWarning(
                    code="effective_sample_shorter_than_requested",
                    source="sample_window",
                    severity="info",
                    message=(
                        f"Requested trailing window was {payload.lookback_days} trading days, but the effective aligned sample was {baseline.effective_observations} observations ending {baseline.effective_end_date}."
                    ),
                )
            )
        return warnings

    @staticmethod
    def _dedupe_warnings(warnings: list[AnalysisWarning]) -> list[AnalysisWarning]:
        deduped: list[AnalysisWarning] = []
        seen: set[tuple[str, str, str, str]] = set()
        for warning in warnings:
            key = (warning.code, warning.source, warning.severity, warning.message)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(warning)
        return deduped

    @staticmethod
    def _factor_cross_section_summary(dynamic_eda) -> dict[str, object] | None:
        if dynamic_eda.question_type != QuestionType.factor_cross_section:
            return None
        table_map = {table.name: table for table in dynamic_eda.tables}
        sectors = table_map.get("Sector Return Comparison")
        correlations = table_map.get("Metric Correlations vs Returns")
        rank_ic = table_map.get("Rank IC Diagnostics")
        buckets = table_map.get("Quantile Bucket Diagnostics")
        regressions = table_map.get("Regression Diagnostics")
        return {
            "top_sector": sectors.rows[0] if sectors and sectors.rows else None,
            "bottom_sector": sectors.rows[-1] if sectors and sectors.rows else None,
            "top_metric_relationship": correlations.rows[0] if correlations and correlations.rows else None,
            "top_rank_ic": rank_ic.rows[0] if rank_ic and rank_ic.rows else None,
            "bucket_monotonicity": buckets.rows[:5] if buckets and buckets.rows else [],
            "top_regression": regressions.rows[0] if regressions and regressions.rows else None,
        }

    @staticmethod
    def _baseline_summary(payload: PortfolioInput) -> str:
        return "\n".join(
            [
                f"- {holding.ticker}: {holding.shares} shares, sector={holding.sector}, company={holding.company_name}"
                for holding in payload.holdings
            ]
        )

    @staticmethod
    def _default_hypothetical_from_plan(
        *,
        payload: PortfolioInput,
        plan,
    ) -> HypotheticalPosition | None:
        existing = {holding.ticker for holding in payload.holdings}
        for ticker in plan.relevant_tickers:
            if ticker not in existing:
                return HypotheticalPosition(ticker=ticker, target_weight=0.05)
        return None

    @staticmethod
    def _stabilize_plan(
        *,
        payload: PortfolioInput,
        plan: AnalysisPlan,
        baseline,
    ) -> AnalysisPlan:
        plan = plan.model_copy(
            update={
                "relevant_tickers": plan.relevant_tickers or [item.ticker for item in baseline.positions[:4]],
            }
        )
        if payload.hypothetical_position is None and PortfolioAnalysisOrchestrator._is_open_ended_candidate_search_question(
            payload.question
        ):
            relevant_tickers = [item.ticker for item in baseline.positions[:6]]
            objective = PortfolioAnalysisOrchestrator._candidate_search_objective(payload.question)
            return plan.model_copy(
                update={
                    "question_type": QuestionType.concentration_diversification,
                    "objective": objective,
                    "dynamic_workflow": PortfolioAnalysisOrchestrator._candidate_search_workflow(objective),
                    "scenario_needed": False,
                    "candidate_search_needed": True,
                    "macro_overlay_needed": False,
                    "relevant_tickers": relevant_tickers,
                    "investigation_steps": PortfolioAnalysisOrchestrator._candidate_search_steps(objective),
                    "caveats": PortfolioAnalysisOrchestrator._candidate_search_caveats(objective),
                }
            )
        if (
            payload.hypothetical_position is None
            and plan.candidate_search_needed
            and PortfolioAnalysisOrchestrator._is_diagnostic_diversification_question(payload.question)
        ):
            return plan.model_copy(
                update={
                    "candidate_search_needed": False,
                    "scenario_needed": False,
                }
            )
        return plan

    @staticmethod
    def _is_open_ended_candidate_search_question(question: str) -> bool:
        normalized = " ".join(question.lower().strip().split())
        request_markers = (
            "what should i add",
            "what stock",
            "which stock",
            "which candidate",
            "find me",
            "screen for",
            "single stock",
            "addition",
            "candidate",
            "what should i own",
            "what should i buy",
            "what do i add",
            "how can i",
            "how do i",
            "recommend",
        )
        if not any(marker in normalized for marker in request_markers):
            return False
        if PortfolioAnalysisOrchestrator._is_diagnostic_diversification_question(normalized):
            return False
        return (
            PortfolioAnalysisOrchestrator._has_risk_adjusted_candidate_intent(normalized)
            or PortfolioAnalysisOrchestrator._has_diversification_candidate_intent(normalized)
        )

    @staticmethod
    def _has_risk_adjusted_candidate_intent(normalized: str) -> bool:
        risk_adjusted_markers = (
            "risk-adjusted",
            "risk adjusted",
            "sharpe",
            "lower beta",
            "reduce beta",
            "less correlated",
            "lower volatility",
            "improve risk-adjusted returns",
            "improve risk adjusted returns",
            "keeping returns while reducing risk",
            "return per unit of risk",
            "preserving return",
            "without killing returns",
            "without hurting returns",
            "do not want to give up returns",
            "don't want to give up returns",
            "does not hurt return",
            "doesn't hurt return",
            "maintain return",
            "keep returns intact",
            "reduce volatility",
        )
        return any(marker in normalized for marker in risk_adjusted_markers)

    @staticmethod
    def _has_diversification_candidate_intent(normalized: str) -> bool:
        diversification_markers = (
            "diversify",
            "less correlated",
            "uncorrelated",
            "reduce concentration",
            "reduce sector crowding",
            "reduce crowding",
            "broaden exposure",
            "spread out",
            "lower concentration",
        )
        return any(marker in normalized for marker in diversification_markers)

    @staticmethod
    def _is_diagnostic_diversification_question(question: str) -> bool:
        normalized = " ".join(question.lower().strip().split())
        diagnostic_markers = (
            "what is the most correlated cluster",
            "most correlated cluster",
            "correlated cluster",
            "which holdings are most correlated",
            "where is the sector crowding",
            "am i too concentrated",
            "how concentrated",
            "what is driving concentration",
            "cluster in this portfolio",
        )
        recommendation_markers = (
            "what should i add",
            "what stock",
            "which stock",
            "which candidate",
            "find me",
            "screen for",
            "recommend",
            "what should i buy",
            "what do i add",
        )
        return any(marker in normalized for marker in diagnostic_markers) and not any(
            marker in normalized for marker in recommendation_markers
        )

    @staticmethod
    def _candidate_search_objective(question: str) -> str:
        normalized = " ".join(question.lower().strip().split())
        if PortfolioAnalysisOrchestrator._has_risk_adjusted_candidate_intent(normalized):
            return "performance"
        return "diversify"

    @staticmethod
    def _candidate_search_workflow(objective: str) -> str:
        if objective == "performance":
            return (
                "Run a candidate-search workflow focused on improving risk-adjusted returns. "
                "Use the existing portfolio's concentration, sector exposures, volatility, beta, "
                "Sharpe ratio, return versus SPY, and correlation structure as the baseline EDA, then shortlist and "
                "rank individual equities that improve Sharpe, maintain or improve trailing return, "
                "reduce beta, reduce volatility, and lower correlation concentration."
            )
        return (
            "Run a candidate-search workflow focused on diversification improvement. "
            "Use the existing portfolio's concentration, sector exposures, top correlated clusters, "
            "Herfindahl index, top-3 weight, and pairwise correlation structure as the baseline EDA, then shortlist "
            "and rank individual equities that lower concentration, reduce sector crowding, and improve correlation balance."
        )

    @staticmethod
    def _candidate_search_steps(objective: str) -> list[str]:
        if objective == "performance":
            return [
                "Confirm the current portfolio's Sharpe ratio, beta, annualized volatility, return versus SPY, and correlation structure.",
                "Use candidate search to shortlist and rank individual equities that improve risk-adjusted return rather than testing the benchmark or an ETF.",
                "Prioritize names that improve Sharpe while preserving or improving trailing return and lowering beta and volatility.",
                "Use sector exposure and standalone portfolio correlation as secondary filters so the recommended addition is not just another highly correlated holding.",
                "Return ranked candidates with before/after metric deltas and explicit rationale tied to the deterministic scenario results.",
            ]
        return [
            "Confirm the current portfolio's top-3 weight, Herfindahl concentration, sector exposures, and internal correlation hotspots.",
            "Use candidate search to shortlist and rank individual equities that reduce concentration and improve diversification rather than testing the benchmark or an ETF.",
            "Prioritize names from underrepresented sectors and with lower standalone correlation to the current portfolio.",
            "Use before/after scenario results to quantify whether the addition lowers sector crowding and pairwise correlation.",
            "Return ranked candidates with explicit diversification metric deltas and rationale tied to deterministic scenario results.",
        ]

    @staticmethod
    def _candidate_search_caveats(objective: str) -> list[str]:
        base = [
            "This is an open-ended addition question, so candidate search is required rather than single-name scenario analysis.",
            "Recommendations must be grounded in deterministic historical metric deltas, not model intuition.",
        ]
        if objective == "performance":
            return [
                *base,
                "If no candidate improves Sharpe while preserving return and lowering beta, the output should say so explicitly.",
            ]
        return [
            *base,
            "If no candidate materially improves diversification metrics without worsening the portfolio profile, the output should say so explicitly.",
        ]

    @staticmethod
    def _overlay_tickers(
        *,
        payload: PortfolioInput,
        plan,
    ) -> list[str]:
        benchmark = payload.benchmark.upper().strip()
        portfolio_tickers = {holding.ticker for holding in payload.holdings}
        filtered = []
        for ticker in plan.relevant_tickers:
            normalized = ticker.upper().strip()
            if normalized == benchmark:
                continue
            if normalized not in portfolio_tickers:
                continue
            if normalized not in filtered:
                filtered.append(normalized)
        return filtered
