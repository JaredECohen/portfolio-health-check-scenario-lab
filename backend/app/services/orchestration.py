from __future__ import annotations

import asyncio
import json
import logging
import re
from uuid import uuid4

from app.agents.runtime import AgentRuntime
from app.models.schemas import (
    AgentCollaboration,
    AnalysisResponse,
    AnalysisPlan,
    AnalysisTable,
    AnalysisWarning,
    CriticResult,
    DynamicEDAResult,
    EDAFinding,
    EarningsOverlayTickerResult,
    FinalMemo,
    EarningsOverlayResult,
    FilingsOverlayTickerResult,
    FilingsOverlayResult,
    HypotheticalPosition,
    MacroOverlayResult,
    OptimizationPreference,
    OverlayBundle,
    PortfolioInput,
    QuestionType,
    ResearchAgenda,
    ResearchSynthesis,
)
from app.tools.agent_tools import AnalysisRunContext, summarize_text_nlp
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
        try:
            baseline_bundle = self.analytics_service.compute_baseline(
                holdings=normalized.holdings,
                benchmark_symbol=normalized.benchmark,
                price_history=price_history,
                benchmark_history=benchmark_history,
                risk_free_rate=risk_free_rate,
            )
        except ValueError as exc:
            if (
                (normalized.start_date is not None or normalized.end_date is not None)
                and "No aligned price history is available" in str(exc)
            ):
                logger.warning("Requested window had no aligned history; falling back to trailing window")
                warnings.append(
                    AnalysisWarning(
                        code="requested_window_unavailable_fallback",
                        source="sample_window",
                        severity="warning",
                        message=(
                            "The requested date window had no aligned price history for all holdings, so the app fell back to the latest available trailing window."
                        ),
                    )
                )
                price_history, benchmark_history = await self.market_data_service.fetch_price_history(
                    tickers=tickers,
                    benchmark_symbol=normalized.benchmark,
                    lookback_days=normalized.lookback_days,
                    start_date=None,
                    end_date=None,
                )
                baseline_bundle = self.analytics_service.compute_baseline(
                    holdings=normalized.holdings,
                    benchmark_symbol=normalized.benchmark,
                    price_history=price_history,
                    benchmark_history=benchmark_history,
                    risk_free_rate=risk_free_rate,
                )
            else:
                raise
        warnings.extend(self._sample_window_warnings(payload=normalized, baseline=baseline_bundle.baseline))
        baseline_summary = self._baseline_summary(normalized)
        try:
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
        except Exception:  # noqa: BLE001
            logger.warning("Planner unavailable; using deterministic fallback plan")
            warnings.append(
                AnalysisWarning(
                    code="planner_agent_fallback",
                    source="planner",
                    severity="warning",
                    message="The planner agent was unavailable for this run, so the app used deterministic question routing instead.",
                )
            )
            plan = self._fallback_plan(
                payload=normalized,
                baseline=baseline_bundle.baseline,
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
        except Exception:  # noqa: BLE001
            logger.warning("Dynamic EDA agent failed; falling back to deterministic service")
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
        initial_eda_summary = self._dynamic_eda_agent_summary(dynamic_eda)
        try:
            research_agenda = await self.agent_runtime.run_research_director(
                f"""
                User question: {normalized.question}
                Plan:
                {plan.model_dump()}

                Portfolio summary:
                {baseline_summary}

                Initial dynamic EDA summary:
                {initial_eda_summary}
                """
            )
        except Exception:  # noqa: BLE001
            logger.warning("Research director unavailable; using deterministic collaboration agenda")
            warnings.append(
                AnalysisWarning(
                    code="research_director_agent_fallback",
                    source="agent_collaboration",
                    severity="warning",
                    message="The research-director agent was unavailable for this run, so the app used a deterministic follow-up agenda instead.",
                )
            )
            research_agenda = self._fallback_research_agenda(
                plan=plan,
                dynamic_eda=dynamic_eda,
                baseline_bundle=baseline_bundle,
            )
        research_agenda_summary = self._research_agenda_summary(research_agenda)

        overlays = OverlayBundle()
        overlay_tasks: list[tuple[str, object]] = []
        if plan.macro_overlay_needed:
            overlay_tasks.append(
                (
                    "macro",
                    self.agent_runtime.run_macro_overlay(
                        (
                            f"Interpret the macro sensitivity for: {normalized.question}\n"
                            f"Research agenda:\n{research_agenda_summary}"
                        ),
                        context=context,
                    ),
                )
            )
        if plan.earnings_overlay_needed and overlay_tickers:
            overlay_tasks.append(
                (
                    "earnings",
                    self.agent_runtime.run_earnings_overlay(
                        (
                            f"Analyze recent earnings transcript signals for {overlay_tickers}\n"
                            f"Research agenda:\n{research_agenda_summary}"
                        ),
                        context=context,
                    ),
                )
            )
        if plan.filings_overlay_needed and overlay_tickers:
            overlay_tasks.append(
                (
                    "filings",
                    self.agent_runtime.run_filings_overlay(
                        (
                            f"Analyze recent filings for {overlay_tickers}\n"
                            f"Research agenda:\n{research_agenda_summary}"
                        ),
                        context=context,
                    ),
                )
            )
        if overlay_tasks:
            overlay_results = await asyncio.gather(
                *(coroutine for _kind, coroutine in overlay_tasks),
                return_exceptions=True,
            )
            for (kind, _coroutine), item in zip(overlay_tasks, overlay_results, strict=True):
                if isinstance(item, Exception):
                    logger.warning("%s overlay execution failed", kind)
                    fallback_overlay = await self._fallback_overlay(
                        kind=kind,
                        context=context,
                        tickers=overlay_tickers,
                    )
                    if fallback_overlay is None:
                        warnings.append(
                            AnalysisWarning(
                                code="overlay_unavailable",
                                source="research_overlay",
                                severity="warning",
                                message="One or more research overlays could not be completed. Core analytics remain available.",
                            )
                        )
                        continue
                    warnings.append(
                        AnalysisWarning(
                            code=f"{kind}_overlay_agent_fallback",
                            source="research_overlay",
                            severity="warning",
                            message=(
                                f"The {kind} overlay agent was unavailable for this run, so the app used deterministic overlay logic instead."
                            ),
                        )
                    )
                    self._assign_overlay_result(overlays=overlays, kind=kind, result=fallback_overlay)
                    continue
                self._assign_overlay_result(overlays=overlays, kind=kind, result=item)

        if plan.candidate_search_needed:
            try:
                dynamic_eda.candidate_search = await self.agent_runtime.run_candidate_search(
                    f"""
                    Rank curated candidate additions for this portfolio objective.

                    Initial dynamic EDA summary:
                    {initial_eda_summary}

                    Research agenda:
                    {research_agenda_summary}
                    """,
                    context=context,
                )
            except Exception:  # noqa: BLE001
                logger.warning("Candidate search failed; attempting deterministic fallback")
                try:
                    dynamic_eda.candidate_search = await self.scenario_service.rank_candidates(
                        baseline_bundle=baseline_bundle,
                        benchmark_symbol=normalized.benchmark,
                        objective=plan.objective,
                        optimization_preferences=plan.optimization_preferences,
                        lookback_days=normalized.lookback_days,
                        start_date=normalized.start_date,
                        end_date=normalized.end_date,
                        max_candidates=5,
                    )
                    warnings.append(
                        AnalysisWarning(
                            code="candidate_search_agent_fallback",
                            source="candidate_search",
                            severity="warning",
                            message="The candidate-search agent was unavailable for this run, so the app used deterministic ranking instead.",
                        )
                    )
                except Exception:  # noqa: BLE001
                    logger.warning("Deterministic candidate search fallback failed")
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

        overlay_summary = self._overlay_agent_summary(overlays)
        try:
            research_synthesis = await self.agent_runtime.run_research_synthesis(
                f"""
                User question: {normalized.question}
                Plan:
                {plan.model_dump()}

                Portfolio summary:
                {baseline_summary}

                Initial dynamic EDA summary:
                {initial_eda_summary}

                Current dynamic EDA summary:
                {self._dynamic_eda_agent_summary(dynamic_eda)}

                Research agenda:
                {research_agenda_summary}

                Overlay summary:
                {overlay_summary}
                """
            )
        except Exception:  # noqa: BLE001
            logger.warning("Research synthesis unavailable; using deterministic synthesis brief")
            warnings.append(
                AnalysisWarning(
                    code="research_synthesis_agent_fallback",
                    source="agent_collaboration",
                    severity="warning",
                    message="The research-synthesis agent was unavailable for this run, so the app used deterministic cross-agent synthesis instead.",
                )
            )
            research_synthesis = self._fallback_research_synthesis(
                dynamic_eda=dynamic_eda,
                overlays=overlays,
            )
        research_synthesis_summary = self._research_synthesis_summary(research_synthesis)
        try:
            refined_dynamic_eda = await self.agent_runtime.run_deep_research(
                f"""
                User question: {normalized.question}
                Plan:
                {plan.model_dump()}

                Portfolio summary:
                {baseline_summary}

                Initial dynamic EDA summary:
                {initial_eda_summary}

                Current dynamic EDA result:
                {dynamic_eda.model_dump()}

                Research agenda:
                {research_agenda.model_dump()}

                Research synthesis:
                {research_synthesis.model_dump()}

                Overlay summary:
                {overlay_summary}
                """
            )
        except Exception:  # noqa: BLE001
            logger.warning("Deep research agent unavailable; using deterministic collaboration enrichment")
            warnings.append(
                AnalysisWarning(
                    code="deep_research_agent_fallback",
                    source="agent_collaboration",
                    severity="warning",
                    message="The deep-research agent was unavailable for this run, so the app used deterministic collaboration enrichment instead.",
                )
            )
            refined_dynamic_eda = dynamic_eda
        dynamic_eda = self._merge_deep_research_result(
            base=dynamic_eda,
            refined=refined_dynamic_eda,
        )
        agent_collaboration = AgentCollaboration(
            research_agenda=research_agenda,
            research_synthesis=research_synthesis,
        )
        dynamic_eda = self._ensure_agent_collaboration_in_dynamic_eda(
            dynamic_eda=dynamic_eda,
            collaboration=agent_collaboration,
        )

        factor_cross_section_summary = self._factor_cross_section_summary(dynamic_eda)
        evidence_pack = {
            "baseline_metrics": baseline_bundle.metrics_map,
            "eda_findings": [item.model_dump() for item in dynamic_eda.findings],
            "factor_cross_section_summary": factor_cross_section_summary,
            "news_intel": dynamic_eda.news_intel.model_dump() if dynamic_eda.news_intel else None,
            "plan": plan.model_dump(),
            "overlays": overlays.model_dump(),
            "agent_collaboration": agent_collaboration.model_dump(),
            "candidate_search": dynamic_eda.candidate_search.model_dump() if dynamic_eda.candidate_search else None,
            "scenario": dynamic_eda.scenario_analysis.model_dump() if dynamic_eda.scenario_analysis else None,
        }
        try:
            draft_memo = await self.agent_runtime.run_writer(
                f"""
                User question: {normalized.question}
                Portfolio summary:
                {baseline_summary}

                Factor cross-section summary:
                {factor_cross_section_summary}

                Agent collaboration:
                {agent_collaboration.model_dump()}

                Evidence pack:
                {evidence_pack}
                """
            )
        except Exception:  # noqa: BLE001
            logger.warning("Writer unavailable; using deterministic memo synthesis")
            warnings.append(
                AnalysisWarning(
                    code="writer_agent_fallback",
                    source="memo",
                    severity="warning",
                    message="The writer agent was unavailable for this run, so the app synthesized the memo deterministically.",
                )
            )
            draft_memo = self._fallback_memo(
                payload=normalized,
                plan=plan,
                baseline_bundle=baseline_bundle,
                dynamic_eda=dynamic_eda,
                overlays=overlays,
                agent_collaboration=agent_collaboration,
                warnings=warnings,
            )
        draft_memo = self._ensure_factor_cross_section_summary_in_memo(
            memo=draft_memo,
            factor_summary=factor_cross_section_summary,
        )
        try:
            critic = await self.agent_runtime.run_critic(
                f"""
                Review this draft memo against the evidence pack.

                Draft memo:
                {draft_memo.model_dump()}

                Factor cross-section summary:
                {factor_cross_section_summary}

                Agent collaboration:
                {agent_collaboration.model_dump()}

                Evidence pack:
                {evidence_pack}
                """
            )
        except Exception:  # noqa: BLE001
            logger.warning("Critic unavailable; using deterministic memo review")
            warnings.append(
                AnalysisWarning(
                    code="critic_agent_fallback",
                    source="critic",
                    severity="warning",
                    message="The critic agent was unavailable for this run, so the app used a deterministic review pass instead.",
                )
            )
            critic = self._fallback_critic(
                memo=draft_memo,
                warnings=warnings,
            )
        critic = critic.model_copy(
            update={
                "revised_memo": self._ensure_factor_cross_section_summary_in_memo(
                    memo=critic.revised_memo,
                    factor_summary=factor_cross_section_summary,
                )
            }
        )
        self.artifact_service.save_session_result(
            session_id=session_id,
            question=normalized.question,
            portfolio_json=normalized.model_dump(),
            plan_json=plan.model_dump(),
            result_json={},
        )
        if plan.question_type == QuestionType.factor_cross_section:
            factor_frame = await self.dynamic_eda_service.build_factor_cross_section_dataset(
                plan=plan,
                baseline_bundle=baseline_bundle,
            )
            if not factor_frame.empty:
                self.artifact_service.save_factor_cross_section_run(
                    session_id=session_id,
                    universe_mode=plan.comparison_universe,
                    sector_filters=plan.comparison_sector_filters,
                    routed_tickers=plan.relevant_tickers,
                    effective_start_date=baseline_bundle.baseline.effective_start_date,
                    effective_end_date=baseline_bundle.baseline.effective_end_date,
                    metric_columns=[
                        str(column)
                        for column in factor_frame.columns
                        if str(column) not in {"ticker", "sector", "company_name"}
                    ],
                    row_count=int(len(factor_frame)),
                    metadata={
                        "comparison_ticker_limit": plan.comparison_ticker_limit,
                        "question_type": plan.question_type.value,
                    },
                )
        response = AnalysisResponse(
            session_id=session_id,
            normalized_portfolio=normalized,
            baseline=baseline_bundle.baseline,
            plan=plan,
            dynamic_eda=dynamic_eda,
            overlays=overlays,
            agent_collaboration=agent_collaboration,
            final_memo=critic.revised_memo,
            critic=critic,
            warnings=self._dedupe_warnings(warnings),
            artifacts=[],
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
    def _assign_overlay_result(*, overlays: OverlayBundle, kind: str, result: object) -> None:
        if kind == "macro" and isinstance(result, MacroOverlayResult):
            overlays.macro = result
        if kind == "earnings" and isinstance(result, EarningsOverlayResult):
            overlays.earnings = result
        if kind == "filings" and isinstance(result, FilingsOverlayResult):
            overlays.filings = result

    async def _fallback_overlay(
        self,
        *,
        kind: str,
        context: AnalysisRunContext,
        tickers: list[str],
    ) -> MacroOverlayResult | EarningsOverlayResult | FilingsOverlayResult | None:
        if kind == "macro":
            return await self._fallback_macro_overlay(context=context)
        if kind == "earnings":
            return await self._fallback_earnings_overlay(context=context, tickers=tickers)
        if kind == "filings":
            return await self._fallback_filings_overlay(context=context, tickers=tickers)
        return None

    async def _fallback_macro_overlay(self, *, context: AnalysisRunContext) -> MacroOverlayResult:
        question_focus = context.plan.question_type.value.replace("_", " ")
        if context.plan.question_type == QuestionType.geopolitical_war:
            try:
                analysis = await self.dynamic_eda_service._analyze_geopolitical_stress(  # noqa: SLF001
                    question=context.question,
                    baseline_bundle=context.baseline_bundle,
                )
            except Exception:  # noqa: BLE001
                analysis = None
            if analysis is None:
                return MacroOverlayResult(
                    question_focus=question_focus,
                    series_used=["WTI", "BRENT", "NATURAL_GAS"],
                    findings=["Commodity shock proxies were unavailable or too sparse for a geopolitical overlay in this run."],
                    portfolio_sensitivities={},
                    benchmark_sensitivities={},
                    caveats=[
                        "This fallback overlay could not align enough commodity shock observations, so conclusions should lean on the baseline and EDA findings."
                    ],
                )
            return MacroOverlayResult(
                question_focus=question_focus,
                series_used=list(analysis.get("screened_series", [])),
                findings=[
                    f"Primary geopolitical proxy was {analysis['primary_series']} with {analysis['primary_shock_days']} stress days in sample.",
                    f"Worst same-day excess return across screened proxies was {analysis['worst_same_day_excess'] * 100:.2f}% versus the benchmark.",
                    f"Best forward 5-day excess response across screened proxies was {(analysis.get('best_forward_5d_excess') or 0.0) * 100:.2f}%.",
                ],
                portfolio_sensitivities={
                    "primary_shock_days": float(analysis["primary_shock_days"]),
                    "worst_same_day_excess": float(analysis["worst_same_day_excess"]),
                    "best_forward_5d_excess": float(analysis.get("best_forward_5d_excess") or 0.0),
                },
                benchmark_sensitivities={},
                caveats=[
                    "This fallback overlay uses commodity shock proxies and benchmark stress filters rather than a structural geopolitical factor model."
                ],
            )
        try:
            analysis = await self.dynamic_eda_service.analyze_rates_regimes(context.baseline_bundle)
        except Exception:  # noqa: BLE001
            analysis = None
        if analysis is None:
            return MacroOverlayResult(
                question_focus=question_focus,
                series_used=["TREASURY_YIELD"],
                findings=["Macro series were unavailable or could not be aligned for this run."],
                portfolio_sensitivities={},
                benchmark_sensitivities={},
                caveats=[
                    "This fallback overlay could not build a usable rate-shock sample, so conclusions should lean on the baseline and deterministic EDA findings."
                ],
            )
        up_stats = analysis.get("yield_up")
        down_stats = analysis.get("yield_down")
        findings: list[str] = [
            f"Primary rates lens was {analysis['series_name']} across {analysis['sample_days']} daily observations from {analysis['sample_start']} to {analysis['sample_end']}."
        ]
        if up_stats is not None:
            findings.append(
                f"Yield-up shocks averaged {up_stats['avg_same_day_excess'] * 100:.2f}% excess return versus the benchmark, with {(up_stats.get('avg_forward_5d_excess') or 0.0) * 100:.2f}% average 5-day excess afterward."
            )
        if down_stats is not None:
            findings.append(
                f"Yield-down shocks averaged {down_stats['avg_same_day_excess'] * 100:.2f}% excess return versus the benchmark, with {(down_stats.get('avg_forward_10d_excess') or 0.0) * 100:.2f}% average 10-day excess afterward."
            )
        return MacroOverlayResult(
            question_focus=question_focus,
            series_used=[str(analysis.get("series_name", "TREASURY_YIELD"))],
            findings=findings,
            portfolio_sensitivities={
                "yield_up_same_day_excess": float(up_stats["avg_same_day_excess"]) if up_stats else 0.0,
                "yield_up_forward_5d_excess": float(up_stats.get("avg_forward_5d_excess") or 0.0) if up_stats else 0.0,
                "yield_down_forward_10d_excess": float(down_stats.get("avg_forward_10d_excess") or 0.0) if down_stats else 0.0,
                "beta_vs_benchmark": context.baseline_bundle.metrics_map["beta_vs_benchmark"],
            },
            benchmark_sensitivities={
                "yield_up_same_day_return": float(up_stats["avg_same_day_benchmark"]) if up_stats else 0.0,
                "yield_down_same_day_return": float(down_stats["avg_same_day_benchmark"]) if down_stats else 0.0,
            },
            caveats=[
                "This fallback overlay is empirical regime analysis, not a structural duration or factor model."
            ],
        )

    async def _fallback_earnings_overlay(
        self,
        *,
        context: AnalysisRunContext,
        tickers: list[str],
    ) -> EarningsOverlayResult:
        results: list[EarningsOverlayTickerResult] = []
        alpha_vantage = getattr(self.scenario_service, "alpha_vantage", None)
        for ticker in tickers:
            holding = next(
                (item for item in context.baseline_bundle.holdings if item.ticker == ticker),
                None,
            )
            company_name = holding.company_name if holding and holding.company_name else ticker
            if alpha_vantage is None:
                results.append(
                    EarningsOverlayTickerResult(
                        ticker=ticker,
                        company_name=company_name,
                        tone="unavailable",
                        findings=["Transcript retrieval service is unavailable in this runtime."],
                        transcript_available=False,
                    )
                )
                continue
            try:
                if context.start_date or context.end_date:
                    transcript = await alpha_vantage.get_windowed_earnings_transcript(
                        ticker,
                        start_date=context.start_date,
                        end_date=context.end_date,
                    )
                else:
                    transcript = await alpha_vantage.get_latest_earnings_transcript(ticker)
            except Exception:  # noqa: BLE001
                transcript = None
            if not transcript:
                results.append(
                    EarningsOverlayTickerResult(
                        ticker=ticker,
                        company_name=company_name,
                        tone="unavailable",
                        findings=["Transcript not available from Alpha Vantage for the selected window."],
                        transcript_available=False,
                    )
                )
                continue
            raw_items = transcript.get("items", "")
            joined = raw_items if isinstance(raw_items, str) else json.dumps(raw_items)
            findings = self._fallback_extract_findings(
                joined,
                {
                    "Guidance": r"\bguidance\b|\boutlook\b",
                    "Demand": r"\bdemand\b|\border\b|\bvolume\b",
                    "Margins": r"\bmargin\b|\bprofit\b",
                    "Risk": r"\brisk\b|\bheadwind\b|\buncertain\b",
                },
            )
            results.append(
                EarningsOverlayTickerResult(
                    ticker=ticker,
                    company_name=company_name,
                    quarter=transcript.get("quarter"),
                    event_date=transcript.get("event_date"),
                    tone=self._fallback_extract_tone(joined),
                    findings=findings or ["Transcript was retrieved, but no dominant earnings theme stood out in the fallback parser."],
                    nlp_summary=summarize_text_nlp(joined),
                )
            )
        return EarningsOverlayResult(companies=results)

    async def _fallback_filings_overlay(
        self,
        *,
        context: AnalysisRunContext,
        tickers: list[str],
    ) -> FilingsOverlayResult:
        results: list[FilingsOverlayTickerResult] = []
        for ticker in tickers:
            holding = next(
                (item for item in context.baseline_bundle.holdings if item.ticker == ticker),
                None,
            )
            company_name = holding.company_name if holding and holding.company_name else ticker
            if holding is None or not holding.cik:
                results.append(
                    FilingsOverlayTickerResult(
                        ticker=ticker,
                        company_name=company_name,
                        findings=["CIK not available for filing lookup."],
                        filing_available=False,
                    )
                )
                continue
            try:
                if context.start_date or context.end_date:
                    filing = await self.sec_edgar_service.get_recent_filing(
                        holding.cik,
                        start_date=context.start_date,
                        end_date=context.end_date,
                    )
                else:
                    filing = await self.sec_edgar_service.get_recent_filing(holding.cik)
            except Exception:  # noqa: BLE001
                filing = None
            if not filing:
                results.append(
                    FilingsOverlayTickerResult(
                        ticker=ticker,
                        company_name=company_name,
                        findings=["Recent 10-K or 10-Q filing was not found."],
                        filing_available=False,
                    )
                )
                continue
            try:
                filing_text = await self.sec_edgar_service.get_filing_text(
                    cik=holding.cik,
                    accession_number=filing["accession_number"],
                    primary_document=filing["primary_document"],
                )
                findings = self.sec_edgar_service.extract_filing_signals(filing_text)
            except Exception:  # noqa: BLE001
                filing_text = ""
                findings = []
            results.append(
                FilingsOverlayTickerResult(
                    ticker=ticker,
                    company_name=company_name,
                    form_type=filing.get("form_type"),
                    filed_at=filing.get("filed_at"),
                    findings=findings or ["Filing was retrieved, but no dominant risk, liquidity, or debt theme stood out in the fallback parser."],
                    nlp_summary=summarize_text_nlp(filing_text) if filing_text else None,
                )
            )
        return FilingsOverlayResult(companies=results)

    @staticmethod
    def _fallback_extract_tone(text: str) -> str:
        lowered = text.lower()
        positive_hits = sum(term in lowered for term in ("strong", "improve", "upside", "momentum"))
        cautious_hits = sum(term in lowered for term in ("cautious", "pressure", "softness", "uncertain"))
        if positive_hits > cautious_hits:
            return "constructive"
        if cautious_hits > positive_hits:
            return "more cautious"
        return "mixed"

    @staticmethod
    def _fallback_extract_findings(text: str, patterns: dict[str, str]) -> list[str]:
        normalized = re.sub(r"\s+", " ", text)
        sentences = re.split(r"(?<=[.!?])\s+", normalized)
        findings: list[str] = []
        for label, pattern in patterns.items():
            compiled = re.compile(pattern, re.IGNORECASE)
            matches = [sentence[:220] for sentence in sentences if compiled.search(sentence)]
            if matches:
                findings.append(f"{label}: {matches[0]}")
        return findings[:5]

    def _fallback_plan(self, *, payload: PortfolioInput, baseline) -> AnalysisPlan:
        normalized_question = " ".join(payload.question.lower().strip().split())
        relevant_tickers = [
            item
            for item in dict.fromkeys(
                [
                    *(
                        [payload.hypothetical_position.ticker]
                        if payload.hypothetical_position is not None
                        else []
                    ),
                    *[
                        holding.ticker
                        for holding in payload.holdings
                        if holding.ticker.lower() in normalized_question
                    ],
                    *[item.ticker for item in baseline.positions[:4]],
                ]
            )
            if item
        ][:6]
        candidate_search_needed = (
            payload.hypothetical_position is None
            and self._is_open_ended_candidate_search_question(payload.question)
        )
        factor_markers = (
            "cross-sectional",
            "cross sectional",
            "factor",
            "sector peers",
            "metrics correlate",
            "correlate with returns",
            "compare sectors",
        )
        geopolitical_markers = (
            "war",
            "geopolitical",
            "middle east",
            "oil shock",
            "conflict",
            "risk-off",
            "defense-led",
        )
        rates_markers = (
            "rate",
            "yield",
            "fed",
            "10y",
            "10-year",
            "2y",
            "2-year",
            "duration",
            "inflation",
            "macro",
            "growth scare",
        )
        performance_markers = (
            "performance",
            "underperform",
            "lagging",
            "trail",
            "detractor",
            "driver",
            "gains",
            "losses",
            "why did",
            "heavy lifting",
            "carried by",
        )
        concentration_markers = (
            "concentrated",
            "concentration",
            "diversified",
            "diversification",
            "correlated",
            "cluster",
            "move together",
            "sector hole",
            "sector crowding",
            "balanced portfolio",
        )
        if payload.hypothetical_position is not None:
            question_type = QuestionType.what_if_addition
        elif any(self._question_contains_marker(normalized_question, marker) for marker in factor_markers):
            question_type = QuestionType.factor_cross_section
        elif any(self._question_contains_marker(normalized_question, marker) for marker in geopolitical_markers):
            question_type = QuestionType.geopolitical_war
        elif any(self._question_contains_marker(normalized_question, marker) for marker in rates_markers):
            question_type = QuestionType.rates_macro
        elif any(self._question_contains_marker(normalized_question, marker) for marker in performance_markers):
            question_type = QuestionType.performance_drivers
        elif candidate_search_needed or any(
            self._question_contains_marker(normalized_question, marker)
            for marker in concentration_markers
        ):
            question_type = QuestionType.concentration_diversification
        else:
            question_type = QuestionType.general_health

        optimization_preferences = (
            self._candidate_search_optimization_preferences(payload.question)
            if candidate_search_needed
            else []
        )

        if candidate_search_needed:
            objective = self._candidate_search_objective(
                payload.question,
                optimization_preferences=optimization_preferences,
            )
            dynamic_workflow = self._candidate_search_workflow(
                objective,
                optimization_preferences=optimization_preferences,
            )
            scenario_needed = False
            macro_overlay_needed = False
            investigation_steps = self._candidate_search_steps(
                objective,
                optimization_preferences=optimization_preferences,
            )
            caveats = self._candidate_search_caveats(
                objective,
                optimization_preferences=optimization_preferences,
            )
        else:
            objective = {
                QuestionType.general_health: "performance",
                QuestionType.concentration_diversification: "diversify",
                QuestionType.performance_drivers: "performance",
                QuestionType.rates_macro: "reduce_macro_sensitivity",
                QuestionType.geopolitical_war: "reduce_macro_sensitivity",
                QuestionType.what_if_addition: "what_if_addition",
                QuestionType.factor_cross_section: "performance",
            }[question_type]
            dynamic_workflow = question_type.value
            scenario_needed = payload.hypothetical_position is not None
            macro_overlay_needed = question_type in {QuestionType.rates_macro, QuestionType.geopolitical_war}
            investigation_steps = self._fallback_investigation_steps(question_type)
            caveats = self._fallback_plan_caveats(question_type)

        earnings_overlay_needed = any(
            marker in normalized_question
            for marker in ("earnings", "transcript", "guidance", "margins", "margin", "demand", "call")
        )
        filings_overlay_needed = any(
            marker in normalized_question
            for marker in ("filing", "10-k", "10-q", "liquidity", "debt", "balance sheet", "regulatory")
        )
        macro_themes = self._fallback_macro_themes(
            normalized_question=normalized_question,
            question_type=question_type,
        )
        preferred_data_sources = self._fallback_preferred_data_sources(
            question_type=question_type,
            macro_themes=macro_themes,
            earnings_overlay_needed=earnings_overlay_needed,
            filings_overlay_needed=filings_overlay_needed,
        )
        dataset_selection_rationale = [
            "Baseline price history is required to compute portfolio return, volatility, beta, and concentration metrics."
        ]
        if macro_overlay_needed:
            dataset_selection_rationale.append(
                "Macro series were selected because the question asks about rate, inflation, or geopolitical sensitivity."
            )
        if earnings_overlay_needed:
            dataset_selection_rationale.append(
                "Earnings transcripts were selected because the question asks about demand, margins, or management tone."
            )
        if filings_overlay_needed:
            dataset_selection_rationale.append(
                "SEC filings were selected because the question asks about balance-sheet, debt, or disclosure risk."
            )
        if question_type in {
            QuestionType.performance_drivers,
            QuestionType.factor_cross_section,
            QuestionType.what_if_addition,
        }:
            dataset_selection_rationale.append(
                "Local factor-return data were selected so the analysis can estimate market, style, quality, investment, and momentum exposures."
            )
        comparison_universe = "sector_peers" if question_type == QuestionType.factor_cross_section else "portfolio_only"
        comparison_sector_filters = (
            [baseline.sector_exposures[0].sector]
            if question_type == QuestionType.factor_cross_section and baseline.sector_exposures
            else []
        )
        comparison_ticker_limit = 25 if question_type == QuestionType.factor_cross_section else None
        return AnalysisPlan(
            question_type=question_type,
            objective=objective,
            explanation=(
                "Deterministic fallback planner selected this workflow because the agent runtime was unavailable."
            ),
            dynamic_workflow=dynamic_workflow,
            scenario_needed=scenario_needed,
            candidate_search_needed=candidate_search_needed,
            macro_overlay_needed=macro_overlay_needed,
            earnings_overlay_needed=earnings_overlay_needed,
            filings_overlay_needed=filings_overlay_needed,
            relevant_tickers=relevant_tickers,
            macro_themes=macro_themes,
            preferred_data_sources=preferred_data_sources,
            dataset_selection_rationale=dataset_selection_rationale,
            optimization_preferences=optimization_preferences,
            comparison_universe=comparison_universe,
            comparison_sector_filters=comparison_sector_filters,
            comparison_ticker_limit=comparison_ticker_limit,
            investigation_steps=investigation_steps,
            caveats=[
                *caveats,
                "Question routing used deterministic fallback logic because the LLM planner was unavailable.",
            ],
        )

    @staticmethod
    def _fallback_macro_themes(*, normalized_question: str, question_type: QuestionType) -> list[str]:
        themes: list[str] = []
        if question_type in {QuestionType.rates_macro, QuestionType.geopolitical_war}:
            themes.append("rates")
        if "inflation" in normalized_question:
            themes.append("inflation")
        if any(token in normalized_question for token in ("oil", "energy", "war", "geopolitical", "gas", "commodity")):
            themes.extend(["oil", "energy"])
        return list(dict.fromkeys(themes))

    @staticmethod
    def _question_contains_marker(question: str, marker: str) -> bool:
        if " " in marker or "-" in marker or any(char.isdigit() for char in marker):
            return marker in question
        return re.search(rf"\b{re.escape(marker)}\b", question) is not None

    @staticmethod
    def _fallback_preferred_data_sources(
        *,
        question_type: QuestionType,
        macro_themes: list[str],
        earnings_overlay_needed: bool,
        filings_overlay_needed: bool,
    ) -> list[str]:
        sources: list[str] = []
        if question_type == QuestionType.rates_macro:
            sources.extend(["TREASURY_YIELD_10Y", "TREASURY_YIELD_2Y", "FEDERAL_FUNDS_RATE"])
        if question_type == QuestionType.geopolitical_war or "oil" in macro_themes or "energy" in macro_themes:
            sources.extend(["WTI", "BRENT", "NATURAL_GAS", "EIA_PETROLEUM_STATUS", "EIA_NATGAS_STORAGE"])
        if question_type in {
            QuestionType.performance_drivers,
            QuestionType.factor_cross_section,
            QuestionType.what_if_addition,
        }:
            sources.extend(["KEN_FRENCH_FF5_DAILY", "KEN_FRENCH_MOMENTUM_DAILY"])
        if earnings_overlay_needed:
            sources.append("EARNINGS_TRANSCRIPTS")
        if filings_overlay_needed:
            sources.append("SEC_FILINGS")
        return list(dict.fromkeys(sources))

    @staticmethod
    def _fallback_investigation_steps(question_type: QuestionType) -> list[str]:
        mapping = {
            QuestionType.general_health: [
                "Check concentration, volatility, beta, and sector skew first.",
                "Review the biggest contributors and detractors for any hidden fragility.",
            ],
            QuestionType.concentration_diversification: [
                "Measure top-name concentration, Herfindahl concentration, and sector crowding.",
                "Review the most correlated pair and overall average pairwise correlation.",
            ],
            QuestionType.performance_drivers: [
                "Break out the largest positive and negative contributors.",
                "Compare portfolio return, beta, and drawdown profile versus the benchmark.",
            ],
            QuestionType.rates_macro: [
                "Run the rates regime analysis to inspect shock-day and forward excess returns.",
                "Check whether beta and sector mix explain the observed macro sensitivity.",
            ],
            QuestionType.geopolitical_war: [
                "Run the geopolitical commodity-shock workflow across oil and gas proxies.",
                "Review the holdings that drive downside during proxy stress days.",
            ],
            QuestionType.what_if_addition: [
                "Compare before and after portfolio metrics for the hypothetical addition.",
                "Check whether the addition improves concentration, beta, volatility, or Sharpe.",
            ],
            QuestionType.factor_cross_section: [
                "Build the stock-level dataset first, then compare sectors and financial metrics versus realized returns.",
                "Check quantile monotonicity and regression diagnostics before drawing conclusions.",
            ],
        }
        return mapping[question_type]

    @staticmethod
    def _fallback_plan_caveats(question_type: QuestionType) -> list[str]:
        mapping = {
            QuestionType.general_health: ["Health-check conclusions are descriptive and grounded in the current aligned sample window."],
            QuestionType.concentration_diversification: ["Diversification conclusions are empirical and may change if the aligned window shifts materially."],
            QuestionType.performance_drivers: ["Performance attribution is historical and does not prove causality."],
            QuestionType.rates_macro: ["Macro sensitivity is based on empirical shock regimes rather than a structural factor model."],
            QuestionType.geopolitical_war: ["Geopolitical sensitivity uses commodity and risk-off proxies rather than a direct geopolitical factor series."],
            QuestionType.what_if_addition: ["What-if analysis assumes a simple 5% target-weight style addition unless a specific hypothetical is supplied."],
            QuestionType.factor_cross_section: ["Cross-sectional relationships are descriptive and can be noisy in smaller universes."],
        }
        return mapping[question_type]

    def _fallback_research_agenda(
        self,
        *,
        plan: AnalysisPlan,
        dynamic_eda: DynamicEDAResult,
        baseline_bundle,
    ) -> ResearchAgenda:
        focus_areas = [
            *[finding.headline for finding in dynamic_eda.findings[:2]],
            *plan.investigation_steps[:2],
        ]
        follow_up_questions = {
            QuestionType.general_health: [
                "Which baseline weakness most deserves closer monitoring over the current sample window?",
                "Are the biggest contributors masking fragility elsewhere in the portfolio?",
            ],
            QuestionType.concentration_diversification: [
                "Which concentration or correlation hotspot matters most for the requested objective?",
                "Does external narrative evidence reinforce or challenge the diversification story?",
            ],
            QuestionType.performance_drivers: [
                "Which holdings explain most of the portfolio's relative return versus the benchmark?",
                "Do external narratives support the observed contributor and detractor pattern?",
            ],
            QuestionType.rates_macro: [
                "Do macro and narrative signals reinforce the observed beta and rates sensitivity?",
                "Which holdings or sectors are most exposed if the same macro regime repeats?",
            ],
            QuestionType.geopolitical_war: [
                "Do commodity-shock proxies align with the portfolio's current sector exposures?",
                "Which holdings become the main transmission channel in a renewed stress regime?",
            ],
            QuestionType.what_if_addition: [
                "Which scenario deltas matter most for the user's stated decision rule?",
                "Does external evidence raise narrative or balance-sheet caveats around the hypothetical addition?",
            ],
            QuestionType.factor_cross_section: [
                "Which metric relationships look strongest after accounting for monotonicity and regression strength?",
                "Do sector leadership and factor diagnostics point to the same underlying signal?",
            ],
        }[plan.question_type]
        overlay_requests = []
        if plan.macro_overlay_needed:
            overlay_requests.append("Cross-check the main quantitative thesis against macro shock or regime evidence.")
        if plan.earnings_overlay_needed:
            overlay_requests.append("Inspect whether guidance, margins, and demand commentary reinforce the core EDA interpretation.")
        if plan.filings_overlay_needed:
            overlay_requests.append("Inspect whether liquidity, leverage, or regulatory disclosures change the risk interpretation.")
        candidate_search_guidance = []
        if plan.candidate_search_needed:
            candidate_search_guidance.extend(
                [
                    f"Use the optimization target `{item.metric}` with direction `{item.direction}`."
                    + (" Treat it as a hard constraint." if item.hard_constraint else "")
                    for item in plan.optimization_preferences
                ]
                or [
                    "Translate the portfolio's main weakness into explicit shortlist and ranking criteria."
                ]
            )
        memo_watchouts = [
            *plan.caveats[:2],
            "Treat narrative overlays as context that must be tied back to observed portfolio metrics.",
        ]
        return ResearchAgenda(
            focus_areas=list(dict.fromkeys(item for item in focus_areas if item))[:5],
            analysis_ideas=list(dict.fromkeys(plan.investigation_steps + plan.dataset_selection_rationale))[:6],
            follow_up_questions=follow_up_questions[:4],
            overlay_requests=overlay_requests[:4],
            candidate_search_guidance=candidate_search_guidance[:4],
            memo_watchouts=list(dict.fromkeys(item for item in memo_watchouts if item))[:4],
        )

    def _fallback_research_synthesis(
        self,
        *,
        dynamic_eda: DynamicEDAResult,
        overlays: OverlayBundle,
    ) -> ResearchSynthesis:
        integrated_insights = [finding.headline for finding in dynamic_eda.findings[:3]]
        confirmations: list[str] = []
        tensions: list[str] = []
        eda_implications: list[str] = []
        candidate_search_implications: list[str] = []
        memo_implications: list[str] = []

        if overlays.macro is not None and overlays.macro.findings:
            integrated_insights.append(f"Macro overlay: {overlays.macro.findings[0]}")
            confirmations.append("Macro overlay should be interpreted alongside beta, volatility, and sector exposure.")
        if overlays.earnings is not None and overlays.earnings.companies:
            item = overlays.earnings.companies[0]
            integrated_insights.append(f"Earnings overlay ({item.ticker}): {item.findings[0]}")
            confirmations.append(f"Earnings tone for {item.ticker} should be checked against contributor and sector metrics.")
        if overlays.filings is not None and overlays.filings.companies:
            item = overlays.filings.companies[0]
            integrated_insights.append(f"Filings overlay ({item.ticker}): {item.findings[0]}")
            tensions.append(f"Filing disclosures for {item.ticker} may qualify otherwise strong return or diversification conclusions.")
        if dynamic_eda.news_intel is not None and dynamic_eda.news_intel.dominant_topics:
            integrated_insights.append(
                "News topics concentrated in "
                + ", ".join(dynamic_eda.news_intel.dominant_topics[:3])
                + "."
            )
            eda_implications.append(
                "Tie external narrative themes back to measured beta, concentration, correlation, or contributor data."
            )
        if dynamic_eda.candidate_search is not None and dynamic_eda.candidate_search.candidates:
            top_candidate = dynamic_eda.candidate_search.candidates[0]
            candidate_search_implications.append(
                f"Top candidate {top_candidate.ticker} should be defended using marginal metric deltas rather than narrative preference."
            )
        if dynamic_eda.scenario_analysis is not None:
            eda_implications.append(
                "Use the before/after scenario deltas to decide whether the hypothetical meaningfully changes the base thesis."
            )

        memo_implications.extend(integrated_insights[:2])
        memo_implications.extend(tensions[:1])
        return ResearchSynthesis(
            integrated_insights=list(dict.fromkeys(item for item in integrated_insights if item))[:6],
            confirmations=list(dict.fromkeys(item for item in confirmations if item))[:4],
            tensions=list(dict.fromkeys(item for item in tensions if item))[:4],
            eda_implications=list(dict.fromkeys(item for item in eda_implications if item))[:4],
            candidate_search_implications=list(
                dict.fromkeys(item for item in candidate_search_implications if item)
            )[:4],
            memo_implications=list(dict.fromkeys(item for item in memo_implications if item))[:4],
        )

    @staticmethod
    def _dynamic_eda_agent_summary(dynamic_eda: DynamicEDAResult) -> str:
        top_candidate = None
        if dynamic_eda.candidate_search is not None and dynamic_eda.candidate_search.candidates:
            item = dynamic_eda.candidate_search.candidates[0]
            top_candidate = {
                "ticker": item.ticker,
                "score": round(float(item.score), 4),
                "objective": dynamic_eda.candidate_search.objective,
            }
        scenario_summary = None
        if dynamic_eda.scenario_analysis is not None:
            scenario_summary = {
                "label": dynamic_eda.scenario_analysis.label,
                "top_deltas": [
                    delta.model_dump()
                    for delta in dynamic_eda.scenario_analysis.deltas[:4]
                ],
            }
        payload = {
            "workflow": dynamic_eda.workflow,
            "question_type": dynamic_eda.question_type.value,
            "top_findings": [
                {"headline": item.headline, "evidence": item.evidence[:2]}
                for item in dynamic_eda.findings[:4]
            ],
            "table_names": [table.name for table in dynamic_eda.tables[:8]],
            "data_sources": [item.series for item in dynamic_eda.data_sources[:8]],
            "news_topics": dynamic_eda.news_intel.dominant_topics[:4] if dynamic_eda.news_intel else [],
            "top_candidate": top_candidate,
            "scenario": scenario_summary,
        }
        return json.dumps(payload, ensure_ascii=True)

    @staticmethod
    def _overlay_agent_summary(overlays: OverlayBundle) -> str:
        payload = {
            "macro": overlays.macro.findings[:3] if overlays.macro else [],
            "earnings": [
                {
                    "ticker": item.ticker,
                    "tone": item.tone,
                    "findings": item.findings[:2],
                }
                for item in (overlays.earnings.companies[:3] if overlays.earnings else [])
            ],
            "filings": [
                {
                    "ticker": item.ticker,
                    "form_type": item.form_type,
                    "findings": item.findings[:2],
                }
                for item in (overlays.filings.companies[:3] if overlays.filings else [])
            ],
        }
        return json.dumps(payload, ensure_ascii=True)

    @staticmethod
    def _research_agenda_summary(agenda: ResearchAgenda) -> str:
        return json.dumps(
            {
                "focus_areas": agenda.focus_areas[:4],
                "analysis_ideas": agenda.analysis_ideas[:4],
                "follow_up_questions": agenda.follow_up_questions[:4],
                "overlay_requests": agenda.overlay_requests[:4],
                "candidate_search_guidance": agenda.candidate_search_guidance[:4],
                "memo_watchouts": agenda.memo_watchouts[:4],
            },
            ensure_ascii=True,
        )

    @staticmethod
    def _research_synthesis_summary(synthesis: ResearchSynthesis) -> str:
        return json.dumps(
            {
                "integrated_insights": synthesis.integrated_insights[:4],
                "confirmations": synthesis.confirmations[:3],
                "tensions": synthesis.tensions[:3],
                "eda_implications": synthesis.eda_implications[:3],
                "candidate_search_implications": synthesis.candidate_search_implications[:3],
                "memo_implications": synthesis.memo_implications[:3],
            },
            ensure_ascii=True,
        )

    def _merge_deep_research_result(
        self,
        *,
        base: DynamicEDAResult,
        refined: DynamicEDAResult,
    ) -> DynamicEDAResult:
        return refined.model_copy(
            update={
                "workflow": refined.workflow or base.workflow,
                "question_type": refined.question_type or base.question_type,
                "findings": self._dedupe_findings([*refined.findings, *base.findings]),
                "tables": self._merge_analysis_tables(base.tables, refined.tables),
                "data_sources": self._merge_data_sources(base.data_sources, refined.data_sources),
                "news_intel": refined.news_intel or base.news_intel,
                "scenario_analysis": refined.scenario_analysis or base.scenario_analysis,
                "candidate_search": refined.candidate_search or base.candidate_search,
            }
        )

    def _ensure_agent_collaboration_in_dynamic_eda(
        self,
        *,
        dynamic_eda: DynamicEDAResult,
        collaboration: AgentCollaboration,
    ) -> DynamicEDAResult:
        findings = list(dynamic_eda.findings)
        tables = [
            table
            for table in dynamic_eda.tables
            if table.name not in {"Research Agenda", "Cross-Agent Synthesis"}
        ]
        agenda = collaboration.research_agenda
        synthesis = collaboration.research_synthesis
        if agenda is not None and (agenda.analysis_ideas or agenda.follow_up_questions):
            findings = self._append_finding_unique(
                findings,
                EDAFinding(
                    headline="Second-pass research agenda prioritized the next analysis steps.",
                    evidence=[
                        *agenda.focus_areas[:2],
                        *agenda.analysis_ideas[:2],
                        *agenda.follow_up_questions[:2],
                    ],
                    metrics={
                        "focus_area_count": float(len(agenda.focus_areas)),
                        "follow_up_question_count": float(len(agenda.follow_up_questions)),
                    },
                ),
            )
            agenda_rows = [
                {"category": "focus_area", "detail": item}
                for item in agenda.focus_areas[:4]
            ] + [
                {"category": "analysis_idea", "detail": item}
                for item in agenda.analysis_ideas[:4]
            ] + [
                {"category": "follow_up_question", "detail": item}
                for item in agenda.follow_up_questions[:4]
            ] + [
                {"category": "overlay_request", "detail": item}
                for item in agenda.overlay_requests[:3]
            ] + [
                {"category": "candidate_guidance", "detail": item}
                for item in agenda.candidate_search_guidance[:3]
            ]
            if agenda_rows:
                tables.append(
                    AnalysisTable(
                        name="Research Agenda",
                        columns=["category", "detail"],
                        rows=agenda_rows,
                    )
                )
        if synthesis is not None and (
            synthesis.integrated_insights
            or synthesis.confirmations
            or synthesis.tensions
            or synthesis.eda_implications
        ):
            findings = self._append_finding_unique(
                findings,
                EDAFinding(
                    headline="Cross-agent synthesis connected overlay and narrative evidence back to the quantitative EDA.",
                    evidence=[
                        *synthesis.integrated_insights[:2],
                        *synthesis.confirmations[:2],
                        *synthesis.tensions[:2],
                    ],
                    metrics={
                        "integrated_insight_count": float(len(synthesis.integrated_insights)),
                        "tension_count": float(len(synthesis.tensions)),
                    },
                    severity="warning" if synthesis.tensions else "info",
                ),
            )
            synthesis_rows = [
                {"category": "integrated_insight", "detail": item}
                for item in synthesis.integrated_insights[:4]
            ] + [
                {"category": "confirmation", "detail": item}
                for item in synthesis.confirmations[:3]
            ] + [
                {"category": "tension", "detail": item}
                for item in synthesis.tensions[:3]
            ] + [
                {"category": "eda_implication", "detail": item}
                for item in synthesis.eda_implications[:3]
            ] + [
                {"category": "candidate_implication", "detail": item}
                for item in synthesis.candidate_search_implications[:3]
            ]
            if synthesis_rows:
                tables.append(
                    AnalysisTable(
                        name="Cross-Agent Synthesis",
                        columns=["category", "detail"],
                        rows=synthesis_rows,
                    )
                )
        return dynamic_eda.model_copy(
            update={
                "findings": findings,
                "tables": tables,
            }
        )

    @staticmethod
    def _append_finding_unique(findings: list[EDAFinding], finding: EDAFinding) -> list[EDAFinding]:
        if any(item.headline == finding.headline for item in findings):
            return findings
        return [*findings, finding]

    @staticmethod
    def _dedupe_findings(findings: list[EDAFinding]) -> list[EDAFinding]:
        deduped: list[EDAFinding] = []
        seen: set[tuple[str, tuple[str, ...]]] = set()
        for finding in findings:
            key = (finding.headline, tuple(finding.evidence))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(finding)
        return deduped

    @staticmethod
    def _merge_analysis_tables(base: list[AnalysisTable], refined: list[AnalysisTable]) -> list[AnalysisTable]:
        merged: dict[str, AnalysisTable] = {table.name: table for table in base}
        order = [table.name for table in base]
        for table in refined:
            if table.name not in merged:
                order.append(table.name)
            merged[table.name] = table
        return [merged[name] for name in order if name in merged]

    @staticmethod
    def _merge_data_sources(base: list, refined: list) -> list:
        deduped = []
        seen: set[tuple[str, str]] = set()
        for item in [*refined, *base]:
            key = (item.source, item.series)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped

    def _fallback_memo(
        self,
        *,
        payload: PortfolioInput,
        plan: AnalysisPlan,
        baseline_bundle,
        dynamic_eda,
        overlays: OverlayBundle,
        agent_collaboration: AgentCollaboration,
        warnings: list[AnalysisWarning],
    ) -> FinalMemo:
        metrics = baseline_bundle.metrics_map
        top_sector = baseline_bundle.baseline.sector_exposures[0] if baseline_bundle.baseline.sector_exposures else None
        top_contributor = baseline_bundle.baseline.best_performers[0] if baseline_bundle.baseline.best_performers else None
        top_detractor = baseline_bundle.baseline.worst_performers[0] if baseline_bundle.baseline.worst_performers else None
        executive_summary: list[str] = []
        evidence: list[str] = []

        if plan.question_type == QuestionType.performance_drivers and top_contributor and top_detractor:
            executive_summary.append(
                f"Performance was led by {top_contributor.ticker} at {self._format_pct(top_contributor.return_pct)} and hurt most by {top_detractor.ticker} at {self._format_pct(top_detractor.return_pct)}."
            )
        elif plan.question_type == QuestionType.what_if_addition and dynamic_eda.scenario_analysis is not None:
            deltas = {
                item.metric: item.delta for item in dynamic_eda.scenario_analysis.deltas
            }
            executive_summary.append(
                f"The hypothetical addition changes beta by {self._format_number(deltas.get('beta_vs_benchmark'))} and Sharpe by {self._format_number(deltas.get('sharpe_ratio'))}."
            )
        elif dynamic_eda.candidate_search is not None and dynamic_eda.candidate_search.candidates:
            top_candidate = dynamic_eda.candidate_search.candidates[0]
            executive_summary.append(
                f"Top screened addition was {top_candidate.ticker} with score {top_candidate.score:.2f} under the {dynamic_eda.candidate_search.objective} objective."
            )

        executive_summary.append(
            f"Top 3 holdings are {self._format_pct(metrics.get('top3_share'))} of capital, annualized volatility is {self._format_pct(metrics.get('annualized_volatility'))}, and beta versus {payload.benchmark} is {self._format_number(metrics.get('beta_vs_benchmark'))}."
        )
        if top_sector is not None:
            executive_summary.append(
                f"Largest sector exposure is {top_sector.sector} at {self._format_pct(top_sector.weight)}, with average pairwise correlation of {self._format_number(metrics.get('average_pairwise_correlation'))}."
            )
        if overlays.macro is not None:
            executive_summary = self._append_unique(executive_summary, overlays.macro.findings[:2])
        if dynamic_eda.findings:
            executive_summary = self._append_unique(
                executive_summary,
                [dynamic_eda.findings[0].headline],
            )

        for finding in dynamic_eda.findings[:3]:
            evidence = self._append_unique(evidence, finding.evidence[:2])
        if dynamic_eda.candidate_search is not None and dynamic_eda.candidate_search.candidates:
            top_candidate = dynamic_eda.candidate_search.candidates[0]
            evidence.append(
                f"{top_candidate.ticker} ranked first with score {top_candidate.score:.2f}; {top_candidate.rationale[0]}"
            )
        if dynamic_eda.scenario_analysis is not None:
            scenario_deltas = {
                item.metric: item.delta for item in dynamic_eda.scenario_analysis.deltas
            }
            evidence.append(
                f"Scenario deltas show Herfindahl moved by {self._format_number(scenario_deltas.get('herfindahl_index'))}, volatility moved by {self._format_pct(scenario_deltas.get('annualized_volatility'))}, and Sharpe moved by {self._format_number(scenario_deltas.get('sharpe_ratio'))}."
            )
        if top_contributor is not None and top_detractor is not None:
            evidence.append(
                f"Best performer was {top_contributor.ticker} with contribution {self._format_pct(top_contributor.contribution_pct)}, while worst performer was {top_detractor.ticker} with contribution {self._format_pct(top_detractor.contribution_pct)}."
            )
        if overlays.earnings is not None and overlays.earnings.companies:
            item = overlays.earnings.companies[0]
            evidence.append(
                f"Earnings overlay for {item.ticker} was {item.tone}; {item.findings[0]}"
            )
        if overlays.filings is not None and overlays.filings.companies:
            item = overlays.filings.companies[0]
            evidence.append(
                f"Filings overlay for {item.ticker} noted: {item.findings[0]}"
            )
        if agent_collaboration.research_synthesis is not None and agent_collaboration.research_synthesis.memo_implications:
            evidence = self._append_unique(
                evidence,
                agent_collaboration.research_synthesis.memo_implications[:2],
            )
        if not evidence:
            evidence.append(
                f"Portfolio return versus benchmark was {self._format_pct(metrics.get('return_vs_benchmark'))} and max drawdown was {self._format_pct(metrics.get('max_drawdown'))} over the aligned sample."
            )

        risks_and_caveats = [warning.message for warning in warnings[:3]] or [
            "This memo was synthesized deterministically because the writer and/or critic agent was unavailable.",
        ]
        next_steps = [
            "Review the highest-conviction baseline and dynamic EDA tables before making position changes.",
        ]
        if dynamic_eda.candidate_search is not None and dynamic_eda.candidate_search.candidates:
            next_steps.append(
                f"Pressure-test the top candidate recommendation ({dynamic_eda.candidate_search.candidates[0].ticker}) against your portfolio constraints before adding exposure."
            )
        elif dynamic_eda.scenario_analysis is not None:
            next_steps.append(
                f"Compare the before/after metrics for {dynamic_eda.scenario_analysis.label} against your required return and risk thresholds."
            )
        if agent_collaboration.research_agenda is not None and agent_collaboration.research_agenda.follow_up_questions:
            next_steps.append(
                f"Resolve the highest-priority follow-up question: {agent_collaboration.research_agenda.follow_up_questions[0]}"
            )
        thesis = executive_summary[0]
        title = {
            QuestionType.general_health: "Portfolio Health Check",
            QuestionType.concentration_diversification: "Diversification Review",
            QuestionType.performance_drivers: "Performance Driver Review",
            QuestionType.rates_macro: "Rates Sensitivity Review",
            QuestionType.geopolitical_war: "Geopolitical Stress Review",
            QuestionType.what_if_addition: "What-If Portfolio Review",
            QuestionType.factor_cross_section: "Cross-Sectional Factor Review",
        }[plan.question_type]
        return FinalMemo(
            title=title,
            thesis=thesis,
            executive_summary=executive_summary[:4],
            evidence=evidence[:6],
            risks_and_caveats=risks_and_caveats[:4],
            next_steps=next_steps[:3],
        )

    def _fallback_critic(
        self,
        *,
        memo: FinalMemo,
        warnings: list[AnalysisWarning],
    ) -> CriticResult:
        approved_claims = [
            item
            for item in [memo.thesis, *memo.executive_summary, *memo.evidence]
            if any(char.isdigit() for char in item)
        ][:4]
        flagged_claims = []
        if warnings:
            flagged_claims.append(
                "Part of this run used deterministic fallback logic, so qualitative language should stay conservative and evidence-linked."
            )
        revised_memo = memo.model_copy(
            update={
                "risks_and_caveats": self._append_unique(
                    memo.risks_and_caveats,
                    flagged_claims,
                )
            }
        )
        return CriticResult(
            approved_claims=approved_claims or [memo.thesis],
            flagged_claims=flagged_claims,
            revised_memo=revised_memo,
        )

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
        top_sector = sectors.rows[0] if sectors and sectors.rows else None
        bottom_sector = sectors.rows[-1] if sectors and sectors.rows else None
        top_metric_relationship = correlations.rows[0] if correlations and correlations.rows else None
        strongest_metric_relationships = correlations.rows[:3] if correlations and correlations.rows else []
        top_rank_ic = rank_ic.rows[0] if rank_ic and rank_ic.rows else None
        top_regression = regressions.rows[0] if regressions and regressions.rows else None
        bucket_rows = buckets.rows if buckets and buckets.rows else []
        monotonic_count = sum(1 for row in bucket_rows if row.get("monotonic"))
        monotonic_assessment = PortfolioAnalysisOrchestrator._quantile_monotonicity_assessment(
            monotonic_count=monotonic_count,
            total=len(bucket_rows),
        )
        rank_ic_assessment = PortfolioAnalysisOrchestrator._rank_ic_assessment(top_rank_ic)
        regression_assessment = PortfolioAnalysisOrchestrator._regression_assessment(top_regression)
        overall_signal_assessment = PortfolioAnalysisOrchestrator._overall_factor_signal_assessment(
            rank_ic_assessment=rank_ic_assessment,
            regression_assessment=regression_assessment,
        )
        summary_lines = PortfolioAnalysisOrchestrator._factor_summary_lines(
            top_sector=top_sector,
            bottom_sector=bottom_sector,
            top_metric_relationship=top_metric_relationship,
            top_rank_ic=top_rank_ic,
            top_regression=top_regression,
            bucket_rows=bucket_rows,
            monotonic_count=monotonic_count,
            monotonic_assessment=monotonic_assessment,
            overall_signal_assessment=overall_signal_assessment,
        )
        return {
            "top_sector": top_sector,
            "bottom_sector": bottom_sector,
            "top_metric_relationship": top_metric_relationship,
            "strongest_metric_relationships": strongest_metric_relationships,
            "top_rank_ic": top_rank_ic,
            "bucket_monotonicity": bucket_rows[:5],
            "top_regression": top_regression,
            "sector_leadership": {
                "leading_sector": top_sector,
                "lagging_sector": bottom_sector,
            },
            "metric_relationships": {
                "top_correlation": top_metric_relationship,
                "top_rank_ic": top_rank_ic,
                "top_regression": top_regression,
            },
            "quantile_monotonicity": {
                "assessment": monotonic_assessment,
                "monotonic_metric_count": monotonic_count,
                "screened_metric_count": len(bucket_rows),
                "top_bucket": bucket_rows[0] if bucket_rows else None,
            },
            "signal_strength": {
                "rank_ic_assessment": rank_ic_assessment,
                "regression_assessment": regression_assessment,
                "overall_assessment": overall_signal_assessment,
            },
            "summary_lines": summary_lines,
        }

    @staticmethod
    def _ensure_factor_cross_section_summary_in_memo(
        *,
        memo: FinalMemo,
        factor_summary: dict[str, object] | None,
    ) -> FinalMemo:
        if not factor_summary:
            return memo
        summary_lines = [
            str(line)
            for line in factor_summary.get("summary_lines", [])
            if isinstance(line, str) and line.strip()
        ]
        if not summary_lines:
            return memo
        executive_summary = PortfolioAnalysisOrchestrator._append_unique(
            memo.executive_summary,
            summary_lines[:2],
        )
        evidence = PortfolioAnalysisOrchestrator._append_unique(
            memo.evidence,
            summary_lines,
        )
        return memo.model_copy(
            update={
                "executive_summary": executive_summary,
                "evidence": evidence,
            }
        )

    @staticmethod
    def _append_unique(existing: list[str], additions: list[str]) -> list[str]:
        combined = list(existing)
        seen = {item.strip() for item in combined if item.strip()}
        for item in additions:
            if not item.strip() or item.strip() in seen:
                continue
            combined.append(item)
            seen.add(item.strip())
        return combined

    @staticmethod
    def _factor_summary_lines(
        *,
        top_sector: dict[str, object] | None,
        bottom_sector: dict[str, object] | None,
        top_metric_relationship: dict[str, object] | None,
        top_rank_ic: dict[str, object] | None,
        top_regression: dict[str, object] | None,
        bucket_rows: list[dict[str, object]],
        monotonic_count: int,
        monotonic_assessment: str,
        overall_signal_assessment: str,
    ) -> list[str]:
        lines: list[str] = []
        if top_sector or bottom_sector:
            leader = (
                f"{top_sector['sector']} led at {PortfolioAnalysisOrchestrator._format_pct(top_sector.get('avg_trailing_return'))}"
                if top_sector and top_sector.get("sector")
                else "no leading sector was identified"
            )
            laggard = (
                f"{bottom_sector['sector']} lagged at {PortfolioAnalysisOrchestrator._format_pct(bottom_sector.get('avg_trailing_return'))}"
                if bottom_sector and bottom_sector.get("sector")
                else "no lagging sector was identified"
            )
            lines.append(f"Cross-sectional sector leadership: {leader}, while {laggard}.")
        if top_metric_relationship:
            lines.append(
                "Strongest metric-return relationship was "
                f"{top_metric_relationship.get('metric')} versus {top_metric_relationship.get('target')} "
                f"(correlation {PortfolioAnalysisOrchestrator._format_number(top_metric_relationship.get('correlation'))}, "
                f"sector-neutral {PortfolioAnalysisOrchestrator._format_number(top_metric_relationship.get('sector_neutral_correlation'))})."
            )
        top_bucket = bucket_rows[0] if bucket_rows else None
        if top_bucket:
            lines.append(
                f"Quantile monotonicity looked {monotonic_assessment}: "
                f"{monotonic_count} of {len(bucket_rows)} screened metrics were monotonic, "
                f"led by {top_bucket.get('metric')} with a Q4-Q1 spread of "
                f"{PortfolioAnalysisOrchestrator._format_pct(top_bucket.get('spread_q4_q1'))}."
            )
        if top_rank_ic or top_regression:
            lines.append(
                "Regression / rank-IC evidence looked "
                f"{overall_signal_assessment}: top Spearman was {PortfolioAnalysisOrchestrator._format_number((top_rank_ic or {}).get('spearman_correlation'))}, "
                f"sector-neutral Spearman was {PortfolioAnalysisOrchestrator._format_number((top_rank_ic or {}).get('sector_neutral_spearman'))}, "
                f"and top regression R-squared was {PortfolioAnalysisOrchestrator._format_number((top_regression or {}).get('r_squared'))}."
            )
        return lines

    @staticmethod
    def _quantile_monotonicity_assessment(*, monotonic_count: int, total: int) -> str:
        if total == 0:
            return "noisy"
        ratio = monotonic_count / total
        if ratio >= 0.6:
            return "monotonic"
        if ratio >= 0.25:
            return "mixed"
        return "noisy"

    @staticmethod
    def _rank_ic_assessment(row: dict[str, object] | None) -> str:
        if not row:
            return "weak"
        top_rank = abs(float(row.get("spearman_correlation") or 0.0))
        sector_neutral = abs(float(row.get("sector_neutral_spearman") or 0.0))
        if top_rank >= 0.3 and sector_neutral >= 0.15:
            return "meaningful"
        if top_rank >= 0.15 or sector_neutral >= 0.05:
            return "mixed"
        return "weak"

    @staticmethod
    def _regression_assessment(row: dict[str, object] | None) -> str:
        if not row:
            return "weak"
        r_squared = float(row.get("r_squared") or 0.0)
        if r_squared >= 0.15:
            return "meaningful"
        if r_squared >= 0.05:
            return "mixed"
        return "weak"

    @staticmethod
    def _overall_factor_signal_assessment(*, rank_ic_assessment: str, regression_assessment: str) -> str:
        pair = {rank_ic_assessment, regression_assessment}
        if pair == {"meaningful"} or pair == {"meaningful", "mixed"}:
            return "meaningful"
        if pair == {"weak"}:
            return "weak"
        return "mixed"

    @staticmethod
    def _format_pct(value: object | None) -> str:
        if value is None:
            return "n/a"
        return f"{float(value) * 100:.2f}%"

    @staticmethod
    def _format_number(value: object | None) -> str:
        if value is None:
            return "n/a"
        return f"{float(value):.2f}"

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
            optimization_preferences = PortfolioAnalysisOrchestrator._candidate_search_optimization_preferences(
                payload.question
            )
            objective = PortfolioAnalysisOrchestrator._candidate_search_objective(
                payload.question,
                optimization_preferences=optimization_preferences,
            )
            return plan.model_copy(
                update={
                    "question_type": QuestionType.concentration_diversification,
                    "objective": objective,
                    "dynamic_workflow": PortfolioAnalysisOrchestrator._candidate_search_workflow(
                        objective,
                        optimization_preferences=optimization_preferences,
                    ),
                    "scenario_needed": False,
                    "candidate_search_needed": True,
                    "macro_overlay_needed": False,
                    "relevant_tickers": relevant_tickers,
                    "optimization_preferences": optimization_preferences,
                    "investigation_steps": PortfolioAnalysisOrchestrator._candidate_search_steps(
                        objective,
                        optimization_preferences=optimization_preferences,
                    ),
                    "caveats": PortfolioAnalysisOrchestrator._candidate_search_caveats(
                        objective,
                        optimization_preferences=optimization_preferences,
                    ),
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
            "optimize",
            "optimization",
            "maximize",
            "minimize",
            "best addition",
            "best stock",
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
            "average pairwise correlation",
            "pairwise correlation",
            "correlation",
            "reduce concentration",
            "reduce sector crowding",
            "reduce crowding",
            "broaden exposure",
            "spread out",
            "lower concentration",
        )
        return any(marker in normalized for marker in diversification_markers)

    @staticmethod
    def _candidate_search_optimization_preferences(question: str) -> list[OptimizationPreference]:
        normalized = " ".join(question.lower().strip().split())
        preferences: list[OptimizationPreference] = []

        def add(metric: str, direction: str, *, hard_constraint: bool = False) -> None:
            for item in preferences:
                if item.metric == metric and item.direction == direction:
                    item.hard_constraint = item.hard_constraint or hard_constraint
                    return
            preferences.append(
                OptimizationPreference(
                    metric=metric,
                    direction=direction,
                    hard_constraint=hard_constraint,
                )
            )

        if any(marker in normalized for marker in ("sharpe", "risk-adjusted", "risk adjusted", "return per unit of risk")):
            add("sharpe_ratio", "maximize")
        if any(
            marker in normalized
            for marker in (
                "average pairwise correlation",
                "pairwise correlation",
                "less correlated",
                "uncorrelated",
                "reduce correlation",
                "lower correlation",
            )
        ):
            add("average_pairwise_correlation", "minimize")
        if any(marker in normalized for marker in ("beta", "market sensitivity", "macro sensitivity")):
            add("beta_vs_benchmark", "minimize")
        if any(marker in normalized for marker in ("volatility", "volatile", "variance", "drawdown risk")):
            add("annualized_volatility", "minimize")
        if any(marker in normalized for marker in ("concentration", "crowding", "top 3 weight", "top3", "herfindahl")):
            add("herfindahl_index", "minimize")
            add("top3_share", "minimize")
        if re.search(r"\b(maximi[sz]e|improve|increase|boost|raise)\b.*\b(return|performance|alpha)\b", normalized):
            add("return_vs_benchmark", "maximize")
        if re.search(r"\b(without|while|keep|maintain|preserv(e|ing))\b.*\b(return|returns|performance)\b", normalized):
            add("trailing_return", "maximize", hard_constraint=True)
        elif any(
            marker in normalized
            for marker in (
                "does not hurt return",
                "doesn't hurt return",
                "without hurting return",
                "without killing returns",
                "keep returns intact",
                "preserving return",
                "maintain return",
                "without degrading return",
            )
        ):
            add("trailing_return", "maximize", hard_constraint=True)

        return preferences

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
    def _candidate_search_objective(
        question: str,
        *,
        optimization_preferences: list[OptimizationPreference] | None = None,
    ) -> str:
        normalized = " ".join(question.lower().strip().split())
        preferences = optimization_preferences or PortfolioAnalysisOrchestrator._candidate_search_optimization_preferences(
            question
        )
        preference_metrics = {item.metric for item in preferences}
        if {"average_pairwise_correlation", "herfindahl_index", "top3_share"} & preference_metrics:
            return "diversify"
        if "beta_vs_benchmark" in preference_metrics and not (
            {"sharpe_ratio", "return_vs_benchmark"} & preference_metrics
        ):
            return "reduce_macro_sensitivity"
        if PortfolioAnalysisOrchestrator._has_diversification_candidate_intent(normalized):
            return "diversify"
        if "beta" in normalized or "macro sensitivity" in normalized:
            return "reduce_macro_sensitivity"
        if PortfolioAnalysisOrchestrator._has_risk_adjusted_candidate_intent(normalized):
            return "performance"
        return "diversify"

    @staticmethod
    def _candidate_search_workflow(
        objective: str,
        *,
        optimization_preferences: list[OptimizationPreference] | None = None,
    ) -> str:
        preference_summary = PortfolioAnalysisOrchestrator._optimization_preference_summary(
            optimization_preferences or []
        )
        if objective == "performance":
            workflow = (
                "Run a candidate-search workflow focused on improving risk-adjusted returns. "
                "Use the existing portfolio's concentration, sector exposures, volatility, beta, "
                "Sharpe ratio, return versus SPY, and correlation structure as the baseline EDA. "
                "Then run a two-stage search: first filter broadly on quality and sector fit using margins, liquidity, "
                "leverage, and recent price strength; then rank final equities by simulated Sharpe improvement, "
                "marginal return versus SPY, lower beta, lower volatility, and lower correlation concentration."
            )
            return f"{workflow} {preference_summary}".strip()
        workflow = (
            "Run a candidate-search workflow focused on diversification improvement. "
            "Use the existing portfolio's concentration, sector exposures, top correlated clusters, "
            "Herfindahl index, top-3 weight, and pairwise correlation structure as the baseline EDA. "
            "Then run a two-stage search: first filter broadly on quality and sector underweights using margins, "
            "liquidity, leverage, and recent price strength; then rank final equities that lower concentration, "
            "reduce sector crowding, and improve correlation balance."
        )
        if objective == "reduce_macro_sensitivity":
            workflow = (
                "Run a candidate-search workflow focused on reducing macro sensitivity. "
                "Use the existing portfolio's beta, volatility, sector exposures, and return versus SPY as the baseline EDA. "
                "Then run a two-stage search: first filter broadly on quality, balance-sheet resilience, and recent price strength; "
                "then rank final equities that lower beta and volatility without unnecessarily degrading returns."
            )
        return f"{workflow} {preference_summary}".strip()

    @staticmethod
    def _candidate_search_steps(
        objective: str,
        *,
        optimization_preferences: list[OptimizationPreference] | None = None,
    ) -> list[str]:
        preference_step = PortfolioAnalysisOrchestrator._optimization_preference_step(
            optimization_preferences or []
        )
        if objective == "performance":
            steps = [
                "Confirm the current portfolio's Sharpe ratio, beta, annualized volatility, return versus SPY, and correlation structure.",
                "Use candidate search to screen broadly first, then shortlist and rank individual equities that improve risk-adjusted return rather than testing the benchmark or an ETF.",
                "Filter the broader universe with quality and trend constraints such as margins, leverage/liquidity, and positive recent price strength before running expensive scenario tests.",
                "Prioritize names that improve Sharpe and return versus SPY while preserving or improving trailing return and lowering beta and volatility.",
                "Use sector exposure and standalone portfolio correlation as secondary filters so the recommended addition is not just another highly correlated holding.",
                "Return ranked candidates with before/after metric deltas and explicit rationale tied to the deterministic scenario results.",
            ]
            return [*steps, preference_step] if preference_step else steps
        if objective == "reduce_macro_sensitivity":
            steps = [
                "Confirm the current portfolio's beta, annualized volatility, return versus SPY, and sector sensitivity first.",
                "Use candidate search to screen broadly first, then shortlist and rank individual equities that reduce beta or volatility rather than testing the benchmark or an ETF.",
                "Filter the broader universe with quality, balance-sheet, and trend constraints before running expensive scenario tests.",
                "Prioritize names that lower beta and volatility while respecting any return-preservation constraints in the question.",
                "Return ranked candidates with before/after metric deltas and explicit rationale tied to the deterministic scenario results.",
            ]
            return [*steps, preference_step] if preference_step else steps
        steps = [
            "Confirm the current portfolio's top-3 weight, Herfindahl concentration, sector exposures, and internal correlation hotspots.",
            "Use candidate search to screen broadly first, then shortlist and rank individual equities that reduce concentration and improve diversification rather than testing the benchmark or an ETF.",
            "Filter the broader universe with quality and trend constraints so the shortlist is not just low-correlation names with weak fundamentals.",
            "Prioritize names from underrepresented sectors and with lower standalone correlation to the current portfolio.",
            "Use before/after scenario results to quantify whether the addition lowers sector crowding and pairwise correlation.",
            "Return ranked candidates with explicit diversification metric deltas and rationale tied to deterministic scenario results.",
        ]
        return [*steps, preference_step] if preference_step else steps

    @staticmethod
    def _candidate_search_caveats(
        objective: str,
        *,
        optimization_preferences: list[OptimizationPreference] | None = None,
    ) -> list[str]:
        base = [
            "This is an open-ended addition question, so candidate search is required rather than single-name scenario analysis.",
            "Recommendations must be grounded in deterministic historical metric deltas, not model intuition.",
        ]
        if any(item.hard_constraint for item in optimization_preferences or []):
            base.append(
                "Hard constraints in the question should be enforced explicitly; if no candidate satisfies them, the output should say so."
            )
        if objective == "performance":
            return [
                *base,
                "If no candidate improves Sharpe while preserving return and lowering beta, the output should say so explicitly.",
            ]
        if objective == "reduce_macro_sensitivity":
            return [
                *base,
                "If no candidate lowers beta or volatility without violating the stated return constraint, the output should say so explicitly.",
            ]
        return [
            *base,
            "If no candidate materially improves diversification metrics without worsening the portfolio profile, the output should say so explicitly.",
        ]

    @staticmethod
    def _optimization_preference_summary(
        optimization_preferences: list[OptimizationPreference],
    ) -> str:
        if not optimization_preferences:
            return ""
        summary = ", ".join(
            f"{item.direction} {PortfolioAnalysisOrchestrator._optimization_metric_label(item.metric)}"
            + (" (hard constraint)" if item.hard_constraint else "")
            for item in optimization_preferences
        )
        return f"Optimization focus: {summary}."

    @staticmethod
    def _optimization_preference_step(
        optimization_preferences: list[OptimizationPreference],
    ) -> str | None:
        if not optimization_preferences:
            return None
        summary = "; ".join(
            f"{item.direction} {PortfolioAnalysisOrchestrator._optimization_metric_label(item.metric)}"
            + (" as a hard constraint" if item.hard_constraint else "")
            for item in optimization_preferences
        )
        return f"Optimize explicitly for: {summary}."

    @staticmethod
    def _optimization_metric_label(metric: str) -> str:
        labels = {
            "sharpe_ratio": "Sharpe ratio",
            "return_vs_benchmark": "return versus SPY",
            "trailing_return": "trailing return",
            "beta_vs_benchmark": "beta versus SPY",
            "annualized_volatility": "annualized volatility",
            "average_pairwise_correlation": "average pairwise correlation",
            "herfindahl_index": "Herfindahl concentration",
            "top3_share": "top 3 weight share",
        }
        return labels.get(metric, metric.replace("_", " "))

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
