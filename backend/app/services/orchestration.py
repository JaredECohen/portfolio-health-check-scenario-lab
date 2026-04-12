from __future__ import annotations

import asyncio
from uuid import uuid4

from app.agents.runtime import AgentRuntime
from app.models.schemas import (
    AnalysisResponse,
    EarningsOverlayResult,
    FilingsOverlayResult,
    HypotheticalPosition,
    MacroOverlayResult,
    OverlayBundle,
    PortfolioInput,
)
from app.tools.agent_tools import AnalysisRunContext
from app.services.analytics import AnalyticsService
from app.services.artifacts import ArtifactService
from app.services.dynamic_eda import DynamicEDAService
from app.services.market_data import MarketDataService
from app.services.portfolio_intake import PortfolioIntakeService
from app.services.scenario import ScenarioService
from app.services.sec_edgar import SecEdgarService


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
        normalized = await self.intake_service.normalize(payload)
        tickers = [holding.ticker for holding in normalized.holdings]
        price_history, benchmark_history = await self.market_data_service.fetch_price_history(
            tickers=tickers,
            benchmark_symbol=normalized.benchmark,
            lookback_days=normalized.lookback_days,
        )
        risk_free_rate = await self.market_data_service.get_risk_free_rate(self.risk_free_fallback)
        baseline_bundle = self.analytics_service.compute_baseline(
            holdings=normalized.holdings,
            benchmark_symbol=normalized.benchmark,
            price_history=price_history,
            benchmark_history=benchmark_history,
            risk_free_rate=risk_free_rate,
        )
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
        )
        dynamic_eda = await self.agent_runtime.run_dynamic_eda(
            f"Run the {plan.dynamic_workflow} workflow for this question: {normalized.question}",
            context=context,
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
        if plan.earnings_overlay_needed and plan.relevant_tickers:
            overlay_tasks.append(
                self.agent_runtime.run_earnings_overlay(
                    f"Analyze recent earnings transcript signals for {plan.relevant_tickers}",
                    context=context,
                )
            )
        if plan.filings_overlay_needed and plan.relevant_tickers:
            overlay_tasks.append(
                self.agent_runtime.run_filings_overlay(
                    f"Analyze recent filings for {plan.relevant_tickers}",
                    context=context,
                )
            )
        if overlay_tasks:
            overlay_results = await asyncio.gather(*overlay_tasks)
            for item in overlay_results:
                if isinstance(item, MacroOverlayResult):
                    overlays.macro = item
                if isinstance(item, EarningsOverlayResult):
                    overlays.earnings = item
                if isinstance(item, FilingsOverlayResult):
                    overlays.filings = item

        if plan.candidate_search_needed:
            dynamic_eda.candidate_search = await self.agent_runtime.run_candidate_search(
                "Rank curated candidate additions for this portfolio objective.",
                context=context,
            )

        after_bundle = None
        if normalized.hypothetical_position or plan.scenario_needed:
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
                )
                dynamic_eda.scenario_analysis = scenario

        evidence_pack = {
            "baseline_metrics": baseline_bundle.metrics_map,
            "eda_findings": [item.model_dump() for item in dynamic_eda.findings],
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
