from __future__ import annotations

from agents import Agent, AgentOutputSchema, Runner

from app.agents.system_prompts import (
    CANDIDATE_SEARCH_PROMPT,
    CRITIC_PROMPT,
    DYNAMIC_EDA_PROMPT,
    EARNINGS_OVERLAY_PROMPT,
    FILINGS_OVERLAY_PROMPT,
    MACRO_OVERLAY_PROMPT,
    PLANNER_PROMPT,
    WRITER_PROMPT,
)
from app.models.schemas import (
    AnalysisPlan,
    CandidateSearchResult,
    CriticResult,
    DynamicEDAResult,
    EarningsOverlayResult,
    FilingsOverlayResult,
    FinalMemo,
    MacroOverlayResult,
)
from app.tools.agent_tools import (
    collect_earnings_overlay_data,
    collect_filings_overlay_data,
    compute_macro_overlay,
    deterministic_text_nlp,
    rank_candidate_positions,
    run_dynamic_eda,
    shortlist_candidate_universe,
)


class AgentRuntime:
    def __init__(self) -> None:
        plan_output = AgentOutputSchema(AnalysisPlan, strict_json_schema=False)
        dynamic_eda_output = AgentOutputSchema(DynamicEDAResult, strict_json_schema=False)
        macro_output = AgentOutputSchema(MacroOverlayResult, strict_json_schema=False)
        earnings_output = AgentOutputSchema(EarningsOverlayResult, strict_json_schema=False)
        filings_output = AgentOutputSchema(FilingsOverlayResult, strict_json_schema=False)
        candidate_output = AgentOutputSchema(CandidateSearchResult, strict_json_schema=False)
        memo_output = AgentOutputSchema(FinalMemo, strict_json_schema=False)
        critic_output = AgentOutputSchema(CriticResult, strict_json_schema=False)
        self.planner = Agent(
            name="analysis_planner",
            model="gpt-5.4",
            instructions=PLANNER_PROMPT,
            output_type=plan_output,
        )
        self.dynamic_eda = Agent(
            name="dynamic_eda",
            model="gpt-5.4-mini",
            instructions=DYNAMIC_EDA_PROMPT,
            tools=[run_dynamic_eda],
            output_type=dynamic_eda_output,
        )
        self.macro_overlay = Agent(
            name="macro_overlay",
            model="gpt-5.4-mini",
            instructions=MACRO_OVERLAY_PROMPT,
            tools=[compute_macro_overlay],
            output_type=macro_output,
        )
        self.earnings_overlay = Agent(
            name="earnings_overlay",
            model="gpt-5.4-mini",
            instructions=EARNINGS_OVERLAY_PROMPT,
            tools=[collect_earnings_overlay_data, deterministic_text_nlp],
            output_type=earnings_output,
        )
        self.filings_overlay = Agent(
            name="filings_overlay",
            model="gpt-5.4-mini",
            instructions=FILINGS_OVERLAY_PROMPT,
            tools=[collect_filings_overlay_data, deterministic_text_nlp],
            output_type=filings_output,
        )
        self.candidate_search = Agent(
            name="candidate_search",
            model="gpt-5.4-mini",
            instructions=CANDIDATE_SEARCH_PROMPT,
            tools=[shortlist_candidate_universe, rank_candidate_positions],
            output_type=candidate_output,
        )
        self.writer = Agent(
            name="hypothesis_writer",
            model="gpt-5.4",
            instructions=WRITER_PROMPT,
            output_type=memo_output,
        )
        self.critic = Agent(
            name="critic",
            model="gpt-5.4",
            instructions=CRITIC_PROMPT,
            output_type=critic_output,
        )

    async def run_planner(self, prompt: str) -> AnalysisPlan:
        result = await Runner.run(self.planner, prompt)
        return result.final_output

    async def run_dynamic_eda(self, prompt: str, *, context: object) -> DynamicEDAResult:
        result = await Runner.run(self.dynamic_eda, prompt, context=context)
        return result.final_output

    async def run_macro_overlay(self, prompt: str, *, context: object) -> MacroOverlayResult:
        result = await Runner.run(self.macro_overlay, prompt, context=context)
        return result.final_output

    async def run_earnings_overlay(self, prompt: str, *, context: object) -> EarningsOverlayResult:
        result = await Runner.run(self.earnings_overlay, prompt, context=context)
        return result.final_output

    async def run_filings_overlay(self, prompt: str, *, context: object) -> FilingsOverlayResult:
        result = await Runner.run(self.filings_overlay, prompt, context=context)
        return result.final_output

    async def run_candidate_search(self, prompt: str, *, context: object) -> CandidateSearchResult:
        result = await Runner.run(self.candidate_search, prompt, context=context)
        return result.final_output

    async def run_writer(self, prompt: str) -> FinalMemo:
        result = await Runner.run(self.writer, prompt)
        return result.final_output

    async def run_critic(self, prompt: str) -> CriticResult:
        result = await Runner.run(self.critic, prompt)
        return result.final_output
