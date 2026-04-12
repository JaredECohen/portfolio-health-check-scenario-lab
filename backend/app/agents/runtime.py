from __future__ import annotations

from agents import Agent, Runner

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
    rank_candidate_positions,
    run_dynamic_eda,
)


class AgentRuntime:
    def __init__(self) -> None:
        self.planner = Agent(
            name="analysis_planner",
            model="gpt-5.4",
            instructions=PLANNER_PROMPT,
            output_type=AnalysisPlan,
        )
        self.dynamic_eda = Agent(
            name="dynamic_eda",
            model="gpt-5.4-mini",
            instructions=DYNAMIC_EDA_PROMPT,
            tools=[run_dynamic_eda],
            output_type=DynamicEDAResult,
        )
        self.macro_overlay = Agent(
            name="macro_overlay",
            model="gpt-5.4-mini",
            instructions=MACRO_OVERLAY_PROMPT,
            tools=[compute_macro_overlay],
            output_type=MacroOverlayResult,
        )
        self.earnings_overlay = Agent(
            name="earnings_overlay",
            model="gpt-5.4-mini",
            instructions=EARNINGS_OVERLAY_PROMPT,
            tools=[collect_earnings_overlay_data],
            output_type=EarningsOverlayResult,
        )
        self.filings_overlay = Agent(
            name="filings_overlay",
            model="gpt-5.4-mini",
            instructions=FILINGS_OVERLAY_PROMPT,
            tools=[collect_filings_overlay_data],
            output_type=FilingsOverlayResult,
        )
        self.candidate_search = Agent(
            name="candidate_search",
            model="gpt-5.4-mini",
            instructions=CANDIDATE_SEARCH_PROMPT,
            tools=[rank_candidate_positions],
            output_type=CandidateSearchResult,
        )
        self.writer = Agent(
            name="hypothesis_writer",
            model="gpt-5.4",
            instructions=WRITER_PROMPT,
            output_type=FinalMemo,
        )
        self.critic = Agent(
            name="critic",
            model="gpt-5.4",
            instructions=CRITIC_PROMPT,
            output_type=CriticResult,
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

