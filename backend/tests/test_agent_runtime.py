from __future__ import annotations

import asyncio
from types import SimpleNamespace

import httpx
import pytest
from agents import Runner

from app.agents.runtime import AgentRuntime
from app.models.schemas import AnalysisPlan, QuestionType


def _plan() -> AnalysisPlan:
    return AnalysisPlan(
        question_type=QuestionType.general_health,
        objective="performance",
        explanation="test",
        dynamic_workflow="general_health",
        scenario_needed=False,
        candidate_search_needed=False,
        macro_overlay_needed=False,
        earnings_overlay_needed=False,
        filings_overlay_needed=False,
        relevant_tickers=["AAPL"],
        macro_themes=[],
        preferred_data_sources=[],
        dataset_selection_rationale=[],
        optimization_preferences=[],
        comparison_universe="portfolio_only",
        comparison_sector_filters=[],
        comparison_ticker_limit=None,
        investigation_steps=[],
        caveats=[],
    )


def test_connection_failure_does_not_poison_future_agent_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = [httpx.ConnectError("boom"), SimpleNamespace(final_output=_plan())]

    async def fake_run(agent, prompt, context=None):  # noqa: ANN001, ARG001
        next_item = responses.pop(0)
        if isinstance(next_item, Exception):
            raise next_item
        return next_item

    monkeypatch.setattr(Runner, "run", fake_run)
    runtime = AgentRuntime()

    with pytest.raises(RuntimeError, match="agent runtime is unavailable"):
        asyncio.run(runtime.run_planner("first"))

    result = asyncio.run(runtime.run_planner("second"))

    assert result.question_type == QuestionType.general_health
