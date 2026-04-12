from __future__ import annotations

import json
import re
from dataclasses import dataclass

import pandas as pd
from agents import RunContextWrapper, function_tool

from app.models.schemas import (
    AnalysisPlan,
    CandidateSearchResult,
    DynamicEDAResult,
    EarningsOverlayResult,
    EarningsOverlayTickerResult,
    FilingsOverlayResult,
    FilingsOverlayTickerResult,
    MacroOverlayResult,
)
from app.services.analytics import AnalyticsBundle
from app.services.dynamic_eda import DynamicEDAService
from app.services.scenario import ScenarioService
from app.services.sec_edgar import SecEdgarService


@dataclass
class AnalysisRunContext:
    question: str
    plan: AnalysisPlan
    baseline_bundle: AnalyticsBundle
    dynamic_eda_service: DynamicEDAService
    scenario_service: ScenarioService
    sec_edgar_service: SecEdgarService
    lookback_days: int
    benchmark_symbol: str
    hypothetical_present: bool


def _extract_tone(text: str) -> str:
    lowered = text.lower()
    positive_hits = sum(term in lowered for term in ("strong", "improve", "upside", "momentum"))
    cautious_hits = sum(term in lowered for term in ("cautious", "pressure", "softness", "uncertain"))
    if positive_hits > cautious_hits:
        return "constructive"
    if cautious_hits > positive_hits:
        return "more cautious"
    return "mixed"


def _extract_findings(text: str, patterns: dict[str, str]) -> list[str]:
    sentences = re.split(r"(?<=[.!?])\s+", re.sub(r"\s+", " ", text))
    findings: list[str] = []
    for label, pattern in patterns.items():
        compiled = re.compile(pattern, re.IGNORECASE)
        matches = [sentence[:220] for sentence in sentences if compiled.search(sentence)]
        if matches:
            findings.append(f"{label}: {matches[0]}")
    return findings[:5]


@function_tool
async def run_dynamic_eda(context: RunContextWrapper[AnalysisRunContext]) -> str:
    result = await context.context.dynamic_eda_service.execute(
        plan=context.context.plan,
        question=context.context.question,
        baseline_bundle=context.context.baseline_bundle,
    )
    return result.model_dump_json()


@function_tool
async def compute_macro_overlay(context: RunContextWrapper[AnalysisRunContext]) -> str:
    alpha_vantage = context.context.scenario_service.alpha_vantage
    question_type = context.context.plan.question_type.value.replace("_", " ")
    try:
        treasury = await alpha_vantage.get_treasury_yield()
        wti = await alpha_vantage.get_wti()
        cpi = await alpha_vantage.get_cpi()
        aligned = pd.concat(
            [
                context.context.baseline_bundle.portfolio_returns.rename("portfolio"),
                context.context.baseline_bundle.benchmark_returns.rename("benchmark"),
                treasury["value"].diff().rename("yield_change"),
                wti["value"].pct_change().rename("oil_change"),
            ],
            axis=1,
        ).dropna()
        cpi_yoy = None
        if len(cpi.index) >= 13:
            cpi_yoy = float((cpi["value"].iloc[-1] / cpi["value"].iloc[-13]) - 1)
        treasury_corr_portfolio = float(aligned["portfolio"].corr(aligned["yield_change"]))
        treasury_corr_benchmark = float(aligned["benchmark"].corr(aligned["yield_change"]))
        oil_corr_portfolio = float(aligned["portfolio"].corr(aligned["oil_change"]))
        oil_corr_benchmark = float(aligned["benchmark"].corr(aligned["oil_change"]))
        findings = [
            f"Portfolio correlation to 10Y yield changes is {treasury_corr_portfolio:.2f} versus {treasury_corr_benchmark:.2f} for the benchmark.",
            f"Portfolio correlation to daily WTI moves is {oil_corr_portfolio:.2f} versus {oil_corr_benchmark:.2f} for the benchmark.",
        ]
        if cpi_yoy is not None:
            findings.append(f"Latest CPI year-over-year change in the macro feed is {cpi_yoy * 100:.2f}%.")
        payload = MacroOverlayResult(
            question_focus=question_type,
            series_used=["TREASURY_YIELD", "CPI", "WTI"],
            findings=findings,
            portfolio_sensitivities={
                "treasury_yield_corr": treasury_corr_portfolio,
                "oil_corr": oil_corr_portfolio,
                "beta_vs_benchmark": context.context.baseline_bundle.metrics_map["beta_vs_benchmark"],
            },
            benchmark_sensitivities={
                "treasury_yield_corr": treasury_corr_benchmark,
                "oil_corr": oil_corr_benchmark,
            },
            caveats=[
                "Macro overlay is based on empirical co-movement and proxy regimes, not a forecast model.",
                "CPI is monthly, so inflation context is lower-frequency than daily market returns.",
            ],
        )
    except Exception:  # noqa: BLE001
        payload = MacroOverlayResult(
            question_focus=question_type,
            series_used=["TREASURY_YIELD", "CPI", "WTI"],
            findings=["Macro series were unavailable or could not be aligned for this run."],
            portfolio_sensitivities={},
            benchmark_sensitivities={},
            caveats=[
                "Macro overlay is unavailable, so conclusions should lean on baseline and scenario evidence instead.",
            ],
        )
    return payload.model_dump_json()


@function_tool
async def collect_earnings_overlay_data(
    context: RunContextWrapper[AnalysisRunContext],
    tickers: list[str],
) -> str:
    results: list[EarningsOverlayTickerResult] = []
    for ticker in tickers:
        company = next(
            (
                holding.company_name or holding.ticker
                for holding in context.context.baseline_bundle.holdings
                if holding.ticker == ticker
            ),
            ticker,
        )
        transcript = await context.context.scenario_service.alpha_vantage.get_latest_earnings_transcript(
            ticker
        )
        if not transcript:
            results.append(
                EarningsOverlayTickerResult(
                    ticker=ticker,
                    company_name=company,
                    quarter=None,
                    tone="unavailable",
                    findings=["Transcript not available from Alpha Vantage for recent quarters."],
                    transcript_available=False,
                )
            )
            continue
        raw_items = transcript["items"]
        joined = json.dumps(raw_items) if not isinstance(raw_items, str) else raw_items
        tone = _extract_tone(joined)
        findings = _extract_findings(
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
                company_name=company,
                quarter=transcript["quarter"],
                tone=tone,
                findings=findings or ["Transcript retrieved, but no targeted language cluster stood out."],
            )
        )
    return EarningsOverlayResult(companies=results).model_dump_json()


@function_tool
async def collect_filings_overlay_data(
    context: RunContextWrapper[AnalysisRunContext],
    tickers: list[str],
) -> str:
    results: list[FilingsOverlayTickerResult] = []
    for ticker in tickers:
        holding = next(
            (item for item in context.context.baseline_bundle.holdings if item.ticker == ticker),
            None,
        )
        if holding is None or not holding.cik:
            results.append(
                FilingsOverlayTickerResult(
                    ticker=ticker,
                    company_name=holding.company_name if holding else ticker,
                    findings=["CIK not available for filing lookup."],
                    filing_available=False,
                )
            )
            continue
        filing = await context.context.sec_edgar_service.get_recent_filing(holding.cik)
        if not filing:
            results.append(
                FilingsOverlayTickerResult(
                    ticker=ticker,
                    company_name=holding.company_name or ticker,
                    findings=["Recent 10-K or 10-Q filing was not found."],
                    filing_available=False,
                )
            )
            continue
        filing_text = await context.context.sec_edgar_service.get_filing_text(
            cik=holding.cik,
            accession_number=filing["accession_number"],
            primary_document=filing["primary_document"],
        )
        findings = context.context.sec_edgar_service.extract_filing_signals(filing_text)
        results.append(
            FilingsOverlayTickerResult(
                ticker=ticker,
                company_name=holding.company_name or ticker,
                form_type=filing["form_type"],
                filed_at=filing["filed_at"],
                findings=findings or ["Recent filing retrieved without a dominant flagged theme."],
            )
        )
    return FilingsOverlayResult(companies=results).model_dump_json()


@function_tool
async def rank_candidate_positions(context: RunContextWrapper[AnalysisRunContext]) -> str:
    result = await context.context.scenario_service.rank_candidates(
        baseline_bundle=context.context.baseline_bundle,
        benchmark_symbol=context.context.benchmark_symbol,
        objective=context.context.plan.objective,
        lookback_days=context.context.lookback_days,
        max_candidates=5,
    )
    return result.model_dump_json()
