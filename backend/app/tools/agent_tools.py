from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import date

from agents import RunContextWrapper, function_tool

from app.models.schemas import (
    AnalysisPlan,
    CandidateSearchResult,
    DynamicEDAResult,
    EntityFrequency,
    EarningsOverlayResult,
    EarningsOverlayTickerResult,
    FilingsOverlayResult,
    FilingsOverlayTickerResult,
    MacroOverlayResult,
    NLPTextSummary,
    TopicCluster,
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
    start_date: date | None
    end_date: date | None


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


STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "has",
    "have",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "their",
    "this",
    "to",
    "was",
    "we",
    "with",
    "our",
    "you",
    "your",
}

POSITIVE_TERMS = (
    "strong",
    "improve",
    "improved",
    "improvement",
    "upside",
    "momentum",
    "growth",
    "resilient",
    "disciplined",
)

CAUTIOUS_TERMS = (
    "cautious",
    "pressure",
    "softness",
    "uncertain",
    "uncertainty",
    "headwind",
    "risk",
    "weaker",
    "volatile",
)

TOPIC_KEYWORDS = {
    "guidance": {"guidance", "outlook", "forecast", "expect"},
    "demand": {"demand", "orders", "order", "volume", "customers", "pipeline"},
    "profitability": {"margin", "margins", "profit", "profitability", "cost", "pricing"},
    "liquidity": {"liquidity", "cash", "credit", "debt", "leverage", "interest"},
    "regulation": {"regulatory", "compliance", "government", "tariff", "privacy", "antitrust"},
    "operations": {"supply", "capacity", "inventory", "manufacturing", "operations"},
}

ENTITY_EXCLUSIONS = {
    "GAAP",
    "EPS",
    "CEO",
    "CFO",
    "SEC",
    "INC",
    "CORP",
    "Q1",
    "Q2",
    "Q3",
    "Q4",
}


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _tokenize_words(text: str) -> list[str]:
    return re.findall(r"\b[a-z][a-z'-]{2,}\b", text.lower())


def _extract_keywords_from_text(text: str, *, limit: int = 8) -> list[str]:
    tokens = [token for token in _tokenize_words(text) if token not in STOPWORDS]
    counts = Counter(tokens)
    return [term for term, _count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:limit]]


def _extract_entities_from_text(text: str, *, limit: int = 8) -> list[EntityFrequency]:
    matches = re.findall(r"\b(?:[A-Z]{2,5}|[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})\b", text)
    counts = Counter(
        match.strip()
        for match in matches
        if match.strip().upper() not in ENTITY_EXCLUSIONS and len(match.strip()) > 1
    )
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:limit]
    return [EntityFrequency(entity=name, count=count) for name, count in ranked]


def _extract_topic_clusters_from_text(text: str, *, limit: int = 4) -> list[TopicCluster]:
    sentences = re.split(r"(?<=[.!?])\s+", _normalize_text(text))
    clusters: list[TopicCluster] = []
    for topic, keywords in TOPIC_KEYWORDS.items():
        hits = []
        hit_keywords: Counter[str] = Counter()
        for sentence in sentences:
            sentence_lower = sentence.lower()
            matched = [keyword for keyword in keywords if re.search(rf"\b{re.escape(keyword)}\b", sentence_lower)]
            if matched:
                hits.append(sentence[:220])
                hit_keywords.update(matched)
        if hits:
            clusters.append(
                TopicCluster(
                    topic=topic,
                    mentions=len(hits),
                    keywords=[term for term, _count in hit_keywords.most_common(3)],
                    representative_text=hits[0],
                )
            )
    return sorted(clusters, key=lambda item: (-item.mentions, item.topic))[:limit]


def summarize_text_nlp(text: str) -> NLPTextSummary:
    normalized = _normalize_text(text)
    lowered = normalized.lower()
    sentiment_counts = {
        "positive": sum(len(re.findall(rf"\b{re.escape(term)}\b", lowered)) for term in POSITIVE_TERMS),
        "cautious": sum(len(re.findall(rf"\b{re.escape(term)}\b", lowered)) for term in CAUTIOUS_TERMS),
    }
    return NLPTextSummary(
        sentiment_counts=sentiment_counts,
        keywords=_extract_keywords_from_text(normalized),
        entities=_extract_entities_from_text(normalized),
        topic_clusters=_extract_topic_clusters_from_text(normalized),
    )


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
    question_type = context.context.plan.question_type.value.replace("_", " ")
    try:
        regime_analysis = await context.context.dynamic_eda_service.analyze_rates_regimes(
            context.context.baseline_bundle
        )
        if regime_analysis is None:
            raise ValueError("No usable rates regime analysis was produced.")
        findings = []
        up_stats = regime_analysis.get("yield_up")
        down_stats = regime_analysis.get("yield_down")
        if up_stats is not None:
            findings.append(
                f"On {up_stats['days']} yield-up shock days (>= {up_stats['threshold_bps']:.2f} bps in the 10Y), the portfolio averaged {up_stats['avg_same_day_excess'] * 100:.2f}% excess return versus SPY."
            )
            findings.append(
                f"Average 5-day excess return after yield-up shocks was {(up_stats['avg_forward_5d_excess'] or 0.0) * 100:.2f}%."
            )
        if down_stats is not None:
            findings.append(
                f"On {down_stats['days']} yield-down shock days (<= {down_stats['threshold_bps']:.2f} bps in the 10Y), the portfolio averaged {down_stats['avg_same_day_excess'] * 100:.2f}% excess return versus SPY."
            )
            findings.append(
                f"Average 10-day excess return after yield-down shocks was {(down_stats['avg_forward_10d_excess'] or 0.0) * 100:.2f}%."
            )
        findings.append(
            "This macro overlay is based on conditional rate-shock regimes and forward windows, not broad unconditional correlation."
        )
        payload = MacroOverlayResult(
            question_focus=question_type,
            series_used=["TREASURY_YIELD"],
            findings=findings,
            portfolio_sensitivities={
                "yield_up_same_day_excess": float(up_stats["avg_same_day_excess"]) if up_stats else 0.0,
                "yield_up_forward_5d_excess": float(up_stats["avg_forward_5d_excess"] or 0.0) if up_stats else 0.0,
                "yield_down_forward_10d_excess": float(down_stats["avg_forward_10d_excess"] or 0.0) if down_stats else 0.0,
                "beta_vs_benchmark": context.context.baseline_bundle.metrics_map["beta_vs_benchmark"],
            },
            benchmark_sensitivities={
                "yield_up_same_day_return": float(up_stats["avg_same_day_benchmark"]) if up_stats else 0.0,
                "yield_down_same_day_return": float(down_stats["avg_same_day_benchmark"]) if down_stats else 0.0,
            },
            caveats=[
                "Rates interpretation is conditional on the recent sample window and percentile-based shock thresholds.",
                "This is still empirical regime analysis, not a structural duration or factor model.",
            ],
        )
    except Exception:  # noqa: BLE001
        payload = MacroOverlayResult(
            question_focus=question_type,
            series_used=["TREASURY_YIELD"],
            findings=["Macro series were unavailable or could not be aligned for this run."],
            portfolio_sensitivities={},
            benchmark_sensitivities={},
            caveats=[
                "Macro overlay is unavailable, so conclusions should lean on baseline and scenario evidence instead.",
            ],
        )
    return payload.model_dump_json()


@function_tool
async def deterministic_text_nlp(text: str) -> str:
    return summarize_text_nlp(text).model_dump_json()


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
        if context.context.start_date or context.context.end_date:
            transcript = await context.context.scenario_service.alpha_vantage.get_windowed_earnings_transcript(
                ticker,
                start_date=context.context.start_date,
                end_date=context.context.end_date,
            )
        else:
            transcript = await context.context.scenario_service.alpha_vantage.get_latest_earnings_transcript(
                ticker
            )
        if not transcript:
            results.append(
                EarningsOverlayTickerResult(
                    ticker=ticker,
                    company_name=company,
                    quarter=None,
                    event_date=None,
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
                event_date=transcript.get("event_date"),
                tone=tone,
                findings=findings or ["Transcript retrieved, but no targeted language cluster stood out."],
                nlp_summary=summarize_text_nlp(joined),
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
        if context.context.start_date or context.context.end_date:
            filing = await context.context.sec_edgar_service.get_recent_filing(
                holding.cik,
                start_date=context.context.start_date,
                end_date=context.context.end_date,
            )
        else:
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
                nlp_summary=summarize_text_nlp(filing_text),
            )
        )
    return FilingsOverlayResult(companies=results).model_dump_json()


@function_tool
async def shortlist_candidate_universe(
    context: RunContextWrapper[AnalysisRunContext],
    preferred_sectors: list[str] | None = None,
    excluded_sectors: list[str] | None = None,
    max_candidates: int = 20,
) -> str:
    result = context.context.scenario_service.shortlist_universe(
        baseline_bundle=context.context.baseline_bundle,
        objective=context.context.plan.objective,
        preferred_sectors=preferred_sectors,
        excluded_sectors=excluded_sectors,
        max_candidates=max_candidates,
    )
    return json.dumps(result)


@function_tool
async def rank_candidate_positions(
    context: RunContextWrapper[AnalysisRunContext],
    tickers: list[str] | None = None,
) -> str:
    result = await context.context.scenario_service.rank_candidates(
        baseline_bundle=context.context.baseline_bundle,
        benchmark_symbol=context.context.benchmark_symbol,
        objective=context.context.plan.objective,
        lookback_days=context.context.lookback_days,
        start_date=context.context.start_date,
        end_date=context.context.end_date,
        candidate_tickers=tickers,
        max_candidates=5,
    )
    return result.model_dump_json()
