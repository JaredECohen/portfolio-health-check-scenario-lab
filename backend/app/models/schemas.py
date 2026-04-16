from __future__ import annotations

from datetime import date
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


class AssetType(str, Enum):
    equity = "Equity"


class QuestionType(str, Enum):
    general_health = "general_health"
    concentration_diversification = "concentration_diversification"
    performance_drivers = "performance_drivers"
    rates_macro = "rates_macro"
    geopolitical_war = "geopolitical_war"
    what_if_addition = "what_if_addition"
    factor_cross_section = "factor_cross_section"


class Holding(BaseModel):
    ticker: str = Field(min_length=1, max_length=10)
    shares: float = Field(gt=0)
    cost_basis: float | None = Field(default=None, ge=0)
    company_name: str | None = None
    sector: str | None = None
    cik: str | None = None
    exchange: str | None = None
    asset_type: AssetType = AssetType.equity

    @field_validator("ticker")
    @classmethod
    def uppercase_ticker(cls, value: str) -> str:
        return value.upper().strip()


class HypotheticalPosition(BaseModel):
    ticker: str
    shares: float | None = Field(default=None, gt=0)
    target_weight: float | None = Field(default=None, gt=0, lt=1)
    company_name: str | None = None
    sector: str | None = None
    cik: str | None = None
    exchange: str | None = None

    @field_validator("ticker")
    @classmethod
    def uppercase_ticker(cls, value: str) -> str:
        return value.upper().strip()

    @model_validator(mode="after")
    def check_shape(self) -> "HypotheticalPosition":
        if self.shares is None and self.target_weight is None:
            raise ValueError("Hypothetical position requires either shares or target_weight.")
        if self.shares is not None and self.target_weight is not None:
            raise ValueError("Hypothetical position must use shares or target_weight, not both.")
        return self


class PortfolioInput(BaseModel):
    holdings: list[Holding] = Field(min_length=1)
    question: str = Field(min_length=5)
    hypothetical_position: HypotheticalPosition | None = None
    benchmark: str = "SPY"
    lookback_days: int = Field(default=252, ge=63, le=756)
    start_date: date | None = None
    end_date: date | None = None

    @field_validator("benchmark")
    @classmethod
    def uppercase_benchmark(cls, value: str) -> str:
        return value.upper().strip()

    @model_validator(mode="after")
    def validate_window(self) -> "PortfolioInput":
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("start_date must be on or before end_date.")
        return self


class TickerMetadata(BaseModel):
    ticker: str
    company_name: str
    cik: str
    exchange: str | None = None
    sector: str | None = None
    asset_type: AssetType = AssetType.equity


class TickerQuote(BaseModel):
    ticker: str
    price: float = Field(gt=0)
    as_of: date


class PositionSnapshot(BaseModel):
    ticker: str
    company_name: str
    sector: str | None = None
    shares: float
    current_price: float
    market_value: float
    weight: float
    trailing_return: float
    cost_basis: float | None = None
    pnl_dollar: float | None = None
    pnl_pct: float | None = None


class SectorExposure(BaseModel):
    sector: str
    market_value: float
    weight: float


class PerformancePoint(BaseModel):
    date: str
    portfolio_index: float
    benchmark_index: float


class Contributor(BaseModel):
    ticker: str
    company_name: str
    return_pct: float
    contribution_pct: float
    weight: float


class PortfolioMetric(BaseModel):
    key: str
    label: str
    value: float | None
    formatted: str


class AnalysisTable(BaseModel):
    name: str
    columns: list[str]
    rows: list[dict[str, Any]]


class EDAFinding(BaseModel):
    headline: str
    evidence: list[str]
    metrics: dict[str, float | str]
    severity: str = "info"


class DataSourceReference(BaseModel):
    source: str
    series: str
    category: str
    description: str
    access: str = "free"
    requires_api_key: bool = False
    status: str = "available"
    url: str | None = None
    rationale: str | None = None


class NewsArticle(BaseModel):
    source: str
    source_type: str
    title: str
    url: str
    published_at: str | None = None
    domain: str | None = None
    summary: str | None = None
    sentiment: float | None = None
    relevance: float | None = None
    tickers: list[str] = Field(default_factory=list)
    topics: list[str] = Field(default_factory=list)


class NewsSourceStats(BaseModel):
    source: str
    article_count: int
    avg_sentiment: float | None = None
    latest_published_at: str | None = None


class NewsIntelResult(BaseModel):
    query: str
    retrieval_sources: list[str]
    articles: list[NewsArticle] = Field(default_factory=list)
    source_stats: list[NewsSourceStats] = Field(default_factory=list)
    dominant_topics: list[str] = Field(default_factory=list)
    caveats: list[str] = Field(default_factory=list)


class BaselineAnalytics(BaseModel):
    total_portfolio_value: float
    total_cost_basis: float | None = None
    benchmark_symbol: str
    risk_free_rate_used: float
    effective_start_date: str
    effective_end_date: str
    effective_observations: int
    metrics: list[PortfolioMetric]
    positions: list[PositionSnapshot]
    sector_exposures: list[SectorExposure]
    contributors: list[Contributor]
    best_performers: list[Contributor]
    worst_performers: list[Contributor]
    correlation_matrix: dict[str, dict[str, float]]
    performance_series: list[PerformancePoint]


class ScenarioDelta(BaseModel):
    metric: str
    before: float | None
    after: float | None
    delta: float | None


class ScenarioAnalytics(BaseModel):
    label: str
    hypothetical_position: HypotheticalPosition
    before_metrics: list[PortfolioMetric]
    after_metrics: list[PortfolioMetric]
    deltas: list[ScenarioDelta]
    before_sector_exposures: list[SectorExposure]
    after_sector_exposures: list[SectorExposure]
    before_positions: list[PositionSnapshot]
    after_positions: list[PositionSnapshot]


class MacroOverlayResult(BaseModel):
    question_focus: str
    series_used: list[str]
    findings: list[str]
    portfolio_sensitivities: dict[str, float]
    benchmark_sensitivities: dict[str, float]
    caveats: list[str]


class EntityFrequency(BaseModel):
    entity: str
    count: int


class TopicCluster(BaseModel):
    topic: str
    mentions: int
    keywords: list[str]
    representative_text: str | None = None


class NLPTextSummary(BaseModel):
    sentiment_counts: dict[str, int]
    keywords: list[str]
    entities: list[EntityFrequency]
    topic_clusters: list[TopicCluster]


class EarningsOverlayTickerResult(BaseModel):
    ticker: str
    company_name: str
    quarter: str | None = None
    event_date: str | None = None
    tone: str
    findings: list[str]
    nlp_summary: NLPTextSummary | None = None
    transcript_available: bool = True


class EarningsOverlayResult(BaseModel):
    companies: list[EarningsOverlayTickerResult]


class FilingsOverlayTickerResult(BaseModel):
    ticker: str
    company_name: str
    form_type: str | None = None
    filed_at: str | None = None
    findings: list[str]
    nlp_summary: NLPTextSummary | None = None
    filing_available: bool = True


class FilingsOverlayResult(BaseModel):
    companies: list[FilingsOverlayTickerResult]


class CandidateRank(BaseModel):
    ticker: str
    company_name: str
    sector: str | None = None
    score: float
    rationale: list[str]
    deltas: list[ScenarioDelta]


class CandidateSearchResult(BaseModel):
    objective: str
    method: str
    candidates: list[CandidateRank]
    screening_summary: list[str] = Field(default_factory=list)


class OptimizationPreference(BaseModel):
    metric: str
    direction: str
    hard_constraint: bool = False


class ResearchAgenda(BaseModel):
    focus_areas: list[str] = Field(default_factory=list)
    analysis_ideas: list[str] = Field(default_factory=list)
    follow_up_questions: list[str] = Field(default_factory=list)
    overlay_requests: list[str] = Field(default_factory=list)
    candidate_search_guidance: list[str] = Field(default_factory=list)
    memo_watchouts: list[str] = Field(default_factory=list)


class ResearchSynthesis(BaseModel):
    integrated_insights: list[str] = Field(default_factory=list)
    confirmations: list[str] = Field(default_factory=list)
    tensions: list[str] = Field(default_factory=list)
    eda_implications: list[str] = Field(default_factory=list)
    candidate_search_implications: list[str] = Field(default_factory=list)
    memo_implications: list[str] = Field(default_factory=list)


class AgentCollaboration(BaseModel):
    research_agenda: ResearchAgenda | None = None
    research_synthesis: ResearchSynthesis | None = None


class AnalysisPlan(BaseModel):
    question_type: QuestionType
    objective: str
    explanation: str
    dynamic_workflow: str
    scenario_needed: bool = False
    candidate_search_needed: bool = False
    macro_overlay_needed: bool = False
    earnings_overlay_needed: bool = False
    filings_overlay_needed: bool = False
    relevant_tickers: list[str] = Field(default_factory=list)
    macro_themes: list[str] = Field(default_factory=list)
    preferred_data_sources: list[str] = Field(default_factory=list)
    dataset_selection_rationale: list[str] = Field(default_factory=list)
    optimization_preferences: list[OptimizationPreference] = Field(default_factory=list)
    comparison_universe: str = "portfolio_only"
    comparison_sector_filters: list[str] = Field(default_factory=list)
    comparison_ticker_limit: int | None = None
    investigation_steps: list[str] = Field(default_factory=list)
    caveats: list[str] = Field(default_factory=list)


class DynamicEDAResult(BaseModel):
    workflow: str
    question_type: QuestionType
    findings: list[EDAFinding]
    tables: list[AnalysisTable] = Field(default_factory=list)
    data_sources: list[DataSourceReference] = Field(default_factory=list)
    news_intel: NewsIntelResult | None = None
    scenario_analysis: ScenarioAnalytics | None = None
    candidate_search: CandidateSearchResult | None = None


class FinalMemo(BaseModel):
    title: str
    thesis: str
    executive_summary: list[str]
    evidence: list[str]
    risks_and_caveats: list[str]
    next_steps: list[str]


class CriticResult(BaseModel):
    approved_claims: list[str]
    flagged_claims: list[str]
    revised_memo: FinalMemo


class ArtifactRecord(BaseModel):
    artifact_id: str
    kind: str
    title: str
    path: str
    url: str


class OverlayBundle(BaseModel):
    macro: MacroOverlayResult | None = None
    earnings: EarningsOverlayResult | None = None
    filings: FilingsOverlayResult | None = None


class AnalysisWarning(BaseModel):
    code: str
    source: str
    severity: str = "warning"
    message: str


class AnalysisResponse(BaseModel):
    session_id: str
    normalized_portfolio: PortfolioInput
    baseline: BaselineAnalytics
    plan: AnalysisPlan
    dynamic_eda: DynamicEDAResult
    overlays: OverlayBundle
    agent_collaboration: AgentCollaboration | None = None
    final_memo: FinalMemo
    critic: CriticResult
    warnings: list[AnalysisWarning] = Field(default_factory=list)
    artifacts: list[ArtifactRecord]
