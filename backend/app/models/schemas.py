from __future__ import annotations

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
        return self


class PortfolioInput(BaseModel):
    holdings: list[Holding] = Field(min_length=1)
    question: str = Field(min_length=5)
    hypothetical_position: HypotheticalPosition | None = None
    benchmark: str = "SPY"
    lookback_days: int = Field(default=252, ge=63, le=756)

    @field_validator("benchmark")
    @classmethod
    def uppercase_benchmark(cls, value: str) -> str:
        return value.upper().strip()


class TickerMetadata(BaseModel):
    ticker: str
    company_name: str
    cik: str
    exchange: str | None = None
    sector: str | None = None
    asset_type: AssetType = AssetType.equity


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


class BaselineAnalytics(BaseModel):
    total_portfolio_value: float
    total_cost_basis: float | None = None
    benchmark_symbol: str
    risk_free_rate_used: float
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


class EarningsOverlayTickerResult(BaseModel):
    ticker: str
    company_name: str
    quarter: str | None = None
    tone: str
    findings: list[str]
    transcript_available: bool = True


class EarningsOverlayResult(BaseModel):
    companies: list[EarningsOverlayTickerResult]


class FilingsOverlayTickerResult(BaseModel):
    ticker: str
    company_name: str
    form_type: str | None = None
    filed_at: str | None = None
    findings: list[str]
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
    investigation_steps: list[str] = Field(default_factory=list)
    caveats: list[str] = Field(default_factory=list)


class DynamicEDAResult(BaseModel):
    workflow: str
    question_type: QuestionType
    findings: list[EDAFinding]
    tables: list[AnalysisTable] = Field(default_factory=list)
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


class AnalysisResponse(BaseModel):
    session_id: str
    normalized_portfolio: PortfolioInput
    baseline: BaselineAnalytics
    plan: AnalysisPlan
    dynamic_eda: DynamicEDAResult
    overlays: OverlayBundle
    final_memo: FinalMemo
    critic: CriticResult
    artifacts: list[ArtifactRecord]

