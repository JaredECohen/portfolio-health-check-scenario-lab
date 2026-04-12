export interface TickerMetadata {
  ticker: string;
  company_name: string;
  cik: string;
  exchange?: string | null;
  sector?: string | null;
  asset_type: "Equity";
}

export interface HoldingRow {
  ticker: string;
  shares: string;
  cost_basis: string;
  company_name?: string | null;
  sector?: string | null;
  cik?: string | null;
  exchange?: string | null;
}

export interface HypotheticalInput {
  ticker: string;
  shares: string;
  target_weight: string;
  company_name?: string | null;
  sector?: string | null;
  cik?: string | null;
  exchange?: string | null;
}

export interface MetricCard {
  key: string;
  label: string;
  value: number | null;
  formatted: string;
}

export interface PositionSnapshot {
  ticker: string;
  company_name: string;
  sector?: string | null;
  shares: number;
  current_price: number;
  market_value: number;
  weight: number;
  trailing_return: number;
  cost_basis?: number | null;
  pnl_dollar?: number | null;
  pnl_pct?: number | null;
}

export interface SectorExposure {
  sector: string;
  market_value: number;
  weight: number;
}

export interface Contributor {
  ticker: string;
  company_name: string;
  return_pct: number;
  contribution_pct: number;
  weight: number;
}

export interface AnalysisTable {
  name: string;
  columns: string[];
  rows: Array<Record<string, string | number | null>>;
}

export interface EDAFinding {
  headline: string;
  evidence: string[];
  metrics: Record<string, string | number>;
  severity: string;
}

export interface ScenarioDelta {
  metric: string;
  before: number | null;
  after: number | null;
  delta: number | null;
}

export interface ScenarioAnalytics {
  label: string;
  hypothetical_position: {
    ticker: string;
    shares?: number | null;
    target_weight?: number | null;
  };
  before_metrics: MetricCard[];
  after_metrics: MetricCard[];
  deltas: ScenarioDelta[];
  before_sector_exposures: SectorExposure[];
  after_sector_exposures: SectorExposure[];
  before_positions: PositionSnapshot[];
  after_positions: PositionSnapshot[];
}

export interface CandidateRank {
  ticker: string;
  company_name: string;
  sector?: string | null;
  score: number;
  rationale: string[];
  deltas: ScenarioDelta[];
}

export interface CandidateSearchResult {
  objective: string;
  method: string;
  candidates: CandidateRank[];
}

export interface OverlayBundle {
  macro?: {
    question_focus: string;
    series_used: string[];
    findings: string[];
    portfolio_sensitivities: Record<string, number>;
    benchmark_sensitivities: Record<string, number>;
    caveats: string[];
  } | null;
  earnings?: {
    companies: Array<{
      ticker: string;
      company_name: string;
      quarter?: string | null;
      tone: string;
      findings: string[];
      transcript_available: boolean;
    }>;
  } | null;
  filings?: {
    companies: Array<{
      ticker: string;
      company_name: string;
      form_type?: string | null;
      filed_at?: string | null;
      findings: string[];
      filing_available: boolean;
    }>;
  } | null;
}

export interface ArtifactRecord {
  artifact_id: string;
  kind: string;
  title: string;
  path: string;
  url: string;
}

export interface AnalysisResponse {
  session_id: string;
  normalized_portfolio: {
    holdings: PositionSnapshot[];
    question: string;
    benchmark: string;
    lookback_days: number;
  };
  baseline: {
    total_portfolio_value: number;
    benchmark_symbol: string;
    risk_free_rate_used: number;
    metrics: MetricCard[];
    positions: PositionSnapshot[];
    sector_exposures: SectorExposure[];
    contributors: Contributor[];
    best_performers: Contributor[];
    worst_performers: Contributor[];
    correlation_matrix: Record<string, Record<string, number>>;
    performance_series: Array<{ date: string; portfolio_index: number; benchmark_index: number }>;
  };
  plan: {
    question_type: string;
    objective: string;
    explanation: string;
    dynamic_workflow: string;
    investigation_steps: string[];
  };
  dynamic_eda: {
    workflow: string;
    question_type: string;
    findings: EDAFinding[];
    tables: AnalysisTable[];
    scenario_analysis?: ScenarioAnalytics | null;
    candidate_search?: CandidateSearchResult | null;
  };
  overlays: OverlayBundle;
  final_memo: {
    title: string;
    thesis: string;
    executive_summary: string[];
    evidence: string[];
    risks_and_caveats: string[];
    next_steps: string[];
  };
  critic: {
    approved_claims: string[];
    flagged_claims: string[];
    revised_memo: AnalysisResponse["final_memo"];
  };
  artifacts: ArtifactRecord[];
}

