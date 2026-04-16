export interface TickerMetadata {
  ticker: string;
  company_name: string;
  cik: string;
  exchange?: string | null;
  sector?: string | null;
  asset_type: "Equity";
}

export interface TickerQuote {
  ticker: string;
  price: number;
  as_of: string;
}

export interface HoldingRow {
  ticker: string;
  shares: string;
  market_value: string;
  company_name?: string | null;
  sector?: string | null;
  cik?: string | null;
  exchange?: string | null;
  latest_price?: number | null;
  price_as_of?: string | null;
  last_edited?: "shares" | "market_value" | null;
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

export interface DataSourceReference {
  source: string;
  series: string;
  category: string;
  description: string;
  access: string;
  requires_api_key: boolean;
  status: string;
  url?: string | null;
  rationale?: string | null;
}

export interface NewsArticle {
  source: string;
  source_type: string;
  title: string;
  url: string;
  published_at?: string | null;
  domain?: string | null;
  summary?: string | null;
  sentiment?: number | null;
  relevance?: number | null;
  tickers: string[];
  topics: string[];
}

export interface NewsSourceStats {
  source: string;
  article_count: number;
  avg_sentiment?: number | null;
  latest_published_at?: string | null;
}

export interface NewsIntelResult {
  query: string;
  retrieval_sources: string[];
  articles: NewsArticle[];
  source_stats: NewsSourceStats[];
  dominant_topics: string[];
  caveats: string[];
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
  screening_summary?: string[];
}

export interface EntityFrequency {
  entity: string;
  count: number;
}

export interface TopicCluster {
  topic: string;
  mentions: number;
  keywords: string[];
  representative_text?: string | null;
}

export interface NLPTextSummary {
  sentiment_counts: Record<string, number>;
  keywords: string[];
  entities: EntityFrequency[];
  topic_clusters: TopicCluster[];
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
      event_date?: string | null;
      tone: string;
      findings: string[];
      nlp_summary?: NLPTextSummary | null;
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
      nlp_summary?: NLPTextSummary | null;
      filing_available: boolean;
    }>;
  } | null;
}

export interface ResearchAgenda {
  focus_areas: string[];
  analysis_ideas: string[];
  follow_up_questions: string[];
  overlay_requests: string[];
  candidate_search_guidance: string[];
  memo_watchouts: string[];
}

export interface ResearchSynthesis {
  integrated_insights: string[];
  confirmations: string[];
  tensions: string[];
  eda_implications: string[];
  candidate_search_implications: string[];
  memo_implications: string[];
}

export interface ArtifactRecord {
  artifact_id: string;
  kind: string;
  title: string;
  path: string;
  url: string;
}

export interface AnalysisWarning {
  code: string;
  source: string;
  severity: string;
  message: string;
}

export interface AnalysisResponse {
  session_id: string;
  normalized_portfolio: {
    holdings: Array<{
      ticker: string;
      shares: number;
      cost_basis?: number | null;
      company_name?: string | null;
      sector?: string | null;
      cik?: string | null;
      exchange?: string | null;
      asset_type: "Equity";
    }>;
    question: string;
    benchmark: string;
    lookback_days: number;
    start_date?: string | null;
    end_date?: string | null;
  };
  baseline: {
    total_portfolio_value: number;
    benchmark_symbol: string;
    risk_free_rate_used: number;
    effective_start_date: string;
    effective_end_date: string;
    effective_observations: number;
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
    relevant_tickers?: string[];
    macro_themes?: string[];
    preferred_data_sources?: string[];
    dataset_selection_rationale?: string[];
    optimization_preferences?: Array<{
      metric: string;
      direction: string;
      hard_constraint?: boolean;
    }>;
    comparison_universe?: string;
    comparison_sector_filters?: string[];
    comparison_ticker_limit?: number | null;
    investigation_steps: string[];
    caveats?: string[];
  };
  dynamic_eda: {
    workflow: string;
    question_type: string;
    findings: EDAFinding[];
    tables: AnalysisTable[];
    data_sources?: DataSourceReference[];
    news_intel?: NewsIntelResult | null;
    scenario_analysis?: ScenarioAnalytics | null;
    candidate_search?: CandidateSearchResult | null;
  };
  overlays: OverlayBundle;
  agent_collaboration?: {
    research_agenda?: ResearchAgenda | null;
    research_synthesis?: ResearchSynthesis | null;
  } | null;
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
  warnings: AnalysisWarning[];
  artifacts: ArtifactRecord[];
}
