import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { ResultsPanel } from "./ResultsPanel";
import type { AnalysisResponse } from "../types";

function buildResult(): AnalysisResponse {
  return {
    session_id: "session-1",
    normalized_portfolio: {
      holdings: [
        {
          ticker: "AAPL",
          shares: 10,
          company_name: "Apple Inc",
          sector: "Technology",
          cik: "0000320193",
          exchange: "Nasdaq",
          asset_type: "Equity",
        },
      ],
      question: "What if I add MSFT?",
      benchmark: "SPY",
      lookback_days: 252,
      start_date: null,
      end_date: null,
    },
    baseline: {
      total_portfolio_value: 10000,
      benchmark_symbol: "SPY",
      risk_free_rate_used: 0.02,
      effective_start_date: "2024-01-02",
      effective_end_date: "2024-03-29",
      effective_observations: 55,
      metrics: [
        { key: "trailing_return", label: "Trailing Return", value: 0.12, formatted: "12.00%" },
      ],
      positions: [
        {
          ticker: "AAPL",
          company_name: "Apple Inc",
          sector: "Technology",
          shares: 10,
          current_price: 100,
          market_value: 1000,
          weight: 1,
          trailing_return: 0.12,
          cost_basis: 90,
          pnl_dollar: 100,
          pnl_pct: 0.11,
        },
      ],
      sector_exposures: [{ sector: "Technology", market_value: 1000, weight: 1 }],
      contributors: [
        {
          ticker: "AAPL",
          company_name: "Apple Inc",
          return_pct: 0.12,
          contribution_pct: 0.12,
          weight: 1,
        },
      ],
      best_performers: [],
      worst_performers: [],
      correlation_matrix: { AAPL: { AAPL: 1 } },
      performance_series: [
        { date: "2024-01-02", portfolio_index: 1, benchmark_index: 1 },
        { date: "2024-03-29", portfolio_index: 1.12, benchmark_index: 1.06 },
      ],
    },
    plan: {
      question_type: "what_if_addition",
      objective: "what_if_addition",
      explanation: "Scenario analysis selected.",
      dynamic_workflow: "what_if",
      preferred_data_sources: ["KEN_FRENCH_FF5_DAILY", "EARNINGS_TRANSCRIPTS"],
      dataset_selection_rationale: ["Factor data is needed to separate market exposure from stock-specific impact."],
      investigation_steps: ["Run the scenario model and compare the major delta metrics."],
    },
    dynamic_eda: {
      workflow: "what_if",
      question_type: "what_if_addition",
      findings: [],
      tables: [],
      scenario_analysis: {
        label: "Add MSFT",
        hypothetical_position: { ticker: "MSFT", target_weight: 0.05 },
        before_metrics: [
          { key: "annualized_volatility", label: "Annualized Volatility", value: 0.2, formatted: "20.00%" },
          { key: "sharpe_ratio", label: "Sharpe Ratio", value: 1.1, formatted: "1.10" },
        ],
        after_metrics: [
          { key: "annualized_volatility", label: "Annualized Volatility", value: 0.22, formatted: "22.00%" },
          { key: "sharpe_ratio", label: "Sharpe Ratio", value: 1.2, formatted: "1.20" },
        ],
        deltas: [
          { metric: "annualized_volatility", before: 0.2, after: 0.22, delta: 0.02 },
          { metric: "sharpe_ratio", before: 1.1, after: 1.2, delta: 0.1 },
        ],
        before_sector_exposures: [{ sector: "Technology", market_value: 1000, weight: 1 }],
        after_sector_exposures: [{ sector: "Technology", market_value: 1050, weight: 1 }],
        before_positions: [
          {
            ticker: "AAPL",
            company_name: "Apple Inc",
            sector: "Technology",
            shares: 10,
            current_price: 100,
            market_value: 1000,
            weight: 1,
            trailing_return: 0.12,
            cost_basis: 90,
            pnl_dollar: 100,
            pnl_pct: 0.11,
          },
        ],
        after_positions: [
          {
            ticker: "AAPL",
            company_name: "Apple Inc",
            sector: "Technology",
            shares: 10,
            current_price: 100,
            market_value: 1000,
            weight: 0.95,
            trailing_return: 0.12,
            cost_basis: 90,
            pnl_dollar: 100,
            pnl_pct: 0.11,
          },
          {
            ticker: "MSFT",
            company_name: "Microsoft Corp",
            sector: "Technology",
            shares: 1,
            current_price: 50,
            market_value: 50,
            weight: 0.05,
            trailing_return: 0.1,
            cost_basis: null,
            pnl_dollar: null,
            pnl_pct: null,
          },
        ],
      },
      candidate_search: null,
    },
    overlays: {
      earnings: {
        companies: [
          {
            ticker: "AAPL",
            company_name: "Apple Inc",
            quarter: "2024Q2",
            event_date: "2024-05-02",
            tone: "mixed",
            findings: ["Guidance: Example finding"],
            nlp_summary: {
              sentiment_counts: { positive: 3, cautious: 1 },
              keywords: ["guidance", "demand", "margin"],
              entities: [{ entity: "Apple", count: 2 }],
              topic_clusters: [
                {
                  topic: "guidance",
                  mentions: 2,
                  keywords: ["guidance", "outlook"],
                  representative_text: "We updated our guidance for the year.",
                },
              ],
            },
            transcript_available: true,
          },
        ],
      },
    },
    agent_collaboration: {
      research_agenda: {
        focus_areas: ["Check whether the improvement is factor-driven or stock-specific."],
        analysis_ideas: ["Compare the scenario outcome to a factor-attribution readout."],
        follow_up_questions: ["Does the added name improve Sharpe because of momentum exposure?"],
        overlay_requests: ["Review the latest earnings tone before finalizing the recommendation."],
        candidate_search_guidance: ["Favor names that improve Sharpe without raising beta materially."],
        memo_watchouts: ["Do not overstate causality from the historical window."],
      },
      research_synthesis: {
        integrated_insights: ["The first-pass EDA and overlay both point to improving risk-adjusted return."],
        confirmations: ["Scenario Sharpe improved in the selected window."],
        tensions: ["The overlay suggests guidance risk is still present."],
        eda_implications: ["Keep the factor attribution in view when describing the scenario."],
        candidate_search_implications: ["Prefer names with positive momentum and lower portfolio correlation."],
        memo_implications: ["Frame the result as conditional on the trailing sample."],
      },
    },
    final_memo: {
      title: "Memo",
      thesis: "Thesis",
      executive_summary: [],
      evidence: [],
      risks_and_caveats: [],
      next_steps: [],
    },
    critic: {
      approved_claims: [],
      flagged_claims: [],
      revised_memo: {
        title: "Memo",
        thesis: "Thesis",
        executive_summary: [],
        evidence: [],
        risks_and_caveats: [],
        next_steps: [],
      },
    },
    warnings: [
      {
        code: "overlay_unavailable",
        source: "research_overlay",
        severity: "warning",
        message: "Overlay failed but baseline remained available.",
      },
      {
        code: "effective_end_shifted",
        source: "sample_window",
        severity: "info",
        message: "Effective analysis end date shifted to 2024-03-29 because full price history was not available through 2024-04-01.",
      },
    ],
    artifacts: [],
  };
}

describe("ResultsPanel", () => {
  it("renders warnings and effective sample information", () => {
    render(<ResultsPanel result={buildResult()} />);

    expect(screen.getByText(/effective sample:/i)).toHaveTextContent(
      "Effective sample: 2024-01-02 to 2024-03-29 (55 observations)",
    );
    expect(screen.getByText("Warnings")).toBeInTheDocument();
    expect(screen.getByText("Overlay failed but baseline remained available.")).toBeInTheDocument();
    expect(screen.getByText("Research overlays")).toBeInTheDocument();
    expect(screen.getByText("Sample window")).toBeInTheDocument();
    expect(screen.getByText("overlay_unavailable")).toBeInTheDocument();
    expect(screen.getByText("effective_end_shifted")).toBeInTheDocument();
    expect(screen.getByText("Event date: 2024-05-02")).toBeInTheDocument();
    expect(screen.getByText(/NLP summary: positive 3, cautious 1/i)).toBeInTheDocument();
    expect(screen.getByText(/Keywords: guidance, demand, margin/i)).toBeInTheDocument();
  });

  it("uses desirability-aware styling for scenario deltas", () => {
    render(<ResultsPanel result={buildResult()} />);

    expect(screen.getByText("+2.0000%")).toHaveClass("scenario-delta--bad");
    expect(screen.getByText("+0.1000")).toHaveClass("scenario-delta--good");
  });

  it("opens the analysis trace modal with agent handoff details", () => {
    render(<ResultsPanel result={buildResult()} />);

    fireEvent.click(screen.getByRole("button", { name: /follow analysis trace/i }));

    expect(screen.getByRole("dialog", { name: /agent analysis trace/i })).toBeInTheDocument();
    expect(screen.getByText("Planner Agent")).toBeInTheDocument();
    expect(screen.getByText("Dynamic EDA Agent")).toBeInTheDocument();
    expect(screen.getByText("Research Director Agent")).toBeInTheDocument();
    expect(screen.getByText(/does not expose private hidden chain-of-thought/i)).toBeInTheDocument();
    expect(screen.getByText("Requested source: KEN_FRENCH_FF5_DAILY")).toBeInTheDocument();
  });
});
