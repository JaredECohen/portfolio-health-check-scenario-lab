import { Fragment, useState } from "react";
import type { AnalysisResponse, MetricCard, NLPTextSummary } from "../types";

type TraceSection = {
  label: string;
  items: string[];
};

type TraceStep = {
  agent: string;
  stage: string;
  summary: string;
  sections: TraceSection[];
};

function currency(value: number) {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(value);
}

function formatPercent(value: number) {
  return `${(value * 100).toFixed(4)}%`;
}

function isPercentageKey(key: string) {
  return [
    "return",
    "weight",
    "volatility",
    "drawdown",
    "share",
    "contribution",
    "excess",
    "hit_rate",
    "pnl_pct",
  ].some((token) => key.includes(token));
}

function formatNumericValue(key: string, value: number) {
  if (isPercentageKey(key)) {
    return formatPercent(value);
  }
  return value.toFixed(4);
}

function formatDeltaValue(key: string, value: number) {
  const formatted = formatNumericValue(key, Math.abs(value));
  if (value > 0) {
    return `+${formatted}`;
  }
  if (value < 0) {
    return `-${formatted}`;
  }
  return formatted;
}

function metricLabelMap(metrics: MetricCard[]) {
  return Object.fromEntries(metrics.map((metric) => [metric.key, metric.label]));
}

function isImprovement(metric: string, delta: number | null) {
  if (delta === null || delta === 0) {
    return null;
  }
  const lowerIsBetter = ["volatility", "drawdown", "correlation", "herfindahl", "top3", "beta"].some(
    (token) => metric.includes(token),
  );
  if (lowerIsBetter) {
    return delta < 0;
  }
  return delta > 0;
}

function desirabilityToneClass(metric: string, delta: number | null) {
  const improvement = isImprovement(metric, delta);
  if (improvement === null) {
    return "scenario-delta scenario-delta--flat";
  }
  return improvement ? "scenario-delta scenario-delta--good" : "scenario-delta scenario-delta--bad";
}

function topSectorChanges(result: NonNullable<AnalysisResponse["dynamic_eda"]["scenario_analysis"]>) {
  const before = new Map(result.before_sector_exposures.map((item) => [item.sector, item.weight]));
  const after = new Map(result.after_sector_exposures.map((item) => [item.sector, item.weight]));
  return Array.from(new Set([...before.keys(), ...after.keys()]))
    .map((sector) => ({
      sector,
      before: before.get(sector) ?? 0,
      after: after.get(sector) ?? 0,
      delta: (after.get(sector) ?? 0) - (before.get(sector) ?? 0),
    }))
    .sort((left, right) => Math.abs(right.delta) - Math.abs(left.delta))
    .slice(0, 5);
}

function renderMetric(metric: MetricCard) {
  const renderedValue =
    metric.value !== null ? formatNumericValue(metric.key, metric.value) : metric.formatted;
  return (
    <article key={metric.key} className="metric-card">
      <span>{metric.label}</span>
      <strong>{renderedValue}</strong>
    </article>
  );
}

function correlationColor(value: number) {
  const clamped = Math.max(-1, Math.min(1, value));
  if (clamped >= 0) {
    const alpha = 0.18 + clamped * 0.42;
    return `rgba(217, 130, 43, ${alpha})`;
  }
  const alpha = 0.18 + Math.abs(clamped) * 0.42;
  return `rgba(25, 101, 176, ${alpha})`;
}

function buildPolylinePoints(points: Array<{ x: number; y: number }>) {
  return points.map((point) => `${point.x},${point.y}`).join(" ");
}

function renderPerformanceChart(
  series: AnalysisResponse["baseline"]["performance_series"],
  benchmarkSymbol: string,
) {
  if (series.length === 0) {
    return <p className="chart-empty">Performance series unavailable.</p>;
  }

  const width = 560;
  const height = 240;
  const padding = 20;
  const values = series.flatMap((point) => [point.portfolio_index, point.benchmark_index]);
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const valueRange = maxValue - minValue || 1;
  const xStep = series.length === 1 ? 0 : (width - padding * 2) / (series.length - 1);
  const projectY = (value: number) =>
    height - padding - ((value - minValue) / valueRange) * (height - padding * 2);
  const portfolioPoints = series.map((point, index) => ({
    x: padding + index * xStep,
    y: projectY(point.portfolio_index),
  }));
  const benchmarkPoints = series.map((point, index) => ({
    x: padding + index * xStep,
    y: projectY(point.benchmark_index),
  }));
  const startPoint = series[0];
  const endPoint = series[series.length - 1];

  return (
    <div className="native-chart">
      <div className="native-chart__legend">
        <span>
          <i className="native-chart__swatch native-chart__swatch--portfolio" />
          Portfolio
        </span>
        <span>
          <i className="native-chart__swatch native-chart__swatch--benchmark" />
          {benchmarkSymbol}
        </span>
      </div>
      <svg
        className="native-chart__svg"
        viewBox={`0 0 ${width} ${height}`}
        role="img"
        aria-label="Portfolio and benchmark cumulative performance"
      >
        <line x1={padding} y1={padding} x2={padding} y2={height - padding} className="native-chart__axis" />
        <line
          x1={padding}
          y1={height - padding}
          x2={width - padding}
          y2={height - padding}
          className="native-chart__axis"
        />
        <polyline
          fill="none"
          stroke="var(--ink)"
          strokeWidth="3"
          points={buildPolylinePoints(portfolioPoints)}
        />
        <polyline
          fill="none"
          stroke="var(--accent)"
          strokeWidth="3"
          points={buildPolylinePoints(benchmarkPoints)}
        />
      </svg>
      <div className="native-chart__labels">
        <span>{startPoint.date}</span>
        <span>
          Portfolio {startPoint.portfolio_index.toFixed(2)} -&gt; {endPoint.portfolio_index.toFixed(2)}
        </span>
        <span>{endPoint.date}</span>
      </div>
    </div>
  );
}

function renderSectorExposureBars(sectors: AnalysisResponse["baseline"]["sector_exposures"]) {
  if (sectors.length === 0) {
    return <p className="chart-empty">Sector exposure unavailable.</p>;
  }

  return (
    <div className="bar-chart">
      {sectors.map((sector) => (
        <article className="bar-chart__row" key={sector.sector}>
          <div className="bar-chart__header">
            <span>{sector.sector}</span>
            <strong>{formatPercent(sector.weight)}</strong>
          </div>
          <div className="bar-chart__track">
            <div className="bar-chart__fill" style={{ width: `${sector.weight * 100}%` }} />
          </div>
        </article>
      ))}
    </div>
  );
}

function renderCorrelationHeatmap(matrix: AnalysisResponse["baseline"]["correlation_matrix"]) {
  const tickers = Object.keys(matrix);
  if (tickers.length === 0) {
    return null;
  }

  return (
    <div className="correlation-heatmap">
      <div
        className="correlation-heatmap__grid"
        style={{ gridTemplateColumns: `120px repeat(${tickers.length}, minmax(72px, 1fr))` }}
      >
        <div className="correlation-heatmap__corner">Corr</div>
        {tickers.map((ticker) => (
          <div key={`column-${ticker}`} className="correlation-heatmap__axis correlation-heatmap__axis--column">
            {ticker}
          </div>
        ))}
        {tickers.map((rowTicker) => (
          <Fragment key={`heatmap-row-${rowTicker}`}>
            <div key={`row-${rowTicker}`} className="correlation-heatmap__axis correlation-heatmap__axis--row">
              {rowTicker}
            </div>
            {tickers.map((columnTicker) => {
              const value = matrix[rowTicker]?.[columnTicker] ?? 0;
              return (
                <div
                  key={`${rowTicker}-${columnTicker}`}
                  className="correlation-heatmap__cell"
                  style={{ background: correlationColor(value) }}
                  title={`${rowTicker} / ${columnTicker}: ${value.toFixed(2)}`}
                >
                  {value.toFixed(2)}
                </div>
              );
            })}
          </Fragment>
        ))}
      </div>
      <div className="correlation-heatmap__legend">
        <span>Negative</span>
        <div className="correlation-heatmap__legend-bar" />
        <span>Positive</span>
      </div>
    </div>
  );
}

function formatCellValue(column: string, value: string | number | null) {
  if (value === null) {
    return "";
  }
  if (typeof value !== "number") {
    return String(value);
  }
  if (column.includes("bps")) {
    return value.toFixed(4);
  }
  return formatNumericValue(column, value);
}

function renderNlpSummary(summary: NLPTextSummary | null | undefined) {
  if (!summary) {
    return null;
  }

  return (
    <div className="overlay-nlp-summary">
      <p>
        NLP summary: positive {summary.sentiment_counts.positive ?? 0}, cautious{" "}
        {summary.sentiment_counts.cautious ?? 0}
      </p>
      {summary.keywords.length > 0 ? <p>Keywords: {summary.keywords.join(", ")}</p> : null}
      {summary.entities.length > 0 ? (
        <p>Entities: {summary.entities.map((item) => `${item.entity} (${item.count})`).join(", ")}</p>
      ) : null}
      {summary.topic_clusters.length > 0 ? (
        <ul>
          {summary.topic_clusters.map((cluster) => (
            <li key={cluster.topic}>
              {cluster.topic} ({cluster.mentions}): {cluster.keywords.join(", ")}
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}

function warningSourceLabel(source: string) {
  const labels: Record<string, string> = {
    sample_window: "Sample window",
    research_overlay: "Research overlays",
    candidate_search: "Candidate search",
  };
  return labels[source] ?? source.split("_").join(" ");
}

function warningSeverityLabel(severity: string) {
  return severity.charAt(0).toUpperCase() + severity.slice(1);
}

function humanize(value: string) {
  const normalized = value.replace(/_/g, " ").trim();
  return normalized.charAt(0).toUpperCase() + normalized.slice(1);
}

function traceItems(items: Array<string | null | undefined>) {
  return items
    .map((item) => item?.trim())
    .filter((item): item is string => Boolean(item));
}

function summarizeOptimizationPreferences(result: AnalysisResponse) {
  return (result.plan.optimization_preferences ?? []).map((preference) => {
    const constraint = preference.hard_constraint ? " as a hard constraint" : "";
    return `${humanize(preference.direction)} ${humanize(preference.metric)}${constraint}.`;
  });
}

function summarizeFinding(finding: AnalysisResponse["dynamic_eda"]["findings"][number]) {
  const evidence = finding.evidence.slice(0, 2).join(" ");
  return evidence ? `${finding.headline} ${evidence}` : finding.headline;
}

function summarizeDataSource(source: NonNullable<AnalysisResponse["dynamic_eda"]["data_sources"]>[number]) {
  return `${source.series}: ${source.rationale || source.description}`;
}

function summarizeScenarioDeltas(result: AnalysisResponse) {
  const scenario = result.dynamic_eda.scenario_analysis;
  if (!scenario) {
    return [];
  }
  return [...scenario.deltas]
    .sort((left, right) => Math.abs(right.delta ?? 0) - Math.abs(left.delta ?? 0))
    .slice(0, 5)
    .map((delta) => {
      if (delta.delta === null) {
        return `${humanize(delta.metric)} had no usable delta.`;
      }
      return `${humanize(delta.metric)} changed by ${formatDeltaValue(delta.metric, delta.delta)}.`;
    });
}

function buildAnalysisTrace(result: AnalysisResponse): TraceStep[] {
  const plannerSections: TraceSection[] = [
    {
      label: "Problem framing",
      items: traceItems([
        `Question: ${result.normalized_portfolio.question}`,
        `Workflow: ${humanize(result.plan.dynamic_workflow)}`,
        result.plan.explanation,
      ]),
    },
    {
      label: "Data needed",
      items: traceItems([
        ...(result.plan.relevant_tickers?.length ? [`Explicit tickers: ${result.plan.relevant_tickers.join(", ")}`] : []),
        ...(result.plan.macro_themes?.length ? [`Macro themes: ${result.plan.macro_themes.join(", ")}`] : []),
        ...(result.plan.preferred_data_sources?.map((source) => `Requested source: ${source}`) ?? []),
        ...(result.plan.dataset_selection_rationale ?? []),
        result.plan.comparison_universe ? `Comparison universe: ${humanize(result.plan.comparison_universe)}` : null,
        result.plan.comparison_sector_filters?.length
          ? `Sector filters: ${result.plan.comparison_sector_filters.join(", ")}`
          : null,
        result.plan.comparison_ticker_limit ? `Universe cap: ${result.plan.comparison_ticker_limit} names.` : null,
      ]),
    },
    {
      label: "Planned analysis",
      items: traceItems([
        ...result.plan.investigation_steps,
        ...summarizeOptimizationPreferences(result),
        ...(result.plan.caveats ?? []).map((item) => `Planner caveat: ${item}`),
      ]),
    },
  ].filter((section) => section.items.length > 0);

  const steps: TraceStep[] = [
    {
      agent: "Planner Agent",
      stage: "Problem framing",
      summary: `Classified the request as ${humanize(result.plan.question_type)} with a ${humanize(result.plan.objective)} objective.`,
      sections: plannerSections,
    },
  ];

  const edaSections: TraceSection[] = [
    {
      label: "Data actually used",
      items: traceItems([
        ...(result.dynamic_eda.data_sources?.map(summarizeDataSource) ?? []),
        result.dynamic_eda.news_intel
          ? `News query: ${result.dynamic_eda.news_intel.query} via ${result.dynamic_eda.news_intel.retrieval_sources.join(", ")}.`
          : null,
      ]),
    },
    {
      label: "Analysis results",
      items: result.dynamic_eda.findings.map(summarizeFinding),
    },
    {
      label: "Structured outputs",
      items: traceItems([
        ...result.dynamic_eda.tables.map((table) => `${table.name} (${table.rows.length} rows).`),
        result.dynamic_eda.candidate_search ? `Candidate search returned ${result.dynamic_eda.candidate_search.candidates.length} ranked names.` : null,
        result.dynamic_eda.scenario_analysis ? `Scenario model ran for ${result.dynamic_eda.scenario_analysis.label}.` : null,
      ]),
    },
  ].filter((section) => section.items.length > 0);

  steps.push({
    agent: "Dynamic EDA Agent",
    stage: "Initial analysis",
    summary: `Ran the ${humanize(result.dynamic_eda.workflow)} workflow across ${result.baseline.effective_observations} observations and produced ${result.dynamic_eda.findings.length} core findings.`,
    sections: edaSections,
  });

  if (result.agent_collaboration?.research_agenda) {
    const agenda = result.agent_collaboration.research_agenda;
    steps.push({
      agent: "Research Director Agent",
      stage: "Follow-up agenda",
      summary: "Turned the first-pass EDA into a deeper investigation plan and subsequent hypotheses.",
      sections: [
        { label: "Focus areas", items: agenda.focus_areas },
        { label: "Next analyses", items: agenda.analysis_ideas },
        { label: "Follow-up questions", items: agenda.follow_up_questions },
        { label: "Overlay requests", items: agenda.overlay_requests },
        { label: "Candidate search guidance", items: agenda.candidate_search_guidance },
        { label: "Memo watchouts", items: agenda.memo_watchouts },
      ].filter((section) => section.items.length > 0),
    });
  }

  const overlaySections: TraceSection[] = [];
  if (result.dynamic_eda.news_intel) {
    overlaySections.push({
      label: "News and narrative evidence",
      items: traceItems([
        `Dominant topics: ${result.dynamic_eda.news_intel.dominant_topics.join(", ") || "none identified"}.`,
        ...result.dynamic_eda.news_intel.caveats,
      ]),
    });
  }
  if (result.overlays.macro) {
    overlaySections.push({
      label: "Macro overlay",
      items: traceItems([
        `Question focus: ${result.overlays.macro.question_focus}`,
        ...result.overlays.macro.findings,
        ...(result.overlays.macro.caveats ?? []).map((item) => `Macro caveat: ${item}`),
      ]),
    });
  }
  if (result.overlays.earnings) {
    overlaySections.push({
      label: "Earnings overlay",
      items: result.overlays.earnings.companies.flatMap((company) =>
        traceItems([
          `${company.ticker}: ${company.findings.join(" ")}`,
          company.nlp_summary?.keywords.length ? `${company.ticker} keywords: ${company.nlp_summary.keywords.join(", ")}` : null,
        ]),
      ),
    });
  }
  if (result.overlays.filings) {
    overlaySections.push({
      label: "Filings overlay",
      items: result.overlays.filings.companies.flatMap((company) =>
        traceItems([
          `${company.ticker}: ${company.findings.join(" ")}`,
          company.nlp_summary?.keywords.length ? `${company.ticker} keywords: ${company.nlp_summary.keywords.join(", ")}` : null,
        ]),
      ),
    });
  }
  if (overlaySections.length > 0) {
    steps.push({
      agent: "Overlay Agents",
      stage: "External evidence",
      summary: "Brought macro, news, earnings, and filings evidence back into the quantitative analysis.",
      sections: overlaySections,
    });
  }

  if (result.agent_collaboration?.research_synthesis) {
    const synthesis = result.agent_collaboration.research_synthesis;
    steps.push({
      agent: "Research Synthesis Agent",
      stage: "Interpretation and next hypotheses",
      summary: "Merged the first-pass EDA and overlays into confirmations, tensions, and the next round of interpretation.",
      sections: [
        { label: "Integrated insights", items: synthesis.integrated_insights },
        { label: "Confirmations", items: synthesis.confirmations },
        { label: "Tensions", items: synthesis.tensions },
        { label: "EDA implications", items: synthesis.eda_implications },
        { label: "Candidate implications", items: synthesis.candidate_search_implications },
        { label: "Memo implications", items: synthesis.memo_implications },
      ].filter((section) => section.items.length > 0),
    });
  }

  if (result.dynamic_eda.candidate_search || result.dynamic_eda.scenario_analysis) {
    steps.push({
      agent: "Decision Analysis Agents",
      stage: "Simulation and ranking",
      summary: "Converted the research into portfolio-change analysis, simulations, and ranked recommendations.",
      sections: [
        {
          label: "Candidate search",
          items: result.dynamic_eda.candidate_search
            ? traceItems([
                result.dynamic_eda.candidate_search.method,
                ...(result.dynamic_eda.candidate_search.screening_summary ?? []),
                ...result.dynamic_eda.candidate_search.candidates.slice(0, 3).map(
                  (candidate) =>
                    `${candidate.ticker} scored ${candidate.score.toFixed(3)}. ${candidate.rationale.slice(0, 2).join(" ")}`,
                ),
              ])
            : [],
        },
        {
          label: "Scenario results",
          items: result.dynamic_eda.scenario_analysis
            ? traceItems([
                `${result.dynamic_eda.scenario_analysis.label}.`,
                ...summarizeScenarioDeltas(result),
              ])
            : [],
        },
      ].filter((section) => section.items.length > 0),
    });
  }

  steps.push({
    agent: "Writer and Critic Agents",
    stage: "Final interpretation and challenge",
    summary: "Turned the evidence into a memo, then challenged the claims before the result was returned.",
    sections: [
      {
        label: "Writer interpretation",
        items: traceItems([result.final_memo.thesis, ...result.final_memo.executive_summary]),
      },
      {
        label: "Evidence carried forward",
        items: result.final_memo.evidence,
      },
      {
        label: "Risks and caveats",
        items: result.final_memo.risks_and_caveats,
      },
      {
        label: "Critic review",
        items: traceItems([
          ...result.critic.approved_claims.map((item) => `Approved: ${item}`),
          ...result.critic.flagged_claims.map((item) => `Flagged: ${item}`),
        ]),
      },
    ].filter((section) => section.items.length > 0),
  });

  return steps;
}

function renderWarnings(warnings: AnalysisResponse["warnings"]) {
  const grouped = warnings.reduce<Map<string, AnalysisResponse["warnings"]>>((accumulator, warning) => {
    const current = accumulator.get(warning.source) ?? [];
    current.push(warning);
    accumulator.set(warning.source, current);
    return accumulator;
  }, new Map());

  return (
    <section className="panel">
      <div className="section-header">
        <div>
          <p className="eyebrow">Run State</p>
          <h3>Warnings</h3>
        </div>
      </div>
      <div className="warning-groups">
        {Array.from(grouped.entries()).map(([source, items]) => (
          <article className="warning-group" key={source}>
            <div className="warning-group__header">
              <strong>{warningSourceLabel(source)}</strong>
              <span>{items.length}</span>
            </div>
            <ul className="warning-list">
              {items.map((warning) => (
                <li key={`${warning.code}-${warning.message}`} className={`warning-item warning-item--${warning.severity}`}>
                  <div className="warning-item__meta">
                    <span className="warning-item__badge">{warningSeverityLabel(warning.severity)}</span>
                    <code>{warning.code}</code>
                  </div>
                  <p>{warning.message}</p>
                </li>
              ))}
            </ul>
          </article>
        ))}
      </div>
    </section>
  );
}

export function ResultsPanel({ result }: { result: AnalysisResponse | null }) {
  const [isTraceOpen, setIsTraceOpen] = useState(false);

  if (!result) {
    return (
      <section className="results-empty">
        <p>Run an analysis to generate the baseline health check, dynamic investigation, overlays, memo, and native charts.</p>
      </section>
    );
  }

  const traceSteps = buildAnalysisTrace(result);

  return (
    <section className="results-shell">
      <div className="results-header">
        <div>
          <p className="eyebrow">Analysis Session</p>
          <h2>{result.final_memo.title}</h2>
          <p>{result.plan.explanation}</p>
          <p>
            Effective sample: {result.baseline.effective_start_date} to {result.baseline.effective_end_date} (
            {result.baseline.effective_observations} observations)
          </p>
          {result.normalized_portfolio.start_date || result.normalized_portfolio.end_date ? (
            <p>
              Window: {result.normalized_portfolio.start_date || "earliest available"} to{" "}
              {result.normalized_portfolio.end_date || "latest available"}
            </p>
          ) : null}
        </div>
        <div className="results-header__aside">
          <div className="results-total">
            <span>Total Portfolio Value</span>
            <strong>{currency(result.baseline.total_portfolio_value)}</strong>
          </div>
          <button type="button" className="ghost-button results-header__action" onClick={() => setIsTraceOpen(true)}>
            Follow Analysis Trace
          </button>
        </div>
      </div>

      {isTraceOpen ? (
        <div className="trace-modal" role="presentation" onClick={() => setIsTraceOpen(false)}>
          <div
            className="trace-modal__card"
            role="dialog"
            aria-modal="true"
            aria-labelledby="analysis-trace-title"
            onClick={(event) => event.stopPropagation()}
          >
            <div className="trace-modal__header">
              <div>
                <p className="eyebrow">Explainability</p>
                <h3 id="analysis-trace-title">Agent Analysis Trace</h3>
                <p className="trace-modal__note">
                  This is a structured summary of the agents&apos; plans, hypotheses, evidence, handoffs, and critiques.
                  It does not expose private hidden chain-of-thought.
                </p>
              </div>
              <button type="button" className="ghost-button" onClick={() => setIsTraceOpen(false)}>
                Close
              </button>
            </div>
            <div className="trace-list">
              {traceSteps.map((step, index) => (
                <article className="trace-step" key={`${step.agent}-${step.stage}`}>
                  <div className="trace-step__header">
                    <span className="trace-step__index">Step {index + 1}</span>
                    <div>
                      <h4>{step.agent}</h4>
                      <p>{step.stage}</p>
                    </div>
                  </div>
                  <p className="trace-step__summary">{step.summary}</p>
                  <div className="trace-step__sections">
                    {step.sections.map((section) => (
                      <section className="trace-step__section" key={`${step.agent}-${section.label}`}>
                        <span className="field-label">{section.label}</span>
                        {section.items.length === 1 ? (
                          <p>{section.items[0]}</p>
                        ) : (
                          <ul>
                            {section.items.map((item) => (
                              <li key={`${section.label}-${item}`}>{item}</li>
                            ))}
                          </ul>
                        )}
                      </section>
                    ))}
                  </div>
                </article>
              ))}
            </div>
          </div>
        </div>
      ) : null}

      {result.warnings.length > 0 ? renderWarnings(result.warnings) : null}

      <div className="metrics-grid">{result.baseline.metrics.map(renderMetric)}</div>

      <section className="panel">
        <h3>Performance Trend</h3>
        {renderPerformanceChart(result.baseline.performance_series, result.baseline.benchmark_symbol)}
      </section>

      <section className="panel">
        <h3>Dynamic EDA Findings</h3>
        <div className="finding-list">
          {result.dynamic_eda.findings.map((finding) => (
            <article className="finding-card" key={finding.headline}>
              <h4>{finding.headline}</h4>
              <ul>
                {finding.evidence.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
            </article>
          ))}
        </div>
      </section>

      {result.dynamic_eda.data_sources && result.dynamic_eda.data_sources.length > 0 ? (
        <section className="panel">
          <h3>Routed Data Sources</h3>
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Series</th>
                  <th>Source</th>
                  <th>Category</th>
                  <th>Status</th>
                  <th>Rationale</th>
                </tr>
              </thead>
              <tbody>
                {result.dynamic_eda.data_sources.map((source) => (
                  <tr key={source.series}>
                    <td>{source.series}</td>
                    <td>{source.source}</td>
                    <td>{source.category}</td>
                    <td>{source.requires_api_key ? `${source.status} (API key)` : source.status}</td>
                    <td>{source.rationale || source.description}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}

      {result.dynamic_eda.news_intel ? (
        <section className="panel">
          <h3>News Retrieval Layer</h3>
          <p>Query: {result.dynamic_eda.news_intel.query}</p>
          <p>Sources: {result.dynamic_eda.news_intel.retrieval_sources.join(", ")}</p>
          {result.dynamic_eda.news_intel.dominant_topics.length > 0 ? (
            <p>Dominant topics: {result.dynamic_eda.news_intel.dominant_topics.join(", ")}</p>
          ) : null}
          {result.dynamic_eda.news_intel.caveats.length > 0 ? (
            <ul>
              {result.dynamic_eda.news_intel.caveats.map((item) => (
                <li key={item}>{item}</li>
              ))}
            </ul>
          ) : null}
        </section>
      ) : null}

      <section className="panel">
        <h3>Positions</h3>
        <div className="table-shell">
          <table>
            <thead>
              <tr>
                <th>Ticker</th>
                <th>Company</th>
                <th>Sector</th>
                <th>Weight</th>
                <th>Return</th>
                <th>Value</th>
              </tr>
            </thead>
            <tbody>
              {result.baseline.positions.map((position) => (
                <tr key={position.ticker}>
                  <td>{position.ticker}</td>
                  <td>{position.company_name}</td>
                  <td>{position.sector || "Unknown"}</td>
                  <td>{formatPercent(position.weight)}</td>
                  <td>{formatPercent(position.trailing_return)}</td>
                  <td>{currency(position.market_value)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel two-column">
        <div>
          <h3>Sector Exposure</h3>
          {renderSectorExposureBars(result.baseline.sector_exposures)}
        </div>
        <div>
          <h3>Performance Drivers</h3>
          <ul className="simple-list">
            {result.baseline.contributors.slice(0, 5).map((item) => (
              <li key={item.ticker}>
                <span>{item.ticker}</span>
                <strong>{formatPercent(item.contribution_pct)}</strong>
              </li>
            ))}
          </ul>
        </div>
      </section>

      <section className="panel">
        <h3>Correlation Heatmap</h3>
        {renderCorrelationHeatmap(result.baseline.correlation_matrix)}
      </section>

      {result.dynamic_eda.tables.length > 0 ? (
        <section className="panel">
          <h3>Investigation Tables</h3>
          {result.dynamic_eda.tables.map((table) => (
            <div className="table-shell" key={table.name}>
              <h4>{table.name}</h4>
              <table>
                <thead>
                  <tr>
                    {table.columns.map((column) => (
                      <th key={column}>{column}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {table.rows.map((row, index) => (
                    <tr key={`${table.name}-${index}`}>
                      {table.columns.map((column) => (
                        <td key={column}>{formatCellValue(column, row[column] ?? null)}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ))}
        </section>
      ) : null}

      {result.dynamic_eda.candidate_search ? (
        <section className="panel">
          <h3>Candidate Additions</h3>
          <p>{result.dynamic_eda.candidate_search.method}</p>
          {result.dynamic_eda.candidate_search.screening_summary?.length ? (
            <ul>
              {result.dynamic_eda.candidate_search.screening_summary.map((line) => (
                <li key={line}>{line}</li>
              ))}
            </ul>
          ) : null}
          <div className="candidate-grid">
            {result.dynamic_eda.candidate_search.candidates.map((candidate) => (
              <article className="candidate-card" key={candidate.ticker}>
                <h4>{candidate.ticker}</h4>
                <p>{candidate.company_name}</p>
                <strong>Score {candidate.score.toFixed(3)}</strong>
                <ul>
                  {candidate.rationale.map((line) => (
                    <li key={line}>{line}</li>
                  ))}
                </ul>
              </article>
            ))}
          </div>
        </section>
      ) : null}

      {result.dynamic_eda.scenario_analysis ? (
        <section className="panel">
          <h3>Scenario Comparison</h3>
          {(() => {
            const scenario = result.dynamic_eda.scenario_analysis;
            const labels = metricLabelMap(scenario.before_metrics);
            const sectorChanges = topSectorChanges(scenario);
            const weightChanges = scenario.after_positions
              .map((afterPosition) => {
                const beforePosition = scenario.before_positions.find(
                  (position) => position.ticker === afterPosition.ticker,
                );
                return {
                  ticker: afterPosition.ticker,
                  before: beforePosition?.weight ?? 0,
                  after: afterPosition.weight,
                  delta: afterPosition.weight - (beforePosition?.weight ?? 0),
                };
              })
              .sort((left, right) => Math.abs(right.delta) - Math.abs(left.delta))
              .slice(0, 5);

            return (
              <div className="scenario-shell">
                <div className="scenario-summary">
                  <article className="scenario-summary__card">
                    <span className="field-label">Scenario</span>
                    <strong>{scenario.label}</strong>
                    <p>
                      {scenario.hypothetical_position.shares !== null &&
                      scenario.hypothetical_position.shares !== undefined
                        ? `${scenario.hypothetical_position.shares.toFixed(4)} shares`
                        : `${formatPercent(scenario.hypothetical_position.target_weight ?? 0)} target weight`}
                    </p>
                  </article>
                  <article className="scenario-summary__card">
                    <span className="field-label">Focus</span>
                    <strong>Before vs after portfolio state</strong>
                    <p>Metrics, sector weights, and top holding weight changes are shown side by side.</p>
                  </article>
                </div>

                <div className="table-shell">
                  <table>
                    <thead>
                      <tr>
                        <th>Metric</th>
                        <th>Before</th>
                        <th>After</th>
                        <th>Delta</th>
                      </tr>
                    </thead>
                    <tbody>
                      {scenario.deltas.map((delta) => (
                        <tr key={delta.metric}>
                          <td>{labels[delta.metric] || delta.metric}</td>
                          <td>{delta.before === null ? "n/a" : formatNumericValue(delta.metric, delta.before)}</td>
                          <td>{delta.after === null ? "n/a" : formatNumericValue(delta.metric, delta.after)}</td>
                          <td>
                            <span className={desirabilityToneClass(delta.metric, delta.delta)}>
                              {delta.delta === null ? "n/a" : formatDeltaValue(delta.metric, delta.delta)}
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                <div className="scenario-grid">
                  <article className="scenario-card">
                    <h4>Largest Sector Shifts</h4>
                    <ul className="simple-list">
                      {sectorChanges.map((change) => (
                        <li key={change.sector}>
                          <span>
                            {change.sector}
                            <small>
                              {formatPercent(change.before)} -&gt; {formatPercent(change.after)}
                            </small>
                          </span>
                          <strong className={desirabilityToneClass("weight", change.delta)}>
                            {formatDeltaValue("weight", change.delta)}
                          </strong>
                        </li>
                      ))}
                    </ul>
                  </article>

                  <article className="scenario-card">
                    <h4>Largest Holding Weight Changes</h4>
                    <ul className="simple-list">
                      {weightChanges.map((change) => (
                        <li key={change.ticker}>
                          <span>
                            {change.ticker}
                            <small>
                              {formatPercent(change.before)} -&gt; {formatPercent(change.after)}
                            </small>
                          </span>
                          <strong className={desirabilityToneClass("weight", change.delta)}>
                            {formatDeltaValue("weight", change.delta)}
                          </strong>
                        </li>
                      ))}
                    </ul>
                  </article>
                </div>
              </div>
            );
          })()}
        </section>
      ) : null}

      {result.overlays.macro || result.overlays.earnings || result.overlays.filings ? (
        <section className="panel">
          <h3>Research Overlays</h3>
          {result.overlays.macro ? (
            <article className="overlay-card">
              <h4>Macro Overlay</h4>
              <ul>
                {result.overlays.macro.findings.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
            </article>
          ) : null}
          {result.overlays.earnings ? (
            <article className="overlay-card">
              <h4>Earnings Overlay</h4>
              {result.overlays.earnings.companies.map((company) => (
                <div key={company.ticker}>
                  <strong>{company.ticker}</strong>
                  {company.event_date ? <p>Event date: {company.event_date}</p> : null}
                  {renderNlpSummary(company.nlp_summary)}
                  <ul>
                    {company.findings.map((item) => (
                      <li key={item}>{item}</li>
                    ))}
                  </ul>
                </div>
              ))}
            </article>
          ) : null}
          {result.overlays.filings ? (
            <article className="overlay-card">
              <h4>Filings Overlay</h4>
              {result.overlays.filings.companies.map((company) => (
                <div key={company.ticker}>
                  <strong>{company.ticker}</strong>
                  {company.filed_at ? <p>Filed at: {company.filed_at}</p> : null}
                  {renderNlpSummary(company.nlp_summary)}
                  <ul>
                    {company.findings.map((item) => (
                      <li key={item}>{item}</li>
                    ))}
                  </ul>
                </div>
              ))}
            </article>
          ) : null}
        </section>
      ) : null}

      <section className="panel">
        <h3>Final Memo</h3>
        <article className="memo-card">
          <p className="memo-thesis">{result.final_memo.thesis}</p>
          <h4>Executive Summary</h4>
          <ul>
            {result.final_memo.executive_summary.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
          <h4>Evidence</h4>
          <ul>
            {result.final_memo.evidence.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
          <h4>Risks and Caveats</h4>
          <ul>
            {result.final_memo.risks_and_caveats.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </article>
      </section>

      <section className="panel two-column">
        <div>
          <h3>Critic Approved</h3>
          <ul>
            {result.critic.approved_claims.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </div>
        <div>
          <h3>Critic Flagged</h3>
          <ul>
            {result.critic.flagged_claims.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </div>
      </section>
    </section>
  );
}
