import { Fragment } from "react";
import { resolveArtifactUrl } from "../lib/api";
import type { AnalysisResponse, MetricCard, NLPTextSummary } from "../types";

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
  if (!result) {
    return (
      <section className="results-empty">
        <p>Run an analysis to generate the baseline health check, dynamic investigation, overlays, memo, and artifacts.</p>
      </section>
    );
  }

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
        <div className="results-total">
          <span>Total Portfolio Value</span>
          <strong>{currency(result.baseline.total_portfolio_value)}</strong>
        </div>
      </div>

      {result.warnings.length > 0 ? renderWarnings(result.warnings) : null}

      <div className="metrics-grid">{result.baseline.metrics.map(renderMetric)}</div>

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
          <ul className="simple-list">
            {result.baseline.sector_exposures.map((sector) => (
              <li key={sector.sector}>
                <span>{sector.sector}</span>
                <strong>{formatPercent(sector.weight)}</strong>
              </li>
            ))}
          </ul>
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

      <section className="panel">
        <h3>Artifacts</h3>
        <div className="artifact-grid">
          {result.artifacts.map((artifact) => (
            <article className="artifact-card" key={artifact.artifact_id}>
              <h4>{artifact.title}</h4>
              {artifact.kind.includes("chart") || artifact.kind.includes("performance") || artifact.kind.includes("heatmap") || artifact.kind.includes("sector") ? (
                <img src={resolveArtifactUrl(artifact.url)} alt={artifact.title} />
              ) : null}
              <a href={resolveArtifactUrl(artifact.url)} target="_blank" rel="noreferrer">
                Open artifact
              </a>
            </article>
          ))}
        </div>
      </section>
    </section>
  );
}
