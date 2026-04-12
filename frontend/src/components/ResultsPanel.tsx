import { resolveArtifactUrl } from "../lib/api";
import type { AnalysisResponse, MetricCard } from "../types";

function currency(value: number) {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(value);
}

function renderMetric(metric: MetricCard) {
  return (
    <article key={metric.key} className="metric-card">
      <span>{metric.label}</span>
      <strong>{metric.formatted}</strong>
    </article>
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
        </div>
        <div className="results-total">
          <span>Total Portfolio Value</span>
          <strong>{currency(result.baseline.total_portfolio_value)}</strong>
        </div>
      </div>

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
                  <td>{(position.weight * 100).toFixed(2)}%</td>
                  <td>{(position.trailing_return * 100).toFixed(2)}%</td>
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
                <strong>{(sector.weight * 100).toFixed(2)}%</strong>
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
                <strong>{(item.contribution_pct * 100).toFixed(2)}%</strong>
              </li>
            ))}
          </ul>
        </div>
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
                        <td key={column}>{String(row[column] ?? "")}</td>
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
          <ul className="simple-list">
            {result.dynamic_eda.scenario_analysis.deltas.map((delta) => (
              <li key={delta.metric}>
                <span>{delta.metric}</span>
                <strong>{delta.delta === null ? "n/a" : delta.delta.toFixed(4)}</strong>
              </li>
            ))}
          </ul>
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

