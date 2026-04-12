import { startTransition, useMemo, useState } from "react";
import { SearchableTickerInput } from "./components/SearchableTickerInput";
import { ResultsPanel } from "./components/ResultsPanel";
import { analyzePortfolio } from "./lib/api";
import type { AnalysisResponse, HoldingRow, HypotheticalInput, TickerMetadata } from "./types";

const QUESTION_EXAMPLES = [
  "What should I add to my portfolio to diversify?",
  "What is driving my performance?",
  "How will a move in rates affect my portfolio?",
  "How would an escalation in war affect my portfolio?",
];

function createBlankHolding(): HoldingRow {
  return { ticker: "", shares: "", cost_basis: "", company_name: "", sector: "" };
}

export default function App() {
  const [holdings, setHoldings] = useState<HoldingRow[]>([
    { ticker: "AAPL", shares: "25", cost_basis: "182", company_name: "Apple Inc", sector: "Technology" },
    { ticker: "JPM", shares: "18", cost_basis: "168", company_name: "JPMorgan Chase & Co", sector: "Financial Services" },
    { ticker: "XOM", shares: "30", cost_basis: "110", company_name: "Exxon Mobil Corp", sector: "Energy" },
  ]);
  const [question, setQuestion] = useState(QUESTION_EXAMPLES[0]);
  const [hypothetical, setHypothetical] = useState<HypotheticalInput>({
    ticker: "",
    shares: "",
    target_weight: "",
  });
  const [result, setResult] = useState<AnalysisResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const readyHoldings = useMemo(
    () => holdings.filter((holding) => holding.ticker && holding.shares),
    [holdings],
  );

  async function handleAnalyze() {
    setLoading(true);
    setError(null);
    try {
      const payload = {
        holdings: readyHoldings.map((holding) => ({
          ticker: holding.ticker,
          shares: Number(holding.shares),
          cost_basis: holding.cost_basis ? Number(holding.cost_basis) : null,
          company_name: holding.company_name,
          sector: holding.sector,
          cik: holding.cik,
          exchange: holding.exchange,
        })),
        question,
        benchmark: "SPY",
        lookback_days: 252,
        hypothetical_position: hypothetical.ticker
          ? {
              ticker: hypothetical.ticker,
              shares: hypothetical.shares ? Number(hypothetical.shares) : null,
              target_weight: hypothetical.target_weight ? Number(hypothetical.target_weight) : null,
              company_name: hypothetical.company_name,
              sector: hypothetical.sector,
              cik: hypothetical.cik,
              exchange: hypothetical.exchange,
            }
          : null,
      };
      const response = await analyzePortfolio(payload);
      startTransition(() => {
        setResult(response);
      });
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Analysis failed.");
    } finally {
      setLoading(false);
    }
  }

  function updateHolding(index: number, ticker: TickerMetadata) {
    setHoldings((current) =>
      current.map((item, itemIndex) =>
        itemIndex === index
          ? {
              ...item,
              ticker: ticker.ticker,
              company_name: ticker.company_name,
              sector: ticker.sector || "Unknown",
              cik: ticker.cik,
              exchange: ticker.exchange,
            }
          : item,
      ),
    );
  }

  return (
    <div className="app-shell">
      <aside className="workspace-panel">
        <div className="hero">
          <p className="eyebrow">Collect -&gt; EDA -&gt; Hypothesize</p>
          <h1>Portfolio Health Check + Research Overlay + Scenario Lab</h1>
          <p>
            Build a manual U.S. equity portfolio, ask a question, and trigger a multi-agent analysis
            flow with deterministic analytics and grounded memo output.
          </p>
        </div>

        <section className="panel">
          <div className="section-header">
            <h2>Portfolio Builder</h2>
            <button
              type="button"
              className="ghost-button"
              onClick={() => setHoldings((current) => [...current, createBlankHolding()])}
            >
              Add position
            </button>
          </div>
          <div className="position-table">
            <div className="position-table__head">
              <span>Ticker</span>
              <span>Shares</span>
              <span>Cost basis</span>
              <span>Company</span>
              <span>Sector</span>
              <span />
            </div>
            {holdings.map((holding, index) => (
              <div className="position-table__row" key={`holding-${index}`}>
                <SearchableTickerInput
                  value={holding.ticker}
                  onSelect={(ticker) => updateHolding(index, ticker)}
                />
                <input
                  type="number"
                  min="0"
                  step="0.01"
                  value={holding.shares}
                  onChange={(event) =>
                    setHoldings((current) =>
                      current.map((item, itemIndex) =>
                        itemIndex === index ? { ...item, shares: event.target.value } : item,
                      ),
                    )
                  }
                />
                <input
                  type="number"
                  min="0"
                  step="0.01"
                  value={holding.cost_basis}
                  onChange={(event) =>
                    setHoldings((current) =>
                      current.map((item, itemIndex) =>
                        itemIndex === index ? { ...item, cost_basis: event.target.value } : item,
                      ),
                    )
                  }
                />
                <div className="readout">{holding.company_name || "Auto-fill"}</div>
                <div className="readout">{holding.sector || "Auto-fill"}</div>
                <button
                  type="button"
                  className="row-action"
                  onClick={() =>
                    setHoldings((current) => current.filter((_, itemIndex) => itemIndex !== index))
                  }
                >
                  Remove
                </button>
              </div>
            ))}
          </div>
        </section>

        <section className="panel">
          <h2>Hypothetical Addition</h2>
          <div className="hypothetical-grid">
            <SearchableTickerInput
              value={hypothetical.ticker}
              onSelect={(ticker) =>
                setHypothetical((current) => ({
                  ...current,
                  ticker: ticker.ticker,
                  company_name: ticker.company_name,
                  sector: ticker.sector || "Unknown",
                  cik: ticker.cik,
                  exchange: ticker.exchange,
                }))
              }
            />
            <input
              type="number"
              min="0"
              step="0.01"
              placeholder="Shares"
              value={hypothetical.shares}
              onChange={(event) =>
                setHypothetical((current) => ({ ...current, shares: event.target.value }))
              }
            />
            <input
              type="number"
              min="0"
              max="1"
              step="0.01"
              placeholder="Target weight (0.05)"
              value={hypothetical.target_weight}
              onChange={(event) =>
                setHypothetical((current) => ({ ...current, target_weight: event.target.value }))
              }
            />
          </div>
        </section>

        <section className="panel">
          <h2>Question</h2>
          <textarea
            rows={5}
            value={question}
            onChange={(event) => setQuestion(event.target.value)}
            placeholder="Ask a portfolio analysis question."
          />
          <div className="example-row">
            {QUESTION_EXAMPLES.map((example) => (
              <button
                type="button"
                key={example}
                className="example-pill"
                onClick={() => setQuestion(example)}
              >
                {example}
              </button>
            ))}
          </div>
        </section>

        {error ? <div className="error-banner">{error}</div> : null}

        <button
          type="button"
          className="primary-button"
          disabled={loading || readyHoldings.length === 0}
          onClick={() => void handleAnalyze()}
        >
          {loading ? "Running multi-agent analysis..." : "Run Portfolio Health Check"}
        </button>
      </aside>

      <main className="results-panel">
        <ResultsPanel result={result} />
      </main>
    </div>
  );
}
