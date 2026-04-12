# Portfolio Health Check + Research Overlay + Scenario Lab

Portfolio Health Check is a full-stack multi-agent web app for manual U.S. equity portfolios. It retrieves real data at runtime, computes deterministic baseline portfolio analytics, runs a question-specific exploratory workflow, adds research overlays when relevant, and produces a grounded memo that is reviewed by a critic agent before it is shown to the user.

This repo is built to make the assignment rubric obvious in code and in runtime behavior:

- `Collect`: Alpha Vantage and SEC EDGAR are queried at runtime based on the user’s holdings and question.
- `EDA`: the app always computes a fixed baseline layer first, then routes into a dynamic investigation path chosen by the planner.
- `Hypothesize`: a writer agent builds a memo from the numeric findings, and a critic agent fact-checks and revises it.

## What Problem It Solves

Most portfolio tools stop at static dashboards. This app answers portfolio questions in workflow form:

1. Normalize the user’s portfolio.
2. Pull external market and research data that depends on the actual portfolio.
3. Run deterministic exploratory analysis based on the actual question.
4. Turn the findings into a grounded investment memo with explicit caveats.

## Exact Feature List

- React + TypeScript frontend with structured portfolio entry.
- Searchable ticker dropdown backed by a local U.S. equity metadata file.
- Manual portfolio builder with shares, optional cost basis, company, and sector.
- Optional hypothetical addition with shares or target weight.
- FastAPI backend with runtime data collection, analytics, caching, artifacts, and agent orchestration.
- Fixed baseline portfolio analytics for every run.
- Dynamic EDA workflows for:
  - General health check
  - Concentration / diversification
  - Performance drivers
  - Rates / macro sensitivity
  - Geopolitical / war proxy analysis
  - What-if addition scenarios
- Multi-agent workflow using the OpenAI Agents SDK.
- Tool-calling to deterministic Python analytics and research retrieval functions.
- Candidate addition ranking over a curated liquid U.S. equity universe.
- Earnings transcript overlay from Alpha Vantage when selected by the planner.
- SEC filing overlay from EDGAR when selected by the planner.
- Persistent artifacts:
  - PNG charts
  - JSON analysis snapshot
  - Markdown final memo
- SQLite cache and session storage.
- Docker + Render deployment configuration.

## Screenshots

No screenshots are committed yet. After local startup, capture:

- `docs/screenshots/portfolio-builder.png`
- `docs/screenshots/results-baseline-and-memo.png`
- `docs/screenshots/scenario-and-artifacts.png`

The UI is designed so those three screenshots cover the major rubric items.

## Architecture Overview

### Frontend

- `frontend/src/App.tsx`
  - Portfolio builder
  - Question entry
  - Hypothetical addition entry
  - Submit action
- `frontend/src/components/SearchableTickerInput.tsx`
  - Searchable ticker input hitting `/api/tickers`
- `frontend/src/components/ResultsPanel.tsx`
  - Baseline stats
  - Dynamic EDA findings
  - Overlays
  - Memo
  - Critic output
  - Artifact rendering

### Backend

- `backend/app/routes/api.py`
  - FastAPI API surface
- `backend/app/services/orchestration.py`
  - End-to-end orchestration for Collect -> EDA -> Hypothesize
- `backend/app/agents/runtime.py`
  - OpenAI Agents SDK agent definitions
- `backend/app/tools/agent_tools.py`
  - Tool-calling bridge from agents to deterministic services
- `backend/app/services/*.py`
  - Market data, intake, analytics, scenarios, dynamic EDA, artifacts, cache

### Data / Persistence

- `backend/data/tickers/us_equities.json`
  - Local dropdown metadata file
- `backend/data/candidate_universe.json`
  - Curated candidate additions universe
- `backend/data/app.db`
  - SQLite cache, session store, artifact metadata
- `backend/artifacts/<session_id>/`
  - Per-run charts and reports

## Agent Roster And Responsibilities

### LLM Agents

- Analysis Planner Agent, `gpt-5.4`
  - Classifies the question
  - Picks the dynamic workflow
  - Chooses overlays
  - Enables scenario or candidate search when needed
- Dynamic EDA Agent, `gpt-5.4-mini`
  - Calls the deterministic dynamic EDA tool
  - Converts results into structured findings
- Macro Overlay Agent, `gpt-5.4-mini`
  - Interprets macro sensitivity payloads
- Earnings Overlay Agent, `gpt-5.4-mini`
  - Interprets transcript retrieval payloads
- Filings Overlay Agent, `gpt-5.4-mini`
  - Interprets SEC filing payloads
- Candidate Position Search Agent, `gpt-5.4-mini`
  - Calls deterministic candidate ranking and returns a structured result
- Hypothesis Writer Agent, `gpt-5.4`
  - Produces the grounded memo
- Critic / Fact-Check Agent, `gpt-5.4`
  - Verifies support, flags overstatement, revises memo

### Deterministic Components

- Portfolio Intake / Normalization service
- Market Data service
- Baseline Portfolio Analytics service
- Scenario Simulation service
- Dynamic EDA service
- Artifact generation service

## Which Components Are LLM Agents Vs Deterministic Tools

### LLM

- `backend/app/agents/runtime.py`
- `backend/app/agents/system_prompts.py`

### Deterministic

- `backend/app/services/portfolio_intake.py`
- `backend/app/services/market_data.py`
- `backend/app/services/analytics.py`
- `backend/app/services/scenario.py`
- `backend/app/services/dynamic_eda.py`
- `backend/app/services/artifacts.py`
- `backend/app/services/alpha_vantage.py`
- `backend/app/services/sec_edgar.py`

### Tool Bridge

- `backend/app/tools/agent_tools.py`

## Exact Collect -> EDA -> Hypothesize Mapping

### 1. Collect

Implemented in:

- `PortfolioAnalysisOrchestrator.analyze()`
- `MarketDataService.fetch_price_history()`
- `AlphaVantageService.*`
- `SecEdgarService.*`

Behavior:

- Portfolio holdings are validated and normalized.
- Historical prices are fetched from Alpha Vantage for each portfolio symbol plus benchmark.
- Macro series are fetched when the question requires them.
- Earnings transcripts are fetched when the planner selects that overlay.
- SEC filings are fetched when the planner selects that overlay.
- All network responses are cached in SQLite.

### 2. EDA

Implemented in:

- `AnalyticsService.compute_baseline()`
- `DynamicEDAService.execute()`
- `run_dynamic_eda()` tool in `backend/app/tools/agent_tools.py`

Behavior:

- The app always computes the fixed baseline first.
- The planner then selects a question-specific investigation workflow.
- The Dynamic EDA Agent calls a deterministic tool that executes that workflow.
- The workflow returns concrete findings, tables, and optional scenario / candidate results.

### 3. Hypothesize

Implemented in:

- `AgentRuntime.run_writer()`
- `AgentRuntime.run_critic()`
- `PortfolioAnalysisOrchestrator.analyze()`

Behavior:

- The writer receives the baseline, EDA findings, overlays, and scenario results.
- It produces a structured memo.
- The critic checks the memo against the evidence pack.
- The critic returns approved claims, flagged claims, and a revised memo.
- The revised memo is the final user-facing memo.

## Why The EDA Is Dynamic

The app does not use a fixed dashboard plus a different narrative. It changes the investigation path based on the planner’s `AnalysisPlan`.

Question type to workflow mapping:

| Question type | Dynamic workflow | Deterministic evidence |
| --- | --- | --- |
| General health | `general_health` | Concentration, volatility, beta, sector skew |
| Concentration / diversification | `concentration_diversification` | Top-3 concentration, Herfindahl, average pairwise correlation, most-correlated pair |
| Performance drivers | `performance_drivers` | Contribution decomposition, return vs benchmark, detractors, drawdown |
| Rates / macro | `rates_macro` | Portfolio vs yield-change co-movement, rate-shock regime sample |
| Geopolitical / war | `geopolitical_war` | Oil-shock plus equity-stress proxy regime analysis |
| What-if addition | `what_if_addition` | Deterministic before/after portfolio metrics for the hypothetical addition |

This logic lives in `backend/app/services/dynamic_eda.py` and is surfaced via the `run_dynamic_eda` tool. Different questions call different code paths and produce different tables / metrics.

## Fixed Baseline Analytics Layer

Every run computes:

- Current position values
- Weights
- Company / sector breakdown
- Total portfolio value
- Trailing return
- Return vs SPY
- Annualized volatility
- Beta vs SPY
- Max drawdown
- Average pairwise correlation
- Herfindahl concentration index
- Top-3 holdings share
- Sharpe ratio
- Best and worst performers
- Correlation heatmap input

Implemented in `backend/app/services/analytics.py`.

## Dynamic Investigation Layer

The planner picks the workflow, then the dynamic EDA tool executes only the relevant investigation path. Examples:

- “What is driving my performance?”
  - Runs contribution decomposition and benchmark-relative analysis.
- “What should I add to diversify?”
  - Runs concentration / diversification analysis, then candidate ranking over the curated universe.
- “How will a move in rates affect my portfolio?”
  - Runs yield-sensitivity EDA and the macro overlay agent.
- “What happens if I add MSFT?”
  - Runs a deterministic before/after scenario even if the user omits the hypothetical form, using a default 5% target-weight addition.

## How Findings Feed The Hypothesis Stage

`PortfolioAnalysisOrchestrator.analyze()` builds an evidence pack containing:

- Baseline metric map
- Dynamic EDA findings
- Planner output
- Macro / earnings / filings overlays
- Candidate search result
- Scenario result

That evidence pack is passed into the writer agent, then into the critic agent. The memo is therefore downstream of actual computed results, not independent text generation.

## How The Critic / Fact-Check Stage Works

This is a clear generator-critic pattern:

1. Writer generates `FinalMemo`.
2. Critic receives:
   - The draft memo
   - The full evidence pack
3. Critic returns `CriticResult`:
   - `approved_claims`
   - `flagged_claims`
   - `revised_memo`

Implemented in:

- `backend/app/agents/runtime.py`
- `backend/app/services/orchestration.py`

## Data Sources Used And Why

- Alpha Vantage
  - Equity daily adjusted prices
  - Benchmark price history
  - Treasury yield
  - CPI
  - WTI oil
  - Earnings call transcripts
  - Reason: one API can cover portfolio prices plus lightweight macro and transcript overlays.
- SEC EDGAR
  - Official filings metadata
  - Recent 10-K / 10-Q documents
  - Reason: official filing source for risk / liquidity / debt / regulatory overlays.
- Local JSON metadata built from SEC source
  - Searchable U.S. equity dropdown
  - Reason: structured offline portfolio entry with no brokerage dependency.
- SQLite
  - Response cache
  - Session persistence
  - Artifact metadata

## OpenAI Model Allocation And Why

- `gpt-5.4`
  - Planner
  - Writer
  - Critic
  - Reason: these require higher-quality routing and synthesis.
- `gpt-5.4-mini`
  - Dynamic EDA wrapper
  - Macro overlay
  - Earnings overlay
  - Filings overlay
  - Candidate search
  - Reason: these are narrower, tool-driven specialist tasks.

## API / Tool Inventory

### External APIs

- OpenAI API
- Alpha Vantage API
- SEC EDGAR

### Deterministic Tools Called By Agents

- `run_dynamic_eda`
- `compute_macro_overlay`
- `collect_earnings_overlay_data`
- `collect_filings_overlay_data`
- `rank_candidate_positions`

### Backend API Endpoints

- `GET /api/health`
- `GET /api/tickers`
- `GET /api/tickers/{ticker}`
- `POST /api/analyze`
- `GET /artifacts/<session_id>/<file>`

## Codebase Structure

```text
.
├── backend/
│   ├── app/
│   │   ├── agents/
│   │   ├── models/
│   │   ├── routes/
│   │   ├── services/
│   │   └── tools/
│   ├── artifacts/
│   ├── data/
│   ├── scripts/
│   ├── Dockerfile
│   └── pyproject.toml
├── frontend/
│   ├── public/
│   ├── src/
│   │   ├── components/
│   │   ├── lib/
│   │   └── types.ts
│   ├── Dockerfile
│   └── package.json
├── docker-compose.yml
├── render.yaml
└── README.md
```

## Setup Instructions

### 1. Environment

Copy `.env.example` to `.env` and fill:

```bash
cp .env.example .env
```

Required:

- `OPENAI_API_KEY`
- `ALPHA_VANTAGE_API_KEY`
- `SEC_USER_AGENT`

Recommended defaults can stay as-is:

- `PORTFOLIO_BENCHMARK=SPY`
- `PORTFOLIO_LOOKBACK_DAYS=252`
- `PORTFOLIO_RISK_FREE_FALLBACK=0.02`
- `PORTFOLIO_API_CORS_ORIGINS=http://localhost:5173`
- `VITE_API_BASE_URL=http://localhost:8000`

### 2. Build The Ticker Metadata File

The repo includes a small bootstrap subset at `backend/data/tickers/us_equities.json` so the UI can render immediately. For real use, regenerate the full file from the official SEC source:

```bash
cd backend
python3 scripts/build_ticker_metadata.py --user-agent "Your Name your-email@example.com"
```

This pulls SEC ticker data, filters ETF-like records heuristically, and overwrites `backend/data/tickers/us_equities.json`.

### 3. Backend

```bash
cd backend
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
uvicorn app.main:app --reload
```

### 4. Frontend

```bash
cd frontend
npm install
npm run dev
```

Open [http://localhost:5173](http://localhost:5173).

## Local Development Instructions

### Docker Compose

```bash
docker compose up --build
```

Frontend:

- [http://localhost:5173](http://localhost:5173)

Backend:

- [http://localhost:8000/docs](http://localhost:8000/docs)

## Deployment Instructions

### Render

This repo includes `render.yaml`.

You still need to supply:

- `OPENAI_API_KEY`
- `ALPHA_VANTAGE_API_KEY`
- `SEC_USER_AGENT`

Steps:

1. Create a new Render Blueprint from this repo.
2. Set the missing secrets.
3. Update `PORTFOLIO_API_CORS_ORIGINS` if the frontend URL differs.
4. Deploy.

### Other Hosts

- Backend can run anywhere that supports Docker or Python ASGI.
- Frontend can run on Vercel, Netlify, Render Static, or any static host.

## Artifact Generation Details

Generated in `backend/app/services/artifacts.py`:

- `cumulative_performance.png`
- `sector_exposure.png`
- `correlation_heatmap.png`
- `scenario_comparison.png` when relevant
- `analysis_response.json`
- `final_memo.md`

Artifacts are saved under `backend/artifacts/<session_id>/` and exposed by FastAPI static file serving.

## Example User Questions

- What should I add to my portfolio to diversify?
- Am I too concentrated?
- What is driving my performance?
- Why did I underperform SPY?
- How will a move in rates affect my portfolio?
- How would an escalation in war affect my portfolio?
- What happens if I add MSFT?

## Exact Rubric Mapping

| Rubric item | Implementation |
| --- | --- |
| Frontend | React UI in `frontend/src/App.tsx` and `frontend/src/components/ResultsPanel.tsx` |
| Agent framework | OpenAI Agents SDK in `backend/app/agents/runtime.py` |
| Tool calling | Agent tools in `backend/app/tools/agent_tools.py` |
| Real external data at runtime | Alpha Vantage + SEC EDGAR services |
| Non-trivial dataset | Multi-symbol price history, benchmark history, macro series, transcripts, filings |
| Multi-agent pattern | Planner routing + specialist overlays + writer/critic loop |
| Deployed | Dockerfiles, `docker-compose.yml`, `render.yaml` |
| README | This document explicitly maps Collect -> EDA -> Hypothesize |
| Code execution | Deterministic Python analytics and scenario simulation |
| Structured output | Pydantic schemas in `backend/app/models/schemas.py` and agent `output_type`s |
| Artifacts | `backend/app/services/artifacts.py` |
| Data visualization | Generated PNG charts and frontend artifact display |
| Additional grab-bag: SQLite retrieval layer | `backend/app/database.py`, `backend/app/services/cache.py` |
| Additional grab-bag: parallel specialist execution | `asyncio.gather()` for overlay agents in `backend/app/services/orchestration.py` |

## File / Function / Class Mapping For Key Rubric Items

| Requirement | File / function |
| --- | --- |
| Portfolio normalization | `backend/app/services/portfolio_intake.py`, `PortfolioIntakeService.normalize()` |
| Market data retrieval | `backend/app/services/market_data.py`, `MarketDataService.fetch_price_history()` |
| Baseline analytics | `backend/app/services/analytics.py`, `AnalyticsService.compute_baseline()` |
| Dynamic EDA routing | `backend/app/services/dynamic_eda.py`, `DynamicEDAService.execute()` |
| Scenario simulation | `backend/app/services/scenario.py`, `ScenarioService.simulate_addition()` |
| Candidate ranking | `backend/app/services/scenario.py`, `ScenarioService.rank_candidates()` |
| Tool-calling layer | `backend/app/tools/agent_tools.py` |
| Planner agent | `backend/app/agents/runtime.py`, `AgentRuntime.planner` |
| Writer / critic loop | `backend/app/services/orchestration.py`, `analyze()` |
| Artifact generation | `backend/app/services/artifacts.py` |
| Ticker metadata generation | `backend/scripts/build_ticker_metadata.py` |

## Known Limitations

- Alpha Vantage free-tier rate limits can slow or interrupt broader candidate-search runs.
- The repo ships with a bootstrap ticker subset; regenerate metadata before serious use.
- ETF exclusion uses heuristics over SEC ticker files, not a separate official ETF master list.
- Macro sensitivity is empirical and proxy-based, not a forecast or factor model.
- Filing extraction is lightweight text-pattern scanning, not a full XBRL parser.
- Earnings transcript coverage depends on Alpha Vantage availability.
- The app is deployment-ready, but no live deployment was completed here because no credentials were supplied.

## Future Improvements

- Add richer candidate universe management with sector-balanced presets.
- Add more explicit factor decomposition.
- Add better transcript parsing and quote grounding.
- Add session history view in the frontend.
- Add background jobs for expensive candidate searches.
- Add automated tests once dependencies are installed in the target environment.
- Add richer SEC enrichment for sector / SIC data in the metadata build step.

## Assumptions Made Autonomously

- Default benchmark is `SPY`.
- Default lookback is 252 trading days.
- If a what-if addition is asked in natural language but no hypothetical form is filled, the app tests a default 5% target-weight addition for the mentioned ticker.
- A fallback risk-free rate of 2% is acceptable when treasury series retrieval fails.
- Candidate search uses a curated liquid U.S. equity universe rather than the full market, by design.

