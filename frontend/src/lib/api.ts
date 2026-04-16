import { AnalysisResponse, TickerMetadata, TickerQuote } from "../types";

const API_BASE = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

export async function searchTickers(query: string): Promise<TickerMetadata[]> {
  const response = await fetch(
    `${API_BASE}/api/tickers?q=${encodeURIComponent(query)}&limit=12`,
  );
  if (!response.ok) {
    throw new Error("Unable to load ticker metadata.");
  }
  return response.json();
}

export async function analyzePortfolio(payload: unknown): Promise<AnalysisResponse> {
  const response = await fetch(`${API_BASE}/api/analyze`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: "Analysis failed." }));
    const detail = error.detail;
    if (typeof detail === "string") {
      throw new Error(detail);
    }
    if (detail && typeof detail === "object") {
      const message =
        typeof detail.message === "string" ? detail.message : "Analysis failed.";
      const requestId =
        typeof detail.request_id === "string" ? ` Request ID: ${detail.request_id}` : "";
      throw new Error(`${message}${requestId}`);
    }
    throw new Error("Analysis failed.");
  }
  return response.json();
}

export async function getTickerDetails(ticker: string): Promise<TickerMetadata> {
  const response = await fetch(`${API_BASE}/api/tickers/${encodeURIComponent(ticker)}`);
  if (!response.ok) {
    throw new Error("Unable to load ticker details.");
  }
  return response.json();
}

export async function getTickerQuote(ticker: string): Promise<TickerQuote> {
  const response = await fetch(`${API_BASE}/api/tickers/${encodeURIComponent(ticker)}/quote`);
  if (!response.ok) {
    throw new Error("Unable to load ticker quote.");
  }
  return response.json();
}
