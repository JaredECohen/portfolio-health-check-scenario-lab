import { AnalysisResponse, TickerMetadata } from "../types";

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
    throw new Error(error.detail || "Analysis failed.");
  }
  return response.json();
}

export function resolveArtifactUrl(url: string): string {
  return `${API_BASE}${url}`;
}

