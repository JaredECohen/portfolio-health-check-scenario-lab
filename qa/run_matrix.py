from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parents[1]
BACKEND_DIR = ROOT / "backend"
OUT_DIR = ROOT / "qa"
PORTFOLIOS_PATH = OUT_DIR / "portfolios.json"
QUESTIONS_PATH = OUT_DIR / "questions.json"
RESULTS_PATH = OUT_DIR / "results.jsonl"
SUMMARY_PATH = OUT_DIR / "summary.csv"
FAILURE_TAXONOMY_PATH = OUT_DIR / "failure_taxonomy.json"
REPORT_PATH = OUT_DIR / "improvement_report.md"
BACKLOG_PATH = OUT_DIR / "improvement_backlog.json"
RUN_STATE_PATH = OUT_DIR / "run_state.json"
QUESTION_TYPE_EXPECTATIONS = {
    "general_health": {"general_health"},
    "concentration_diversification": {"concentration_diversification"},
    "performance_drivers": {"performance_drivers"},
    "rates_macro": {"rates_macro"},
    "geopolitical_war": {"geopolitical_war", "rates_macro"},
    "what_if_addition": {"what_if_addition"},
}


def set_output_dir(path: Path) -> None:
    global OUT_DIR
    global PORTFOLIOS_PATH
    global QUESTIONS_PATH
    global RESULTS_PATH
    global SUMMARY_PATH
    global FAILURE_TAXONOMY_PATH
    global REPORT_PATH
    global BACKLOG_PATH
    global RUN_STATE_PATH

    OUT_DIR = path
    PORTFOLIOS_PATH = OUT_DIR / "portfolios.json"
    QUESTIONS_PATH = OUT_DIR / "questions.json"
    RESULTS_PATH = OUT_DIR / "results.jsonl"
    SUMMARY_PATH = OUT_DIR / "summary.csv"
    FAILURE_TAXONOMY_PATH = OUT_DIR / "failure_taxonomy.json"
    REPORT_PATH = OUT_DIR / "improvement_report.md"
    BACKLOG_PATH = OUT_DIR / "improvement_backlog.json"
    RUN_STATE_PATH = OUT_DIR / "run_state.json"


PORTFOLIO_ARCHETYPES: list[dict[str, Any]] = [
    {
        "id": "mega_cap_tech",
        "name": "Mega-Cap Tech Concentration",
        "description": "High growth, high concentration, tech-heavy",
        "holdings": {"AAPL": 40, "MSFT": 28, "NVDA": 12, "GOOGL": 20},
        "question_bias": ["concentration_diversification", "rates_macro", "performance_drivers"],
    },
    {
        "id": "fin_energy_barbell",
        "name": "Financials + Energy Barbell",
        "description": "Cyclical value exposure with rate and commodity sensitivity",
        "holdings": {"JPM": 30, "BAC": 24, "XOM": 28, "CVX": 22, "GS": 12},
        "question_bias": ["performance_drivers", "rates_macro", "general_health"],
    },
    {
        "id": "healthcare_defensive",
        "name": "Healthcare Defensive",
        "description": "Quality defensive healthcare basket",
        "holdings": {"LLY": 18, "JNJ": 22, "UNH": 16, "MRK": 24, "ABBV": 20},
        "question_bias": ["general_health", "performance_drivers", "what_if_addition"],
    },
    {
        "id": "industrial_cycle",
        "name": "Industrial Cyclical",
        "description": "Industrials and capital goods with macro beta",
        "holdings": {"CAT": 24, "DE": 18, "GE": 20, "RTX": 18, "HON": 16, "ETN": 14},
        "question_bias": ["rates_macro", "geopolitical_war", "performance_drivers"],
    },
    {
        "id": "staples_utilities",
        "name": "Staples + Utilities Defensive",
        "description": "Low beta defensive posture",
        "holdings": {"PG": 22, "KO": 18, "PEP": 18, "NEE": 16, "SO": 20, "DUK": 14},
        "question_bias": ["general_health", "risk_adjusted_returns", "what_if_addition"],
    },
    {
        "id": "high_beta_momentum",
        "name": "High Beta Momentum",
        "description": "High beta growth and momentum names",
        "holdings": {"TSLA": 20, "PLTR": 18, "COIN": 14, "SMCI": 12, "AMD": 16, "NFLX": 14, "META": 18},
        "question_bias": ["rates_macro", "concentration_diversification", "risk_adjusted_returns"],
    },
    {
        "id": "dividend_value",
        "name": "Dividend Value",
        "description": "Stable cash flow and dividend-oriented large caps",
        "holdings": {"PG": 18, "KO": 16, "PFE": 18, "CVX": 14, "JPM": 14, "HD": 12, "VZ": 18},
        "question_bias": ["general_health", "risk_adjusted_returns", "performance_drivers"],
    },
    {
        "id": "balanced_large_cap",
        "name": "Balanced Large Cap",
        "description": "Diversified large-cap core portfolio",
        "holdings": {"AAPL": 14, "MSFT": 14, "JPM": 10, "XOM": 10, "UNH": 10, "CAT": 8, "PG": 8, "LIN": 8, "AMZN": 10, "META": 8},
        "question_bias": ["general_health", "performance_drivers", "concentration_diversification"],
    },
    {
        "id": "five_name_concentrated",
        "name": "Five-Name Concentrated",
        "description": "Highly concentrated idiosyncratic portfolio",
        "holdings": {"AAPL": 28, "NVDA": 24, "LLY": 18, "JPM": 14, "XOM": 16},
        "question_bias": ["concentration_diversification", "risk_adjusted_returns", "general_health"],
    },
    {
        "id": "fifteen_name_diversified",
        "name": "Fifteen-Name Diversified",
        "description": "Broader multi-sector equity mix",
        "holdings": {"AAPL": 8, "MSFT": 8, "GOOGL": 7, "JPM": 7, "XOM": 7, "UNH": 7, "CAT": 6, "PG": 6, "LIN": 6, "RTX": 6, "KO": 5, "PLD": 5, "SHW": 5, "AMZN": 8, "META": 9},
        "question_bias": ["general_health", "concentration_diversification", "what_if_addition"],
    },
    {
        "id": "communication_platforms",
        "name": "Communication Platforms",
        "description": "Ad and platform-heavy internet portfolio",
        "holdings": {"META": 26, "GOOGL": 24, "NFLX": 18, "DIS": 12, "TTD": 10, "RDDT": 10},
        "question_bias": ["performance_drivers", "risk_adjusted_returns", "concentration_diversification"],
    },
    {
        "id": "consumer_discretionary",
        "name": "Consumer Discretionary Growth",
        "description": "Consumer and platform demand sensitivity",
        "holdings": {"AMZN": 22, "TSLA": 18, "HD": 14, "MCD": 12, "NKE": 12, "BKNG": 10, "SBUX": 12},
        "question_bias": ["performance_drivers", "rates_macro", "general_health"],
    },
    {
        "id": "energy_materials",
        "name": "Energy + Materials",
        "description": "Commodity-linked cyclicals",
        "holdings": {"XOM": 24, "CVX": 18, "COP": 14, "LIN": 16, "SHW": 12, "NEM": 16},
        "question_bias": ["geopolitical_war", "rates_macro", "performance_drivers"],
    },
    {
        "id": "financial_quality",
        "name": "Financial Quality",
        "description": "Large-cap banks and exchanges",
        "holdings": {"JPM": 22, "GS": 14, "MS": 14, "SCHW": 16, "ICE": 16, "SPGI": 18},
        "question_bias": ["rates_macro", "performance_drivers", "general_health"],
    },
    {
        "id": "small_tactical_growth",
        "name": "Small Tactical Growth",
        "description": "Higher volatility emerging leaders",
        "holdings": {"PLTR": 16, "DUOL": 12, "APP": 14, "SNOW": 14, "CRWD": 14, "MDB": 12, "SHOP": 18},
        "question_bias": ["risk_adjusted_returns", "concentration_diversification", "performance_drivers"],
    },
    {
        "id": "semis_focus",
        "name": "Semiconductor Focus",
        "description": "Semis and AI infrastructure cluster",
        "holdings": {"NVDA": 24, "AMD": 14, "AVGO": 18, "QCOM": 12, "TXN": 16, "MU": 16},
        "question_bias": ["concentration_diversification", "rates_macro", "what_if_addition"],
    },
    {
        "id": "recovery_reopening",
        "name": "Recovery / Reopening",
        "description": "Travel, leisure, and cyclicals",
        "holdings": {"BKNG": 18, "DAL": 12, "MAR": 12, "RCL": 10, "MGM": 10, "CAT": 18, "DE": 20},
        "question_bias": ["geopolitical_war", "performance_drivers", "general_health"],
    },
    {
        "id": "real_asset_tilt",
        "name": "Real Asset Tilt",
        "description": "Real assets, infrastructure, and property",
        "holdings": {"PLD": 18, "AMT": 16, "NEE": 16, "XOM": 14, "LIN": 14, "SHW": 10, "CAT": 12},
        "question_bias": ["rates_macro", "risk_adjusted_returns", "what_if_addition"],
    },
    {
        "id": "quality_compounders",
        "name": "Quality Compounders",
        "description": "Steady compounders across sectors",
        "holdings": {"MSFT": 18, "V": 14, "MA": 14, "COST": 16, "PG": 12, "SPGI": 14, "LLY": 12},
        "question_bias": ["general_health", "risk_adjusted_returns", "performance_drivers"],
    },
    {
        "id": "mixed_macro_sensitive",
        "name": "Mixed Macro Sensitive",
        "description": "Blend of rates, oil, defense, and cyclicals",
        "holdings": {"JPM": 14, "XOM": 14, "RTX": 14, "CAT": 14, "NEE": 14, "TSLA": 12, "AAPL": 18},
        "question_bias": ["rates_macro", "geopolitical_war", "concentration_diversification"],
    },
]


QUESTION_TEMPLATES: list[dict[str, Any]] = [
    {"category": "general_health", "text": "Give me a full health check on this portfolio."},
    {"category": "general_health", "text": "Is this portfolio healthy or carrying hidden risk?"},
    {"category": "general_health", "text": "What are the biggest strengths and weaknesses in this portfolio right now?"},
    {"category": "general_health", "text": "Assess the overall health of my portfolio and tell me what stands out."},
    {"category": "general_health", "text": "If you were reviewing this as an investment committee, what would worry you first?"},
    {"category": "general_health", "text": "Give me the first-pass diagnosis on this portfolio."},
    {"category": "general_health", "text": "Where is this portfolio most fragile?"},
    {"category": "general_health", "text": "What risks am I underestimating in this portfolio?"},
    {"category": "concentration_diversification", "text": "Am I too concentrated?"},
    {"category": "concentration_diversification", "text": "Where is diversification weakest here?"},
    {"category": "concentration_diversification", "text": "What should I add to diversify this portfolio?"},
    {"category": "concentration_diversification", "text": "How clustered is this portfolio by sector and correlation?"},
    {"category": "concentration_diversification", "text": "What is the biggest diversification gap in this portfolio?"},
    {"category": "concentration_diversification", "text": "Which holdings are making this portfolio feel less diversified than it looks?"},
    {"category": "concentration_diversification", "text": "If I wanted a more balanced portfolio, where should I start?"},
    {"category": "concentration_diversification", "text": "What single addition would most reduce concentration risk?"},
    {"category": "concentration_diversification", "text": "Do sector exposures make this portfolio less diversified than the holding count implies?"},
    {"category": "concentration_diversification", "text": "I have a feeling these names all move together. Is that true?"},
    {"category": "performance_drivers", "text": "What is driving my performance?"},
    {"category": "performance_drivers", "text": "Why did I underperform SPY?"},
    {"category": "performance_drivers", "text": "Which names are doing the heavy lifting here?"},
    {"category": "performance_drivers", "text": "Break down what actually drove gains and losses in this portfolio."},
    {"category": "performance_drivers", "text": "If I explain this portfolio's performance to a PM, what are the main drivers?"},
    {"category": "performance_drivers", "text": "What are my biggest detractors and why do they matter so much?"},
    {"category": "performance_drivers", "text": "Was this portfolio's performance broad-based or carried by a few names?"},
    {"category": "performance_drivers", "text": "Why did this portfolio trail the benchmark over this period?"},
    {"category": "rates_macro", "text": "How will a move in rates affect my portfolio?"},
    {"category": "rates_macro", "text": "How rate-sensitive is this portfolio really?"},
    {"category": "rates_macro", "text": "If the 10Y jumps again, what happens to this portfolio?"},
    {"category": "rates_macro", "text": "Which names make this portfolio vulnerable to higher yields?"},
    {"category": "rates_macro", "text": "Is this portfolio better positioned for falling rates or rising rates?"},
    {"category": "rates_macro", "text": "Show me how this portfolio behaves during rate shocks."},
    {"category": "rates_macro", "text": "Does this portfolio have hidden duration risk?"},
    {"category": "rates_macro", "text": "How much of my risk here is really a macro rates trade?"},
    {"category": "geopolitical_war", "text": "How would an escalation in war affect my portfolio?"},
    {"category": "geopolitical_war", "text": "If geopolitical tensions spike, what breaks first in this portfolio?"},
    {"category": "geopolitical_war", "text": "How exposed is this portfolio to an oil shock or defense-led risk-off move?"},
    {"category": "geopolitical_war", "text": "Would this portfolio likely hold up or get hit in a geopolitical shock?"},
    {"category": "geopolitical_war", "text": "Stress test this portfolio for a Middle East escalation."},
    {"category": "geopolitical_war", "text": "Where are my war-scenario vulnerabilities?"},
    {"category": "geopolitical_war", "text": "Which holdings would probably help versus hurt if a conflict escalates?"},
    {"category": "geopolitical_war", "text": "How much geopolitical shock sensitivity is embedded in this portfolio?"},
    {"category": "what_if_addition", "text": "What happens if I add MSFT?", "hypothetical": {"ticker": "MSFT", "target_weight": 0.05}},
    {"category": "what_if_addition", "text": "What happens if I add JPM?", "hypothetical": {"ticker": "JPM", "target_weight": 0.05}},
    {"category": "what_if_addition", "text": "What happens if I add XOM?", "hypothetical": {"ticker": "XOM", "target_weight": 0.05}},
    {"category": "what_if_addition", "text": "If I add LLY at a 5% target weight, what changes?", "hypothetical": {"ticker": "LLY", "target_weight": 0.05}},
    {"category": "what_if_addition", "text": "Model a 5% addition to RTX for me.", "hypothetical": {"ticker": "RTX", "target_weight": 0.05}},
    {"category": "what_if_addition", "text": "What would adding NEE do to the risk profile?", "hypothetical": {"ticker": "NEE", "target_weight": 0.05}},
    {"category": "what_if_addition", "text": "How would a small PLD position change this portfolio?", "hypothetical": {"ticker": "PLD", "target_weight": 0.04}},
    {"category": "what_if_addition", "text": "Run a what-if if I added KO.", "hypothetical": {"ticker": "KO", "target_weight": 0.05}},
    {"category": "risk_adjusted_returns", "text": "What should I add to improve risk-adjusted returns?"},
    {"category": "risk_adjusted_returns", "text": "What stock could improve Sharpe without killing returns?"},
    {"category": "risk_adjusted_returns", "text": "How can I lower beta while preserving return potential?"},
    {"category": "risk_adjusted_returns", "text": "What should I add if I want better return per unit of risk?"},
    {"category": "risk_adjusted_returns", "text": "Find me an addition that improves Sharpe and reduces beta."},
    {"category": "risk_adjusted_returns", "text": "What single stock best improves this portfolio's risk-adjusted profile?"},
    {"category": "risk_adjusted_returns", "text": "I want less risk but I do not want to give up returns. What should I add?"},
    {"category": "risk_adjusted_returns", "text": "Screen for something less correlated that still helps returns."},
    {"category": "sector_diversification", "text": "What sector am I underexposed to?"},
    {"category": "sector_diversification", "text": "Which sector addition would improve diversification most?"},
    {"category": "sector_diversification", "text": "Are my sector bets too lopsided?"},
    {"category": "sector_diversification", "text": "Where are the biggest sector holes in this portfolio?"},
    {"category": "sector_diversification", "text": "Should I diversify by adding a different sector, and if so which one?"},
    {"category": "sector_diversification", "text": "How much of my risk is really just sector concentration?"},
    {"category": "benchmark_underperformance", "text": "Why am I lagging SPY?"},
    {"category": "benchmark_underperformance", "text": "Explain the underperformance versus SPY in plain English."},
    {"category": "benchmark_underperformance", "text": "What made this portfolio trail the benchmark?"},
    {"category": "benchmark_underperformance", "text": "Is underperformance here coming from stock picking or portfolio construction?"},
    {"category": "drawdown_risk", "text": "Where would the drawdown likely come from in a selloff?"},
    {"category": "drawdown_risk", "text": "How ugly has drawdown been and what is driving it?"},
    {"category": "drawdown_risk", "text": "What is the main drawdown risk in this portfolio?"},
    {"category": "drawdown_risk", "text": "If markets roll over, which names are likely to amplify downside?"},
    {"category": "macro_sensitivity", "text": "What macro variables matter most for this portfolio?"},
    {"category": "macro_sensitivity", "text": "Is this portfolio more exposed to rates, oil, or growth scares?"},
    {"category": "macro_sensitivity", "text": "Which macro factor seems to explain this portfolio best?"},
    {"category": "macro_sensitivity", "text": "What macro overlay would you put on this portfolio?"},
    {"category": "messy_natural_language", "text": "ok so like what is actually going on here and what would you add maybe?"},
    {"category": "messy_natural_language", "text": "this feels kinda risky but i cant tell if its fake diversified or actually diversified"},
    {"category": "messy_natural_language", "text": "if yields rip and oil rips too am i cooked here?"},
    {"category": "messy_natural_language", "text": "what's the real story, not the generic dashboard answer"},
    {"category": "messy_natural_language", "text": "what stock would make this less dumb"},
    {"category": "messy_natural_language", "text": "why does this portfolio feel worse than spy lately"},
    {"category": "messy_natural_language", "text": "give me the so what on this mix"},
    {"category": "messy_natural_language", "text": "where is the hidden concentration if i squint at this"},
    {"category": "windowed", "text": "What drove performance during 2024 only?", "start_date": "2024-01-01", "end_date": "2024-12-31"},
    {"category": "windowed", "text": "How rate-sensitive was this portfolio in the first half of 2024?", "start_date": "2024-01-01", "end_date": "2024-06-30"},
    {"category": "windowed", "text": "What should I add to diversify based only on 2023 data?", "start_date": "2023-01-01", "end_date": "2023-12-31"},
    {"category": "windowed", "text": "How did this portfolio behave after July 2024?", "start_date": "2024-07-01"},
    {"category": "windowed", "text": "What was the key risk before 2024 ended?", "end_date": "2024-12-31"},
    {"category": "filings_overlay", "text": "Which filing-based risks matter most in this portfolio?"},
    {"category": "earnings_overlay", "text": "Are earnings call signals getting more cautious in my biggest positions?"},
    {"category": "earnings_overlay", "text": "What do recent transcripts say about demand and margins for my main names?"},
    {"category": "filings_overlay", "text": "Are there liquidity or debt themes in recent filings that I should care about?"},
    {"category": "what_if_addition", "text": "What if I added AVGO at 5%?", "hypothetical": {"ticker": "AVGO", "target_weight": 0.05}},
    {"category": "drawdown_risk", "text": "How concentrated is downside risk in the top three holdings?"},
    {"category": "sector_diversification", "text": "If I want to reduce sector crowding, where should I look?"},
    {"category": "risk_adjusted_returns", "text": "Which candidate lowers volatility and beta but does not hurt return in the lookback?"},
    {"category": "messy_natural_language", "text": "if i had to own this tomorrow and couldnt hedge it what would freak me out most"},
    {"category": "benchmark_underperformance", "text": "Did I lose to SPY because of concentration, beta, or stock picks?"},
    {"category": "concentration_diversification", "text": "What is the most correlated cluster in this portfolio?"},
]


QUESTION_CATEGORY_MAP = {
    "risk_adjusted_returns": "concentration_diversification",
    "sector_diversification": "concentration_diversification",
    "benchmark_underperformance": "performance_drivers",
    "drawdown_risk": "general_health",
    "macro_sensitivity": "rates_macro",
    "earnings_overlay": "performance_drivers",
    "filings_overlay": "general_health",
    "messy_natural_language": None,
    "windowed": None,
}


EXPECTED_CANDIDATE_SEARCH = {
    "concentration_diversification",
    "risk_adjusted_returns",
    "sector_diversification",
}

EXPECTED_SCENARIO = {"what_if_addition"}

EXPECTED_MACRO = {"rates_macro", "macro_sensitivity", "geopolitical_war"}


@dataclass
class MetadataIndex:
    by_ticker: dict[str, dict[str, Any]]

    @classmethod
    def load(cls) -> "MetadataIndex":
        path = BACKEND_DIR / "data" / "tickers" / "us_equities.json"
        rows = json.loads(path.read_text(encoding="utf-8"))
        return cls(by_ticker={row["ticker"].upper(): row for row in rows})

    def build_holding(self, ticker: str, shares: float, cost_basis_multiplier: float = 0.92) -> dict[str, Any]:
        item = self.by_ticker[ticker]
        return {
            "ticker": ticker,
            "shares": shares,
            "cost_basis": None,
            "company_name": item.get("company_name"),
            "sector": item.get("sector") or "Unknown",
            "cik": item.get("cik"),
            "exchange": item.get("exchange"),
        }


def get_cached_alpha_vantage_symbols() -> set[str]:
    db_path = BACKEND_DIR / "data" / "app.db"
    if not db_path.exists():
        return set()
    import sqlite3

    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT payload_json FROM http_cache WHERE source='alpha_vantage'").fetchall()
    symbols: set[str] = set()
    for (payload_json,) in rows:
        try:
            payload = json.loads(payload_json)
        except Exception:
            continue
        if isinstance(payload, dict):
            meta = payload.get("Meta Data")
            if isinstance(meta, dict):
                symbol = meta.get("2. Symbol")
                if isinstance(symbol, str):
                    symbols.add(symbol.upper())
            if "quarterlyEarnings" in payload and isinstance(payload.get("symbol"), str):
                symbols.add(payload["symbol"].upper())
    return {symbol for symbol in symbols if symbol != "SPY"}


def load_cached_alpha_vantage_payloads() -> list[dict[str, Any]]:
    db_path = BACKEND_DIR / "data" / "app.db"
    if not db_path.exists():
        return []
    import sqlite3

    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT payload_json FROM http_cache WHERE source='alpha_vantage'").fetchall()
    payloads: list[dict[str, Any]] = []
    for (payload_json,) in rows:
        try:
            payload = json.loads(payload_json)
        except Exception:
            continue
        if isinstance(payload, dict):
            payloads.append(payload)
    return payloads


def quarter_end_date(quarter_id: str) -> date:
    year = int(quarter_id[:4])
    quarter = int(quarter_id[-1])
    month = quarter * 3
    day = 31 if month in {3, 12} else 30
    return date(year, month, day)


def generate_cache_backed_portfolios(metadata: MetadataIndex, cached_symbols: set[str]) -> list[dict[str, Any]]:
    templates = [
        ("cache_tech_core", "Cache Tech Core", "Cache-backed tech core", {"AAPL": 40, "MSFT": 35, "GOOGL": 30}),
        ("cache_growth_barbell", "Cache Growth Barbell", "Growth + platform concentration", {"AAPL": 26, "AMZN": 24, "PLTR": 20, "DUOL": 14, "GOOGL": 16}),
        ("cache_quality", "Cache Quality Blend", "Quality large-cap mix", {"AAPL": 24, "MSFT": 22, "JPM": 16, "LLY": 18, "UNH": 16}),
        ("cache_macro_mix", "Cache Macro Mix", "Rates and oil sensitivity", {"JPM": 26, "XOM": 24, "AAPL": 18, "MSFT": 18, "GOOGL": 14}),
        ("cache_health_focus", "Cache Healthcare Focus", "Healthcare heavy portfolio", {"LLY": 34, "UNH": 30, "JPM": 16, "AAPL": 14}),
        ("cache_energy_finance", "Cache Energy + Finance", "Value and macro-sensitive", {"JPM": 32, "XOM": 30, "UNH": 12, "AAPL": 14, "MSFT": 12}),
        ("cache_high_beta", "Cache High Beta", "High beta cached mix", {"PLTR": 28, "DUOL": 18, "AMZN": 18, "GOOGL": 16, "AAPL": 12}),
        ("cache_balanced", "Cache Balanced", "Balanced cached large-cap", {"AAPL": 18, "MSFT": 18, "GOOGL": 14, "JPM": 14, "XOM": 12, "UNH": 12, "LLY": 12}),
        ("cache_five_name", "Cache Five Name", "Concentrated five-name mix", {"AAPL": 24, "MSFT": 22, "JPM": 18, "LLY": 18, "XOM": 18}),
        ("cache_ten_name", "Cache Ten Name", "Ten-name cached diversified", {"AAPL": 10, "MSFT": 10, "GOOGL": 10, "AMZN": 10, "JPM": 10, "LLY": 10, "UNH": 10, "XOM": 10, "PLTR": 10, "DUOL": 10}),
        ("cache_platforms", "Cache Platforms", "Platform-heavy growth", {"GOOGL": 28, "AMZN": 24, "AAPL": 16, "PLTR": 18, "DUOL": 14}),
        ("cache_defensive_growth", "Cache Defensive Growth", "Quality growth with healthcare ballast", {"MSFT": 22, "AAPL": 18, "LLY": 20, "UNH": 18, "JPM": 12, "XOM": 10}),
        ("cache_energy_hedge", "Cache Energy Hedge", "Oil hedge portfolio", {"XOM": 36, "JPM": 18, "AAPL": 14, "GOOGL": 14, "UNH": 18}),
        ("cache_rates_sensitive", "Cache Rates Sensitive", "Tech plus financials", {"AAPL": 20, "MSFT": 18, "GOOGL": 18, "JPM": 24, "AMZN": 20}),
        ("cache_health_quality", "Cache Health Quality", "Health and quality compounders", {"LLY": 28, "UNH": 26, "MSFT": 18, "AAPL": 14, "JPM": 14}),
        ("cache_momentum", "Cache Momentum", "Momentum-biased cached names", {"PLTR": 24, "DUOL": 18, "AAPL": 16, "AMZN": 16, "MSFT": 14, "GOOGL": 12}),
        ("cache_drawdown_test", "Cache Drawdown Test", "Drawdown-stress test mix", {"PLTR": 18, "DUOL": 12, "XOM": 18, "JPM": 18, "AAPL": 18, "LLY": 16}),
        ("cache_big_tech_plus", "Cache Big Tech Plus", "Mega-cap tech with macro ballast", {"AAPL": 24, "MSFT": 24, "GOOGL": 20, "AMZN": 18, "JPM": 14}),
        ("cache_barbell", "Cache Barbell", "Growth and defense barbell", {"PLTR": 18, "DUOL": 12, "LLY": 20, "UNH": 18, "XOM": 16, "JPM": 16}),
        ("cache_all_weatherish", "Cache All-Weather-ish", "Cached blend across growth, defense, finance, and energy", {"AAPL": 16, "MSFT": 16, "JPM": 14, "XOM": 12, "LLY": 14, "UNH": 14, "GOOGL": 14}),
    ]
    portfolios: list[dict[str, Any]] = []
    for portfolio_id, name, description, holdings_map in templates:
        holdings = [
            metadata.build_holding(ticker=ticker, shares=shares)
            for ticker, shares in holdings_map.items()
            if ticker in cached_symbols and ticker in metadata.by_ticker
        ]
        portfolios.append(
            {
                "id": portfolio_id,
                "name": name,
                "description": description,
                "question_bias": ["general_health", "concentration_diversification", "performance_drivers"],
                "holdings": holdings,
            }
        )
    return portfolios


def ensure_output_dir() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def generate_inputs() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ensure_output_dir()
    metadata = MetadataIndex.load()
    cached_symbols = get_cached_alpha_vantage_symbols()
    if len(cached_symbols) >= 8:
        portfolios = generate_cache_backed_portfolios(metadata, cached_symbols)
    else:
        portfolios = []
        for archetype in PORTFOLIO_ARCHETYPES:
            holdings = []
            for ticker, shares in archetype["holdings"].items():
                if ticker not in metadata.by_ticker:
                    continue
                holdings.append(metadata.build_holding(ticker=ticker, shares=shares))
            portfolios.append(
                {
                    "id": archetype["id"],
                    "name": archetype["name"],
                    "description": archetype["description"],
                    "question_bias": archetype["question_bias"],
                    "holdings": holdings,
                }
            )

    questions: list[dict[str, Any]] = []
    hypothetical_fallbacks = ["MSFT", "JPM", "XOM", "LLY", "UNH", "GOOGL", "AMZN", "PLTR", "AAPL"]
    for index, template in enumerate(QUESTION_TEMPLATES, start=1):
        expected = QUESTION_CATEGORY_MAP.get(template["category"], template["category"])
        hypothetical = template.get("hypothetical")
        if hypothetical and cached_symbols:
            ticker = hypothetical["ticker"].upper()
            if ticker not in cached_symbols:
                replacement = next(
                    (item for item in hypothetical_fallbacks if item in cached_symbols),
                    None,
                )
                if replacement:
                    hypothetical = {**hypothetical, "ticker": replacement}
        questions.append(
            {
                "id": f"q{index:03d}",
                "category": template["category"],
                "expected_question_type": expected,
                "text": template["text"],
                "hypothetical_position": hypothetical,
                "start_date": template.get("start_date"),
                "end_date": template.get("end_date"),
            }
        )

    if len(portfolios) != 20:
        raise ValueError(f"Expected 20 portfolios, found {len(portfolios)} after metadata validation.")
    if len(questions) != 100:
        raise ValueError(f"Expected 100 questions, found {len(questions)}.")

    write_json(PORTFOLIOS_PATH, portfolios)
    write_json(QUESTIONS_PATH, questions)
    return portfolios, questions


def expected_question_type(question: dict[str, Any]) -> str | None:
    return question.get("expected_question_type")


def build_payload(portfolio: dict[str, Any], question: dict[str, Any]) -> dict[str, Any]:
    return {
        "holdings": portfolio["holdings"],
        "question": question["text"],
        "benchmark": "SPY",
        "lookback_days": 252,
        "start_date": question.get("start_date"),
        "end_date": question.get("end_date"),
        "hypothetical_position": question.get("hypothetical_position"),
    }


def response_issues(response: dict[str, Any], question: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    plan = response.get("plan", {})
    dynamic = response.get("dynamic_eda", {})
    overlays = response.get("overlays", {})
    final_memo = response.get("final_memo", {})
    critic = response.get("critic", {})
    warnings = response.get("warnings", [])
    actual_type = plan.get("question_type")
    expected_type = expected_question_type(question)
    if expected_type and actual_type not in QUESTION_TYPE_EXPECTATIONS.get(expected_type, {expected_type}):
        issues.append("planner_mismatch")
    if not dynamic.get("findings"):
        issues.append("no_eda_findings")
    if len(dynamic.get("findings", [])) < 2:
        issues.append("sparse_eda")
    if question["category"] in EXPECTED_CANDIDATE_SEARCH and not dynamic.get("candidate_search"):
        issues.append("missing_candidate_search")
    if question["category"] in EXPECTED_SCENARIO and not dynamic.get("scenario_analysis"):
        issues.append("missing_scenario_analysis")
    if question["category"] in EXPECTED_MACRO and not overlays.get("macro"):
        issues.append("missing_macro_overlay")
    if question["category"] in {"earnings_overlay"} and not overlays.get("earnings"):
        issues.append("missing_earnings_overlay")
    if question["category"] in {"filings_overlay"} and not overlays.get("filings"):
        issues.append("missing_filings_overlay")
    executive_summary = final_memo.get("executive_summary") or []
    if not executive_summary:
        issues.append("empty_memo_summary")
    memo_text = " ".join(
        [
            final_memo.get("title", ""),
            final_memo.get("thesis", ""),
            *executive_summary,
            *(final_memo.get("evidence") or []),
        ]
    )
    if not re.search(r"\d", memo_text):
        issues.append("memo_lacks_numbers")
    if not critic.get("approved_claims") and not critic.get("flagged_claims"):
        issues.append("critic_added_no_value")
    if warnings:
        issues.append("warnings_present")
    candidate_search = dynamic.get("candidate_search") or {}
    if question["category"] in EXPECTED_CANDIDATE_SEARCH:
        candidates = candidate_search.get("candidates") or []
        if candidates and len({candidate.get("ticker") for candidate in candidates}) < min(3, len(candidates)):
            issues.append("repetitive_candidate_output")
    return issues


def score_response(response: dict[str, Any], question: dict[str, Any], status_code: int) -> dict[str, float]:
    dynamic = response.get("dynamic_eda", {})
    overlays = response.get("overlays", {})
    final_memo = response.get("final_memo", {})
    critic = response.get("critic", {})
    warnings = response.get("warnings", [])
    actual_type = response.get("plan", {}).get("question_type")
    expected_type = expected_question_type(question)

    technical_success = 1.0 if status_code == 200 else 0.0
    question_understanding = 1.0
    if expected_type and actual_type not in QUESTION_TYPE_EXPECTATIONS.get(expected_type, {expected_type}):
        question_understanding = 0.3
    eda_relevance = min(1.0, (len(dynamic.get("findings", [])) / 2.0) + (0.25 if dynamic.get("tables") else 0.0))
    if question["category"] in EXPECTED_CANDIDATE_SEARCH and dynamic.get("candidate_search"):
        eda_relevance = min(1.0, eda_relevance + 0.15)
    if question["category"] in EXPECTED_SCENARIO and dynamic.get("scenario_analysis"):
        eda_relevance = min(1.0, eda_relevance + 0.15)
    analytical_rigor = 0.25
    if response.get("baseline", {}).get("metrics"):
        analytical_rigor += 0.25
    if dynamic.get("tables"):
        analytical_rigor += 0.25
    if response.get("baseline", {}).get("effective_observations"):
        analytical_rigor += 0.15
    if re.search(r"\d", " ".join((final_memo.get("evidence") or []) + (final_memo.get("executive_summary") or []))):
        analytical_rigor += 0.10
    grounding_quality = 0.25
    if final_memo.get("evidence"):
        grounding_quality += 0.25
    if critic.get("approved_claims"):
        grounding_quality += 0.25
    if critic.get("flagged_claims"):
        grounding_quality += 0.10
    if warnings:
        grounding_quality -= 0.10
    usefulness = 0.25
    if final_memo.get("thesis"):
        usefulness += 0.25
    if final_memo.get("executive_summary"):
        usefulness += 0.20
    if dynamic.get("candidate_search") or dynamic.get("scenario_analysis"):
        usefulness += 0.20
    if question["category"] in EXPECTED_MACRO and overlays.get("macro"):
        usefulness += 0.10
    critic_effectiveness = 0.2
    if critic.get("approved_claims"):
        critic_effectiveness += 0.4
    if critic.get("flagged_claims"):
        critic_effectiveness += 0.2
    if critic.get("revised_memo"):
        critic_effectiveness += 0.2
    overall = (
        technical_success * 0.20
        + question_understanding * 0.15
        + min(1.0, eda_relevance) * 0.20
        + min(1.0, analytical_rigor) * 0.15
        + max(0.0, min(1.0, grounding_quality)) * 0.15
        + min(1.0, usefulness) * 0.10
        + min(1.0, critic_effectiveness) * 0.05
    )
    return {
        "technical_success": round(technical_success * 5, 2),
        "question_understanding": round(question_understanding * 5, 2),
        "eda_relevance": round(min(1.0, eda_relevance) * 5, 2),
        "analytical_rigor": round(min(1.0, analytical_rigor) * 5, 2),
        "grounding_quality": round(max(0.0, min(1.0, grounding_quality)) * 5, 2),
        "usefulness": round(min(1.0, usefulness) * 5, 2),
        "critic_effectiveness": round(min(1.0, critic_effectiveness) * 5, 2),
        "overall": round(overall * 5, 2),
    }


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row) + "\n")


def load_results(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def get_test_client() -> TestClient:
    backend_path = str(BACKEND_DIR)
    if backend_path not in sys.path:
        sys.path.insert(0, backend_path)
    from app.main import app
    from app.services.alpha_vantage import AlphaVantageError, AlphaVantageService
    from app.services.scenario import ScenarioService

    cached_symbols = get_cached_alpha_vantage_symbols()
    cached_payloads = load_cached_alpha_vantage_payloads()

    if cached_payloads:
        async def cache_backed_request(
            self: AlphaVantageService,
            *,
            params: dict[str, Any],
            ttl_seconds: int = 60 * 60 * 12,
        ) -> Any:
            del ttl_seconds
            function = str(params.get("function", "")).upper()
            symbol = str(params.get("symbol", "")).upper()
            quarter = str(params.get("quarter", "")).upper()

            if function == "TIME_SERIES_DAILY_ADJUSTED":
                for payload in cached_payloads:
                    meta = payload.get("Meta Data")
                    if (
                        isinstance(meta, dict)
                        and "Time Series (Daily)" in payload
                        and str(meta.get("2. Symbol", "")).upper() == symbol
                    ):
                        return payload
                raise AlphaVantageError(f"No cached daily history is available for {symbol}.")

            if function == "OVERVIEW":
                for payload in cached_payloads:
                    if str(payload.get("Symbol", "")).upper() == symbol:
                        return payload
                raise AlphaVantageError(f"No cached company overview is available for {symbol}.")

            if function == "TREASURY_YIELD":
                for payload in cached_payloads:
                    if str(payload.get("name", "")).startswith("10-Year Treasury"):
                        return payload
                raise AlphaVantageError("No cached Treasury yield series is available.")

            if function == "WTI":
                for payload in cached_payloads:
                    if str(payload.get("name", "")).startswith("Crude Oil Prices WTI"):
                        return payload
                raise AlphaVantageError("No cached WTI series is available.")

            if function == "CPI":
                for payload in cached_payloads:
                    if str(payload.get("name", "")).startswith("Consumer Price Index"):
                        return payload
                raise AlphaVantageError("No cached CPI series is available.")

            if function == "EARNINGS_CALL_TRANSCRIPT":
                for payload in cached_payloads:
                    if (
                        str(payload.get("symbol", "")).upper() == symbol
                        and str(payload.get("quarter", "")).upper() == quarter
                        and "transcript" in payload
                    ):
                        return payload
                raise AlphaVantageError(
                    f"No cached earnings transcript is available for {symbol} {quarter}."
                )

            if function == "EARNINGS":
                quarters = sorted(
                    {
                        str(payload.get("quarter", "")).upper()
                        for payload in cached_payloads
                        if str(payload.get("symbol", "")).upper() == symbol
                        and str(payload.get("quarter", "")).upper()
                    },
                    reverse=True,
                )
                if not quarters:
                    raise AlphaVantageError(f"No cached earnings metadata is available for {symbol}.")
                return {
                    "symbol": symbol,
                    "quarterlyEarnings": [
                        {
                            "fiscalDateEnding": quarter_end_date(quarter_id).isoformat(),
                            "reportedDate": quarter_end_date(quarter_id).isoformat(),
                        }
                        for quarter_id in quarters
                    ],
                }

            raise AlphaVantageError(f"No cache-backed handler is available for Alpha Vantage function {function}.")

        AlphaVantageService._request = cache_backed_request

    if cached_symbols:
        original_candidate_universe_rows = ScenarioService._candidate_universe_rows

        def cache_backed_candidate_universe_rows(
            self: ScenarioService,
            candidate_tickers: list[str] | None = None,
        ) -> list[dict[str, Any]]:
            rows = original_candidate_universe_rows(self, candidate_tickers)
            allowed = cached_symbols | {"SPY"}
            return [
                row
                for row in rows
                if str(row.get("ticker", "")).upper().strip() in allowed
            ]

        ScenarioService._candidate_universe_rows = cache_backed_candidate_universe_rows

    return TestClient(app)


def save_run_state(state: dict[str, Any]) -> None:
    write_json(RUN_STATE_PATH, state)


def summarize_results(results: list[dict[str, Any]], target_runs: int) -> None:
    attempted = len(results)
    completed = sum(1 for result in results if result["status_code"] == 200)
    category_counter: Counter[str] = Counter()
    portfolio_counter: Counter[str] = Counter()
    issue_counter: Counter[str] = Counter()
    category_scores: dict[str, list[float]] = defaultdict(list)
    portfolio_scores: dict[str, list[float]] = defaultdict(list)

    with SUMMARY_PATH.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "run_id",
                "portfolio_id",
                "portfolio_name",
                "question_id",
                "question_category",
                "status_code",
                "overall_score",
                "issues",
                "warnings_count",
            ],
        )
        writer.writeheader()
        for result in results:
            issues = result.get("issues", [])
            issue_counter.update(issues)
            category = result["question_category"]
            portfolio = result["portfolio_id"]
            category_counter[category] += 1
            portfolio_counter[portfolio] += 1
            overall_score = float(result.get("scores", {}).get("overall", 0.0))
            category_scores[category].append(overall_score)
            portfolio_scores[portfolio].append(overall_score)
            writer.writerow(
                {
                    "run_id": result["run_id"],
                    "portfolio_id": portfolio,
                    "portfolio_name": result["portfolio_name"],
                    "question_id": result["question_id"],
                    "question_category": category,
                    "status_code": result["status_code"],
                    "overall_score": overall_score,
                    "issues": ";".join(issues),
                    "warnings_count": len(result.get("warnings", [])),
                }
            )

    failure_taxonomy = {
        "attempted_runs": attempted,
        "completed_runs": completed,
        "target_runs": target_runs,
        "issue_counts": issue_counter.most_common(),
        "category_scores": {
            category: round(sum(scores) / len(scores), 3)
            for category, scores in sorted(category_scores.items())
            if scores
        },
        "portfolio_scores": {
            portfolio: round(sum(scores) / len(scores), 3)
            for portfolio, scores in sorted(portfolio_scores.items())
            if scores
        },
    }
    write_json(FAILURE_TAXONOMY_PATH, failure_taxonomy)

    top_issues = issue_counter.most_common(10)
    best_categories = sorted(
        ((category, sum(scores) / len(scores)) for category, scores in category_scores.items() if scores),
        key=lambda item: item[1],
        reverse=True,
    )[:5]
    worst_categories = sorted(
        ((category, sum(scores) / len(scores)) for category, scores in category_scores.items() if scores),
        key=lambda item: item[1],
    )[:5]
    best_portfolios = sorted(
        ((portfolio, sum(scores) / len(scores)) for portfolio, scores in portfolio_scores.items() if scores),
        key=lambda item: item[1],
        reverse=True,
    )[:5]
    worst_portfolios = sorted(
        ((portfolio, sum(scores) / len(scores)) for portfolio, scores in portfolio_scores.items() if scores),
        key=lambda item: item[1],
    )[:5]

    prioritized_backlog = build_backlog(issue_counter)
    write_json(BACKLOG_PATH, prioritized_backlog)

    lines = [
        "# QA Improvement Report",
        "",
        "## Run Summary",
        f"- Total runs attempted: {attempted}",
        f"- Total runs completed: {completed}",
        f"- Completion rate: {completion_rate(completed, target_runs):.2f}%",
        f"- Success rate: {completion_rate(completed, attempted):.2f}%",
        "",
        "## Score Distribution",
        *score_distribution_lines(results),
        "",
        "## Best Question Types",
        *[f"- {category}: {score:.2f}/5" for category, score in best_categories],
        "",
        "## Worst Question Types",
        *[f"- {category}: {score:.2f}/5" for category, score in worst_categories],
        "",
        "## Best Portfolio Archetypes",
        *[f"- {portfolio}: {score:.2f}/5" for portfolio, score in best_portfolios],
        "",
        "## Worst Portfolio Archetypes",
        *[f"- {portfolio}: {score:.2f}/5" for portfolio, score in worst_portfolios],
        "",
        "## Top 10 Recurring Issues",
        *[f"- {issue}: {count}" for issue, count in top_issues],
        "",
        "## Recommended Improvements",
        *[f"- P{item['priority']}: {item['title']} - {item['reason']}" for item in prioritized_backlog[:12]],
        "",
        "## Product / UX Improvements",
        *product_ux_lines(issue_counter),
        "",
        "## Analytics Improvements",
        *analytics_lines(issue_counter),
        "",
        "## Agent / Prompt Improvements",
        *agent_lines(issue_counter),
        "",
        "## Reliability / Test Improvements",
        *reliability_lines(issue_counter),
        "",
        "## Evaluation Framework Improvements",
        *evaluation_lines(),
        "",
        "## Notes",
        "- Runs are checkpointed in results.jsonl and can be resumed.",
        "- Scoring is deterministic and rubric-based; it is designed to highlight likely quality gaps rather than replace human review.",
    ]
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


def score_distribution_lines(results: list[dict[str, Any]]) -> list[str]:
    buckets = Counter()
    for result in results:
        overall = float(result.get("scores", {}).get("overall", 0.0))
        bucket = f"{math.floor(overall)}-{math.floor(overall) + 1}"
        buckets[bucket] += 1
    return [f"- {bucket}: {count}" for bucket, count in sorted(buckets.items())]


def completion_rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return (numerator / denominator) * 100.0


def build_backlog(issue_counter: Counter[str]) -> list[dict[str, Any]]:
    mapping = [
        ("planner_mismatch", 1, "Tighten planner routing", "Planner misclassifications reduce trust and lead to irrelevant workflows."),
        ("missing_candidate_search", 1, "Harden candidate-search activation", "Users asking what to add should consistently get screened candidates."),
        ("missing_macro_overlay", 2, "Improve macro overlay resilience", "Macro questions lose depth when overlays drop out."),
        ("missing_scenario_analysis", 2, "Guarantee what-if scenario execution", "What-if questions need explicit before/after scenario outputs."),
        ("sparse_eda", 1, "Increase question-specific EDA depth", "Thin EDA makes memos feel generic."),
        ("memo_lacks_numbers", 1, "Force memo grounding with numeric evidence", "Narrative without numbers weakens assignment compliance."),
        ("critic_added_no_value", 2, "Improve critic prompts or UI framing", "The critic stage should visibly add rigor."),
        ("warnings_present", 3, "Classify and reduce degraded-mode runs", "Frequent warnings indicate reliability or coverage gaps."),
        ("missing_earnings_overlay", 3, "Improve overlay selectivity", "Overlay questions should return focused transcript evidence."),
        ("missing_filings_overlay", 3, "Improve filing retrieval and extraction", "Filing-specific questions should not go mostly empty."),
        ("no_eda_findings", 1, "Prevent empty investigations", "Successful runs still need differentiated EDA findings."),
    ]
    backlog = []
    for issue, priority, title, reason in mapping:
        if issue_counter.get(issue):
            backlog.append(
                {
                    "issue": issue,
                    "count": issue_counter[issue],
                    "priority": priority,
                    "title": title,
                    "reason": reason,
                }
            )
    backlog.sort(key=lambda item: (item["priority"], -item["count"]))
    return backlog


def product_ux_lines(issue_counter: Counter[str]) -> list[str]:
    lines = [
        "- Surface the effective analysis window and degraded-mode warnings more prominently in the results header.",
        "- Add question-specific empty-state guidance when candidate search or overlays are unavailable.",
    ]
    if issue_counter.get("warnings_present"):
        lines.append("- Group warnings by source so users can tell whether the weakness came from data access, overlays, or planning.")
    return lines


def analytics_lines(issue_counter: Counter[str]) -> list[str]:
    lines = [
        "- Expand candidate ranking diagnostics so users can see why top candidates were selected or rejected.",
        "- Add stronger consistency checks between requested window and effective aligned sample for every overlay and table.",
    ]
    if issue_counter.get("sparse_eda"):
        lines.append("- Add more question-type-specific tables so different prompts visibly trigger different investigations.")
    return lines


def agent_lines(issue_counter: Counter[str]) -> list[str]:
    lines = [
        "- Tighten planner instructions for ambiguous user phrasing and benchmark-underperformance prompts.",
        "- Add explicit guardrails so writer/critic always reference quantitative evidence in the memo body.",
    ]
    if issue_counter.get("planner_mismatch"):
        lines.append("- Capture planner misroutes in telemetry and retrain prompt examples around those exact phrasings.")
    return lines


def reliability_lines(issue_counter: Counter[str]) -> list[str]:
    lines = [
        "- Persist run-level request IDs and backend warning categories for easier debugging.",
        "- Add nightly regression runs over a smaller fixed portfolio/question suite.",
        "- Cache more market-data and overlay inputs to reduce overnight evaluation cost and rate-limit exposure.",
    ]
    if issue_counter.get("warnings_present"):
        lines.append("- Separate overlay failures from core analytics failures in structured response telemetry.")
    return lines


def evaluation_lines() -> list[str]:
    return [
        "- Add a small human-reviewed gold set to calibrate the deterministic rubric.",
        "- Track longitudinal output drift by saving planner type, workflow, warnings, and memo metrics per run.",
    ]


def canonical_run_id(portfolio_id: str, question_id: str) -> str:
    return f"{portfolio_id}__{question_id}"


def question_sort_key(portfolio: dict[str, Any], question: dict[str, Any]) -> tuple[int, int, str]:
    bias = portfolio.get("question_bias", [])
    category = question["category"]
    preferred = category in bias or QUESTION_CATEGORY_MAP.get(category) in bias
    window_penalty = 1 if question.get("start_date") or question.get("end_date") else 0
    overlay_penalty = 1 if category in {"earnings_overlay", "filings_overlay"} else 0
    return (0 if preferred else 1, window_penalty + overlay_penalty, question["id"])


def build_run_queue(portfolios: list[dict[str, Any]], questions: list[dict[str, Any]]) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    queue: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for portfolio in portfolios:
        for question in sorted(questions, key=lambda item: question_sort_key(portfolio, item)):
            queue.append((portfolio, question))
    return queue


def build_balanced_question_queue(
    portfolios: list[dict[str, Any]],
    questions: list[dict[str, Any]],
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    counts = Counter()
    queue: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for question in questions:
        category = question["category"]
        mapped = QUESTION_CATEGORY_MAP.get(category, category)
        candidates = sorted(
            portfolios,
            key=lambda portfolio: (
                counts[portfolio["id"]],
                0 if category in portfolio.get("question_bias", []) or mapped in portfolio.get("question_bias", []) else 1,
                portfolio["id"],
            ),
        )
        chosen = candidates[0]
        counts[chosen["id"]] += 1
        queue.append((chosen, question))
    return queue


def load_completed_ids() -> set[str]:
    return {row["run_id"] for row in load_results(RESULTS_PATH)}


def post_json(base_url: str, payload: dict[str, Any], timeout_seconds: float = 180.0) -> tuple[int, dict[str, Any]]:
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/api/analyze",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8")
            return response.status, json.loads(body)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        try:
            payload_json = json.loads(body)
        except Exception:
            payload_json = {"detail": body}
        return exc.code, payload_json


def run_matrix(
    target_runs: int,
    sleep_seconds: float,
    checkpoint_every: int,
    base_url: str | None = None,
    sample_strategy: str = "full",
    shard_index: int = 0,
    shard_count: int = 1,
) -> dict[str, Any]:
    ensure_output_dir()
    portfolios = load_json(PORTFOLIOS_PATH) if PORTFOLIOS_PATH.exists() else generate_inputs()[0]
    questions = load_json(QUESTIONS_PATH) if QUESTIONS_PATH.exists() else generate_inputs()[1]
    if sample_strategy == "balanced_questions":
        queue = build_balanced_question_queue(portfolios, questions)
    else:
        queue = build_run_queue(portfolios, questions)
    if shard_count > 1:
        queue = [item for index, item in enumerate(queue) if index % shard_count == shard_index]
    completed_ids = load_completed_ids()
    client = None if base_url else get_test_client()
    attempted = 0
    completed = 0
    for portfolio, question in queue:
        if attempted >= target_runs:
            break
        run_id = canonical_run_id(portfolio["id"], question["id"])
        if run_id in completed_ids:
            attempted += 1
            completed += 1
            continue
        payload = build_payload(portfolio, question)
        started_at = datetime.now(UTC).isoformat()
        status_code = 0
        response_json: dict[str, Any] = {}
        error_message = None
        request_id = None
        try:
            if base_url:
                status_code, response_json = post_json(base_url, payload)
            else:
                response = client.post("/api/analyze", json=payload)
                status_code = response.status_code
                response_json = response.json()
        except Exception as exc:  # noqa: BLE001
            error_message = str(exc)
        scores: dict[str, float]
        issues: list[str]
        warnings: list[str]
        if status_code == 200:
            scores = score_response(response_json, question, status_code)
            issues = response_issues(response_json, question)
            warnings = response_json.get("warnings", [])
            completed += 1
        else:
            detail = response_json.get("detail") if isinstance(response_json, dict) else None
            if isinstance(detail, dict):
                error_message = detail.get("message") or error_message
                request_id = detail.get("request_id")
            elif isinstance(detail, str):
                error_message = detail
            scores = {
                "technical_success": 0.0,
                "question_understanding": 0.0,
                "eda_relevance": 0.0,
                "analytical_rigor": 0.0,
                "grounding_quality": 0.0,
                "usefulness": 0.0,
                "critic_effectiveness": 0.0,
                "overall": 0.0,
            }
            issues = ["technical_failure"]
            warnings = []
        attempted += 1
        result_row = {
            "run_id": run_id,
            "portfolio_id": portfolio["id"],
            "portfolio_name": portfolio["name"],
            "question_id": question["id"],
            "question_category": question["category"],
            "question_text": question["text"],
            "status_code": status_code,
            "started_at": started_at,
            "completed_at": datetime.now(UTC).isoformat(),
            "warnings": warnings,
            "request_id": request_id,
            "error_message": error_message,
            "scores": scores,
            "issues": issues,
            "response_excerpt": build_response_excerpt(response_json),
        }
        append_jsonl(RESULTS_PATH, result_row)
        if attempted % checkpoint_every == 0 or attempted == target_runs:
            results = load_results(RESULTS_PATH)
            summarize_results(results, target_runs=target_runs)
            save_run_state(
                {
                    "target_runs": target_runs,
                    "attempted": attempted,
                    "completed": completed,
                    "shard_index": shard_index,
                    "shard_count": shard_count,
                    "last_run_id": run_id,
                    "updated_at": datetime.now(UTC).isoformat(),
                }
            )
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    results = load_results(RESULTS_PATH)
    summarize_results(results, target_runs=target_runs)
    state = {
        "target_runs": target_runs,
        "attempted": attempted,
        "completed": completed,
        "shard_index": shard_index,
        "shard_count": shard_count,
        "updated_at": datetime.now(UTC).isoformat(),
        "backend_started_by_harness": False,
    }
    save_run_state(state)
    return state


def build_response_excerpt(response_json: dict[str, Any]) -> dict[str, Any]:
    if not response_json:
        return {}
    return {
        "plan": response_json.get("plan"),
        "warnings": response_json.get("warnings"),
        "finding_count": len(response_json.get("dynamic_eda", {}).get("findings", [])),
        "table_names": [table.get("name") for table in response_json.get("dynamic_eda", {}).get("tables", [])],
        "candidate_count": len(response_json.get("dynamic_eda", {}).get("candidate_search", {}).get("candidates", []))
        if response_json.get("dynamic_eda", {}).get("candidate_search")
        else 0,
        "scenario_present": bool(response_json.get("dynamic_eda", {}).get("scenario_analysis")),
        "memo_title": response_json.get("final_memo", {}).get("title"),
        "critic_flagged_count": len(response_json.get("critic", {}).get("flagged_claims", [])),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run simulated QA matrix against the portfolio analysis API.")
    parser.add_argument("--generate-only", action="store_true", help="Only generate portfolios/questions datasets.")
    parser.add_argument("--target-runs", type=int, default=2000, help="Maximum number of portfolio/question combinations to run.")
    parser.add_argument("--sleep-seconds", type=float, default=1.0, help="Delay between runs to reduce API pressure.")
    parser.add_argument("--checkpoint-every", type=int, default=10, help="How often to regenerate summaries.")
    parser.add_argument("--base-url", type=str, default=None, help="Optional live backend base URL, e.g. http://127.0.0.1:8000.")
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Optional output directory for artifacts/results. Defaults to qa/.",
    )
    parser.add_argument(
        "--sample-strategy",
        choices=["full", "balanced_questions"],
        default="full",
        help="Queue strategy. 'balanced_questions' runs each question once against a balanced portfolio assignment.",
    )
    parser.add_argument("--shard-index", type=int, default=0, help="Zero-based shard index for parallel runs.")
    parser.add_argument("--shard-count", type=int, default=1, help="Total shard count for parallel runs.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.output_dir:
        set_output_dir(Path(args.output_dir).resolve())
    if args.shard_count < 1:
        raise ValueError("--shard-count must be at least 1.")
    if args.shard_index < 0 or args.shard_index >= args.shard_count:
        raise ValueError("--shard-index must be between 0 and shard_count - 1.")
    generate_inputs()
    if args.generate_only:
        print("Generated portfolios and questions.")
        return
    state = run_matrix(
        target_runs=args.target_runs,
        sleep_seconds=args.sleep_seconds,
        checkpoint_every=args.checkpoint_every,
        base_url=args.base_url,
        sample_strategy=args.sample_strategy,
        shard_index=args.shard_index,
        shard_count=args.shard_count,
    )
    print(json.dumps(state, indent=2))


if __name__ == "__main__":
    main()
