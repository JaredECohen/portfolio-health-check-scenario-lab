from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = """
CREATE TABLE IF NOT EXISTS http_cache (
    cache_key TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT,
    payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS analysis_sessions (
    session_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    question TEXT NOT NULL,
    portfolio_json TEXT NOT NULL,
    plan_json TEXT,
    result_json TEXT
);

CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    path TEXT NOT NULL,
    created_at TEXT NOT NULL,
    metadata_json TEXT,
    FOREIGN KEY(session_id) REFERENCES analysis_sessions(session_id)
);

CREATE TABLE IF NOT EXISTS dim_company (
    ticker TEXT PRIMARY KEY,
    cik TEXT NOT NULL,
    company_name TEXT NOT NULL,
    sector TEXT,
    industry TEXT,
    exchange TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_series (
    series_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    category TEXT NOT NULL,
    frequency TEXT,
    unit TEXT,
    title TEXT NOT NULL,
    metadata_json TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_macro_series (
    series_id TEXT NOT NULL,
    date TEXT NOT NULL,
    value REAL NOT NULL,
    vintage_date TEXT NOT NULL DEFAULT '',
    metadata_json TEXT,
    PRIMARY KEY(series_id, date, vintage_date),
    FOREIGN KEY(series_id) REFERENCES dim_series(series_id)
);

CREATE TABLE IF NOT EXISTS fact_company_fundamentals (
    cik TEXT NOT NULL,
    ticker TEXT NOT NULL,
    metric TEXT NOT NULL,
    period_end TEXT NOT NULL,
    fiscal_period TEXT,
    fiscal_year INTEGER,
    value REAL,
    unit TEXT,
    form_type TEXT NOT NULL DEFAULT '',
    filed_at TEXT,
    frame TEXT NOT NULL DEFAULT '',
    accession_number TEXT,
    PRIMARY KEY(ticker, metric, period_end, form_type, frame)
);

CREATE TABLE IF NOT EXISTS fact_company_filings (
    cik TEXT NOT NULL,
    ticker TEXT NOT NULL,
    accession_number TEXT PRIMARY KEY,
    form_type TEXT NOT NULL,
    filed_at TEXT,
    acceptance_datetime TEXT,
    primary_document TEXT,
    is_xbrl INTEGER NOT NULL DEFAULT 0,
    is_inline_xbrl INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS fact_short_interest (
    ticker TEXT NOT NULL,
    settlement_date TEXT NOT NULL,
    short_interest REAL,
    avg_daily_volume REAL,
    days_to_cover REAL,
    source TEXT NOT NULL,
    PRIMARY KEY(ticker, settlement_date, source)
);

CREATE TABLE IF NOT EXISTS fact_news_intel (
    url_hash TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    published_at TEXT,
    domain TEXT,
    title TEXT NOT NULL,
    summary TEXT,
    tickers_json TEXT NOT NULL,
    topics_json TEXT NOT NULL,
    sentiment REAL,
    relevance REAL,
    url TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_sales_trends (
    series_id TEXT NOT NULL,
    date TEXT NOT NULL,
    value REAL NOT NULL,
    source TEXT NOT NULL,
    segment TEXT NOT NULL DEFAULT '',
    region TEXT NOT NULL DEFAULT '',
    metadata_json TEXT,
    PRIMARY KEY(series_id, date, segment, region)
);

CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL,
    row_count INTEGER NOT NULL DEFAULT 0,
    watermark TEXT,
    details_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_macro_series_series_date ON fact_macro_series(series_id, date DESC);
CREATE INDEX IF NOT EXISTS idx_company_fundamentals_ticker_metric_date ON fact_company_fundamentals(ticker, metric, period_end DESC);
CREATE INDEX IF NOT EXISTS idx_company_filings_ticker_filed_at ON fact_company_filings(ticker, filed_at DESC);
CREATE INDEX IF NOT EXISTS idx_short_interest_ticker_date ON fact_short_interest(ticker, settlement_date DESC);
CREATE INDEX IF NOT EXISTS idx_sales_trends_series_date ON fact_sales_trends(series_id, date DESC);
"""


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as connection:
            connection.executescript(SCHEMA)
            connection.commit()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()
