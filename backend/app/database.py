from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import unquote, urlparse

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - exercised only when postgres support is unavailable
    psycopg = None
    dict_row = None


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

CREATE TABLE IF NOT EXISTS factor_cross_section_runs (
    session_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    universe_mode TEXT NOT NULL,
    sector_filters_json TEXT NOT NULL,
    routed_tickers_json TEXT NOT NULL,
    effective_start_date TEXT,
    effective_end_date TEXT,
    metric_columns_json TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    metadata_json TEXT
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

CREATE TABLE IF NOT EXISTS fact_factor_returns (
    dataset_id TEXT NOT NULL,
    model TEXT NOT NULL,
    frequency TEXT NOT NULL,
    factor TEXT NOT NULL,
    date TEXT NOT NULL,
    value REAL NOT NULL,
    source TEXT NOT NULL,
    metadata_json TEXT,
    PRIMARY KEY(dataset_id, factor, date)
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
CREATE INDEX IF NOT EXISTS idx_factor_returns_frequency_date ON fact_factor_returns(frequency, date DESC);
CREATE INDEX IF NOT EXISTS idx_factor_returns_factor_date ON fact_factor_returns(factor, date DESC);
CREATE INDEX IF NOT EXISTS idx_company_fundamentals_ticker_metric_date ON fact_company_fundamentals(ticker, metric, period_end DESC);
CREATE INDEX IF NOT EXISTS idx_company_filings_ticker_filed_at ON fact_company_filings(ticker, filed_at DESC);
CREATE INDEX IF NOT EXISTS idx_short_interest_ticker_date ON fact_short_interest(ticker, settlement_date DESC);
CREATE INDEX IF NOT EXISTS idx_sales_trends_series_date ON fact_sales_trends(series_id, date DESC);
CREATE INDEX IF NOT EXISTS idx_factor_cross_section_runs_created_at ON factor_cross_section_runs(created_at DESC);
"""


class PostgresConnection:
    def __init__(self, connection: Any) -> None:
        self._connection = connection

    def execute(self, query: str, params: Any = None) -> Any:
        normalized = Database.normalize_query(query, backend="postgres")
        if params is None:
            return self._connection.execute(normalized)
        return self._connection.execute(normalized, params)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)


class Database:
    def __init__(self, target: Path | str) -> None:
        self.backend, self.path, self.url = self._parse_target(target)

    @property
    def display_target(self) -> str:
        if self.url is not None:
            parsed = urlparse(self.url)
            if parsed.password is None:
                return self.url
            auth = parsed.username or ""
            if auth:
                auth = f"{auth}:***@"
            host = parsed.hostname or ""
            if parsed.port is not None:
                host = f"{host}:{parsed.port}"
            return f"{parsed.scheme}://{auth}{host}{parsed.path}"
        assert self.path is not None
        return str(self.path)

    @property
    def is_postgres(self) -> bool:
        return self.backend == "postgres"

    def initialize(self) -> None:
        if self.is_postgres:
            with self._connect_postgres() as connection:
                wrapper = PostgresConnection(connection)
                for statement in self._schema_statements():
                    wrapper.execute(statement)
                connection.commit()
            return

        assert self.path is not None
        if self.path != Path(":memory:"):
            self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path, timeout=30.0) as connection:
            connection.execute("PRAGMA journal_mode=WAL;")
            connection.execute("PRAGMA busy_timeout = 30000;")
            connection.execute("PRAGMA foreign_keys = ON;")
            connection.executescript(SCHEMA)
            connection.commit()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection | PostgresConnection]:
        if self.is_postgres:
            with self._connect_postgres() as connection:
                wrapper = PostgresConnection(connection)
                try:
                    yield wrapper
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
            return

        assert self.path is not None
        connection = sqlite3.connect(self.path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 30000;")
        connection.execute("PRAGMA foreign_keys = ON;")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _connect_postgres(self) -> Any:
        if psycopg is None or dict_row is None:
            raise RuntimeError(
                "DATABASE_URL is configured, but psycopg is not installed. "
                "Install psycopg[binary] to enable PostgreSQL support."
            )
        assert self.url is not None
        return psycopg.connect(self.url, row_factory=dict_row)

    @staticmethod
    def normalize_query(query: str, *, backend: str) -> str:
        if backend != "postgres":
            return query
        return query.replace("?", "%s")

    @staticmethod
    def _schema_statements() -> list[str]:
        return [statement.strip() for statement in SCHEMA.split(";") if statement.strip()]

    @staticmethod
    def _parse_target(target: Path | str) -> tuple[str, Path | None, str | None]:
        if isinstance(target, Path):
            return "sqlite", target, None

        value = str(target).strip()
        if value.startswith(("postgresql://", "postgres://")):
            return "postgres", None, value
        if value.startswith("sqlite://"):
            return "sqlite", Database._sqlite_path_from_url(value), None
        return "sqlite", Path(value), None

    @staticmethod
    def _sqlite_path_from_url(url: str) -> Path:
        parsed = urlparse(url)
        if parsed.scheme != "sqlite":
            raise ValueError(f"Unsupported sqlite URL: {url}")
        if parsed.path == "/:memory:":
            return Path(":memory:")
        if parsed.netloc:
            return Path(unquote(f"//{parsed.netloc}{parsed.path}"))
        if url.startswith("sqlite:////"):
            return Path(unquote(parsed.path))
        return Path(unquote(parsed.path.lstrip("/")))
