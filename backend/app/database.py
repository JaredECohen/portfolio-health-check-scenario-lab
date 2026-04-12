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

