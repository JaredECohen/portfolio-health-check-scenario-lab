from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from app.database import Database


class CacheService:
    def __init__(self, database: Database) -> None:
        self.database = database

    def get_json(self, cache_key: str) -> Any | None:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT payload_json, expires_at FROM http_cache WHERE cache_key = ?",
                (cache_key,),
            ).fetchone()
        if row is None:
            return None
        expires_at = row["expires_at"]
        if expires_at and datetime.fromisoformat(expires_at) < datetime.now(UTC):
            return None
        return json.loads(row["payload_json"])

    def set_json(
        self,
        cache_key: str,
        payload: Any,
        *,
        source: str,
        ttl_seconds: int | None = None,
    ) -> None:
        created_at = datetime.now(UTC)
        expires_at = (
            (created_at + timedelta(seconds=ttl_seconds)).isoformat() if ttl_seconds else None
        )
        with self.database.connect() as connection:
            connection.execute(
                """
                INSERT INTO http_cache(cache_key, source, created_at, expires_at, payload_json)
                VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                  source = excluded.source,
                  created_at = excluded.created_at,
                  expires_at = excluded.expires_at,
                  payload_json = excluded.payload_json
                """,
                (
                    cache_key,
                    source,
                    created_at.isoformat(),
                    expires_at,
                    json.dumps(payload),
                ),
            )

