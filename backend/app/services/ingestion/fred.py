from __future__ import annotations

import json
from datetime import UTC, date, datetime
from typing import Any
from uuid import uuid4

import httpx

from app.database import Database
from app.services.series_registry import FRED_SERIES_REGISTRY, registry_by_series_id


class FredIngestionError(RuntimeError):
    pass


class FredIngestionService:
    BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

    def __init__(self, database: Database, *, api_key: str | None) -> None:
        self.database = database
        self.api_key = api_key

    def sync_curated_series(
        self,
        *,
        series_ids: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> int:
        if not self.api_key:
            raise FredIngestionError("FRED_API_KEY is not configured.")
        registry = registry_by_series_id()
        selected = series_ids or [item["series_id"] for item in FRED_SERIES_REGISTRY]
        started_at = datetime.now(UTC).isoformat()
        run_id = uuid4().hex
        self._start_run(run_id=run_id, started_at=started_at)
        row_count = 0
        try:
            with self.database.connect() as connection:
                for series_id in selected:
                    metadata = registry.get(series_id)
                    if metadata is None:
                        raise FredIngestionError(f"Series {series_id} is not in the curated registry.")
                    connection.execute(
                        """
                        INSERT INTO dim_series(series_id, source, category, frequency, unit, title, metadata_json, updated_at)
                        VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(series_id) DO UPDATE SET
                          source = excluded.source,
                          category = excluded.category,
                          frequency = excluded.frequency,
                          unit = excluded.unit,
                          title = excluded.title,
                          metadata_json = excluded.metadata_json,
                          updated_at = excluded.updated_at
                        """,
                        (
                            series_id,
                            metadata["source"],
                            metadata["category"],
                            metadata["frequency"],
                            metadata["unit"],
                            metadata["title"],
                            json.dumps(metadata),
                            datetime.now(UTC).isoformat(),
                        ),
                    )
                    observations = self._fetch_series_observations(
                        series_id=series_id,
                        start_date=start_date,
                        end_date=end_date,
                    )
                    for observation in observations:
                        connection.execute(
                            """
                            INSERT INTO fact_macro_series(series_id, date, value, vintage_date, metadata_json)
                            VALUES(?, ?, ?, ?, ?)
                            ON CONFLICT(series_id, date, vintage_date) DO UPDATE SET
                              value = excluded.value,
                              metadata_json = excluded.metadata_json
                            """,
                            (
                                series_id,
                                observation["date"],
                                observation["value"],
                                observation["vintage_date"],
                                json.dumps(observation["metadata"]),
                            ),
                        )
                        row_count += 1
            self._finish_run(run_id=run_id, status="success", row_count=row_count)
            return row_count
        except Exception as exc:  # noqa: BLE001
            self._finish_run(run_id=run_id, status="failed", row_count=row_count, details={"error": str(exc)})
            raise

    def _fetch_series_observations(
        self,
        *,
        series_id: str,
        start_date: date | None,
        end_date: date | None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
        }
        if start_date is not None:
            params["observation_start"] = start_date.isoformat()
        if end_date is not None:
            params["observation_end"] = end_date.isoformat()
        response = httpx.get(self.BASE_URL, params=params, timeout=60.0)
        response.raise_for_status()
        payload = response.json()
        observations: list[dict[str, Any]] = []
        for item in payload.get("observations", []):
            value = item.get("value")
            if value in (None, "."):
                continue
            observations.append(
                {
                    "date": item["date"],
                    "value": float(value),
                    "vintage_date": item.get("realtime_end") or "",
                    "metadata": {
                        "realtime_start": item.get("realtime_start"),
                        "realtime_end": item.get("realtime_end"),
                    },
                }
            )
        return observations

    def _start_run(self, *, run_id: str, started_at: str) -> None:
        with self.database.connect() as connection:
            connection.execute(
                """
                INSERT INTO ingestion_runs(run_id, source, started_at, status, row_count)
                VALUES(?, ?, ?, ?, 0)
                """,
                (run_id, "fred_curated", started_at, "running"),
            )

    def _finish_run(
        self,
        *,
        run_id: str,
        status: str,
        row_count: int,
        details: dict[str, Any] | None = None,
    ) -> None:
        with self.database.connect() as connection:
            connection.execute(
                """
                UPDATE ingestion_runs
                SET completed_at = ?, status = ?, row_count = ?, watermark = ?, details_json = ?
                WHERE run_id = ?
                """,
                (
                    datetime.now(UTC).isoformat(),
                    status,
                    row_count,
                    datetime.now(UTC).date().isoformat() if status == "success" else None,
                    json.dumps(details or {}),
                    run_id,
                ),
            )
