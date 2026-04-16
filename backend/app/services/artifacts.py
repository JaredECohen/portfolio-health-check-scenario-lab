from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from fastapi.encoders import jsonable_encoder

from app.database import Database


class ArtifactService:
    """Persist structured run metadata without writing downloadable files."""

    def __init__(self, database: Database, artifacts_dir: Path | None = None) -> None:
        self.database = database
        self.artifacts_dir = artifacts_dir

    def save_session_result(
        self,
        *,
        session_id: str,
        question: str,
        portfolio_json: dict,
        plan_json: dict,
        result_json: dict,
    ) -> None:
        with self.database.connect() as connection:
            connection.execute(
                """
                INSERT INTO analysis_sessions(session_id, created_at, question, portfolio_json, plan_json, result_json)
                VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                  question = excluded.question,
                  portfolio_json = excluded.portfolio_json,
                  plan_json = excluded.plan_json,
                  result_json = excluded.result_json
                """,
                (
                    session_id,
                    datetime.now(UTC).isoformat(),
                    question,
                    self._json_text(portfolio_json),
                    self._json_text(plan_json),
                    self._json_text(result_json),
                ),
            )

    def save_factor_cross_section_run(
        self,
        *,
        session_id: str,
        universe_mode: str,
        sector_filters: list[str],
        routed_tickers: list[str],
        effective_start_date: str | None,
        effective_end_date: str | None,
        metric_columns: list[str],
        row_count: int,
        metadata: dict | None = None,
    ) -> None:
        with self.database.connect() as connection:
            connection.execute(
                """
                INSERT INTO factor_cross_section_runs(
                    session_id,
                    created_at,
                    universe_mode,
                    sector_filters_json,
                    routed_tickers_json,
                    effective_start_date,
                    effective_end_date,
                    metric_columns_json,
                    row_count,
                    metadata_json
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                  universe_mode = excluded.universe_mode,
                  sector_filters_json = excluded.sector_filters_json,
                  routed_tickers_json = excluded.routed_tickers_json,
                  effective_start_date = excluded.effective_start_date,
                  effective_end_date = excluded.effective_end_date,
                  metric_columns_json = excluded.metric_columns_json,
                  row_count = excluded.row_count,
                  metadata_json = excluded.metadata_json
                """,
                (
                    session_id,
                    datetime.now(UTC).isoformat(),
                    universe_mode,
                    self._json_text(sector_filters),
                    self._json_text(routed_tickers),
                    effective_start_date,
                    effective_end_date,
                    self._json_text(metric_columns),
                    row_count,
                    self._json_text(metadata or {}),
                ),
            )

    @staticmethod
    def _json_text(payload: object, *, indent: int | None = None) -> str:
        return json.dumps(jsonable_encoder(payload), indent=indent)
