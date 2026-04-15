from __future__ import annotations

import json
from datetime import date

from app.database import Database
from app.services.artifacts import ArtifactService


def test_save_session_result_serializes_date_fields(tmp_path) -> None:
    database = Database(tmp_path / "app.db")
    database.initialize()
    service = ArtifactService(database=database, artifacts_dir=tmp_path / "artifacts")

    service.save_session_result(
        session_id="session-1",
        question="What drove performance during 2024 only?",
        portfolio_json={
            "question": "What drove performance during 2024 only?",
            "start_date": date(2024, 1, 1),
            "end_date": date(2024, 12, 31),
        },
        plan_json={"question_type": "performance_drivers"},
        result_json={
            "warnings": [],
            "effective_start_date": "2024-01-02",
            "effective_end_date": "2024-12-31",
        },
    )

    with database.connect() as connection:
        row = connection.execute(
            "SELECT portfolio_json, plan_json, result_json FROM analysis_sessions WHERE session_id = ?",
            ("session-1",),
        ).fetchone()

    assert row is not None
    stored_portfolio = json.loads(row["portfolio_json"])
    assert stored_portfolio["start_date"] == "2024-01-01"
    assert stored_portfolio["end_date"] == "2024-12-31"
