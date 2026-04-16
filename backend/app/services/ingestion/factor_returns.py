from __future__ import annotations

import csv
import json
from datetime import UTC, date, datetime
from io import BytesIO, StringIO
from typing import Any
from uuid import uuid4
from zipfile import ZipFile

import httpx
import pandas as pd

from app.database import Database
from app.services.factor_registry import FACTOR_DATASET_BY_ID, FACTOR_DATASET_REGISTRY


class FactorIngestionError(RuntimeError):
    pass


class FactorReturnsIngestionService:
    def __init__(self, database: Database) -> None:
        self.database = database

    def sync_datasets(
        self,
        *,
        dataset_ids: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> int:
        selected_ids = dataset_ids or [str(item["dataset_id"]) for item in FACTOR_DATASET_REGISTRY]
        started_at = datetime.now(UTC).isoformat()
        run_id = uuid4().hex
        self._start_run(run_id=run_id, started_at=started_at)
        row_count = 0
        try:
            with self.database.connect() as connection:
                for dataset_id in selected_ids:
                    metadata = FACTOR_DATASET_BY_ID.get(dataset_id)
                    if metadata is None:
                        raise FactorIngestionError(f"Dataset {dataset_id} is not in the curated factor registry.")
                    observations = self._fetch_dataset_rows(
                        dataset=metadata,
                        start_date=start_date,
                        end_date=end_date,
                    )
                    for observation in observations:
                        connection.execute(
                            """
                            INSERT INTO fact_factor_returns(
                                dataset_id,
                                model,
                                frequency,
                                factor,
                                date,
                                value,
                                source,
                                metadata_json
                            )
                            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(dataset_id, factor, date) DO UPDATE SET
                              model = excluded.model,
                              frequency = excluded.frequency,
                              value = excluded.value,
                              source = excluded.source,
                              metadata_json = excluded.metadata_json
                            """,
                            (
                                dataset_id,
                                metadata["model"],
                                metadata["frequency"],
                                observation["factor"],
                                observation["date"],
                                observation["value"],
                                metadata["source"],
                                json.dumps(observation["metadata"]),
                            ),
                        )
                        row_count += 1
            self._finish_run(run_id=run_id, status="success", row_count=row_count)
            return row_count
        except Exception as exc:  # noqa: BLE001
            self._finish_run(run_id=run_id, status="failed", row_count=row_count, details={"error": str(exc)})
            raise

    def _fetch_dataset_rows(
        self,
        *,
        dataset: dict[str, Any],
        start_date: date | None,
        end_date: date | None,
    ) -> list[dict[str, Any]]:
        response = httpx.get(str(dataset["url"]), timeout=120.0)
        response.raise_for_status()
        return self._parse_zipped_csv(
            payload=response.content,
            dataset=dataset,
            start_date=start_date,
            end_date=end_date,
        )

    def _parse_zipped_csv(
        self,
        *,
        payload: bytes,
        dataset: dict[str, Any],
        start_date: date | None,
        end_date: date | None,
    ) -> list[dict[str, Any]]:
        with ZipFile(BytesIO(payload)) as archive:
            names = archive.namelist()
            if not names:
                raise FactorIngestionError("Downloaded factor archive contained no members.")
            raw_text = archive.read(names[0]).decode("utf-8-sig", errors="ignore")
        reader = csv.reader(StringIO(raw_text))
        rows = [[cell.strip() for cell in row] for row in reader]
        header_index = self._header_index(rows)
        header = rows[header_index]
        factors = [self._normalize_factor_name(item) for item in header[1:] if item.strip()]
        if not factors:
            raise FactorIngestionError(f"Unable to parse factor headers for {dataset['dataset_id']}.")
        frequency = str(dataset["frequency"])
        expected_length = 8 if frequency == "daily" else 6
        observations: list[dict[str, Any]] = []
        for row in rows[header_index + 1 :]:
            if not row or not row[0]:
                if observations:
                    break
                continue
            raw_date = row[0].strip()
            if not raw_date.isdigit() or len(raw_date) != expected_length:
                if observations:
                    break
                continue
            parsed_date = self._normalize_date(raw_date, frequency=frequency)
            parsed_date_obj = date.fromisoformat(parsed_date)
            if start_date is not None and parsed_date_obj < start_date:
                continue
            if end_date is not None and parsed_date_obj > end_date:
                continue
            for factor, raw_value in zip(factors, row[1 : len(factors) + 1], strict=False):
                if raw_value in ("", "-99.99", "-999", "NaN"):
                    continue
                observations.append(
                    {
                        "date": parsed_date,
                        "factor": factor,
                        "value": float(raw_value) / 100.0,
                        "metadata": {
                            "source_url": dataset["url"],
                            "title": dataset["title"],
                            "raw_date": raw_date,
                        },
                    }
                )
        if not observations:
            raise FactorIngestionError(f"No usable factor observations were parsed for {dataset['dataset_id']}.")
        return observations

    @staticmethod
    def _header_index(rows: list[list[str]]) -> int:
        for index, row in enumerate(rows):
            first = row[0].strip() if row else ""
            remaining = [item.strip() for item in row[1:] if item.strip()]
            if first == "" and remaining:
                return index
        raise FactorIngestionError("Unable to locate factor header row in archive payload.")

    @staticmethod
    def _normalize_factor_name(name: str) -> str:
        normalized = name.strip().upper().replace("-", "_").replace(" ", "_")
        alias_map = {
            "MKT_RF": "MKT_RF",
            "MKT_RF_": "MKT_RF",
            "MOM": "MOM",
            "ST_REV": "ST_REV",
            "LT_REV": "LT_REV",
        }
        return alias_map.get(normalized, normalized)

    @staticmethod
    def _normalize_date(raw_date: str, *, frequency: str) -> str:
        if frequency == "daily":
            return datetime.strptime(raw_date, "%Y%m%d").date().isoformat()
        monthly = pd.Period(raw_date, freq="M").end_time.normalize()
        return monthly.date().isoformat()

    def _start_run(self, *, run_id: str, started_at: str) -> None:
        with self.database.connect() as connection:
            connection.execute(
                """
                INSERT INTO ingestion_runs(run_id, source, started_at, status, row_count)
                VALUES(?, ?, ?, ?, 0)
                """,
                (run_id, "factor_returns", started_at, "running"),
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
