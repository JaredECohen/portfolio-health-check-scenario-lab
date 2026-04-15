from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import Any
from uuid import uuid4
from zipfile import ZipFile

import httpx

from app.database import Database


COMPANYFACTS_BULK_URL = "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip"
SUBMISSIONS_BULK_URL = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
DEFAULT_METRICS = {
    "Revenues",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "GrossProfit",
    "OperatingIncomeLoss",
    "NetIncomeLoss",
    "NetCashProvidedByUsedInOperatingActivities",
    "PaymentsToAcquirePropertyPlantAndEquipment",
    "CashAndCashEquivalentsAtCarryingValue",
    "AssetsCurrent",
    "LiabilitiesCurrent",
    "LongTermDebtNoncurrent",
    "LongTermDebtAndCapitalLeaseObligations",
    "CommonStockSharesOutstanding",
}


@dataclass
class SecBulkLoadResult:
    companies: int = 0
    fundamentals: int = 0
    filings: int = 0


class SecBulkIngestionService:
    def __init__(self, database: Database, *, user_agent: str) -> None:
        self.database = database
        self.user_agent = user_agent

    def bootstrap(self, *, metrics: set[str] | None = None, max_companies: int | None = None) -> SecBulkLoadResult:
        metrics = metrics or DEFAULT_METRICS
        started_at = datetime.now(UTC).isoformat()
        run_id = uuid4().hex
        self._start_run(run_id=run_id, source="sec_bulk", started_at=started_at)
        try:
            companyfacts_zip = self._download_zip(COMPANYFACTS_BULK_URL)
            submissions_zip = self._download_zip(SUBMISSIONS_BULK_URL)
            companyfacts = self._read_json_members(companyfacts_zip, prefix="CIK")
            submissions = self._read_json_members(submissions_zip, prefix="CIK")
            if max_companies is not None:
                companyfacts = dict(list(companyfacts.items())[:max_companies])
                submissions = {key: value for key, value in submissions.items() if key in companyfacts}
            result = self._load_payloads(companyfacts=companyfacts, submissions=submissions, metrics=metrics)
            self._finish_run(
                run_id=run_id,
                status="success",
                row_count=result.companies + result.fundamentals + result.filings,
                watermark=datetime.now(UTC).date().isoformat(),
                details={
                    "companies": result.companies,
                    "fundamentals": result.fundamentals,
                    "filings": result.filings,
                },
            )
            return result
        except Exception as exc:  # noqa: BLE001
            self._finish_run(
                run_id=run_id,
                status="failed",
                row_count=0,
                watermark=None,
                details={"error": str(exc)},
            )
            raise

    def bootstrap_sample_from_dim_company(
        self,
        *,
        metrics: set[str] | None = None,
        max_companies: int = 10,
        offset: int = 0,
        only_missing: bool = False,
        source: str = "sec_sample",
    ) -> SecBulkLoadResult:
        metrics = metrics or DEFAULT_METRICS
        started_at = datetime.now(UTC).isoformat()
        run_id = uuid4().hex
        self._start_run(run_id=run_id, source=source, started_at=started_at)
        try:
            seed_rows = self._dim_company_seed_rows(limit=max_companies, offset=offset, only_missing=only_missing)
            companyfacts: dict[str, dict[str, Any]] = {}
            submissions: dict[str, dict[str, Any]] = {}
            failures: list[dict[str, str]] = []
            for seed in seed_rows:
                cik = self._normalize_cik(seed["cik"])
                try:
                    companyfacts[f"CIK{cik}"] = self._request_json(
                        f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
                    )
                    submissions[f"CIK{cik}"] = self._request_json(
                        f"https://data.sec.gov/submissions/CIK{cik}.json"
                    )
                except Exception as exc:  # noqa: BLE001
                    failures.append(
                        {
                            "ticker": str(seed["ticker"]),
                            "cik": cik,
                            "error": str(exc),
                        }
                    )
            result = self._load_payloads(companyfacts=companyfacts, submissions=submissions, metrics=metrics)
            self._finish_run(
                run_id=run_id,
                status="success" if not failures else "partial_success",
                row_count=result.companies + result.fundamentals + result.filings,
                watermark=datetime.now(UTC).date().isoformat(),
                details={
                    "offset": offset,
                    "requested_companies": len(seed_rows),
                    "companies": result.companies,
                    "fundamentals": result.fundamentals,
                    "filings": result.filings,
                    "failures": failures,
                },
            )
            return result
        except Exception as exc:  # noqa: BLE001
            self._finish_run(
                run_id=run_id,
                status="failed",
                row_count=0,
                watermark=None,
                details={"error": str(exc)},
            )
            raise

    def load_from_directory(
        self,
        *,
        companyfacts_path: Path,
        submissions_path: Path,
        metrics: set[str] | None = None,
        max_companies: int | None = None,
    ) -> SecBulkLoadResult:
        metrics = metrics or DEFAULT_METRICS
        companyfacts = self._read_json_members(companyfacts_path.read_bytes(), prefix="CIK")
        submissions = self._read_json_members(submissions_path.read_bytes(), prefix="CIK")
        if max_companies is not None:
            companyfacts = dict(list(companyfacts.items())[:max_companies])
            submissions = {key: value for key, value in submissions.items() if key in companyfacts}
        return self._load_payloads(companyfacts=companyfacts, submissions=submissions, metrics=metrics)

    def _dim_company_seed_rows(
        self,
        *,
        limit: int,
        offset: int = 0,
        only_missing: bool = False,
    ) -> list[dict[str, Any]]:
        missing_filter = ""
        if only_missing:
            missing_filter = """
                AND NOT EXISTS (
                    SELECT 1
                    FROM fact_company_fundamentals fundamentals
                    WHERE fundamentals.ticker = dim_company.ticker
                )
            """
        with self.database.connect() as connection:
            rows = connection.execute(
                f"""
                SELECT ticker, cik
                FROM dim_company
                WHERE cik IS NOT NULL AND cik != ''
                {missing_filter}
                ORDER BY ticker
                LIMIT ?
                OFFSET ?
                """,
                (limit, offset),
            ).fetchall()
        return [dict(row) for row in rows]

    def _load_payloads(
        self,
        *,
        companyfacts: dict[str, dict[str, Any]],
        submissions: dict[str, dict[str, Any]],
        metrics: set[str],
    ) -> SecBulkLoadResult:
        updated_at = datetime.now(UTC).isoformat()
        result = SecBulkLoadResult()
        with self.database.connect() as connection:
            for cik, facts_payload in companyfacts.items():
                ticker = self._resolve_primary_ticker(facts_payload, submissions.get(cik))
                company_name = facts_payload.get("entityName") or (submissions.get(cik) or {}).get("name")
                if not ticker or not company_name:
                    continue
                connection.execute(
                    """
                    INSERT INTO dim_company(ticker, cik, company_name, sector, industry, exchange, updated_at)
                    VALUES(?, ?, ?, COALESCE((SELECT sector FROM dim_company WHERE ticker = ?), NULL),
                           COALESCE((SELECT industry FROM dim_company WHERE ticker = ?), NULL),
                           COALESCE((SELECT exchange FROM dim_company WHERE ticker = ?), NULL), ?)
                    ON CONFLICT(ticker) DO UPDATE SET
                      cik = excluded.cik,
                      company_name = excluded.company_name,
                      updated_at = excluded.updated_at
                    """,
                    (ticker, cik, company_name, ticker, ticker, ticker, updated_at),
                )
                result.companies += 1
                result.fundamentals += self._insert_company_facts(
                    connection=connection,
                    cik=cik,
                    ticker=ticker,
                    payload=facts_payload,
                    metrics=metrics,
                )

            for cik, payload in submissions.items():
                ticker = self._resolve_primary_ticker(companyfacts.get(cik), payload)
                if not ticker:
                    continue
                result.filings += self._insert_submissions(
                    connection=connection,
                    cik=cik,
                    ticker=ticker,
                    payload=payload,
                )
        return result

    def _download_zip(self, url: str) -> bytes:
        headers = {"User-Agent": self.user_agent, "Accept-Encoding": "gzip, deflate"}
        response = httpx.get(url, headers=headers, timeout=120.0)
        response.raise_for_status()
        return response.content

    def _request_json(self, url: str) -> dict[str, Any]:
        headers = {"User-Agent": self.user_agent, "Accept-Encoding": "gzip, deflate"}
        response = httpx.get(url, headers=headers, timeout=60.0)
        response.raise_for_status()
        return dict(response.json())

    @staticmethod
    def _normalize_cik(value: Any) -> str:
        normalized = str(value).strip().upper()
        if normalized.startswith("CIK"):
            normalized = normalized[3:]
        return normalized.zfill(10)

    @staticmethod
    def _read_json_members(payload: bytes, *, prefix: str) -> dict[str, dict[str, Any]]:
        members: dict[str, dict[str, Any]] = {}
        with ZipFile(BytesIO(payload)) as archive:
            for member in archive.namelist():
                if not member.endswith(".json"):
                    continue
                content = json.loads(archive.read(member).decode("utf-8"))
                key = Path(member).stem
                if not key.startswith(prefix):
                    key = f"{prefix}{key.zfill(10)}"
                members[key] = content
        return members

    @staticmethod
    def _resolve_primary_ticker(
        companyfacts_payload: dict[str, Any] | None,
        submissions_payload: dict[str, Any] | None,
    ) -> str | None:
        if companyfacts_payload and companyfacts_payload.get("tickers"):
            tickers = companyfacts_payload["tickers"]
            if tickers:
                return str(tickers[0]).upper()
        if submissions_payload and submissions_payload.get("tickers"):
            tickers = submissions_payload["tickers"]
            if tickers:
                return str(tickers[0]).upper()
        return None

    @staticmethod
    def _insert_company_facts(
        *,
        connection,
        cik: str,
        ticker: str,
        payload: dict[str, Any],
        metrics: set[str],
    ) -> int:
        inserted = 0
        facts = payload.get("facts") or {}
        for taxonomy_payload in facts.values():
            for metric, metric_payload in taxonomy_payload.items():
                if metric not in metrics:
                    continue
                units = metric_payload.get("units") or {}
                for unit_name, observations in units.items():
                    for item in observations:
                        period_end = item.get("end")
                        if not period_end:
                            continue
                        connection.execute(
                            """
                            INSERT INTO fact_company_fundamentals(
                                cik, ticker, metric, period_end, fiscal_period, fiscal_year, value, unit,
                                form_type, filed_at, frame, accession_number
                            )
                            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(ticker, metric, period_end, form_type, frame) DO UPDATE SET
                              value = excluded.value,
                              fiscal_period = excluded.fiscal_period,
                              fiscal_year = excluded.fiscal_year,
                              unit = excluded.unit,
                              filed_at = excluded.filed_at,
                              accession_number = excluded.accession_number
                            """,
                            (
                                cik,
                                ticker,
                                metric,
                                period_end,
                                item.get("fp"),
                                item.get("fy"),
                                item.get("val"),
                                unit_name,
                                item.get("form") or "",
                                item.get("filed"),
                                item.get("frame") or "",
                                item.get("accn"),
                            ),
                        )
                        inserted += 1
        return inserted

    @staticmethod
    def _insert_submissions(*, connection, cik: str, ticker: str, payload: dict[str, Any]) -> int:
        filings = payload.get("filings", {}).get("recent", {})
        accession_numbers = filings.get("accessionNumber", [])
        forms = filings.get("form", [])
        filed_dates = filings.get("filingDate", [])
        acceptance_dates = filings.get("acceptanceDateTime", [])
        primary_documents = filings.get("primaryDocument", [])
        is_xbrl = filings.get("isXBRL", [])
        is_inline_xbrl = filings.get("isInlineXBRL", [])
        inserted = 0
        for index, accession_number in enumerate(accession_numbers):
            connection.execute(
                """
                INSERT INTO fact_company_filings(
                    cik, ticker, accession_number, form_type, filed_at, acceptance_datetime,
                    primary_document, is_xbrl, is_inline_xbrl
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(accession_number) DO UPDATE SET
                  form_type = excluded.form_type,
                  filed_at = excluded.filed_at,
                  acceptance_datetime = excluded.acceptance_datetime,
                  primary_document = excluded.primary_document,
                  is_xbrl = excluded.is_xbrl,
                  is_inline_xbrl = excluded.is_inline_xbrl
                """,
                (
                    cik,
                    ticker,
                    accession_number,
                    forms[index] if index < len(forms) else "",
                    filed_dates[index] if index < len(filed_dates) else None,
                    acceptance_dates[index] if index < len(acceptance_dates) else None,
                    primary_documents[index] if index < len(primary_documents) else None,
                    int(bool(is_xbrl[index])) if index < len(is_xbrl) else 0,
                    int(bool(is_inline_xbrl[index])) if index < len(is_inline_xbrl) else 0,
                ),
            )
            inserted += 1
        return inserted

    def _start_run(self, *, run_id: str, source: str, started_at: str) -> None:
        with self.database.connect() as connection:
            connection.execute(
                """
                INSERT INTO ingestion_runs(run_id, source, started_at, status, row_count)
                VALUES(?, ?, ?, ?, 0)
                """,
                (run_id, source, started_at, "running"),
            )

    def _finish_run(
        self,
        *,
        run_id: str,
        status: str,
        row_count: int,
        watermark: str | None,
        details: dict[str, Any],
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
                    watermark,
                    json.dumps(details),
                    run_id,
                ),
            )
