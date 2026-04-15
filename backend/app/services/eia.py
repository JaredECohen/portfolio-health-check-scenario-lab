from __future__ import annotations

import hashlib
import json
import re
from typing import Any

import httpx

from app.services.cache import CacheService


class EIAServiceError(RuntimeError):
    pass


class EIAService:
    PETROLEUM_URL = "https://ir.eia.gov/wpsr/table4.csv"
    NATGAS_URL = "https://ir.eia.gov/ngs/wngsr.json"

    def __init__(self, api_key: str | None, cache: CacheService) -> None:
        self.api_key = api_key
        self.cache = cache

    def _cache_key(self, label: str) -> str:
        digest = hashlib.sha256(label.encode("utf-8")).hexdigest()
        return f"eia:{digest}"

    async def _get_text(self, *, url: str, ttl_seconds: int = 60 * 60 * 12) -> str:
        cache_key = self._cache_key(url)
        cached = self.cache.get_json(cache_key)
        if cached is not None:
            return str(cached["text"])
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            text = response.text
        self.cache.set_json(cache_key, {"text": text}, source="eia", ttl_seconds=ttl_seconds)
        return text

    async def get_petroleum_storage_snapshot(self) -> dict[str, Any]:
        text = await self._get_text(url=self.PETROLEUM_URL, ttl_seconds=60 * 60 * 6)
        report_date_match = re.search(
            r'"Crude oil and petroleum products","Week ending ([A-Za-z]+\s+\d{1,2},\s+\d{4})"',
            text,
        )
        commercial_match = re.search(
            r'"Commercial \(Excluding SPR\)","(?P<current>-?\d+(?:\.\d+)?)","(?P<previous>-?\d+(?:\.\d+)?)","(?P<change>-?\d+(?:\.\d+)?)"',
            text,
        )
        total_match = re.search(
            r'"Total Stocks Excluding SPR","(?P<current>-?\d+(?:\.\d+)?)","(?P<previous>-?\d+(?:\.\d+)?)","(?P<change>-?\d+(?:\.\d+)?)"',
            text,
        )
        if commercial_match is None:
            raise EIAServiceError("Could not parse EIA petroleum storage snapshot.")
        return {
            "report_date": report_date_match.group(1) if report_date_match else None,
            "commercial_crude": {
                "level_million_bbl": float(commercial_match.group("current")),
                "previous_million_bbl": float(commercial_match.group("previous")),
                "weekly_change_million_bbl": float(commercial_match.group("change")),
            },
            "total_ex_spr": {
                "level_million_bbl": float(total_match.group("current")) if total_match else None,
                "previous_million_bbl": float(total_match.group("previous")) if total_match else None,
                "weekly_change_million_bbl": float(total_match.group("change")) if total_match else None,
            },
        }

    async def get_natgas_storage_snapshot(self) -> dict[str, Any]:
        text = await self._get_text(url=self.NATGAS_URL, ttl_seconds=60 * 60 * 6)
        payload = json.loads(text)
        report = payload[0] if isinstance(payload, list) and payload else {}
        series = report.get("series") or []
        total_row = next(
            (
                item
                for item in series
                if str(item.get("name", "")).strip().lower() == "total"
            ),
            None,
        )
        if total_row is None:
            raise EIAServiceError("Could not parse EIA natural gas storage snapshot.")
        return {
            "report_date": report.get("report_date"),
            "total_lower_48": {
                "working_gas_bcf": float(total_row["value"]),
                "net_change_bcf": float(total_row["net_change"]),
                "year_ago_bcf": float(total_row["year_ago"]) if total_row.get("year_ago") not in (None, "") else None,
                "five_year_avg_bcf": (
                    float(total_row["five_year_avg"]) if total_row.get("five_year_avg") not in (None, "") else None
                ),
                "vs_5y_pct": float(total_row["pct_chg_five_year_avg"])
                if total_row.get("pct_chg_five_year_avg") not in (None, "")
                else None,
            },
        }
