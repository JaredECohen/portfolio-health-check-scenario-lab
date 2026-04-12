from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any

import httpx
import pandas as pd

from app.services.cache import CacheService


class AlphaVantageError(RuntimeError):
    pass


class AlphaVantageService:
    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(self, api_key: str | None, cache: CacheService) -> None:
        self.api_key = api_key
        self.cache = cache

    def _cache_key(self, params: dict[str, Any]) -> str:
        digest = hashlib.sha256(repr(sorted(params.items())).encode("utf-8")).hexdigest()
        return f"alpha_vantage:{digest}"

    async def _request(
        self,
        *,
        params: dict[str, Any],
        ttl_seconds: int = 60 * 60 * 12,
    ) -> Any:
        if not self.api_key:
            raise AlphaVantageError("ALPHA_VANTAGE_API_KEY is not configured.")
        full_params = {**params, "apikey": self.api_key}
        cache_key = self._cache_key(full_params)
        cached = self.cache.get_json(cache_key)
        if cached is not None:
            return cached
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(self.BASE_URL, params=full_params)
            response.raise_for_status()
            payload = response.json()
        if "Error Message" in payload:
            raise AlphaVantageError(payload["Error Message"])
        if "Note" in payload:
            raise AlphaVantageError(payload["Note"])
        self.cache.set_json(cache_key, payload, source="alpha_vantage", ttl_seconds=ttl_seconds)
        return payload

    async def get_daily_adjusted(self, symbol: str, *, outputsize: str = "compact") -> pd.DataFrame:
        payload = await self._request(
            params={
                "function": "TIME_SERIES_DAILY_ADJUSTED",
                "symbol": symbol,
                "outputsize": outputsize,
            },
        )
        time_series = payload.get("Time Series (Daily)", {})
        rows = []
        for date_str, values in time_series.items():
            rows.append(
                {
                    "date": pd.to_datetime(date_str),
                    "adjusted_close": float(values["5. adjusted close"]),
                    "close": float(values["4. close"]),
                    "volume": float(values["6. volume"]),
                }
            )
        if not rows:
            raise AlphaVantageError(f"No daily price history returned for {symbol}.")
        frame = pd.DataFrame(rows).sort_values("date").set_index("date")
        return frame

    async def get_company_overview(self, symbol: str) -> dict[str, Any]:
        return await self._request(
            params={"function": "OVERVIEW", "symbol": symbol},
            ttl_seconds=60 * 60 * 24 * 7,
        )

    async def get_treasury_yield(self, maturity: str = "10year") -> pd.DataFrame:
        payload = await self._request(
            params={"function": "TREASURY_YIELD", "interval": "daily", "maturity": maturity},
            ttl_seconds=60 * 60 * 24,
        )
        rows = [
            {"date": pd.to_datetime(item["date"]), "value": float(item["value"])}
            for item in payload.get("data", [])
            if item.get("value") not in (None, ".")
        ]
        return pd.DataFrame(rows).sort_values("date").set_index("date")

    async def get_cpi(self) -> pd.DataFrame:
        payload = await self._request(
            params={"function": "CPI", "interval": "monthly"},
            ttl_seconds=60 * 60 * 24,
        )
        rows = [
            {"date": pd.to_datetime(item["date"]), "value": float(item["value"])}
            for item in payload.get("data", [])
            if item.get("value") not in (None, ".")
        ]
        return pd.DataFrame(rows).sort_values("date").set_index("date")

    async def get_wti(self) -> pd.DataFrame:
        payload = await self._request(
            params={"function": "WTI", "interval": "daily"},
            ttl_seconds=60 * 60 * 24,
        )
        rows = [
            {"date": pd.to_datetime(item["date"]), "value": float(item["value"])}
            for item in payload.get("data", [])
            if item.get("value") not in (None, ".")
        ]
        return pd.DataFrame(rows).sort_values("date").set_index("date")

    async def get_latest_earnings_transcript(self, symbol: str) -> dict[str, Any] | None:
        now = datetime.now(UTC)
        quarter = ((now.month - 1) // 3) + 1
        candidates = []
        for offset in range(0, 8):
            quarter_num = quarter - offset
            year = now.year
            while quarter_num <= 0:
                quarter_num += 4
                year -= 1
            candidates.append(f"{year}Q{quarter_num}")
        for quarter_id in candidates:
            try:
                payload = await self._request(
                    params={
                        "function": "EARNINGS_CALL_TRANSCRIPT",
                        "symbol": symbol,
                        "quarter": quarter_id,
                    },
                    ttl_seconds=60 * 60 * 24 * 7,
                )
            except AlphaVantageError:
                continue
            if payload:
                if isinstance(payload, list) and payload:
                    return {"quarter": quarter_id, "items": payload}
                if isinstance(payload, dict) and payload.get("transcript"):
                    return {"quarter": quarter_id, "items": payload["transcript"]}
        return None

