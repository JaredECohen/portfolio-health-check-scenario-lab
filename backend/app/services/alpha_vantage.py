from __future__ import annotations

import asyncio
import hashlib
import random
from datetime import UTC, date, datetime
from typing import Any

import httpx
import pandas as pd

from app.services.cache import CacheService


class AlphaVantageError(RuntimeError):
    pass


class AlphaVantageService:
    BASE_URL = "https://www.alphavantage.co/query"
    MAX_RETRIES = 3

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
        last_error: Exception | None = None
        async with httpx.AsyncClient(timeout=30.0) as client:
            for attempt in range(1, self.MAX_RETRIES + 1):
                try:
                    response = await client.get(self.BASE_URL, params=full_params)
                    response.raise_for_status()
                    payload = response.json()
                    if "Error Message" in payload:
                        raise AlphaVantageError(payload["Error Message"])
                    if "Note" in payload:
                        if attempt == self.MAX_RETRIES:
                            raise AlphaVantageError(
                                "Alpha Vantage rate limit or transient note prevented this request."
                            )
                        await self._backoff(attempt)
                        continue
                    self.cache.set_json(
                        cache_key,
                        payload,
                        source="alpha_vantage",
                        ttl_seconds=ttl_seconds,
                    )
                    return payload
                except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                    last_error = exc
                    status_code = getattr(getattr(exc, "response", None), "status_code", None)
                    if status_code not in {None, 429, 500, 502, 503, 504}:
                        break
                    if attempt == self.MAX_RETRIES:
                        break
                    await self._backoff(attempt)
        raise AlphaVantageError(f"Alpha Vantage request failed after retries: {last_error}")

    async def get_daily_adjusted(self, symbol: str, *, outputsize: str = "compact") -> pd.DataFrame:
        payload = await self._request(
            params={
                "function": "TIME_SERIES_DAILY_ADJUSTED",
                "symbol": symbol,
                "outputsize": outputsize,
            },
        )
        time_series = payload.get("Time Series (Daily)", {})
        if not time_series:
            raise AlphaVantageError(f"No daily price history returned for {symbol}.")
        frame = (
            pd.DataFrame.from_dict(time_series, orient="index")
            .rename(
                columns={
                    "5. adjusted close": "adjusted_close",
                    "4. close": "close",
                    "6. volume": "volume",
                }
            )
            .loc[:, ["adjusted_close", "close", "volume"]]
        )
        frame.index = pd.to_datetime(frame.index, format="%Y-%m-%d", errors="coerce")
        frame = frame[frame.index.notna()]
        if frame.empty:
            raise AlphaVantageError(f"No parseable daily price history returned for {symbol}.")
        frame = frame.astype(float).sort_index()
        frame.index.name = "date"
        return frame

    async def get_company_overview(self, symbol: str) -> dict[str, Any]:
        return await self._request(
            params={"function": "OVERVIEW", "symbol": symbol},
            ttl_seconds=60 * 60 * 24 * 7,
        )

    async def get_news_sentiment(
        self,
        *,
        tickers: list[str] | None = None,
        topics: list[str] | None = None,
        keywords: str | None = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"function": "NEWS_SENTIMENT", "limit": max(1, min(limit, 50))}
        if tickers:
            params["tickers"] = ",".join(sorted(set(tickers)))
        if topics:
            params["topics"] = ",".join(sorted(set(topics)))
        if keywords:
            params["keywords"] = keywords
        return await self._request(params=params, ttl_seconds=60 * 30)

    async def get_treasury_yield(self, maturity: str = "10year") -> pd.DataFrame:
        return await self.get_economic_series(
            function_name="TREASURY_YIELD",
            interval="daily",
            maturity=maturity,
        )

    async def get_cpi(self) -> pd.DataFrame:
        return await self.get_economic_series(function_name="CPI", interval="monthly")

    async def get_wti(self) -> pd.DataFrame:
        return await self.get_commodity_series(function_name="WTI")

    async def get_brent(self) -> pd.DataFrame:
        return await self.get_commodity_series(function_name="BRENT")

    async def get_natural_gas(self) -> pd.DataFrame:
        return await self.get_commodity_series(function_name="NATURAL_GAS")

    async def get_inflation_expectation(self) -> pd.DataFrame:
        return await self.get_economic_series(function_name="INFLATION_EXPECTATION", interval="monthly")

    async def get_federal_funds_rate(self) -> pd.DataFrame:
        return await self.get_economic_series(function_name="FEDERAL_FUNDS_RATE", interval="daily")

    async def get_unemployment(self) -> pd.DataFrame:
        return await self.get_economic_series(function_name="UNEMPLOYMENT", interval="monthly")

    async def get_retail_sales(self) -> pd.DataFrame:
        return await self.get_economic_series(function_name="RETAIL_SALES", interval="monthly")

    async def get_consumer_sentiment(self) -> pd.DataFrame:
        return await self.get_economic_series(function_name="CONSUMER_SENTIMENT", interval="monthly")

    async def get_real_gdp(self) -> pd.DataFrame:
        return await self.get_economic_series(function_name="REAL_GDP", interval="quarterly")

    async def get_commodity_series(self, *, function_name: str, interval: str = "daily") -> pd.DataFrame:
        payload = await self._request(
            params={"function": function_name, "interval": interval},
            ttl_seconds=60 * 60 * 24,
        )
        return self._frame_from_payload(payload)

    async def get_economic_series(
        self,
        *,
        function_name: str,
        interval: str,
        maturity: str | None = None,
    ) -> pd.DataFrame:
        params: dict[str, Any] = {"function": function_name, "interval": interval}
        if maturity is not None:
            params["maturity"] = maturity
        payload = await self._request(
            params=params,
            ttl_seconds=60 * 60 * 24,
        )
        return self._frame_from_payload(payload)

    @staticmethod
    def _frame_from_payload(payload: dict[str, Any]) -> pd.DataFrame:
        frame = pd.DataFrame(payload.get("data", []))
        if frame.empty or "date" not in frame.columns or "value" not in frame.columns:
            return pd.DataFrame(columns=["value"], index=pd.DatetimeIndex([], name="date"))
        frame = frame.loc[frame["value"].notna() & (frame["value"] != "."), ["date", "value"]].copy()
        frame["date"] = pd.to_datetime(frame["date"], format="%Y-%m-%d", errors="coerce")
        frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
        frame = frame.dropna(subset=["date", "value"])
        if frame.empty:
            return pd.DataFrame(columns=["value"], index=pd.DatetimeIndex([], name="date"))
        return frame.sort_values("date").set_index("date")

    async def get_latest_earnings_transcript(self, symbol: str) -> dict[str, Any] | None:
        metadata = await self.get_quarterly_earnings_metadata(symbol)
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
                    return {
                        "quarter": quarter_id,
                        "items": payload,
                        "event_date": metadata.get(quarter_id, {}).get("reported_date"),
                    }
                if isinstance(payload, dict) and payload.get("transcript"):
                    return {
                        "quarter": quarter_id,
                        "items": payload["transcript"],
                        "event_date": metadata.get(quarter_id, {}).get("reported_date"),
                    }
        return None

    async def get_quarterly_earnings_metadata(self, symbol: str) -> dict[str, dict[str, str | None]]:
        payload = await self._request(
            params={"function": "EARNINGS", "symbol": symbol},
            ttl_seconds=60 * 60 * 24,
        )
        metadata: dict[str, dict[str, str | None]] = {}
        for item in payload.get("quarterlyEarnings", []):
            fiscal_date = item.get("fiscalDateEnding")
            if not fiscal_date:
                continue
            quarter_id = self._quarter_id_from_date(date.fromisoformat(fiscal_date))
            metadata[quarter_id] = {
                "reported_date": item.get("reportedDate"),
                "fiscal_date": fiscal_date,
            }
        return metadata

    async def get_windowed_earnings_transcript(
        self,
        symbol: str,
        *,
        start_date: date | None,
        end_date: date | None,
    ) -> dict[str, Any] | None:
        if start_date is None and end_date is None:
            return await self.get_latest_earnings_transcript(symbol)
        metadata = await self.get_quarterly_earnings_metadata(symbol)
        now = datetime.now(UTC)
        quarter = ((now.month - 1) // 3) + 1
        for offset in range(0, 12):
            quarter_num = quarter - offset
            year = now.year
            while quarter_num <= 0:
                quarter_num += 4
                year -= 1
            quarter_id = f"{year}Q{quarter_num}"
            event_date = self._resolve_transcript_event_date(
                metadata=metadata,
                quarter_id=quarter_id,
                year=year,
                quarter=quarter_num,
            )
            if start_date and event_date < start_date:
                continue
            if end_date and event_date > end_date:
                continue
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
                    return {
                        "quarter": quarter_id,
                        "items": payload,
                        "event_date": event_date.isoformat(),
                    }
                if isinstance(payload, dict) and payload.get("transcript"):
                    return {
                        "quarter": quarter_id,
                        "items": payload["transcript"],
                        "event_date": event_date.isoformat(),
                    }
        return None

    @staticmethod
    def _quarter_end_date(year: int, quarter: int) -> date:
        if quarter == 1:
            return date(year, 3, 31)
        if quarter == 2:
            return date(year, 6, 30)
        if quarter == 3:
            return date(year, 9, 30)
        return date(year, 12, 31)

    @staticmethod
    def _quarter_id_from_date(value: date) -> str:
        quarter = ((value.month - 1) // 3) + 1
        return f"{value.year}Q{quarter}"

    def _resolve_transcript_event_date(
        self,
        *,
        metadata: dict[str, dict[str, str | None]],
        quarter_id: str,
        year: int,
        quarter: int,
    ) -> date:
        reported_date = metadata.get(quarter_id, {}).get("reported_date")
        if reported_date:
            return date.fromisoformat(reported_date)
        return self._quarter_end_date(year, quarter)

    @staticmethod
    async def _backoff(attempt: int) -> None:
        delay_seconds = (0.4 * (2 ** (attempt - 1))) + random.uniform(0.0, 0.2)
        await asyncio.sleep(delay_seconds)
