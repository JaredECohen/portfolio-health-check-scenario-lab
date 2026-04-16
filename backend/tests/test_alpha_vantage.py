from __future__ import annotations

import asyncio
from typing import Any

from app.services.alpha_vantage import AlphaVantageService


class DummyCache:
    def get_json(self, cache_key: str) -> Any | None:  # noqa: ARG002
        return None

    def set_json(self, cache_key: str, payload: Any, *, source: str, ttl_seconds: int | None = None) -> None:  # noqa: ARG002
        return None


class StubAlphaVantage(AlphaVantageService):
    def __init__(self) -> None:
        super().__init__(api_key="test", cache=DummyCache())

    async def _request(self, *, params: dict[str, Any], ttl_seconds: int = 60 * 60 * 12) -> Any:  # noqa: ARG002
        if params["function"] == "TIME_SERIES_DAILY_ADJUSTED":
            return {
                "Time Series (Daily)": {
                    "2024-05-03": {
                        "4. close": "170.10",
                        "5. adjusted close": "169.55",
                        "6. volume": "1234567",
                    },
                    "2024-05-02": {
                        "4. close": "168.00",
                        "5. adjusted close": "167.25",
                        "6. volume": "7654321",
                    },
                }
            }
        if params["function"] == "TIME_SERIES_DAILY":
            return {
                "Time Series (Daily)": {
                    "2024-05-03": {
                        "4. close": "170.10",
                        "5. volume": "1234567",
                    },
                    "2024-05-02": {
                        "4. close": "168.00",
                        "5. volume": "7654321",
                    },
                }
            }
        if params["function"] == "TREASURY_YIELD":
            return {
                "data": [
                    {"date": "2024-05-03", "value": "4.56"},
                    {"date": "2024-05-02", "value": "4.61"},
                    {"date": "2024-05-01", "value": "."},
                ]
            }
        if params["function"] == "EARNINGS":
            return {
                "quarterlyEarnings": [
                    {
                        "fiscalDateEnding": "2024-03-31",
                        "reportedDate": "2024-05-02",
                    }
                ]
            }
        if params["function"] == "EARNINGS_CALL_TRANSCRIPT":
            return [{"speaker": "CEO", "content": "Prepared remarks"}]
        raise AssertionError(f"Unexpected params: {params}")


def test_windowed_earnings_transcript_uses_reported_date() -> None:
    service = StubAlphaVantage()

    transcript = asyncio.run(
        service.get_windowed_earnings_transcript(
            "AAPL",
            start_date=__import__("datetime").date(2024, 5, 1),
            end_date=__import__("datetime").date(2024, 5, 3),
        )
    )

    assert transcript is not None
    assert transcript["quarter"] == "2024Q1"
    assert transcript["event_date"] == "2024-05-02"


def test_daily_adjusted_history_is_vectorized_and_sorted() -> None:
    service = StubAlphaVantage()

    frame = asyncio.run(service.get_daily_adjusted("AAPL", outputsize="full"))

    assert list(frame.columns) == ["adjusted_close", "close", "volume"]
    assert frame.index.name == "date"
    assert frame.index[0].isoformat() == "2024-05-02T00:00:00"
    assert float(frame.iloc[-1]["adjusted_close"]) == 170.10


class DailyUnavailableAlphaVantage(StubAlphaVantage):
    async def _request(self, *, params: dict[str, Any], ttl_seconds: int = 60 * 60 * 12) -> Any:  # noqa: ARG002
        if params["function"] == "TIME_SERIES_DAILY":
            return {"Information": "No free daily data available for this symbol"}
        return await super()._request(params=params, ttl_seconds=ttl_seconds)


def test_daily_adjusted_history_falls_back_to_adjusted_endpoint() -> None:
    service = DailyUnavailableAlphaVantage()

    frame = asyncio.run(service.get_daily_adjusted("AAPL", outputsize="full"))

    assert list(frame.columns) == ["adjusted_close", "close", "volume"]
    assert frame.index.name == "date"
    assert frame.index[0].isoformat() == "2024-05-02T00:00:00"
    assert float(frame.iloc[-1]["adjusted_close"]) == 169.55
    assert float(frame.iloc[-1]["close"]) == 170.10


class FullHistoryNormalizedAlphaVantage(StubAlphaVantage):
    def __init__(self) -> None:
        super().__init__()
        self.daily_outputsizes: list[str] = []

    async def _request(self, *, params: dict[str, Any], ttl_seconds: int = 60 * 60 * 12) -> Any:  # noqa: ARG002
        if params["function"] == "TIME_SERIES_DAILY":
            self.daily_outputsizes.append(params["outputsize"])
        return await super()._request(params=params, ttl_seconds=ttl_seconds)


def test_daily_adjusted_history_normalizes_full_to_compact() -> None:
    service = FullHistoryNormalizedAlphaVantage()

    frame = asyncio.run(service.get_daily_adjusted("AAPL", outputsize="full"))

    assert list(frame.columns) == ["adjusted_close", "close", "volume"]
    assert frame.index.name == "date"
    assert frame.index[0].isoformat() == "2024-05-02T00:00:00"
    assert float(frame.iloc[-1]["adjusted_close"]) == 170.10
    assert service.daily_outputsizes == ["compact"]


def test_economic_series_history_is_vectorized_and_filters_missing_values() -> None:
    service = StubAlphaVantage()

    frame = asyncio.run(service.get_treasury_yield())

    assert list(frame.columns) == ["value"]
    assert frame.index.name == "date"
    assert len(frame) == 2
    assert frame.index[0].isoformat() == "2024-05-02T00:00:00"
    assert float(frame.iloc[-1]["value"]) == 4.56
