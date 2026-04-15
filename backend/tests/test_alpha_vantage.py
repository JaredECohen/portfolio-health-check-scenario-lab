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
