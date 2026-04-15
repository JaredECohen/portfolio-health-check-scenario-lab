from __future__ import annotations

import asyncio
from typing import Any

from app.services.news_intel import NewsIntelService


class DummyCache:
    def __init__(self) -> None:
        self.items: dict[str, Any] = {}

    def get_json(self, cache_key: str) -> Any | None:
        return self.items.get(cache_key)

    def set_json(self, cache_key: str, payload: Any, *, source: str, ttl_seconds: int | None = None) -> None:  # noqa: ARG002
        self.items[cache_key] = payload


class StubAlphaVantage:
    async def get_news_sentiment(self, *, tickers=None, topics=None, keywords=None, limit=20):  # noqa: ANN001, ARG002
        return {
            "feed": [
                {
                    "title": "Apple faces inflation pressure",
                    "url": "https://example.com/apple-inflation",
                    "time_published": "20250822T120000",
                    "summary": "Apple and Microsoft discussed inflation and rates.",
                    "overall_sentiment_score": "-0.12",
                    "ticker_sentiment": [{"ticker": "AAPL", "relevance_score": "0.91"}],
                    "topics": [{"topic": "technology"}],
                }
            ]
        }


class StubNewsIntelService(NewsIntelService):
    async def _get_json(self, *, cache_label: str, url: str, headers=None, ttl_seconds: int = 60 * 30):  # noqa: ANN001, ARG002
        if "gdeltproject" in url:
            return {
                "articles": [
                    {
                        "title": "Inflation fears ripple through markets",
                        "url": "https://news.example.com/inflation-fears",
                        "seendate": "2025-08-22T10:00:00Z",
                        "domain": "news.example.com",
                    }
                ]
            }
        raise AssertionError(f"Unexpected URL: {url}")


def test_news_intel_collects_and_normalizes_multiple_sources() -> None:
    service = StubNewsIntelService(
        alpha_vantage=StubAlphaVantage(),
        cache=DummyCache(),
    )

    result = asyncio.run(
        service.collect(
            question="How exposed is Apple to rates and inflation?",
            tickers=["AAPL", "MSFT"],
            topics=["rates", "inflation"],
        )
    )

    assert "Alpha Vantage NEWS_SENTIMENT" in result.retrieval_sources
    assert "GDELT DOC 2.0" in result.retrieval_sources
    assert len(result.articles) >= 2
    assert "rates" in result.dominant_topics
    assert any(article.source == "Alpha Vantage NEWS_SENTIMENT" for article in result.articles)
