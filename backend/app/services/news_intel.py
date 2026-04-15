from __future__ import annotations

import hashlib
from collections import Counter
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote_plus, urlparse

import httpx

from app.models.schemas import NewsArticle, NewsIntelResult, NewsSourceStats
from app.services.alpha_vantage import AlphaVantageError, AlphaVantageService
from app.services.cache import CacheService


TOPIC_KEYWORDS: dict[str, tuple[str, ...]] = {
    "rates": ("rate", "yield", "fed", "treasury", "duration"),
    "inflation": ("inflation", "cpi", "pricing", "prices"),
    "energy": ("energy", "oil", "crude", "brent", "gas", "lng"),
    "geopolitics": ("war", "conflict", "sanction", "tariff", "geopolitic"),
    "earnings": ("earnings", "guidance", "quarter", "results"),
    "credit": ("credit", "spread", "refinancing", "debt"),
    "growth": ("consumer", "retail", "gdp", "recession", "demand"),
}


class NewsIntelService:
    def __init__(
        self,
        *,
        alpha_vantage: AlphaVantageService,
        cache: CacheService,
    ) -> None:
        self.alpha_vantage = alpha_vantage
        self.cache = cache

    async def collect(
        self,
        *,
        question: str,
        tickers: list[str],
        topics: list[str],
        limit_per_source: int = 12,
    ) -> NewsIntelResult:
        query = self._build_query(question=question, tickers=tickers, topics=topics)
        retrieval_sources: list[str] = ["Alpha Vantage NEWS_SENTIMENT", "GDELT DOC 2.0"]
        articles: list[NewsArticle] = []
        caveats: list[str] = []

        alpha_items = await self._fetch_alpha_vantage_news(
            question=question,
            tickers=tickers,
            topics=topics,
            limit=limit_per_source,
        )
        articles.extend(alpha_items)
        articles.extend(await self._fetch_gdelt(query=query, limit=limit_per_source))

        normalized = self._normalize_articles(articles=articles, tickers=tickers, topics=topics)
        source_stats = self._source_stats(normalized)
        dominant_topics = self._dominant_topics(normalized)
        if not alpha_items:
            caveats.append("Alpha Vantage NEWS_SENTIMENT returned no usable items for this query.")
        if not normalized:
            caveats.append("No external news records were available after normalization.")
        return NewsIntelResult(
            query=query,
            retrieval_sources=retrieval_sources,
            articles=normalized[:25],
            source_stats=source_stats,
            dominant_topics=dominant_topics,
            caveats=list(dict.fromkeys(caveats)),
        )

    def _build_query(self, *, question: str, tickers: list[str], topics: list[str]) -> str:
        query_parts = [question.strip()]
        if tickers:
            query_parts.append(" OR ".join(sorted(set(tickers))))
        if topics:
            query_parts.append(" OR ".join(sorted(set(topics))))
        return " ".join(part for part in query_parts if part).strip()

    def _cache_key(self, label: str) -> str:
        digest = hashlib.sha256(label.encode("utf-8")).hexdigest()
        return f"news_intel:{digest}"

    async def _get_json(
        self,
        *,
        cache_label: str,
        url: str,
        headers: dict[str, str] | None = None,
        ttl_seconds: int = 60 * 30,
    ) -> Any:
        cache_key = self._cache_key(cache_label)
        cached = self.cache.get_json(cache_key)
        if cached is not None:
            return cached
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            payload = response.json()
        self.cache.set_json(cache_key, payload, source="news_intel", ttl_seconds=ttl_seconds)
        return payload

    async def _fetch_alpha_vantage_news(
        self,
        *,
        question: str,
        tickers: list[str],
        topics: list[str],
        limit: int,
    ) -> list[NewsArticle]:
        try:
            payload = await self.alpha_vantage.get_news_sentiment(
                tickers=tickers,
                topics=topics or None,
                keywords=question,
                limit=limit,
            )
        except AlphaVantageError:
            return []
        rows: list[NewsArticle] = []
        for item in payload.get("feed", []):
            sentiment = item.get("overall_sentiment_score")
            ticker_sentiment = item.get("ticker_sentiment") or []
            rows.append(
                NewsArticle(
                    source="Alpha Vantage NEWS_SENTIMENT",
                    source_type="news",
                    title=item.get("title") or "",
                    url=item.get("url") or "",
                    published_at=self._normalize_timestamp(item.get("time_published")),
                    domain=urlparse(item.get("url") or "").netloc or None,
                    summary=item.get("summary"),
                    sentiment=float(sentiment) if sentiment not in (None, "") else None,
                    relevance=float(ticker_sentiment[0].get("relevance_score"))
                    if ticker_sentiment and ticker_sentiment[0].get("relevance_score") not in (None, "")
                    else None,
                    tickers=[entry.get("ticker", "") for entry in ticker_sentiment if entry.get("ticker")],
                    topics=[entry.get("topic", "") for entry in item.get("topics", []) if entry.get("topic")],
                )
            )
        return rows

    async def _fetch_gdelt(self, *, query: str, limit: int) -> list[NewsArticle]:
        url = (
            "https://api.gdeltproject.org/api/v2/doc/doc"
            f"?query={quote_plus(query)}&mode=artlist&maxrecords={max(1, min(limit, 50))}&format=json&sort=datedesc"
        )
        try:
            payload = await self._get_json(cache_label=f"gdelt:{query}:{limit}", url=url)
        except Exception:  # noqa: BLE001
            return []
        rows: list[NewsArticle] = []
        for item in payload.get("articles", []):
            rows.append(
                NewsArticle(
                    source="GDELT DOC 2.0",
                    source_type="news",
                    title=item.get("title") or "",
                    url=item.get("url") or "",
                    published_at=item.get("seendate"),
                    domain=urlparse(item.get("domain") or item.get("url") or "").netloc or item.get("domain"),
                    summary=item.get("socialimage") or item.get("sourcecountry"),
                )
            )
        return rows

    def _normalize_articles(
        self,
        *,
        articles: list[NewsArticle],
        tickers: list[str],
        topics: list[str],
    ) -> list[NewsArticle]:
        deduped: dict[str, NewsArticle] = {}
        for article in articles:
            if not article.url or not article.title:
                continue
            key = self._dedupe_key(article.url)
            tagged = article.model_copy(
                update={
                    "tickers": sorted(set([*article.tickers, *self._infer_tickers(article=article, tickers=tickers)])),
                    "topics": sorted(set([*article.topics, *self._infer_topics(article=article, topics=topics)])),
                }
            )
            existing = deduped.get(key)
            if existing is None or self._article_rank(tagged) > self._article_rank(existing):
                deduped[key] = tagged
        normalized = sorted(deduped.values(), key=self._article_rank, reverse=True)
        return normalized

    @staticmethod
    def _dedupe_key(url: str) -> str:
        parsed = urlparse(url)
        normalized = f"{parsed.netloc.lower()}{parsed.path.rstrip('/')}"
        return normalized or url

    @staticmethod
    def _normalize_timestamp(value: str | None) -> str | None:
        if not value:
            return None
        if len(value) == 15 and value.endswith("00"):
            return datetime.strptime(value, "%Y%m%dT%H%M%S").replace(tzinfo=UTC).isoformat()
        return value

    def _infer_tickers(self, *, article: NewsArticle, tickers: list[str]) -> list[str]:
        haystack = " ".join([article.title, article.summary or ""]).upper()
        return [ticker for ticker in tickers if ticker in haystack]

    def _infer_topics(self, *, article: NewsArticle, topics: list[str]) -> list[str]:
        haystack = " ".join([article.title, article.summary or ""]).lower()
        inferred = [topic for topic in topics if topic.lower() in haystack]
        for topic, keywords in TOPIC_KEYWORDS.items():
            if any(keyword in haystack for keyword in keywords):
                inferred.append(topic)
        return inferred

    def _source_stats(self, articles: list[NewsArticle]) -> list[NewsSourceStats]:
        grouped: dict[str, list[NewsArticle]] = {}
        for article in articles:
            grouped.setdefault(article.source, []).append(article)
        rows: list[NewsSourceStats] = []
        for source, items in grouped.items():
            sentiments = [item.sentiment for item in items if item.sentiment is not None]
            latest = max((item.published_at for item in items if item.published_at), default=None)
            rows.append(
                NewsSourceStats(
                    source=source,
                    article_count=len(items),
                    avg_sentiment=(sum(sentiments) / len(sentiments)) if sentiments else None,
                    latest_published_at=latest,
                )
            )
        rows.sort(key=lambda item: item.article_count, reverse=True)
        return rows

    def _dominant_topics(self, articles: list[NewsArticle]) -> list[str]:
        counts = Counter(topic for article in articles for topic in article.topics)
        return [topic for topic, _count in counts.most_common(6)]

    @staticmethod
    def _article_rank(article: NewsArticle) -> tuple[float, float, str]:
        relevance = float(article.relevance or 0.0)
        sentiment = abs(float(article.sentiment or 0.0))
        published = article.published_at or ""
        return (relevance, sentiment, published)
