from __future__ import annotations

import hashlib
import re
from typing import Any

import httpx

from app.services.cache import CacheService


class SecEdgarService:
    BASE_JSON_URL = "https://data.sec.gov"
    BASE_ARCHIVES_URL = "https://www.sec.gov/Archives/edgar/data"

    def __init__(self, user_agent: str, cache: CacheService) -> None:
        self.user_agent = user_agent
        self.cache = cache

    async def _request_json(self, url: str, ttl_seconds: int = 60 * 60 * 24) -> Any:
        cache_key = f"sec-json:{hashlib.sha256(url.encode('utf-8')).hexdigest()}"
        cached = self.cache.get_json(cache_key)
        if cached is not None:
            return cached
        headers = {"User-Agent": self.user_agent, "Accept-Encoding": "gzip, deflate"}
        async with httpx.AsyncClient(timeout=30.0, headers=headers) as client:
            response = await client.get(url)
            response.raise_for_status()
            payload = response.json()
        self.cache.set_json(cache_key, payload, source="sec_edgar", ttl_seconds=ttl_seconds)
        return payload

    async def _request_text(self, url: str, ttl_seconds: int = 60 * 60 * 24) -> str:
        cache_key = f"sec-text:{hashlib.sha256(url.encode('utf-8')).hexdigest()}"
        cached = self.cache.get_json(cache_key)
        if cached is not None:
            return str(cached)
        headers = {"User-Agent": self.user_agent, "Accept-Encoding": "gzip, deflate"}
        async with httpx.AsyncClient(timeout=30.0, headers=headers) as client:
            response = await client.get(url)
            response.raise_for_status()
            payload = response.text
        self.cache.set_json(cache_key, payload, source="sec_edgar", ttl_seconds=ttl_seconds)
        return payload

    async def get_submissions(self, cik: str) -> dict[str, Any]:
        cik_padded = str(cik).zfill(10)
        url = f"{self.BASE_JSON_URL}/submissions/CIK{cik_padded}.json"
        return await self._request_json(url)

    async def get_recent_filing(
        self,
        cik: str,
        forms: tuple[str, ...] = ("10-K", "10-Q"),
    ) -> dict[str, Any] | None:
        payload = await self.get_submissions(cik)
        recent = payload.get("filings", {}).get("recent", {})
        forms_list = recent.get("form", [])
        accession_numbers = recent.get("accessionNumber", [])
        filing_dates = recent.get("filingDate", [])
        primary_docs = recent.get("primaryDocument", [])
        for index, form_type in enumerate(forms_list):
            if form_type in forms:
                return {
                    "form_type": form_type,
                    "accession_number": accession_numbers[index],
                    "filed_at": filing_dates[index],
                    "primary_document": primary_docs[index],
                }
        return None

    async def get_filing_text(
        self,
        *,
        cik: str,
        accession_number: str,
        primary_document: str,
    ) -> str:
        accession_compact = accession_number.replace("-", "")
        cik_compact = str(int(cik))
        url = f"{self.BASE_ARCHIVES_URL}/{cik_compact}/{accession_compact}/{primary_document}"
        return await self._request_text(url)

    def extract_filing_signals(self, filing_text: str) -> list[str]:
        normalized = re.sub(r"\s+", " ", filing_text)
        paragraphs = re.split(r"(?<=\.)\s+", normalized)
        patterns = {
            "liquidity": re.compile(r"\bliquidity\b|\bcash\b|\bcredit\b", re.IGNORECASE),
            "debt": re.compile(r"\bdebt\b|\bleverage\b|\binterest\b", re.IGNORECASE),
            "regulatory": re.compile(r"\bregulat|\bcompliance\b|\bgovernment\b", re.IGNORECASE),
            "operations": re.compile(r"\bsupply chain\b|\bcapacity\b|\bmanufactur", re.IGNORECASE),
            "margin": re.compile(r"\bmargin\b|\bprofitab", re.IGNORECASE),
            "risk": re.compile(r"\brisk\b|\buncertain\b|\bheadwind\b", re.IGNORECASE),
        }
        findings: list[str] = []
        for label, pattern in patterns.items():
            hits = [paragraph[:220] for paragraph in paragraphs if pattern.search(paragraph)]
            if hits:
                findings.append(f"{label.title()} theme surfaced in the latest filing narrative.")
                findings.append(f"Representative disclosure theme: {hits[0]}")
        return findings[:6]

