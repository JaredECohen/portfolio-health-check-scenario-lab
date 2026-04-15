from __future__ import annotations

import asyncio
from datetime import date
import hashlib
import html
import random
import re
from typing import Any

import httpx

from app.services.cache import CacheService


class SecEdgarService:
    BASE_JSON_URL = "https://data.sec.gov"
    BASE_ARCHIVES_URL = "https://www.sec.gov/Archives/edgar/data"
    MAX_RETRIES = 3

    def __init__(self, user_agent: str, cache: CacheService) -> None:
        self.user_agent = user_agent
        self.cache = cache

    async def _request_json(self, url: str, ttl_seconds: int = 60 * 60 * 24) -> Any:
        cache_key = f"sec-json:{hashlib.sha256(url.encode('utf-8')).hexdigest()}"
        cached = self.cache.get_json(cache_key)
        if cached is not None:
            return cached
        headers = {"User-Agent": self.user_agent, "Accept-Encoding": "gzip, deflate"}
        payload = await self._request_with_retries(url=url, headers=headers, parse="json")
        self.cache.set_json(cache_key, payload, source="sec_edgar", ttl_seconds=ttl_seconds)
        return payload

    async def _request_text(self, url: str, ttl_seconds: int = 60 * 60 * 24) -> str:
        cache_key = f"sec-text:{hashlib.sha256(url.encode('utf-8')).hexdigest()}"
        cached = self.cache.get_json(cache_key)
        if cached is not None:
            return str(cached)
        headers = {"User-Agent": self.user_agent, "Accept-Encoding": "gzip, deflate"}
        payload = await self._request_with_retries(url=url, headers=headers, parse="text")
        self.cache.set_json(cache_key, payload, source="sec_edgar", ttl_seconds=ttl_seconds)
        return payload

    async def _request_with_retries(
        self,
        *,
        url: str,
        headers: dict[str, str],
        parse: str,
    ) -> Any:
        last_error: Exception | None = None
        async with httpx.AsyncClient(timeout=30.0, headers=headers) as client:
            for attempt in range(1, self.MAX_RETRIES + 1):
                try:
                    response = await client.get(url)
                    response.raise_for_status()
                    return response.json() if parse == "json" else response.text
                except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                    last_error = exc
                    status_code = getattr(getattr(exc, "response", None), "status_code", None)
                    if status_code not in {None, 429, 500, 502, 503, 504}:
                        break
                    if attempt == self.MAX_RETRIES:
                        break
                    await self._backoff(attempt)
        raise RuntimeError(f"SEC request failed after retries: {last_error}")

    async def get_submissions(self, cik: str) -> dict[str, Any]:
        cik_padded = str(cik).zfill(10)
        url = f"{self.BASE_JSON_URL}/submissions/CIK{cik_padded}.json"
        return await self._request_json(url)

    async def get_company_facts(self, cik: str) -> dict[str, Any]:
        cik_padded = str(cik).zfill(10)
        url = f"{self.BASE_JSON_URL}/api/xbrl/companyfacts/CIK{cik_padded}.json"
        return await self._request_json(url)

    async def get_recent_filing(
        self,
        cik: str,
        forms: tuple[str, ...] = ("10-K", "10-Q"),
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, Any] | None:
        payload = await self.get_submissions(cik)
        recent = payload.get("filings", {}).get("recent", {})
        forms_list = recent.get("form", [])
        accession_numbers = recent.get("accessionNumber", [])
        filing_dates = recent.get("filingDate", [])
        primary_docs = recent.get("primaryDocument", [])
        for index, form_type in enumerate(forms_list):
            if form_type in forms:
                filing_date = date.fromisoformat(filing_dates[index])
                if start_date and filing_date < start_date:
                    continue
                if end_date and filing_date > end_date:
                    continue
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

    @staticmethod
    def _clean_filing_text(filing_text: str) -> str:
        cleaned = html.unescape(filing_text)
        cleaned = re.sub(r"(?is)<script.*?>.*?</script>", " ", cleaned)
        cleaned = re.sub(r"(?is)<style.*?>.*?</style>", " ", cleaned)
        cleaned = re.sub(r"(?is)<table.*?>.*?</table>", " ", cleaned)
        cleaned = re.sub(r"(?i)<br\s*/?>", ". ", cleaned)
        cleaned = re.sub(r"(?i)</p>|</div>|</li>|</tr>|</h\d>", ". ", cleaned)
        cleaned = re.sub(r"(?is)<[^>]+>", " ", cleaned)
        cleaned = re.sub(r"[\u00a0\t\r\n]+", " ", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned)
        return cleaned.strip()

    @staticmethod
    def _is_readable_sentence(sentence: str) -> bool:
        stripped = sentence.strip()
        if len(stripped) < 80:
            return False
        if sum(character.isalpha() for character in stripped) < 40:
            return False
        if stripped.count("|") > 1:
            return False
        return True

    def extract_filing_signals(self, filing_text: str) -> list[str]:
        normalized = self._clean_filing_text(filing_text)
        sentences = [
            sentence.strip()
            for sentence in re.split(r"(?<=[.!?])\s+", normalized)
            if self._is_readable_sentence(sentence)
        ]
        patterns = {
            "liquidity": re.compile(r"\bliquidity\b|\bcash\b|\bcredit\b", re.IGNORECASE),
            "debt": re.compile(r"\bdebt\b|\bleverage\b|\binterest\b", re.IGNORECASE),
            "regulatory": re.compile(
                r"\bregulator(?:y|s)?\b|\bcompliance\b|\bgovernment\b|\bprivacy\b|\btariff\b|\bantitrust\b",
                re.IGNORECASE,
            ),
            "operations": re.compile(r"\bsupply chain\b|\bcapacity\b|\bmanufactur", re.IGNORECASE),
            "margin": re.compile(r"\bmargin\b|\bprofitab", re.IGNORECASE),
            "risk": re.compile(r"\brisk\b|\buncertain\b|\bheadwind\b", re.IGNORECASE),
        }
        exclusions = {
            "regulatory": re.compile(
                r"interactive data file|rule 405|section 13|section 15\(d\)|nasdaq global select|commission file number|employer identification",
                re.IGNORECASE,
            )
        }
        findings: list[str] = []
        for label, pattern in patterns.items():
            exclusion = exclusions.get(label)
            hits = [
                sentence[:240]
                for sentence in sentences
                if pattern.search(sentence) and not (exclusion and exclusion.search(sentence))
            ]
            if hits:
                findings.append(f"{label.title()} theme surfaced in the latest filing narrative.")
                findings.append(f"Representative disclosure theme: {hits[0]}")
        return findings[:6]

    @staticmethod
    async def _backoff(attempt: int) -> None:
        delay_seconds = (0.3 * (2 ** (attempt - 1))) + random.uniform(0.0, 0.2)
        await asyncio.sleep(delay_seconds)
