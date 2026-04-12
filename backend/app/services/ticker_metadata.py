from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

from app.models.schemas import TickerMetadata


class TickerMetadataService:
    def __init__(self, metadata_path: Path) -> None:
        self.metadata_path = metadata_path

    @lru_cache(maxsize=1)
    def _load(self) -> list[TickerMetadata]:
        if not self.metadata_path.exists():
            return []
        payload = json.loads(self.metadata_path.read_text(encoding="utf-8"))
        return [TickerMetadata.model_validate(item) for item in payload]

    def all(self) -> list[TickerMetadata]:
        return self._load()

    def get(self, ticker: str) -> TickerMetadata | None:
        ticker = ticker.upper().strip()
        for item in self._load():
            if item.ticker == ticker:
                return item
        return None

    def search(self, query: str | None = None, limit: int = 25) -> list[TickerMetadata]:
        records = self._load()
        if not query:
            return records[:limit]
        needle = query.upper().strip()
        filtered = [
            item
            for item in records
            if needle in item.ticker or needle in item.company_name.upper()
        ]
        filtered.sort(key=lambda item: (not item.ticker.startswith(needle), item.ticker))
        return filtered[:limit]

