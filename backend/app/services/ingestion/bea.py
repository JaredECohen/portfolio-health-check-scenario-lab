from __future__ import annotations


class BeaIngestionService:
    def __init__(self, api_key: str | None) -> None:
        self.api_key = api_key

    def sync(self) -> None:
        raise NotImplementedError("BEA ingestion is planned but not implemented in the SEC/FRED milestone.")
