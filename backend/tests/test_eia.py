from __future__ import annotations

import asyncio
from typing import Any

from app.services.eia import EIAService


class DummyCache:
    def get_json(self, cache_key: str) -> Any | None:  # noqa: ARG002
        return None

    def set_json(self, cache_key: str, payload: Any, *, source: str, ttl_seconds: int | None = None) -> None:  # noqa: ARG002
        return None


class StubEIAService(EIAService):
    def __init__(self) -> None:
        super().__init__(api_key="test", cache=DummyCache())

    async def _get_text(self, *, url: str, ttl_seconds: int = 60 * 60 * 12) -> str:  # noqa: ARG002
        if "table4.csv" in url:
            return (
                '"Sourcekey","PET.WCESTUS1.W","PET.WGTSTUS1.W","Date","2025-08-22","Week ending August 22, 2025",'
                '"Crude oil and petroleum products","Week ending August 22, 2025",'
                '"Commercial (Excluding SPR)","426.708","422.458","4.250",'
                '"Total Stocks Excluding SPR","1608.159","1602.587","5.572"'
            )
        if "wngsr.json" in url:
            return (
                '[{"report_date":"2025-08-22","series":['
                '{"name":"Total","value":"3555","net_change":"-43","year_ago":"3342","five_year_avg":"3443","pct_chg_five_year_avg":"3.3"}'
                ']}]'
            )
        raise AssertionError(f"Unexpected URL: {url}")


def test_eia_service_parses_petroleum_and_natgas_snapshots() -> None:
    service = StubEIAService()

    petroleum = asyncio.run(service.get_petroleum_storage_snapshot())
    natgas = asyncio.run(service.get_natgas_storage_snapshot())

    assert petroleum["commercial_crude"]["level_million_bbl"] == 426.708
    assert petroleum["commercial_crude"]["weekly_change_million_bbl"] == 4.25
    assert natgas["total_lower_48"]["working_gas_bcf"] == 3555.0
    assert natgas["total_lower_48"]["net_change_bcf"] == -43.0
