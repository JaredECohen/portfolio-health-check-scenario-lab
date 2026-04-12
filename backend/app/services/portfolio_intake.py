from __future__ import annotations

from app.models.schemas import AssetType, Holding, HypotheticalPosition, PortfolioInput
from app.services.alpha_vantage import AlphaVantageService, AlphaVantageError
from app.services.ticker_metadata import TickerMetadataService


class PortfolioIntakeError(ValueError):
    pass


class PortfolioIntakeService:
    def __init__(
        self,
        ticker_metadata: TickerMetadataService,
        alpha_vantage: AlphaVantageService,
    ) -> None:
        self.ticker_metadata = ticker_metadata
        self.alpha_vantage = alpha_vantage

    async def normalize(self, payload: PortfolioInput) -> PortfolioInput:
        normalized_holdings = [
            await self._normalize_holding(holding)
            for holding in payload.holdings
        ]
        hypothetical = None
        if payload.hypothetical_position:
            hypothetical = await self._normalize_hypothetical(payload.hypothetical_position)
        return payload.model_copy(
            update={"holdings": normalized_holdings, "hypothetical_position": hypothetical}
        )

    async def _normalize_holding(self, holding: Holding) -> Holding:
        metadata = self.ticker_metadata.get(holding.ticker)
        if metadata is None:
            raise PortfolioIntakeError(
                f"{holding.ticker} is not in the supported U.S. public equity universe."
            )
        sector = metadata.sector
        if not sector:
            try:
                overview = await self.alpha_vantage.get_company_overview(holding.ticker)
                sector = overview.get("Sector") or metadata.sector
            except AlphaVantageError:
                sector = metadata.sector
        return holding.model_copy(
            update={
                "company_name": metadata.company_name,
                "sector": sector or "Unknown",
                "cik": metadata.cik,
                "exchange": metadata.exchange,
                "asset_type": AssetType.equity,
            }
        )

    async def _normalize_hypothetical(self, position: HypotheticalPosition) -> HypotheticalPosition:
        metadata = self.ticker_metadata.get(position.ticker)
        if metadata is None:
            raise PortfolioIntakeError(
                f"{position.ticker} is not in the supported U.S. public equity universe."
            )
        sector = metadata.sector
        if not sector:
            try:
                overview = await self.alpha_vantage.get_company_overview(position.ticker)
                sector = overview.get("Sector") or metadata.sector
            except AlphaVantageError:
                sector = metadata.sector
        return position.model_copy(
            update={
                "company_name": metadata.company_name,
                "sector": sector or "Unknown",
                "cik": metadata.cik,
                "exchange": metadata.exchange,
            }
        )

