from __future__ import annotations

from app.models.schemas import TickerMetadata
from app.services.stock_dataset_builder import StockDatasetBuilder


class DummyAlphaVantage:
    pass


class StubTickerMetadataService:
    def __init__(self, rows: list[TickerMetadata]) -> None:
        self.rows = rows

    def all(self) -> list[TickerMetadata]:
        return self.rows

    def get(self, ticker: str) -> TickerMetadata | None:
        ticker = ticker.upper().strip()
        for row in self.rows:
            if row.ticker == ticker:
                return row
        return None


def test_sector_peers_stay_centered_on_anchor_sectors() -> None:
    metadata = StubTickerMetadataService(
        [
            TickerMetadata(ticker="AAPL", company_name="Apple", cik="1", sector="Technology", exchange="NASDAQ"),
            TickerMetadata(ticker="JPM", company_name="JPMorgan", cik="2", sector="Financials", exchange="NYSE"),
            TickerMetadata(ticker="MSFT", company_name="Microsoft", cik="3", sector="Technology", exchange="NASDAQ"),
            TickerMetadata(ticker="NVDA", company_name="NVIDIA", cik="4", sector="Technology", exchange="NASDAQ"),
            TickerMetadata(ticker="GS", company_name="Goldman Sachs", cik="5", sector="Financials", exchange="NYSE"),
            TickerMetadata(ticker="BAC", company_name="Bank of America", cik="6", sector="Financials", exchange="NYSE"),
            TickerMetadata(ticker="XOM", company_name="Exxon", cik="7", sector="Energy", exchange="NYSE"),
        ]
    )
    builder = StockDatasetBuilder(
        alpha_vantage=DummyAlphaVantage(),  # type: ignore[arg-type]
        ticker_metadata_service=metadata,
    )

    universe = builder._resolve_universe(
        tickers=["AAPL"],
        comparison_universe="sector_peers",
        comparison_sector_filters=[],
        comparison_ticker_limit=6,
        portfolio_tickers=["JPM"],
        comparison_objective="performance",
        portfolio_sector_weights={"Technology": 0.6, "Financials": 0.4},
    )

    assert universe[:2] == ["AAPL", "JPM"]
    assert "MSFT" in universe
    assert "GS" in universe
    assert "XOM" not in universe


def test_candidate_universe_subset_reuses_shortlist_priorities() -> None:
    metadata = StubTickerMetadataService(
        [
            TickerMetadata(ticker="AAPL", company_name="Apple", cik="1", sector="Technology", exchange="NASDAQ"),
            TickerMetadata(ticker="MSFT", company_name="Microsoft", cik="2", sector="Technology", exchange="NASDAQ"),
            TickerMetadata(ticker="NVDA", company_name="NVIDIA", cik="3", sector="Technology", exchange="NASDAQ"),
            TickerMetadata(ticker="ORCL", company_name="Oracle", cik="4", sector="Technology", exchange="NYSE"),
            TickerMetadata(ticker="JNJ", company_name="Johnson & Johnson", cik="5", sector="Healthcare", exchange="NYSE"),
            TickerMetadata(ticker="DUK", company_name="Duke Energy", cik="6", sector="Utilities", exchange="NYSE"),
            TickerMetadata(ticker="PEP", company_name="PepsiCo", cik="7", sector="Consumer Staples", exchange="NASDAQ"),
        ]
    )
    builder = StockDatasetBuilder(
        alpha_vantage=DummyAlphaVantage(),  # type: ignore[arg-type]
        ticker_metadata_service=metadata,
    )

    universe = builder._resolve_universe(
        tickers=["AAPL"],
        comparison_universe="candidate_universe_subset",
        comparison_sector_filters=[],
        comparison_ticker_limit=5,
        portfolio_tickers=["MSFT"],
        comparison_objective="diversify",
        portfolio_sector_weights={"Technology": 0.8},
    )

    assert universe[:2] == ["AAPL", "MSFT"]
    assert universe[2:] == ["DUK", "JNJ", "PEP"]
