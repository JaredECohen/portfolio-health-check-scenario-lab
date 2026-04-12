from __future__ import annotations

import asyncio

import pandas as pd

from app.services.alpha_vantage import AlphaVantageService, AlphaVantageError


class MarketDataService:
    def __init__(self, alpha_vantage: AlphaVantageService) -> None:
        self.alpha_vantage = alpha_vantage

    async def fetch_price_history(
        self,
        *,
        tickers: list[str],
        benchmark_symbol: str,
        lookback_days: int,
    ) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
        symbols = list(dict.fromkeys([*tickers, benchmark_symbol]))
        tasks = [self.alpha_vantage.get_daily_adjusted(symbol, outputsize="full") for symbol in symbols]
        responses = await asyncio.gather(*tasks)
        history = {
            symbol: frame.tail(lookback_days).copy()
            for symbol, frame in zip(symbols, responses, strict=True)
        }
        benchmark_history = history.pop(benchmark_symbol)
        return history, benchmark_history

    async def get_risk_free_rate(self, fallback_rate: float) -> float:
        try:
            treasury = await self.alpha_vantage.get_treasury_yield()
            if treasury.empty:
                return fallback_rate
            return float(treasury["value"].iloc[0]) / 100.0
        except AlphaVantageError:
            return fallback_rate

