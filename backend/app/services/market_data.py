from __future__ import annotations

import asyncio
from datetime import date

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
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
        symbols = list(dict.fromkeys([*tickers, benchmark_symbol]))
        tasks = [self.alpha_vantage.get_daily_adjusted(symbol, outputsize="full") for symbol in symbols]
        responses = await asyncio.gather(*tasks)
        history = {
            symbol: self._slice_history(
                frame=frame,
                lookback_days=lookback_days,
                start_date=start_date,
                end_date=end_date,
            )
            for symbol, frame in zip(symbols, responses, strict=True)
        }
        benchmark_history = history.pop(benchmark_symbol)
        return history, benchmark_history

    @staticmethod
    def _slice_history(
        *,
        frame: pd.DataFrame,
        lookback_days: int,
        start_date: date | None,
        end_date: date | None,
    ) -> pd.DataFrame:
        sliced = frame.copy()
        if start_date is not None:
            sliced = sliced[sliced.index >= pd.Timestamp(start_date)]
        if end_date is not None:
            sliced = sliced[sliced.index <= pd.Timestamp(end_date)]
        if start_date is None and end_date is None:
            sliced = sliced.tail(lookback_days)
        return sliced

    async def get_risk_free_rate(self, fallback_rate: float) -> float:
        try:
            treasury = await self.alpha_vantage.get_treasury_yield()
            if treasury.empty:
                return fallback_rate
            return float(treasury["value"].iloc[0]) / 100.0
        except AlphaVantageError:
            return fallback_rate
