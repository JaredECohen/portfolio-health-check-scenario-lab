from __future__ import annotations

import pandas as pd
import pytest

from app.models.schemas import Holding
from app.services.analytics import AnalyticsService


def _price_frame(start: str, periods: int, base: float) -> pd.DataFrame:
    index = pd.date_range(start=start, periods=periods, freq="B")
    return pd.DataFrame({"adjusted_close": [base + value for value in range(periods)]}, index=index)


def test_compute_baseline_surfaces_effective_sample_window() -> None:
    service = AnalyticsService()
    holdings = [
        Holding(ticker="AAPL", shares=10, company_name="Apple Inc", sector="Technology"),
        Holding(ticker="MSFT", shares=5, company_name="Microsoft Corp", sector="Technology"),
    ]
    price_history = {
        "AAPL": _price_frame("2024-01-02", 55, 100),
        "MSFT": _price_frame("2024-01-02", 55, 200),
    }
    benchmark_history = _price_frame("2024-01-02", 55, 400)

    bundle = service.compute_baseline(
        holdings=holdings,
        benchmark_symbol="SPY",
        price_history=price_history,
        benchmark_history=benchmark_history,
        risk_free_rate=0.02,
    )

    assert bundle.baseline.effective_start_date == "2024-01-02"
    assert bundle.baseline.effective_end_date == "2024-03-18"
    assert bundle.baseline.effective_observations == 55


def test_compute_baseline_requires_minimum_observations() -> None:
    service = AnalyticsService()
    holdings = [
        Holding(ticker="AAPL", shares=10, company_name="Apple Inc", sector="Technology"),
        Holding(ticker="MSFT", shares=5, company_name="Microsoft Corp", sector="Technology"),
    ]
    price_history = {
        "AAPL": _price_frame("2024-01-02", 20, 100),
        "MSFT": _price_frame("2024-01-02", 20, 200),
    }
    benchmark_history = _price_frame("2024-01-02", 20, 400)

    with pytest.raises(ValueError, match="At least 40 aligned observations"):
        service.compute_baseline(
            holdings=holdings,
            benchmark_symbol="SPY",
            price_history=price_history,
            benchmark_history=benchmark_history,
            risk_free_rate=0.02,
        )
