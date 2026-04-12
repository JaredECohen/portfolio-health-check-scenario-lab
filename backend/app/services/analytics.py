from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from typing import Any

import numpy as np
import pandas as pd

from app.models.schemas import (
    BaselineAnalytics,
    Contributor,
    Holding,
    PerformancePoint,
    PortfolioMetric,
    PositionSnapshot,
    SectorExposure,
)


DEFAULT_METRIC_ORDER = [
    ("trailing_return", "Trailing Return"),
    ("return_vs_benchmark", "Return vs SPY"),
    ("annualized_volatility", "Annualized Volatility"),
    ("beta_vs_benchmark", "Beta vs SPY"),
    ("max_drawdown", "Max Drawdown"),
    ("average_pairwise_correlation", "Avg Pairwise Corr"),
    ("herfindahl_index", "Herfindahl Index"),
    ("top3_share", "Top 3 Weight"),
    ("sharpe_ratio", "Sharpe Ratio"),
]


@dataclass
class AnalyticsBundle:
    baseline: BaselineAnalytics
    price_frame: pd.DataFrame
    holding_returns: pd.DataFrame
    benchmark_prices: pd.Series
    benchmark_returns: pd.Series
    portfolio_value_series: pd.Series
    portfolio_returns: pd.Series
    current_prices: dict[str, float]
    risk_free_rate: float
    metrics_map: dict[str, float]
    holdings: list[Holding]


class AnalyticsService:
    def compute_baseline(
        self,
        *,
        holdings: list[Holding],
        benchmark_symbol: str,
        price_history: dict[str, pd.DataFrame],
        benchmark_history: pd.DataFrame,
        risk_free_rate: float,
    ) -> AnalyticsBundle:
        adjusted_close = pd.concat(
            {
                holding.ticker: price_history[holding.ticker]["adjusted_close"]
                for holding in holdings
            },
            axis=1,
        ).dropna(how="any")
        benchmark_prices = benchmark_history["adjusted_close"].reindex(adjusted_close.index).dropna()
        adjusted_close = adjusted_close.loc[benchmark_prices.index]

        current_prices = adjusted_close.iloc[-1].to_dict()
        market_values = {
            holding.ticker: current_prices[holding.ticker] * holding.shares for holding in holdings
        }
        total_value = float(sum(market_values.values()))
        weights = {ticker: value / total_value for ticker, value in market_values.items()}
        value_frame = adjusted_close.copy()
        for holding in holdings:
            value_frame[holding.ticker] = value_frame[holding.ticker] * holding.shares
        portfolio_value = value_frame.sum(axis=1)
        portfolio_returns = portfolio_value.pct_change().dropna()
        benchmark_returns = benchmark_prices.pct_change().dropna()
        aligned_returns = pd.concat([portfolio_returns, benchmark_returns], axis=1).dropna()
        aligned_returns.columns = ["portfolio", "benchmark"]

        holding_returns = adjusted_close.pct_change().dropna()
        holding_correlation = holding_returns.corr().fillna(0.0)
        upper_triangle = holding_correlation.where(
            np.triu(np.ones(holding_correlation.shape), k=1).astype(bool)
        )
        pairwise_values = [
            float(value)
            for value in upper_triangle.stack().tolist()
            if value is not None and np.isfinite(value)
        ]
        average_pairwise_corr = float(np.mean(pairwise_values)) if pairwise_values else 0.0

        trailing_return = float((portfolio_value.iloc[-1] / portfolio_value.iloc[0]) - 1)
        benchmark_return = float((benchmark_prices.iloc[-1] / benchmark_prices.iloc[0]) - 1)
        annualized_volatility = float(portfolio_returns.std() * sqrt(252))
        benchmark_variance = float(aligned_returns["benchmark"].var())
        beta = (
            float(aligned_returns.cov().iloc[0, 1] / benchmark_variance)
            if benchmark_variance
            else 0.0
        )
        cumulative = (1 + portfolio_returns).cumprod()
        running_peak = cumulative.cummax()
        max_drawdown = float((cumulative / running_peak - 1).min())
        annualized_return = float(portfolio_returns.mean() * 252)
        sharpe = (
            float((annualized_return - risk_free_rate) / annualized_volatility)
            if annualized_volatility
            else 0.0
        )
        herfindahl = float(sum(weight ** 2 for weight in weights.values()))
        top3_share = float(sum(sorted(weights.values(), reverse=True)[:3]))

        metrics_map = {
            "trailing_return": trailing_return,
            "return_vs_benchmark": trailing_return - benchmark_return,
            "annualized_volatility": annualized_volatility,
            "beta_vs_benchmark": beta,
            "max_drawdown": max_drawdown,
            "average_pairwise_correlation": average_pairwise_corr,
            "herfindahl_index": herfindahl,
            "top3_share": top3_share,
            "sharpe_ratio": sharpe,
        }

        positions: list[PositionSnapshot] = []
        contributors: list[Contributor] = []
        sector_totals: dict[str, float] = {}
        for holding in holdings:
            ticker = holding.ticker
            series = adjusted_close[ticker]
            trailing_position_return = float((series.iloc[-1] / series.iloc[0]) - 1)
            market_value = market_values[ticker]
            pnl_dollar = None
            pnl_pct = None
            if holding.cost_basis is not None:
                total_cost = holding.cost_basis * holding.shares
                pnl_dollar = market_value - total_cost
                pnl_pct = (market_value / total_cost - 1) if total_cost else None
            positions.append(
                PositionSnapshot(
                    ticker=ticker,
                    company_name=holding.company_name or ticker,
                    sector=holding.sector,
                    shares=holding.shares,
                    current_price=float(series.iloc[-1]),
                    market_value=float(market_value),
                    weight=float(weights[ticker]),
                    trailing_return=trailing_position_return,
                    cost_basis=holding.cost_basis,
                    pnl_dollar=pnl_dollar,
                    pnl_pct=pnl_pct,
                )
            )
            contributors.append(
                Contributor(
                    ticker=ticker,
                    company_name=holding.company_name or ticker,
                    return_pct=trailing_position_return,
                    contribution_pct=float(weights[ticker] * trailing_position_return),
                    weight=float(weights[ticker]),
                )
            )
            sector_key = holding.sector or "Unknown"
            sector_totals[sector_key] = sector_totals.get(sector_key, 0.0) + market_value

        sector_exposures = [
            SectorExposure(
                sector=sector,
                market_value=float(value),
                weight=float(value / total_value),
            )
            for sector, value in sorted(sector_totals.items(), key=lambda item: item[1], reverse=True)
        ]
        contributors.sort(key=lambda item: item.contribution_pct, reverse=True)
        best_performers = sorted(contributors, key=lambda item: item.return_pct, reverse=True)[:3]
        worst_performers = sorted(contributors, key=lambda item: item.return_pct)[:3]

        performance_series = [
            PerformancePoint(
                date=index.strftime("%Y-%m-%d"),
                portfolio_index=float(value),
                benchmark_index=float(benchmark_value),
            )
            for index, value, benchmark_value in zip(
                portfolio_value.index,
                portfolio_value / portfolio_value.iloc[0],
                benchmark_prices / benchmark_prices.iloc[0],
            )
        ]
        metrics = [
            PortfolioMetric(
                key=key,
                label=label,
                value=metrics_map[key],
                formatted=self.format_metric(key, metrics_map[key]),
            )
            for key, label in DEFAULT_METRIC_ORDER
        ]
        total_cost_basis = (
            float(
                sum(
                    holding.cost_basis * holding.shares
                    for holding in holdings
                    if holding.cost_basis is not None
                )
            )
            if any(holding.cost_basis is not None for holding in holdings)
            else None
        )
        baseline = BaselineAnalytics(
            total_portfolio_value=total_value,
            total_cost_basis=total_cost_basis,
            benchmark_symbol=benchmark_symbol,
            risk_free_rate_used=risk_free_rate,
            metrics=metrics,
            positions=positions,
            sector_exposures=sector_exposures,
            contributors=contributors,
            best_performers=best_performers,
            worst_performers=worst_performers,
            correlation_matrix=holding_correlation.round(4).to_dict(),
            performance_series=performance_series,
        )
        return AnalyticsBundle(
            baseline=baseline,
            price_frame=adjusted_close,
            holding_returns=holding_returns,
            benchmark_prices=benchmark_prices,
            benchmark_returns=benchmark_returns,
            portfolio_value_series=portfolio_value,
            portfolio_returns=portfolio_returns,
            current_prices=current_prices,
            risk_free_rate=risk_free_rate,
            metrics_map=metrics_map,
            holdings=holdings,
        )

    @staticmethod
    def format_metric(key: str, value: float | None) -> str:
        if value is None:
            return "n/a"
        if "return" in key or "volatility" in key or "drawdown" in key or "share" in key:
            return f"{value * 100:.2f}%"
        if "correlation" in key or "beta" in key or "sharpe" in key or "herfindahl" in key:
            return f"{value:.2f}"
        return f"{value:.2f}"

    def metric_list_from_map(self, metrics_map: dict[str, Any]) -> list[PortfolioMetric]:
        metrics: list[PortfolioMetric] = []
        for key, label in DEFAULT_METRIC_ORDER:
            value = metrics_map.get(key)
            metrics.append(
                PortfolioMetric(
                    key=key,
                    label=label,
                    value=value,
                    formatted=self.format_metric(key, value),
                )
            )
        return metrics
