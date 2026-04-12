from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from app.models.schemas import (
    CandidateRank,
    CandidateSearchResult,
    Holding,
    HypotheticalPosition,
    ScenarioAnalytics,
    ScenarioDelta,
    TickerMetadata,
)
from app.services.analytics import AnalyticsBundle, AnalyticsService
from app.services.alpha_vantage import AlphaVantageService
from app.services.ticker_metadata import TickerMetadataService


class ScenarioService:
    def __init__(
        self,
        analytics_service: AnalyticsService,
        alpha_vantage: AlphaVantageService,
        ticker_metadata: TickerMetadataService,
        candidate_universe_path: Path,
    ) -> None:
        self.analytics_service = analytics_service
        self.alpha_vantage = alpha_vantage
        self.ticker_metadata = ticker_metadata
        self.candidate_universe_path = candidate_universe_path

    async def simulate_addition(
        self,
        *,
        baseline_bundle: AnalyticsBundle,
        hypothetical_position: HypotheticalPosition,
        benchmark_symbol: str,
        lookback_days: int,
    ) -> tuple[ScenarioAnalytics, AnalyticsBundle]:
        price_history = {
            holding.ticker: pd.DataFrame({"adjusted_close": baseline_bundle.price_frame[holding.ticker]})
            for holding in baseline_bundle.holdings
        }
        benchmark_history = pd.DataFrame({"adjusted_close": baseline_bundle.benchmark_prices})
        new_holding = Holding(
            ticker=hypothetical_position.ticker,
            shares=await self._resolve_shares(
                hypothetical_position,
                baseline_bundle.baseline.total_portfolio_value,
            ),
            cost_basis=None,
            company_name=hypothetical_position.company_name,
            sector=hypothetical_position.sector,
            cik=hypothetical_position.cik,
            exchange=hypothetical_position.exchange,
        )
        if new_holding.ticker not in price_history:
            scenario_history = await self.alpha_vantage.get_daily_adjusted(new_holding.ticker)
            price_history[new_holding.ticker] = scenario_history.tail(lookback_days)
        holdings_after = [*baseline_bundle.holdings, new_holding]
        after_bundle = self.analytics_service.compute_baseline(
            holdings=holdings_after,
            benchmark_symbol=benchmark_symbol,
            price_history=price_history,
            benchmark_history=benchmark_history,
            risk_free_rate=baseline_bundle.risk_free_rate,
        )
        deltas = []
        for key, before in baseline_bundle.metrics_map.items():
            after = after_bundle.metrics_map.get(key)
            delta = None if before is None or after is None else after - before
            deltas.append(ScenarioDelta(metric=key, before=before, after=after, delta=delta))
        result = ScenarioAnalytics(
            label=f"Add {new_holding.ticker}",
            hypothetical_position=hypothetical_position,
            before_metrics=baseline_bundle.baseline.metrics,
            after_metrics=after_bundle.baseline.metrics,
            deltas=deltas,
            before_sector_exposures=baseline_bundle.baseline.sector_exposures,
            after_sector_exposures=after_bundle.baseline.sector_exposures,
            before_positions=baseline_bundle.baseline.positions,
            after_positions=after_bundle.baseline.positions,
        )
        return result, after_bundle

    async def rank_candidates(
        self,
        *,
        baseline_bundle: AnalyticsBundle,
        benchmark_symbol: str,
        objective: str,
        lookback_days: int,
        max_candidates: int = 5,
    ) -> CandidateSearchResult:
        candidate_rows = json.loads(self.candidate_universe_path.read_text(encoding="utf-8"))
        benchmark_history = pd.DataFrame({"adjusted_close": baseline_bundle.benchmark_prices})
        current_history = {
            holding.ticker: pd.DataFrame({"adjusted_close": baseline_bundle.price_frame[holding.ticker]})
            for holding in baseline_bundle.holdings
        }
        ranks: list[CandidateRank] = []
        for item in candidate_rows:
            metadata = TickerMetadata.model_validate(item)
            if metadata.ticker in current_history:
                continue
            hypothetical = HypotheticalPosition(
                ticker=metadata.ticker,
                target_weight=0.05,
                company_name=metadata.company_name,
                sector=metadata.sector,
                cik=metadata.cik,
                exchange=metadata.exchange,
            )
            candidate_history = await self.alpha_vantage.get_daily_adjusted(metadata.ticker)
            price_history = dict(current_history)
            price_history[metadata.ticker] = candidate_history.tail(lookback_days)
            shares = await self._resolve_shares(
                hypothetical,
                baseline_bundle.baseline.total_portfolio_value,
            )
            after_holdings = [
                *baseline_bundle.holdings,
                Holding(
                    ticker=metadata.ticker,
                    shares=shares,
                    company_name=metadata.company_name,
                    sector=metadata.sector,
                    cik=metadata.cik,
                    exchange=metadata.exchange,
                ),
            ]
            after_bundle = self.analytics_service.compute_baseline(
                holdings=after_holdings,
                benchmark_symbol=benchmark_symbol,
                price_history=price_history,
                benchmark_history=benchmark_history,
                risk_free_rate=baseline_bundle.risk_free_rate,
            )
            deltas = [
                ScenarioDelta(
                    metric=key,
                    before=baseline_bundle.metrics_map.get(key),
                    after=after_bundle.metrics_map.get(key),
                    delta=(
                        None
                        if baseline_bundle.metrics_map.get(key) is None
                        or after_bundle.metrics_map.get(key) is None
                        else after_bundle.metrics_map[key] - baseline_bundle.metrics_map[key]
                    ),
                )
                for key in (
                    "annualized_volatility",
                    "average_pairwise_correlation",
                    "herfindahl_index",
                    "top3_share",
                    "sharpe_ratio",
                    "beta_vs_benchmark",
                )
            ]
            score = self._score_candidate(deltas, objective)
            rationale = self._candidate_rationale(deltas, objective)
            ranks.append(
                CandidateRank(
                    ticker=metadata.ticker,
                    company_name=metadata.company_name,
                    sector=metadata.sector,
                    score=score,
                    rationale=rationale,
                    deltas=deltas,
                )
            )
        ranks.sort(key=lambda rank: rank.score, reverse=True)
        return CandidateSearchResult(
            objective=objective,
            method="5% target-weight addition scenario over a curated liquid U.S. equity universe",
            candidates=ranks[:max_candidates],
        )

    async def _resolve_shares(
        self,
        position: HypotheticalPosition,
        total_portfolio_value: float,
    ) -> float:
        if position.shares is not None:
            return position.shares
        price_history = await self.alpha_vantage.get_daily_adjusted(position.ticker)
        latest_price = float(price_history["adjusted_close"].iloc[-1])
        return (total_portfolio_value * float(position.target_weight)) / latest_price

    @staticmethod
    def _score_candidate(deltas: list[ScenarioDelta], objective: str) -> float:
        metrics = {delta.metric: delta.delta or 0.0 for delta in deltas}
        if objective == "diversify":
            return (
                (-metrics["herfindahl_index"] * 3.0)
                + (-metrics["average_pairwise_correlation"] * 2.0)
                + (-metrics["top3_share"] * 2.0)
                + (metrics["sharpe_ratio"] * 1.5)
            )
        if objective == "reduce_macro_sensitivity":
            return (-metrics["beta_vs_benchmark"] * 3.0) + (-metrics["annualized_volatility"] * 2.0)
        return (metrics["sharpe_ratio"] * 2.0) + (-metrics["annualized_volatility"] * 1.5)

    @staticmethod
    def _candidate_rationale(deltas: list[ScenarioDelta], objective: str) -> list[str]:
        metrics = {delta.metric: delta.delta or 0.0 for delta in deltas}
        if objective == "diversify":
            return [
                f"Herfindahl changes by {metrics['herfindahl_index']:.4f}.",
                f"Average pairwise correlation changes by {metrics['average_pairwise_correlation']:.4f}.",
                f"Top 3 weight changes by {metrics['top3_share'] * 100:.2f} percentage points.",
            ]
        if objective == "reduce_macro_sensitivity":
            return [
                f"Portfolio beta changes by {metrics['beta_vs_benchmark']:.4f}.",
                f"Annualized volatility changes by {metrics['annualized_volatility'] * 100:.2f} percentage points.",
            ]
        return [
            f"Sharpe changes by {metrics['sharpe_ratio']:.4f}.",
            f"Annualized volatility changes by {metrics['annualized_volatility'] * 100:.2f} percentage points.",
        ]
