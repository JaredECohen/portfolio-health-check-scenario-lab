from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from app.models.schemas import (
    CandidateRank,
    CandidateSearchResult,
    Holding,
    HypotheticalPosition,
    OptimizationPreference,
    ScenarioAnalytics,
    ScenarioDelta,
    TickerMetadata,
)
from app.services.analytics import AnalyticsBundle, AnalyticsService
from app.services.alpha_vantage import AlphaVantageError, AlphaVantageService
from app.services.candidate_universe import (
    focus_candidate_universe_sectors,
    prioritize_candidate_universe_rows,
)
from app.services.factor_analytics import (
    estimate_factor_profile,
    factor_similarity_to_profile,
    factor_support_score,
)
from app.services.feature_store import FeatureStore
from app.services.stock_dataset_builder import FUNDAMENTAL_METRICS, StockDatasetBuilder
from app.services.ticker_metadata import TickerMetadataService


FUNDAMENTAL_SHORTLIST_LIMIT = 60
PRICE_SCREEN_LIMIT = 24
SCENARIO_EVALUATION_LIMIT = 12
DEFENSIVE_SECTORS = {"HEALTHCARE", "CONSUMER STAPLES", "UTILITIES"}


class ScenarioService:
    def __init__(
        self,
        analytics_service: AnalyticsService,
        alpha_vantage: AlphaVantageService,
        ticker_metadata: TickerMetadataService,
        candidate_universe_path: Path,
        *,
        feature_store: FeatureStore | None = None,
        stock_dataset_builder: StockDatasetBuilder | None = None,
    ) -> None:
        self.analytics_service = analytics_service
        self.alpha_vantage = alpha_vantage
        self.ticker_metadata = ticker_metadata
        self.candidate_universe_path = candidate_universe_path
        self.feature_store = feature_store
        self.stock_dataset_builder = stock_dataset_builder or StockDatasetBuilder(
            alpha_vantage=alpha_vantage,
            ticker_metadata_service=ticker_metadata,
            feature_store=feature_store,
        )

    async def simulate_addition(
        self,
        *,
        baseline_bundle: AnalyticsBundle,
        hypothetical_position: HypotheticalPosition,
        benchmark_symbol: str,
        lookback_days: int,
        start_date: date | None = None,
        end_date: date | None = None,
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
                lookback_days=lookback_days,
                start_date=start_date,
                end_date=end_date,
            ),
            cost_basis=None,
            company_name=hypothetical_position.company_name,
            sector=hypothetical_position.sector,
            cik=hypothetical_position.cik,
            exchange=hypothetical_position.exchange,
        )
        if new_holding.ticker not in price_history:
            scenario_history = await self.alpha_vantage.get_daily_adjusted(new_holding.ticker)
            sliced_history = self._slice_history(
                frame=scenario_history,
                lookback_days=lookback_days,
                start_date=start_date,
                end_date=end_date,
            )
            if sliced_history.empty:
                raise ValueError(
                    f"No price history is available for {new_holding.ticker} in the requested analysis window."
                )
            price_history[new_holding.ticker] = sliced_history
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
        optimization_preferences: list[OptimizationPreference] | None = None,
        lookback_days: int,
        start_date: date | None = None,
        end_date: date | None = None,
        candidate_tickers: list[str] | None = None,
        max_candidates: int = 5,
    ) -> CandidateSearchResult:
        window_start, window_end = self._analysis_window(baseline_bundle)
        resolved_preferences = self._resolved_optimization_preferences(
            objective=objective,
            optimization_preferences=optimization_preferences or [],
        )
        screen = await self._screen_candidate_universe(
            baseline_bundle=baseline_bundle,
            benchmark_symbol=benchmark_symbol,
            objective=objective,
            optimization_preferences=resolved_preferences,
            lookback_days=lookback_days,
            start_date=window_start,
            end_date=window_end,
            candidate_tickers=candidate_tickers,
            max_candidates=max_candidates,
        )
        benchmark_history = pd.DataFrame({"adjusted_close": baseline_bundle.benchmark_prices})
        current_history = {
            holding.ticker: pd.DataFrame({"adjusted_close": baseline_bundle.price_frame[holding.ticker]})
            for holding in baseline_bundle.holdings
        }
        selected_rows = list(screen["candidates"])
        evaluation_limit = min(
            len(selected_rows),
            max(
                SCENARIO_EVALUATION_LIMIT,
                max_candidates * 3,
                len(candidate_tickers or []),
            ),
        )
        evaluated_ranks: list[CandidateRank] = []
        screened_ranks: list[CandidateRank] = []
        for item in selected_rows[:evaluation_limit]:
            metadata = TickerMetadata.model_validate(item)
            if metadata.ticker in current_history:
                continue
            try:
                candidate_history = await self.alpha_vantage.get_daily_adjusted(metadata.ticker, outputsize="full")
            except AlphaVantageError:
                continue
            sliced_history = self._slice_history(
                frame=candidate_history,
                lookback_days=lookback_days,
                start_date=window_start,
                end_date=window_end,
            )
            if sliced_history.empty:
                continue
            latest_price = float(sliced_history["adjusted_close"].iloc[-1])
            shares = self._shares_for_target_weight(
                total_portfolio_value=baseline_bundle.baseline.total_portfolio_value,
                target_weight=0.05,
                latest_price=latest_price,
            )
            price_history = dict(current_history)
            price_history[metadata.ticker] = sliced_history
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
            try:
                after_bundle = self.analytics_service.compute_baseline(
                    holdings=after_holdings,
                    benchmark_symbol=benchmark_symbol,
                    price_history=price_history,
                    benchmark_history=benchmark_history,
                    risk_free_rate=baseline_bundle.risk_free_rate,
                )
            except Exception:  # noqa: BLE001
                continue
            candidate_signals = self._candidate_signals(
                baseline_bundle=baseline_bundle,
                after_bundle=after_bundle,
                candidate_history=sliced_history["adjusted_close"],
                candidate_sector=metadata.sector,
                screen_row=item,
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
                    "trailing_return",
                    "return_vs_benchmark",
                    "annualized_volatility",
                    "average_pairwise_correlation",
                    "herfindahl_index",
                    "top3_share",
                    "sharpe_ratio",
                    "beta_vs_benchmark",
                )
            ]
            score = self._score_candidate(
                deltas,
                candidate_signals,
                objective,
                optimization_preferences=resolved_preferences,
            )
            rationale = self._candidate_rationale(
                deltas,
                candidate_signals,
                objective,
                optimization_preferences=resolved_preferences,
            )
            rank = CandidateRank(
                ticker=metadata.ticker,
                company_name=metadata.company_name,
                sector=metadata.sector,
                score=score,
                rationale=rationale,
                deltas=deltas,
            )
            evaluated_ranks.append(rank)
            if self._meets_candidate_constraints(
                deltas,
                objective=objective,
                optimization_preferences=resolved_preferences,
            ):
                screened_ranks.append(rank)
        ranks = screened_ranks or evaluated_ranks
        ranks.sort(key=lambda rank: rank.score, reverse=True)
        screening_summary = [
            *screen["screening_summary"],
            (
                "Ran full 5% addition simulations on "
                f"{len(evaluated_ranks)} finalists and kept {len(screened_ranks) or len(evaluated_ranks)} "
                "after the marginal risk/return screen."
            ),
        ]
        return CandidateSearchResult(
            objective=objective,
            method=self._compose_candidate_search_method(
                objective=objective,
                optimization_preferences=resolved_preferences,
                universe_size=int(screen["universe_size"]),
                fundamental_pool_size=int(screen["fundamental_pool_size"]),
                price_screen_size=int(screen["price_screen_size"]),
                evaluation_count=len(evaluated_ranks),
            ),
            candidates=ranks[:max_candidates],
            screening_summary=screening_summary,
        )

    async def shortlist_universe(
        self,
        *,
        baseline_bundle: AnalyticsBundle,
        objective: str,
        optimization_preferences: list[OptimizationPreference] | None = None,
        lookback_days: int,
        start_date: date | None = None,
        end_date: date | None = None,
        preferred_sectors: list[str] | None = None,
        excluded_sectors: list[str] | None = None,
        max_candidates: int = 20,
    ) -> dict[str, Any]:
        window_start, window_end = self._analysis_window(baseline_bundle)
        resolved_preferences = self._resolved_optimization_preferences(
            objective=objective,
            optimization_preferences=optimization_preferences or [],
        )
        screen = await self._screen_candidate_universe(
            baseline_bundle=baseline_bundle,
            benchmark_symbol=baseline_bundle.baseline.benchmark_symbol,
            objective=objective,
            optimization_preferences=resolved_preferences,
            lookback_days=lookback_days,
            start_date=window_start,
            end_date=window_end,
            max_candidates=max_candidates,
            preferred_sectors=preferred_sectors,
            excluded_sectors=excluded_sectors,
        )
        sector_exposures = baseline_bundle.baseline.sector_exposures
        return {
            "universe_size": int(screen["universe_size"]),
            "objective": objective,
            "focus_sectors": list(screen["focus_sectors"]),
            "portfolio_sector_weights": dict(screen["portfolio_sector_weights"]),
            "screening_summary": list(screen["screening_summary"]),
            "candidates": [
                {
                    "ticker": item["ticker"],
                    "company_name": item["company_name"],
                    "sector": item.get("sector"),
                    "exchange": item.get("exchange"),
                    "screen_score": self._round_optional(item.get("screen_score"), digits=6),
                    "trailing_return": self._round_optional(item.get("trailing_return"), digits=6),
                    "return_63d": self._round_optional(item.get("return_63d"), digits=6),
                    "operating_margin": self._round_optional(item.get("operating_margin"), digits=6),
                    "net_margin": self._round_optional(item.get("net_margin"), digits=6),
                    "gross_margin": self._round_optional(item.get("gross_margin"), digits=6),
                    "correlation_vs_benchmark": self._round_optional(
                        item.get("correlation_vs_benchmark"),
                        digits=6,
                    ),
                    "correlation_vs_portfolio": self._round_optional(
                        item.get("correlation_vs_portfolio"),
                        digits=6,
                    ),
                    "factor_growth_tilt_beta": self._round_optional(
                        item.get("factor_growth_tilt_beta"),
                        digits=6,
                    ),
                    "factor_momentum_beta": self._round_optional(
                        item.get("factor_momentum_beta"),
                        digits=6,
                    ),
                    "factor_similarity_to_portfolio": self._round_optional(
                        item.get("factor_similarity_to_portfolio"),
                        digits=6,
                    ),
                    "factor_support_score": self._round_optional(
                        item.get("factor_support_score"),
                        digits=6,
                    ),
                }
                for item in screen["candidates"][:max_candidates]
            ],
            "portfolio_sector_exposures": [
                {
                    "sector": item.sector,
                    "weight": round(item.weight, 6),
                    "market_value": round(item.market_value, 2),
                }
                for item in sector_exposures[:8]
            ],
        }

    async def _resolve_shares(
        self,
        position: HypotheticalPosition,
        total_portfolio_value: float,
        *,
        lookback_days: int,
        start_date: date | None,
        end_date: date | None,
    ) -> float:
        if position.shares is not None:
            return position.shares
        price_history = await self.alpha_vantage.get_daily_adjusted(position.ticker)
        sliced_history = self._slice_history(
            frame=price_history,
            lookback_days=lookback_days,
            start_date=start_date,
            end_date=end_date,
        )
        if sliced_history.empty:
            raise ValueError(
                f"No price history is available for {position.ticker} in the requested analysis window."
            )
        latest_price = float(sliced_history["adjusted_close"].iloc[-1])
        return self._shares_for_target_weight(
            total_portfolio_value=total_portfolio_value,
            target_weight=float(position.target_weight),
            latest_price=latest_price,
        )

    @staticmethod
    def _shares_for_target_weight(
        *,
        total_portfolio_value: float,
        target_weight: float,
        latest_price: float,
    ) -> float:
        return (total_portfolio_value * target_weight) / latest_price

    @staticmethod
    def _score_candidate(
        deltas: list[ScenarioDelta],
        candidate_signals: dict[str, float],
        objective: str,
        *,
        optimization_preferences: list[OptimizationPreference] | None = None,
    ) -> float:
        metrics = {delta.metric: delta.delta or 0.0 for delta in deltas}
        if objective == "diversify":
            score = (
                (-metrics["herfindahl_index"] * 3.0)
                + (-metrics["average_pairwise_correlation"] * 2.25)
                + (-metrics["top3_share"] * 2.0)
                + (metrics["return_vs_benchmark"] * 1.0)
                + (metrics["sharpe_ratio"] * 1.25)
                + (-candidate_signals["portfolio_correlation"] * 1.75)
                + (-candidate_signals["factor_similarity_to_portfolio"] * 1.0)
                + (-candidate_signals["candidate_sector_current_weight"] * 2.0)
                + (-candidate_signals["largest_sector_weight_delta"] * 2.0)
                + (candidate_signals["new_sector_bonus"] * 0.75)
            )
        elif objective == "reduce_macro_sensitivity":
            score = (
                (-metrics["beta_vs_benchmark"] * 3.0)
                + (-metrics["annualized_volatility"] * 2.0)
                + (metrics["return_vs_benchmark"] * 0.75)
                + (-candidate_signals["portfolio_correlation"] * 1.0)
                + (-candidate_signals["factor_similarity_to_portfolio"] * 0.75)
            )
        else:
            score = (
                (metrics["sharpe_ratio"] * 3.5)
                + (metrics["return_vs_benchmark"] * 2.5)
                + (metrics["trailing_return"] * 1.5)
                + (-metrics["beta_vs_benchmark"] * 2.0)
                + (-metrics["annualized_volatility"] * 1.5)
                + (-metrics["average_pairwise_correlation"] * 1.5)
                + (-candidate_signals["portfolio_correlation"] * 2.0)
                + (candidate_signals["factor_support_score"] * 0.75)
                + (-candidate_signals["factor_similarity_to_portfolio"] * 0.25)
            )
        for preference in optimization_preferences or []:
            score += 2.5 * ScenarioService._preference_metric_value(
                preference.metric,
                preference.direction,
                metrics=metrics,
                candidate_signals=candidate_signals,
            )
        return score

    @classmethod
    def _candidate_rationale(
        cls,
        deltas: list[ScenarioDelta],
        candidate_signals: dict[str, float],
        objective: str,
        *,
        optimization_preferences: list[OptimizationPreference] | None = None,
    ) -> list[str]:
        metrics = {delta.metric: delta.delta or 0.0 for delta in deltas}
        quality_line = (
            "Fundamental screen kept it for "
            f"operating margin {cls._format_pct(candidate_signals.get('operating_margin'))}, "
            f"net margin {cls._format_pct(candidate_signals.get('net_margin'))}, "
            f"gross margin {cls._format_pct(candidate_signals.get('gross_margin'))}, "
            f"63-day return {cls._format_pct(candidate_signals.get('return_63d'))}, "
            f"and benchmark correlation {cls._format_decimal(candidate_signals.get('correlation_vs_benchmark'))}."
        )
        factor_line = (
            "Factor profile over the aligned window showed "
            f"growth tilt beta {cls._format_decimal(candidate_signals.get('factor_growth_tilt_beta'))}, "
            f"momentum beta {cls._format_decimal(candidate_signals.get('factor_momentum_beta'))}, "
            f"factor similarity to the current portfolio {cls._format_decimal(candidate_signals.get('factor_similarity_to_portfolio'))}, "
            f"and factor support score {cls._format_decimal(candidate_signals.get('factor_support_score'))}."
        )
        preference_line = cls._optimization_preference_rationale(
            optimization_preferences or [],
            metrics=metrics,
            candidate_signals=candidate_signals,
        )
        if objective == "diversify":
            rationale = [
                quality_line,
                factor_line,
                f"Herfindahl changes by {metrics['herfindahl_index']:.4f} and average pairwise correlation changes by {metrics['average_pairwise_correlation']:.4f}.",
                f"Top 3 weight changes by {metrics['top3_share'] * 100:.2f} percentage points while return vs benchmark changes by {metrics['return_vs_benchmark'] * 100:.2f} percentage points.",
                (
                    "Candidate sector weight was "
                    f"{candidate_signals['candidate_sector_current_weight'] * 100:.2f}% before the addition and "
                    f"would be {candidate_signals['candidate_sector_after_weight'] * 100:.2f}% after it."
                ),
                f"Standalone correlation to the current portfolio was {candidate_signals['portfolio_correlation']:.4f} over the selected window.",
            ]
            return [preference_line, *rationale] if preference_line else rationale
        if objective == "reduce_macro_sensitivity":
            rationale = [
                quality_line,
                factor_line,
                f"Portfolio beta changes by {metrics['beta_vs_benchmark']:.4f} while annualized volatility changes by {metrics['annualized_volatility'] * 100:.2f} percentage points.",
                f"Return vs benchmark changes by {metrics['return_vs_benchmark'] * 100:.2f} percentage points.",
                f"Standalone correlation to the current portfolio was {candidate_signals['portfolio_correlation']:.4f} over the selected window.",
            ]
            return [preference_line, *rationale] if preference_line else rationale
        rationale = [
            quality_line,
            factor_line,
            (
                f"Sharpe changes by {metrics['sharpe_ratio']:.4f}, return vs benchmark changes by "
                f"{metrics['return_vs_benchmark'] * 100:.2f} percentage points, and trailing return changes by "
                f"{metrics['trailing_return'] * 100:.2f} percentage points."
            ),
            f"Portfolio beta changes by {metrics['beta_vs_benchmark']:.4f} and annualized volatility changes by {metrics['annualized_volatility'] * 100:.2f} percentage points.",
            f"Average pairwise correlation changes by {metrics['average_pairwise_correlation']:.4f}; standalone correlation to the current portfolio was {candidate_signals['portfolio_correlation']:.4f}.",
        ]
        return [preference_line, *rationale] if preference_line else rationale

    @staticmethod
    def _candidate_signals(
        *,
        baseline_bundle: AnalyticsBundle,
        after_bundle: AnalyticsBundle,
        candidate_history: pd.Series,
        candidate_sector: str | None,
        screen_row: dict[str, Any] | None = None,
    ) -> dict[str, float]:
        candidate_returns = candidate_history.pct_change().dropna()
        aligned = pd.concat(
            [candidate_returns.rename("candidate"), baseline_bundle.portfolio_returns.rename("portfolio")],
            axis=1,
        ).dropna()
        portfolio_corr = float(aligned["candidate"].corr(aligned["portfolio"])) if not aligned.empty else 0.0
        baseline_sector_weights = {
            item.sector: item.weight for item in baseline_bundle.baseline.sector_exposures
        }
        after_sector_weights = {
            item.sector: item.weight for item in after_bundle.baseline.sector_exposures
        }
        sector_key = candidate_sector or "Unknown"
        current_sector_weight = float(baseline_sector_weights.get(sector_key, 0.0))
        after_sector_weight = float(after_sector_weights.get(sector_key, 0.0))
        largest_sector_before = float(baseline_bundle.baseline.sector_exposures[0].weight)
        largest_sector_after = float(after_bundle.baseline.sector_exposures[0].weight)
        screen = screen_row or {}
        return {
            "portfolio_correlation": 0.0 if pd.isna(portfolio_corr) else portfolio_corr,
            "candidate_sector_current_weight": current_sector_weight,
            "candidate_sector_after_weight": after_sector_weight,
            "largest_sector_weight_delta": largest_sector_after - largest_sector_before,
            "new_sector_bonus": 1.0 if current_sector_weight == 0.0 else 0.0,
            "operating_margin": ScenarioService._as_float(screen.get("operating_margin")),
            "net_margin": ScenarioService._as_float(screen.get("net_margin")),
            "gross_margin": ScenarioService._as_float(screen.get("gross_margin")),
            "return_63d": ScenarioService._as_float(screen.get("return_63d")),
            "correlation_vs_benchmark": ScenarioService._as_float(screen.get("correlation_vs_benchmark")),
            "factor_growth_tilt_beta": ScenarioService._nan_to_zero(screen.get("factor_growth_tilt_beta")),
            "factor_momentum_beta": ScenarioService._nan_to_zero(screen.get("factor_momentum_beta")),
            "factor_similarity_to_portfolio": ScenarioService._nan_to_zero(screen.get("factor_similarity_to_portfolio")),
            "factor_support_score": ScenarioService._nan_to_zero(screen.get("factor_support_score")),
        }

    @staticmethod
    def _meets_risk_adjusted_screen(deltas: list[ScenarioDelta]) -> bool:
        metrics = {delta.metric: delta.delta for delta in deltas}
        sharpe_delta = metrics.get("sharpe_ratio")
        return_delta = metrics.get("trailing_return")
        beta_delta = metrics.get("beta_vs_benchmark")
        volatility_delta = metrics.get("annualized_volatility")
        if sharpe_delta is None or return_delta is None or beta_delta is None or volatility_delta is None:
            return False
        return sharpe_delta > 0 and return_delta >= 0 and beta_delta <= 0 and volatility_delta <= 0

    @classmethod
    def _meets_candidate_constraints(
        cls,
        deltas: list[ScenarioDelta],
        *,
        objective: str,
        optimization_preferences: list[OptimizationPreference],
    ) -> bool:
        metrics = {delta.metric: delta.delta for delta in deltas}
        for preference in optimization_preferences:
            if not preference.hard_constraint:
                continue
            value = metrics.get(preference.metric)
            if value is None:
                return False
            if preference.direction == "maximize" and value < 0:
                return False
            if preference.direction == "minimize" and value > 0:
                return False
        if objective == "performance":
            return cls._meets_risk_adjusted_screen(deltas)
        return True

    async def _screen_candidate_universe(
        self,
        *,
        baseline_bundle: AnalyticsBundle,
        benchmark_symbol: str,
        objective: str,
        optimization_preferences: list[OptimizationPreference],
        lookback_days: int,
        start_date: date,
        end_date: date,
        max_candidates: int,
        candidate_tickers: list[str] | None = None,
        preferred_sectors: list[str] | None = None,
        excluded_sectors: list[str] | None = None,
    ) -> dict[str, Any]:
        candidate_rows = self._candidate_universe_rows(candidate_tickers)
        current_tickers = {holding.ticker for holding in baseline_bundle.holdings}
        portfolio_sector_weights = {
            item.sector: item.weight
            for item in baseline_bundle.baseline.sector_exposures
            if item.sector
        }
        preferred = {
            str(sector).upper().strip()
            for sector in (preferred_sectors or [])
            if str(sector).strip()
        }
        excluded = {
            str(sector).upper().strip()
            for sector in (excluded_sectors or [])
            if str(sector).strip()
        }
        filtered_rows = [
            row
            for row in candidate_rows
            if str(row.get("ticker") or "").upper().strip() not in current_tickers
            and str(row.get("sector") or "").upper().strip() not in excluded
        ]
        focus_sectors = preferred or focus_candidate_universe_sectors(
            objective=objective,
            portfolio_sector_weights=portfolio_sector_weights,
            all_rows=filtered_rows,
        )
        fundamental_frame = self._fundamental_screen_frame(
            candidate_rows=filtered_rows,
            portfolio_sector_weights=portfolio_sector_weights,
            objective=objective,
            focus_sectors=focus_sectors,
            preferred_sectors=preferred,
        )
        if fundamental_frame.empty:
            return {
                "universe_size": 0,
                "fundamental_pool_size": 0,
                "price_screen_size": 0,
                "focus_sectors": sorted(focus_sectors),
                "portfolio_sector_weights": dict(sorted(portfolio_sector_weights.items())),
                "candidates": [],
                "screening_summary": [
                    "No common-equity candidates remained after excluding current holdings and unsupported symbols."
                ],
            }

        fundamental_pool_limit = min(
            len(fundamental_frame),
            len(candidate_tickers or [])
            or max(FUNDAMENTAL_SHORTLIST_LIMIT, max_candidates * 8),
        )
        fundamental_pool = fundamental_frame.head(fundamental_pool_limit)
        price_screen_frame = await self._price_screen_frame(
            baseline_bundle=baseline_bundle,
            benchmark_symbol=benchmark_symbol,
            objective=objective,
            optimization_preferences=optimization_preferences,
            lookback_days=lookback_days,
            start_date=start_date,
            end_date=end_date,
            screened_frame=fundamental_pool,
            portfolio_sector_weights=portfolio_sector_weights,
        )
        price_screen_limit = min(
            len(price_screen_frame),
            len(candidate_tickers or [])
            or max(PRICE_SCREEN_LIMIT, max_candidates * 4),
        )
        ranked_candidates = (
            price_screen_frame.sort_values(
                by=["screen_score", "fundamental_score", "momentum_score"],
                ascending=False,
            )
            .head(price_screen_limit)
            .to_dict("records")
        )
        screening_summary = [
            (
                "Started from "
                f"{len(filtered_rows)} common-equity candidates after excluding current holdings, ETFs, and non-rankable symbols."
            ),
            (
                "Applied a local fundamental quality screen using SEC-derived margins, liquidity, leverage, and "
                f"sector-fit signals, keeping {len(fundamental_pool)} names for price-based EDA."
            ),
        ]
        if not price_screen_frame.empty:
            if bool(price_screen_frame.attrs.get("fallback_used")):
                screening_summary.append(
                    "No candidate passed every price/quality gate, so the screen fell back to the best available price-history cohort."
                )
            screening_summary.append(
                "Fetched aligned price histories and local factor exposures for "
                f"{int(price_screen_frame.attrs.get('price_history_count', len(price_screen_frame)))} names and kept "
                f"{len(price_screen_frame)} after positive-price-strength, quality, and factor-fit checks."
            )
        else:
            screening_summary.append(
                "No candidate had enough aligned price history to pass the price-strength EDA stage."
            )
        return {
            "universe_size": len(filtered_rows),
            "fundamental_pool_size": len(fundamental_pool),
            "price_screen_size": len(price_screen_frame),
            "focus_sectors": sorted(focus_sectors),
            "portfolio_sector_weights": dict(sorted(portfolio_sector_weights.items())),
            "candidates": ranked_candidates,
            "screening_summary": screening_summary,
        }

    def _fundamental_screen_frame(
        self,
        *,
        candidate_rows: list[dict[str, Any]],
        portfolio_sector_weights: dict[str, float],
        objective: str,
        focus_sectors: set[str],
        preferred_sectors: set[str],
    ) -> pd.DataFrame:
        if not candidate_rows:
            return pd.DataFrame()
        frame = self._feature_store_fundamental_frame(candidate_rows)
        if frame.empty:
            return frame
        for column in (
            "net_margin",
            "operating_margin",
            "gross_margin",
            "current_ratio",
            "debt_to_revenue",
        ):
            if column not in frame:
                frame[column] = np.nan
        normalized_sector_weights = {
            str(sector).upper().strip(): float(weight)
            for sector, weight in portfolio_sector_weights.items()
            if str(sector).strip()
        }
        frame["sector_upper"] = frame["sector"].fillna("Unknown").astype(str).str.upper().str.strip()
        frame["current_sector_weight"] = frame["sector_upper"].map(normalized_sector_weights).fillna(0.0)
        frame["new_sector_bonus"] = (frame["current_sector_weight"] == 0.0).astype(float)
        frame["focus_sector_bonus"] = frame["sector_upper"].isin(focus_sectors).astype(float)
        frame["preferred_sector_bonus"] = frame["sector_upper"].isin(preferred_sectors).astype(float)
        frame["defensive_bonus"] = frame["sector_upper"].isin(DEFENSIVE_SECTORS).astype(float)
        fundamental_columns = [
            "net_margin",
            "operating_margin",
            "gross_margin",
            "current_ratio",
            "debt_to_revenue",
        ]
        frame["fundamental_data_points"] = frame[fundamental_columns].notna().sum(axis=1)
        frame["profitability_score"] = self._combine_rank_scores(
            frame,
            higher_better=["net_margin", "operating_margin", "gross_margin"],
            lower_better=[],
        )
        frame["balance_sheet_score"] = self._combine_rank_scores(
            frame,
            higher_better=["current_ratio"],
            lower_better=["debt_to_revenue"],
        )
        frame["coverage_score"] = (
            frame["fundamental_data_points"] / float(len(fundamental_columns))
        ).clip(lower=0.0, upper=1.0)
        frame["sector_fit_score"] = (
            (1.0 - frame["current_sector_weight"]).clip(lower=0.0, upper=1.0)
            + (frame["new_sector_bonus"] * 0.2)
            + (frame["focus_sector_bonus"] * 0.1)
            + (frame["preferred_sector_bonus"] * 0.15)
            + (
                frame["defensive_bonus"] * 0.1
                if objective == "performance"
                else 0.0
            )
        ).clip(lower=0.0, upper=1.0)
        if objective == "diversify":
            frame["fundamental_score"] = (
                frame["sector_fit_score"] * 0.4
                + frame["profitability_score"] * 0.25
                + frame["balance_sheet_score"] * 0.2
                + frame["coverage_score"] * 0.15
            )
        elif objective == "reduce_macro_sensitivity":
            frame["fundamental_score"] = (
                frame["balance_sheet_score"] * 0.35
                + frame["profitability_score"] * 0.25
                + frame["sector_fit_score"] * 0.2
                + frame["coverage_score"] * 0.2
            )
        else:
            frame["fundamental_score"] = (
                frame["profitability_score"] * 0.35
                + frame["balance_sheet_score"] * 0.2
                + frame["sector_fit_score"] * 0.25
                + frame["coverage_score"] * 0.2
            )
        if self.feature_store is None or frame["fundamental_data_points"].eq(0).all():
            prioritized = prioritize_candidate_universe_rows(
                candidate_rows=candidate_rows,
                current_tickers=set(),
                portfolio_sector_weights=portfolio_sector_weights,
                objective=objective,
            )
            ticker_order = {
                str(row.get("ticker") or "").upper().strip(): index for index, row in enumerate(prioritized)
            }
            frame["fundamental_score"] = (
                1.0
                - (
                    frame["ticker"]
                    .map(ticker_order)
                    .fillna(len(ticker_order))
                    / max(len(ticker_order), 1)
                )
            )
        return frame.sort_values(
            by=["fundamental_score", "sector_fit_score", "coverage_score"],
            ascending=False,
        ).reset_index(drop=True)

    def _feature_store_fundamental_frame(self, candidate_rows: list[dict[str, Any]]) -> pd.DataFrame:
        metadata_frame = pd.DataFrame(
            [
                {
                    "ticker": str(row.get("ticker") or "").upper().strip(),
                    "company_name": row.get("company_name"),
                    "sector": row.get("sector"),
                    "exchange": row.get("exchange"),
                    "cik": row.get("cik"),
                }
                for row in candidate_rows
                if str(row.get("ticker") or "").upper().strip()
            ]
        ).drop_duplicates(subset=["ticker"])
        if metadata_frame.empty or self.feature_store is None:
            return metadata_frame
        panel_rows = self.feature_store.latest_company_fundamentals_panel(
            metadata_frame["ticker"].tolist(),
            metrics=FUNDAMENTAL_METRICS,
        )
        if not panel_rows:
            return metadata_frame
        panel = pd.DataFrame(panel_rows)
        if panel.empty:
            return metadata_frame
        pivot = (
            panel.pivot_table(index="ticker", columns="metric", values="value", aggfunc="first")
            .reset_index()
        )
        merged = metadata_frame.merge(pivot, on="ticker", how="left")
        return StockDatasetBuilder._derive_financial_ratios(merged)

    async def _price_screen_frame(
        self,
        *,
        baseline_bundle: AnalyticsBundle,
        benchmark_symbol: str,
        objective: str,
        optimization_preferences: list[OptimizationPreference],
        lookback_days: int,
        start_date: date,
        end_date: date,
        screened_frame: pd.DataFrame,
        portfolio_sector_weights: dict[str, float],
    ) -> pd.DataFrame:
        if screened_frame.empty:
            return pd.DataFrame()
        portfolio_factor_profile = self._portfolio_factor_profile(
            baseline_bundle=baseline_bundle,
            start_date=start_date,
            end_date=end_date,
        )
        tickers = screened_frame["ticker"].astype(str).tolist()
        comparison = await self.stock_dataset_builder.build_cross_section(
            tickers=tickers,
            benchmark_symbol=benchmark_symbol,
            lookback_days=lookback_days,
            start_date=start_date,
            end_date=end_date,
            comparison_universe="custom_ticker_basket",
            comparison_ticker_limit=len(tickers),
            portfolio_tickers=[holding.ticker for holding in baseline_bundle.holdings],
            portfolio_returns=baseline_bundle.portfolio_returns,
            comparison_objective=objective,
            portfolio_sector_weights=portfolio_sector_weights,
        )
        if comparison.empty:
            return comparison
        merged = comparison.merge(
            screened_frame[
                [
                    "ticker",
                    "exchange",
                    "cik",
                    "fundamental_score",
                    "profitability_score",
                    "balance_sheet_score",
                    "coverage_score",
                    "sector_fit_score",
                    "current_sector_weight",
                    "new_sector_bonus",
                ]
            ],
            on="ticker",
            how="left",
        )
        for column in (
            "operating_margin",
            "net_margin",
            "gross_margin",
            "current_ratio",
            "debt_to_revenue",
            "factor_growth_tilt_beta",
            "factor_momentum_beta",
            "factor_market_beta",
            "factor_size_beta",
            "factor_value_beta",
            "factor_profitability_beta",
            "factor_investment_beta",
            "factor_r_squared",
        ):
            if column not in merged:
                merged[column] = np.nan
        merged["factor_similarity_to_portfolio"] = merged.apply(
            lambda row: factor_similarity_to_profile(row, portfolio_factor_profile),
            axis=1,
        )
        merged["factor_support_score"] = merged.apply(
            lambda row: factor_support_score(row, portfolio_factor_profile),
            axis=1,
        )
        merged["momentum_score"] = self._combine_rank_scores(
            merged,
            higher_better=["trailing_return", "return_63d", "return_21d"],
            lower_better=[],
        )
        merged["stability_score"] = self._combine_rank_scores(
            merged,
            higher_better=[],
            lower_better=["annualized_volatility", "beta_vs_benchmark", "correlation_vs_benchmark"],
        )
        merged["portfolio_correlation_score"] = self._combine_rank_scores(
            merged,
            higher_better=[],
            lower_better=["correlation_vs_portfolio"],
        )
        merged["factor_diversification_score"] = self._combine_rank_scores(
            merged,
            higher_better=[],
            lower_better=["factor_similarity_to_portfolio"],
        )
        merged["factor_support_rank"] = self._combine_rank_scores(
            merged,
            higher_better=["factor_support_score", "factor_momentum_beta", "factor_growth_tilt_beta"],
            lower_better=[],
        )
        merged["price_growth_positive"] = (
            merged["trailing_return"].fillna(-1.0).gt(0.0)
            | merged["return_63d"].fillna(-1.0).gt(0.0)
            | merged["return_21d"].fillna(-1.0).gt(0.0)
        )
        margin_support = (
            merged["operating_margin"].fillna(-1.0).gt(0.05)
            | merged["net_margin"].fillna(-1.0).gt(0.03)
            | merged["gross_margin"].fillna(-1.0).gt(0.25)
        )
        balance_support = (
            merged["current_ratio"].fillna(1.0).ge(1.0)
            | merged["debt_to_revenue"].fillna(0.0).le(1.0)
        )
        margin_missing = merged[["operating_margin", "net_margin", "gross_margin"]].isna().all(axis=1)
        balance_missing = merged[["current_ratio", "debt_to_revenue"]].isna().all(axis=1)
        merged["quality_gate_pass"] = (margin_support | margin_missing) & (balance_support | balance_missing)
        if objective == "diversify":
            merged["screen_score"] = (
                merged["sector_fit_score"] * 0.25
                + merged["portfolio_correlation_score"] * 0.25
                + merged["stability_score"] * 0.2
                + merged["profitability_score"] * 0.15
                + merged["momentum_score"] * 0.15
                + merged["factor_diversification_score"] * 0.15
            )
        elif objective == "reduce_macro_sensitivity":
            merged["screen_score"] = (
                merged["stability_score"] * 0.35
                + merged["balance_sheet_score"] * 0.25
                + merged["portfolio_correlation_score"] * 0.15
                + merged["momentum_score"] * 0.15
                + merged["fundamental_score"] * 0.1
                + merged["factor_diversification_score"] * 0.1
            )
        else:
            merged["screen_score"] = (
                merged["fundamental_score"] * 0.3
                + merged["momentum_score"] * 0.25
                + merged["stability_score"] * 0.2
                + merged["portfolio_correlation_score"] * 0.15
                + merged["sector_fit_score"] * 0.1
                + merged["factor_support_rank"] * 0.1
            )
        for preference in optimization_preferences:
            merged["screen_score"] = merged["screen_score"] + (
                0.35
                * self._preference_metric_value_series(
                    merged,
                    metric=preference.metric,
                    direction=preference.direction,
                )
            )
        mask = self._objective_price_screen_mask(
            merged,
            objective=objective,
            optimization_preferences=optimization_preferences,
        )
        screened = merged.loc[mask].copy()
        fallback_used = False
        if screened.empty:
            fallback_used = True
            screened = merged.copy()
        screened.attrs["fallback_used"] = fallback_used
        screened.attrs["price_history_count"] = len(merged)
        return screened.reset_index(drop=True)

    @staticmethod
    def _objective_price_screen_mask(
        frame: pd.DataFrame,
        *,
        objective: str,
        optimization_preferences: list[OptimizationPreference],
    ) -> pd.Series:
        if frame.empty:
            return pd.Series(dtype=bool)
        price_floor = frame["price_growth_positive"].fillna(False)
        quality_floor = frame["quality_gate_pass"].fillna(False)
        hard_constraints = pd.Series(True, index=frame.index)
        for preference in optimization_preferences:
            if not preference.hard_constraint:
                continue
            series = ScenarioService._frame_metric_series(frame, preference.metric)
            if preference.direction == "maximize":
                hard_constraints = hard_constraints & series.fillna(-10.0).ge(0.0)
            else:
                hard_constraints = hard_constraints & series.fillna(10.0).le(0.0)
        if objective == "diversify":
            return hard_constraints & quality_floor & (
                price_floor | frame["trailing_return"].fillna(-1.0).gt(-0.05)
            )
        if objective == "reduce_macro_sensitivity":
            return (
                hard_constraints
                & quality_floor
                & price_floor
                & (
                    frame["beta_vs_benchmark"].fillna(10.0).le(1.0)
                    | frame["correlation_vs_benchmark"].fillna(10.0).le(0.75)
                )
            )
        return hard_constraints & quality_floor & price_floor

    @staticmethod
    def _compose_candidate_search_method(
        *,
        objective: str,
        optimization_preferences: list[OptimizationPreference],
        universe_size: int,
        fundamental_pool_size: int,
        price_screen_size: int,
        evaluation_count: int,
    ) -> str:
        objective_label = {
            "performance": "risk-adjusted return improvement",
            "diversify": "diversification improvement",
            "reduce_macro_sensitivity": "macro-sensitivity reduction",
        }.get(objective, objective)
        preference_summary = ""
        if optimization_preferences:
            rendered = ", ".join(
                f"{item.direction} {ScenarioService._metric_label(item.metric)}"
                + (" (hard constraint)" if item.hard_constraint else "")
                for item in optimization_preferences
            )
            preference_summary = f" Optimization focus: {rendered}."
        return (
            f"Two-stage {objective_label} screen over {universe_size} common-equity candidates: "
            f"local fundamental quality filter (margins, liquidity, leverage, sector fit) -> "
            f"price-strength and factor-exposure EDA on {fundamental_pool_size} names, with {price_screen_size} passing the screen -> "
            f"5% addition simulation on {evaluation_count} finalists ranked by marginal Sharpe, "
            "return vs benchmark, beta, volatility, and portfolio-correlation impact."
            f"{preference_summary}"
        )

    def _portfolio_factor_profile(
        self,
        *,
        baseline_bundle: AnalyticsBundle,
        start_date: date,
        end_date: date,
    ) -> dict[str, Any] | None:
        if self.feature_store is None:
            return None
        factor_frame = self.feature_store.factor_model_frame(
            frequency="daily",
            start_date=start_date,
            end_date=end_date,
        )
        if factor_frame.empty:
            return None
        return estimate_factor_profile(baseline_bundle.portfolio_returns, factor_frame)

    @staticmethod
    def _resolved_optimization_preferences(
        *,
        objective: str,
        optimization_preferences: list[OptimizationPreference],
    ) -> list[OptimizationPreference]:
        if optimization_preferences:
            return optimization_preferences
        if objective == "diversify":
            return [
                OptimizationPreference(metric="average_pairwise_correlation", direction="minimize"),
                OptimizationPreference(metric="herfindahl_index", direction="minimize"),
                OptimizationPreference(metric="top3_share", direction="minimize"),
                OptimizationPreference(metric="return_vs_benchmark", direction="maximize"),
            ]
        if objective == "reduce_macro_sensitivity":
            return [
                OptimizationPreference(metric="beta_vs_benchmark", direction="minimize"),
                OptimizationPreference(metric="annualized_volatility", direction="minimize"),
                OptimizationPreference(metric="return_vs_benchmark", direction="maximize"),
            ]
        return [
            OptimizationPreference(metric="sharpe_ratio", direction="maximize"),
            OptimizationPreference(metric="return_vs_benchmark", direction="maximize"),
            OptimizationPreference(metric="beta_vs_benchmark", direction="minimize"),
            OptimizationPreference(metric="annualized_volatility", direction="minimize"),
        ]

    @staticmethod
    def _preference_metric_value(
        metric: str,
        direction: str,
        *,
        metrics: dict[str, float],
        candidate_signals: dict[str, float],
    ) -> float:
        value = metrics.get(metric)
        if value is None:
            if metric == "portfolio_correlation":
                value = candidate_signals.get("portfolio_correlation", 0.0)
            else:
                value = 0.0
        if direction == "maximize":
            return value
        return -value

    @staticmethod
    def _frame_metric_series(frame: pd.DataFrame, metric: str) -> pd.Series:
        if metric in frame:
            return pd.to_numeric(frame[metric], errors="coerce")
        if metric == "sharpe_ratio" and {"trailing_return", "annualized_volatility"} <= set(frame.columns):
            returns = pd.to_numeric(frame["trailing_return"], errors="coerce")
            volatility = pd.to_numeric(frame["annualized_volatility"], errors="coerce")
            volatility = volatility.mask(volatility <= 0.0)
            return returns.div(volatility)
        if metric in {"average_pairwise_correlation", "portfolio_correlation"}:
            if "correlation_vs_portfolio" in frame:
                return pd.to_numeric(frame["correlation_vs_portfolio"], errors="coerce")
            if "correlation_vs_benchmark" in frame:
                return pd.to_numeric(frame["correlation_vs_benchmark"], errors="coerce")
        if metric in {"herfindahl_index", "top3_share"} and "current_sector_weight" in frame:
            return pd.to_numeric(frame["current_sector_weight"], errors="coerce")
        return pd.Series(np.nan, index=frame.index, dtype=float)

    @classmethod
    def _preference_metric_value_series(
        cls,
        frame: pd.DataFrame,
        *,
        metric: str,
        direction: str,
    ) -> pd.Series:
        series = cls._frame_metric_series(frame, metric)
        if direction == "maximize":
            return cls._rank_percentile(series, higher_is_better=True)
        return cls._rank_percentile(series, higher_is_better=False)

    @classmethod
    def _optimization_preference_rationale(
        cls,
        optimization_preferences: list[OptimizationPreference],
        *,
        metrics: dict[str, float],
        candidate_signals: dict[str, float],
    ) -> str | None:
        if not optimization_preferences:
            return None
        fragments = []
        for preference in optimization_preferences:
            value = cls._preference_metric_value(
                preference.metric,
                "maximize",
                metrics=metrics,
                candidate_signals=candidate_signals,
            )
            if preference.metric == "portfolio_correlation":
                rendered = cls._format_decimal(candidate_signals.get("portfolio_correlation"))
            elif "return" in preference.metric or "volatility" in preference.metric:
                rendered = cls._format_pct(value)
            else:
                rendered = cls._format_decimal(value)
            fragments.append(
                f"{preference.direction} {cls._metric_label(preference.metric)} -> {rendered}"
                + (" (hard constraint)" if preference.hard_constraint else "")
            )
        return "Optimization targets: " + "; ".join(fragments) + "."

    @staticmethod
    def _metric_label(metric: str) -> str:
        labels = {
            "sharpe_ratio": "Sharpe ratio",
            "return_vs_benchmark": "return versus SPY",
            "trailing_return": "trailing return",
            "beta_vs_benchmark": "beta versus SPY",
            "annualized_volatility": "annualized volatility",
            "average_pairwise_correlation": "average pairwise correlation",
            "herfindahl_index": "Herfindahl concentration",
            "top3_share": "top 3 weight share",
            "portfolio_correlation": "portfolio correlation",
        }
        return labels.get(metric, metric.replace("_", " "))

    @staticmethod
    def _analysis_window(baseline_bundle: AnalyticsBundle) -> tuple[date, date]:
        return (
            pd.Timestamp(baseline_bundle.baseline.effective_start_date).date(),
            pd.Timestamp(baseline_bundle.baseline.effective_end_date).date(),
        )

    @staticmethod
    def _combine_rank_scores(
        frame: pd.DataFrame,
        *,
        higher_better: list[str],
        lower_better: list[str],
    ) -> pd.Series:
        components: list[pd.Series] = []
        for column in higher_better:
            if column in frame:
                components.append(ScenarioService._rank_percentile(frame[column], higher_is_better=True))
        for column in lower_better:
            if column in frame:
                components.append(ScenarioService._rank_percentile(frame[column], higher_is_better=False))
        if not components:
            return pd.Series(np.zeros(len(frame)), index=frame.index, dtype=float)
        stacked = pd.concat(components, axis=1)
        return stacked.mean(axis=1).fillna(0.0)

    @staticmethod
    def _rank_percentile(series: pd.Series, *, higher_is_better: bool) -> pd.Series:
        numeric = pd.to_numeric(series, errors="coerce")
        if numeric.notna().sum() == 0:
            return pd.Series(np.zeros(len(series)), index=series.index, dtype=float)
        return numeric.rank(pct=True, ascending=higher_is_better).fillna(0.0)

    def _candidate_universe_rows(self, candidate_tickers: list[str] | None = None) -> list[dict[str, Any]]:
        records = [
            item.model_dump()
            for item in self.ticker_metadata.all()
            if item.company_name
            and self._is_rankable_symbol(item.ticker)
            and self._is_common_equity_name(item.company_name)
        ]
        if not records and self.candidate_universe_path.exists():
            records = json.loads(self.candidate_universe_path.read_text(encoding="utf-8"))
        if not candidate_tickers:
            return records
        ticker_order = {
            ticker.upper().strip(): index for index, ticker in enumerate(candidate_tickers) if ticker.strip()
        }
        filtered = [
            row for row in records if str(row.get("ticker", "")).upper().strip() in ticker_order
        ]
        filtered.sort(key=lambda row: ticker_order[str(row.get("ticker", "")).upper().strip()])
        return filtered

    @staticmethod
    def _is_rankable_symbol(ticker: str) -> bool:
        ticker = ticker.upper().strip()
        if not ticker or len(ticker) > 5:
            return False
        if len(ticker) == 5 and ticker[-1] in {"R", "U", "W"}:
            return False
        return ticker.replace(".", "").replace("-", "").isalnum()

    @staticmethod
    def _is_common_equity_name(company_name: str) -> bool:
        lowered = company_name.lower()
        excluded_terms = (
            "warrant",
            "right",
            "unit",
            "preferred",
            "depositary share",
            "etf",
            "exchange traded fund",
            "etn",
            "closed-end fund",
        )
        return not any(term in lowered for term in excluded_terms)

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

    @staticmethod
    def _as_float(value: Any) -> float:
        if value in (None, ""):
            return float("nan")
        numeric = float(value)
        return float("nan") if pd.isna(numeric) else numeric

    @staticmethod
    def _nan_to_zero(value: Any) -> float:
        numeric = ScenarioService._as_float(value)
        return 0.0 if pd.isna(numeric) else float(numeric)

    @staticmethod
    def _format_pct(value: float | None) -> str:
        if value is None or pd.isna(value):
            return "n/a"
        return f"{float(value) * 100:.2f}%"

    @staticmethod
    def _format_decimal(value: float | None) -> str:
        if value is None or pd.isna(value):
            return "n/a"
        return f"{float(value):.4f}"

    @staticmethod
    def _round_optional(value: Any, *, digits: int) -> float | None:
        if value in (None, ""):
            return None
        numeric = float(value)
        if pd.isna(numeric):
            return None
        return round(numeric, digits)
