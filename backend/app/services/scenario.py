from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

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
from app.services.alpha_vantage import AlphaVantageError, AlphaVantageService
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
        lookback_days: int,
        start_date: date | None = None,
        end_date: date | None = None,
        candidate_tickers: list[str] | None = None,
        max_candidates: int = 5,
    ) -> CandidateSearchResult:
        candidate_rows = self._candidate_universe_rows(candidate_tickers)
        benchmark_history = pd.DataFrame({"adjusted_close": baseline_bundle.benchmark_prices})
        current_history = {
            holding.ticker: pd.DataFrame({"adjusted_close": baseline_bundle.price_frame[holding.ticker]})
            for holding in baseline_bundle.holdings
        }
        prioritized_candidates = (
            candidate_rows
            if candidate_tickers
            else self._prioritize_candidates(
                candidate_rows=candidate_rows,
                current_holdings=baseline_bundle.holdings,
                portfolio_sector_weights={
                    item.sector: item.weight for item in baseline_bundle.baseline.sector_exposures
                },
                objective=objective,
            )
        )
        evaluated_ranks: list[CandidateRank] = []
        screened_ranks: list[CandidateRank] = []
        evaluation_limit = min(len(prioritized_candidates), 20 if candidate_tickers else 25)
        for item in prioritized_candidates[:evaluation_limit]:
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
            try:
                candidate_history = await self.alpha_vantage.get_daily_adjusted(metadata.ticker)
            except AlphaVantageError:
                continue
            price_history = dict(current_history)
            sliced_history = self._slice_history(
                frame=candidate_history,
                lookback_days=lookback_days,
                start_date=start_date,
                end_date=end_date,
            )
            if sliced_history.empty:
                continue
            price_history[metadata.ticker] = sliced_history
            shares = await self._resolve_shares(
                hypothetical,
                baseline_bundle.baseline.total_portfolio_value,
                lookback_days=lookback_days,
                start_date=start_date,
                end_date=end_date,
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
            score = self._score_candidate(deltas, candidate_signals, objective)
            rationale = self._candidate_rationale(deltas, candidate_signals, objective)
            rank = CandidateRank(
                ticker=metadata.ticker,
                company_name=metadata.company_name,
                sector=metadata.sector,
                score=score,
                rationale=rationale,
                deltas=deltas,
            )
            evaluated_ranks.append(rank)
            if objective != "performance" or self._meets_risk_adjusted_screen(deltas):
                screened_ranks.append(rank)
        ranks = screened_ranks or evaluated_ranks
        ranks.sort(key=lambda rank: rank.score, reverse=True)
        return CandidateSearchResult(
            objective=objective,
            method=(
                "5% target-weight equity screen over an agent-shortlisted subset drawn from the full U.S. equity universe, "
                "ranked on historical Sharpe improvement, maintained or improved return, lower beta, lower volatility, and lower correlation"
            ),
            candidates=ranks[:max_candidates],
        )

    def shortlist_universe(
        self,
        *,
        baseline_bundle: AnalyticsBundle,
        objective: str,
        preferred_sectors: list[str] | None = None,
        excluded_sectors: list[str] | None = None,
        max_candidates: int = 20,
    ) -> dict[str, Any]:
        all_rows = self._candidate_universe_rows()
        current_tickers = {holding.ticker for holding in baseline_bundle.holdings}
        sector_exposures = baseline_bundle.baseline.sector_exposures
        portfolio_sector_weights = {item.sector: item.weight for item in sector_exposures}
        preferred = {sector.upper().strip() for sector in (preferred_sectors or []) if sector.strip()}
        excluded = {sector.upper().strip() for sector in (excluded_sectors or []) if sector.strip()}
        focus_sectors = preferred or self._focus_sectors(
            objective=objective,
            portfolio_sector_weights=portfolio_sector_weights,
            all_rows=all_rows,
        )

        def row_priority(row: dict[str, Any]) -> tuple[float, float, int, str]:
            ticker = str(row.get("ticker", "")).upper().strip()
            sector = str(row.get("sector") or "Unknown")
            sector_upper = sector.upper()
            current_sector_weight = portfolio_sector_weights.get(sector, 0.0)
            focus_penalty = 0.0 if sector_upper in focus_sectors else 1.0
            exclusion_penalty = 1.0 if sector_upper in excluded else 0.0
            existing_sector_penalty = current_sector_weight
            ticker_penalty = len(ticker)
            return (exclusion_penalty, focus_penalty + existing_sector_penalty, ticker_penalty, ticker)

        shortlisted = [
            {
                "ticker": str(row.get("ticker", "")).upper().strip(),
                "company_name": row.get("company_name"),
                "sector": row.get("sector"),
                "exchange": row.get("exchange"),
            }
            for row in sorted(all_rows, key=row_priority)
            if str(row.get("ticker", "")).upper().strip() not in current_tickers
            and str(row.get("sector") or "Unknown").upper() not in excluded
        ][:max_candidates]
        return {
            "universe_size": len(all_rows),
            "objective": objective,
            "portfolio_sector_exposures": [
                {
                    "sector": item.sector,
                    "weight": round(item.weight, 6),
                    "market_value": round(item.market_value, 2),
                }
                for item in sector_exposures[:8]
            ],
            "focus_sectors": sorted(focus_sectors),
            "candidates": shortlisted,
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
        return (total_portfolio_value * float(position.target_weight)) / latest_price

    @staticmethod
    def _score_candidate(
        deltas: list[ScenarioDelta],
        candidate_signals: dict[str, float],
        objective: str,
    ) -> float:
        metrics = {delta.metric: delta.delta or 0.0 for delta in deltas}
        if objective == "diversify":
            return (
                (-metrics["herfindahl_index"] * 3.0)
                + (-metrics["average_pairwise_correlation"] * 2.0)
                + (-metrics["top3_share"] * 2.0)
                + (metrics["sharpe_ratio"] * 1.5)
                + (-candidate_signals["portfolio_correlation"] * 1.5)
                + (-candidate_signals["candidate_sector_current_weight"] * 2.0)
                + (-candidate_signals["largest_sector_weight_delta"] * 2.0)
                + (candidate_signals["new_sector_bonus"] * 0.75)
            )
        if objective == "reduce_macro_sensitivity":
            return (
                (-metrics["beta_vs_benchmark"] * 3.0)
                + (-metrics["annualized_volatility"] * 2.0)
                + (-candidate_signals["portfolio_correlation"] * 1.0)
            )
        return (
            (metrics["sharpe_ratio"] * 4.0)
            + (metrics["trailing_return"] * 2.5)
            + (-metrics["beta_vs_benchmark"] * 2.0)
            + (-metrics["annualized_volatility"] * 1.5)
            + (-metrics["average_pairwise_correlation"] * 1.5)
            + (-candidate_signals["portfolio_correlation"] * 2.0)
        )

    @staticmethod
    def _candidate_rationale(
        deltas: list[ScenarioDelta],
        candidate_signals: dict[str, float],
        objective: str,
    ) -> list[str]:
        metrics = {delta.metric: delta.delta or 0.0 for delta in deltas}
        if objective == "diversify":
            return [
                f"Herfindahl changes by {metrics['herfindahl_index']:.4f}.",
                f"Average pairwise correlation changes by {metrics['average_pairwise_correlation']:.4f}.",
                f"Top 3 weight changes by {metrics['top3_share'] * 100:.2f} percentage points.",
                f"Standalone correlation to the current portfolio was {candidate_signals['portfolio_correlation']:.4f} over the selected window.",
                f"Candidate sector weight was {candidate_signals['candidate_sector_current_weight'] * 100:.2f}% before the addition and would be {candidate_signals['candidate_sector_after_weight'] * 100:.2f}% after it.",
            ]
        if objective == "reduce_macro_sensitivity":
            return [
                f"Portfolio beta changes by {metrics['beta_vs_benchmark']:.4f}.",
                f"Annualized volatility changes by {metrics['annualized_volatility'] * 100:.2f} percentage points.",
                f"Standalone correlation to the current portfolio was {candidate_signals['portfolio_correlation']:.4f} over the selected window.",
            ]
        return [
            f"Sharpe changes by {metrics['sharpe_ratio']:.4f} while trailing return changes by {metrics['trailing_return'] * 100:.2f} percentage points.",
            f"Portfolio beta changes by {metrics['beta_vs_benchmark']:.4f} and annualized volatility changes by {metrics['annualized_volatility'] * 100:.2f} percentage points.",
            f"Average pairwise correlation changes by {metrics['average_pairwise_correlation']:.4f}; standalone correlation to the current portfolio was {candidate_signals['portfolio_correlation']:.4f}.",
        ]

    @staticmethod
    def _candidate_signals(
        *,
        baseline_bundle: AnalyticsBundle,
        after_bundle: AnalyticsBundle,
        candidate_history: pd.Series,
        candidate_sector: str | None,
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
        return {
            "portfolio_correlation": 0.0 if pd.isna(portfolio_corr) else portfolio_corr,
            "candidate_sector_current_weight": current_sector_weight,
            "candidate_sector_after_weight": after_sector_weight,
            "largest_sector_weight_delta": largest_sector_after - largest_sector_before,
            "new_sector_bonus": 1.0 if current_sector_weight == 0.0 else 0.0,
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

    @staticmethod
    def _prioritize_candidates(
        *,
        candidate_rows: list[dict[str, str]],
        current_holdings: list[Holding],
        portfolio_sector_weights: dict[str, float],
        objective: str,
    ) -> list[dict[str, str]]:
        current_tickers = {holding.ticker for holding in current_holdings}

        def sort_key(row: dict[str, str]) -> tuple[float, float, str]:
            ticker = row.get("ticker", "")
            sector = row.get("sector") or "Unknown"
            sector_upper = sector.upper()
            in_portfolio = 1.0 if ticker in current_tickers else 0.0
            sector_weight = portfolio_sector_weights.get(sector, 0.0)
            if objective == "performance":
                defensive_bonus = (
                    0.0 if sector_upper in {"UTILITIES", "CONSUMER STAPLES", "HEALTHCARE"} else 0.5
                )
                return (in_portfolio, sector_weight + defensive_bonus, ticker)
            return (in_portfolio, sector_weight, ticker)

        return sorted(candidate_rows, key=sort_key)

    def _candidate_universe_rows(self, candidate_tickers: list[str] | None = None) -> list[dict[str, Any]]:
        records = [
            item.model_dump()
            for item in self.ticker_metadata.all()
            if item.company_name and self._is_rankable_symbol(item.ticker) and self._is_common_equity_name(item.company_name)
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
    def _focus_sectors(
        *,
        objective: str,
        portfolio_sector_weights: dict[str, float],
        all_rows: list[dict[str, Any]],
    ) -> set[str]:
        universe_sectors = sorted(
            {
                str(row.get("sector") or "Unknown").upper()
                for row in all_rows
                if str(row.get("sector") or "").strip()
            }
        )
        if objective == "performance":
            preferred = {"HEALTHCARE", "CONSUMER STAPLES", "UTILITIES"}
            return preferred & set(universe_sectors) or set(universe_sectors[:3])
        sorted_portfolio_sectors = sorted(
            portfolio_sector_weights.items(),
            key=lambda item: item[1],
        )
        underweight = {
            sector.upper()
            for sector, weight in sorted_portfolio_sectors
            if weight < 0.1
        }
        unowned = {
            sector for sector in universe_sectors if sector not in {key.upper() for key in portfolio_sector_weights}
        }
        return (unowned or underweight or set(universe_sectors[:4]))

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
