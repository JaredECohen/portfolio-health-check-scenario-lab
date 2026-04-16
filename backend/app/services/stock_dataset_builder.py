from __future__ import annotations

from collections import Counter
from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from app.services.factor_analytics import estimate_factor_profile, exposure_columns_from_profile
from app.services.alpha_vantage import AlphaVantageService
from app.services.candidate_universe import shortlist_candidate_universe_rows
from app.services.feature_store import FeatureStore
from app.services.sec_edgar import SecEdgarService
from app.services.ticker_metadata import TickerMetadataService


FUNDAMENTAL_METRICS = [
    "Revenues",
    "GrossProfit",
    "OperatingIncomeLoss",
    "NetIncomeLoss",
    "CashAndCashEquivalentsAtCarryingValue",
    "AssetsCurrent",
    "LiabilitiesCurrent",
    "LongTermDebtNoncurrent",
    "LongTermDebtAndCapitalLeaseObligations",
    "CommonStockSharesOutstanding",
]


class StockDatasetBuilder:
    def __init__(
        self,
        *,
        alpha_vantage: AlphaVantageService,
        ticker_metadata_service: TickerMetadataService | None = None,
        feature_store: FeatureStore | None = None,
        sec_edgar_service: SecEdgarService | None = None,
    ) -> None:
        self.alpha_vantage = alpha_vantage
        self.ticker_metadata_service = ticker_metadata_service
        self.feature_store = feature_store
        self.sec_edgar_service = sec_edgar_service

    async def build_cross_section(
        self,
        *,
        tickers: list[str],
        benchmark_symbol: str,
        lookback_days: int,
        start_date: date | None = None,
        end_date: date | None = None,
        comparison_universe: str = "portfolio_only",
        comparison_sector_filters: list[str] | None = None,
        comparison_ticker_limit: int | None = None,
        portfolio_tickers: list[str] | None = None,
        portfolio_returns: pd.Series | None = None,
        comparison_objective: str = "performance",
        portfolio_sector_weights: dict[str, float] | None = None,
    ) -> pd.DataFrame:
        unique_tickers = self._resolve_universe(
            tickers=tickers,
            comparison_universe=comparison_universe,
            comparison_sector_filters=comparison_sector_filters or [],
            comparison_ticker_limit=comparison_ticker_limit,
            portfolio_tickers=portfolio_tickers or [],
            comparison_objective=comparison_objective,
            portfolio_sector_weights=portfolio_sector_weights or {},
        )
        if not unique_tickers:
            return pd.DataFrame()
        benchmark_history = await self.alpha_vantage.get_daily_adjusted(benchmark_symbol, outputsize="full")
        benchmark_series = self._slice_series(
            benchmark_history["adjusted_close"],
            lookback_days=lookback_days,
            start_date=start_date,
            end_date=end_date,
        )
        factor_frame = self._factor_frame(
            start_date=start_date,
            end_date=end_date,
            lookback_days=lookback_days,
        )
        rows: list[dict[str, Any]] = []
        for ticker in unique_tickers:
            try:
                price_history = await self.alpha_vantage.get_daily_adjusted(ticker, outputsize="full")
            except Exception:  # noqa: BLE001
                continue
            price_series = self._slice_series(
                price_history["adjusted_close"],
                lookback_days=lookback_days,
                start_date=start_date,
                end_date=end_date,
            )
            if len(price_series) < 40:
                continue
            metadata = self._metadata_for_ticker(ticker)
            aligned = pd.concat(
                [
                    price_series.rename("price"),
                    benchmark_series.rename("benchmark"),
                ],
                axis=1,
            ).dropna()
            if len(aligned) < 40:
                continue
            returns = aligned["price"].pct_change().dropna()
            benchmark_returns = aligned["benchmark"].pct_change().dropna()
            aligned_returns = pd.concat([returns, benchmark_returns], axis=1).dropna()
            aligned_returns.columns = ["stock", "benchmark"]
            aligned_portfolio_returns = (
                pd.concat(
                    [
                        returns.rename("stock"),
                        portfolio_returns.rename("portfolio"),
                    ],
                    axis=1,
                ).dropna()
                if portfolio_returns is not None
                else pd.DataFrame()
            )
            forward_21d_panel = self._rolling_forward_returns(aligned["price"], 21)
            forward_63d_panel = self._rolling_forward_returns(aligned["price"], 63)
            fundamentals = await self._fundamentals_for_ticker(
                ticker=ticker,
                cik=metadata.get("cik"),
            )
            trailing_return = float(aligned["price"].iloc[-1] / aligned["price"].iloc[0] - 1)
            benchmark_return = float(aligned["benchmark"].iloc[-1] / aligned["benchmark"].iloc[0] - 1)
            row = {
                "ticker": ticker,
                "sector": metadata.get("sector") or "Unknown",
                "company_name": metadata.get("company_name") or ticker,
                "effective_observations": int(len(aligned)),
                "trailing_return": trailing_return,
                "return_vs_benchmark": trailing_return - benchmark_return,
                "return_63d": self._window_return(aligned["price"], 63),
                "return_21d": self._window_return(aligned["price"], 21),
                "forward_21d_return": self._panel_mean(forward_21d_panel),
                "forward_21d_median": self._panel_median(forward_21d_panel),
                "forward_21d_std": self._panel_std(forward_21d_panel),
                "forward_63d_return": self._panel_mean(forward_63d_panel),
                "annualized_volatility": float(returns.std() * np.sqrt(252)),
                "beta_vs_benchmark": self._beta(aligned_returns),
                "correlation_vs_benchmark": float(aligned_returns["stock"].corr(aligned_returns["benchmark"])),
                "correlation_vs_portfolio": (
                    float(aligned_portfolio_returns["stock"].corr(aligned_portfolio_returns["portfolio"]))
                    if not aligned_portfolio_returns.empty
                    else np.nan
                ),
            }
            row.update(fundamentals)
            row.update(
                exposure_columns_from_profile(
                    estimate_factor_profile(returns, factor_frame)
                )
            )
            rows.append(row)
        frame = pd.DataFrame(rows)
        if frame.empty:
            return frame
        return self._derive_financial_ratios(frame)

    @staticmethod
    def _slice_series(
        series: pd.Series,
        *,
        lookback_days: int,
        start_date: date | None,
        end_date: date | None,
    ) -> pd.Series:
        sliced = series.copy()
        if start_date is not None:
            sliced = sliced[sliced.index >= pd.Timestamp(start_date)]
        if end_date is not None:
            sliced = sliced[sliced.index <= pd.Timestamp(end_date)]
        if start_date is None and end_date is None:
            sliced = sliced.tail(lookback_days)
        return sliced

    def _metadata_for_ticker(self, ticker: str) -> dict[str, Any]:
        if self.ticker_metadata_service is None:
            return {"ticker": ticker}
        metadata = self.ticker_metadata_service.get(ticker)
        return metadata.model_dump() if metadata is not None else {"ticker": ticker}

    def _factor_frame(
        self,
        *,
        start_date: date | None,
        end_date: date | None,
        lookback_days: int,
    ) -> pd.DataFrame:
        if self.feature_store is None:
            return pd.DataFrame()
        frame = self.feature_store.factor_model_frame(
            frequency="daily",
            start_date=start_date,
            end_date=end_date,
        )
        if frame.empty:
            return frame
        if start_date is None and end_date is None:
            return frame.tail(lookback_days)
        return frame

    def _resolve_universe(
        self,
        *,
        tickers: list[str],
        comparison_universe: str,
        comparison_sector_filters: list[str],
        comparison_ticker_limit: int | None,
        portfolio_tickers: list[str],
        comparison_objective: str,
        portfolio_sector_weights: dict[str, float],
    ) -> list[str]:
        custom_tickers = [ticker.upper().strip() for ticker in dict.fromkeys(tickers) if ticker.strip()]
        portfolio = [ticker.upper().strip() for ticker in dict.fromkeys(portfolio_tickers) if ticker.strip()]
        base_tickers = [ticker for ticker in dict.fromkeys([*custom_tickers, *portfolio]) if ticker]
        limit = comparison_ticker_limit or 25
        if comparison_universe == "custom_ticker_basket":
            return base_tickers[:limit]
        if comparison_universe == "portfolio_only" or self.ticker_metadata_service is None:
            return base_tickers[:limit]

        sector_filters = {sector.upper().strip() for sector in comparison_sector_filters if sector.strip()}
        universe_rows = [
            item.model_dump()
            for item in self.ticker_metadata_service.all()
            if item.ticker and item.company_name
        ]
        capacity = max(limit - len(base_tickers), 0)
        if capacity <= 0:
            return base_tickers[:limit]

        if comparison_universe == "sector_peers":
            selected = self._sector_peer_tickers(
                universe_rows=universe_rows,
                base_tickers=base_tickers,
                sector_filters=sector_filters,
                limit=capacity,
            )
        elif comparison_universe == "candidate_universe_subset":
            selected = self._candidate_subset_tickers(
                universe_rows=universe_rows,
                base_tickers=base_tickers,
                sector_filters=sector_filters,
                objective=comparison_objective,
                portfolio_sector_weights=portfolio_sector_weights or self._derived_sector_weights(base_tickers),
                limit=capacity,
            )
        else:
            selected = []
        combined = [ticker for ticker in dict.fromkeys([*base_tickers, *selected]) if ticker]
        return combined[:limit]

    def _sector_peer_tickers(
        self,
        *,
        universe_rows: list[dict[str, Any]],
        base_tickers: list[str],
        sector_filters: set[str],
        limit: int,
    ) -> list[str]:
        if limit <= 0:
            return []
        anchor_sectors: Counter[str] = Counter()
        anchor_exchanges: dict[str, set[str]] = {}
        for ticker in base_tickers:
            metadata = self.ticker_metadata_service.get(ticker) if self.ticker_metadata_service is not None else None
            if metadata is None or not metadata.sector:
                continue
            sector_key = metadata.sector.upper().strip()
            anchor_sectors[sector_key] += 1
            exchange_key = (metadata.exchange or "").upper().strip()
            if exchange_key:
                anchor_exchanges.setdefault(sector_key, set()).add(exchange_key)
        ordered_sectors = sorted(sector_filters) or list(anchor_sectors)
        if not ordered_sectors:
            return []
        seen = set(base_tickers)
        buckets: dict[str, list[str]] = {sector: [] for sector in ordered_sectors}
        for row in universe_rows:
            ticker = str(row.get("ticker") or "").upper().strip()
            sector = str(row.get("sector") or "").upper().strip()
            if not ticker or ticker in seen or sector not in buckets:
                continue
            exchange = str(row.get("exchange") or "").upper().strip()
            priority = 0 if exchange and exchange in anchor_exchanges.get(sector, set()) else 1
            buckets[sector].append((priority, ticker))
        for sector, entries in buckets.items():
            entries.sort(key=lambda item: item)
            buckets[sector] = [ticker for _priority, ticker in entries]

        sector_order = sorted(
            ordered_sectors,
            key=lambda sector: (-anchor_sectors.get(sector, 0), sector),
        )
        selected: list[str] = []
        while len(selected) < limit and any(buckets.get(sector) for sector in sector_order):
            for sector in sector_order:
                if not buckets.get(sector):
                    continue
                selected.append(buckets[sector].pop(0))
                if len(selected) >= limit:
                    break
        return selected

    def _candidate_subset_tickers(
        self,
        *,
        universe_rows: list[dict[str, Any]],
        base_tickers: list[str],
        sector_filters: set[str],
        objective: str,
        portfolio_sector_weights: dict[str, float],
        limit: int,
    ) -> list[str]:
        if limit <= 0:
            return []
        shortlist = shortlist_candidate_universe_rows(
            all_rows=universe_rows,
            current_tickers=set(base_tickers),
            portfolio_sector_weights=portfolio_sector_weights,
            objective=objective,
            preferred_sectors=sorted(sector_filters) or None,
            max_candidates=limit,
        )
        return [str(row.get("ticker") or "").upper().strip() for row in shortlist["candidates"]]

    def _derived_sector_weights(self, tickers: list[str]) -> dict[str, float]:
        if self.ticker_metadata_service is None or not tickers:
            return {}
        counts: Counter[str] = Counter()
        for ticker in tickers:
            metadata = self.ticker_metadata_service.get(ticker)
            if metadata is None or not metadata.sector:
                continue
            counts[metadata.sector.upper().strip()] += 1
        total = sum(counts.values())
        if total == 0:
            return {}
        return {sector: count / total for sector, count in counts.items()}

    async def _fundamentals_for_ticker(self, *, ticker: str, cik: str | None) -> dict[str, float | None]:
        if self.feature_store is not None:
            rows = self.feature_store.latest_company_fundamentals(ticker, metrics=FUNDAMENTAL_METRICS)
            if rows:
                return self._fundamentals_from_rows(rows)
        if self.sec_edgar_service is not None and cik:
            try:
                payload = await self.sec_edgar_service.get_company_facts(cik)
            except Exception:  # noqa: BLE001
                return {}
            return self._fundamentals_from_companyfacts(payload)
        return {}

    @staticmethod
    def _fundamentals_from_rows(rows: list[dict[str, Any]]) -> dict[str, float | None]:
        return {
            row["metric"]: float(row["value"]) if row.get("value") is not None else None
            for row in rows
        }

    @staticmethod
    def _fundamentals_from_companyfacts(payload: dict[str, Any]) -> dict[str, float | None]:
        facts = payload.get("facts") or {}
        selected: dict[str, tuple[str, float]] = {}
        for taxonomy in facts.values():
            for metric in FUNDAMENTAL_METRICS:
                metric_payload = taxonomy.get(metric)
                if not metric_payload:
                    continue
                latest_end = None
                latest_value = None
                for unit_rows in (metric_payload.get("units") or {}).values():
                    for item in unit_rows:
                        if item.get("val") in (None, "") or item.get("end") is None:
                            continue
                        if latest_end is None or item["end"] > latest_end:
                            latest_end = item["end"]
                            latest_value = float(item["val"])
                if latest_value is not None:
                    selected[metric] = (latest_end or "", latest_value)
        return {metric: value for metric, (_end, value) in selected.items()}

    @staticmethod
    def _derive_financial_ratios(frame: pd.DataFrame) -> pd.DataFrame:
        enriched = frame.copy()
        if {"NetIncomeLoss", "Revenues"}.issubset(enriched.columns):
            enriched["net_margin"] = enriched["NetIncomeLoss"] / enriched["Revenues"].replace(0, np.nan)
        if {"OperatingIncomeLoss", "Revenues"}.issubset(enriched.columns):
            enriched["operating_margin"] = enriched["OperatingIncomeLoss"] / enriched["Revenues"].replace(0, np.nan)
        if {"GrossProfit", "Revenues"}.issubset(enriched.columns):
            enriched["gross_margin"] = enriched["GrossProfit"] / enriched["Revenues"].replace(0, np.nan)
        if {"AssetsCurrent", "LiabilitiesCurrent"}.issubset(enriched.columns):
            enriched["current_ratio"] = enriched["AssetsCurrent"] / enriched["LiabilitiesCurrent"].replace(0, np.nan)
        debt_column = None
        if "LongTermDebtAndCapitalLeaseObligations" in enriched.columns:
            debt_column = "LongTermDebtAndCapitalLeaseObligations"
        elif "LongTermDebtNoncurrent" in enriched.columns:
            debt_column = "LongTermDebtNoncurrent"
        if debt_column and "Revenues" in enriched.columns:
            enriched["debt_to_revenue"] = enriched[debt_column] / enriched["Revenues"].replace(0, np.nan)
        return enriched.replace([np.inf, -np.inf], np.nan)

    @staticmethod
    def _window_return(series: pd.Series, window: int) -> float | None:
        if len(series) <= window:
            return None
        return float(series.iloc[-1] / series.iloc[-(window + 1)] - 1)

    @staticmethod
    def _rolling_forward_returns(series: pd.Series, horizon: int) -> pd.Series:
        if len(series) <= horizon:
            return pd.Series(dtype=float)
        forward = (series.shift(-horizon) / series) - 1
        return forward.dropna()

    @staticmethod
    def _panel_mean(series: pd.Series) -> float | None:
        if series.empty:
            return None
        return float(series.mean())

    @staticmethod
    def _panel_median(series: pd.Series) -> float | None:
        if series.empty:
            return None
        return float(series.median())

    @staticmethod
    def _panel_std(series: pd.Series) -> float | None:
        if series.empty:
            return None
        return float(series.std(ddof=0))

    @staticmethod
    def _beta(aligned_returns: pd.DataFrame) -> float:
        benchmark_var = float(aligned_returns["benchmark"].var())
        if benchmark_var == 0:
            return 0.0
        return float(aligned_returns.cov().iloc[0, 1] / benchmark_var)
