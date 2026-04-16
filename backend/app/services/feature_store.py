from __future__ import annotations

import json
from datetime import date
from typing import Any

import pandas as pd

from app.database import Database
from app.services.factor_registry import CORE_FACTOR_DATASETS


class FeatureStore:
    def __init__(self, database: Database) -> None:
        self.database = database

    def latest_company_fundamentals(self, ticker: str, metrics: list[str] | None = None) -> list[dict[str, Any]]:
        params: list[Any] = [ticker.upper()]
        metric_filter = ""
        if metrics:
            placeholders = ", ".join("?" for _ in metrics)
            metric_filter = f"AND metric IN ({placeholders})"
            params.extend(metrics)
        query = f"""
            WITH ranked AS (
                SELECT
                    ticker,
                    metric,
                    period_end,
                    fiscal_period,
                    fiscal_year,
                    value,
                    unit,
                    form_type,
                    filed_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY ticker, metric
                        ORDER BY period_end DESC, filed_at DESC
                    ) AS row_num
                FROM fact_company_fundamentals
                WHERE ticker = ?
                {metric_filter}
            )
            SELECT ticker, metric, period_end, fiscal_period, fiscal_year, value, unit, form_type, filed_at
            FROM ranked
            WHERE row_num = 1
            ORDER BY metric
        """
        with self.database.connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def latest_company_fundamentals_panel(
        self,
        tickers: list[str],
        metrics: list[str] | None = None,
        *,
        chunk_size: int = 250,
    ) -> list[dict[str, Any]]:
        normalized = [ticker.upper().strip() for ticker in dict.fromkeys(tickers) if ticker.strip()]
        if not normalized:
            return []
        metric_filter = ""
        metric_params: list[Any] = []
        if metrics:
            placeholders = ", ".join("?" for _ in metrics)
            metric_filter = f"AND metric IN ({placeholders})"
            metric_params.extend(metrics)
        rows: list[dict[str, Any]] = []
        for start in range(0, len(normalized), chunk_size):
            ticker_chunk = normalized[start : start + chunk_size]
            ticker_placeholders = ", ".join("?" for _ in ticker_chunk)
            query = f"""
                WITH ranked AS (
                    SELECT
                        ticker,
                        metric,
                        period_end,
                        fiscal_period,
                        fiscal_year,
                        value,
                        unit,
                        form_type,
                        filed_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY ticker, metric
                            ORDER BY period_end DESC, filed_at DESC
                        ) AS row_num
                    FROM fact_company_fundamentals
                    WHERE ticker IN ({ticker_placeholders})
                    {metric_filter}
                )
                SELECT ticker, metric, period_end, fiscal_period, fiscal_year, value, unit, form_type, filed_at
                FROM ranked
                WHERE row_num = 1
                ORDER BY ticker, metric
            """
            params = [*ticker_chunk, *metric_params]
            with self.database.connect() as connection:
                rows.extend(dict(row) for row in connection.execute(query, params).fetchall())
        return rows

    def trailing_fundamental_trend(self, ticker: str, metric: str, limit: int = 8) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute(
                """
                SELECT ticker, metric, period_end, fiscal_period, fiscal_year, value, unit, form_type, filed_at
                FROM fact_company_fundamentals
                WHERE ticker = ? AND metric = ?
                ORDER BY period_end DESC, filed_at DESC
                LIMIT ?
                """,
                (ticker.upper(), metric, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def macro_snapshot(self, series_ids: list[str]) -> list[dict[str, Any]]:
        if not series_ids:
            return []
        placeholders = ", ".join("?" for _ in series_ids)
        query = f"""
            WITH ranked AS (
                SELECT
                    series_id,
                    date,
                    value,
                    vintage_date,
                    metadata_json,
                    ROW_NUMBER() OVER (
                        PARTITION BY series_id
                        ORDER BY date DESC, vintage_date DESC
                    ) AS row_num
                FROM fact_macro_series
                WHERE series_id IN ({placeholders})
            )
            SELECT ranked.series_id, ranked.date, ranked.value, ranked.vintage_date, ranked.metadata_json,
                   dim_series.source, dim_series.category, dim_series.title, dim_series.unit
            FROM ranked
            JOIN dim_series ON dim_series.series_id = ranked.series_id
            WHERE ranked.row_num = 1
            ORDER BY ranked.series_id
        """
        with self.database.connect() as connection:
            rows = connection.execute(query, series_ids).fetchall()
        results = []
        for row in rows:
            item = dict(row)
            item["metadata"] = json.loads(item.pop("metadata_json")) if item.get("metadata_json") else None
            results.append(item)
        return results

    def factor_model_frame(
        self,
        *,
        frequency: str = "daily",
        start_date: date | None = None,
        end_date: date | None = None,
        dataset_ids: list[str] | None = None,
        factors: list[str] | None = None,
    ) -> pd.DataFrame:
        selected_datasets = dataset_ids or CORE_FACTOR_DATASETS.get(frequency, [])
        if not selected_datasets:
            return pd.DataFrame()
        params: list[Any] = [frequency, *selected_datasets]
        dataset_placeholders = ", ".join("?" for _ in selected_datasets)
        factor_filter = ""
        if factors:
            factor_placeholders = ", ".join("?" for _ in factors)
            factor_filter = f"AND factor IN ({factor_placeholders})"
            params.extend(factors)
        date_filter = ""
        if start_date is not None:
            date_filter += " AND date >= ?"
            params.append(start_date.isoformat())
        if end_date is not None:
            date_filter += " AND date <= ?"
            params.append(end_date.isoformat())
        query = f"""
            SELECT date, factor, value
            FROM fact_factor_returns
            WHERE frequency = ?
              AND dataset_id IN ({dataset_placeholders})
              {factor_filter}
              {date_filter}
            ORDER BY date
        """
        with self.database.connect() as connection:
            rows = connection.execute(query, params).fetchall()
        if not rows:
            return pd.DataFrame()
        frame = pd.DataFrame([dict(row) for row in rows])
        if frame.empty:
            return frame
        pivot = frame.pivot_table(index="date", columns="factor", values="value", aggfunc="first").sort_index()
        pivot.index = pd.to_datetime(pivot.index)
        return pivot
