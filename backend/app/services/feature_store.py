from __future__ import annotations

import json
from typing import Any

from app.database import Database


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
