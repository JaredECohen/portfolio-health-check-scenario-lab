from __future__ import annotations

from typing import Any


def shortlist_candidate_universe_rows(
    *,
    all_rows: list[dict[str, Any]],
    current_tickers: set[str] | list[str],
    portfolio_sector_weights: dict[str, float],
    objective: str,
    preferred_sectors: list[str] | None = None,
    excluded_sectors: list[str] | None = None,
    max_candidates: int = 20,
) -> dict[str, Any]:
    current = {_normalize_ticker(ticker) for ticker in current_tickers if _normalize_ticker(ticker)}
    sector_weights = _normalized_sector_weights(portfolio_sector_weights)
    preferred = {_normalize_sector(sector) for sector in (preferred_sectors or []) if _normalize_sector(sector)}
    excluded = {_normalize_sector(sector) for sector in (excluded_sectors or []) if _normalize_sector(sector)}
    focus_sectors = preferred or focus_candidate_universe_sectors(
        objective=objective,
        portfolio_sector_weights=sector_weights,
        all_rows=all_rows,
    )

    def row_priority(row: dict[str, Any]) -> tuple[float, float, int, str]:
        ticker = _normalize_ticker(row.get("ticker"))
        sector_upper = _normalize_sector(row.get("sector")) or "UNKNOWN"
        current_sector_weight = sector_weights.get(sector_upper, 0.0)
        focus_penalty = 0.0 if sector_upper in focus_sectors else 1.0
        exclusion_penalty = 1.0 if sector_upper in excluded else 0.0
        return (
            exclusion_penalty,
            focus_penalty + current_sector_weight,
            len(ticker),
            ticker,
        )

    shortlisted = [
        {
            "ticker": _normalize_ticker(row.get("ticker")),
            "company_name": row.get("company_name"),
            "sector": row.get("sector"),
            "exchange": row.get("exchange"),
        }
        for row in sorted(all_rows, key=row_priority)
        if _normalize_ticker(row.get("ticker")) not in current
        and (_normalize_sector(row.get("sector")) or "UNKNOWN") not in excluded
    ][:max_candidates]
    return {
        "universe_size": len(all_rows),
        "objective": objective,
        "focus_sectors": sorted(focus_sectors),
        "portfolio_sector_weights": dict(sorted(sector_weights.items())),
        "candidates": shortlisted,
    }


def prioritize_candidate_universe_rows(
    *,
    candidate_rows: list[dict[str, Any]],
    current_tickers: set[str] | list[str],
    portfolio_sector_weights: dict[str, float],
    objective: str,
) -> list[dict[str, Any]]:
    current = {_normalize_ticker(ticker) for ticker in current_tickers if _normalize_ticker(ticker)}
    sector_weights = _normalized_sector_weights(portfolio_sector_weights)

    def sort_key(row: dict[str, Any]) -> tuple[float, float, str]:
        ticker = _normalize_ticker(row.get("ticker"))
        sector_upper = _normalize_sector(row.get("sector")) or "UNKNOWN"
        in_portfolio = 1.0 if ticker in current else 0.0
        sector_weight = sector_weights.get(sector_upper, 0.0)
        if objective == "performance":
            defensive_bonus = 0.0 if sector_upper in {"UTILITIES", "CONSUMER STAPLES", "HEALTHCARE"} else 0.5
            return (in_portfolio, sector_weight + defensive_bonus, ticker)
        return (in_portfolio, sector_weight, ticker)

    return sorted(candidate_rows, key=sort_key)


def focus_candidate_universe_sectors(
    *,
    objective: str,
    portfolio_sector_weights: dict[str, float],
    all_rows: list[dict[str, Any]],
) -> set[str]:
    sector_weights = _normalized_sector_weights(portfolio_sector_weights)
    universe_sectors = sorted(
        {
            sector
            for row in all_rows
            if (sector := _normalize_sector(row.get("sector")))
        }
    )
    if objective == "performance":
        preferred = {"HEALTHCARE", "CONSUMER STAPLES", "UTILITIES"}
        return preferred & set(universe_sectors) or set(universe_sectors[:3])
    sorted_portfolio_sectors = sorted(sector_weights.items(), key=lambda item: item[1])
    underweight = {sector for sector, weight in sorted_portfolio_sectors if weight < 0.1}
    unowned = {sector for sector in universe_sectors if sector not in set(sector_weights)}
    return unowned or underweight or set(universe_sectors[:4])


def _normalize_ticker(value: Any) -> str:
    return str(value or "").upper().strip()


def _normalize_sector(value: Any) -> str:
    return str(value or "").upper().strip()


def _normalized_sector_weights(portfolio_sector_weights: dict[str, float]) -> dict[str, float]:
    return {
        _normalize_sector(sector): float(weight)
        for sector, weight in portfolio_sector_weights.items()
        if _normalize_sector(sector)
    }
