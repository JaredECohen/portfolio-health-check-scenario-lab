from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from app.services.factor_registry import CORE_FACTOR_ORDER, FACTOR_BETA_COLUMNS, FACTOR_DISPLAY_NAMES


def available_factor_columns(frame: pd.DataFrame) -> list[str]:
    return [column for column in CORE_FACTOR_ORDER if column in frame.columns and frame[column].notna().any()]


def estimate_factor_profile(
    return_series: pd.Series,
    factor_frame: pd.DataFrame,
    *,
    min_observations: int = 40,
) -> dict[str, Any] | None:
    if return_series.empty or factor_frame.empty:
        return None
    aligned = pd.concat(
        [
            pd.to_numeric(return_series, errors="coerce").rename("asset"),
            factor_frame,
        ],
        axis=1,
        sort=False,
    ).dropna()
    factor_columns = available_factor_columns(aligned)
    if len(aligned) < max(min_observations, len(factor_columns) + 5) or not factor_columns:
        return None
    excess_returns = aligned["asset"] - aligned.get("RF", 0.0)
    design = np.column_stack(
        [
            np.ones(len(aligned)),
            aligned[factor_columns].to_numpy(dtype=float),
        ]
    )
    try:
        coefficients, *_ = np.linalg.lstsq(design, excess_returns.to_numpy(dtype=float), rcond=None)
    except np.linalg.LinAlgError:
        return None
    fitted = design @ coefficients
    residual = excess_returns.to_numpy(dtype=float) - fitted
    total = float(np.sum((excess_returns.to_numpy(dtype=float) - excess_returns.mean()) ** 2))
    r_squared = 1.0 - (float(np.sum(residual**2)) / total) if total else 0.0
    betas = {
        factor: float(coefficients[index + 1])
        for index, factor in enumerate(factor_columns)
    }
    factor_return_sums = aligned[factor_columns].sum(axis=0).to_dict()
    contributions = {
        factor: float(betas[factor] * float(factor_return_sums.get(factor, 0.0)))
        for factor in factor_columns
    }
    primary_factor = max(factor_columns, key=lambda factor: abs(betas.get(factor, 0.0)))
    return {
        "observations": int(len(aligned)),
        "alpha_intercept": float(coefficients[0]),
        "alpha_total": float(coefficients[0] * len(aligned)),
        "r_squared": float(r_squared),
        "betas": betas,
        "contributions": contributions,
        "total_excess_return": float(excess_returns.sum()),
        "primary_factor": primary_factor,
    }


def exposure_columns_from_profile(profile: dict[str, Any] | None) -> dict[str, Any]:
    if profile is None:
        return {}
    betas = profile.get("betas", {})
    primary_factor = str(profile.get("primary_factor") or "")
    row = {
        "factor_alpha_total": round(float(profile.get("alpha_total", 0.0)), 6),
        "factor_r_squared": round(float(profile.get("r_squared", 0.0)), 6),
        "factor_observations": int(profile.get("observations", 0)),
        "factor_primary_exposure": FACTOR_DISPLAY_NAMES.get(primary_factor, primary_factor.replace("_", " ")),
        "factor_growth_tilt_beta": round(float(-betas.get("HML", 0.0)), 6),
    }
    for factor, column in FACTOR_BETA_COLUMNS.items():
        row[column] = round(float(betas.get(factor, 0.0)), 6)
    return row


def attribution_rows_from_profile(profile: dict[str, Any] | None) -> list[dict[str, Any]]:
    if profile is None:
        return []
    betas = profile.get("betas", {})
    contributions = profile.get("contributions", {})
    total_excess = float(profile.get("total_excess_return", 0.0))
    rows = []
    for factor in CORE_FACTOR_ORDER:
        if factor not in betas:
            continue
        attributed = float(contributions.get(factor, 0.0))
        rows.append(
            {
                "factor": factor,
                "factor_label": FACTOR_DISPLAY_NAMES.get(factor, factor),
                "beta": round(float(betas.get(factor, 0.0)), 6),
                "attributed_excess_return": round(attributed, 6),
                "share_of_total_excess_return": (
                    round(attributed / total_excess, 6)
                    if total_excess and np.isfinite(total_excess)
                    else None
                ),
                "signal": factor_signal_label(factor=factor, beta=float(betas.get(factor, 0.0))),
            }
        )
    rows.sort(key=lambda item: abs(float(item["attributed_excess_return"])), reverse=True)
    return rows


def beta_vector_from_mapping(values: dict[str, Any] | pd.Series | dict[str, float]) -> dict[str, float]:
    vector: dict[str, float] = {}
    for factor, column in FACTOR_BETA_COLUMNS.items():
        raw = None
        if isinstance(values, pd.Series):
            raw = values.get(column)
        else:
            raw = values.get(column) if column in values else values.get(factor)
        if raw in (None, ""):
            continue
        numeric = float(raw)
        if np.isfinite(numeric):
            vector[factor] = numeric
    return vector


def factor_similarity_to_profile(values: dict[str, Any] | pd.Series, profile: dict[str, Any] | None) -> float:
    if profile is None:
        return 0.0
    candidate = beta_vector_from_mapping(values)
    baseline = {
        factor: float(value)
        for factor, value in (profile.get("betas", {}) or {}).items()
        if factor in candidate and np.isfinite(float(value))
    }
    shared = sorted(set(candidate) & set(baseline))
    if len(shared) < 2:
        return 0.0
    left = np.array([candidate[factor] for factor in shared], dtype=float)
    right = np.array([baseline[factor] for factor in shared], dtype=float)
    denom = float(np.linalg.norm(left) * np.linalg.norm(right))
    if denom == 0.0:
        return 0.0
    return float(np.dot(left, right) / denom)


def factor_support_score(values: dict[str, Any] | pd.Series, profile: dict[str, Any] | None) -> float:
    if profile is None:
        return 0.0
    candidate = beta_vector_from_mapping(values)
    contributions = {
        factor: float(value)
        for factor, value in (profile.get("contributions", {}) or {}).items()
        if factor in candidate and np.isfinite(float(value))
    }
    if not contributions:
        return 0.0
    scale = sum(abs(value) for value in contributions.values())
    if scale == 0.0:
        return 0.0
    return float(
        sum(candidate[factor] * (contributions[factor] / scale) for factor in contributions)
    )


def top_factor_summary(profile: dict[str, Any] | None) -> tuple[str | None, float | None]:
    rows = attribution_rows_from_profile(profile)
    if not rows:
        return None, None
    top = rows[0]
    return str(top["factor_label"]), float(top["attributed_excess_return"])


def factor_signal_label(*, factor: str, beta: float) -> str:
    if factor == "HML":
        return "value tilt" if beta >= 0 else "growth tilt"
    if factor == "MKT_RF":
        return "higher market sensitivity" if beta >= 0 else "defensive market tilt"
    if factor == "MOM":
        return "positive momentum tilt" if beta >= 0 else "contrarian tilt"
    if factor == "SMB":
        return "small-cap tilt" if beta >= 0 else "large-cap tilt"
    if factor == "RMW":
        return "profitability tilt" if beta >= 0 else "low-profitability tilt"
    if factor == "CMA":
        return "conservative-investment tilt" if beta >= 0 else "aggressive-investment tilt"
    return FACTOR_DISPLAY_NAMES.get(factor, factor)
