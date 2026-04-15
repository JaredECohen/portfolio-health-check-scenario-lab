from datetime import date

import pytest

from app.config import Settings
from app.models.schemas import AnalysisPlan, Holding, HypotheticalPosition, PortfolioInput, QuestionType


def test_hypothetical_position_requires_exactly_one_sizing_method() -> None:
    with pytest.raises(ValueError):
        HypotheticalPosition(ticker="MSFT")

    with pytest.raises(ValueError):
        HypotheticalPosition(ticker="MSFT", shares=10, target_weight=0.05)

    position = HypotheticalPosition(ticker="MSFT", target_weight=0.05)
    assert position.target_weight == 0.05


def test_portfolio_input_rejects_reversed_date_window() -> None:
    with pytest.raises(ValueError):
        PortfolioInput(
            holdings=[Holding(ticker="AAPL", shares=1)],
            question="How is this portfolio doing?",
            start_date=date(2024, 2, 1),
            end_date=date(2024, 1, 1),
        )


def test_analysis_plan_macro_fields_default_empty() -> None:
    plan = AnalysisPlan(
        question_type=QuestionType.general_health,
        objective="performance",
        explanation="baseline",
        dynamic_workflow="general_health",
    )

    assert plan.macro_themes == []
    assert plan.preferred_data_sources == []
    assert plan.dataset_selection_rationale == []


def test_settings_accepts_legacy_eai_api_key_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EAI_API_KEY", "test-eia-key")

    settings = Settings()

    assert settings.eia_api_key == "test-eia-key"
