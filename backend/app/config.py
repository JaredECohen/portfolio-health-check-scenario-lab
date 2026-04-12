from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


BACKEND_ROOT = Path(__file__).resolve().parents[1]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=BACKEND_ROOT.parent / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    openai_api_key: str | None = Field(default=None, alias="OPENAI_API_KEY")
    alpha_vantage_api_key: str | None = Field(default=None, alias="ALPHA_VANTAGE_API_KEY")
    sec_user_agent: str = Field(
        default="Portfolio Health Check research@example.com",
        alias="SEC_USER_AGENT",
    )
    app_env: str = Field(default="development", alias="PORTFOLIO_APP_ENV")
    api_cors_origins: str = Field(
        default="http://localhost:5173",
        alias="PORTFOLIO_API_CORS_ORIGINS",
    )
    benchmark_symbol: str = Field(default="SPY", alias="PORTFOLIO_BENCHMARK")
    default_lookback_days: int = Field(default=252, alias="PORTFOLIO_LOOKBACK_DAYS")
    risk_free_fallback: float = Field(default=0.02, alias="PORTFOLIO_RISK_FREE_FALLBACK")

    @property
    def data_dir(self) -> Path:
        return BACKEND_ROOT / "data"

    @property
    def artifacts_dir(self) -> Path:
        return BACKEND_ROOT / "artifacts"

    @property
    def ticker_metadata_path(self) -> Path:
        return self.data_dir / "tickers" / "us_equities.json"

    @property
    def candidate_universe_path(self) -> Path:
        return self.data_dir / "candidate_universe.json"

    @property
    def sqlite_path(self) -> Path:
        return self.data_dir / "app.db"

    @property
    def cors_origins(self) -> list[str]:
        return [item.strip() for item in self.api_cors_origins.split(",") if item.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

