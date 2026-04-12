from __future__ import annotations

from functools import lru_cache

from fastapi import APIRouter, Depends, HTTPException, Query

from app.agents.runtime import AgentRuntime
from app.config import Settings, get_settings
from app.database import Database
from app.models.schemas import AnalysisResponse, PortfolioInput, TickerMetadata
from app.services.analytics import AnalyticsService
from app.services.alpha_vantage import AlphaVantageService
from app.services.artifacts import ArtifactService
from app.services.cache import CacheService
from app.services.dynamic_eda import DynamicEDAService
from app.services.market_data import MarketDataService
from app.services.orchestration import PortfolioAnalysisOrchestrator
from app.services.portfolio_intake import PortfolioIntakeError, PortfolioIntakeService
from app.services.scenario import ScenarioService
from app.services.sec_edgar import SecEdgarService
from app.services.ticker_metadata import TickerMetadataService


router = APIRouter(prefix="/api")


@lru_cache(maxsize=1)
def get_database() -> Database:
    settings = get_settings()
    database = Database(settings.sqlite_path)
    database.initialize()
    return database


@lru_cache(maxsize=1)
def get_ticker_metadata_service() -> TickerMetadataService:
    return TickerMetadataService(get_settings().ticker_metadata_path)


def get_cache_service() -> CacheService:
    return CacheService(get_database())


def get_alpha_vantage_service() -> AlphaVantageService:
    settings = get_settings()
    return AlphaVantageService(settings.alpha_vantage_api_key, get_cache_service())


def get_orchestrator(settings: Settings = Depends(get_settings)) -> PortfolioAnalysisOrchestrator:
    analytics_service = AnalyticsService()
    alpha_vantage = get_alpha_vantage_service()
    ticker_metadata = get_ticker_metadata_service()
    return PortfolioAnalysisOrchestrator(
        intake_service=PortfolioIntakeService(ticker_metadata, alpha_vantage),
        market_data_service=MarketDataService(alpha_vantage),
        analytics_service=analytics_service,
        dynamic_eda_service=DynamicEDAService(alpha_vantage),
        scenario_service=ScenarioService(
            analytics_service=analytics_service,
            alpha_vantage=alpha_vantage,
            ticker_metadata=ticker_metadata,
            candidate_universe_path=settings.candidate_universe_path,
        ),
        sec_edgar_service=SecEdgarService(settings.sec_user_agent, get_cache_service()),
        artifact_service=ArtifactService(get_database(), settings.artifacts_dir),
        agent_runtime=AgentRuntime(),
        risk_free_fallback=settings.risk_free_fallback,
    )


@router.get("/health")
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/tickers", response_model=list[TickerMetadata])
async def search_tickers(
    q: str | None = Query(default=None),
    limit: int = Query(default=25, ge=1, le=100),
) -> list[TickerMetadata]:
    service = get_ticker_metadata_service()
    return service.search(q, limit)


@router.get("/tickers/{ticker}", response_model=TickerMetadata)
async def get_ticker(ticker: str) -> TickerMetadata:
    service = get_ticker_metadata_service()
    result = service.get(ticker)
    if result is None:
        raise HTTPException(status_code=404, detail="Ticker not found")
    return result


@router.post("/analyze", response_model=AnalysisResponse)
async def analyze_portfolio(
    payload: PortfolioInput,
    orchestrator: PortfolioAnalysisOrchestrator = Depends(get_orchestrator),
) -> AnalysisResponse:
    try:
        return await orchestrator.analyze(payload)
    except PortfolioIntakeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

