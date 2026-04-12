from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.config import get_settings
from app.routes.api import router as api_router, get_database


settings = get_settings()
get_database().initialize()
settings.artifacts_dir.mkdir(parents=True, exist_ok=True)

app = FastAPI(
    title="Portfolio Health Check + Research Overlay + Scenario Lab",
    version="0.1.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(api_router)
app.mount("/artifacts", StaticFiles(directory=settings.artifacts_dir), name="artifacts")

