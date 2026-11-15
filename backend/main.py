"""FastAPI application entrypoint."""

from __future__ import annotations

from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from backend.api.chat import router as chat_router
from backend.api.internal_jobs import router as internal_jobs_router
from backend.api.market import router as market_router
from backend.api.market_gold import router as market_gold_router
from backend.api.news import router as news_router
from backend.api.annual_reports import router as annual_reports_router
from backend.api.agent import router as agent_router
from backend.api.realtime_quotes import router as realtime_quotes_router
from backend.api.intraday_operations import router as intraday_operations_router
from backend.config import settings
from backend.lakehouse.catalog import close_catalog
from backend.lakehouse.starrocks_daily_reader import StarRocksUnavailable
from backend.logging_utils import configure_logging


configure_logging(settings.LOG_LEVEL)
STATIC_DIR = Path(__file__).resolve().parent / "static"

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        yield
    finally:
        close_catalog()


app = FastAPI(
    lifespan=lifespan,
    title="OpenFinance-Agent",
    description="Financial LLM Agent API powered by FastAPI and LangGraph",
    version="0.2.0",
)

@app.exception_handler(StarRocksUnavailable)
async def starrocks_unavailable_handler(request, exc):
    return JSONResponse(status_code=503, content={"detail": str(exc)})


app.include_router(chat_router)
app.include_router(internal_jobs_router)
app.include_router(market_router)
app.include_router(market_gold_router)
app.include_router(news_router)
app.include_router(annual_reports_router)
app.include_router(agent_router)
app.include_router(realtime_quotes_router)
app.include_router(intraday_operations_router)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/ui", tags=["frontend"])
def frontend() -> FileResponse:
    """Serve the browser UI."""

    return FileResponse(STATIC_DIR / "index.html")


@app.get("/ops", tags=["frontend"])
def operations_frontend() -> FileResponse:
    return FileResponse(STATIC_DIR / "operations.html")


@app.get("/", tags=["health"])
def health_check() -> dict[str, str]:
    """Simple health check endpoint."""

    return {"status": "ok", "message": "OpenFinance-Agent is running"}
