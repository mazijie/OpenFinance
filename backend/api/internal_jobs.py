"""Combined internal job routes for orchestrators."""

from fastapi import APIRouter

from backend.api import (
    internal_finance_jobs,
    internal_maintenance_jobs,
    internal_market_jobs,
    internal_news_jobs,
    internal_report_jobs,
)

router = APIRouter()
for module in (internal_market_jobs, internal_news_jobs, internal_finance_jobs, internal_report_jobs, internal_maintenance_jobs):
    router.include_router(module.router)
