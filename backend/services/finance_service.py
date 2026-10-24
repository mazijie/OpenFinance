"""Business service for finance lakehouse lookup."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from backend.config import settings
from backend.repositories.finance_repository import FinanceRepository
from backend.schemas.finance import MarketQueryParams, MarketSearchResult, SecurityOptionsResult


class FinanceService:
    """Application service used by market tools and agents."""

    def __init__(self, repository: FinanceRepository | None = None) -> None:
        self.repository = repository or FinanceRepository()

    def search_market(
        self,
        *,
        symbol: str | None = None,
        security_id: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        adjustment_type: str | None = None,
        as_of: datetime | None = None,
        limit: int = 500,
    ) -> MarketSearchResult:
        resolved_end = end_date or datetime.now(UTC).date()
        resolved_start = start_date or (resolved_end - timedelta(days=settings.FINANCE_DEFAULT_LOOKBACK_DAYS))
        params = MarketQueryParams(
            symbol=symbol,
            security_id=security_id,
            start_date=resolved_start,
            end_date=resolved_end,
            adjustment_type=adjustment_type or settings.FINANCE_DEFAULT_ADJUSTMENT_TYPE,
            as_of=as_of or datetime.now(UTC),
            limit=limit,
        )
        items = self.repository.search_market(params)
        if not items:
            selector = params.security_id or params.symbol or "requested security"
            return MarketSearchResult(items=[], message=f"No internal market data found for {selector}.")
        return MarketSearchResult(items=items, message=None)

    def list_security_options(self) -> SecurityOptionsResult:
        return SecurityOptionsResult(items=self.repository.list_security_options())

    def is_trading_day(self, trading_date: date) -> bool | None:
        return self.repository.is_trading_day(trading_date)
