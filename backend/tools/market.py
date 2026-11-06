"""Lakehouse-backed market data tool implementations."""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

from backend.services.finance_service import FinanceService
from backend.tools.base import BaseTool, ToolResult, build_error_result, build_success_result

logger = logging.getLogger(__name__)


class MarketDataError(Exception):
    """Raised when market data cannot be fetched or parsed."""


class InvalidSymbolError(MarketDataError):
    """Raised when the symbol is invalid."""


class MarketLakehouseTool(BaseTool):
    """Tool wrapper for querying internal lakehouse market data."""

    name = "market_lakehouse_tool"
    description = "Query internal lakehouse daily bars and interval summaries for a symbol or security_id."

    def __init__(self, service: FinanceService | None = None) -> None:
        self.service = service or FinanceService()

    def run(self, **kwargs: Any) -> ToolResult:
        symbol = kwargs.get("symbol")
        security_id = kwargs.get("security_id")
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        adjustment_type = str(kwargs.get("adjustment_type") or "none").strip().lower()
        if adjustment_type != "none":
            return build_error_result("Only unadjusted daily bars are supported")
        as_of = kwargs.get("as_of")

        try:
            normalized_symbol = validate_symbol(symbol) if symbol else None
            normalized_security_id = validate_security_id(security_id) if security_id else None
            resolved_start = _coerce_date(start_date) if start_date is not None else None
            resolved_end = _coerce_date(end_date) if end_date is not None else None
            resolved_as_of = _coerce_datetime(as_of) if as_of is not None else None
        except (InvalidSymbolError, ValueError) as exc:
            return build_error_result(str(exc))

        if not normalized_symbol and not normalized_security_id:
            return build_error_result("symbol or security_id is required")

        result = self.service.search_market(
            symbol=normalized_symbol,
            security_id=normalized_security_id,
            start_date=resolved_start,
            end_date=resolved_end,
            adjustment_type=adjustment_type,
            as_of=resolved_as_of,
        )
        if not result.items:
            return build_error_result(result.message or "No internal market data found")

        item = result.items[0]
        payload = {
            "symbol": item.symbol,
            "security_id": item.security_id,
            "exchange": item.exchange,
            "start_date": item.start_date.isoformat(),
            "end_date": item.end_date.isoformat(),
            "adjustment_type": item.adjustment_type,
            "first_close": item.first_close,
            "last_close": item.last_close,
            "interval_return": item.interval_return,
            "cumulative_return": item.cumulative_return,
            "annualized_volatility": item.annualized_volatility,
            "recent_trading_date": item.recent_trading_date.isoformat() if item.recent_trading_date else None,
            "suspended_days": item.suspended_days,
            "data_gap_note": item.data_gap_note,
            "adjustment_note": item.adjustment_note,
            "bars": item.bars,
            "source": "internal_lakehouse",
            "message": result.message,
        }
        logger.info(
            "Tool result: market_lakehouse_tool, symbol=%s, security_id=%s, bars=%d",
            normalized_symbol,
            normalized_security_id,
            len(item.bars),
        )
        return build_success_result(payload)


class MarketDataTool(MarketLakehouseTool):
    """Backward-compatible alias for the old market tool name."""

    name = "market_data_tool"
    description = "Query internal lakehouse daily bars and interval summaries for a symbol or security_id."


def validate_symbol(symbol: str) -> str:
    """Validate and normalize symbol input."""

    if not isinstance(symbol, str):
        raise InvalidSymbolError("Stock symbol must be a string")

    normalized_symbol = symbol.strip().upper()
    if not normalized_symbol:
        raise InvalidSymbolError("Stock symbol cannot be empty")
    if len(normalized_symbol) > 16:
        raise InvalidSymbolError("Stock symbol is too long")

    allowed_chars = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-")
    if any(char not in allowed_chars for char in normalized_symbol):
        raise InvalidSymbolError("Stock symbol contains invalid characters")
    return normalized_symbol


def validate_security_id(security_id: str) -> str:
    """Validate and normalize security identifier."""

    normalized = validate_symbol(security_id)
    if len(normalized) < 4:
        raise InvalidSymbolError("security_id is too short")
    return normalized


def get_market_data(
    symbol: str | None = None,
    *,
    security_id: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    adjustment_type: str = "none",
    as_of: datetime | None = None,
    service: FinanceService | None = None,
) -> ToolResult:
    """Convenient function interface used by the agent layer."""

    tool = MarketDataTool(service=service)
    return tool.run(
        symbol=symbol,
        security_id=security_id,
        start_date=start_date,
        end_date=end_date,
        adjustment_type=adjustment_type,
        as_of=as_of,
    )


def _coerce_date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value))


def _coerce_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
