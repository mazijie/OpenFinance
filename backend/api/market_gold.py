"""Market screening and single-security research endpoints."""
from datetime import UTC, date, datetime
from typing import Literal

from fastapi import APIRouter, HTTPException, Query
from backend.services.market_gold_service import MarketGoldService

router = APIRouter(prefix='/api/market/gold', tags=['market'])


@router.get('/screen')
def screen(trading_date: date | None = None,
           sort_by: Literal['return_1d', 'return_5d', 'return_20d', 'amount', 'avg_amount_1d', 'avg_amount_5d', 'avg_amount_20d', 'ma_5d', 'ma_20d', 'price_ma_5d_deviation', 'price_ma_20d_deviation', 'volatility_20d_annualized', 'avg_volume_5d', 'volume_ratio_5d'] = 'return_1d',
           direction: Literal['asc', 'desc'] = 'desc', limit: int = Query(20, ge=1, le=100),
           min_value: float | None = None, max_value: float | None = None):
    try:
        return MarketGoldService().screen(as_of=datetime.now(UTC), trading_date=trading_date, sort_by=sort_by,
                                          direction=direction, limit=limit, min_value=min_value, max_value=max_value)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@router.get('/overview')
def overview(symbol: str = Query(min_length=1, max_length=16), query: str = Query('近期经营及重要新闻', min_length=1, max_length=300),
             as_of: datetime | None = None, limit: int = Query(5, ge=1, le=20)):
    cutoff = as_of or datetime.now(UTC)
    if cutoff.tzinfo is None:
        raise HTTPException(422, 'as_of must include a timezone')
    return MarketGoldService().overview(symbol=symbol.strip().upper(), query=query, as_of=cutoff, limit=limit)
