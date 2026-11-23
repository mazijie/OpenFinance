"""Market screening and single-security research endpoints."""
from datetime import UTC, date, datetime
import json
from typing import Literal

from fastapi import APIRouter, HTTPException, Query
from backend.services.market_gold_service import MarketGoldService, SORT_FIELDS

router = APIRouter(prefix='/api/market/gold', tags=['market'])


@router.get('/screen')
def screen(trading_date: date | None = None,
           sort_by: str = 'return_1d',
           direction: Literal['asc', 'desc'] = 'desc', limit: int = Query(20, ge=1, le=100),
           min_value: float | None = None, max_value: float | None = None,
           symbol: str | None = Query(None, min_length=1, max_length=16),
           filters: str | None = Query(None, max_length=8000)):
    try:
        if sort_by not in SORT_FIELDS:
            raise ValueError('Invalid market screen parameters')
        return MarketGoldService().screen(as_of=datetime.now(UTC), trading_date=trading_date, sort_by=sort_by,
                                          direction=direction, limit=limit, min_value=min_value, max_value=max_value,
                                          symbol=symbol.strip().upper() if symbol else None,
                                          filters=json.loads(filters) if filters is not None else None)
    except (ValueError, TypeError) as exc:
        raise HTTPException(422, str(exc)) from exc


@router.get('/ranking')
def ranking(period: Literal['day', 'five_days', 'month', 'year'] = 'day',
            limit: int = Query(20, ge=1, le=100),
            side: Literal['positive', 'negative'] = 'positive'):
    return MarketGoldService().ranking(period=period, as_of=datetime.now(UTC), limit=limit, side=side)


@router.get('/overview')
def overview(symbol: str = Query(min_length=1, max_length=16), query: str = Query('近期经营及重要新闻', min_length=1, max_length=300),
             as_of: datetime | None = None, limit: int = Query(5, ge=1, le=20)):
    cutoff = as_of or datetime.now(UTC)
    if cutoff.tzinfo is None:
        raise HTTPException(422, 'as_of must include a timezone')
    return MarketGoldService().overview(symbol=symbol.strip().upper(), query=query, as_of=cutoff, limit=limit)
