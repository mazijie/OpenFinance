"""Read the latest streaming snapshot and most recent completed sample window."""
import psycopg
from fastapi import APIRouter, HTTPException, Query
from backend.services.realtime_quote_service import RealtimeQuoteService

router = APIRouter(prefix="/api/realtime-quotes", tags=["market"])


@router.get("/intraday")
def intraday(symbol: str = Query(min_length=1, max_length=16)):
    try:
        return RealtimeQuoteService().intraday(symbol)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="行情服务暂不可用") from exc


@router.get("")
def quotes(symbol: str | None = Query(default=None, max_length=16)):
    try:
        return RealtimeQuoteService().lookup(symbol)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="行情服务暂不可用") from exc
