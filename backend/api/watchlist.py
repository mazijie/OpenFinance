"""Watchlist CRUD API."""

from __future__ import annotations

from functools import lru_cache

from fastapi import APIRouter, Depends, HTTPException, status

from backend.schemas.monitor import WatchlistItem, WatchlistItemCreate, WatchlistItemPatch
from backend.services.watchlist_service import WatchlistService

router = APIRouter(prefix="/api/watchlist", tags=["watchlist"])


@lru_cache(maxsize=1)
def get_watchlist_service() -> WatchlistService:
    return WatchlistService()


@router.get("", response_model=list[WatchlistItem])
def list_watchlist(service: WatchlistService = Depends(get_watchlist_service)) -> list[WatchlistItem]:
    return service.list()


@router.post("", response_model=WatchlistItem, status_code=status.HTTP_201_CREATED)
def create_watchlist_item(
    request: WatchlistItemCreate,
    service: WatchlistService = Depends(get_watchlist_service),
) -> WatchlistItem:
    try:
        return service.create(request)
    except ValueError as exc:
        status_code = 409 if "already in the watchlist" in str(exc) else 422
        raise HTTPException(status_code=status_code, detail=str(exc)) from exc


@router.patch("/{symbol}", response_model=WatchlistItem)
def update_watchlist_item(
    symbol: str,
    request: WatchlistItemPatch,
    service: WatchlistService = Depends(get_watchlist_service),
) -> WatchlistItem:
    item = service.update(symbol, request)
    if item is None:
        raise HTTPException(status_code=404, detail="Watchlist item not found")
    return item


@router.delete("/{symbol}", status_code=status.HTTP_204_NO_CONTENT)
def delete_watchlist_item(
    symbol: str,
    service: WatchlistService = Depends(get_watchlist_service),
) -> None:
    if not service.delete(symbol):
        raise HTTPException(status_code=404, detail="Watchlist item not found")
