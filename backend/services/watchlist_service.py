"""Watchlist business rules and security-master validation."""

from __future__ import annotations

from datetime import UTC, datetime

from backend.monitoring.store import MonitorStore
from backend.schemas.monitor import (
    WatchlistItem,
    WatchlistItemCreate,
    WatchlistItemPatch,
)
from backend.services.finance_service import FinanceService


class WatchlistService:
    def __init__(
        self,
        *,
        store: MonitorStore | None = None,
        finance_service: FinanceService | None = None,
    ) -> None:
        self.store = store or MonitorStore()
        self.finance_service = finance_service or FinanceService()

    def list(self) -> list[WatchlistItem]:
        return self.store.list_watchlist()

    def get(self, symbol: str) -> WatchlistItem | None:
        return self.store.get_watchlist(symbol)

    def create(self, request: WatchlistItemCreate) -> WatchlistItem:
        option = self._resolve_security(request.symbol)
        now = datetime.now(UTC)
        item = WatchlistItem(
            symbol=option.symbol,
            security_id=option.symbol,
            name=option.name,
            exchange=option.exchange,
            enabled=request.enabled,
            thresholds=request.thresholds,
            note=request.note,
            created_at=now,
            updated_at=now,
        )
        return self.store.insert_watchlist(item)

    def update(self, symbol: str, request: WatchlistItemPatch) -> WatchlistItem | None:
        return self.store.update_watchlist(
            symbol,
            enabled=request.enabled,
            thresholds=request.thresholds,
            note=request.note,
            replace_note="note" in request.model_fields_set,
        )

    def delete(self, symbol: str) -> bool:
        return self.store.delete_watchlist(symbol)

    def _resolve_security(self, symbol: str):
        normalized = symbol.strip().upper()
        if not (
            (len(normalized) == 9 and normalized.endswith((".SH", ".SZ", ".BJ")))
            or (len(normalized) == 6 and normalized.isdigit())
        ):
            raise ValueError("Only A-share symbols in 600519.SH or 600519 format are supported")
        options = self.finance_service.list_security_options().items
        candidates = [
            option
            for option in options
            if option.symbol.upper() == normalized
            or (len(normalized) == 6 and option.symbol.upper().startswith(normalized))
        ]
        if len(candidates) != 1:
            raise ValueError(f"{normalized} does not uniquely match the security master")
        return candidates[0]
