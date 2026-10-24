"""Repository layer for querying finance lakehouse data."""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, date, datetime
from statistics import stdev
import threading
import time

from backend.config import settings
from backend.lakehouse.finance_writer import FinanceWriter
from backend.schemas.finance import DailyBarRecord, MarketQueryParams, MarketSummary, SecurityOption


class FinanceRepository:
    """Read-only repository over finance Silver and Gold data."""

    def __init__(self, writer: FinanceWriter | None = None) -> None:
        self.writer = writer or FinanceWriter()
        self._security_options_cache: list[SecurityOption] | None = None
        self._security_options_expires_at = 0.0
        self._security_options_lock = threading.Lock()

    def search_market(self, params: MarketQueryParams) -> list[MarketSummary]:
        """Return summarized market data for one symbol or security."""

        visible_bars = self._resolve_visible_bars(params)
        grouped: dict[tuple[str, str, str], list[DailyBarRecord]] = defaultdict(list)
        for bar in visible_bars:
            grouped[(bar.security_id, bar.symbol, bar.exchange)].append(bar)

        summaries = [self._summarize_bars(records, params.adjustment_type) for records in grouped.values() if records]
        summaries.sort(key=lambda item: (item.recent_trading_date or item.end_date, item.symbol), reverse=True)
        return summaries[: params.limit]

    def list_security_options(self) -> list[SecurityOption]:
        """Return the distinct securities represented in the local lakehouse."""

        now = time.monotonic()
        with self._security_options_lock:
            if self._security_options_cache is not None and now < self._security_options_expires_at:
                return list(self._security_options_cache)

            scan = getattr(self.writer, "scan_security_options", None)
            if callable(scan):
                options = [SecurityOption.model_validate(item) for item in scan()]
            else:  # pragma: no cover - compatibility for lightweight test doubles
                options = [
                    SecurityOption(
                        symbol=record.symbol.upper(),
                        name=record.name,
                        exchange=record.exchange.upper(),
                    )
                    for record in self.writer.scan_security_master()
                ]
            options.sort(key=lambda item: item.symbol)
            self._security_options_cache = options
            self._security_options_expires_at = now + 300
            return list(options)

    def is_trading_day(self, trading_date: date) -> bool | None:
        """Return whether the local exchange calendar marks the date open."""

        scan = getattr(self.writer, "scan_trading_calendar_for_date", None)
        if callable(scan):
            rows = scan(trading_date)
        else:  # pragma: no cover - compatibility for test doubles
            rows = [
                row
                for row in self.writer.scan_trading_calendar()
                if row.trading_date == trading_date
            ]
        if not rows:
            return None
        return any(
            row.is_open
            for row in rows
            if row.exchange.upper() in {"SH", "SZ", "BJ"}
        )

    def _resolve_visible_bars(self, params: MarketQueryParams) -> list[DailyBarRecord]:
        as_of = params.as_of or datetime.now(UTC)
        scan = getattr(self.writer, "scan_daily_bars_for_query", None)
        if callable(scan):
            selected = scan(
                symbol=params.symbol,
                security_id=params.security_id,
                start_date=params.start_date,
                end_date=params.end_date,
                adjustment_type=params.adjustment_type,
                as_of=as_of,
                limit=params.limit,
            )
        else:  # pragma: no cover - compatibility for lightweight test doubles
            selected = [
                bar
                for bar in self.writer.scan_daily_bars()
                if self._matches_selector(bar, params)
                and params.start_date <= bar.trading_date <= params.end_date
                and bar.adjustment_type == params.adjustment_type
                and bar.available_at <= as_of
                and bar.ingested_at <= as_of
            ]

        latest_by_key: dict[tuple[str, object, str], DailyBarRecord] = {}
        for bar in selected:
            key = (bar.security_id, bar.trading_date, bar.provider)
            current = latest_by_key.get(key)
            if current is None or (bar.available_at, bar.ingested_at, bar.source_version or "", bar.payload_hash or "") > (
                current.available_at,
                current.ingested_at,
                current.source_version or "",
                current.payload_hash or "",
            ):
                latest_by_key[key] = bar
        return sorted(latest_by_key.values(), key=lambda item: (item.security_id, item.trading_date))

    def _matches_selector(self, bar: DailyBarRecord, params: MarketQueryParams) -> bool:
        if params.security_id and bar.security_id != params.security_id:
            return False
        if params.symbol and bar.symbol.upper() != params.symbol.upper():
            return False
        return True

    def _summarize_bars(self, bars: list[DailyBarRecord], adjustment_type: str) -> MarketSummary:
        ordered = sorted(bars, key=lambda item: item.trading_date)
        first = ordered[0]
        last = ordered[-1]
        returns = []
        for previous, current in zip(ordered, ordered[1:]):
            if previous.close:
                returns.append((current.close / previous.close) - 1.0)

        interval_return = None
        cumulative_return = None
        if first.close:
            interval_return = (last.close / first.close) - 1.0
            cumulative_return = interval_return

        annualized_volatility = None
        if len(returns) >= 2:
            annualized_volatility = stdev(returns) * (252**0.5)

        bars_payload = [
            {
                "trading_date": bar.trading_date.isoformat(),
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
                "amount": bar.amount,
                "turnover": bar.turnover,
                "is_suspended": bar.is_suspended,
                "is_st": bar.is_st,
                "adjustment_factor": bar.adjustment_factor,
                "provider": bar.provider,
                "source_record_id": bar.source_record_id,
                "source_version": bar.source_version,
                "payload_hash": bar.payload_hash,
                "available_at": bar.available_at.isoformat(),
                "ingested_at": bar.ingested_at.isoformat(),
                "raw_object_path": bar.raw_object_path,
            }
            for bar in ordered
        ]

        data_gap_note = None
        suspended_days = sum(1 for bar in ordered if bar.is_suspended)
        if suspended_days:
            data_gap_note = f"{suspended_days} suspended days were observed in the selected range."
        elif len(ordered) <= 1:
            data_gap_note = "Too few bars to assess data gaps."

        return MarketSummary(
            symbol=first.symbol,
            security_id=first.security_id,
            exchange=first.exchange,
            start_date=first.trading_date,
            end_date=last.trading_date,
            adjustment_type=adjustment_type,
            first_close=first.close,
            last_close=last.close,
            interval_return=interval_return,
            cumulative_return=cumulative_return,
            annualized_volatility=annualized_volatility,
            recent_trading_date=last.trading_date,
            suspended_days=suspended_days,
            data_gap_note=data_gap_note,
            adjustment_note=self._adjustment_note(adjustment_type),
            bars=bars_payload,
        )

    def _adjustment_note(self, adjustment_type: str) -> str:
        if adjustment_type in {"qfq", "forward"}:
            return "Prices are interpreted as forward-adjusted and kept with explicit adjustment factors."
        if adjustment_type in {"hfq", "backward"}:
            return "Prices are interpreted as backward-adjusted and kept with explicit adjustment factors."
        return "Prices are stored without adjustment and must be interpreted together with adjustment_factor."
