"""Silver writer for finance records."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from datetime import date
import time
from typing import Any

from backend.lakehouse.finance_tables import (
    DAILY_BARS_ARROW_SCHEMA,
    SECURITY_MASTER_ARROW_SCHEMA,
    TRADING_CALENDAR_ARROW_SCHEMA,
    ensure_daily_bars_table,
    ensure_security_master_table,
    ensure_trading_calendar_table,
)
from backend.logging_utils import get_logger
from backend.lakehouse.write_coordination import coordinated_write_with_retry, upsert_with_retry
from backend.schemas.finance import DailyBarRecord, SecurityMasterRecord, TradingCalendarRecord

logger = get_logger(__name__)
TRADING_CALENDAR_UPSERT_BATCH_SIZE = 256


class _SimpleColumn:
    def __init__(self, values: list[object]) -> None:
        self._values = values

    def to_pylist(self) -> list[object]:
        return list(self._values)


class _SimpleArrowTable:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    @classmethod
    def from_pylist(cls, rows: list[dict], schema=None):  # noqa: ANN001
        return cls(list(rows))

    @property
    def num_rows(self) -> int:
        return len(self._rows)

    def to_pylist(self) -> list[dict]:
        return list(self._rows)

    def column(self, name: str) -> _SimpleColumn:
        return _SimpleColumn([row.get(name) for row in self._rows])


class FinanceWriter:
    """Write and read finance records from Iceberg."""

    def __init__(self) -> None:
        self.security_master_table = ensure_security_master_table()
        self.trading_calendar_table = ensure_trading_calendar_table()
        self.daily_bars_table = ensure_daily_bars_table()

    def append_security_master(self, records: list[SecurityMasterRecord]) -> int:
        if not records:
            return 0
        arrow_module = self._get_arrow_module()
        arrow_table = arrow_module.Table.from_pylist([record.model_dump() for record in records], schema=SECURITY_MASTER_ARROW_SCHEMA)
        result = upsert_with_retry(
            lambda: self._reload_security_master(),
            arrow_table,
            table_name="security_master",
            join_cols=["provider", "security_id"],
        )
        return result.rows_inserted + result.rows_updated

    def append_trading_calendar(self, records: list[TradingCalendarRecord]) -> int:
        if not records:
            return 0
        started = time.monotonic()
        written = [0]
        result = coordinated_write_with_retry(
            lambda: self._append_trading_calendar_locked(records, written), table_name="trading_calendar"
        )
        logger.info("Trading calendar write completed", event="trading_calendar_write_completed",
                    extra_fields={**result, "duration_seconds": round(time.monotonic() - started, 3)})
        return result["written_count"]

    def _append_trading_calendar_locked(self, records: list[TradingCalendarRecord], written: list[int]) -> dict[str, int]:
        fields = ("exchange", "trading_date", "is_open", "session_open_at", "session_close_at")
        rows = [record.model_dump() for record in records]
        keys = [(row["exchange"], row["trading_date"]) for row in rows]
        if len(set(keys)) != len(keys):
            raise ValueError("Duplicate trading calendar keys in input")
        table = self._reload_trading_calendar()
        existing = {
            (row["exchange"], row["trading_date"]): tuple(row[name] for name in fields)
            for row in table.scan().select(*fields).to_arrow().to_pylist()
        }
        changed = []
        new_count = changed_count = unchanged_count = 0
        for row, key in zip(rows, keys, strict=True):
            previous = existing.get(key)
            if previous is None:
                new_count += 1
                changed.append(row)
            elif previous != tuple(row[name] for name in fields):
                changed_count += 1
                changed.append(row)
            else:
                unchanged_count += 1
        arrow_module = self._get_arrow_module()
        for start in range(0, len(changed), TRADING_CALENDAR_UPSERT_BATCH_SIZE):
            batch = changed[start : start + TRADING_CALENDAR_UPSERT_BATCH_SIZE]
            arrow_table = arrow_module.Table.from_pylist(
                batch, schema=TRADING_CALENDAR_ARROW_SCHEMA
            )
            result = self._reload_trading_calendar().upsert(
                arrow_table,
                join_cols=["exchange", "trading_date"],
                snapshot_properties={"openfinance.write-mode": "upsert"},
            )
            written[0] += result.rows_inserted + result.rows_updated
        return {"input_count": len(rows), "new_count": new_count, "changed_count": changed_count,
                "unchanged_count": unchanged_count, "written_count": written[0]}

    def append_daily_bars(self, records: list[DailyBarRecord]) -> int:
        if not records:
            return 0

        return coordinated_write_with_retry(
            lambda: self._append_daily_bars_locked(records), table_name="daily_bars"
        )

    def _append_daily_bars_locked(self, records: list[DailyBarRecord]) -> int:
        keys = {
            (
                record.provider,
                record.adjustment_type,
                record.security_id,
                record.trading_date,
            )
            for record in records
        }
        existing_versions = self._existing_daily_versions(keys)
        new_records: list[DailyBarRecord] = []
        changed_records: list[DailyBarRecord] = []
        unchanged_count = 0
        for record in records:
            key = (
                record.provider,
                record.adjustment_type,
                record.security_id,
                record.trading_date,
            )
            existing_version = existing_versions.get(key)
            if existing_version is None:
                new_records.append(record)
            elif existing_version == self._daily_record_version(record):
                unchanged_count += 1
            else:
                changed_records.append(record)

        inserted_count = self._append_new_daily_bars(new_records)
        updated_count = self._update_changed_daily_bars(changed_records)
        written_count = inserted_count + updated_count
        logger.info(
            "Silver 行情写入完成",
            event="silver_market_written",
            extra_fields={
                "offered_count": len(records),
                "new_count": len(new_records),
                "changed_count": len(changed_records),
                "unchanged_count": unchanged_count,
                "inserted_count": inserted_count,
                "updated_count": updated_count,
                "written_count": written_count,
            },
        )
        return written_count

    def _append_new_daily_bars(self, records: list[DailyBarRecord]) -> int:
        if not records:
            return 0
        arrow_module = self._get_arrow_module()
        arrow_table = arrow_module.Table.from_pylist(
            [record.model_dump() for record in records],
            schema=DAILY_BARS_ARROW_SCHEMA,
        )
        self._reload_daily_bars().append(arrow_table)
        return len(records)

    def _update_changed_daily_bars(self, records: list[DailyBarRecord]) -> int:
        if not records:
            return 0
        arrow_module = self._get_arrow_module()
        arrow_table = arrow_module.Table.from_pylist(
            [record.model_dump() for record in records],
            schema=DAILY_BARS_ARROW_SCHEMA,
        )
        result = self._reload_daily_bars().upsert(
            arrow_table,
            join_cols=["provider", "adjustment_type", "security_id", "trading_date"],
            snapshot_properties={"openfinance.write-mode": "upsert"},
        )
        return result.rows_inserted + result.rows_updated

    def _existing_daily_versions(
        self,
        keys: set[tuple[str, str, str, date]],
    ) -> dict[tuple[str, str, str, date], tuple[Any, Any, str, str]]:
        if not keys:
            return {}
        providers = sorted({provider for provider, _, _, _ in keys})
        adjustment_types = sorted({adjustment_type for _, adjustment_type, _, _ in keys})
        security_ids = sorted({security_id for _, _, security_id, _ in keys})
        trading_dates = sorted({trading_date for _, _, _, trading_date in keys})

        logger.info(
            "Scanning existing daily versions",
            extra_fields={"key_count": len(keys), "security_count": len(security_ids),
                          "date_count": len(trading_dates)},
        )
        try:
            from pyiceberg.expressions import And, In

            table = (
                self._reload_daily_bars()
                .scan(
                    row_filter=And(
                        In("provider", providers),
                        In("adjustment_type", adjustment_types),
                        In("security_id", security_ids),
                        In("trading_date", trading_dates),
                    )
                )
                .select(
                    "provider",
                    "adjustment_type",
                    "security_id",
                    "trading_date",
                    "available_at",
                    "ingested_at",
                    "source_version",
                    "payload_hash",
                )
                .to_arrow()
            )
        except ImportError:  # pragma: no cover - test fallback without pyiceberg
            table = self._scan_arrow_table(self._reload_daily_bars())

        logger.info("Existing daily versions scan completed", extra_fields={"row_count": table.num_rows})
        existing: dict[tuple[str, str, str, date], tuple[Any, Any, str, str]] = {}
        for row in table.to_pylist():
            key = (
                row["provider"],
                row["adjustment_type"],
                row["security_id"],
                row["trading_date"],
            )
            if key in keys:
                existing[key] = (
                    row.get("available_at"),
                    row.get("ingested_at"),
                    str(row.get("source_version") or ""),
                    str(row.get("payload_hash") or ""),
                )
        return existing

    @staticmethod
    def _daily_record_version(record: DailyBarRecord) -> tuple[Any, Any, str, str]:
        return (
            record.available_at,
            record.ingested_at,
            record.source_version or "",
            record.payload_hash or "",
        )

    def scan_security_master(self) -> list[SecurityMasterRecord]:
        return [SecurityMasterRecord.model_validate(row) for row in self._scan_arrow_table(self.security_master_table).to_pylist()]

    def scan_security_options(self) -> list[dict[str, str]]:
        """Read only the fields used by the security selector."""

        rows = (
            self.security_master_table.scan()
            .select("symbol", "name", "exchange", "ingested_at")
            .to_arrow()
            .to_pylist()
        )
        selected = {}
        for row in rows:
            exchange = str(row["exchange"]).upper()
            symbol = str(row["symbol"]).upper().split(".")[0]
            key = (symbol, exchange)
            rank = (str(row.get("ingested_at") or ""), str(row.get("name") or ""))
            if key not in selected or rank > selected[key][0]:
                selected[key] = (rank, {"symbol": symbol, "name": str(row["name"]), "exchange": exchange})
        return [item for _, item in selected.values()]

    def scan_trading_calendar(self) -> list[TradingCalendarRecord]:
        return [TradingCalendarRecord.model_validate(row) for row in self._scan_arrow_table(self.trading_calendar_table).to_pylist()]

    def scan_trading_calendar_for_date(self, trading_date: date) -> list[TradingCalendarRecord]:
        try:
            from pyiceberg.expressions import EqualTo

            rows = (
                self.trading_calendar_table.scan(row_filter=EqualTo("trading_date", trading_date))
                .to_arrow()
                .to_pylist()
            )
        except ImportError:  # pragma: no cover - local fallback
            rows = self.trading_calendar_table.scan().to_arrow().to_pylist()
        return [
            TradingCalendarRecord.model_validate(row)
            for row in rows
            if row.get("trading_date") == trading_date
        ]

    def scan_daily_bars(self) -> list[DailyBarRecord]:
        return [DailyBarRecord.model_validate(row) for row in self._scan_arrow_table(self.daily_bars_table).to_pylist()]

    def has_daily_bars_before(self, *, symbol: str, adjustment_type: str, before: date, as_of: datetime, table=None, snapshot_id=None) -> bool:
        """Check older history without materializing all earlier bars."""
        from pyiceberg.expressions import And, EqualTo, LessThan, LessThanOrEqual
        predicate = And(EqualTo("symbol", symbol), EqualTo("adjustment_type", adjustment_type),
                        LessThan("trading_date", before), LessThanOrEqual("available_at", as_of),
                        LessThanOrEqual("ingested_at", as_of))
        options = {"snapshot_id": snapshot_id} if snapshot_id is not None else {}
        return (table or self.daily_bars_table).scan(row_filter=predicate, selected_fields=("trading_date",), limit=1, **options).to_arrow().num_rows > 0

    def scan_daily_bars_for_query(
        self,
        *,
        symbol: str | None,
        security_id: str | None,
        start_date: date,
        end_date: date,
        adjustment_type: str,
        as_of: datetime,
        limit: int,
        table=None,
        snapshot_id=None,
    ) -> list[DailyBarRecord]:
        """Read one security's visible rows using Iceberg predicate pushdown."""

        try:
            from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThanOrEqual

            predicates = [
                EqualTo("adjustment_type", adjustment_type),
                GreaterThanOrEqual("trading_date", start_date),
                LessThanOrEqual("trading_date", end_date),
                LessThanOrEqual("available_at", as_of),
                LessThanOrEqual("ingested_at", as_of),
            ]
            if security_id:
                predicates.append(EqualTo("security_id", security_id))
            elif symbol:
                predicates.append(EqualTo("symbol", symbol))
            else:
                raise ValueError("symbol or security_id is required")

            rows = (
                (table or self.daily_bars_table).scan(
                    row_filter=And(*predicates),
                    **({"snapshot_id": snapshot_id} if snapshot_id is not None else {}),
                )
                .select(
                    "security_id",
                    "symbol",
                    "exchange",
                    "trading_date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "amount",
                    "turnover",
                    "is_suspended",
                    "is_st",
                    "adjustment_factor",
                    "adjustment_type",
                    "provider",
                    "source_record_id",
                    "source_version",
                    "payload_hash",
                    "raw_object_path",
                    "event_time",
                    "available_at",
                    "ingested_at",
                    "schema_version",
                )
                .to_arrow()
                .to_pylist()
            )
            return [DailyBarRecord.model_validate(row) for row in rows]
        except ImportError:  # pragma: no cover - test fallback without pyiceberg
            return [
                bar
                for bar in self.scan_daily_bars()
                if (not security_id or bar.security_id == security_id)
                and (not symbol or bar.symbol.upper() == symbol.upper())
                and start_date <= bar.trading_date <= end_date
                and bar.adjustment_type == adjustment_type
                and bar.available_at <= as_of
                and bar.ingested_at <= as_of
            ][:limit]

    def existing_daily_keys(self, keys: Iterable[tuple[str, str, str, object]]) -> set[tuple[str, str, str, object]]:
        wanted = {key for key in keys}
        if not wanted:
            return set()
        try:  # Predicate pruning avoids materializing the entire daily_bars table.
            from pyiceberg.expressions import And, In

            providers = sorted({provider for provider, _, _, _ in wanted})
            adjustment_types = sorted({adjustment_type for _, adjustment_type, _, _ in wanted})
            security_ids = sorted({security_id for _, _, security_id, _ in wanted})
            trading_dates = sorted({trading_date for _, _, _, trading_date in wanted if isinstance(trading_date, date)})
            if not providers or not adjustment_types or not security_ids or not trading_dates:
                return set()
            table = self.daily_bars_table.scan(
                row_filter=And(
                    In("provider", providers),
                    In("adjustment_type", adjustment_types),
                    In("security_id", security_ids),
                    In("trading_date", trading_dates),
                )
            ).to_arrow()
            return {
                (row["provider"], row["adjustment_type"], row["security_id"], row["trading_date"])
                for row in table.to_pylist()
                if (row["provider"], row["adjustment_type"], row["security_id"], row["trading_date"]) in wanted
            }
        except ImportError:  # pragma: no cover - test fallback without pyiceberg
            return {
                (bar.provider, bar.adjustment_type, bar.security_id, bar.trading_date)
                for bar in self.scan_daily_bars()
                if (bar.provider, bar.adjustment_type, bar.security_id, bar.trading_date) in wanted
            }

    def scan_daily_bar_keys(
        self,
        *,
        provider: str,
        adjustment_type: str,
        start_date: date,
        end_date: date,
    ) -> set[tuple[str, date]]:
        """Return security/date keys in a narrow partition-pruned range."""

        try:
            from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThanOrEqual

            table = self.daily_bars_table.scan(
                row_filter=And(
                    EqualTo("provider", provider),
                    EqualTo("adjustment_type", adjustment_type),
                    GreaterThanOrEqual("trading_date", start_date),
                    LessThanOrEqual("trading_date", end_date),
                )
            ).select("security_id", "trading_date").to_arrow()
            return {
                (str(row["security_id"]), row["trading_date"])
                for row in table.to_pylist()
            }
        except ImportError:  # pragma: no cover - test fallback without pyiceberg
            return {
                (bar.security_id, bar.trading_date)
                for bar in self.scan_daily_bars()
                if bar.provider == provider
                and bar.adjustment_type == adjustment_type
                and start_date <= bar.trading_date <= end_date
            }

    def _scan_arrow_table(self, table):
        return table.scan().to_arrow()

    def _reload_security_master(self):
        self.security_master_table = ensure_security_master_table()
        return self.security_master_table

    def _reload_trading_calendar(self):
        self.trading_calendar_table = ensure_trading_calendar_table()
        return self.trading_calendar_table

    def _reload_daily_bars(self):
        self.daily_bars_table = ensure_daily_bars_table()
        return self.daily_bars_table

    def _get_arrow_module(self):
        try:  # pragma: no cover - exercised in real environment
            import pyarrow as pa  # type: ignore
        except ImportError:  # pragma: no cover - test fallback
            class _ArrowModule:
                Table = _SimpleArrowTable

            return _ArrowModule()
        return pa
