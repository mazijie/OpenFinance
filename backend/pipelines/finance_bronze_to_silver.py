"""Pipeline to normalize Bronze finance objects into Silver Iceberg tables."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time
from hashlib import sha256
from typing import Any, Callable

from backend.ingestion.writers.bronze_writer import BronzeWriter
from backend.lakehouse.finance_writer import FinanceWriter
from backend.logging_utils import get_logger
from backend.schemas.finance import DailyBarRecord, FinanceBronzeObject, SecurityMasterRecord, TradingCalendarRecord

logger = get_logger(__name__)


def _record_version(record: Any) -> tuple[datetime, datetime, str, str]:
    minimum = datetime.min.replace(tzinfo=UTC)
    return (
        getattr(record, "available_at", None) or minimum,
        getattr(record, "ingested_at", None) or minimum,
        getattr(record, "source_version", None) or "",
        getattr(record, "payload_hash", None) or "",
    )


@dataclass(slots=True)
class FinancePipelineStats:
    read_count: int = 0
    filtered_count: int = 0
    duplicate_count: int = 0
    failed_count: int = 0
    written_security_master: int = 0
    written_trading_calendar: int = 0
    written_daily_bars: int = 0
    processed_objects: int = 0

    def to_dict(self) -> dict[str, int]:
        return asdict(self)

    def merge(self, other: "FinancePipelineStats") -> None:
        for field_name in self.__dataclass_fields__:
            setattr(
                self,
                field_name,
                getattr(self, field_name) + getattr(other, field_name),
            )


class FinanceBronzeToSilverPipeline:
    """Normalize finance Bronze JSON files into Silver tables."""

    def __init__(self, bronze_writer: BronzeWriter | None = None, finance_writer: FinanceWriter | None = None) -> None:
        self.bronze_writer = bronze_writer or BronzeWriter()
        self.finance_writer = finance_writer or FinanceWriter()

    def process_object(self, object_path: str) -> FinancePipelineStats:
        """Normalize one object while preserving the single-object API."""

        return self.process_objects([object_path])

    def process_objects(self, object_paths: list[str], *, check_lease: Callable[[], None] | None = None) -> FinancePipelineStats:
        """Normalize a whole Bronze batch and commit each Silver table once."""

        paths = list(
            dict.fromkeys(
                str(path).strip()
                for path in object_paths
                if str(path).strip()
            )
        )
        if not paths:
            raise ValueError("At least one Bronze object path is required")

        stats = FinancePipelineStats()
        security_records: list[SecurityMasterRecord] = []
        calendar_records: list[TradingCalendarRecord] = []
        daily_records: list[DailyBarRecord] = []

        # Read and validate the complete batch before any write. A malformed
        # object therefore fails the job without leaving a partially applied
        # Bronze batch behind.
        for object_path in paths:
            if check_lease: check_lease()
            object_security_records, object_calendar_records, object_daily_records, object_stats = (
                self._normalize_object(object_path)
            )
            stats.merge(object_stats)
            security_records.extend(object_security_records)
            calendar_records.extend(object_calendar_records)
            daily_records.extend(object_daily_records)

        security_records, security_duplicates = self._deduplicate_records(
            security_records,
            key=lambda record: (record.provider, record.security_id),
        )
        calendar_records, calendar_duplicates = self._deduplicate_records(
            calendar_records,
            key=lambda record: (record.exchange, record.trading_date),
        )
        daily_records, daily_duplicates = self._deduplicate_records(
            daily_records,
            key=lambda record: (
                record.provider,
                record.adjustment_type,
                record.security_id,
                record.trading_date,
            ),
        )
        stats.duplicate_count += security_duplicates + calendar_duplicates + daily_duplicates

        if check_lease: check_lease()
        stats.written_security_master = self.finance_writer.append_security_master(security_records)
        if check_lease: check_lease()
        stats.written_trading_calendar = self.finance_writer.append_trading_calendar(calendar_records)
        if check_lease: check_lease()
        stats.written_daily_bars = self.finance_writer.append_daily_bars(daily_records)
        logger.info(
            "Finance Bronze to Silver completed",
            event="finance_bronze_to_silver_completed",
            extra_fields=stats.to_dict(),
        )
        return stats

    def _normalize_object(
        self,
        object_path: str,
    ) -> tuple[
        list[SecurityMasterRecord],
        list[TradingCalendarRecord],
        list[DailyBarRecord],
        FinancePipelineStats,
    ]:
        payload = self.bronze_writer.read_json(object_path)
        bronze_object = FinanceBronzeObject.model_validate(payload)
        stats = FinancePipelineStats(processed_objects=1)

        security_master_payload = list(bronze_object.raw_response.get("security_master") or [])
        calendar_payload = list(bronze_object.raw_response.get("trading_calendar") or [])
        daily_bars_payload = list(bronze_object.raw_response.get("daily_bars") or [])
        stats.read_count = len(security_master_payload) + len(calendar_payload) + len(daily_bars_payload)

        security_records: list[SecurityMasterRecord] = []
        for row in security_master_payload:
            try:
                security_records.append(self._normalize_security_master(bronze_object, object_path, row))
            except ValueError:
                stats.filtered_count += 1
            except Exception:
                stats.failed_count += 1

        calendar_records: list[TradingCalendarRecord] = []
        for row in calendar_payload:
            try:
                calendar_records.append(self._normalize_trading_calendar(row))
            except ValueError:
                stats.filtered_count += 1
            except Exception:
                stats.failed_count += 1

        daily_records: list[DailyBarRecord] = []
        for row in daily_bars_payload:
            try:
                record = self._normalize_daily_bar(bronze_object, object_path, row)
            except ValueError:
                stats.filtered_count += 1
                continue
            except Exception:
                stats.failed_count += 1
                continue

            daily_records.append(record)

        return security_records, calendar_records, daily_records, stats

    @staticmethod
    def _deduplicate_records(records: list[Any], *, key: Any) -> tuple[list[Any], int]:
        """Keep the latest visible record for each business key within one batch."""

        latest: dict[Any, Any] = {}
        duplicate_count = 0
        for record in records:
            record_key = key(record)
            current = latest.get(record_key)
            duplicate_count += current is not None
            if current is None or _record_version(record) >= _record_version(current):
                latest[record_key] = record
        return list(latest.values()), duplicate_count

    def _normalize_security_master(
        self,
        bronze_object: FinanceBronzeObject,
        object_path: str,
        row: dict[str, Any],
    ) -> SecurityMasterRecord:
        symbol = str(row.get("symbol") or "").strip().upper()
        exchange = str(row.get("exchange") or "").strip().upper()
        name = str(row.get("name") or "").strip()
        if not symbol or not exchange or not name:
            raise ValueError("security master missing key fields")
        security_id = self._normalize_security_id(row.get("security_id"), symbol, exchange)
        symbol = next(part for part in reversed(security_id.split(".")) if part != exchange)
        list_date = self._parse_date(row.get("list_date"))
        event_time = datetime.combine(list_date or bronze_object.ingested_at.date(), time.min, tzinfo=UTC)
        return SecurityMasterRecord(
            security_id=security_id,
            symbol=symbol,
            exchange=exchange,
            security_type=str(row.get("security_type") or "stock").strip().lower(),
            name=name,
            list_date=list_date,
            delist_date=self._parse_date(row.get("delist_date")),
            status=str(row.get("status") or "active").strip().lower(),
            provider=bronze_object.provider,
            source_record_id=str(row.get("source_record_id") or security_id),
            source_version=str(row.get("source_version") or bronze_object.source_version or ""),
            payload_hash=str(row.get("payload_hash") or bronze_object.payload_hash or self._payload_hash(row)),
            raw_object_path=object_path,
            event_time=event_time,
            available_at=bronze_object.ingested_at,
            ingested_at=bronze_object.ingested_at,
            schema_version=1,
        )

    def _normalize_trading_calendar(self, row: dict[str, Any]) -> TradingCalendarRecord:
        exchange = str(row.get("exchange") or "").strip().upper()
        trading_date = self._parse_date(row.get("trading_date"))
        if not exchange or trading_date is None:
            raise ValueError("trading calendar missing key fields")
        return TradingCalendarRecord(
            exchange=exchange,
            trading_date=trading_date,
            is_open=bool(row.get("is_open", True)),
            session_open_at=row.get("session_open_at"),
            session_close_at=row.get("session_close_at"),
        )

    def _normalize_daily_bar(
        self,
        bronze_object: FinanceBronzeObject,
        object_path: str,
        row: dict[str, Any],
    ) -> DailyBarRecord:
        symbol = str(row.get("symbol") or "").strip().upper()
        exchange = str(row.get("exchange") or "").strip().upper()
        trading_date = self._parse_date(row.get("trading_date") or row.get("date"))
        if not symbol or not exchange or trading_date is None:
            raise ValueError("daily bar missing key fields")

        open_price = self._parse_float(row.get("open"))
        high_price = self._parse_float(row.get("high"))
        low_price = self._parse_float(row.get("low"))
        close_price = self._parse_float(row.get("close"))
        volume = self._parse_float(row.get("volume"))
        amount = self._parse_optional_float(row.get("amount"))
        turnover = self._parse_optional_float(row.get("turnover"))
        self._validate_bar_prices(open_price, high_price, low_price, close_price, volume)

        security_id = self._normalize_security_id(row.get("security_id"), symbol, exchange)
        adjustment_type = str(row.get("adjustment_type") or bronze_object.request_params.get("adjustment_type") or "qfq").strip().lower()
        source_record_id = str(row.get("source_record_id") or f"{symbol}-{trading_date.isoformat()}-{adjustment_type}")
        source_version = str(row.get("source_version") or bronze_object.source_version or "")
        payload_hash = str(row.get("payload_hash") or bronze_object.payload_hash or self._payload_hash(row))
        event_time = datetime.combine(trading_date, time.min, tzinfo=UTC)

        is_suspended = bool(row.get("is_suspended", False))
        if not is_suspended and volume == 0 and open_price == high_price == low_price == close_price == 0:
            is_suspended = True

        return DailyBarRecord(
            security_id=security_id,
            symbol=symbol,
            exchange=exchange,
            trading_date=trading_date,
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            volume=volume,
            amount=amount,
            turnover=turnover,
            is_suspended=is_suspended,
            is_st=bool(row.get("is_st", False)),
            adjustment_factor=self._parse_float(row.get("adjustment_factor", 1.0)),
            adjustment_type=adjustment_type,
            provider=bronze_object.provider,
            source_record_id=source_record_id,
            source_version=source_version,
            payload_hash=payload_hash,
            raw_object_path=object_path,
            event_time=event_time,
            available_at=bronze_object.ingested_at,
            ingested_at=bronze_object.ingested_at,
            schema_version=1,
        )

    def _parse_date(self, value: Any) -> date | None:
        if value in {None, ""}:
            return None
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        return date.fromisoformat(str(value))

    def _parse_float(self, value: Any) -> float:
        parsed = float(value)
        if parsed < 0:
            raise ValueError("numeric field cannot be negative")
        return parsed

    def _parse_optional_float(self, value: Any) -> float | None:
        if value in {None, ""}:
            return None
        return self._parse_float(value)

    def _validate_bar_prices(self, open_price: float, high_price: float, low_price: float, close_price: float, volume: float) -> None:
        if high_price < max(open_price, low_price, close_price):
            raise ValueError("high price is inconsistent")
        if low_price > min(open_price, high_price, close_price):
            raise ValueError("low price is inconsistent")
        if volume < 0:
            raise ValueError("volume must be non-negative")

    def _payload_hash(self, row: dict[str, Any]) -> str:
        return sha256(repr(sorted(row.items())).encode("utf-8")).hexdigest()

    @staticmethod
    def _normalize_security_id(explicit_id: Any, symbol: str, exchange: str) -> str:
        candidate = str(explicit_id or "").strip().upper()
        if candidate:
            return candidate
        if "." in symbol:
            left, right = symbol.split(".", 1)
            if right == exchange:
                return symbol
            if left == exchange:
                return f"{right}.{exchange}"
        return f"{symbol}.{exchange}"
