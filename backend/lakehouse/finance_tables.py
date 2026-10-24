"""Iceberg table definitions for finance lakehouse data."""

from __future__ import annotations

from typing import Any

try:  # pragma: no cover - exercised in real environment
    import pyarrow as pa
except ImportError:  # pragma: no cover - test fallback
    pa = None

try:  # pragma: no cover - exercised in real environment
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform, MonthTransform
    from pyiceberg.types import BooleanType, DateType, DoubleType, IntegerType, NestedField, StringType, TimestamptzType
except ImportError:  # pragma: no cover - test fallback
    PartitionField = PartitionSpec = Schema = MonthTransform = IdentityTransform = None  # type: ignore[assignment]
    BooleanType = DateType = DoubleType = IntegerType = NestedField = StringType = TimestamptzType = None  # type: ignore[assignment]

from backend.config import settings
from backend.lakehouse.catalog import get_catalog


SECURITY_MASTER_ARROW_SCHEMA = (
    pa.schema(
        [
            pa.field("security_id", pa.string(), nullable=False),
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("exchange", pa.string(), nullable=False),
            pa.field("security_type", pa.string(), nullable=False),
            pa.field("name", pa.string(), nullable=False),
            pa.field("list_date", pa.date32()),
            pa.field("delist_date", pa.date32()),
            pa.field("status", pa.string(), nullable=False),
            pa.field("provider", pa.string(), nullable=False),
            pa.field("source_record_id", pa.string()),
            pa.field("source_version", pa.string()),
            pa.field("payload_hash", pa.string()),
            pa.field("raw_object_path", pa.string(), nullable=False),
            pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("available_at", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("schema_version", pa.int32(), nullable=False),
        ]
    )
    if pa is not None
    else None
)

TRADING_CALENDAR_ARROW_SCHEMA = (
    pa.schema(
        [
            pa.field("exchange", pa.string(), nullable=False),
            pa.field("trading_date", pa.date32(), nullable=False),
            pa.field("is_open", pa.bool_(), nullable=False),
            pa.field("session_open_at", pa.timestamp("us", tz="UTC")),
            pa.field("session_close_at", pa.timestamp("us", tz="UTC")),
        ]
    )
    if pa is not None
    else None
)

DAILY_BARS_ARROW_SCHEMA = (
    pa.schema(
        [
            pa.field("security_id", pa.string(), nullable=False),
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("exchange", pa.string(), nullable=False),
            pa.field("trading_date", pa.date32(), nullable=False),
            pa.field("open", pa.float64(), nullable=False),
            pa.field("high", pa.float64(), nullable=False),
            pa.field("low", pa.float64(), nullable=False),
            pa.field("close", pa.float64(), nullable=False),
            pa.field("volume", pa.float64(), nullable=False),
            pa.field("amount", pa.float64()),
            pa.field("turnover", pa.float64()),
            pa.field("is_suspended", pa.bool_(), nullable=False),
            pa.field("is_st", pa.bool_(), nullable=False),
            pa.field("adjustment_factor", pa.float64(), nullable=False),
            pa.field("adjustment_type", pa.string(), nullable=False),
            pa.field("provider", pa.string(), nullable=False),
            pa.field("source_record_id", pa.string()),
            pa.field("source_version", pa.string()),
            pa.field("payload_hash", pa.string()),
            pa.field("raw_object_path", pa.string(), nullable=False),
            pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("available_at", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("schema_version", pa.int32(), nullable=False),
        ]
    )
    if pa is not None
    else None
)


SECURITY_MASTER_SCHEMA = (
    Schema(
        NestedField(field_id=1, name="security_id", field_type=StringType(), required=True),
        NestedField(field_id=2, name="symbol", field_type=StringType(), required=True),
        NestedField(field_id=3, name="exchange", field_type=StringType(), required=True),
        NestedField(field_id=4, name="security_type", field_type=StringType(), required=True),
        NestedField(field_id=5, name="name", field_type=StringType(), required=True),
        NestedField(field_id=6, name="list_date", field_type=DateType(), required=False),
        NestedField(field_id=7, name="delist_date", field_type=DateType(), required=False),
        NestedField(field_id=8, name="status", field_type=StringType(), required=True),
        NestedField(field_id=9, name="provider", field_type=StringType(), required=True),
        NestedField(field_id=10, name="source_record_id", field_type=StringType(), required=False),
        NestedField(field_id=11, name="source_version", field_type=StringType(), required=False),
        NestedField(field_id=12, name="payload_hash", field_type=StringType(), required=False),
        NestedField(field_id=13, name="raw_object_path", field_type=StringType(), required=True),
        NestedField(field_id=14, name="event_time", field_type=TimestamptzType(), required=True),
        NestedField(field_id=15, name="available_at", field_type=TimestamptzType(), required=True),
        NestedField(field_id=16, name="ingested_at", field_type=TimestamptzType(), required=True),
        NestedField(field_id=17, name="schema_version", field_type=IntegerType(), required=True),
    )
    if Schema is not None
    else None
)

TRADING_CALENDAR_SCHEMA = (
    Schema(
        NestedField(field_id=1, name="exchange", field_type=StringType(), required=True),
        NestedField(field_id=2, name="trading_date", field_type=DateType(), required=True),
        NestedField(field_id=3, name="is_open", field_type=BooleanType(), required=True),
        NestedField(field_id=4, name="session_open_at", field_type=TimestamptzType(), required=False),
        NestedField(field_id=5, name="session_close_at", field_type=TimestamptzType(), required=False),
    )
    if Schema is not None
    else None
)

DAILY_BARS_SCHEMA = (
    Schema(
        NestedField(field_id=1, name="security_id", field_type=StringType(), required=True),
        NestedField(field_id=2, name="symbol", field_type=StringType(), required=True),
        NestedField(field_id=3, name="exchange", field_type=StringType(), required=True),
        NestedField(field_id=4, name="trading_date", field_type=DateType(), required=True),
        NestedField(field_id=5, name="open", field_type=DoubleType(), required=True),
        NestedField(field_id=6, name="high", field_type=DoubleType(), required=True),
        NestedField(field_id=7, name="low", field_type=DoubleType(), required=True),
        NestedField(field_id=8, name="close", field_type=DoubleType(), required=True),
        NestedField(field_id=9, name="volume", field_type=DoubleType(), required=True),
        NestedField(field_id=10, name="amount", field_type=DoubleType(), required=False),
        NestedField(field_id=11, name="turnover", field_type=DoubleType(), required=False),
        NestedField(field_id=12, name="is_suspended", field_type=BooleanType(), required=True),
        NestedField(field_id=13, name="is_st", field_type=BooleanType(), required=True),
        NestedField(field_id=14, name="adjustment_factor", field_type=DoubleType(), required=True),
        NestedField(field_id=15, name="adjustment_type", field_type=StringType(), required=True),
        NestedField(field_id=16, name="provider", field_type=StringType(), required=True),
        NestedField(field_id=17, name="source_record_id", field_type=StringType(), required=False),
        NestedField(field_id=18, name="source_version", field_type=StringType(), required=False),
        NestedField(field_id=19, name="payload_hash", field_type=StringType(), required=False),
        NestedField(field_id=20, name="raw_object_path", field_type=StringType(), required=True),
        NestedField(field_id=21, name="event_time", field_type=TimestamptzType(), required=True),
        NestedField(field_id=22, name="available_at", field_type=TimestamptzType(), required=True),
        NestedField(field_id=23, name="ingested_at", field_type=TimestamptzType(), required=True),
        NestedField(field_id=24, name="schema_version", field_type=IntegerType(), required=True),
    )
    if Schema is not None
    else None
)


SECURITY_MASTER_PARTITION_SPEC = (
    PartitionSpec()
    if PartitionSpec is not None
    else None
)

TRADING_CALENDAR_PARTITION_SPEC = (
    PartitionSpec()
    if PartitionSpec is not None
    else None
)

DAILY_BARS_PARTITION_SPEC = (
    PartitionSpec(
        PartitionField(source_id=4, field_id=1001, transform=MonthTransform(), name="trading_month"),
        PartitionField(source_id=3, field_id=1002, transform=IdentityTransform(), name="exchange"),
        PartitionField(source_id=16, field_id=1003, transform=IdentityTransform(), name="provider"),
    )
    if PartitionSpec is not None
    else None
)


def ensure_finance_namespace() -> None:
    """Create namespace used by finance tables when missing."""

    catalog = get_catalog()
    namespace = (settings.ICEBERG_NAMESPACE,)
    try:
        catalog.load_namespace_properties(namespace)
    except Exception:
        catalog.create_namespace(namespace)


def finance_table_identifier(table_name: str) -> str:
    return f"{settings.ICEBERG_NAMESPACE}.{table_name}"


def ensure_finance_table(table_name: str, schema: Any, partition_spec: Any) -> Any:
    """Create a finance table when missing."""

    catalog = get_catalog()
    ensure_finance_namespace()
    identifier = finance_table_identifier(table_name)
    try:
        return catalog.load_table(identifier)
    except Exception:
        return catalog.create_table(identifier=identifier, schema=schema, partition_spec=partition_spec)


def ensure_security_master_table() -> Any:
    return ensure_finance_table(settings.ICEBERG_SECURITY_MASTER_TABLE, SECURITY_MASTER_SCHEMA, SECURITY_MASTER_PARTITION_SPEC)


def ensure_trading_calendar_table() -> Any:
    return ensure_finance_table(settings.ICEBERG_TRADING_CALENDAR_TABLE, TRADING_CALENDAR_SCHEMA, TRADING_CALENDAR_PARTITION_SPEC)


def ensure_daily_bars_table() -> Any:
    return ensure_finance_table(settings.ICEBERG_DAILY_BARS_TABLE, DAILY_BARS_SCHEMA, DAILY_BARS_PARTITION_SPEC)
