"""Iceberg schemas for annual-report Silver data."""

from __future__ import annotations

from typing import Any

try:
    import pyarrow as pa
except ImportError:  # pragma: no cover
    pa = None

try:
    from pyiceberg.schema import Schema
    from pyiceberg.types import BooleanType, IntegerType, LongType, NestedField, StringType, TimestamptzType
except ImportError:  # pragma: no cover
    Schema = BooleanType = IntegerType = LongType = NestedField = StringType = TimestamptzType = None  # type: ignore[assignment]

from backend.config import settings
from backend.lakehouse.catalog import get_catalog


def _arrow(fields):
    return pa.schema(fields) if pa is not None else None


REPORT_ARROW_SCHEMA = _arrow([
    pa.field("report_id", pa.string(), nullable=False), pa.field("stock_code", pa.string(), nullable=False),
    pa.field("exchange", pa.string(), nullable=False), pa.field("company_name", pa.string()),
    pa.field("report_year", pa.int32(), nullable=False), pa.field("announcement_id", pa.string(), nullable=False),
    pa.field("title", pa.string(), nullable=False), pa.field("announcement_time", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("pdf_url", pa.string(), nullable=False), pa.field("is_canonical", pa.bool_(), nullable=False),
    pa.field("version_type", pa.string(), nullable=False), pa.field("pdf_sha256", pa.string(), nullable=False),
    pa.field("pdf_size", pa.int64(), nullable=False), pa.field("page_count", pa.int32(), nullable=False),
    pa.field("extraction_status", pa.string(), nullable=False), pa.field("raw_object_path", pa.string(), nullable=False),
    pa.field("pdf_object_path", pa.string(), nullable=False), pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
]) if pa is not None else None

PAGE_ARROW_SCHEMA = _arrow([
    pa.field("report_id", pa.string(), nullable=False), pa.field("stock_code", pa.string(), nullable=False),
    pa.field("report_year", pa.int32(), nullable=False), pa.field("page_number", pa.int32(), nullable=False),
    pa.field("text", pa.string(), nullable=False), pa.field("text_sha256", pa.string(), nullable=False),
    pa.field("char_count", pa.int32(), nullable=False), pa.field("extraction_method", pa.string(), nullable=False),
    pa.field("extraction_status", pa.string(), nullable=False),
]) if pa is not None else None

SECTION_ARROW_SCHEMA = _arrow([
    pa.field("section_id", pa.string(), nullable=False), pa.field("report_id", pa.string(), nullable=False),
    pa.field("stock_code", pa.string(), nullable=False), pa.field("report_year", pa.int32(), nullable=False),
    pa.field("section_title", pa.string(), nullable=False), pa.field("section_path", pa.string(), nullable=False),
    pa.field("start_page", pa.int32(), nullable=False), pa.field("end_page", pa.int32(), nullable=False),
    pa.field("text", pa.string(), nullable=False), pa.field("section_order", pa.int32(), nullable=False),
]) if pa is not None else None


def _schema(fields):
    return Schema(*fields) if Schema is not None else None


REPORT_SCHEMA = _schema([
    NestedField(1, "report_id", StringType(), required=True), NestedField(2, "stock_code", StringType(), required=True),
    NestedField(3, "exchange", StringType(), required=True), NestedField(4, "company_name", StringType(), required=False),
    NestedField(5, "report_year", IntegerType(), required=True), NestedField(6, "announcement_id", StringType(), required=True),
    NestedField(7, "title", StringType(), required=True), NestedField(8, "announcement_time", TimestamptzType(), required=True),
    NestedField(9, "pdf_url", StringType(), required=True), NestedField(10, "is_canonical", BooleanType(), required=True),
    NestedField(11, "version_type", StringType(), required=True), NestedField(12, "pdf_sha256", StringType(), required=True),
    NestedField(13, "pdf_size", LongType(), required=True), NestedField(14, "page_count", IntegerType(), required=True),
    NestedField(15, "extraction_status", StringType(), required=True), NestedField(16, "raw_object_path", StringType(), required=True),
    NestedField(17, "pdf_object_path", StringType(), required=True), NestedField(18, "ingested_at", TimestamptzType(), required=True),
]) if Schema is not None else None
PAGE_SCHEMA = _schema([NestedField(1, "report_id", StringType(), required=True), NestedField(2, "stock_code", StringType(), required=True), NestedField(3, "report_year", IntegerType(), required=True), NestedField(4, "page_number", IntegerType(), required=True), NestedField(5, "text", StringType(), required=True), NestedField(6, "text_sha256", StringType(), required=True), NestedField(7, "char_count", IntegerType(), required=True), NestedField(8, "extraction_method", StringType(), required=True), NestedField(9, "extraction_status", StringType(), required=True)]) if Schema is not None else None
SECTION_SCHEMA = _schema([NestedField(1, "section_id", StringType(), required=True), NestedField(2, "report_id", StringType(), required=True), NestedField(3, "stock_code", StringType(), required=True), NestedField(4, "report_year", IntegerType(), required=True), NestedField(5, "section_title", StringType(), required=True), NestedField(6, "section_path", StringType(), required=True), NestedField(7, "start_page", IntegerType(), required=True), NestedField(8, "end_page", IntegerType(), required=True), NestedField(9, "text", StringType(), required=True), NestedField(10, "section_order", IntegerType(), required=True)]) if Schema is not None else None


def _ensure(name: str, schema: Any):
    catalog = get_catalog()
    namespace = (settings.ICEBERG_NAMESPACE,)
    try:
        catalog.load_namespace_properties(namespace)
    except Exception:
        catalog.create_namespace(namespace)
    identifier = f"{settings.ICEBERG_NAMESPACE}.{name}"
    try:
        return catalog.load_table(identifier)
    except Exception:
        return catalog.create_table(identifier=identifier, schema=schema)


def ensure_annual_report_tables() -> tuple[Any, Any, Any]:
    return _ensure(settings.ICEBERG_ANNUAL_REPORT_TABLE, REPORT_SCHEMA), _ensure(settings.ICEBERG_ANNUAL_REPORT_PAGE_TABLE, PAGE_SCHEMA), _ensure(settings.ICEBERG_ANNUAL_REPORT_SECTION_TABLE, SECTION_SCHEMA)
