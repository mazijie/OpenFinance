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
    pa.field("pdf_sha256", pa.string(), nullable=False), pa.field("page_count", pa.int32(), nullable=False),
    pa.field("extraction_status", pa.string(), nullable=False), pa.field("pdf_object_path", pa.string(), nullable=False),
    pa.field("parse_run_id", pa.string(), nullable=False), pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
]) if pa is not None else None

PAGE_ARROW_SCHEMA = _arrow([
    pa.field("report_id", pa.string(), nullable=False), pa.field("parse_run_id", pa.string(), nullable=False),
    pa.field("page_number", pa.int32(), nullable=False), pa.field("page_type", pa.string(), nullable=False),
    pa.field("text", pa.string(), nullable=False), pa.field("extraction_method", pa.string(), nullable=False),
    pa.field("extraction_status", pa.string(), nullable=False), pa.field("ir_page_path", pa.string()),
]) if pa is not None else None

SECTION_ARROW_SCHEMA = _arrow([
    pa.field("section_id", pa.string(), nullable=False), pa.field("report_id", pa.string(), nullable=False),
    pa.field("parse_run_id", pa.string(), nullable=False), pa.field("section_title", pa.string(), nullable=False),
    pa.field("section_path", pa.string(), nullable=False), pa.field("level", pa.int32(), nullable=False),
    pa.field("parent_section_id", pa.string()),
    pa.field("start_page", pa.int32(), nullable=False), pa.field("end_page", pa.int32(), nullable=False),
    pa.field("section_order", pa.int32(), nullable=False),
]) if pa is not None else None

PARSE_RUN_ARROW_SCHEMA = _arrow([
    pa.field("parse_run_id", pa.string(), nullable=False), pa.field("report_id", pa.string(), nullable=False),
    pa.field("pdf_sha256", pa.string(), nullable=False), pa.field("parser_version", pa.string(), nullable=False),
    pa.field("status", pa.string(), nullable=False), pa.field("started_at", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("finished_at", pa.timestamp("us", tz="UTC")), pa.field("ir_object_path", pa.string(), nullable=False),
]) if pa is not None else None

REGION_ARROW_SCHEMA = _arrow([
    pa.field("region_id", pa.string(), nullable=False), pa.field("report_id", pa.string(), nullable=False),
    pa.field("parse_run_id", pa.string(), nullable=False), pa.field("page_number", pa.int32(), nullable=False),
    pa.field("region_type", pa.string(), nullable=False), pa.field("bbox", pa.string(), nullable=False),
    pa.field("reading_order", pa.int32(), nullable=False), pa.field("text", pa.string(), nullable=False),
    pa.field("content_ref", pa.string()),
]) if pa is not None else None

CHUNK_ARROW_SCHEMA = _arrow([
    pa.field("chunk_id", pa.string(), nullable=False), pa.field("report_id", pa.string(), nullable=False),
    pa.field("parse_run_id", pa.string(), nullable=False), pa.field("section_path", pa.string(), nullable=False),
    pa.field("content_type", pa.string(), nullable=False), pa.field("page_start", pa.int32(), nullable=False),
    pa.field("page_end", pa.int32(), nullable=False), pa.field("region_ids", pa.string(), nullable=False),
    pa.field("chunk_text", pa.string(), nullable=False), pa.field("content_hash", pa.string(), nullable=False),
]) if pa is not None else None


def _schema(fields):
    return Schema(*fields) if Schema is not None else None


REPORT_SCHEMA = _schema([
    NestedField(1, "report_id", StringType(), required=True), NestedField(2, "stock_code", StringType(), required=True),
    NestedField(3, "exchange", StringType(), required=True), NestedField(4, "company_name", StringType(), required=False),
    NestedField(5, "report_year", IntegerType(), required=True), NestedField(6, "announcement_id", StringType(), required=True),
    NestedField(7, "title", StringType(), required=True), NestedField(8, "announcement_time", TimestamptzType(), required=True),
    NestedField(9, "pdf_sha256", StringType(), required=True), NestedField(10, "page_count", IntegerType(), required=True),
    NestedField(11, "extraction_status", StringType(), required=True), NestedField(12, "pdf_object_path", StringType(), required=True),
    NestedField(13, "parse_run_id", StringType(), required=True), NestedField(14, "ingested_at", TimestamptzType(), required=True),
]) if Schema is not None else None
PAGE_SCHEMA = _schema([NestedField(1, "report_id", StringType(), required=True), NestedField(2, "parse_run_id", StringType(), required=True), NestedField(3, "page_number", IntegerType(), required=True), NestedField(4, "page_type", StringType(), required=True), NestedField(5, "text", StringType(), required=True), NestedField(6, "extraction_method", StringType(), required=True), NestedField(7, "extraction_status", StringType(), required=True), NestedField(8, "ir_page_path", StringType(), required=False)]) if Schema is not None else None
SECTION_SCHEMA = _schema([NestedField(1, "section_id", StringType(), required=True), NestedField(2, "report_id", StringType(), required=True), NestedField(3, "parse_run_id", StringType(), required=True), NestedField(4, "section_title", StringType(), required=True), NestedField(5, "section_path", StringType(), required=True), NestedField(6, "level", IntegerType(), required=True), NestedField(7, "parent_section_id", StringType(), required=False), NestedField(8, "start_page", IntegerType(), required=True), NestedField(9, "end_page", IntegerType(), required=True), NestedField(10, "section_order", IntegerType(), required=True)]) if Schema is not None else None
PARSE_RUN_SCHEMA = _schema([NestedField(1, "parse_run_id", StringType(), required=True), NestedField(2, "report_id", StringType(), required=True), NestedField(3, "pdf_sha256", StringType(), required=True), NestedField(4, "parser_version", StringType(), required=True), NestedField(5, "status", StringType(), required=True), NestedField(6, "started_at", TimestamptzType(), required=True), NestedField(7, "finished_at", TimestamptzType(), required=False), NestedField(8, "ir_object_path", StringType(), required=True)]) if Schema is not None else None
REGION_SCHEMA = _schema([NestedField(1, "region_id", StringType(), required=True), NestedField(2, "report_id", StringType(), required=True), NestedField(3, "parse_run_id", StringType(), required=True), NestedField(4, "page_number", IntegerType(), required=True), NestedField(5, "region_type", StringType(), required=True), NestedField(6, "bbox", StringType(), required=True), NestedField(7, "reading_order", IntegerType(), required=True), NestedField(8, "text", StringType(), required=True), NestedField(9, "content_ref", StringType(), required=False)]) if Schema is not None else None
CHUNK_SCHEMA = _schema([NestedField(1, "chunk_id", StringType(), required=True), NestedField(2, "report_id", StringType(), required=True), NestedField(3, "parse_run_id", StringType(), required=True), NestedField(4, "section_path", StringType(), required=True), NestedField(5, "content_type", StringType(), required=True), NestedField(6, "page_start", IntegerType(), required=True), NestedField(7, "page_end", IntegerType(), required=True), NestedField(8, "region_ids", StringType(), required=True), NestedField(9, "chunk_text", StringType(), required=True), NestedField(10, "content_hash", StringType(), required=True)]) if Schema is not None else None


def _ensure(name: str, schema: Any):
    from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError

    if schema is None:
        raise RuntimeError("pyiceberg is required for annual-report Silver tables")
    catalog = get_catalog()
    namespace = (settings.ICEBERG_NAMESPACE,)
    try:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        pass
    identifier = f"{settings.ICEBERG_NAMESPACE}.{name}"
    try:
        table = catalog.load_table(identifier)
    except NoSuchTableError:
        return catalog.create_table(identifier=identifier, schema=schema)
    if table.schema().fields != schema.fields:
        raise RuntimeError(
            f"Iceberg schema mismatch for {identifier}; existing data was preserved. "
            "Inspect and migrate the table explicitly before continuing."
        )
    return table


def ensure_annual_report_tables() -> tuple[Any, Any, Any]:
    return _ensure(settings.ICEBERG_ANNUAL_REPORT_TABLE, REPORT_SCHEMA), _ensure(settings.ICEBERG_ANNUAL_REPORT_PAGE_TABLE, PAGE_SCHEMA), _ensure(settings.ICEBERG_ANNUAL_REPORT_SECTION_TABLE, SECTION_SCHEMA)


def ensure_annual_report_extended_tables() -> tuple[Any, Any, Any]:
    """Ensure the minimal parse-run, region and chunk-manifest tables."""

    return (
        _ensure(settings.ICEBERG_ANNUAL_REPORT_PARSE_RUN_TABLE, PARSE_RUN_SCHEMA),
        _ensure(settings.ICEBERG_ANNUAL_REPORT_REGION_TABLE, REGION_SCHEMA),
        _ensure(settings.ICEBERG_ANNUAL_REPORT_CHUNK_TABLE, CHUNK_SCHEMA),
    )
