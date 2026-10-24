"""Iceberg table definitions."""

from __future__ import annotations

from typing import Any

try:  # pragma: no cover - exercised in real environment
    import pyarrow as pa
except ImportError:  # pragma: no cover - test fallback
    pa = None

try:  # pragma: no cover - exercised in real environment
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import DayTransform, IdentityTransform
    from pyiceberg.types import ListType, NestedField, StringType, TimestamptzType
except ImportError:  # pragma: no cover - test fallback
    PartitionField = PartitionSpec = Schema = DayTransform = IdentityTransform = ListType = NestedField = StringType = TimestamptzType = None  # type: ignore[assignment]

from backend.config import settings
from backend.lakehouse.catalog import get_catalog

NEWS_TABLE_ARROW_SCHEMA = (
    pa.schema(
        [
            pa.field("news_id", pa.string(), nullable=False),
            pa.field("external_news_id", pa.string()),
            pa.field("provider", pa.string(), nullable=False),
            pa.field("category", pa.string()),
            pa.field("source_name", pa.string()),
            pa.field("title", pa.string(), nullable=False),
            pa.field("summary", pa.string()),
            pa.field("content", pa.string()),
            pa.field("url", pa.string(), nullable=False),
            pa.field("cover_image_url", pa.string()),
            pa.field("published_at", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("language", pa.string()),
            pa.field("symbols", pa.list_(pa.string())),
            pa.field("entities", pa.list_(pa.string())),
            pa.field("content_hash", pa.string(), nullable=False),
            pa.field("raw_object_path", pa.string(), nullable=False),
        ]
    )
    if pa is not None
    else None
)

NEWS_TABLE_SCHEMA = (
    Schema(
        NestedField(field_id=1, name="news_id", field_type=StringType(), required=True),
        NestedField(field_id=2, name="external_news_id", field_type=StringType(), required=False),
        NestedField(field_id=3, name="provider", field_type=StringType(), required=True),
        NestedField(field_id=4, name="category", field_type=StringType(), required=False),
        NestedField(field_id=5, name="source_name", field_type=StringType(), required=False),
        NestedField(field_id=6, name="title", field_type=StringType(), required=True),
        NestedField(field_id=7, name="summary", field_type=StringType(), required=False),
        NestedField(field_id=8, name="content", field_type=StringType(), required=False),
        NestedField(field_id=9, name="url", field_type=StringType(), required=True),
        NestedField(field_id=10, name="cover_image_url", field_type=StringType(), required=False),
        NestedField(field_id=11, name="published_at", field_type=TimestamptzType(), required=True),
        NestedField(field_id=12, name="ingested_at", field_type=TimestamptzType(), required=True),
        NestedField(
            field_id=13,
            name="language",
            field_type=StringType(),
            required=False,
        ),
        NestedField(
            field_id=14,
            name="symbols",
            field_type=ListType(element_id=15, element_type=StringType(), element_required=False),
            required=False,
        ),
        NestedField(
            field_id=16,
            name="entities",
            field_type=ListType(element_id=17, element_type=StringType(), element_required=False),
            required=False,
        ),
        NestedField(field_id=18, name="content_hash", field_type=StringType(), required=True),
        NestedField(field_id=19, name="raw_object_path", field_type=StringType(), required=True),
    )
    if Schema is not None
    else None
)

NEWS_TABLE_PARTITION_SPEC = (
    PartitionSpec(
        PartitionField(source_id=11, field_id=1001, transform=DayTransform(), name="published_day"),
        PartitionField(source_id=3, field_id=1002, transform=IdentityTransform(), name="provider"),
    )
    if PartitionSpec is not None
    else None
)


def ensure_news_table() -> Any:
    """Create namespace and news table when missing."""

    catalog = get_catalog()
    namespace = (settings.ICEBERG_NAMESPACE,)
    try:
        catalog.load_namespace_properties(namespace)
    except Exception:
        catalog.create_namespace(namespace)

    identifier = settings.iceberg_news_table_identifier
    try:
        return catalog.load_table(identifier)
    except Exception:
        return catalog.create_table(
            identifier=identifier,
            schema=NEWS_TABLE_SCHEMA,
            partition_spec=NEWS_TABLE_PARTITION_SPEC,
        )
