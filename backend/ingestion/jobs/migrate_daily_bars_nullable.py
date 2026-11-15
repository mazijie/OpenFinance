"""Evolve existing daily-bar tables to accept missing provider fields."""

from backend.config import settings
from backend.lakehouse.catalog import get_catalog
from backend.lakehouse.finance_tables import finance_table_identifier
from backend.lakehouse.write_coordination import iceberg_write_lock


def migrate() -> None:
    identifier = finance_table_identifier(settings.ICEBERG_DAILY_BARS_TABLE)
    with iceberg_write_lock():
        table = get_catalog().load_table(identifier)
        schema = table.schema()
        expected = {"is_suspended": 12, "is_st": 13, "adjustment_factor": 14}
        for name, field_id in expected.items():
            field = schema.find_field(name)
            if field.field_id != field_id:
                raise ValueError(f"Unexpected field id for {name}: {field.field_id}; expected {field_id}")
        required = [name for name in expected if schema.find_field(name).required]
        if required:
            with table.update_schema() as update:
                for name in required:
                    update.make_column_optional(name)
        table.refresh()
        for name in expected:
            field = table.schema().find_field(name)
            if field.required:
                raise RuntimeError(f"{name} is still required")
            print(f"{name}: optional (field_id={field.field_id})")
    print(f"Schema migration complete: {identifier}")


if __name__ == "__main__":
    migrate()
