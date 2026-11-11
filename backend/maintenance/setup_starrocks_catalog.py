"""Configure the StarRocks Iceberg catalog and its read-only application user."""
from __future__ import annotations

import os

from backend.config import settings
from backend.lakehouse.starrocks_daily_reader import _identifier


def required(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise ValueError(f"{name} must be set")
    return value


def main() -> None:
    import pymysql

    catalog = _identifier(settings.STARROCKS_CATALOG)
    namespace = _identifier(settings.ICEBERG_NAMESPACE)
    table = _identifier(settings.ICEBERG_DAILY_BARS_TABLE)
    user = _identifier(settings.STARROCKS_USER)
    properties = {
        "type": "iceberg", "iceberg.catalog.type": "jdbc",
        "iceberg.catalog.uri": required("STARROCKS_ICEBERG_JDBC_URI"),
        "iceberg.catalog.jdbc.user": required("STARROCKS_ICEBERG_JDBC_USER"),
        "iceberg.catalog.jdbc.password": required("STARROCKS_ICEBERG_JDBC_PASSWORD"),
        "iceberg.catalog.jdbc.init-catalog-tables": "false",
        "iceberg.catalog.warehouse": settings.ICEBERG_WAREHOUSE,
        "iceberg_meta_cache_ttl_sec": "0",
        "aws.s3.endpoint": required("STARROCKS_S3_ENDPOINT"),
        "aws.s3.access_key": required("STARROCKS_S3_ACCESS_KEY"),
        "aws.s3.secret_key": required("STARROCKS_S3_SECRET_KEY"),
        "aws.s3.enable_ssl": os.environ.get("STARROCKS_S3_ENABLE_SSL", "false"),
        "aws.s3.enable_path_style_access": "true",
    }
    reader_password = required("STARROCKS_PASSWORD")
    connection = pymysql.connect(host=settings.STARROCKS_HOST, port=settings.STARROCKS_PORT,
                                 user=os.environ.get("STARROCKS_ADMIN_USER", "root"),
                                 password=os.environ.get("STARROCKS_ADMIN_PASSWORD", ""),
                                 connect_timeout=settings.STARROCKS_CONNECT_TIMEOUT_SECONDS)
    try:
        with connection.cursor() as cursor:
            assignments = ", ".join(f"{connection.escape(key)} = {connection.escape(value)}" for key, value in properties.items())
            cursor.execute("SHOW CATALOGS")
            existing = any(row[0] == settings.STARROCKS_CATALOG for row in cursor.fetchall())
            if existing:
                cursor.execute(f"SHOW CREATE CATALOG {catalog}")
                definition = str(cursor.fetchone())
                for key in ("iceberg.catalog.uri", "iceberg.catalog.warehouse", "aws.s3.endpoint", "iceberg_meta_cache_ttl_sec"):
                    if properties[key] not in definition:
                        raise RuntimeError(f"Existing catalog does not match expected {key}")
            else:
                cursor.execute(f"CREATE EXTERNAL CATALOG {catalog} PROPERTIES ({assignments})")
            cursor.execute(f"CREATE USER IF NOT EXISTS {user} IDENTIFIED BY %s", (reader_password,))
            cursor.execute(f"ALTER USER {user} IDENTIFIED BY %s", (reader_password,))
            cursor.execute(f"GRANT USAGE ON CATALOG {catalog} TO USER {user}")
            cursor.execute(f"SET CATALOG {catalog}")
            cursor.execute(f"GRANT SELECT ON TABLE {namespace}.{table} TO USER {user}")
    finally:
        connection.close()
    print("Catalog and read-only user configured. Verify snapshot compatibility before enabling the reader.")


if __name__ == "__main__":
    main()
