#!/usr/bin/env bash
set -euo pipefail
: "${STARROCKS_ICEBERG_JDBC_PASSWORD:?Set STARROCKS_ICEBERG_JDBC_PASSWORD in .env}"
psql -X -v ON_ERROR_STOP=1 <<'SQL'
\getenv ro_password STARROCKS_ICEBERG_JDBC_PASSWORD
BEGIN;
SELECT 'CREATE ROLE openfinance_starrocks_ro LOGIN' WHERE NOT EXISTS
  (SELECT 1 FROM pg_roles WHERE rolname = 'openfinance_starrocks_ro') \gexec
ALTER ROLE openfinance_starrocks_ro LOGIN PASSWORD :'ro_password';
GRANT CONNECT ON DATABASE openfinance TO openfinance_starrocks_ro;
GRANT USAGE ON SCHEMA openfinance TO openfinance_starrocks_ro;
GRANT SELECT ON openfinance.iceberg_tables, openfinance.iceberg_namespace_properties TO openfinance_starrocks_ro;
COMMIT;
SQL
echo 'StarRocks PostgreSQL read-only role configured.'
