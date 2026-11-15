"""Exercise Spark snapshot aggregation and transactional PostgreSQL publishing."""
import os
from contextlib import closing
from datetime import date
from pathlib import Path
import sys
import uuid

import psycopg2
from psycopg2 import sql
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "spark" / "jobs"))
import build_market_history_index as job


def main():
    name = "market_history_test_" + uuid.uuid4().hex[:12]
    credentials = dict(host="postgres", dbname="openfinance", user=os.environ["POSTGRES_USER"],
                       password=os.environ["POSTGRES_PASSWORD"])
    admin = psycopg2.connect(**credentials)
    admin.autocommit = True
    created = False
    try:
        with admin.cursor() as cur:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        created = True
        with closing(psycopg2.connect(**{**credentials, "dbname": name})) as db:
            with db, db.cursor() as cur:
                cur.execute("CREATE SCHEMA openfinance")
                cur.execute("""CREATE TABLE openfinance.market_history_index_state (
                    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton), table_uuid uuid NOT NULL,
                    snapshot_id bigint NOT NULL, valid_from timestamptz NOT NULL, valid_until timestamptz,
                    published_at timestamptz NOT NULL DEFAULT now())""")
                cur.execute("""CREATE TABLE openfinance.market_history_index (
                    symbol text NOT NULL, adjustment_type text NOT NULL, earliest_date date,
                    PRIMARY KEY (symbol, adjustment_type))""")
        spark = SparkSession.builder.getOrCreate()
        spark.sql("CREATE NAMESPACE IF NOT EXISTS test.market")
        spark.sql("""CREATE TABLE test.market.daily_bars (
            symbol string, adjustment_type string, trading_date date,
            available_at timestamp, ingested_at timestamp) USING iceberg""")
        spark.sql("""INSERT INTO test.market.daily_bars VALUES
            ('600519.SH', 'none', DATE '2020-01-01', TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-01 00:00:00'),
            ('600519.SH', 'none', DATE '2019-01-01', current_timestamp() + INTERVAL 1 DAY, TIMESTAMP '2020-01-01 00:00:00'),
            ('600519.SH', 'qfq', DATE '2021-01-01', TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-01 00:00:00')""")
        job.TABLE = "test.market.daily_bars"
        os.environ["MARKET_HISTORY_INDEX_DSN"] = psycopg2.extensions.make_dsn(**{**credentials, "dbname": name})
        job.main()
        with closing(psycopg2.connect(**{**credentials, "dbname": name})) as db, db.cursor() as cur:
            cur.execute("SELECT snapshot_id, valid_until > valid_from FROM openfinance.market_history_index_state")
            snapshot_id, valid_window = cur.fetchone()
            assert snapshot_id > 0 and valid_window
            cur.execute("SELECT symbol, adjustment_type, earliest_date FROM openfinance.market_history_index ORDER BY adjustment_type")
            assert cur.fetchall() == [("600519.SH", "none", date(2020, 1, 1)),
                                      ("600519.SH", "qfq", date(2021, 1, 1))]
        print("Spark snapshot indexing and PostgreSQL publication passed")
    finally:
        if created:
            with admin.cursor() as cur:
                cur.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(name)))
        admin.close()


if __name__ == "__main__":
    main()
