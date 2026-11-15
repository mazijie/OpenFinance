"""Destructive migration test, restricted to an explicitly provided disposable database."""
import os
import subprocess
import sys
from urllib.parse import urlparse
from uuid import uuid4

import psycopg
import pytest


def test_upgrade_removes_monitor_and_sessions():
    url = os.getenv("OPENFINANCE_MIGRATION_TEST_URL")
    if not url:
        pytest.skip("OPENFINANCE_MIGRATION_TEST_URL requires an empty disposable database")
    dsn = url.replace("postgresql+psycopg://", "postgresql://", 1)
    database = urlparse(dsn).path.lstrip("/")
    if not database.startswith("openfinance_migration_test_"):
        pytest.fail("Refusing to run against a database without the test-only name prefix")
    with psycopg.connect(dsn) as conn:
        if conn.execute("SELECT to_regclass('openfinance.agent_threads')").fetchone()[0]:
            pytest.fail("Migration test requires an empty disposable database")

    env = os.environ.copy()
    env["DATABASE_URL"] = url
    subprocess.run([sys.executable, "-m", "alembic", "upgrade", "0013_market_history_index"], env=env, check=True)
    thread_id = uuid4()
    with psycopg.connect(dsn) as conn:
        conn.execute("INSERT INTO openfinance.agent_threads(thread_id,title) VALUES (%s,'test')", (thread_id,))
        conn.execute("INSERT INTO openfinance.agent_messages(message_id,thread_id,seq,role,kind) VALUES (%s,%s,1,'assistant','monitor_report')", (uuid4(), thread_id))
        conn.execute("INSERT INTO openfinance.agent_events(event_id,thread_id,event_type) VALUES (%s,%s,'monitor.queued')", (uuid4(), thread_id))
        conn.execute("INSERT INTO openfinance.watchlist_items(symbol,security_id,name,exchange) VALUES ('600519.SH','600519.SH','test','SH')")
        conn.execute("INSERT INTO openfinance.agent_web_domains(host_pattern) VALUES ('example.test')")
        for schema in ("public", "openfinance"):
            for table in ("checkpoint_writes", "checkpoint_blobs", "checkpoints"):
                conn.execute(f"CREATE TABLE {schema}.{table} (thread_id text NOT NULL)")
                conn.execute(f"INSERT INTO {schema}.{table} VALUES (%s)", (str(thread_id),))

    subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"], env=env, check=True)
    with psycopg.connect(dsn) as conn:
        for table in ("monitor_agent_traces", "monitor_reports", "monitor_runs", "watchlist_items"):
            assert conn.execute("SELECT to_regclass(%s)", (f"openfinance.{table}",)).fetchone()[0] is None
        for table in ("agent_threads", "agent_messages", "agent_turns", "agent_events", "agent_tool_calls", "agent_approvals"):
            assert conn.execute(f"SELECT count(*) FROM openfinance.{table}").fetchone()[0] == 0
        for schema in ("public", "openfinance"):
            for table in ("checkpoint_writes", "checkpoint_blobs", "checkpoints"):
                assert conn.execute(f"SELECT count(*) FROM {schema}.{table}").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM openfinance.agent_web_domains WHERE host_pattern='example.test'").fetchone()[0] == 1
