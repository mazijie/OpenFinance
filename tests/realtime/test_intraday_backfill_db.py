"""Exercise actual PostgreSQL SQL in a rolled-back isolated schema."""
import importlib
import os
import uuid
from datetime import date, datetime
from unittest.mock import patch

import psycopg
import pytest
import requests
from realtime import intraday_backfill as backfill
from realtime.tencent import CHINA
from psycopg.rows import dict_row
from backend.services.realtime_quote_service import RealtimeQuoteService


class SchemaConnection:
    def __init__(self, conn, schema):
        self.conn, self.schema = conn, schema
    def execute(self, sql, args=()):
        return self.conn.execute(sql.replace('openfinance.', self.schema + '.'), args)
    def cursor(self):
        return SchemaCursor(self.conn.cursor(), self.schema)


class SchemaCursor:
    def __init__(self, cursor, schema):
        self.cursor, self.schema = cursor, schema
    def __enter__(self):
        self.cursor.__enter__()
        return self
    def __exit__(self, *args):
        return self.cursor.__exit__(*args)
    def executemany(self, sql, args):
        return self.cursor.executemany(sql.replace('openfinance.', self.schema + '.'), args)


@pytest.mark.skipif(not os.getenv('BACKFILL_TEST_DATABASE_URL'), reason='requires PostgreSQL')
def test_incremental_scan_backfill_and_invalidation():
    day = date(2026, 11, 12)
    schema = 'test_backfill_' + uuid.uuid4().hex
    migration = importlib.import_module('migrations.versions.0015_intraday_backfill')
    with psycopg.connect(os.environ['BACKFILL_TEST_DATABASE_URL']) as conn:
        try:
            conn.execute(f'CREATE SCHEMA {schema}')
            conn.execute(f'''CREATE TABLE {schema}.realtime_quote_windows (
                symbol text, window_start timestamp, window_end timestamp, sample_avg double precision,
                window_basis text, recompute_stale boolean DEFAULT false, PRIMARY KEY(symbol,window_start))''')
            c = SchemaConnection(conn, schema)
            with patch.object(migration.op, 'execute', lambda sql: c.execute(sql)):
                migration.upgrade()
                importlib.import_module('migrations.versions.0016_intraday_provider_pause').upgrade()
            c.execute('''CREATE TABLE openfinance.realtime_quote_late (symbol text, window_start timestamp, resolved_at timestamptz)''')
            c.execute('INSERT INTO openfinance.intraday_universe VALUES (%s,%s)', (day, '000009.SZ'))
            c.execute("INSERT INTO openfinance.realtime_quote_windows VALUES (%s,%s,%s,10,'capture_time',false)",
                      ('000009.SZ', datetime(2026,11,12,1,30), datetime(2026,11,12,1,31)))
            end = datetime(2026,11,12,1,33)
            count, _ = backfill.scan(c, day, end)
            assert count == 1
            assert backfill.scan(c, day, end)[0] == 0
            assert backfill.missing_for_symbol(c, day, '000009.SZ', end) == [datetime(2026,11,12,1,31), datetime(2026,11,12,1,32)]
            class Session:
                def get(self, *args, **kwargs):
                    class Response:
                        text = ""
                        def raise_for_status(self): pass
                        def json(self):
                            return {'code': 0, 'data': {'sz000009': {'data': {'date':'20261112', 'data':['0930 1 0','0931 2 0','0932 3 0']}}}}
                    return Response()
            assert backfill.process_one(c, day, end, datetime(2026,11,12,10,0,tzinfo=CHINA), Session())
            assert not backfill.missing_for_symbol(c, day, '000009.SZ', end)
            assert c.execute('SELECT count(*) FROM openfinance.intraday_backfill_points').fetchone()[0] == 2
            assert c.execute('SELECT status FROM openfinance.intraday_backfill_jobs').fetchone()[0] == 'complete'
            c.execute("UPDATE openfinance.intraday_backfill_points SET fetched_at=%s", (datetime(2026,11,12,1,34,tzinfo=backfill.UTC),))
            # Real read query prefers a sample even when a Tencent row exists for the same minute.
            c.execute("INSERT INTO openfinance.intraday_backfill_points (symbol,window_start,price,fetched_at) VALUES (%s,%s,9,%s)",
                      ('000009.SZ', datetime(2026,11,12,1,30), datetime(2026,11,12,1,34,tzinfo=backfill.UTC)))
            class ReadConnection(SchemaConnection):
                def __enter__(self): return self
                def __exit__(self, *args): pass
                def execute(self, sql, args=()):
                    return self.conn.cursor(row_factory=dict_row).execute(sql.replace('openfinance.', self.schema + '.'), args)
            with patch('backend.services.realtime_quote_service.psycopg.connect', return_value=ReadConnection(conn, schema)):
                result = RealtimeQuoteService().intraday('000009.SZ', datetime(2026,11,12,2,0,tzinfo=backfill.UTC))
            assert [p['price'] for p in result['points']] == [10, 2, 3]
            assert [p['source'] for p in result['points']] == ['sample', 'tencent_minute', 'tencent_minute']
            c.execute('DELETE FROM openfinance.intraday_backfill_points WHERE window_start=%s', (datetime(2026,11,12,1,31),))
            assert backfill.scan(c, day, end)[0] == 1
            assert c.execute('SELECT status FROM openfinance.intraday_backfill_jobs').fetchone()[0] == 'pending'
            class FailedSession:
                def get(self, *args, **kwargs): raise requests.Timeout('timeout')
            with pytest.raises(requests.Timeout):
                backfill.process_one(c, day, end, datetime(2026,11,12,10,0,tzinfo=CHINA), FailedSession())
            retry = c.execute('SELECT attempts,last_error,next_attempt_at > now() FROM openfinance.intraday_backfill_jobs').fetchone()
            assert retry[0] == 2 and 'timeout' in retry[1] and retry[2]
            blocked_at = datetime(2026,11,12,14,0,tzinfo=CHINA)
            assert backfill.pause_provider(c, blocked_at, 'HTTP 501').date() == date(2026,11,13)
            assert backfill.provider_paused(c, blocked_at)[1] == 'HTTP 501'
            assert backfill.provider_paused(c, datetime(2026,11,13,9,30,tzinfo=CHINA)) is None
        finally:
            conn.rollback()


@pytest.mark.skipif(not os.getenv('BACKFILL_TEST_DATABASE_URL'), reason='requires PostgreSQL')
def test_manual_run_limits_and_single_execution():
    from fastapi import HTTPException
    from backend.api import intraday_operations as operations
    from datetime import timedelta, timezone

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            value = cls(2026, 11, 12, 10, 10, tzinfo=CHINA)
            return value.astimezone(tz) if tz else value.replace(tzinfo=None)

    class ConnectionProxy(SchemaConnection):
        def __init__(self, conn, schema, rows=False):
            super().__init__(conn, schema)
            self.rows = rows
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def execute(self, sql, args=()):
            cursor = self.conn.cursor(row_factory=dict_row) if self.rows else self.conn.cursor()
            return cursor.execute(sql.replace('openfinance.', self.schema + '.'), args)
        def commit(self): pass

    day = date(2026, 11, 12)
    schema = 'test_backfill_' + uuid.uuid4().hex
    with psycopg.connect(os.environ['BACKFILL_TEST_DATABASE_URL']) as conn:
        try:
            conn.execute(f'CREATE SCHEMA {schema}')
            conn.execute(f'''CREATE TABLE {schema}.realtime_quote_windows (
                symbol text, window_start timestamp, window_end timestamp, sample_avg double precision,
                window_basis text, recompute_stale boolean DEFAULT false, PRIMARY KEY(symbol,window_start))''')
            base = SchemaConnection(conn, schema)
            migration = importlib.import_module('migrations.versions.0015_intraday_backfill')
            with patch.object(migration.op, 'execute', lambda sql: base.execute(sql)):
                migration.upgrade()
                importlib.import_module('migrations.versions.0016_intraday_provider_pause').upgrade()
                importlib.import_module('migrations.versions.0017_manual_intraday').upgrade()
            for i in range(12):
                base.execute('INSERT INTO openfinance.intraday_universe VALUES (%s,%s)', (day, f'{i:06}.SZ'))
            def connect(*args, **kwargs):
                return ConnectionProxy(conn, schema, kwargs.get('row_factory') == dict_row)
            with patch('backend.api.intraday_operations.psycopg.connect', side_effect=connect), \
                 patch.object(operations, 'datetime', FrozenDateTime), \
                 patch.object(backfill, 'datetime', FrozenDateTime), \
                 patch.object(backfill.subprocess, 'run'), \
                 patch.object(backfill, 'fetch_minutes', return_value={datetime(2026,11,12,1,30): 10.5}) as fetch:
                single = operations.create_run(operations.RunRequest(scope='symbol', symbol='000001.SZ'))
                with pytest.raises(HTTPException) as exc:
                    operations.create_run(operations.RunRequest(scope='batch'))
                assert exc.value.status_code == 409
                backfill.run_manual('unused', single['id'], day)
                assert fetch.call_count == 1
                assert base.execute('SELECT count(*) FROM openfinance.intraday_backfill_points').fetchone()[0] == 1
                assert base.execute('SELECT status FROM openfinance.intraday_manual_runs WHERE id=%s', (single['id'],)).fetchone()[0] == 'completed'
                batch = operations.create_run(operations.RunRequest(scope='batch'))
                assert len(batch['symbols']) == 10 and len(set(batch['symbols'])) == 10
                assert fetch.call_count == 1  # submission alone never contacts Tencent
                assert base.execute('SELECT count(*) FROM openfinance.intraday_manual_items WHERE run_id=%s', (batch['id'],)).fetchone()[0] == 10
                base.execute('DELETE FROM openfinance.intraday_request_clock')
                response = requests.Response()
                response.status_code = 501
                response._content = b'waf.tencent.com'
                fetch.reset_mock()
                fetch.side_effect = requests.HTTPError('blocked', response=response)
                backfill.run_manual('unused', batch['id'], day)
                assert fetch.call_count == 1
                assert base.execute('SELECT status FROM openfinance.intraday_manual_runs WHERE id=%s', (batch['id'],)).fetchone()[0] == 'blocked'
                assert base.execute("SELECT count(*) FROM openfinance.intraday_manual_items WHERE run_id=%s AND status='queued'", (batch['id'],)).fetchone()[0] == 9
                assert base.execute('SELECT count(*) FROM openfinance.intraday_provider_pause').fetchone()[0] == 1
                base.execute("UPDATE openfinance.intraday_provider_pause SET paused_until=now()+interval '1 hour'")
                with pytest.raises(HTTPException) as paused:
                    operations.create_run(operations.RunRequest(scope='symbol', symbol='000001.SZ'))
                assert paused.value.status_code == 409
                assert fetch.call_count == 1
                base.execute("INSERT INTO openfinance.intraday_manual_runs(trading_day,scope,status) VALUES (%s,'batch','queued')", (day,))
                backfill.interrupt_unfinished(base)
                assert base.execute("SELECT count(*) FROM openfinance.intraday_manual_runs WHERE status='interrupted'").fetchone()[0] == 1
                assert fetch.call_count == 1
        finally:
            conn.rollback()
