"""Detect current-day gaps and fill them from Tencent without modifying sampled windows."""
from datetime import datetime, time as daytime, timedelta, timezone
import logging
import math
import os
import subprocess
import time

import psycopg
import requests
from realtime.tencent import CHINA, to_tencent

log = logging.getLogger(__name__)
UTC = timezone.utc


def trading_minutes(day, start, end):
    """Return UTC-naive minute starts in [start, end), excluding lunch."""
    minutes = []
    for opening, closing in ((daytime(9, 30), daytime(11, 30)), (daytime(13), daytime(15))):
        current = datetime.combine(day, opening, CHINA).astimezone(UTC).replace(tzinfo=None)
        stop = datetime.combine(day, closing, CHINA).astimezone(UTC).replace(tzinfo=None) + timedelta(minutes=1)
        current = max(current, start)
        while current < min(stop, end):
            minutes.append(current)
            current += timedelta(minutes=1)
    return minutes


def parse_minutes(payload, symbol, day, now):
    code = to_tencent(symbol)
    if not code or payload.get('code') != 0:
        raise ValueError('Tencent minute response rejected')
    try:
        data = payload['data'][code]['data']
        if data['date'] != day.strftime('%Y%m%d'):
            raise ValueError('Tencent minute response date mismatch')
        rows = data['data']
        if not isinstance(rows, list):
            raise ValueError('invalid minute data')
    except (KeyError, TypeError) as exc:
        raise ValueError('invalid Tencent minute response') from exc
    result = {}
    for row in rows:
        try:
            fields = row.split()
            if len(fields[0]) != 4 or not fields[0].isdigit():
                raise ValueError('invalid minute')
            hour, minute = int(fields[0][:2]), int(fields[0][2:])
            instant = datetime.combine(day, daytime(hour, minute), CHINA)
            price = float(fields[1])
            clock = (hour, minute)
            if not ((9, 30) <= clock <= (11, 30) or (13, 0) <= clock <= (15, 0)):
                continue
            if instant >= now.astimezone(CHINA).replace(second=0, microsecond=0):
                continue
            if not math.isfinite(price) or price <= 0:
                raise ValueError('invalid price')
            key = instant.astimezone(UTC).replace(tzinfo=None)
            if key in result:
                raise ValueError('duplicate minute')
            result[key] = price
        except (ValueError, IndexError, AttributeError) as exc:
            raise ValueError('invalid Tencent minute row') from exc
    return result


def fetch_minutes(symbol, day, now, session=requests):
    code = to_tencent(symbol)
    if not code:
        raise ValueError('unsupported symbol')
    response = session.get('https://web.ifzq.gtimg.cn/appstock/app/minute/query',
                           params={'code': code}, timeout=(5, 20))
    response.raise_for_status()
    if 'waf.tencent.com' in response.text.lower():
        raise requests.HTTPError('Tencent WAF blocked request', response=response)
    return parse_minutes(response.json(), symbol, day, now)


def record_universe(dsn, day, items):
    symbols = sorted({row['symbol'] for row in items if to_tencent(row['symbol'])})
    if not symbols or not any(row.get('status') == 'valid' for row in items):
        return
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            existing = {r[0] for r in cur.execute('SELECT symbol FROM openfinance.intraday_universe WHERE trading_day=%s', (day,)).fetchall()}
            added = [symbol for symbol in symbols if symbol not in existing]
            if added:
                cur.executemany('INSERT INTO openfinance.intraday_universe (trading_day,symbol) VALUES (%s,%s) ON CONFLICT DO NOTHING',
                                [(day, symbol) for symbol in added])
                cur.execute('DELETE FROM openfinance.intraday_scan_progress WHERE trading_day=%s', (day,))


def missing_for_symbol(conn, day, symbol, end):
    start = datetime.combine(day, daytime(9, 30), CHINA).astimezone(UTC).replace(tzinfo=None)
    minutes = trading_minutes(day, start, end)
    if not minutes:
        return []
    rows = conn.execute("""SELECT m.minute FROM unnest(%s::timestamp[]) AS m(minute)
        WHERE NOT EXISTS (SELECT 1 FROM openfinance.realtime_quote_windows w
            WHERE w.symbol=%s AND w.window_start=m.minute AND w.window_basis='capture_time' AND NOT w.recompute_stale)
        AND NOT EXISTS (SELECT 1 FROM openfinance.intraday_backfill_points b
            WHERE b.symbol=%s AND b.window_start=m.minute) ORDER BY m.minute""", (minutes, symbol, symbol)).fetchall()
    return [row[0] for row in rows]


def scan(conn, day, end):
    start = datetime.combine(day, daytime(9, 30), CHINA).astimezone(UTC).replace(tzinfo=None)
    row = conn.execute('SELECT scanned_through FROM openfinance.intraday_scan_progress WHERE trading_day=%s FOR UPDATE', (day,)).fetchone()
    if row:
        start = max(start, row[0])
    minutes = trading_minutes(day, start, end)
    count = 0
    if minutes:
        result = conn.execute("""INSERT INTO openfinance.intraday_backfill_jobs (trading_day,symbol)
            SELECT DISTINCT %s,u.symbol FROM openfinance.intraday_universe u CROSS JOIN unnest(%s::timestamp[]) AS m(minute)
            WHERE u.trading_day=%s
              AND NOT EXISTS (SELECT 1 FROM openfinance.realtime_quote_windows w WHERE w.symbol=u.symbol
                  AND w.window_start=m.minute AND w.window_basis='capture_time' AND NOT w.recompute_stale)
              AND NOT EXISTS (SELECT 1 FROM openfinance.intraday_backfill_points b WHERE b.symbol=u.symbol AND b.window_start=m.minute)
            ON CONFLICT (trading_day,symbol) DO UPDATE SET status='pending',
                next_attempt_at=CASE WHEN openfinance.intraday_backfill_jobs.status='complete' THEN now()
                    ELSE openfinance.intraday_backfill_jobs.next_attempt_at END, updated_at=now()""", (day, minutes, day))
        count = result.rowcount
    conn.execute("""INSERT INTO openfinance.intraday_scan_progress (trading_day,scanned_through) VALUES (%s,%s)
        ON CONFLICT (trading_day) DO UPDATE SET scanned_through=GREATEST(openfinance.intraday_scan_progress.scanned_through,EXCLUDED.scanned_through),updated_at=now()""", (day, end))
    return count, start


def process_one(conn, day, end, now, session=requests):
    job = conn.execute("""SELECT symbol,attempts FROM openfinance.intraday_backfill_jobs
        WHERE trading_day=%s AND status='pending' AND next_attempt_at<=now()
        ORDER BY priority_at DESC NULLS LAST, next_attempt_at, symbol LIMIT 1 FOR UPDATE SKIP LOCKED""", (day,)).fetchone()
    if not job:
        return False
    symbol, attempts = job
    missing = missing_for_symbol(conn, day, symbol, end)
    if not missing:
        conn.execute("UPDATE openfinance.intraday_backfill_jobs SET status='complete',last_error=NULL,updated_at=now() WHERE trading_day=%s AND symbol=%s", (day, symbol))
        return True
    try:
        points = fetch_minutes(symbol, day, now, session)
        rows = [(symbol, minute, points[minute]) for minute in missing if minute in points]
        with conn.cursor() as cur:
            cur.executemany("""INSERT INTO openfinance.intraday_backfill_points (symbol,window_start,price)
                VALUES (%s,%s,%s) ON CONFLICT (symbol,window_start) DO NOTHING""", rows)
        remaining = len(missing) - len(rows)
        conn.execute("""UPDATE openfinance.intraday_backfill_jobs SET status=%s, attempts=attempts+1,
            next_attempt_at=now()+interval '30 minutes',last_error=%s,priority_at=NULL,updated_at=now()
            WHERE trading_day=%s AND symbol=%s""", ('pending' if remaining else 'complete', f'{remaining} minutes unavailable' if remaining else None, day, symbol))
    except (requests.RequestException, ValueError) as exc:
        delay = min(1800, 60 * 2 ** min(attempts, 5))
        conn.execute("""UPDATE openfinance.intraday_backfill_jobs SET attempts=attempts+1,
            next_attempt_at=now()+(%s * interval '1 second'),last_error=%s,priority_at=NULL,updated_at=now()
            WHERE trading_day=%s AND symbol=%s""", (delay, str(exc)[:500], day, symbol))
        raise
    return True


def pause_provider(conn, now, reason):
    """Stop requests until the next Shanghai day after explicit blocking."""
    next_day = now.astimezone(CHINA).date() + timedelta(days=1)
    resume = datetime.combine(next_day, daytime(9, 30), CHINA)
    conn.execute("""INSERT INTO openfinance.intraday_provider_pause (provider,paused_until,reason)
        VALUES ('tencent_minute',%s,%s) ON CONFLICT (provider) DO UPDATE
        SET paused_until=GREATEST(openfinance.intraday_provider_pause.paused_until,EXCLUDED.paused_until),
            reason=EXCLUDED.reason,updated_at=now()""", (resume, reason[:500]))
    return resume


def provider_paused(conn, now):
    row = conn.execute("SELECT paused_until,reason FROM openfinance.intraday_provider_pause WHERE provider='tencent_minute' AND paused_until>%s", (now,)).fetchone()
    return row



def completed_cutoff(now):
    return (now.astimezone(UTC) - timedelta(minutes=2)).replace(second=0, microsecond=0, tzinfo=None)


def request_delay(last_requested_at, now):
    return max(0, 30 - (now - last_requested_at).total_seconds()) if last_requested_at else 0


def interrupt_unfinished(conn):
    conn.execute("UPDATE openfinance.intraday_manual_runs SET status='interrupted',finished_at=now(),error='服务重启，需重新发起' WHERE status IN ('queued','running')")


def run_manual(dsn, run_id, day):
    now = datetime.now(CHINA)
    start = datetime.combine(day, daytime(9, 30), CHINA).astimezone(UTC)
    end = completed_cutoff(now)
    if end > start.replace(tzinfo=None):
        try:
            subprocess.run(['python', '-m', 'realtime.recompute', '--start', start.isoformat(),
                            '--end', end.replace(tzinfo=UTC).isoformat()], timeout=180, check=True)
        except (subprocess.SubprocessError, OSError):
            log.exception('Kafka recompute failed; using available data')
    with psycopg.connect(dsn) as conn:
        symbols = [row[0] for row in conn.execute("SELECT symbol FROM openfinance.intraday_manual_items WHERE run_id=%s ORDER BY symbol", (run_id,)).fetchall()]
    for symbol in symbols:
        now = datetime.now(CHINA)
        if now.date() != day:
            with psycopg.connect(dsn) as conn:
                conn.execute("UPDATE openfinance.intraday_manual_runs SET status='interrupted',finished_at=now(),error='交易日已结束' WHERE id=%s", (run_id,))
            return
        with psycopg.connect(dsn) as conn:
            missing = missing_for_symbol(conn, day, symbol, completed_cutoff(now))
            if not missing:
                conn.execute("UPDATE openfinance.intraday_manual_items SET status='complete',completed_at=now() WHERE run_id=%s AND symbol=%s", (run_id,symbol))
                continue
        while True:
            with psycopg.connect(dsn) as conn:
                if datetime.now(CHINA).date() != day:
                    conn.execute("UPDATE openfinance.intraday_manual_runs SET status='interrupted',finished_at=now(),error='交易日已结束' WHERE id=%s", (run_id,))
                    return
                paused = provider_paused(conn, datetime.now(CHINA))
                if paused:
                    conn.execute("UPDATE openfinance.intraday_manual_runs SET status='blocked',finished_at=now(),error=%s WHERE id=%s", (paused[1],run_id))
                    return
                clock = conn.execute("SELECT last_requested_at FROM openfinance.intraday_request_clock WHERE provider='tencent_minute' FOR UPDATE").fetchone()
                if clock:
                    delay = request_delay(clock[0], datetime.now(UTC))
                    if delay > 0:
                        conn.commit()
                        time.sleep(min(delay, 5))
                        continue
                # Reserve before the HTTP request so crashes and restarts cannot bypass the interval.
                conn.execute("""INSERT INTO openfinance.intraday_request_clock(provider,last_requested_at) VALUES ('tencent_minute',now())
                    ON CONFLICT (provider) DO UPDATE SET last_requested_at=EXCLUDED.last_requested_at""")
                conn.execute("UPDATE openfinance.intraday_manual_items SET status='requesting',requested_at=now() WHERE run_id=%s AND symbol=%s", (run_id,symbol))
                conn.execute("""INSERT INTO openfinance.intraday_backfill_jobs (trading_day,symbol,status,attempts,updated_at)
                    VALUES (%s,%s,'pending',1,now()) ON CONFLICT (trading_day,symbol) DO UPDATE
                    SET attempts=openfinance.intraday_backfill_jobs.attempts+1,updated_at=now()""", (day,symbol))
                break
        try:
            points = fetch_minutes(symbol, day, datetime.now(CHINA))
            rows = [(symbol, minute, points[minute]) for minute in missing if minute in points]
            with psycopg.connect(dsn) as conn:
                with conn.cursor() as cur:
                    cur.executemany("""INSERT INTO openfinance.intraday_backfill_points(symbol,window_start,price)
                        VALUES (%s,%s,%s) ON CONFLICT(symbol,window_start) DO NOTHING""", rows)
                remaining = len(missing) - len(rows)
                error = f'仍有 {remaining} 分钟无数据' if remaining else None
                conn.execute("UPDATE openfinance.intraday_manual_items SET status=%s,points_added=%s,error=%s,completed_at=now() WHERE run_id=%s AND symbol=%s", ('partial' if remaining else 'complete',len(rows),error,run_id,symbol))
                conn.execute("UPDATE openfinance.intraday_backfill_jobs SET status=%s,last_error=%s,updated_at=now() WHERE trading_day=%s AND symbol=%s", ('pending' if remaining else 'complete',error,day,symbol))
        except (requests.RequestException, ValueError) as exc:
            response = getattr(exc, 'response', None)
            blocked = response is not None and (response.status_code in (429,501) or 'waf.tencent.com' in response.text.lower())
            with psycopg.connect(dsn) as conn:
                if blocked:
                    pause_provider(conn, datetime.now(CHINA), f'HTTP {response.status_code}: provider blocked requests')
                conn.execute("UPDATE openfinance.intraday_manual_items SET status='failed',error=%s,completed_at=now() WHERE run_id=%s AND symbol=%s", (str(exc)[:500],run_id,symbol))
                conn.execute("UPDATE openfinance.intraday_backfill_jobs SET last_error=%s,updated_at=now() WHERE trading_day=%s AND symbol=%s", (str(exc)[:500],day,symbol))
                if blocked:
                    conn.execute("UPDATE openfinance.intraday_manual_runs SET status='blocked',error=%s,finished_at=now() WHERE id=%s", (str(exc)[:500],run_id))
                    return
    with psycopg.connect(dsn) as conn:
        conn.execute("UPDATE openfinance.intraday_manual_runs SET status='completed',finished_at=now() WHERE id=%s", (run_id,))


def main():
    logging.basicConfig(level=logging.INFO)
    dsn = os.environ['DATABASE_URL']
    with psycopg.connect(dsn, autocommit=True) as lock:
        if not lock.execute("SELECT pg_try_advisory_lock(hashtextextended(%s, 0))", ('openfinance_intraday_backfill_worker',)).fetchone()[0]:
            log.error('another intraday backfill worker is active')
            return
        with psycopg.connect(dsn) as conn:
            interrupt_unfinished(conn)
        while True:
            try:
                lock.execute('SELECT 1')
                with psycopg.connect(dsn) as conn:
                    row = conn.execute("""UPDATE openfinance.intraday_manual_runs SET status='running',started_at=now()
                        WHERE id=(SELECT id FROM openfinance.intraday_manual_runs WHERE status='queued' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED)
                        RETURNING id,trading_day""").fetchone()
                if not row:
                    time.sleep(2)
                    continue
                try:
                    run_manual(dsn, *row)
                except Exception as exc:
                    log.exception('manual backfill run failed')
                    with psycopg.connect(dsn) as conn:
                        conn.execute("UPDATE openfinance.intraday_manual_runs SET status='failed',finished_at=now(),error=%s WHERE id=%s", (str(exc)[:500],row[0]))
            except Exception:
                log.exception('manual backfill worker failed')
                time.sleep(10)


if __name__ == '__main__':
    main()
