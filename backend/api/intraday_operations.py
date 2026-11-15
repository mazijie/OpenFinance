"""Manual intraday backfill controls for the operations page."""
from datetime import datetime, time as daytime, timedelta, timezone

import psycopg
from psycopg.rows import dict_row
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend.config import settings
from backend.services.realtime_quote_service import symbol_candidates
from realtime.intraday_backfill import CHINA, trading_minutes

router = APIRouter(prefix='/api/operations/intraday', tags=['operations'])


class RunRequest(BaseModel):
    scope: str
    symbol: str | None = None


@router.post('/runs', status_code=202)
def create_run(request: RunRequest):
    if request.scope not in ('symbol', 'batch'):
        raise HTTPException(422, '任务范围无效')
    symbol = None
    if request.scope == 'symbol':
        try:
            candidates = symbol_candidates(request.symbol or '')
            if len(candidates) != 1:
                raise ValueError('请提供交易所后缀')
            symbol = candidates[0]
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
    now = datetime.now(CHINA)
    day = now.date()
    start = datetime.combine(day, daytime(9, 30), CHINA).astimezone(timezone.utc).replace(tzinfo=None)
    end = (now.astimezone(timezone.utc) - timedelta(minutes=2)).replace(second=0, microsecond=0, tzinfo=None)
    minutes = trading_minutes(day, start, end)
    if not minutes:
        raise HTTPException(409, '当天没有可检查的交易时段')
    try:
        with psycopg.connect(settings.DATABASE_URL, row_factory=dict_row) as conn:
            conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", ('openfinance_manual_intraday_create',))
            pause = conn.execute("SELECT paused_until FROM openfinance.intraday_provider_pause WHERE provider='tencent_minute' AND paused_until>now()").fetchone()
            if pause:
                raise HTTPException(409, f"腾讯请求暂停至 {pause['paused_until'].astimezone(CHINA).isoformat()}")
            if conn.execute("SELECT 1 FROM openfinance.intraday_manual_runs WHERE status IN ('queued','running')").fetchone():
                raise HTTPException(409, '已有任务正在执行')
            if symbol:
                if not conn.execute('SELECT 1 FROM openfinance.intraday_universe WHERE trading_day=%s AND symbol=%s', (day,symbol)).fetchone():
                    raise HTTPException(409, '该证券不在当日采集范围内，或当天尚未成功采集')
                symbols = [symbol]
            else:
                rows = conn.execute("""SELECT u.symbol FROM openfinance.intraday_universe u
                    LEFT JOIN openfinance.intraday_backfill_jobs j ON j.trading_day=u.trading_day AND j.symbol=u.symbol
                    WHERE u.trading_day=%s AND EXISTS (
                        SELECT 1 FROM unnest(%s::timestamp[]) m(minute)
                        WHERE NOT EXISTS (SELECT 1 FROM openfinance.realtime_quote_windows w WHERE w.symbol=u.symbol
                            AND w.window_start=m.minute AND w.window_basis='capture_time' AND NOT w.recompute_stale)
                        AND NOT EXISTS (SELECT 1 FROM openfinance.intraday_backfill_points b WHERE b.symbol=u.symbol AND b.window_start=m.minute))
                    ORDER BY CASE WHEN COALESCE(j.attempts,0)=0 THEN 0 ELSE 1 END,j.updated_at NULLS FIRST,u.symbol LIMIT 10""", (day,minutes)).fetchall()
                symbols = [row['symbol'] for row in rows]
                if not symbols:
                    raise HTTPException(409, '当天没有可补录的缺口或证券范围不可用')
            run = conn.execute("INSERT INTO openfinance.intraday_manual_runs (trading_day,scope,status) VALUES (%s,%s,'queued') RETURNING id", (day,request.scope)).fetchone()
            with conn.cursor() as cur:
                cur.executemany('INSERT INTO openfinance.intraday_manual_items (run_id,symbol) VALUES (%s,%s)', [(run['id'],s) for s in symbols])
            return {'id': run['id'], 'symbols': symbols}
    except psycopg.Error as exc:
        raise HTTPException(503, '运维服务暂不可用') from exc


@router.get('/runs')
def list_runs():
    try:
        with psycopg.connect(settings.DATABASE_URL, row_factory=dict_row) as conn:
            runs = conn.execute('SELECT * FROM openfinance.intraday_manual_runs ORDER BY id DESC LIMIT 20').fetchall()
            ids = [r['id'] for r in runs]
            items = conn.execute('SELECT * FROM openfinance.intraday_manual_items WHERE run_id=ANY(%s) ORDER BY symbol', (ids,)).fetchall() if ids else []
            pause = conn.execute("SELECT paused_until,reason FROM openfinance.intraday_provider_pause WHERE provider='tencent_minute' AND paused_until>now()").fetchone()
            for run in runs:
                run['items'] = [item for item in items if item['run_id']==run['id']]
            return {'runs': runs, 'pause': pause}
    except psycopg.Error as exc:
        raise HTTPException(503, '运维服务暂不可用') from exc
