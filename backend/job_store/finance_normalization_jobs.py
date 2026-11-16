"""Durable queue for finance Bronze-to-Silver normalization jobs."""
from __future__ import annotations
import json
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator
from uuid import UUID, uuid4
import psycopg
from backend.config import settings
from backend.job_store.job_lease import JobLeaseLost

class FinanceNormalizationJobStore:
    _schema = "openfinance"
    _table = "finance_normalization_jobs"
    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = dsn or settings.DATABASE_URL
        if not self.dsn: raise RuntimeError("DATABASE_URL is not configured")
    @property
    def fqtn(self): return f"{self._schema}.{self._table}"
    @contextmanager
    def _connection(self) -> Iterator[Any]:
        conn = psycopg.connect(self.dsn, autocommit=True, connect_timeout=10, options="-c statement_timeout=1800000 -c lock_timeout=300000", keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=3)
        try: yield conn
        finally: conn.close()
    def ensure_schema(self) -> None:
        with self._connection() as c, c.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
            cur.execute(f"CREATE TABLE IF NOT EXISTS {self.fqtn} (job_id uuid PRIMARY KEY, status text NOT NULL CHECK (status IN ('queued','running','success','failed')), payload jsonb NOT NULL, result jsonb, error text, attempts integer NOT NULL DEFAULT 0, created_at timestamptz NOT NULL DEFAULT now(), started_at timestamptz, finished_at timestamptz, heartbeat_at timestamptz)")
            cur.execute(f"CREATE INDEX IF NOT EXISTS finance_normalization_jobs_status_idx ON {self.fqtn} (status, created_at)")
    def create(self, payload: dict[str, Any]) -> str:
        self.ensure_schema(); jid = uuid4()
        with self._connection() as c, c.cursor() as cur: cur.execute(f"INSERT INTO {self.fqtn} (job_id,status,payload) VALUES (%s,'queued',%s::jsonb)",(jid,json.dumps(payload,default=str)))
        return str(jid)
    def get(self, job_id: str) -> dict[str, Any] | None:
        self.ensure_schema()
        with self._connection() as c, c.cursor() as cur:
            cur.execute(f"SELECT job_id,status,result,error,attempts,created_at,started_at,finished_at FROM {self.fqtn} WHERE job_id=%s",(UUID(job_id),)); row=cur.fetchone()
        if row is None:return None
        keys=("job_id","status","result","error","attempts","created_at","started_at","finished_at")
        return {k:(v.isoformat() if isinstance(v,datetime) else str(v) if isinstance(v,UUID) else v) for k,v in zip(keys,row,strict=True)}
    def heartbeat(self, jid: str, attempt: int) -> None:
        self.ensure_schema()
        with self._connection() as c, c.cursor() as cur:
            cur.execute(f"UPDATE {self.fqtn} SET heartbeat_at=now() WHERE job_id=%s AND status='running' AND attempts=%s", (UUID(jid), attempt))
            if cur.rowcount != 1: raise JobLeaseLost(f"Job {jid} attempt {attempt} no longer owns the lease")
    def claim(self, stale_after_seconds: int | None = None) -> dict[str, Any] | None:
        self.ensure_schema()
        stale_seconds = (
            stale_after_seconds
            if stale_after_seconds is not None
            else settings.FINANCE_NORMALIZATION_STALE_SECONDS
        )
        with self._connection() as c, c.cursor() as cur:
            cur.execute(f"WITH candidate AS (SELECT job_id FROM {self.fqtn} WHERE status='queued' OR (status='running' AND heartbeat_at < now()-interval '{int(stale_seconds)} seconds') ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE {self.fqtn} j SET status='running',attempts=j.attempts+1,started_at=COALESCE(j.started_at,now()),heartbeat_at=now(),error=NULL FROM candidate WHERE j.job_id=candidate.job_id RETURNING j.job_id,j.payload,j.attempts"); row=cur.fetchone()
        return None if row is None else {"job_id":str(row[0]),"payload":row[1],"attempts":row[2]}
    def _update(self,jid,status,attempt,result=None,error=None):
        with self._connection() as c, c.cursor() as cur:
            cur.execute(f"UPDATE {self.fqtn} SET status=%s,result=%s::jsonb,error=%s,finished_at=now(),heartbeat_at=now() WHERE job_id=%s AND status='running' AND attempts=%s",(status,json.dumps(result,default=str) if result is not None else "null",error,UUID(jid),attempt))
            if cur.rowcount != 1: raise JobLeaseLost(f"Job {jid} attempt {attempt} no longer owns the lease")
    def complete(self,jid,result,attempt): self._update(jid,"success",attempt,result=result)
    def fail(self,jid,error,attempt): self._update(jid,"failed",attempt,error=error)
