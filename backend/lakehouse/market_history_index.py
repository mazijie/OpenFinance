"""Read the complete index only for the snapshot and time it represents."""
from datetime import date, datetime

import psycopg

from backend.config import settings
from backend.logging_utils import get_logger

logger = get_logger(__name__)


def has_earlier(*, table_uuid: str, snapshot_id: int, as_of: datetime,
                symbol: str, adjustment_type: str, before: date) -> bool | None:
    """Return None if the index is unavailable, stale, or outside its time window."""
    if not settings.DATABASE_URL:
        return None
    try:
        with psycopg.connect(settings.DATABASE_URL, connect_timeout=2,
                             options="-c statement_timeout=2000") as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT i.earliest_date
                FROM openfinance.market_history_index_state s
                LEFT JOIN openfinance.market_history_index i
                  ON i.symbol = %s AND i.adjustment_type = %s
                WHERE s.singleton AND s.table_uuid = %s AND s.snapshot_id = %s
                  AND s.valid_from <= %s AND (s.valid_until IS NULL OR %s < s.valid_until)
            """, (symbol, adjustment_type, table_uuid, snapshot_id, as_of, as_of))
            row = cur.fetchone()
        if row is None:
            return None
        return row[0] is not None and row[0] < before
    except Exception:
        logger.warning("Market history index unavailable", exc_info=True)
        return None
