"""Read-only StarRocks adapters for online Iceberg-backed queries."""
from __future__ import annotations

import json
from datetime import UTC, datetime

from backend.config import settings
from backend.lakehouse.starrocks_daily_reader import StarRocksDailyReader, _utc_naive
from backend.schemas.annual_report import (
    AnnualReportPageRecord, AnnualReportParseRunRecord, AnnualReportRecord,
    AnnualReportRegionRecord,
)
from backend.schemas.finance import GoldMetricRecord, SecurityMasterRecord, TradingCalendarRecord
from backend.schemas.news import NewsArticleRecord


class StarRocksReader(StarRocksDailyReader):
    def __init__(self):
        super().__init__(allow_latest=True)

    def _rows(self, table, where="", args=()):
        rows = self._query(f"SELECT * FROM {self._table(None, table)}{where}", args)
        # The SQL session is UTC; MySQL protocol returns timestamps without tzinfo.
        for row in rows:
            for key, value in row.items():
                if isinstance(value, datetime) and value.tzinfo is None:
                    row[key] = value.replace(tzinfo=UTC)
        return rows

    def scan_security_master(self):
        return [SecurityMasterRecord.model_validate(row) for row in self._rows(settings.ICEBERG_SECURITY_MASTER_TABLE)]

    def scan_security_options(self):
        selected = {}
        table = self._table(None, settings.ICEBERG_SECURITY_MASTER_TABLE)
        rows = self._query(f"SELECT `symbol`, `name`, `exchange`, `ingested_at` FROM {table}", ())
        for row in rows:
            exchange = str(row['exchange']).upper()
            symbol = str(row['symbol']).upper().split('.')[0]
            key = (symbol, exchange)
            rank = (str(row.get('ingested_at') or ''), str(row.get('name') or ''))
            if key not in selected or rank > selected[key][0]:
                selected[key] = (rank, {'symbol': symbol, 'name': str(row['name']), 'exchange': exchange})
        return [item for _, item in selected.values()]

    def scan_trading_calendar_for_date(self, trading_date):
        rows = self._rows(settings.ICEBERG_TRADING_CALENDAR_TABLE, ' WHERE `trading_date` = %s', (trading_date,))
        return [TradingCalendarRecord.model_validate(row) for row in rows]

    @staticmethod
    def _news_records(rows):
        for row in rows:
            for field in ('symbols', 'entities'):
                if isinstance(row.get(field), str):
                    row[field] = json.loads(row[field])
                elif row.get(field) is None:
                    row[field] = []
        return [NewsArticleRecord.model_validate(row) for row in rows]

    def scan_records(self, *, start_at=None, end_at=None):
        where, args = '', ()
        if start_at is not None and end_at is not None:
            where = ' WHERE `published_at` >= %s AND `published_at` <= %s ORDER BY `published_at` DESC'
            args = (_utc_naive(start_at), _utc_naive(end_at))
        return self._news_records(self._rows(settings.ICEBERG_NEWS_TABLE, where, args))

    def news_page(self, *, page, page_size):
        table = self._table(None, settings.ICEBERG_NEWS_TABLE)
        # Preserve the total even when the requested page has no rows.
        sql = (f'SELECT totals.total, page_rows.* FROM (SELECT COUNT(*) AS total FROM {table}) totals '
               f'LEFT JOIN (SELECT * FROM {table} ORDER BY `published_at` DESC, `ingested_at` DESC, '
               '`news_id` ASC LIMIT %s OFFSET %s) page_rows ON 1 = 1 '
               'ORDER BY page_rows.`published_at` DESC, page_rows.`ingested_at` DESC, page_rows.`news_id` ASC')
        rows = self._query(sql, (page_size, (page - 1) * page_size))
        total = int(rows[0]['total']) if rows else 0
        for row in rows:
            row.pop('total', None)
            for key, value in row.items():
                if isinstance(value, datetime) and value.tzinfo is None:
                    row[key] = value.replace(tzinfo=UTC)
        return self._news_records([row for row in rows if row.get('news_id') is not None]), total

    def scan_reports(self, report_id=None):
        where, args = (' WHERE `report_id` = %s', (report_id,)) if report_id is not None else ('', ())
        return [AnnualReportRecord.model_validate(row) for row in self._rows(settings.ICEBERG_ANNUAL_REPORT_TABLE, where, args)]

    def scan_pages(self, report_id=None):
        where, args = (' WHERE `report_id` = %s', (report_id,)) if report_id is not None else ('', ())
        return [AnnualReportPageRecord.model_validate(row) for row in self._rows(settings.ICEBERG_ANNUAL_REPORT_PAGE_TABLE, where, args)]

    def scan_parse_runs(self, report_id=None):
        where, args = (' WHERE `report_id` = %s', (report_id,)) if report_id is not None else ('', ())
        return [AnnualReportParseRunRecord.model_validate(row) for row in self._rows(settings.ICEBERG_ANNUAL_REPORT_PARSE_RUN_TABLE, where, args)]

    def scan_regions(self, report_id=None, parse_run_id=None):
        clauses, args = [], []
        for name, value in (('report_id', report_id), ('parse_run_id', parse_run_id)):
            if value is not None:
                clauses.append(f'`{name}` = %s')
                args.append(value)
        where = ' WHERE ' + ' AND '.join(clauses) if clauses else ''
        rows = self._rows(settings.ICEBERG_ANNUAL_REPORT_REGION_TABLE, where, tuple(args))
        for row in rows:
            if isinstance(row.get('bbox'), str):
                row['bbox'] = json.loads(row['bbox'])
        return [AnnualReportRegionRecord.model_validate(row) for row in rows]

    def scan(self, *, symbol, trading_date, cutoff_at, history_rows=25):
        rows = self._rows(settings.ICEBERG_GOLD_METRICS_TABLE,
                          ' WHERE `symbol` = %s AND `trading_date` <= %s AND `feature_available_at` <= %s',
                          (symbol.upper(), trading_date, _utc_naive(cutoff_at)))
        records = sorted((GoldMetricRecord.model_validate(row) for row in rows), key=lambda row: row.trading_date)
        return records[-history_rows:]
