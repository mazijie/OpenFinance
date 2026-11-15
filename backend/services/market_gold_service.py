"""Business-facing market cross sections and citable stock research."""
from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Literal

from backend.lakehouse.starrocks_daily_reader import StarRocksDailyReader

TABLE = 'gold_market_cross_section'
SORT_FIELDS = ('return_1d', 'return_5d', 'return_20d', 'amount', 'avg_amount_1d', 'avg_amount_5d', 'avg_amount_20d',
               'ma_5d', 'ma_20d', 'price_ma_5d_deviation', 'price_ma_20d_deviation',
               'volatility_20d_annualized', 'avg_volume_5d', 'volume_ratio_5d')
METHOD = '指标按最近行情记录计算，成交量比使用此前五条记录；数据不足时相关指标为空，缺失交易日可能使实际跨度变长。历史数据使用当前已知版本。'


class MarketGoldService:
    def __init__(self, reader=None, evidence_service=None):
        self.reader = reader or StarRocksDailyReader(allow_latest=True)
        self.evidence_service = evidence_service

    def screen(self, *, as_of: datetime, trading_date: date | None = None,
               sort_by: str = 'return_1d', direction: Literal['asc', 'desc'] = 'desc',
               limit: int = 20, min_value: float | None = None, max_value: float | None = None,
               symbol: str | None = None):
        if sort_by not in SORT_FIELDS or direction not in ('asc', 'desc') or not 1 <= limit <= 100:
            raise ValueError('Invalid market screen parameters')
        if min_value is not None and max_value is not None and min_value > max_value:
            raise ValueError('min_value must not exceed max_value')
        table = self.reader._table(None, TABLE)
        date_sql = f'SELECT MAX(`trading_date`) AS `day` FROM {table} WHERE `trading_date` <= %s'
        date_args = [trading_date or as_of.date()]
        if symbol:
            date_sql += ' AND `symbol` = %s'
            date_args.append(symbol.strip().upper())
        date_rows = self.reader._query(date_sql, tuple(date_args))
        day = trading_date if trading_date else (date_rows[0]['day'] if date_rows else None)
        if day is None:
            return {'trading_date': None, 'items': [], 'method': METHOD, 'as_of': as_of.isoformat()}
        where = ['`trading_date` = %s']
        args = [day]
        if symbol:
            where.append('`symbol` = %s')
            args.append(symbol.strip().upper())
        else:
            where.append(f'`{sort_by}` IS NOT NULL')
        for operator, value in (('>=', min_value), ('<=', max_value)):
            if value is not None:
                where.append(f'`{sort_by}` {operator} %s')
                args.append(value)
        sql = f'SELECT * FROM {table} WHERE ' + ' AND '.join(where) + f' ORDER BY `{sort_by}` {direction.upper()}, `security_id` ASC LIMIT %s'
        args.append(limit)
        rows = self.reader._query(sql, tuple(args))
        for row in rows:
            row['data_status'] = row.get('data_status') or ('partial' if any(row.get(name) is None for name in
                                      ('close', 'volume', 'amount', 'turnover', 'return_1d', 'return_5d', 'return_20d', 'avg_amount_5d', 'avg_amount_20d'))
                                  else 'complete')
        return {'trading_date': day, 'items': rows, 'method': METHOD, 'as_of': as_of.isoformat()}

    def overview(self, *, symbol: str, query: str, as_of: datetime, limit: int = 5):
        from backend.harness.evidence import EvidenceService
        missing = []
        market = None
        try:
            result = self.screen(as_of=as_of, symbol=symbol, limit=1)
            market = result['items'][0] if result['items'] else None
            if market is None:
                missing.append('market: 该日期没有行情横截面')
        except Exception:
            missing.append('market: 行情横截面不可用')
        try:
            evidence = (self.evidence_service or EvidenceService()).search(
                query=query, sources=['news', 'annual_report'], symbol=symbol,
                as_of=as_of, limit=limit)
            documents = [item.model_dump(mode='json') for item in evidence.documents]
            missing.extend(evidence.missing)
            for source in ('news', 'annual_report'):
                if not any(item.source_type == source for item in evidence.documents):
                    missing.append(f'{source}: 未找到相关证据')
        except Exception:
            documents = []
            missing.append('news/annual_report: 证据检索不可用')
        return {'symbol': symbol.upper(), 'as_of': as_of.isoformat(), 'market': market,
                'documents': documents, 'missing': missing, 'method': METHOD}
