from datetime import UTC, date, datetime
from types import SimpleNamespace

import pytest

from backend.services.market_gold_service import MarketGoldService


class Reader:
    def __init__(self):
        self.calls = []

    def _table(self, snapshot, name):
        return name

    def _query(self, sql, args):
        self.calls.append((sql, args))
        if 'MAX(' in sql:
            return [{'day': date(2026, 11, 10)}]
        return [{'symbol': '600519.SH', 'trading_date': date(2026, 11, 10), 'return_5d': 0.1}]


def test_screen_uses_latest_date_and_parameterized_bounds():
    reader = Reader()
    result = MarketGoldService(reader).screen(as_of=datetime(2026, 11, 12, tzinfo=UTC), sort_by='return_5d', min_value=0.05, limit=3)
    assert result['trading_date'] == date(2026, 11, 10)
    assert result['items'][0]['return_5d'] == 0.1
    assert reader.calls[1][1] == (date(2026, 11, 10), 0.05, 3)
    assert '`return_5d` >= %s' in reader.calls[1][0]
    assert 'feature_available_at' not in reader.calls[1][0]


def test_screen_rejects_invalid_order_and_bounds():
    service = MarketGoldService(Reader())
    with pytest.raises(ValueError):
        service.screen(as_of=datetime.now(UTC), sort_by='close; DROP TABLE x')
    with pytest.raises(ValueError):
        service.screen(as_of=datetime.now(UTC), min_value=2, max_value=1)


@pytest.mark.parametrize('field', ['ma_5d', 'ma_20d', 'price_ma_5d_deviation',
                                   'price_ma_20d_deviation', 'volatility_20d_annualized',
                                   'avg_volume_5d', 'volume_ratio_5d'])
def test_new_metrics_can_be_screened(field):
    reader = Reader()
    MarketGoldService(reader).screen(as_of=datetime.now(UTC), sort_by=field, min_value=0.5)
    assert f'`{field}` >= %s' in reader.calls[1][0]


def test_overview_retains_evidence_when_market_unavailable():
    class FailingReader(Reader):
        def _query(self, sql, args):
            raise RuntimeError('offline')
    class Evidence:
        def search(self, **kwargs):
            return SimpleNamespace(documents=[], missing=[])
    result = MarketGoldService(FailingReader(), Evidence()).overview(symbol='600519.sh', query='经营', as_of=datetime.now(UTC))
    assert result['market'] is None
    assert len(result['missing']) == 3


@pytest.mark.parametrize(('period', 'field'), [('day', 'return_1d'), ('five_days', 'return_5d'), ('month', 'return_20d')])
def test_ranking_uses_requested_daily_window(period, field):
    reader = Reader()
    result = MarketGoldService(reader).ranking(period=period, as_of=datetime(2026, 11, 12, tzinfo=UTC), limit=5)
    assert result['return_field'] == field
    assert f'`{field}` IS NOT NULL' in reader.calls[1][0]
    assert reader.calls[1][1][-1] == 5


def test_year_ranking_uses_one_year_baseline_with_bounded_staleness():
    reader = Reader()
    result = MarketGoldService(reader).ranking(period='year', as_of=datetime(2026, 11, 12, tzinfo=UTC), limit=5)
    sql, args = reader.calls[1]
    assert result['return_field'] == 'return_1y'
    assert args == (date(2025, 10, 27), date(2025, 11, 10), date(2026, 11, 10), 5)
    assert 'previous.`close` > 0' in sql
    assert 'ORDER BY `return_1y` DESC' in sql
    assert 'current.`close` > previous.`close`' in sql


@pytest.mark.parametrize(('side', 'operator', 'direction'), [('positive', '>', 'DESC'), ('negative', '<', 'ASC')])
@pytest.mark.parametrize(('period', 'field'), [('day', 'return_1d'), ('five_days', 'return_5d'), ('month', 'return_20d')])
def test_ranking_filters_and_sorts_by_return_sign(period, field, side, operator, direction):
    reader = Reader()
    MarketGoldService(reader).ranking(period=period, side=side, as_of=datetime(2026, 11, 12, tzinfo=UTC))
    sql, _ = reader.calls[1]
    assert f'`{field}` {operator} 0' in sql
    assert f'ORDER BY `{field}` {direction}' in sql


def test_year_negative_ranking_filters_losses_and_sorts_ascending():
    reader = Reader()
    MarketGoldService(reader).ranking(period='year', side='negative', as_of=datetime(2026, 11, 12, tzinfo=UTC))
    sql, _ = reader.calls[1]
    assert 'current.`close` < previous.`close`' in sql
    assert 'ORDER BY `return_1y` ASC' in sql


def test_ranking_rejects_invalid_side():
    with pytest.raises(ValueError):
        MarketGoldService(Reader()).ranking(period='year', side='unknown', as_of=datetime.now(UTC))
