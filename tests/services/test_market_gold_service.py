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
