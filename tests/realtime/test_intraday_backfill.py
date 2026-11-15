from datetime import date, datetime, timedelta, timezone

import pytest
from realtime.intraday_backfill import parse_minutes, trading_minutes
from realtime.tencent import CHINA

DAY = date(2026, 11, 12)
NOW = datetime(2026, 11, 12, 14, 30, tzinfo=CHINA)


def response(rows, day='20261112', code='sz000009'):
    return {'code': 0, 'data': {code: {'data': {'date': day, 'data': rows}}}}


def test_validates_date_prices_and_discards_future_lunch_and_close():
    points = parse_minutes(response(['0930 6.62 98 64876', '1130 6.1 1', '1300 6.4 1', '1429 6.46 1', '1430 6.5 1', '1500 6.5 1']), '000009.SZ', DAY, NOW)
    assert list(points.values()) == [6.62, 6.1, 6.4, 6.46]
    assert datetime(2026, 11, 12, 1, 30) in points
    for payload in [response(['0930 1'], day='20261111'), response(['0930 nan']), response(['0930 1', '0930 2']), response(['0930 -1'])]:
        with pytest.raises(ValueError):
            parse_minutes(payload, '000009.SZ', DAY, NOW)


def test_trading_minutes_exclude_lunch_and_include_session_endpoints():
    start = datetime(2026, 11, 12, 1, 30)
    end = datetime(2026, 11, 12, 7, 1)
    minutes = trading_minutes(DAY, start, end)
    assert len(minutes) == 242
    assert minutes[119] == datetime(2026, 11, 12, 3, 29)
    assert minutes[120] == datetime(2026, 11, 12, 3, 30)
    assert minutes[121] == datetime(2026, 11, 12, 5, 0)
    assert minutes[-1] == datetime(2026, 11, 12, 7, 0)
    assert trading_minutes(DAY, start, start + timedelta(minutes=3)) == minutes[:3]


def test_request_interval_is_based_on_persisted_request_time():
    from realtime.intraday_backfill import request_delay
    now = datetime(2026, 11, 12, 2, 0, tzinfo=timezone.utc)
    assert request_delay(None, now) == 0
    assert request_delay(now - timedelta(seconds=10), now) == 20
    assert request_delay(now - timedelta(seconds=30), now) == 0
    assert request_delay(now - timedelta(seconds=40), now) == 0
