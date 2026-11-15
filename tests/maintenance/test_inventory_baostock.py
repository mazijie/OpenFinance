import pytest
from backend.maintenance.inventory_baostock import classify_bronze


def test_classifies_mixed_and_preserves_unknown_fields():
    result = classify_bronze({'raw_response': {'trading_calendar': [1], 'daily_bars': [2, 3], 'other': [4]}})
    assert result == {'kind': 'mixed', 'counts': {'trading_calendar': 1, 'security_master': 0, 'daily_bars': 2}, 'other_nonempty_fields': ['other']}


def test_rejects_invalid_payload():
    with pytest.raises(ValueError):
        classify_bronze({'raw_response': {'daily_bars': 'invalid'}})


def test_calendar_copy_retains_calendar_and_removes_market():
    import json
    from backend.maintenance.preserve_baostock_calendars import calendar_copy
    source = {'provider': 'baostock', 'payload_hash': 'old', 'raw_response': {'trading_calendar': [{'date': '2026-11-06'}], 'security_master': [1], 'daily_bars': [2]}}
    path, content = calendar_copy('source', source)
    result = json.loads(content)
    assert path.startswith('finance/provider=baostock/calendar-preserved/')
    assert result['raw_response'] == {'trading_calendar': [{'date': '2026-11-06'}], 'security_master': [], 'daily_bars': []}
    assert result['payload_hash'] is None
    assert result['request_params'] == {'kind': 'trading_calendar'}
    assert result['source_record_id'] is None
    assert source['raw_response']['daily_bars'] == [2]
    assert calendar_copy('source', source) == (path, content)
