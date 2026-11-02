from realtime.collector import normalize


def test_normalize_filters_invalid_prices_and_preserves_unknown_source_time():
    rows = [{"代码": "000001", "名称": "平安银行", "最新价": 10.2},
            {"代码": "600519", "最新价": "-"},
            {"代码": "600000", "最新价": float("nan")},
            {"代码": "000002", "最新价": 4}]
    result = normalize(rows, {"000001", "600519", "600000"}, "2026-01-01T00:00:00+00:00")
    assert len(result) == 1
    assert result[0]["symbol"] == "000001"
    assert result[0]["price"] == 10.2
    assert result[0]["source_time"] is None


def test_idle_heartbeat_targets_all_partitions():
    from realtime.collector import send_watermark_once
    from datetime import datetime, timezone
    class Future:
        def get(self, timeout): return None
    class Producer:
        def __init__(self): self.records = []
        def partitions_for(self, topic): return {0, 1, 2}
        def send(self, topic, **kwargs): self.records.append(kwargs); return Future()
    producer = Producer()
    send_watermark_once(producer, datetime(2020, 1, 1, tzinfo=timezone.utc))
    assert [item["partition"] for item in producer.records] == [0, 1, 2]
    assert all(item["value"]["captured_at"] == "2020-01-01T00:00:00+00:00" for item in producer.records)
