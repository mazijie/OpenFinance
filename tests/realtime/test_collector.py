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


def test_next_round_can_publish_while_previous_round_is_waiting(monkeypatch):
    from datetime import datetime, timedelta
    from threading import Event

    import realtime.collector as collector

    first_started = Event()
    release_first = Event()
    published = []
    states = []
    first = datetime(2026, 11, 2, 9, 30, tzinfo=collector.CHINA)
    second = first + timedelta(minutes=1)

    class Future:
        def get(self, timeout):
            return None

    class Producer:
        def send(self, topic, *, key, value):
            published.append(value["round_id"])
            return Future()

    def collect(symbols, minute, fetch):
        if minute == first:
            first_started.set()
            assert release_first.wait(5)
        return [{"symbol": "600519.SH", "round_id": minute.isoformat(), "status": "valid"}]

    monkeypatch.setenv("DATABASE_URL", "unused")
    monkeypatch.setattr(collector, "record_round", lambda dsn, minute, status, **kwargs: states.append((minute, status)))
    monkeypatch.setattr(collector, "collect_round", collect)
    monkeypatch.setattr(collector, "persist_quote_metrics", lambda rows: None)
    producer = Producer()
    first_thread = collector.start_quote_round(first, "mock", producer, lambda *args: None)
    try:
        assert first_started.wait(5)
        second_thread = collector.start_quote_round(second, "mock", producer, lambda *args: None)
        second_thread.join(5)
        assert not second_thread.is_alive()
        assert published == [second.isoformat()]
        assert (second, "published") in states
    finally:
        release_first.set()
        first_thread.join(5)
    assert not first_thread.is_alive()
    assert published == [second.isoformat(), first.isoformat()]
    assert (first, "published") in states


def test_index_collection_does_not_overlap_between_rounds(monkeypatch):
    from datetime import datetime, timedelta
    from threading import Event

    import realtime.collector as collector

    started = Event()
    release = Event()
    calls = []

    def collect_indices(minute):
        calls.append(minute)
        started.set()
        release.wait(5)

    monkeypatch.setattr(collector, "collect_indices", collect_indices)
    indices = collector.IndexCollector()
    first = datetime(2026, 11, 2, 9, 30, tzinfo=collector.CHINA)
    indices.start(first)
    try:
        assert started.wait(5)
        indices.start(first + timedelta(minutes=1))
        assert calls == [first]
    finally:
        release.set()
        indices.thread.join(5)
    assert not indices.thread.is_alive()
