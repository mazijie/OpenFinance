"""Recompute completed capture-time windows from a bounded Kafka snapshot."""
import argparse
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import math
import os

import psycopg
from kafka import KafkaConsumer, TopicPartition

UTC = timezone.utc
LOCK = "openfinance_quote_window_recompute"


def parse_instant(value):
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("time must include a UTC offset")
    return result.astimezone(UTC)


def parse_range(start, end):
    start, end = parse_instant(start), parse_instant(end)
    if start >= end or any((start.second, start.microsecond, end.second, end.microsecond)):
        raise ValueError("range must be increasing and aligned to whole minutes")
    return start, end


def read_snapshot(consumer, start, on_record=None):
    partitions = consumer.partitions_for_topic("quotes.raw")
    if not partitions:
        raise RuntimeError("quotes.raw has no available partitions")
    assigned = [TopicPartition("quotes.raw", part) for part in sorted(partitions)]
    consumer.assign(assigned)
    begins, ends = consumer.beginning_offsets(assigned), consumer.end_offsets(assigned)
    for part in assigned: consumer.seek(part, begins[part])
    records, first_timestamps = [], {}
    empty_polls = 0
    while any(consumer.position(part) < ends[part] for part in assigned):
        batches = consumer.poll(timeout_ms=1000, max_records=1000)
        if not batches:
            empty_polls += 1
            if empty_polls >= 30:
                raise RuntimeError("Kafka snapshot did not reach its captured end offsets")
            continue
        empty_polls = 0
        for part, batch in batches.items():
            for message in batch:
                if message.offset >= ends[part]: continue
                first_timestamps.setdefault(part, message.timestamp)
                if on_record is None:
                    records.append(message)
                else:
                    on_record(message)
    start_ms = int(start.timestamp() * 1000)
    for part in assigned:
        if begins[part] > 0 and (first_timestamps.get(part) is None or first_timestamps[part] > start_ms):
            raise RuntimeError(f"Kafka retention coverage cannot be established for partition {part.partition}")
    if consumer.partitions_for_topic("quotes.raw") != partitions:
        raise RuntimeError("Kafka partitions changed during recomputation")
    return records, {str(part.partition): {"begin": begins[part], "end": ends[part]} for part in assigned}


class SnapshotAggregator:
    def __init__(self, start, end):
        self.start, self.end = start, end
        self.seen, self.samples, self.in_range = {}, defaultdict(list), set()

    def accept(self, message):
        if message.value is None: return
        row = json.loads(message.value.decode("utf-8"))
        if row.get("round_id") is None: return  # heartbeat
        symbol = row.get("symbol")
        if not symbol or not row.get("captured_at"):
            raise ValueError("raw quote is missing its symbol or capture time")
        original = datetime.fromisoformat(row["captured_at"].replace("Z", "+00:00"))
        if original.tzinfo is None or original.utcoffset() != timedelta(0):
            raise ValueError("captured_at must use UTC, matching the Flink source contract")
        captured = parse_instant(row["captured_at"])
        identity = (symbol, str(row["round_id"]))
        if self.start <= captured < self.end:
            self.in_range.add((symbol, str(row["round_id"]), captured))
        if identity in self.seen:
            if self.seen[identity] != message.partition:
                raise RuntimeError("duplicate quote identity spans Kafka partitions; ordering is ambiguous")
            return
        self.seen[identity] = message.partition
        if not self.start <= captured < self.end or row.get("status") != "valid": return
        price = row.get("price")
        if isinstance(price, bool) or not isinstance(price, (int, float)) or not math.isfinite(price) or price <= 0:
            raise ValueError("invalid price in a quote marked valid")
        window = captured.replace(second=0, microsecond=0, tzinfo=None)
        self.samples[(symbol, window)].append(float(price))


def aggregate(records, start, end):
    # With a stable partition count, each symbol's Kafka key preserves its order.
    aggregator = SnapshotAggregator(start, end)
    for message in sorted(records, key=lambda item: (item.partition, item.offset)):
        aggregator.accept(message)
    return aggregator.samples, aggregator.in_range


def write_results(dsn, samples, in_range, start, end, offsets):
    with psycopg.connect(dsn) as conn, conn.transaction(), conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (LOCK,))
        for (symbol, window), values in samples.items():
            cur.execute("""INSERT INTO openfinance.realtime_quote_windows
                (symbol,window_start,window_end,sample_min,sample_max,sample_avg,sample_count,recomputed_at,window_basis)
                VALUES (%s,%s,%s,%s,%s,%s,%s,now(),'capture_time') ON CONFLICT (symbol,window_start) DO UPDATE SET
                window_end=EXCLUDED.window_end,sample_min=EXCLUDED.sample_min,sample_max=EXCLUDED.sample_max,
                sample_avg=EXCLUDED.sample_avg,sample_count=EXCLUDED.sample_count,recomputed_at=EXCLUDED.recomputed_at,
                window_basis=EXCLUDED.window_basis,recompute_stale=false""",
                (symbol, window, window + timedelta(minutes=1), min(values), max(values), sum(values)/len(values), len(values)))
        cur.execute("""SELECT symbol,round_id,captured_at FROM openfinance.realtime_quote_late
            WHERE resolved_at IS NULL AND window_start >= (%s::timestamptz AT TIME ZONE 'UTC')
            AND window_start < (%s::timestamptz AT TIME ZONE 'UTC')""", (start, end))
        resolved = [row for row in cur.fetchall()
                    if row in in_range and (row[0], row[2].astimezone(UTC).replace(second=0, microsecond=0, tzinfo=None)) in samples]
        if resolved:
            cur.executemany("""UPDATE openfinance.realtime_quote_late SET resolved_at=now()
                WHERE symbol=%s AND round_id=%s AND captured_at=%s AND resolved_at IS NULL""", resolved)
        cur.execute("""INSERT INTO openfinance.realtime_quote_recomputations
            (range_start,range_end,window_count,offsets) VALUES (%s,%s,%s,%s::jsonb)""",
            (start, end, len(samples), json.dumps(offsets)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, help="inclusive, ISO timestamp with UTC offset")
    parser.add_argument("--end", required=True, help="exclusive, ISO timestamp with UTC offset")
    args = parser.parse_args()
    start, end = parse_range(args.start, args.end)
    dsn = os.environ["DATABASE_URL"]
    with psycopg.connect(dsn, autocommit=True) as lock_conn, lock_conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(hashtextextended(%s, 0))", ("openfinance_quote_recompute_serial",))
        if not cur.fetchone()[0]:
            raise RuntimeError("another quote-window recomputation is running")
        try:
            consumer = KafkaConsumer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
                                     enable_auto_commit=False, group_id=None, auto_offset_reset="none")
            aggregator = SnapshotAggregator(start, end)
            try:
                _, offsets = read_snapshot(consumer, start, aggregator.accept)
            finally:
                consumer.close()
            samples, in_range = aggregator.samples, aggregator.in_range
            write_results(dsn, samples, in_range, start, end, offsets)
        finally:
            cur.execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", ("openfinance_quote_recompute_serial",))
    print(json.dumps({"start": start.isoformat(), "end": end.isoformat(), "windows": len(samples), "offsets": offsets}))


if __name__ == "__main__":
    main()
