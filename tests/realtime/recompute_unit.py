import json
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from realtime.recompute import aggregate, parse_range, read_snapshot


class RecomputeUnit(unittest.TestCase):
    def test_rejects_ambiguous_or_unaligned_range(self):
        with self.assertRaises(ValueError): parse_range("2020-01-01T00:00:01Z", "2020-01-01T00:01:00Z")
        with self.assertRaises(ValueError): parse_range("2020-01-01T00:00:00", "2020-01-01T00:01:00Z")

    def test_deduplicates_first_sample_and_excludes_invalid(self):
        start, end = parse_range("2020-01-01T00:00:00Z", "2020-01-01T00:01:00Z")
        def message(offset, round_id, price, status="valid"):
            data = {"round_id": round_id, "symbol": "A", "price": price, "status": status, "captured_at": "2020-01-01T00:00:20Z"}
            return SimpleNamespace(partition=0, offset=offset, value=json.dumps(data).encode())
        samples, identities = aggregate([message(2, "b", 30, "missing"), message(1, "a", 20), message(0, "a", 10)], start, end)
        self.assertEqual(list(samples.values()), [[10]])
        self.assertEqual(len(identities), 2)

    def test_fails_closed_when_retention_cannot_cover_range(self):
        class Consumer:
            def partitions_for_topic(self, topic): return {0}
            def assign(self, parts): self.part = parts[0]; self.pos = 8
            def beginning_offsets(self, parts): return {self.part: 8}
            def end_offsets(self, parts): return {self.part: 9}
            def seek(self, part, position): pass
            def position(self, part): return self.pos
            def poll(self, **kwargs):
                self.pos = 9
                return {self.part: [SimpleNamespace(offset=8, timestamp=1577836920000)]}
        start = datetime(2020, 1, 1, tzinfo=timezone.utc)
        with self.assertRaisesRegex(RuntimeError, "retention coverage"):
            read_snapshot(Consumer(), start)


if __name__ == "__main__": unittest.main()
