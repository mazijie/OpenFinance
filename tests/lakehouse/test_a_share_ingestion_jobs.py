from __future__ import annotations

from backend.lakehouse.a_share_ingestion_jobs import AShareIngestionJobStore


def test_payload_fingerprint_is_stable_across_key_order() -> None:
    first = {
        "symbols": ["000001.SZ", "600519.SH"],
        "start_date": "2026-09-01",
        "end_date": "2026-09-03",
        "adjustment_type": "qfq",
    }
    second = {
        "adjustment_type": "qfq",
        "end_date": "2026-09-03",
        "start_date": "2026-09-01",
        "symbols": ["000001.SZ", "600519.SH"],
    }

    assert AShareIngestionJobStore._payload_fingerprint(first) == AShareIngestionJobStore._payload_fingerprint(second)
