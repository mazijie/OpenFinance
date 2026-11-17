"""Send one repeatable mock snapshot; intended only for an isolated Kafka cluster."""
import json
import os
from datetime import datetime, timezone
from kafka import KafkaProducer


def main():
    if os.getenv("CONSISTENCY_ISOLATED_KAFKA") != "1":
        raise RuntimeError("Set CONSISTENCY_ISOLATED_KAFKA=1 only for a disposable Kafka cluster")
    instant = datetime.now(timezone.utc).isoformat()
    row = {"symbol": "TEST.ISOLATED", "name": "模拟数据", "price": 12.0,
           "previous_close": 10.0, "source": "mock", "source_time": instant,
           "captured_at": instant, "status": "valid", "round_id": instant,
           "event_id": instant + "/TEST.ISOLATED", "expected": 1, "failed_batches": 0}
    producer = KafkaProducer(bootstrap_servers=os.environ["KAFKA_BOOTSTRAP"], acks="all",
                             retries=3, enable_idempotence=True,
                             value_serializer=lambda value: json.dumps(value).encode())
    for _ in range(2):
        producer.send("quotes.raw", key=row["symbol"].encode(), value=row).get(timeout=30)
    producer.close()
    print(instant)


if __name__ == "__main__":
    main()
