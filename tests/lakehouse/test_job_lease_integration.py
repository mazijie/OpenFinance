"""Integration tests require an explicitly isolated PostgreSQL database."""
import os
from uuid import uuid4

import pytest
from backend.lakehouse.a_share_ingestion_jobs import AShareIngestionJobStore
from backend.lakehouse.finance_normalization_jobs import FinanceNormalizationJobStore
from backend.lakehouse.job_lease import JobLeaseLost


@pytest.mark.parametrize("store_type", [AShareIngestionJobStore, FinanceNormalizationJobStore])
def test_reclaimed_job_rejects_stale_attempt(store_type):
    dsn = os.getenv("CONSISTENCY_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("requires an explicitly isolated PostgreSQL database")
    store = store_type(dsn=dsn)
    job_id = store.create({"test": str(uuid4())})
    old = store.claim()
    new = store.claim(stale_after_seconds=-1)
    assert old["job_id"] == new["job_id"] == job_id
    assert new["attempts"] == old["attempts"] + 1
    with pytest.raises(JobLeaseLost): store.heartbeat(job_id, old["attempts"])
    with pytest.raises(JobLeaseLost): store.complete(job_id, {"old": True}, old["attempts"])
    with pytest.raises(JobLeaseLost): store.fail(job_id, "old", old["attempts"])
    store.heartbeat(job_id, new["attempts"])
    store.complete(job_id, {"new": True}, new["attempts"])
    assert store.get(job_id)["status"] == "success"
