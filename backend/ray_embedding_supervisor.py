"""Keep the remote Ray Serve embedding application deployed."""
from __future__ import annotations

import logging
import os
import time

import httpx
from ray.job_submission import JobStatus

from backend.config import PROJECT_ROOT, settings
from backend.services.annual_report_ray_service import AnnualReportRayService

log = logging.getLogger(__name__)


def _ready() -> bool:
    base = settings.resolved_embedding_base_url.rstrip("/")
    if base.endswith("/v1"):
        base = base[:-3]
    try:
        with httpx.Client(timeout=30, trust_env=False) as client:
            response = client.get(base + "/health")
            body = response.json()
            if response.status_code != 200 or body.get("model") != settings.resolved_embedding_model or body.get("dimension") != settings.resolved_embedding_dimension:
                return False
            response = client.post(base + "/v1/embeddings", headers={"Authorization": "Bearer " + settings.EMBEDDING_API_KEY}, json={"model": settings.resolved_embedding_model, "input": ["health check"], "input_type": "document"})
            body = response.json()
            return response.status_code == 200 and body.get("model") == settings.resolved_embedding_model and len(body.get("data", [])) == 1
    except Exception:
        return False



def main() -> None:
    logging.basicConfig(level=logging.INFO)
    job_id = None
    failures = 0
    while True:
        try:
            if not settings.EMBEDDING_API_KEY:
                raise RuntimeError("EMBEDDING_API_KEY must be set for the Ray embedding service")
            if _ready():
                job_id = None
                failures = 0
            else:
                failures += 1
                if failures < 3:
                    time.sleep(30)
                    continue
                client = AnnualReportRayService()._client()
                status = client.get_job_status(job_id) if job_id else None
                if status not in {JobStatus.PENDING, JobStatus.RUNNING}:
                    token = settings.EMBEDDING_API_KEY
                    path = os.getenv("RAY_EMBEDDING_MODEL_PATH", "/models/modelscope/models/Qwen--Qwen3-Embedding-4B/snapshots/master")
                    runtime_env = {
                        "working_dir": settings.RAY_RUNTIME_WORKING_DIR.strip() or str(PROJECT_ROOT),
                        "excludes": ["/output/", "/*.tar", "/.env", "/.env.*", "/.git/", "/.cache/", "/.pytest_cache/", "/data/"],
                        "env_vars": {"RAY_EMBEDDING_TOKEN": token, "RAY_EMBEDDING_MODEL_PATH": path},
                    }
                    job_id = client.submit_job(entrypoint="python -m backend.deploy_ray_embedding", runtime_env=runtime_env, metadata={"application": "openfinance-embedding"})
                    log.info("Submitted Ray embedding deployment: %s", job_id)
        except Exception:
            log.exception("Unable to ensure Ray embedding service")
        time.sleep(30)


if __name__ == "__main__":
    main()
