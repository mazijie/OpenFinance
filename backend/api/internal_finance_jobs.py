"""Internal job trigger APIs used by orchestrators."""

from __future__ import annotations


from fastapi import APIRouter, Header, HTTPException, status

from backend.schemas.jobs import BronzeToSilverRequest

from backend.api.internal_auth import _require_internal_token

router = APIRouter(prefix="/internal/jobs", tags=["internal-jobs"])


@router.post("/finance-bronze-to-silver", status_code=status.HTTP_200_OK)
def trigger_finance_bronze_to_silver(
    request: BronzeToSilverRequest,
    x_openfinance_token: str | None = Header(default=None),
) -> dict[str, int]:
    """Run finance Bronze-to-Silver normalization inside the backend runtime."""

    from backend.pipelines.finance_bronze_to_silver import FinanceBronzeToSilverPipeline

    _require_internal_token(x_openfinance_token)
    return FinanceBronzeToSilverPipeline().process_objects(request.object_paths).to_dict()


@router.post("/finance-bronze-to-silver/submit", status_code=status.HTTP_202_ACCEPTED)
def submit_finance_bronze_to_silver(request: BronzeToSilverRequest, x_openfinance_token: str | None = Header(default=None)) -> dict[str, str]:
    from backend.job_store.finance_normalization_jobs import FinanceNormalizationJobStore
    _require_internal_token(x_openfinance_token)
    return {"job_id": FinanceNormalizationJobStore().create(request.model_dump(mode="json")), "status": "queued"}


@router.get("/finance-bronze-to-silver/jobs/{job_id}")
def get_finance_bronze_to_silver_job(job_id: str, x_openfinance_token: str | None = Header(default=None)) -> dict[str, object]:
    from backend.job_store.finance_normalization_jobs import FinanceNormalizationJobStore
    _require_internal_token(x_openfinance_token)
    try: job = FinanceNormalizationJobStore().get(job_id)
    except ValueError as exc: raise HTTPException(status_code=400, detail="Invalid job id") from exc
    if job is None: raise HTTPException(status_code=404, detail="Job not found")
    return job
