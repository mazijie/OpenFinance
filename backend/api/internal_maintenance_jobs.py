"""Internal job trigger APIs used by orchestrators."""

from __future__ import annotations


from fastapi import APIRouter, Header, status

from backend.config import settings

from backend.api.internal_auth import _require_internal_token

router = APIRouter(prefix="/internal/jobs", tags=["internal-jobs"])


@router.post("/prune-harness-checkpoints", status_code=status.HTTP_200_OK)
def prune_harness_checkpoints(
    x_openfinance_token: str | None = Header(default=None),
) -> dict[str, int]:
    """Prune checkpoints for archived research threads."""

    from backend.harness.store import HarnessStore

    _require_internal_token(x_openfinance_token)
    removed = HarnessStore().prune_archived_checkpoints(
        older_than_days=settings.HARNESS_CHECKPOINT_RETENTION_DAYS,
    )
    return {"threads": removed}
