"""Threaded research-agent harness API with SSE streaming."""

from __future__ import annotations

import json
from functools import lru_cache
from typing import AsyncIterator

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse

from backend.config import settings
from backend.api.llm_credentials import LLMCredentials, require_llm_credentials
from backend.harness.service import AgentHarness, HarnessService
from backend.schemas.harness import (
    AgentEvent,
    AgentMessage,
    AgentThread,
    AgentThreadCreate,
    AgentThreadUpdate,
    AgentTurn,
    AgentTurnDetail,
    AgentTurnRequest,
    ApprovalDecision,
    WebDomain,
    WebDomainCreate,
    WebDomainUpdate,
)

router = APIRouter(prefix="/api/agent", tags=["agent-harness"])


@lru_cache(maxsize=1)
def get_harness_service() -> HarnessService:
    return AgentHarness()


def _require_harness() -> None:
    if not settings.HARNESS_V1_ENABLED:
        raise HTTPException(status_code=503, detail="Unified agent harness is disabled")


@router.get("/threads", response_model=list[AgentThread])
def list_threads(
    include_archived: bool = Query(default=False),
    service: HarnessService = Depends(get_harness_service),
) -> list[AgentThread]:
    _require_harness()
    return service.list_threads(include_archived=include_archived)


@router.post("/threads", response_model=AgentThread, status_code=status.HTTP_201_CREATED)
def create_thread(
    request: AgentThreadCreate,
    service: HarnessService = Depends(get_harness_service),
) -> AgentThread:
    _require_harness()
    return service.create_thread(request.title)


@router.get("/threads/{thread_id}", response_model=AgentThread)
def get_thread(
    thread_id: str,
    service: HarnessService = Depends(get_harness_service),
) -> AgentThread:
    _require_harness()
    try:
        value = service.get_thread(thread_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Invalid thread id") from exc
    if value is None:
        raise HTTPException(status_code=404, detail="Thread not found")
    return value


@router.patch("/threads/{thread_id}", response_model=AgentThread)
def update_thread(
    thread_id: str,
    request: AgentThreadUpdate,
    service: HarnessService = Depends(get_harness_service),
) -> AgentThread:
    _require_harness()
    value = service.update_thread(thread_id, title=request.title, status=request.status)
    if value is None:
        raise HTTPException(status_code=404, detail="Thread not found")
    return value


@router.delete("/threads/{thread_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_thread(
    thread_id: str,
    service: HarnessService = Depends(get_harness_service),
) -> None:
    _require_harness()
    if not service.delete_thread(thread_id):
        raise HTTPException(status_code=404, detail="Thread not found")


@router.get("/threads/{thread_id}/messages", response_model=list[AgentMessage])
def list_messages(
    thread_id: str,
    after_seq: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=200),
    service: HarnessService = Depends(get_harness_service),
) -> list[AgentMessage]:
    _require_harness()
    try:
        return service.list_messages(thread_id, after_seq=after_seq, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Invalid thread id") from exc


@router.get("/threads/{thread_id}/events", response_model=list[AgentEvent])
def list_events(
    thread_id: str,
    after_seq: int = Query(default=0, ge=0),
    service: HarnessService = Depends(get_harness_service),
) -> list[AgentEvent]:
    _require_harness()
    return service.list_events(thread_id, after_seq=after_seq)


@router.post("/threads/{thread_id}/turns")
def create_turn(
    thread_id: str,
    request: AgentTurnRequest,
    credentials: LLMCredentials = Depends(require_llm_credentials),
    service: HarnessService = Depends(get_harness_service),
) -> StreamingResponse:
    _require_harness()
    try:
        turn = service.start_turn(thread_id, request)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return StreamingResponse(
        _sse(
            service.stream_turn(
                thread_id,
                turn.turn_id,
                llm_api_key=credentials.api_key,
                model_name=credentials.model_name,
            )
        ),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/threads/{thread_id}/approvals/{approval_id}")
def resume_approval(
    thread_id: str,
    approval_id: str,
    decision: ApprovalDecision,
    credentials: LLMCredentials = Depends(require_llm_credentials),
    service: HarnessService = Depends(get_harness_service),
) -> StreamingResponse:
    _require_harness()
    try:
        approval = service.store.get_approval(approval_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Invalid approval id") from exc
    if approval is None or approval.thread_id != thread_id:
        raise HTTPException(status_code=404, detail="Approval not found")
    if approval.status != "pending":
        raise HTTPException(status_code=409, detail=f"Approval is already {approval.status}")
    return StreamingResponse(
        _sse(
            service.resume_approval(
                thread_id=thread_id,
                approval_id=approval_id,
                decision=decision,
                llm_api_key=credentials.api_key,
                model_name=credentials.model_name,
            )
        ),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/threads/{thread_id}/turns/{turn_id}/cancel", response_model=AgentTurn)
def cancel_turn(
    thread_id: str,
    turn_id: str,
    service: HarnessService = Depends(get_harness_service),
) -> AgentTurn:
    _require_harness()
    try:
        return service.cancel_turn(thread_id, turn_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/runs/{turn_id}", response_model=AgentTurnDetail)
def get_run_detail(
    turn_id: str,
    service: HarnessService = Depends(get_harness_service),
) -> AgentTurnDetail:
    _require_harness()
    try:
        return service.run_detail(turn_id)
    except (LookupError, ValueError) as exc:
        raise HTTPException(status_code=404, detail="Run not found") from exc


@router.get("/web-domains", response_model=list[WebDomain])
def list_web_domains(
    service: HarnessService = Depends(get_harness_service),
) -> list[WebDomain]:
    _require_harness()
    return service.store.list_web_domains()


@router.post("/web-domains", response_model=WebDomain, status_code=status.HTTP_201_CREATED)
def add_web_domain(
    request: WebDomainCreate,
    service: HarnessService = Depends(get_harness_service),
) -> WebDomain:
    _require_harness()
    return service.store.add_web_domain(request.host_pattern)


@router.delete("/web-domains/{host_pattern}", status_code=status.HTTP_204_NO_CONTENT)
def delete_web_domain(
    host_pattern: str,
    service: HarnessService = Depends(get_harness_service),
) -> None:
    _require_harness()
    if not service.store.delete_web_domain(host_pattern):
        raise HTTPException(status_code=404, detail="Web domain not found")


@router.patch("/web-domains/{host_pattern}", response_model=WebDomain)
def update_web_domain(
    host_pattern: str,
    request: WebDomainUpdate,
    service: HarnessService = Depends(get_harness_service),
) -> WebDomain:
    _require_harness()
    domain = service.store.set_web_domain_enabled(host_pattern, request.enabled)
    if domain is None:
        raise HTTPException(status_code=404, detail="Web domain not found")
    return domain


async def _sse(events: AsyncIterator[dict]) -> AsyncIterator[str]:
    async for event in events:
        event_type = str(event.get("event_type") or "message")
        yield f"event: {event_type}\ndata: {json.dumps(event, ensure_ascii=False, default=str)}\n\n"

