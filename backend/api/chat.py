"""Chat API routes."""

from __future__ import annotations

import json
import logging
from typing import Any, AsyncIterator

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse

from backend.api.agent import get_harness_service
from backend.api.llm_credentials import LLMCredentials, require_llm_credentials
from backend.harness.service import HarnessService
from backend.schemas.chat import ChatRequest, ChatResponse
from backend.schemas.harness import AgentTurnRequest

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["chat"])


@router.post("/chat", response_model=ChatResponse, status_code=status.HTTP_200_OK)
async def chat(
    request: ChatRequest,
    credentials: LLMCredentials = Depends(require_llm_credentials),
    service: HarnessService = Depends(get_harness_service),
) -> ChatResponse:
    """Handle finance analysis requests."""

    logger.info("收到聊天请求: question=%s", request.question)

    return await _chat_with_harness(request, credentials, service)


@router.post("/chat/stream")
async def chat_stream(
    request: ChatRequest,
    credentials: LLMCredentials = Depends(require_llm_credentials),
    service: HarnessService = Depends(get_harness_service),
) -> StreamingResponse:
    """Stream assistant progress and the final answer as SSE."""

    logger.info("收到流式聊天请求: question=%s", request.question)
    try:
        thread, turn = _start_harness_turn(request, service)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    return StreamingResponse(
        _sse(
            service.stream_turn(
                thread.thread_id,
                turn.turn_id,
                llm_api_key=credentials.api_key,
                model_name=credentials.model_name,
            )
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


async def _chat_with_harness(
    request: ChatRequest,
    credentials: LLMCredentials,
    service: HarnessService,
) -> ChatResponse:
    try:
        thread, turn = _start_harness_turn(request, service)
        requires_approval = False
        approvals: list[dict[str, Any]] = []
        async for event in service.stream_turn(
            thread.thread_id,
            turn.turn_id,
            llm_api_key=credentials.api_key,
            model_name=credentials.model_name,
        ):
            if event.get("event_type") == "approval.required":
                requires_approval = True
                approvals.append(dict(event.get("payload") or {}))
        messages = service.list_messages(thread.thread_id, limit=200)
        assistant = next(
            (
                message
                for message in reversed(messages)
                if message.turn_id == turn.turn_id and message.role == "assistant" and message.kind == "text"
            ),
            None,
        )
        return ChatResponse(
            question=request.question,
            analysis=assistant.content if assistant else (
                "已生成待审批操作，请前往研究对话确认。" if requires_approval else "Agent 未生成回答。"
            ),
            thread_id=thread.thread_id,
            turn_id=turn.turn_id,
            requires_approval=requires_approval,
            approvals=approvals,
        )
    except Exception as exc:
        logger.exception("Unified harness chat failed")
        raise HTTPException(status_code=500, detail=f"Agent execution failed: {exc}") from exc


def _start_harness_turn(request: ChatRequest, service: HarnessService):
    thread = service.get_thread(request.thread_id) if request.thread_id else None
    if thread is None:
        thread = service.create_thread(_title_from_question(request.question))
    turn = service.start_turn(
        thread.thread_id,
        AgentTurnRequest(question=request.question, mode=request.mode),
    )
    return thread, turn


async def _sse(events: AsyncIterator[dict[str, Any]]) -> AsyncIterator[str]:
    async for event in events:
        event_type = str(event.get("event_type") or "message")
        yield f"event: {event_type}\ndata: {json.dumps(event, ensure_ascii=False, default=str)}\n\n"


def _title_from_question(question: str) -> str:
    normalized = " ".join(question.split())
    return normalized[:40] + ("…" if len(normalized) > 40 else "")
