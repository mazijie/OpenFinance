"""Chat API routes."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, status

from backend.agent.workflow import FinanceAgent
from backend.schemas.chat import ChatRequest, ChatResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["chat"])

finance_agent = FinanceAgent()


@router.post("/chat", response_model=ChatResponse, status_code=status.HTTP_200_OK)
def chat(request: ChatRequest) -> ChatResponse:
    """Handle finance analysis requests."""

    logger.info("收到聊天请求: question=%s", request.question)

    try:
        result = finance_agent.run(question=request.question)
    except Exception as exc:
        logger.exception("Agent执行失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Agent execution failed: {exc}",
        ) from exc

    return ChatResponse(
        question=result.get("question", request.question),
        symbol=result.get("symbol"),
        analysis=result.get("answer", ""),
        tool_results=result.get("tool_results"),
    )
