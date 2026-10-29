"""High-level lifecycle service for threads, turns, approvals, and SSE events."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any, AsyncIterator, Callable
from uuid import UUID

from langchain_core.messages import AIMessage, HumanMessage
from langgraph.types import Command

from backend.config import settings
from backend.harness.context import ContextManager
from backend.harness.evidence import EvidenceService
from backend.harness.graph import HarnessGraph, TurnCancelled
from backend.harness.policy import PolicyGuard
from backend.harness.store import HarnessStore
from backend.harness.tools import ToolContext, ToolRegistry
from backend.harness.web import WebResearchService
from backend.schemas.harness import (
    AgentApproval,
    AgentEvent,
    AgentMessage,
    AgentThread,
    AgentTurn,
    AgentTurnDetail,
    AgentTurnRequest,
    ApprovalDecision,
)
from backend.services.llm import LLMService


class HarnessService:
    def __init__(
        self,
        *,
        store: HarnessStore | None = None,
        checkpointer: Any | None = None,
        llm_factory: Callable[[], LLMService] | None = None,
        budget_llm_factory: Callable[[], LLMService] | None = None,
        evidence_service: EvidenceService | None = None,
        web_service: WebResearchService | None = None,
    ) -> None:
        self.store = store or HarnessStore()
        self.store.ensure_schema()
        self.checkpointer = checkpointer
        self.llm_factory = llm_factory
        self.budget_llm_factory = budget_llm_factory
        self.web_service = web_service or WebResearchService(store=self.store)
        self.evidence_service = evidence_service or EvidenceService(web_service=self.web_service)

    def create_thread(self, title: str | None = None) -> AgentThread:
        return self.store.create_thread(title)

    def list_threads(self, *, include_archived: bool = False) -> list[AgentThread]:
        return self.store.list_threads(include_archived=include_archived)

    def get_thread(self, thread_id: str) -> AgentThread | None:
        return self.store.get_thread(thread_id)

    def update_thread(self, thread_id: str, *, title: str | None, status: str | None) -> AgentThread | None:
        return self.store.update_thread(thread_id, title=title, status=status)

    def delete_thread(self, thread_id: str) -> bool:
        return self.store.delete_thread(thread_id)

    def list_messages(self, thread_id: str, *, after_seq: int = 0, limit: int = 100) -> list[AgentMessage]:
        self.store.mark_thread_read(thread_id)
        return self.store.list_messages(thread_id, after_seq=after_seq, limit=limit)

    def start_turn(self, thread_id: str, request: AgentTurnRequest) -> AgentTurn:
        thread = self.store.get_thread(thread_id)
        if thread is None:
            raise LookupError("Thread not found")
        budget = (
            PolicyGuard.pending_budget(request.mode)
            if settings.HARNESS_DYNAMIC_BUDGET_ENABLED
            else PolicyGuard.budget(request.mode)
        )
        active = [
            turn
            for turn in self.store.list_turns(thread_id)
            if turn.status in {"queued", "running", "waiting_approval"}
        ]
        if active:
            raise RuntimeError("Thread already has an active turn")
        budget_payload = budget.to_dict()
        budget_payload["as_of"] = (request.as_of or datetime.now(UTC)).isoformat()
        if settings.HARNESS_DYNAMIC_BUDGET_ENABLED:
            budget_payload.update(
                {
                    "mode": request.mode,
                    "dynamic": True,
                    "budget_status": "pending",
                    "current_tier": "medium",
                    "initial_tier": None,
                    "task_profile": None,
                    "hard_limits": PolicyGuard.hard_budget(request.mode).to_dict(),
                    "revisions": [],
                    "stop_reason": None,
                }
            )
        try:
            turn = self.store.create_turn(
                thread_id=thread_id,
                question=request.question,
                mode=request.mode,
                budget=budget_payload,
            )
        except Exception as exc:
            if "agent_one_active_turn_idx" in str(exc) or "duplicate key" in str(exc):
                raise RuntimeError("Thread already has an active turn") from exc
            raise
        self.store.append_message(
            thread_id=thread_id,
            role="user",
            kind="text",
            content=request.question,
            payload={"mode": request.mode},
            turn_id=turn.turn_id,
        )
        self.store.update_turn(turn.turn_id, status="running")
        return self.store.get_turn(turn.turn_id) or turn

    async def stream_turn(
        self,
        thread_id: str,
        turn_id: str,
        *,
        llm_api_key: str,
        model_name: str,
    ) -> AsyncIterator[dict[str, Any]]:
        turn = self.store.get_turn(turn_id)
        thread = self.store.get_thread(thread_id)
        if turn is None or thread is None:
            raise LookupError("Thread or turn not found")
        llm_factory = self._request_llm_factory(llm_api_key=llm_api_key, model_name=model_name)
        budget_llm_factory = self._request_budget_llm_factory(api_key=llm_api_key)
        if self.checkpointer is not None:
            async for event in self._stream_with_checkpointer(
                thread,
                turn,
                self.checkpointer,
                llm_factory=llm_factory,
                budget_llm_factory=budget_llm_factory,
            ):
                yield event
            return
        async with _postgres_checkpointer() as checkpointer:
            async for event in self._stream_with_checkpointer(
                thread,
                turn,
                checkpointer,
                llm_factory=llm_factory,
                budget_llm_factory=budget_llm_factory,
            ):
                yield event

    async def _stream_with_checkpointer(
        self,
        thread: AgentThread,
        turn: AgentTurn,
        checkpointer: Any,
        *,
        llm_factory: Callable[[], LLMService],
        budget_llm_factory: Callable[[], LLMService],
    ) -> AsyncIterator[dict[str, Any]]:
        started = self.store.append_event(
            thread_id=thread.thread_id,
            turn_id=turn.turn_id,
            event_type="turn.started",
            payload={
                "turn_id": turn.turn_id,
                "mode": turn.mode,
                "budget": turn.budget,
            },
        )
        yield started.model_dump(mode="json")
        summary = self._build_rolling_summary(thread.thread_id)
        self.store.set_rolling_summary(thread.thread_id, summary)
        as_of = datetime.fromisoformat(str(turn.budget.get("as_of") or datetime.now(UTC).isoformat()))
        context = ToolContext(
            thread_id=thread.thread_id,
            turn_id=turn.turn_id,
            run_kind="interactive",
            as_of=as_of,
            store=self.store,
            evidence_service=self.evidence_service,
            web_service=self.web_service,
        )
        tools = ToolRegistry(context=context)
        graph = HarnessGraph(
            store=self.store,
            checkpointer=checkpointer,
            thread_id=thread.thread_id,
            turn_id=turn.turn_id,
            mode=turn.mode,
            run_kind="interactive",
            as_of=as_of,
            tools=tools,
            llm_factory=llm_factory,
            budget_llm_factory=budget_llm_factory,
            dynamic_budget_enabled=settings.HARNESS_DYNAMIC_BUDGET_ENABLED,
        ).graph
        config = {"configurable": {"thread_id": thread.thread_id}}
        run_started = datetime.now(UTC).timestamp()
        hard_timeout_seconds = int(
            (turn.budget.get("hard_limits") or {}).get(
                "timeout_seconds",
                turn.budget.get("timeout_seconds", settings.HARNESS_BUDGET_HARD_TIMEOUT_SECONDS),
            )
        )
        graph_input = {
            "messages": [HumanMessage(content=turn.question)],
            "thread_id": thread.thread_id,
            "turn_id": turn.turn_id,
            "run_kind": "interactive",
            "mode": turn.mode,
            "as_of": as_of.isoformat(),
            "rolling_summary": summary,
            "budget": turn.budget,
            "task_profile": turn.budget.get("task_profile") or {},
            "budget_tier": turn.budget.get("current_tier", "medium"),
            "initial_budget_tier": turn.budget.get("initial_tier"),
            "budget_reviews": [],
            "budget_stop_reason": None,
            "steps_used": 0,
            "tool_calls_used": 0,
            "tool_rounds": 0,
            "tool_signatures": [],
            "new_tool_signatures": [],
            "evidence_seen_ids": [],
            "new_evidence_ids": [],
            "no_progress_rounds": 0,
            "last_required_coverage": 0,
            "run_started_ts": run_started,
            "deadline_ts": run_started + int(turn.budget["timeout_seconds"]),
            "hard_deadline_ts": run_started + hard_timeout_seconds,
            "evidence": [],
            "citations": [],
            "missing": [],
        }
        final_answer = ""
        try:
            async for chunk in graph.astream(
                graph_input,
                config=config,
                stream_mode=["messages", "updates", "custom"],
                version="v2",
            ):
                chunk_type = chunk.get("type")
                data = chunk.get("data")
                if chunk_type == "messages":
                    message, metadata = data
                    if (
                        isinstance(message, (AIMessage,))
                        and metadata.get("langgraph_node") == "synthesize"
                        and getattr(message, "content", None)
                    ):
                        text = message.content if isinstance(message.content, str) else str(message.content)
                        if text:
                            yield {
                                "event_type": "message.delta",
                                "thread_id": thread.thread_id,
                                "turn_id": turn.turn_id,
                                "payload": {"text": text},
                            }
                elif chunk_type == "custom" and isinstance(data, dict):
                    yield data
                elif chunk_type == "updates" and isinstance(data, dict):
                    if "synthesize" in data:
                        final_answer = str((data["synthesize"] or {}).get("final_answer") or "")
            latest = self.store.get_turn(turn.turn_id)
            if latest and latest.status == "waiting_approval":
                return
            snapshot = await graph.aget_state(config)
            values = dict(snapshot.values or {})
            checkpoint_id = (
                (snapshot.config or {}).get("configurable", {}).get("checkpoint_id")
                or thread.thread_id
            )
            stats = {
                "final_answer_chars": len(final_answer),
                "status": "completed",
                "steps_used": values.get("steps_used", 0),
                "tool_calls_used": values.get("tool_calls_used", 0),
                "budget_exhausted": bool(values.get("budget_exhausted")),
                "budget_tier": values.get("budget_tier"),
                "budget_stop_reason": values.get("budget_stop_reason"),
                "budget_revision_count": len(
                    (values.get("budget") or {}).get("revisions") or []
                ),
            }
            self.store.update_turn(
                turn.turn_id,
                status="completed",
                checkpoint_id=str(checkpoint_id),
                stats=stats,
            )
            completed = self.store.append_event(
                thread_id=thread.thread_id,
                turn_id=turn.turn_id,
                event_type="turn.completed",
                payload={"turn_id": turn.turn_id, **stats},
            )
            yield completed.model_dump(mode="json")
        except TurnCancelled:
            self.store.update_turn(turn.turn_id, status="cancelled", stats={"status": "cancelled"})
            cancelled = self.store.append_event(
                thread_id=thread.thread_id,
                turn_id=turn.turn_id,
                event_type="turn.cancelled",
                payload={"turn_id": turn.turn_id},
            )
            yield cancelled.model_dump(mode="json")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.store.update_turn(turn.turn_id, status="failed", error=str(exc), stats={"status": "failed"})
            try:
                self.store.append_message(
                    thread_id=thread.thread_id,
                    role="system",
                    kind="system",
                    content=f"Agent 运行失败：{exc}",
                    turn_id=turn.turn_id,
                )
            finally:
                failed = self.store.append_event(
                    thread_id=thread.thread_id,
                    turn_id=turn.turn_id,
                    event_type="turn.failed",
                    payload={"turn_id": turn.turn_id, "error": str(exc)},
                )
                yield failed.model_dump(mode="json")

    async def resume_approval(
        self,
        *,
        thread_id: str,
        approval_id: str,
        decision: ApprovalDecision,
        llm_api_key: str,
        model_name: str,
    ) -> AsyncIterator[dict[str, Any]]:
        approval = self.store.get_approval(approval_id)
        if approval is None or approval.thread_id != thread_id:
            raise LookupError("Approval not found")
        if approval.status != "pending":
            raise RuntimeError(f"Approval is already {approval.status}")
        turn = self.store.get_turn(approval.turn_id)
        if turn is None:
            raise LookupError("Turn not found")
        llm_factory = self._request_llm_factory(llm_api_key=llm_api_key, model_name=model_name)
        budget_llm_factory = self._request_budget_llm_factory(api_key=llm_api_key)

        if self.checkpointer is not None:
            async for event in self._resume_with_checkpointer(
                thread_id,
                turn,
                decision,
                self.checkpointer,
                llm_factory=llm_factory,
                budget_llm_factory=budget_llm_factory,
            ):
                yield event
            return
        async with _postgres_checkpointer() as checkpointer:
            async for event in self._resume_with_checkpointer(
                thread_id,
                turn,
                decision,
                checkpointer,
                llm_factory=llm_factory,
                budget_llm_factory=budget_llm_factory,
            ):
                yield event

    async def _resume_with_checkpointer(
        self,
        thread_id: str,
        turn: AgentTurn,
        decision: ApprovalDecision,
        checkpointer: Any,
        *,
        llm_factory: Callable[[], LLMService],
        budget_llm_factory: Callable[[], LLMService],
    ) -> AsyncIterator[dict[str, Any]]:
        self.store.update_turn(turn.turn_id, status="running")
        thread = self.store.get_thread(thread_id)
        if thread is None:
            raise LookupError("Thread not found")
        as_of = datetime.fromisoformat(str(turn.budget.get("as_of") or datetime.now(UTC).isoformat()))
        context = ToolContext(
            thread_id=thread_id,
            turn_id=turn.turn_id,
            run_kind="interactive",
            as_of=as_of,
            store=self.store,
            evidence_service=self.evidence_service,
            web_service=self.web_service,
        )
        graph = HarnessGraph(
            store=self.store,
            checkpointer=checkpointer,
            thread_id=thread_id,
            turn_id=turn.turn_id,
            mode=turn.mode,
            run_kind="interactive",
            as_of=as_of,
            tools=ToolRegistry(context=context),
            llm_factory=llm_factory,
            budget_llm_factory=budget_llm_factory,
            dynamic_budget_enabled=settings.HARNESS_DYNAMIC_BUDGET_ENABLED,
        ).graph
        config = {"configurable": {"thread_id": thread_id}}
        final_answer = ""
        try:
            async for chunk in graph.astream(
                Command(resume=decision.model_dump(mode="json")),
                config=config,
                stream_mode=["messages", "updates", "custom"],
                version="v2",
            ):
                chunk_type = chunk.get("type")
                data = chunk.get("data")
                if chunk_type == "messages":
                    message, metadata = data
                    if (
                        isinstance(message, AIMessage)
                        and metadata.get("langgraph_node") == "synthesize"
                        and getattr(message, "content", None)
                    ):
                        text = message.content if isinstance(message.content, str) else str(message.content)
                        if text:
                            yield {
                                "event_type": "message.delta",
                                "thread_id": thread_id,
                                "turn_id": turn.turn_id,
                                "payload": {"text": text},
                            }
                elif chunk_type == "custom" and isinstance(data, dict):
                    yield data
                elif chunk_type == "updates" and isinstance(data, dict) and "synthesize" in data:
                    final_answer = str((data["synthesize"] or {}).get("final_answer") or "")
            latest = self.store.get_turn(turn.turn_id)
            if latest and latest.status == "waiting_approval":
                return
            snapshot = await graph.aget_state(config)
            values = dict(snapshot.values or {})
            checkpoint_id = (
                (snapshot.config or {}).get("configurable", {}).get("checkpoint_id")
                or thread_id
            )
            stats = {
                "final_answer_chars": len(final_answer),
                "status": "completed",
                "steps_used": values.get("steps_used", 0),
                "tool_calls_used": values.get("tool_calls_used", 0),
                "budget_exhausted": bool(values.get("budget_exhausted")),
                "budget_tier": values.get("budget_tier"),
                "budget_stop_reason": values.get("budget_stop_reason"),
                "budget_revision_count": len(
                    (values.get("budget") or {}).get("revisions") or []
                ),
            }
            self.store.update_turn(
                turn.turn_id,
                status="completed",
                checkpoint_id=str(checkpoint_id),
                stats=stats,
            )
            completed = self.store.append_event(
                thread_id=thread_id,
                turn_id=turn.turn_id,
                event_type="turn.completed",
                payload={"turn_id": turn.turn_id, **stats},
            )
            yield completed.model_dump(mode="json")
        except TurnCancelled:
            self.store.update_turn(turn.turn_id, status="cancelled", stats={"status": "cancelled"})
            yield self.store.append_event(
                thread_id=thread_id,
                turn_id=turn.turn_id,
                event_type="turn.cancelled",
                payload={"turn_id": turn.turn_id},
            ).model_dump(mode="json")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.store.update_turn(turn.turn_id, status="failed", error=str(exc), stats={"status": "failed"})
            yield self.store.append_event(
                thread_id=thread_id,
                turn_id=turn.turn_id,
                event_type="turn.failed",
                payload={"turn_id": turn.turn_id, "error": str(exc)},
            ).model_dump(mode="json")

    def cancel_turn(self, thread_id: str, turn_id: str) -> AgentTurn:
        turn = self.store.get_turn(turn_id)
        if turn is None or turn.thread_id != thread_id:
            raise LookupError("Turn not found")
        if turn.status not in {"queued", "running", "waiting_approval"}:
            return turn
        self.store.expire_turn_approvals(turn_id)
        self.store.update_turn(turn_id, status="cancelled", stats={"status": "cancelled"})
        return self.store.get_turn(turn_id) or turn

    def run_detail(self, turn_id: str) -> AgentTurnDetail:
        turn = self.store.get_turn(turn_id)
        if turn is None:
            raise LookupError("Turn not found")
        return AgentTurnDetail(
            turn=turn,
            tool_calls=self.store.list_tool_calls(turn_id),
            approvals=self.store.list_approvals(turn_id),
            events=self.store.list_events(thread_id=turn.thread_id, turn_id=turn_id),
        )

    def _build_rolling_summary(self, thread_id: str) -> str:
        messages = self.store.list_messages(thread_id, limit=200)
        turns = self.store.list_turns(thread_id)
        active_turn_id = turns[0].turn_id if turns else None
        manager = ContextManager()
        summary, _ = manager.compact(
            previous_summary=self.store.get_thread(thread_id).rolling_summary if self.store.get_thread(thread_id) else "",
            messages=[
                {
                    "role": "assistant" if message.role == "assistant" else "user",
                    "content": message.content,
                }
                for message in messages
                if message.kind == "text" and message.turn_id != active_turn_id
            ],
        )
        return summary

    def _request_llm_factory(
        self,
        *,
        llm_api_key: str,
        model_name: str,
    ) -> Callable[[], LLMService]:
        if self.llm_factory is not None:
            return self.llm_factory
        return lambda: LLMService(api_key=llm_api_key, model_name=model_name)

    def _request_budget_llm_factory(
        self,
        *,
        api_key: str,
    ) -> Callable[[], LLMService]:
        if self.budget_llm_factory is not None:
            return self.budget_llm_factory
        if self.llm_factory is not None:
            return self.llm_factory
        return lambda: LLMService(api_key=api_key, model_name=settings.DEEPSEEK_STANDARD_MODEL)

    def list_events(self, thread_id: str, *, after_seq: int = 0) -> list[AgentEvent]:
        return self.store.list_events(thread_id=thread_id, after_seq=after_seq)


class AgentHarness(HarnessService):
    """Named application harness facade used by the unified agent API."""


@asynccontextmanager
async def _postgres_checkpointer():
    try:
        from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
    except ImportError as exc:
        raise RuntimeError(
            "langgraph-checkpoint-postgres is required for persistent harness sessions"
        ) from exc
    async with AsyncPostgresSaver.from_conn_string(settings.DATABASE_URL) as saver:
        await saver.setup()
        yield saver

