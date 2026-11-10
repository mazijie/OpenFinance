from __future__ import annotations

import json
from datetime import UTC, datetime
from importlib.metadata import version
from types import SimpleNamespace
from uuid import uuid4

import pytest
from langchain_core.messages import AIMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command
from packaging.version import Version

from backend.harness.budget import TaskAnalysisResult, fallback_task_profile
from backend.harness.graph import HarnessGraph
from backend.schemas.harness import AgentApproval, AgentEvent, AgentMessage

pytestmark = pytest.mark.skipif(
    Version(version("langgraph")) < Version("1.2.12"),
    reason="typed interrupt response schemas require LangGraph 1.2.12+",
)


@tool
def example_write_tool(symbol: str) -> str:
    """Perform a test action after approval."""
    return json.dumps({"added": symbol})


class FakeTools:
    tools = [example_write_tool]
    tool_map = {"example_write_tool": example_write_tool}


class BoundFakeClient:
    def __init__(self, owner):
        self.owner = owner
        self.calls = 0

    def with_config(self, **kwargs):
        return self

    def invoke(self, messages, config=None):
        self.calls += 1
        if self.calls == 1:
            return AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "example_write_tool",
                        "args": {"symbol": "000001.SZ"},
                        "id": "call-write",
                        "type": "tool_call",
                    }
                ],
            )
        return AIMessage(content="")


class FakeLLM:
    def __init__(self):
        self.bound = BoundFakeClient(self)
        self.client = SimpleNamespace(
            bind_tools=lambda tools, strict=False: self.bound,
            invoke=lambda messages, config=None: AIMessage(content="操作已完成。"),
        )


class FakeStore:
    def __init__(self):
        self.events = []
        self.messages = []
        self.approval = None
        self.budgets = []

    def get_turn(self, turn_id):
        return SimpleNamespace(status="running")

    def update_turn(self, *args, **kwargs):
        return None

    def update_turn_budget(self, turn_id, budget):
        self.budgets.append((turn_id, dict(budget)))

    def append_event(self, *, thread_id, turn_id, event_type, payload):
        event = AgentEvent(
            event_id=str(uuid4()),
            event_seq=len(self.events) + 1,
            thread_id=thread_id,
            turn_id=turn_id,
            event_type=event_type,
            payload=payload,
            created_at=datetime.now(UTC),
        )
        self.events.append(event)
        return event

    def append_message(self, **kwargs):
        message = AgentMessage(
            message_id=str(uuid4()),
            thread_id=kwargs["thread_id"],
            seq=len(self.messages) + 1,
            role=kwargs["role"],
            kind=kwargs["kind"],
            content=kwargs["content"],
            citations=kwargs.get("citations") or [],
            payload=kwargs.get("payload") or {},
            turn_id=kwargs.get("turn_id"),
            created_at=datetime.now(UTC),
        )
        self.messages.append(message)
        return message

    def find_turn_message(self, **kwargs):
        return next((message for message in self.messages if message.kind == kwargs["kind"]), None)

    def update_message_payload(self, message_id, payload):
        for index, message in enumerate(self.messages):
            if message.message_id == message_id:
                self.messages[index] = message.model_copy(update={"payload": payload})

    def create_approval(self, *, thread_id, turn_id, call_id, tool_name, proposed_action):
        self.approval = AgentApproval(
            approval_id=str(uuid4()),
            thread_id=thread_id,
            turn_id=turn_id,
            call_id=call_id,
            tool_name=tool_name,
            proposed_action=proposed_action,
            status="pending",
            idempotency_key="idem",
            created_at=datetime.now(UTC),
        )
        return self.approval

    def decide_approval(self, approval_id, *, approved):
        self.approval = self.approval.model_copy(
            update={"status": "approved" if approved else "rejected", "decided_at": datetime.now(UTC)}
        )
        return self.approval

    def record_tool_call(self, **kwargs):
        return None


class CountingAnalyzer:
    def __init__(self):
        self.calls = 0

    def analyze(self, question, *, run_kind):
        self.calls += 1
        return TaskAnalysisResult(
            profile=fallback_task_profile(question),
            source="rule",
            latency_ms=0,
        )


def test_write_tool_pauses_and_resumes_after_approval(monkeypatch):
    monkeypatch.setattr("backend.harness.policy.WRITE_TOOLS", {"example_write_tool"})
    store = FakeStore()
    llm = FakeLLM()
    analyzer = CountingAnalyzer()
    graph = HarnessGraph(
        store=store,
        checkpointer=InMemorySaver(),
        thread_id="00000000-0000-0000-0000-000000000001",
        turn_id="00000000-0000-0000-0000-000000000002",
        mode="standard",
        run_kind="interactive",
        as_of=datetime(2026, 10, 26, tzinfo=UTC),
        tools=FakeTools(),
        llm_factory=lambda: llm,
        dynamic_budget_enabled=True,
        task_analyzer=analyzer,
    ).graph
    config = {"configurable": {"thread_id": "00000000-0000-0000-0000-000000000001"}}
    state = {
        "messages": [{"role": "user", "content": "加入监控"}],
        "thread_id": "00000000-0000-0000-0000-000000000001",
        "turn_id": "00000000-0000-0000-0000-000000000002",
        "run_kind": "interactive",
        "mode": "standard",
        "as_of": "2026-10-26T00:00:00+00:00",
        "rolling_summary": "",
        "budget": {"max_tool_calls": 6, "max_steps": 3, "timeout_seconds": 60, "max_parallel_tools": 4},
        "steps_used": 0,
        "tool_calls_used": 0,
        "tool_rounds": 0,
        "tool_signatures": [],
        "new_tool_signatures": [],
        "evidence_seen_ids": [],
        "new_evidence_ids": [],
        "no_progress_rounds": 0,
        "last_required_coverage": 0,
        "run_started_ts": datetime.now(UTC).timestamp(),
        "deadline_ts": datetime.now(UTC).timestamp() + 60,
        "hard_deadline_ts": datetime.now(UTC).timestamp() + 360,
        "evidence": [],
        "citations": [],
        "missing": [],
    }
    interrupted = graph.invoke(state, config=config)
    assert interrupted["__interrupt__"]
    resumed = graph.invoke(Command(resume={"approved": True}), config=config)
    assert resumed["final_answer"] == "操作已完成。"
    assert store.approval.status == "approved"
    assert analyzer.calls == 1

