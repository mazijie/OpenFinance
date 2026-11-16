from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

from langchain_core.messages import AIMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver

from backend.harness.graph import HarnessGraph
from backend.schemas.harness import AgentEvent, AgentMessage


@tool
def research_search(query: str, symbol: str | None = None) -> str:
    """Search evidence."""
    return json.dumps(
        {
            "documents": [
                {
                    "evidence_id": "e1",
                    "source_type": "market",
                    "source_id": "000001.SZ",
                    "title": "行情证据",
                    "text": "区间收益 10%。",
                    "published_at": "2026-10-25T00:00:00Z",
                    "locator": "2026-10-25",
                }
            ],
            "missing": [],
            "sources": ["market"],
            "as_of": "2026-10-25T00:00:00Z",
        }
    )


class FakeTools:
    tools = [research_search]
    tool_map = {"research_search": research_search}


class BoundFakeClient:
    def __init__(self, owner):
        self.owner = owner

    def with_config(self, **kwargs):
        return self

    def invoke(self, messages, config=None):
        self.owner.planner_calls += 1
        if self.owner.planner_calls == 1:
            return AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "research_search",
                        "args": {"query": "最近行情", "symbol": "000001.SZ"},
                        "id": "call-1",
                        "type": "tool_call",
                    }
                ],
            )
        return AIMessage(content="")


class FakeClient:
    def __init__(self):
        self.planner_calls = 0

    def bind_tools(self, tools, strict=False):
        return BoundFakeClient(self)

    def invoke(self, messages, config=None):
        return AIMessage(content="区间收益为 10%。[1]")


class FakeLLM:
    def __init__(self):
        self.client = FakeClient()


class FakeStore:
    def __init__(self):
        self.events = []
        self.messages = []
        self.tool_calls = []
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

    def record_tool_call(self, **kwargs):
        self.tool_calls.append(kwargs)

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


def test_graph_runs_tool_loop_and_generates_citation():
    store = FakeStore()
    llm = FakeLLM()
    graph = HarnessGraph(
        store=store,
        checkpointer=InMemorySaver(),
        thread_id="00000000-0000-0000-0000-000000000001",
        turn_id="00000000-0000-0000-0000-000000000002",
        mode="standard",
        run_kind="interactive",
        as_of=datetime(2026, 10, 25, tzinfo=UTC),
        tools=FakeTools(),
        llm_factory=lambda: llm,
    ).graph
    result = graph.invoke(
        {
            "messages": [{"role": "user", "content": "分析 000001.SZ"}],
            "thread_id": "00000000-0000-0000-0000-000000000001",
            "turn_id": "00000000-0000-0000-0000-000000000002",
            "run_kind": "interactive",
            "mode": "standard",
            "as_of": "2026-10-25T00:00:00+00:00",
            "rolling_summary": "",
            "budget": {"max_tool_calls": 6, "max_steps": 3, "timeout_seconds": 60, "max_parallel_tools": 4},
            "steps_used": 0,
            "tool_calls_used": 0,
            "deadline_ts": datetime.now(UTC).timestamp() + 60,
            "evidence": [],
            "citations": [],
            "missing": [],
        },
        config={"configurable": {"thread_id": "00000000-0000-0000-0000-000000000001"}},
    )

    assert result["final_answer"] == "区间收益为 10%。[1]"
    assert result["citations"][0]["citation_id"] == 1
    assert store.messages[0].kind == "text"


def test_budget_exhaustion_routes_to_synthesis_instead_of_reusing_tool_calls():
    graph = HarnessGraph(
        store=FakeStore(),
        checkpointer=InMemorySaver(),
        thread_id="00000000-0000-0000-0000-000000000001",
        turn_id="00000000-0000-0000-0000-000000000002",
        mode="standard",
        run_kind="interactive",
        as_of=datetime(2026, 10, 25, tzinfo=UTC),
        tools=FakeTools(),
        llm_factory=lambda: FakeLLM(),
    )

    route = graph._route_after_agent(
        {
            "budget_exhausted": True,
            "messages": [
                AIMessage(
                    content="",
                    tool_calls=[
                        {
                            "name": "research_search",
                            "args": {"query": "最近行情"},
                            "id": "call-1",
                            "type": "tool_call",
                        }
                    ],
                )
            ],
        }
    )

    assert route == "synthesize"


def test_dynamic_budget_analyzes_task_and_stops_after_required_evidence():
    store = FakeStore()
    now = datetime.now(UTC).timestamp()
    graph = HarnessGraph(
        store=store,
        checkpointer=InMemorySaver(),
        thread_id="00000000-0000-0000-0000-000000000001",
        turn_id="00000000-0000-0000-0000-000000000002",
        mode="standard",
        run_kind="interactive",
        as_of=datetime(2026, 10, 25, tzinfo=UTC),
        tools=FakeTools(),
        llm_factory=lambda: FakeLLM(),
    ).graph

    result = graph.invoke(
        {
            "messages": [{"role": "user", "content": "查询 000001.SZ 最近一个月行情"}],
            "thread_id": "00000000-0000-0000-0000-000000000001",
            "turn_id": "00000000-0000-0000-0000-000000000002",
            "run_kind": "interactive",
            "mode": "standard",
            "as_of": "2026-10-25T00:00:00+00:00",
            "rolling_summary": "",
            "budget": {},
            "steps_used": 0,
            "tool_calls_used": 0,
            "tool_rounds": 0,
            "tool_signatures": [],
            "new_tool_signatures": [],
            "evidence_seen_ids": [],
            "new_evidence_ids": [],
            "no_progress_rounds": 0,
            "last_required_coverage": 0,
            "run_started_ts": now,
            "deadline_ts": now + 45,
            "hard_deadline_ts": now + 360,
            "evidence": [],
            "citations": [],
            "missing": [],
        },
        config={"configurable": {"thread_id": "00000000-0000-0000-0000-000000000001"}},
    )

    assert result["budget_tier"] == "simple"
    assert result["budget_stop_reason"] == "evidence_complete"
    assert result["task_profile"]["task_type"] == "query"
    assert any(event.event_type == "task.analyzed" for event in store.events)
    assert any(event.event_type == "budget.reviewed" for event in store.events)
    assert any(event.event_type == "budget.stopped" for event in store.events)
    assert store.budgets[-1][1]["stop_reason"] == "evidence_complete"

