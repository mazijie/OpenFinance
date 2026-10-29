from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

from fastapi.testclient import TestClient

from backend.api.agent import get_harness_service
from backend.main import app
from backend.schemas.harness import AgentApproval, AgentThread, AgentTurn


class FakeHarnessService:
    def __init__(self):
        now = datetime.now(UTC)
        self.thread = AgentThread(
            thread_id="00000000-0000-0000-0000-000000000001",
            title="测试线程",
            status="active",
            kind="user",
            created_at=now,
            updated_at=now,
        )
        self.turn = AgentTurn(
            turn_id="00000000-0000-0000-0000-000000000002",
            thread_id=self.thread.thread_id,
            question="分析 000001.SZ",
            mode="standard",
            status="running",
            budget={},
            created_at=now,
        )
        self.approval = AgentApproval(
            approval_id="00000000-0000-0000-0000-000000000003",
            thread_id=self.thread.thread_id,
            turn_id=self.turn.turn_id,
            call_id="call-1",
            tool_name="add_watchlist_item",
            proposed_action={"symbol": "000001.SZ"},
            status="pending",
            idempotency_key="approval-key",
            created_at=now,
        )
        self.store = SimpleNamespace(get_approval=self.get_approval)
        self.last_stream_credentials = None
        self.last_resume_credentials = None

    def list_threads(self, **kwargs):
        return [self.thread]

    def create_thread(self, title=None):
        return self.thread

    def get_thread(self, thread_id):
        return self.thread

    def update_thread(self, thread_id, **kwargs):
        return self.thread

    def delete_thread(self, thread_id):
        return True

    def list_messages(self, thread_id, **kwargs):
        return []

    def list_events(self, thread_id, **kwargs):
        return []

    def start_turn(self, thread_id, request):
        return self.turn

    async def stream_turn(self, thread_id, turn_id, *, llm_api_key, model_name):
        self.last_stream_credentials = (llm_api_key, model_name)
        yield {
            "event_type": "turn.started",
            "thread_id": thread_id,
            "turn_id": turn_id,
            "payload": {"turn_id": turn_id},
        }
        yield {
            "event_type": "turn.completed",
            "thread_id": thread_id,
            "turn_id": turn_id,
            "payload": {"turn_id": turn_id},
        }

    def get_approval(self, approval_id):
        return self.approval if approval_id == self.approval.approval_id else None

    async def resume_approval(
        self,
        *,
        thread_id,
        approval_id,
        decision,
        llm_api_key,
        model_name,
    ):
        self.last_resume_credentials = (llm_api_key, model_name)
        yield {
            "event_type": "turn.completed",
            "thread_id": thread_id,
            "turn_id": self.turn.turn_id,
            "payload": {"turn_id": self.turn.turn_id},
        }

    def cancel_turn(self, thread_id, turn_id):
        return self.turn


def test_agent_thread_and_stream_api():
    service = FakeHarnessService()
    app.dependency_overrides[get_harness_service] = lambda: service
    client = TestClient(app)
    try:
        assert client.post("/api/agent/threads", json={}).status_code == 201
        assert client.get("/api/agent/threads").status_code == 200
        with client.stream(
            "POST",
            f"/api/agent/threads/{service.thread.thread_id}/turns",
            json={"question": "分析 000001.SZ", "mode": "standard"},
            headers={"X-LLM-API-Key": "user-deepseek-key", "X-LLM-Model": "deepseek-flash"},
        ) as response:
            body = "".join(response.iter_text())
        assert response.status_code == 200
        assert "turn.started" in body
        assert "turn.completed" in body
        assert service.last_stream_credentials == ("user-deepseek-key", "deepseek-flash")
    finally:
        app.dependency_overrides.clear()


def test_agent_turn_requires_request_scoped_llm_key():
    service = FakeHarnessService()
    app.dependency_overrides[get_harness_service] = lambda: service
    client = TestClient(app)
    try:
        response = client.post(
            f"/api/agent/threads/{service.thread.thread_id}/turns",
            json={"question": "分析 000001.SZ", "mode": "standard"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 401
    assert "X-LLM-API-Key" in response.json()["detail"]
    assert service.last_stream_credentials is None


def test_agent_turn_rejects_unknown_model_before_starting():
    service = FakeHarnessService()
    app.dependency_overrides[get_harness_service] = lambda: service
    client = TestClient(app)
    try:
        response = client.post(
            f"/api/agent/threads/{service.thread.thread_id}/turns",
            json={"question": "分析 000001.SZ", "mode": "standard"},
            headers={"X-LLM-API-Key": "user-deepseek-key", "X-LLM-Model": "unknown-model"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422


def test_approval_resume_uses_the_same_request_scoped_credentials():
    service = FakeHarnessService()
    app.dependency_overrides[get_harness_service] = lambda: service
    client = TestClient(app)
    try:
        with client.stream(
            "POST",
            (
                f"/api/agent/threads/{service.thread.thread_id}"
                f"/approvals/{service.approval.approval_id}"
            ),
            json={"approved": True},
            headers={"X-LLM-API-Key": "second-user-key", "X-LLM-Model": "deepseek-v4-pro"},
        ) as response:
            body = "".join(response.iter_text())
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert "turn.completed" in body
    assert service.last_resume_credentials == ("second-user-key", "deepseek-v4-pro")


def test_compat_chat_uses_request_scoped_llm_credentials():
    service = FakeHarnessService()
    app.dependency_overrides[get_harness_service] = lambda: service
    client = TestClient(app)
    try:
        response = client.post(
            "/api/chat",
            json={"question": "分析 000001.SZ"},
            headers={"X-LLM-API-Key": "compat-user-key", "X-LLM-Model": "deepseek-flash"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json()["thread_id"] == service.thread.thread_id
    assert service.last_stream_credentials == ("compat-user-key", "deepseek-flash")


def test_compat_chat_requires_request_scoped_llm_key():
    service = FakeHarnessService()
    app.dependency_overrides[get_harness_service] = lambda: service
    client = TestClient(app)
    try:
        response = client.post("/api/chat", json={"question": "分析 000001.SZ"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 401
    assert service.last_stream_credentials is None


def test_streaming_chat_returns_progress_events_and_uses_request_credentials():
    service = FakeHarnessService()
    app.dependency_overrides[get_harness_service] = lambda: service
    client = TestClient(app)
    try:
        with client.stream(
            "POST",
            "/api/chat/stream",
            json={"question": "分析 000001.SZ"},
            headers={"X-LLM-API-Key": "stream-user-key", "X-LLM-Model": "deepseek-flash"},
        ) as response:
            body = "".join(response.iter_text())
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert "turn.started" in body
    assert "turn.completed" in body
    assert service.last_stream_credentials == ("stream-user-key", "deepseek-flash")

