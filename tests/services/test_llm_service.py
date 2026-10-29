from __future__ import annotations

import pytest

from backend.services import llm as llm_module
from backend.services.llm import LLMService, LLMServiceError, redact_llm_error


class FakeChatOpenAI:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


def test_llm_service_uses_only_the_explicit_request_key(monkeypatch):
    monkeypatch.setattr(llm_module, "ChatOpenAI", FakeChatOpenAI)

    service = LLMService(api_key="request-key", model_name="deepseek-v4-pro")

    assert service.client.kwargs["api_key"] == "request-key"
    assert service.client.kwargs["model"] == "deepseek-v4-pro"
    assert service.client.kwargs["base_url"] == "https://api.deepseek.com"


def test_llm_service_does_not_fall_back_to_environment_configuration(monkeypatch):
    monkeypatch.setattr(llm_module, "ChatOpenAI", FakeChatOpenAI)

    with pytest.raises(LLMServiceError, match="X-LLM-API-Key"):
        LLMService()


def test_provider_error_redacts_api_key_fragments():
    message = redact_llm_error(
        "Authentication fails. Your api key: ****-abcd is invalid. Authorization: sk-secret-value"
    )

    assert "****-abcd" not in message
    assert "sk-secret-value" not in message
    assert "[redacted]" in message
