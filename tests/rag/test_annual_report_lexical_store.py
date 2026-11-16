from __future__ import annotations

from types import SimpleNamespace

from backend.rag.annual_reports.annual_report_lexical_store import AnnualReportLexicalStore


class Response:
    def __init__(self, status_code: int, payload=None, text: str = "") -> None:
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

    def json(self):
        return self._payload


class ExistingIndexClient:
    def __init__(self, version: str) -> None:
        self.version = version
        self.calls = []

    def put(self, url, json):
        self.calls.append(("put", url, json))
        if len([call for call in self.calls if call[0] == "put"]) == 1:
            return Response(400, text="resource_already_exists_exception")
        return Response(200)

    def get(self, url):
        self.calls.append(("get", url))
        return Response(200, {"annual-reports": {"mappings": {"_meta": {"schema_version": self.version}}}})

    def delete(self, url):
        self.calls.append(("delete", url))
        return Response(200)


def _store(client):
    store = object.__new__(AnnualReportLexicalStore)
    store.enabled = True
    store.url = "http://opensearch:9200"
    store.index = "annual-reports"
    store.client = client
    return store


def test_old_annual_report_mapping_is_deleted_and_recreated():
    client = ExistingIndexClient("annual-report-v2")

    _store(client).ensure_index()

    assert [call[0] for call in client.calls] == ["put", "get", "delete", "put"]
    assert client.calls[-1][2]["mappings"]["_meta"]["schema_version"] == "annual-report-v3"


def test_current_annual_report_mapping_is_reused():
    client = ExistingIndexClient("annual-report-v3")

    _store(client).ensure_index()

    assert [call[0] for call in client.calls] == ["put", "get"]
