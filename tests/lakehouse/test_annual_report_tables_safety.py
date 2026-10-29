from types import SimpleNamespace

import pytest

from backend.lakehouse import annual_report_tables
from pyiceberg.exceptions import NamespaceAlreadyExistsError


def test_existing_incompatible_table_is_never_purged(monkeypatch):
    class Catalog:
        def create_namespace(self, namespace):
            raise NamespaceAlreadyExistsError("exists")

        def load_table(self, identifier):
            return SimpleNamespace(schema=lambda: SimpleNamespace(fields=("old",)))

        def purge_table(self, identifier):
            raise AssertionError("must not purge existing data")

    monkeypatch.setattr(annual_report_tables, "get_catalog", lambda: Catalog())
    with pytest.raises(RuntimeError, match="schema mismatch.*existing data was preserved"):
        annual_report_tables._ensure("annual_reports", SimpleNamespace(fields=("new",)))
