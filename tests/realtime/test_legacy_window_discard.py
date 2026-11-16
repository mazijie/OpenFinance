from realtime.consumer import persist


def test_legacy_window_is_discarded_without_database_write():
    class NoDatabase:
        def cursor(self):
            raise AssertionError("legacy messages must not touch the database")

    persist(NoDatabase(), "quotes.windows", {"window_basis": "legacy_processing_time"})
    persist(NoDatabase(), "quotes.windows", {})
