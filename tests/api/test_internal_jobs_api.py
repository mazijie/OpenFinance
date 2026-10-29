from __future__ import annotations

from datetime import UTC, datetime
import sys
import types

from fastapi.testclient import TestClient

from backend.main import app


def test_internal_ingest_requires_token(monkeypatch) -> None:
    from backend.config import settings

    monkeypatch.setattr(settings, "OPENFINANCE_INTERNAL_API_TOKEN", "internal-token")
    client = TestClient(app)

    response = client.post("/internal/jobs/ingest-finance-news", json={})

    assert response.status_code == 403


def test_annual_report_bronze_manifest_listing_filters_and_verifies_pdf(monkeypatch) -> None:
    from backend.config import settings

    paths = [
        "reports/provider=cninfo/stock=600519/report_year=2024/announcement_id=1/metadata.json",
        "reports/provider=cninfo/stock=000001/report_year=2024/announcement_id=2/metadata.json",
        "reports/provider=cninfo/stock=600519/report_year=2023/announcement_id=3/metadata.json",
    ]
    payloads = {
        path: {
            "ingested_at": "2026-10-27T00:00:00+00:00",
            "announcement": {
                "announcement_id": path.split("announcement_id=")[1].split("/")[0],
                "stock_code": "600519" if "600519" in path else "000001",
                "exchange": "SH" if "600519" in path else "SZ",
                "report_year": 2023 if "report_year=2023" in path else 2024,
                "title": "Annual report",
                "announcement_time": "2025-04-02T16:00:00+00:00",
                "pdf_url": "https://example.test/report.pdf",
            },
            "pdf_sha256": "a" * 64,
            "pdf_size": 10,
            "pdf_object_path": path.removesuffix("metadata.json") + "report.pdf",
        }
        for path in paths
    }

    class FakeBronzeWriter:
        verified: list[str] = []

        def list_objects(self, *, prefix: str) -> list[str]:
            assert prefix == "reports/provider=cninfo/"
            return paths

        def read_json(self, path: str) -> dict[str, object]:
            return payloads[path]

        def object_exists(self, path: str) -> bool:
            self.verified.append(path)
            return True

    monkeypatch.setattr("backend.ingestion.writers.bronze_writer.BronzeWriter", FakeBronzeWriter)
    monkeypatch.setattr(settings, "OPENFINANCE_INTERNAL_API_TOKEN", "internal-token")
    response = TestClient(app).post(
        "/internal/jobs/annual-reports-bronze-manifests",
        headers={"X-OpenFinance-Token": "internal-token"},
        json={"years": [2024], "stocks": ["600519.SH"]},
    )

    assert response.status_code == 200
    assert response.json() == [paths[0]]
    assert FakeBronzeWriter.verified == [payloads[paths[0]]["pdf_object_path"]]


def test_internal_a_share_universe_requires_token(monkeypatch) -> None:
    from backend.config import settings

    monkeypatch.setattr(settings, "OPENFINANCE_INTERNAL_API_TOKEN", "internal-token")
    client = TestClient(app)

    response = client.get("/internal/jobs/list-a-share-symbols")

    assert response.status_code == 403


def test_internal_a_share_universe_success(monkeypatch) -> None:
    from backend.config import settings

    fake_module = types.ModuleType("backend.providers.finance")

    class FakeProvider:
        def list_a_share_symbols(self, **kwargs):
            class Result:
                status = "available"
                as_of_date = datetime(2026, 8, 20, tzinfo=UTC).date()
                symbols = ["000001.SZ", "600519.SH"]
            return Result()

    fake_module.BaoStockFinanceProvider = FakeProvider
    monkeypatch.setitem(sys.modules, "backend.providers.finance", fake_module)
    monkeypatch.setattr(settings, "OPENFINANCE_INTERNAL_API_TOKEN", "internal-token")
    client = TestClient(app)

    response = client.get(
        "/internal/jobs/list-a-share-symbols",
        headers={"X-OpenFinance-Token": "internal-token"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "status": "available",
        "as_of_date": "2026-08-20",
        "symbols": ["000001.SZ", "600519.SH"],
        "symbol_count": 2,
    }


def test_internal_a_share_coverage_success(monkeypatch) -> None:
    from backend.config import settings

    fake_module = types.ModuleType("backend.lakehouse.a_share_coverage")

    class FakeCoverageStore:
        def record_coverage(self, **kwargs):
            assert kwargs["universe_as_of_date"].isoformat() == "2026-08-20"
            assert kwargs["processed_batches"] == 2
            return True

    fake_module.AShareCoverageStore = FakeCoverageStore
    monkeypatch.setitem(sys.modules, "backend.lakehouse.a_share_coverage", fake_module)
    monkeypatch.setattr(settings, "OPENFINANCE_INTERNAL_API_TOKEN", "internal-token")
    client = TestClient(app)

    response = client.post("/internal/jobs/a-share-universe-coverage", headers={"X-OpenFinance-Token": "internal-token"}, json={
        "provider": "baostock", "adjustment_type": "qfq", "universe_as_of_date": "2026-08-20",
        "universe_symbol_count": 200, "processed_batches": 2, "written_daily_bars": 0,
        "dag_id": "openfinance_a_share_backfill_pipeline", "dag_run_id": "manual__2026-08-20",
    })

    assert response.status_code == 200
    assert response.json() == {"created": True}


def test_internal_ingest_job_success(monkeypatch) -> None:
    from backend.config import settings

    fake_module = types.ModuleType("backend.ingestion.jobs.ingest_news")

    class FakeResult:
        def to_dict(self) -> dict[str, object]:
            return {
                "category": "caijing",
                "object_path": "news/provider=juhe/test.json",
                "article_count": 3,
                "start_time": "2026-07-17T00:00:00+00:00",
                "end_time": "2026-07-17T06:00:00+00:00",
                "limit": 20,
            }

    async def fake_ingest_finance_news(**kwargs) -> FakeResult:
        assert kwargs["limit"] == 20
        assert kwargs["start_time"] == datetime(2026, 7, 17, 0, 0, tzinfo=UTC)
        return FakeResult()

    fake_module.ingest_finance_news = fake_ingest_finance_news
    monkeypatch.setitem(sys.modules, "backend.ingestion.jobs.ingest_news", fake_module)
    monkeypatch.setattr(settings, "OPENFINANCE_INTERNAL_API_TOKEN", "internal-token")
    client = TestClient(app)

    response = client.post(
        "/internal/jobs/ingest-finance-news",
        headers={"X-OpenFinance-Token": "internal-token"},
        json={
            "start_time": "2026-07-17T00:00:00+00:00",
            "end_time": "2026-07-17T06:00:00+00:00",
            "limit": 20,
        },
    )

    assert response.status_code == 200
    assert response.json()["object_path"] == "news/provider=juhe/test.json"


def test_internal_a_share_ingest_job_success(monkeypatch) -> None:
    from backend.config import settings

    fake_module = types.ModuleType("backend.ingestion.jobs.ingest_a_share_daily_bars")

    class FakeResult:
        def to_dict(self) -> dict[str, object]:
            return {
                "provider": "baostock",
                "object_path": "finance/provider=baostock/test.json",
                "symbol_count": 1,
                "daily_bar_count": 3,
                "start_date": "2026-07-17",
                "end_date": "2026-07-19",
                "adjustment_type": "qfq",
            }

    def fake_ingest_a_share_daily_bars(**kwargs) -> FakeResult:
        assert kwargs["symbols"] == ["600519.SH"]
        assert kwargs["adjustment_type"] == "qfq"
        return FakeResult()

    fake_module.ingest_a_share_daily_bars = fake_ingest_a_share_daily_bars
    monkeypatch.setitem(sys.modules, "backend.ingestion.jobs.ingest_a_share_daily_bars", fake_module)
    monkeypatch.setattr(settings, "OPENFINANCE_INTERNAL_API_TOKEN", "internal-token")
    client = TestClient(app)

    response = client.post(
        "/internal/jobs/ingest-a-share-daily-bars",
        headers={"X-OpenFinance-Token": "internal-token"},
        json={
            "symbols": ["600519.SH"],
            "start_date": "2026-07-17T00:00:00+00:00",
            "end_date": "2026-07-19T00:00:00+00:00",
            "adjustment_type": "qfq",
        },
    )

    assert response.status_code == 200
    assert response.json()["object_path"] == "finance/provider=baostock/test.json"


def test_internal_bronze_to_silver_job_success(monkeypatch) -> None:
    from backend.config import settings

    fake_module = types.ModuleType("backend.pipelines.news_bronze_to_silver")

    class FakeStats:
        def to_dict(self) -> dict[str, int]:
            return {
                "read_count": 3,
                "filtered_count": 0,
                "duplicate_count": 0,
                "failed_count": 0,
                "written_count": 3,
                "processed_objects": 1,
            }

    class FakePipeline:
        def run(self, *, object_paths: list[str]):
            assert object_paths == ["news/provider=juhe/test.json"]
            return FakeStats()

    fake_module.NewsBronzeToSilverPipeline = FakePipeline
    monkeypatch.setitem(sys.modules, "backend.pipelines.news_bronze_to_silver", fake_module)
    monkeypatch.setattr(settings, "OPENFINANCE_INTERNAL_API_TOKEN", "internal-token")
    client = TestClient(app)

    response = client.post(
        "/internal/jobs/news-bronze-to-silver",
        headers={"X-OpenFinance-Token": "internal-token"},
        json={"object_paths": ["news/provider=juhe/test.json"]},
    )

    assert response.status_code == 200
    assert response.json()["processed_objects"] == 1


def test_submit_and_get_finance_normalization_job(monkeypatch) -> None:
    from backend.config import settings

    fake_module = types.ModuleType("backend.lakehouse.finance_normalization_jobs")

    class FakeStore:
        created_payload: dict[str, object] | None = None

        def create(self, payload: dict[str, object]) -> str:
            self.created_payload = payload
            return "0c7f5f3d-6b25-4cb7-a5d4-796518ce53ce"

        def get(self, job_id: str) -> dict[str, object] | None:
            assert job_id == "0c7f5f3d-6b25-4cb7-a5d4-796518ce53ce"
            return {"job_id": job_id, "status": "success", "result": {"processed_objects": 1}}

    fake_module.FinanceNormalizationJobStore = FakeStore
    monkeypatch.setitem(sys.modules, "backend.lakehouse.finance_normalization_jobs", fake_module)
    monkeypatch.setattr(settings, "OPENFINANCE_INTERNAL_API_TOKEN", "internal-token")
    client = TestClient(app)

    submitted = client.post(
        "/internal/jobs/finance-bronze-to-silver/submit",
        headers={"X-OpenFinance-Token": "internal-token"},
        json={"object_paths": ["finance/provider=baostock/test.json"]},
    )
    status = client.get(
        "/internal/jobs/finance-bronze-to-silver/jobs/0c7f5f3d-6b25-4cb7-a5d4-796518ce53ce",
        headers={"X-OpenFinance-Token": "internal-token"},
    )

    assert submitted.status_code == 202
    assert submitted.json()["status"] == "queued"
    assert status.status_code == 200
    assert status.json()["result"] == {"processed_objects": 1}


def test_internal_rag_index_job_success(monkeypatch) -> None:
    from backend.config import settings

    fake_module = types.ModuleType("backend.rag.indexer")

    class FakeStats:
        def model_dump(self) -> dict[str, object]:
            return {
                "scanned_articles": 2,
                "skipped_articles": 1,
                "skipped_existing": 0,
                "chunk_count": 3,
                "written_chunks": 3,
                "embeddings_failed": 0,
                "errors": 0,
                "details": {},
            }

    class FakeIndexer:
        def run(self):
            return FakeStats()

    fake_module.NewsRAGIndexer = FakeIndexer
    monkeypatch.setitem(sys.modules, "backend.rag.indexer", fake_module)
    monkeypatch.setattr(settings, "OPENFINANCE_INTERNAL_API_TOKEN", "internal-token")
    client = TestClient(app)

    response = client.post(
        "/internal/jobs/news-rag-index",
        headers={"X-OpenFinance-Token": "internal-token"},
        json={"rebuild_all": False},
    )

    assert response.status_code == 200
    assert response.json()["written_chunks"] == 3
