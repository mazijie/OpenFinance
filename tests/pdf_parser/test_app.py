from __future__ import annotations

import hashlib
from datetime import datetime, timezone

from backend.ingestion.writers.derived_writer import DerivedObjectNotFound
from backend.pdf_parser import app as parser_app
from backend.schemas.annual_report import AnnualReportAnnouncement, AnnualReportBronzeObject


class FakeRect:
    def __init__(self, *coordinates: float) -> None:
        self.coordinates = coordinates

    def __and__(self, other: "FakeRect") -> "FakeRect":
        return FakeRect(*self.coordinates)

    @property
    def is_empty(self) -> bool:
        return False

    @property
    def width(self) -> float:
        return self.coordinates[2] - self.coordinates[0]

    @property
    def height(self) -> float:
        return self.coordinates[3] - self.coordinates[1]


class FakePage:
    rect = FakeRect(0, 0, 595, 842)

    def get_pixmap(self, **kwargs: object) -> object:
        raise RuntimeError("Invalid bandwriter header dimensions/setup")


class FakeDocument:
    def __init__(self) -> None:
        self.page = FakePage()

    def __len__(self) -> int:
        return 1

    def __iter__(self):
        return iter([self.page])

    def __getitem__(self, index: int) -> FakePage:
        return self.page

    def close(self) -> None:
        pass


class FakeBronzeWriter:
    def __init__(self, manifest: AnnualReportBronzeObject, pdf_bytes: bytes) -> None:
        self.manifest = manifest.model_dump(mode="json")
        self.pdf_bytes = pdf_bytes

    def read_json(self, path: str) -> dict[str, object]:
        return self.manifest

    def read_bytes(self, path: str) -> bytes:
        return self.pdf_bytes


class FakeDerivedWriter:
    def __init__(self) -> None:
        self.objects: dict[str, dict[str, object] | bytes] = {}

    def read_json(self, path: str) -> dict[str, object]:
        value = self.objects.get(path)
        if not isinstance(value, dict):
            raise DerivedObjectNotFound(path)
        return value

    def write_json(self, path: str, payload: dict[str, object]) -> str:
        self.objects[path] = payload
        return path

    def write_bytes(self, path: str, data: bytes, *, content_type: str) -> str:
        self.objects[path] = data
        return path

    def object_exists(self, path: str) -> bool:
        return path in self.objects


def test_region_crop_failure_keeps_ir_and_marks_page_for_review(monkeypatch) -> None:
    pdf_bytes = b"test pdf bytes"
    pdf_sha256 = hashlib.sha256(pdf_bytes).hexdigest()
    announcement = AnnualReportAnnouncement(
        announcement_id="announcement-1",
        stock_code="600000",
        exchange="SH",
        report_year=2024,
        title="2024 annual report",
        announcement_time=datetime(2025, 4, 1, tzinfo=timezone.utc),
        pdf_url="https://example.test/report.pdf",
    )
    manifest = AnnualReportBronzeObject(
        ingested_at=datetime(2025, 4, 1, tzinfo=timezone.utc),
        announcement=announcement,
        pdf_sha256=pdf_sha256,
        pdf_size=len(pdf_bytes),
        pdf_object_path="reports/report.pdf",
    )
    fake_fitz = type(
        "FakeFitz",
        (),
        {
            "open": staticmethod(lambda **kwargs: FakeDocument()),
            "Rect": staticmethod(FakeRect),
            "Matrix": staticmethod(lambda *args: object()),
        },
    )
    monkeypatch.setattr(parser_app, "fitz", fake_fitz)
    monkeypatch.setattr(
        parser_app,
        "extract_document_ir",
        lambda pdf, **kwargs: {
            "report_id": kwargs["report_id"],
            "parse_run_id": kwargs["parse_run_id"],
            "parser_version": kwargs["parser_version"],
            "pages": [
                {
                    "page_number": 1,
                    "width": 595.0,
                    "height": 842.0,
                    "page_type": "complex",
                    "extraction_method": "paddleocr_pp_structurev3",
                    "extraction_status": "ok",
                    "diagnostics": [],
                    "regions": [
                        {
                            "region_id": "region-1",
                            "region_type": "figure",
                            "bbox": [10.0, 10.0, 50.0, 50.0],
                            "reading_order": 1,
                            "text": "Revenue trend",
                            "clean_text": "Revenue trend",
                        }
                    ],
                    "text": "Revenue trend",
                    "quality": 1.0,
                }
            ],
            "section_tree": [],
            "quality": {
                "page_count": 1,
                "needs_review_pages": [],
                "average_page_quality": 1.0,
                "page_diagnostics": [],
            },
        },
    )

    derived = FakeDerivedWriter()
    service = parser_app.PdfParserService(
        bronze=FakeBronzeWriter(manifest, pdf_bytes),
        derived=derived,
    )
    result = service.parse("reports/metadata.json")

    assert result.status == "partial"
    assert result.needs_review_pages == [1]
    ir = derived.objects[result.ir_object_path]
    assert isinstance(ir, dict)
    page = ir["pages"][0]
    assert page["extraction_status"] == "needs_review"
    assert page["regions"][0]["content_ref"].endswith("region=001.json")
    assert page["diagnostics"][0]["code"] == "region_asset_render_failed"
    assert ir["quality"]["page_diagnostics"][0]["page_number"] == 1
