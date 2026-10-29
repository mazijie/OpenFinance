import pytest
import fitz

from backend.pipelines.annual_report_parser import AnnualReportParseError, AnnualReportParserUnavailable, _normalize_mineru_result


def test_mineru_maps_positions_tables_and_order():
    payload = {"schema": "docvortex.middle", "schema_version": "2.0", "pages": [{"blocks": [
        {"type": "header", "bbox": [0, 0, .5, .05], "content": [{"content": "header"}]},
        {"type": "paragraph_title", "bbox": [.1, .2, .5, .3], "content": [{"content": "第一节"}]},
        {"type": "table", "bbox": [.1, .4, .9, .8], "content": [{"type": "table_body", "content": "<table><tr><td colspan=2>收入</td></tr><tr><td>2024</td><td>100</td></tr></table>"}]},
    ]}]}
    result = _normalize_mineru_result(payload, 4, fitz.Rect(10, 20, 210, 120))
    assert [r["region_type"] for r in result] == ["heading", "table"]
    assert result[0]["bbox"] == [30, 40, 110, 50]
    assert result[1]["table"]["cells"][0]["colspan"] == 2
    assert "2024" in result[1]["text"]
    assert result[0]["confidence"] is None


def test_mineru_rejects_invalid_geometry_and_contract():
    with pytest.raises(AnnualReportParseError, match="contract"):
        _normalize_mineru_result({}, 1, fitz.Rect(0, 0, 100, 100))
    payload = {"schema": "docvortex.middle", "schema_version": "2.0", "pages": [{"blocks": [{"type": "text", "bbox": [0, 0, 100, 100], "content": "x"}]}]}
    with pytest.raises(AnnualReportParseError, match="bounding box"):
        _normalize_mineru_result(payload, 1, fitz.Rect(0, 0, 100, 100))


def test_missing_mineru_fails_report_instead_of_marking_every_page(monkeypatch):
    import builtins
    from backend.pipelines import annual_report_parser as parser

    real_import = builtins.__import__
    def guarded_import(name, *args, **kwargs):
        if name == "mineru.parser":
            raise ImportError("not installed")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    with pytest.raises(AnnualReportParserUnavailable, match="not installed"):
        parser._extract_mineru_regions(None, 1)
