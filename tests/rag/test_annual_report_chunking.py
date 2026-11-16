from backend.rag.annual_reports.annual_report_chunking import build_ir_chunks


def test_structure_aware_chunks_keep_tables_atomic() -> None:
    chunks = build_ir_chunks(
        {
            "pages": [
                {
                    "page_number": 4,
                    "regions": [
                        {"region_id": "h1", "region_type": "heading", "clean_text": "第一章 经营情况"},
                        {"region_id": "p1", "region_type": "paragraph", "clean_text": "公司经营情况保持稳定。"},
                        {"region_id": "t1", "region_type": "table", "clean_text": "| 指标 | 数值 |\n| --- | --- |\n| 营收 | 10 |"},
                    ],
                }
            ]
        },
        report_id="report-1",
        parse_run_id="run-1",
    )
    assert [chunk.content_type for chunk in chunks] == ["paragraph", "table"]
    assert chunks[0].section_path == "第一章 经营情况"
    assert chunks[1].region_ids == ["t1"]

def test_large_table_repeats_headers_without_losing_rows(monkeypatch):
    from backend.config import settings
    monkeypatch.setattr(settings, 'RAG_CHUNK_SIZE', 200)
    monkeypatch.setattr(settings, 'RAG_CHUNK_OVERLAP', 40)
    rows = [['指标', '数值']] + [[f'指标{i}', '12345'] for i in range(50)]
    chunks = build_ir_chunks({'pages': [{'page_number': 1, 'regions': [{'region_id': 'table', 'region_type': 'table', 'clean_text': 'table', 'table': {'rows': rows}}]}]}, report_id='report', parse_run_id='run')
    assert len(chunks) > 1
    assert all(c.chunk_text.startswith('| 指标 | 数值 |') for c in chunks)
    data_rows = [line for c in chunks for line in c.chunk_text.splitlines()[2:]]
    assert len(data_rows) == 50
    assert len(set(data_rows)) == 50


def test_prose_chunk_can_cross_page_boundary_and_retains_all_region_ids():
    ir = {'pages': [{'page_number': 1, 'regions': [{'region_id': 'p1', 'region_type': 'paragraph', 'clean_text': '跨页段落第一部分。'}]}, {'page_number': 2, 'regions': [{'region_id': 'p2', 'region_type': 'paragraph', 'clean_text': '跨页段落第二部分。'}]}]}
    chunks = build_ir_chunks(ir, report_id='report', parse_run_id='run')
    assert len(chunks) == 1
    assert (chunks[0].page_start, chunks[0].page_end) == (1, 2)
    assert chunks[0].region_ids == ['p1', 'p2']


def test_duplicate_table_fragments_keep_rows_and_have_stable_distinct_ids(monkeypatch):
    from backend.config import settings

    monkeypatch.setattr(settings, 'RAG_CHUNK_SIZE', 50)
    monkeypatch.setattr(settings, 'RAG_CHUNK_OVERLAP', 10)
    rows = [['header'], ['x' * 45], ['x' * 45]]
    ir = {'pages': [{'page_number': 1, 'regions': [{'region_id': 'table', 'region_type': 'table', 'clean_text': 'table', 'table': {'rows': rows}}]}]}
    chunks = build_ir_chunks(ir, report_id='report', parse_run_id='run')
    again = build_ir_chunks(ir, report_id='report', parse_run_id='run')
    assert len(chunks) == 2
    assert chunks[0].chunk_text == chunks[1].chunk_text
    assert chunks[0].chunk_id != chunks[1].chunk_id
    assert [chunk.chunk_id for chunk in chunks] == [chunk.chunk_id for chunk in again]
