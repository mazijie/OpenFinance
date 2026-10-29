"""Run one read-only MinerU page check on the Ray GPU node."""
import json
import os

import ray

from backend.config import settings


@ray.remote(num_cpus=4, num_gpus=1, resources={"annual_report_gpu": 1})
def check(path: str, page_number: int):
    import fitz
    from backend.ingestion.writers.bronze_writer import BronzeWriter
    from backend.pipelines.annual_report_parser import _extract_mineru_regions

    writer = BronzeWriter()
    manifest = writer.read_json(path)
    doc = fitz.open(stream=writer.read_bytes(manifest["pdf_object_path"]), filetype="pdf")
    try:
        regions = _extract_mineru_regions(doc[page_number - 1], page_number)
        return {"page": page_number, "regions": len(regions),
                "tables": sum(r["region_type"] == "table" for r in regions),
                "title_found": any("第一节" in r["text"] for r in regions),
                "company_found": any("海南椰岛" in r["text"] for r in regions)}
    finally:
        doc.close()


if __name__ == "__main__":
    ray.init(address=settings.RAY_ADDRESS)
    try:
        print(json.dumps(ray.get(check.remote(os.environ["MINERU_SMOKE_MANIFEST"], 4)), ensure_ascii=False), flush=True)
    finally:
        ray.shutdown()
