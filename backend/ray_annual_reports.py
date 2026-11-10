"""Ray entrypoint for CPU/GPU annual-report processing.

The Ray driver keeps only manifest paths and small status payloads in memory.
PDF bytes and parser artifacts remain in MinIO, while each report reuses the
existing deterministic Derived checkpoints and Silver idempotency rules.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from backend.config import settings


def _chunks(values: list[str], size: int) -> Iterable[list[str]]:
    for start in range(0, len(values), max(1, size)):
        yield values[start : start + max(1, size)]


def _report_identity(manifest_path: str) -> tuple[str, str]:
    from backend.ingestion.writers.bronze_writer import BronzeWriter
    from backend.schemas.annual_report import AnnualReportBronzeObject

    manifest = AnnualReportBronzeObject.model_validate(BronzeWriter().read_json(manifest_path))
    report_id = str(uuid5(NAMESPACE_URL, f"cninfo:{manifest.announcement.announcement_id}:{manifest.pdf_sha256}"))
    return report_id, manifest.pdf_sha256


def _parse_manifest_impl(manifest_path: str, *, force: bool) -> dict[str, Any]:
    from backend.pdf_parser.app import PdfParserService
    from backend.pipelines.annual_report_pipeline_state import AnnualReportPipelineStateStore

    report_id, pdf_sha256 = _report_identity(manifest_path)
    state = AnnualReportPipelineStateStore()
    state.ensure_schema()
    lease = state.claim_stage(
        report_id=report_id,
        pdf_sha256=pdf_sha256,
        stage="parse",
        stage_version=settings.PDF_PARSER_VERSION,
    )
    try:
        result = PdfParserService().parse(manifest_path, force=force)
        payload = result.model_dump(mode="json")
        state.complete_stage(lease, output_ref=str(payload["ir_object_path"]))
        return payload
    except Exception as exc:
        try:
            state.fail_stage(lease, exc)
        except Exception:
            pass
        raise


def _preflight_manifest_impl(manifest_path: str) -> dict[str, Any]:
    """Classify a Bronze PDF without loading Paddle or reserving a GPU."""

    import fitz

    from backend.ingestion.writers.bronze_writer import BronzeWriter
    from backend.pipelines.annual_report_parser import _classify_page, _extract_native_regions
    from backend.schemas.annual_report import AnnualReportBronzeObject

    manifest = AnnualReportBronzeObject.model_validate(BronzeWriter().read_json(manifest_path))
    pdf_bytes = BronzeWriter().read_bytes(manifest.pdf_object_path)
    document = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        page_types: list[str] = []
        for page_number, page in enumerate(document, start=1):
            regions, metrics = _extract_native_regions(page, page_number)
            page_types.append(_classify_page(metrics, regions))
        return {
            "manifest_path": manifest_path,
            "needs_gpu": any(page_type != "text" for page_type in page_types),
            "page_types": page_types,
        }
    finally:
        document.close()


def _materialize_silver_impl(parse_result: dict[str, Any]) -> dict[str, Any]:
    from backend.pipelines.annual_reports_ir_to_silver import AnnualReportsIRToSilverPipeline

    # Propagate the original exception to the Ray job log instead of reducing
    # it to failed_count=1, which hides the reason from operators.
    return AnnualReportsIRToSilverPipeline().process_result(parse_result).to_dict()


def run_job(manifest_paths: list[str], *, force: bool = False) -> dict[str, Any]:
    """Run a bounded, idempotent annual-report batch on the Ray cluster."""

    import ray

    ray.init(address=settings.RAY_ADDRESS, namespace="openfinance-annual-reports", ignore_reinit_error=True)

    @ray.remote
    def preflight(manifest_path: str) -> dict[str, Any]:
        return _preflight_manifest_impl(manifest_path)

    @ray.remote
    def parse_cpu(manifest_path: str, force_value: bool) -> dict[str, Any]:
        return _parse_manifest_impl(manifest_path, force=force_value)

    @ray.remote(max_restarts=0, max_task_retries=0)
    class GpuParser:
        def parse(self, manifest_path: str, force_value: bool) -> dict[str, Any]:
            return _parse_manifest_impl(manifest_path, force=force_value)

    @ray.remote
    def materialize(parse_result: dict[str, Any]) -> dict[str, Any]:
        return _materialize_silver_impl(parse_result)

    cpu_label = settings.RAY_CPU_RESOURCE_LABEL
    gpu_label = settings.RAY_GPU_RESOURCE_LABEL
    max_in_flight = max(1, settings.RAY_MAX_IN_FLIGHT_REPORTS)
    parse_results: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    silver_stats = {"processed_runs": 0, "written_reports": 0, "written_pages": 0, "written_regions": 0, "written_sections": 0, "written_chunks": 0, "failed_count": 0}
    gpu_actor = None

    try:
        for batch in _chunks(list(dict.fromkeys(manifest_paths)), max_in_flight):
            preflight_refs = [
                preflight.options(num_cpus=1, resources={cpu_label: 0.001}).remote(path)
                for path in batch
            ]
            classifications: list[dict[str, Any]] = []
            for path, ref in zip(batch, preflight_refs, strict=True):
                try:
                    classifications.append(ray.get(ref))
                except Exception as exc:
                    failures.append({"manifest_path": path, "stage": "preflight", "error": str(exc)[:1000]})

            needs_gpu = {item["manifest_path"] for item in classifications if item.get("needs_gpu")}
            if needs_gpu and gpu_actor is None:
                cluster_resources = ray.cluster_resources()
                if float(cluster_resources.get("GPU", 0)) < 1 or float(cluster_resources.get(gpu_label, 0)) < 1:
                    raise RuntimeError(
                        f"Ray GPU resource unavailable: expected GPU=1 and {gpu_label}=1, resources={cluster_resources}"
                    )
                gpu_actor = GpuParser.options(
                    num_cpus=settings.RAY_GPU_TASK_CPUS,
                    num_gpus=1,
                    resources={gpu_label: 1},
                ).remote()

            parse_refs: list[tuple[str, Any]] = []
            for item in classifications:
                path = str(item["manifest_path"])
                if path in needs_gpu:
                    parse_refs.append((path, gpu_actor.parse.remote(path, force)))
                else:
                    parse_refs.append((
                        path,
                        parse_cpu.options(
                            num_cpus=settings.RAY_CPU_TASK_CPUS,
                            resources={cpu_label: 0.001},
                        ).remote(path, force),
                    ))

            silver_refs: list[tuple[str, Any]] = []
            for path, ref in parse_refs:
                try:
                    result = ray.get(ref)
                    parse_results.append(result)
                    silver_refs.append((path, materialize.options(num_cpus=settings.RAY_CPU_TASK_CPUS, resources={cpu_label: 0.001}).remote(result)))
                except Exception as exc:
                    failures.append({"manifest_path": path, "stage": "parse", "error": str(exc)[:1000]})

            if gpu_actor is not None:
                ray.kill(gpu_actor)
                gpu_actor = None

            for path, ref in silver_refs:
                try:
                    stats = ray.get(ref)
                    for key in silver_stats:
                        silver_stats[key] += int(stats.get(key, 0))
                except Exception as exc:
                    failures.append({"manifest_path": path, "stage": "silver", "error": str(exc)[:1000]})
                    silver_stats["failed_count"] += 1
    finally:
        if gpu_actor is not None:
            ray.kill(gpu_actor)
        ray.shutdown()

    return {
        "status": "partial" if failures else "succeeded",
        "processed_reports": len(parse_results),
        "failed_reports": len(failures),
        "failures": failures,
        "silver": silver_stats,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run annual-report processing on Ray")
    parser.add_argument("--manifest-paths-json", default=None)
    args = parser.parse_args()
    raw_paths = args.manifest_paths_json or __import__("os").environ.get("ANNUAL_REPORT_RAY_MANIFESTS", "[]")
    force = __import__("os").environ.get("ANNUAL_REPORT_RAY_FORCE", "false").lower() == "true"
    paths = json.loads(raw_paths)
    if not isinstance(paths, list) or not all(isinstance(path, str) and path for path in paths):
        raise ValueError("ANNUAL_REPORT_RAY_MANIFESTS must be a JSON array of manifest paths")
    result = run_job(paths, force=force)
    print(json.dumps(result, ensure_ascii=False))
    if result["status"] != "succeeded":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
