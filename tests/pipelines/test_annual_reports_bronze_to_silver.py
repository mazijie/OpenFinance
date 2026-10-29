from backend.pipelines.annual_reports_bronze_to_silver import AnnualReportsBronzeToSilverPipeline


class ExplodingBronzeWriter:
    def list_objects(self, *, prefix: str):
        raise AssertionError("an explicit empty batch must not scan Bronze")


class NoopReportWriter:
    pass


def test_empty_object_batch_does_not_fallback_to_full_bronze_scan() -> None:
    pipeline = AnnualReportsBronzeToSilverPipeline(
        parser_client=ExplodingBronzeWriter(), ir_pipeline=NoopReportWriter()
    )

    stats = pipeline.run([])

    assert stats.processed_objects == 0
    assert stats.written_reports == 0
    assert stats.failed_count == 0
