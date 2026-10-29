"""Create durable annual-report pipeline and index-generation state."""

from alembic import op

revision = "0003_annual_report_state"
down_revision = "0002_agent_harness"
branch_labels = None
depends_on = None


def upgrade() -> None:
    from backend.pipelines.annual_report_pipeline_state import AnnualReportPipelineStateStore

    AnnualReportPipelineStateStore().ensure_schema()


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS openfinance.annual_report_index_generations")
    op.execute("DROP TABLE IF EXISTS openfinance.annual_report_processing_state")
