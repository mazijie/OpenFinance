"""Track the embedding model used by the news vector index."""
from alembic import op

revision = "0020_news_embedding_model"
down_revision = "0019_report_chunk_identity"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TABLE openfinance.news_index_reconciliation ADD COLUMN embedding_model text")


def downgrade():
    op.execute("ALTER TABLE openfinance.news_index_reconciliation DROP COLUMN embedding_model")
