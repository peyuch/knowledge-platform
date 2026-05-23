"""add_entity_normalization_and_graphrag_dlq

Revision ID: c9d2e3f4a5b6
Revises: b8c1d2e3f4a5
Create Date: 2026-05-22 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB


# revision identifiers, used by Alembic.
revision: str = 'c9d2e3f4a5b6'
down_revision: Union[str, Sequence[str], None] = 'b8c1d2e3f4a5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "entity_normalization",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("standard_name", sa.String(256), nullable=False),
        sa.Column("alias", sa.String(256), nullable=False, unique=True),
        sa.Column("entity_type", sa.String(32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("idx_en_alias", "entity_normalization", ["alias"])
    op.create_index("idx_en_entity_type", "entity_normalization", ["entity_type", "standard_name"])

    op.create_table(
        "dead_letter_graphrag",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("chunk_id", sa.String(36), nullable=False),
        sa.Column("topic", sa.String(128), nullable=False),
        sa.Column("partition", sa.Integer(), nullable=False),
        sa.Column("kafka_offset", sa.BigInteger(), nullable=False),
        sa.Column("payload", JSONB(), nullable=False),
        sa.Column("error_type", sa.String(256), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("retry_count", sa.SmallInteger(), server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("idx_graphrag_dlq_chunk_id", "dead_letter_graphrag", ["chunk_id"])
    op.create_index(
        "idx_graphrag_dlq_retry",
        "dead_letter_graphrag",
        ["retry_count", "created_at"],
        postgresql_where=sa.text("retry_count < 10"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("idx_graphrag_dlq_retry", table_name="dead_letter_graphrag")
    op.drop_index("idx_graphrag_dlq_chunk_id", table_name="dead_letter_graphrag")
    op.drop_table("dead_letter_graphrag")
    op.drop_index("idx_en_entity_type", table_name="entity_normalization")
    op.drop_index("idx_en_alias", table_name="entity_normalization")
    op.drop_table("entity_normalization")
