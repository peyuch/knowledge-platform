"""add_dead_letter_index

Revision ID: b8c1d2e3f4a5
Revises: 14b4dc0c4b08
Create Date: 2026-05-22 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB


# revision identifiers, used by Alembic.
revision: str = 'b8c1d2e3f4a5'
down_revision: Union[str, Sequence[str], None] = '14b4dc0c4b08'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "dead_letter_index",
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
    op.create_index("idx_dlq_chunk_id", "dead_letter_index", ["chunk_id"])
    op.create_index(
        "idx_dlq_retry",
        "dead_letter_index",
        ["retry_count", "created_at"],
        postgresql_where=sa.text("retry_count < 10"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("idx_dlq_retry", table_name="dead_letter_index")
    op.drop_index("idx_dlq_chunk_id", table_name="dead_letter_index")
    op.drop_table("dead_letter_index")
