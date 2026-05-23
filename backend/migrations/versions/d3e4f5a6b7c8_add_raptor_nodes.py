"""add_raptor_nodes

Revision ID: d3e4f5a6b7c8
Revises: c9d2e3f4a5b6
Create Date: 2026-05-22 14:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, ARRAY


# revision identifiers, used by Alembic.
revision: str = 'd3e4f5a6b7c8'
down_revision: Union[str, Sequence[str], None] = 'c9d2e3f4a5b6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "raptor_nodes",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("doc_id", UUID(as_uuid=True), sa.ForeignKey("documents.id", ondelete="CASCADE"), nullable=False),
        sa.Column("parent_id", UUID(as_uuid=True), sa.ForeignKey("raptor_nodes.id", ondelete="CASCADE"), nullable=True),
        sa.Column("chunk_id", UUID(as_uuid=True), nullable=True),
        sa.Column("level", sa.SmallInteger(), nullable=False),
        sa.Column("node_type", sa.String(16), nullable=False),
        sa.Column("cluster_label", sa.Integer(), nullable=True),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("token_count", sa.Integer(), nullable=True),
        sa.Column("heading_path", ARRAY(sa.Text()), nullable=False, server_default=sa.text("'{}'::text[]")),
        sa.Column(
            "source_chunk_ids",
            ARRAY(UUID(as_uuid=True)),
            nullable=False,
            server_default=sa.text("'{}'::uuid[]"),
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("idx_raptor_doc_id", "raptor_nodes", ["doc_id"])
    op.create_index("idx_raptor_doc_level", "raptor_nodes", ["doc_id", "level"])
    op.create_index("idx_raptor_parent_id", "raptor_nodes", ["parent_id"])
    op.create_index("idx_raptor_node_type", "raptor_nodes", ["node_type"])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("idx_raptor_node_type", table_name="raptor_nodes")
    op.drop_index("idx_raptor_parent_id", table_name="raptor_nodes")
    op.drop_index("idx_raptor_doc_level", table_name="raptor_nodes")
    op.drop_index("idx_raptor_doc_id", table_name="raptor_nodes")
    op.drop_table("raptor_nodes")
