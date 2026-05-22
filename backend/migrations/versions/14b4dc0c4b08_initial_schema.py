"""initial_schema

Revision ID: 14b4dc0c4b08
Revises:
Create Date: 2026-05-22 01:27:54.337611

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB, ARRAY, INET


# revision identifiers, used by Alembic.
revision: str = '14b4dc0c4b08'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""

    # ENUM types — created via table columns (SQLAlchemy auto-creates them).

    # --- batches ---
    op.create_table(
        "batches",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("name", sa.String(256), nullable=True),
        sa.Column("created_by", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    # --- documents ---
    op.create_table(
        "documents",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("filename", sa.String(512), nullable=False),
        sa.Column("file_type", sa.String(16), nullable=False),
        sa.Column("file_hash", sa.String(64), nullable=False, unique=True),
        sa.Column("file_size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("language", sa.String(8), server_default="zh"),
        sa.Column("raw_url", sa.String(1024), nullable=True),
        sa.Column("markdown_url", sa.String(1024), nullable=True),
        sa.Column("json_url", sa.String(1024), nullable=True),
        sa.Column("page_count", sa.Integer(), nullable=True),
        sa.Column("chunk_count", sa.Integer(), nullable=True),
        sa.Column("total_tokens", sa.BigInteger(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    # --- ingestion_tasks ---
    op.create_table(
        "ingestion_tasks",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("batch_id", UUID(as_uuid=True), sa.ForeignKey("batches.id", ondelete="SET NULL"), nullable=True),
        sa.Column("doc_id", UUID(as_uuid=True), sa.ForeignKey("documents.id", ondelete="SET NULL"), nullable=True),
        sa.Column("retry_of", UUID(as_uuid=True), sa.ForeignKey("ingestion_tasks.id", ondelete="SET NULL"), nullable=True),
        sa.Column("trace_id", sa.String(64), nullable=False),
        sa.Column("file_hash", sa.String(64), nullable=False),
        sa.Column("original_filename", sa.String(512), nullable=False),
        sa.Column("file_type", sa.String(16), nullable=False),
        sa.Column("file_size_bytes", sa.BigInteger(), nullable=True),
        sa.Column(
            "status",
            sa.Enum("pending", "parsing", "chunking", "storing", "done", "failed", "cancelled", name="taskstatus"),
            nullable=False,
            server_default="pending",
        ),
        sa.Column("progress", sa.SmallInteger(), server_default="0"),
        sa.Column("current_step", sa.String(256), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("retry_count", sa.SmallInteger(), server_default="0"),
        sa.Column("mineru_task_id", sa.String(64), nullable=True),
        sa.Column("asr_task_id", sa.String(64), nullable=True),
        sa.Column("last_heartbeat", sa.DateTime(timezone=True), nullable=True),
        sa.Column("parse_duration_ms", sa.BigInteger(), nullable=True),
        sa.Column("chunk_duration_ms", sa.BigInteger(), nullable=True),
        sa.Column("metadata", JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_by", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_check_constraint(
        "chk_done_has_doc",
        "ingestion_tasks",
        "status != 'done' OR doc_id IS NOT NULL",
    )

    # --- outbox ---
    op.create_table(
        "outbox",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("aggregate_type", sa.String(64), nullable=False),
        sa.Column("aggregate_id", UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(128), nullable=False),
        sa.Column("payload", JSONB(), nullable=False),
        sa.Column("trace_id", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
    )

    # --- chunks ---
    op.create_table(
        "chunks",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("doc_id", UUID(as_uuid=True), sa.ForeignKey("documents.id", ondelete="CASCADE"), nullable=False),
        sa.Column("parent_id", UUID(as_uuid=True), sa.ForeignKey("chunks.id", ondelete="CASCADE"), nullable=True),
        sa.Column("outbox_id", UUID(as_uuid=True), sa.ForeignKey("outbox.id", ondelete="SET NULL"), nullable=True),
        sa.Column("heading_level", sa.String(4), nullable=False),
        sa.Column(
            "granularity",
            sa.Enum("LARGE", "SMALL", name="chunkgranularity"),
            nullable=False,
        ),
        sa.Column("heading_path", ARRAY(sa.Text()), nullable=False, server_default=sa.text("'{}'::text[]")),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.String(32), nullable=False),
        sa.Column("token_count", sa.Integer(), nullable=True),
        sa.Column("page_start", sa.Integer(), nullable=True),
        sa.Column("page_end", sa.Integer(), nullable=True),
        sa.Column("has_table", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("has_formula", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("has_image", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_check_constraint(
        "chk_heading_level",
        "chunks",
        "heading_level IN ('H1','H2','H3','H4','LEAF')",
    )

    # --- audit_logs ---
    op.create_table(
        "audit_logs",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("user_id", sa.String(128), nullable=False),
        sa.Column("action", sa.String(64), nullable=False),
        sa.Column("resource_type", sa.String(64), nullable=False),
        sa.Column("resource_id", UUID(as_uuid=True), nullable=False),
        sa.Column("details", JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("ip_address", INET(), nullable=True),
        sa.Column("trace_id", sa.String(64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )


def downgrade() -> None:
    """Downgrade schema."""

    op.drop_table("audit_logs")
    op.drop_table("chunks")
    op.execute("DROP TYPE IF EXISTS chunkgranularity")
    op.drop_table("outbox")
    op.drop_table("ingestion_tasks")
    op.execute("DROP TYPE IF EXISTS taskstatus")
    op.drop_table("documents")
    op.drop_table("batches")
