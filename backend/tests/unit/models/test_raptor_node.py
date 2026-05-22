"""Tests for RaptorNode ORM model."""

import uuid
from datetime import datetime, timezone
from models.raptor_node import RaptorNode


def test_raptor_node_creation():
    node = RaptorNode(
        id=uuid.uuid4(),
        doc_id=uuid.uuid4(),
        level=0,
        node_type="LEAF",
        content="原文内容",
        token_count=500,
        heading_path=["第一章", "1.1 概述"],
        created_at=datetime.now(timezone.utc),
    )
    assert node.level == 0
    assert node.node_type == "LEAF"
    assert node.token_count == 500


def test_raptor_node_summary_type():
    node = RaptorNode(
        id=uuid.uuid4(),
        doc_id=uuid.uuid4(),
        parent_id=uuid.uuid4(),
        level=2,
        node_type="SUMMARY",
        cluster_label=3,
        content="摘要内容",
        token_count=300,
        source_chunk_ids=[uuid.uuid4(), uuid.uuid4()],
    )
    assert node.node_type == "SUMMARY"
    assert node.cluster_label == 3
    assert len(node.source_chunk_ids) == 2
