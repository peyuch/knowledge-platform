"""RaptorNode CRUD + tree build orchestrator."""

import asyncio
import hashlib
import logging
import uuid
from datetime import datetime, timezone

import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from core.config import settings
from core.kafka import get_raptor_kafka
from services.raptor.clusterer import (
    soft_cluster,
    assign_clusters,
    calculate_optimal_k,
    should_stop_clustering,
)
from services.raptor.summarizer import get_summarizer
from models.raptor_node import RaptorNode

logger = logging.getLogger(__name__)


class RaptorStore:
    def __init__(self):
        engine = create_engine(settings.database_url_sync)
        self.Session = sessionmaker(bind=engine)

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def create_leaf_nodes(self, chunks: list[dict], doc_id: str) -> list[dict]:
        """Create level=0 LEAF nodes from LARGE chunks. Returns node dicts."""
        nodes = []
        with self.Session() as db:
            for chunk in chunks:
                node = RaptorNode(
                    doc_id=uuid.UUID(doc_id),
                    chunk_id=uuid.UUID(chunk["chunk_id"]),
                    level=0,
                    node_type="LEAF",
                    content=chunk["content"],
                    token_count=chunk.get("token_count", 0),
                    heading_path=chunk.get("heading_path", []),
                    created_at=datetime.now(timezone.utc),
                    updated_at=datetime.now(timezone.utc),
                )
                db.add(node)
                db.flush()
                nodes.append({
                    "id": str(node.id),
                    "doc_id": doc_id,
                    "chunk_id": chunk["chunk_id"],
                    "level": 0,
                    "node_type": "LEAF",
                    "content": node.content,
                    "token_count": node.token_count,
                    "heading_path": node.heading_path or [],
                    "embedding": chunk.get("embedding"),
                })
            db.commit()
        return nodes

    def create_summary_node(
        self,
        doc_id: str,
        parent_ids: list[str],
        level: int,
        cluster_label: int,
        content: str,
        token_count: int,
        source_chunk_ids: list[str],
        heading_path: list[str],
    ) -> dict:
        """Create a SUMMARY or ROOT node."""
        with self.Session() as db:
            node = RaptorNode(
                doc_id=uuid.UUID(doc_id),
                level=level,
                node_type="ROOT" if level >= 3 else "SUMMARY",
                cluster_label=cluster_label,
                content=content,
                token_count=token_count,
                source_chunk_ids=[uuid.UUID(cid) for cid in source_chunk_ids],
                heading_path=heading_path,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
            db.add(node)
            db.flush()

            # Link parents (self-referential FK for tree structure)
            for pid in parent_ids:
                parent = db.query(RaptorNode).filter(RaptorNode.id == uuid.UUID(pid)).first()
                if parent:
                    parent.parent_id = node.id

            db.commit()
            db.refresh(node)
            return {
                "id": str(node.id),
                "doc_id": doc_id,
                "level": level,
                "node_type": node.node_type,
                "cluster_label": cluster_label,
                "content": content,
                "token_count": token_count,
                "source_chunk_ids": source_chunk_ids,
                "heading_path": heading_path,
            }

    # ------------------------------------------------------------------
    # Tree Build
    # ------------------------------------------------------------------

    def build_tree(self, doc_id: str, leaf_nodes: list[dict], trace_id: str) -> None:
        """Build RAPTOR tree from leaf nodes. Pushes SUMMARY nodes to Kafka."""
        total_tokens = sum(n.get("token_count", 0) for n in leaf_nodes)
        if total_tokens < 1000:
            logger.info(f"Doc {doc_id}: {total_tokens} tokens -- skipping clustering (short doc)")
            return

        self._build_level(leaf_nodes, level=0, doc_id=doc_id, trace_id=trace_id)

    def _build_level(
        self, nodes: list[dict], level: int, doc_id: str, trace_id: str
    ) -> None:
        if should_stop_clustering(level, nodes):
            return

        embeddings = np.array(
            [n.get("embedding") or self._get_embedding(n["id"]) for n in nodes],
            dtype=np.float32,
        )
        k = calculate_optimal_k(nodes)
        probs = soft_cluster(embeddings, n_components=k)
        clusters = assign_clusters(probs)

        summarizer = get_summarizer()
        kafka = get_raptor_kafka()
        summary_nodes = []

        for cluster_idx, member_indices in enumerate(clusters):
            if len(member_indices) < 2:
                continue

            cluster_nodes = [nodes[i] for i in member_indices]
            texts = [n["content"] for n in cluster_nodes]

            # Run async summarizer in sync context via asyncio.run
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    import concurrent.futures
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        future = executor.submit(asyncio.run, summarizer.summarize(texts))
                        summary_text = future.result(timeout=120)
                else:
                    summary_text = asyncio.run(summarizer.summarize(texts))
            except RuntimeError:
                summary_text = asyncio.run(summarizer.summarize(texts))

            token_count = len(summary_text)  # approximate
            heading = f"第{level + 1}层摘要 — 簇{cluster_idx + 1}"
            source_ids = list({n.get("chunk_id", n["id"]) for n in cluster_nodes})

            node = self.create_summary_node(
                doc_id=doc_id,
                parent_ids=[n["id"] for n in cluster_nodes],
                level=level + 1,
                cluster_label=cluster_idx,
                content=summary_text,
                token_count=token_count,
                source_chunk_ids=source_ids,
                heading_path=[heading],
            )
            summary_nodes.append(node)

            # Push to Kafka for #2 mini-consumer
            msg = {
                "action": "updated",
                "event": "raptor.node_updated",
                "chunk_id": node["id"],
                "doc_id": doc_id,
                "parent_id": cluster_nodes[0].get("chunk_id"),
                "heading_level": "SUMMARY",
                "granularity": "SUMMARY",
                "heading_path": node["heading_path"],
                "content": summary_text,
                "content_hash": hashlib.md5(summary_text.encode()).hexdigest(),
                "level": level + 1,
                "node_type": node["node_type"],
                "source_chunk_ids": source_ids,
                "token_count": token_count,
                "trace_id": trace_id,
                "metadata": {},
            }
            kafka.send_single(None, msg)

        if summary_nodes:
            self._build_level(summary_nodes, level + 1, doc_id, trace_id)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_embedding(self, node_id: str) -> list[float]:
        """Get embedding from pgvector. Stub -- real impl uses pgvector query."""
        with self.Session() as db:
            result = db.execute(
                text("SELECT embedding FROM raptor_nodes WHERE id = :id"),
                {"id": node_id},
            )
            row = result.fetchone()
            return list(row[0]) if row and row[0] else [0.0] * 384

    def delete_doc_nodes(self, doc_id: str) -> None:
        """Delete all nodes for a document."""
        with self.Session() as db:
            db.query(RaptorNode).filter(RaptorNode.doc_id == uuid.UUID(doc_id)).delete()
            db.commit()
