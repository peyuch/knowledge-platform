"""Three-way parallel recall + Min-Max fusion."""

import asyncio
import time
import logging
from typing import Any

from common.constants import (
    SEARCH_FUSION_WEIGHTS,
    SEARCH_DEFAULT_WEIGHTS,
    SEARCH_ABSOLUTE_MIN_THRESHOLD,
)

logger = logging.getLogger(__name__)


class HybridSearcher:
    def __init__(self, es_store=None, milvus_store=None, neo4j_store=None):
        self._es = es_store
        self._milvus = milvus_store
        self._neo4j = neo4j_store

    async def search(
        self, query: str, query_type: str, top_k: int = 10, filters: dict | None = None
    ) -> list[dict]:
        t0 = time.time()
        weights = SEARCH_FUSION_WEIGHTS.get(query_type, SEARCH_DEFAULT_WEIGHTS)
        recall_k = top_k * 3

        bm25_task = asyncio.create_task(self._bm25_search(query, recall_k, filters))
        vector_task = asyncio.create_task(self._vector_search(query, recall_k, filters))
        graph_task = asyncio.create_task(self._graph_search(query, recall_k, filters))

        bm25_results, vector_results, graph_results = await asyncio.gather(
            bm25_task, vector_task, graph_task, return_exceptions=True
        )

        if isinstance(bm25_results, Exception):
            logger.warning(f"BM25 recall failed: {bm25_results}")
            bm25_results = []
        if isinstance(vector_results, Exception):
            logger.warning(f"Vector recall failed: {vector_results}")
            vector_results = []
        if isinstance(graph_results, Exception):
            logger.warning(f"Graph recall failed: {graph_results}")
            graph_results = []

        merged = self._fuse(bm25_results, vector_results, graph_results, weights, top_k)
        for r in merged:
            r["_took_ms"] = (time.time() - t0) * 1000
        return merged

    async def _bm25_search(
        self, query: str, top_k: int, filters: dict | None
    ) -> list[dict]:
        """ES BM25 full-text search."""
        if self._es is None:
            return []

        try:
            must_clauses = [{"match": {"content": {"query": query, "operator": "and"}}}]
            if filters:
                if filters.get("department"):
                    must_clauses.append(
                        {"term": {"department": filters["department"]}}
                    )
                if filters.get("file_type"):
                    must_clauses.append(
                        {"term": {"file_type": filters["file_type"]}}
                    )

            body = {"query": {"bool": {"must": must_clauses}}, "size": top_k}

            resp = self._es._client.search(index="chunks", body=body)
            results = []
            for hit in resp["hits"]["hits"]:
                src = hit["_source"]
                results.append(
                    {
                        "content": src.get("content", ""),
                        "chunk_id": src.get("chunk_id", hit["_id"]),
                        "doc_id": src.get("doc_id", ""),
                        "score": float(hit["_score"] or 0),
                        "source": "bm25",
                    }
                )
            return results
        except Exception as e:
            logger.warning(f"BM25 search error: {e}")
            return []

    async def _vector_search(
        self, query: str, top_k: int, filters: dict | None
    ) -> list[dict]:
        """Milvus vector similarity search with embedding + ANN."""
        if self._milvus is None:
            return []

        try:
            from pymilvus import Collection
            from services.indexing.embedder import Embedder

            embedder = Embedder()
            query_vec = embedder.encode([query])[0].tolist()

            expr_parts = []
            if filters:
                if filters.get("department"):
                    expr_parts.append(f'department == "{filters["department"]}"')
                if filters.get("file_type"):
                    expr_parts.append(
                        f'file_type == "{filters["file_type"]}"'
                    )

            expr = " && ".join(expr_parts) if expr_parts else None

            collection = Collection("chunks")
            collection.load()

            search_params = {"metric_type": "IP", "params": {"nprobe": 16}}
            results = collection.search(
                data=[query_vec],
                anns_field="embedding",
                param=search_params,
                limit=top_k,
                expr=expr,
                output_fields=["chunk_id", "doc_id", "content", "page_start"],
            )

            items = []
            for hits in results:
                for hit in hits:
                    items.append(
                        {
                            "content": hit.entity.get("content", ""),
                            "chunk_id": hit.entity.get("chunk_id", str(hit.id)),
                            "doc_id": hit.entity.get("doc_id", ""),
                            "page_start": hit.entity.get("page_start"),
                            "score": float(hit.score),
                            "source": "vector",
                        }
                    )
            return items
        except Exception as e:
            logger.warning(f"Vector search error: {e}")
            return []

    async def _graph_search(
        self, query: str, top_k: int, filters: dict | None
    ) -> list[dict]:
        """Neo4j graph traversal — entity matching + 1-hop neighbor expansion."""
        if self._neo4j is None:
            return []

        try:
            from services.indexing.embedder import Embedder

            embedder = Embedder()
            query_vec = embedder.encode([query])[0]

            with self._neo4j._driver.session() as session:
                result = session.run(
                    """
                    CALL db.index.vector.queryNodes('entity_embedding_idx', $top_k, $embedding)
                    YIELD node, score
                    WHERE score > 0.5
                    OPTIONAL MATCH (node)-[r]-(neighbor:Entity)
                    RETURN node.entity_id AS entity_id, node.name AS name,
                           labels(node) AS labels, score,
                           collect(DISTINCT {entity_id: neighbor.entity_id, name: neighbor.name,
                           rel_type: type(r)})[0..5] AS neighbors
                    ORDER BY score DESC
                    LIMIT $top_k
                    """,
                    {"top_k": top_k, "embedding": query_vec.tolist()},
                )

                items = []
                for record in result:
                    entity_id = f"graph:{record['entity_id']}"
                    labels = record.get("labels", [])
                    labels_str = ", ".join(l for l in labels if l != "Entity")
                    neighbor_texts = []
                    for n in record.get("neighbors", []):
                        if n and n.get("name"):
                            neighbor_texts.append(
                                f"{n['name']} ({n.get('rel_type', 'related')})"
                            )

                    content = f"[{labels_str}] {record['name']}"
                    if neighbor_texts:
                        content += " | 关联: " + "; ".join(neighbor_texts)

                    items.append(
                        {
                            "content": content,
                            "chunk_id": entity_id,
                            "entity_id": record["entity_id"],
                            "doc_id": "",
                            "score": float(record["score"]),
                            "source": "graph",
                        }
                    )
                return items
        except Exception as e:
            logger.warning(f"Graph search error: {e}")
            return []

    def _fuse(
        self, bm25: list, vec: list, graph: list, weights: dict, top_k: int
    ) -> list:
        """Min-Max normalize per source, weighted sum, deduplicate, sort.

        Routes where max_score < ABSOLUTE_MIN_THRESHOLD get penalized by 0.1.
        """
        def minmax(items, key):
            if not items:
                return
            vals = [r[key] for r in items]
            vmin, vmax = min(vals), max(vals)
            if vmax == vmin:
                for r in items:
                    r["_norm"] = 0.5
                return
            for r in items:
                r["_norm"] = (r[key] - vmin) / (vmax - vmin)

            # Penalize low-confidence routes
            if vmax < SEARCH_ABSOLUTE_MIN_THRESHOLD:
                for r in items:
                    r["_norm"] = max(0, r["_norm"] - 0.1)

        minmax(bm25, "score")
        minmax(vec, "score")
        minmax(graph, "score")

        seen = set()
        merged = {}
        for r in bm25 + vec + graph:
            cid = r.get("chunk_id", r.get("entity_id", f"unknown_{id(r)}"))
            w = r.get("_norm", 0) * weights.get(r.get("source", "unknown"), 0.3)
            if cid not in seen or w > merged.get(cid, {}).get("_weighted", 0):
                r["_weighted"] = w
                merged[cid] = r
            seen.add(cid)

        result = sorted(
            merged.values(), key=lambda r: r.get("_weighted", 0), reverse=True
        )
        return result[:top_k]
