"""Neo4j store — schema management, CRUD, vector index, UNWIND batch writes."""

import uuid
import logging
from typing import Any

from neo4j import GraphDatabase, Driver

from core.config import settings
from common.constants import GRAPHRAG_TX_TIMEOUT_SECONDS

logger = logging.getLogger(__name__)

ENTITY_TYPES = ["Policy", "Person", "Dept", "Process", "Role", "Regulation", "Risk", "Document"]


class Neo4jStore:
    def __init__(self):
        self._driver: Driver = GraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
        )

    # ── Schema ───────────────────────────────────────────

    def ensure_schema(self) -> None:
        with self._driver.session() as session:
            # Entity unique constraint
            session.run("CREATE CONSTRAINT entity_id_unique IF NOT EXISTS "
                        "FOR (e:Entity) REQUIRE e.entity_id IS UNIQUE")

            # Vector index (HNSW)
            session.run("""
                CREATE VECTOR INDEX entity_embedding_idx IF NOT EXISTS
                FOR (e:Entity) ON (e.embedding)
                OPTIONS {indexConfig: {
                  `vector.dimensions`: 384,
                  `vector.similarity_function`: 'COSINE',
                  `vector.hnsw.m`: 16,
                  `vector.hnsw.ef_construction`: 200
                }}
            """)

            # B-tree indexes
            for etype in ENTITY_TYPES:
                session.run(f"CREATE INDEX {etype.lower()}_name_idx IF NOT EXISTS "
                            f"FOR (e:{etype}) ON (e.name)")

            session.run("CREATE INDEX entity_doc_id_idx IF NOT EXISTS "
                        "FOR (e:Entity) ON (e.doc_id)")
            session.run("CREATE INDEX entity_department_idx IF NOT EXISTS "
                        "FOR (e:Entity) ON (e.department)")
            session.run("CREATE INDEX pending_resolution_doc_idx IF NOT EXISTS "
                        "FOR (e:PendingResolution) ON (e.doc_id)")
            session.run("CREATE INDEX rebuild_shadow_doc_idx IF NOT EXISTS "
                        "FOR (e:RebuildShadow) ON (e.doc_id)")

            logger.info("Neo4j schema ensured (constraints + indexes)")

    # ── Batch Write ──────────────────────────────────────

    def upsert_entities_and_relations(
        self, entities: list[dict], relations: list[dict]
    ) -> None:
        """UNWIND batch write: MERGE entities + relations in one transaction."""
        with self._driver.session() as session:
            tx = session.begin_transaction(timeout=GRAPHRAG_TX_TIMEOUT_SECONDS)
            try:
                if entities:
                    self._upsert_entities_by_type(tx, entities)
                if relations:
                    tx.run("""
                        UNWIND $relations AS r
                        MATCH (a:Entity {entity_id: r.source_entity_id})
                        MATCH (b:Entity {entity_id: r.target_entity_id})
                        MERGE (a)-[rel:{rel_type}]->(b)
                        SET rel += r.props
                    """, {"relations": relations})
                tx.commit()
            except Exception:
                tx.rollback()
                raise

    def _upsert_entities_by_type(self, session, entities: list[dict]) -> None:
        """Run UNWIND per entity type (Neo4j requires static labels)."""
        by_type: dict[str, list] = {}
        for e in entities:
            by_type.setdefault(e["type"], []).append(e)

        for etype, batch in by_type.items():
            session.run(f"""
                UNWIND $entities AS e
                MERGE (n:Entity:{etype} {{name: e.name}})
                ON CREATE SET n.entity_id = e.entity_id,
                              n.embedding = e.embedding,
                              n.aliases = e.aliases,
                              n.confidence = e.confidence,
                              n.access_level = e.access_level,
                              n.department = e.department,
                              n.doc_id = e.doc_id,
                              n:{etype},
                              n:PendingResolution
                ON MATCH  SET n.embedding = e.embedding
            """, {"entities": batch})

    # ── Query ─────────────────────────────────────────────

    def get_pending_nodes(self, doc_id: str) -> list[dict]:
        with self._driver.session() as session:
            result = session.run(
                "MATCH (e:PendingResolution {doc_id: $doc_id}) RETURN e",
                {"doc_id": doc_id},
            )
            return [dict(record["e"]) for record in result]

    def remove_pending_label(self, doc_id: str) -> None:
        with self._driver.session() as session:
            session.run(
                "MATCH (e:PendingResolution {doc_id: $doc_id}) REMOVE e:PendingResolution",
                {"doc_id": doc_id},
            )

    def merge_entities(self, entity_ids: list[str]) -> None:
        """Use APOC to safely merge duplicate entities."""
        with self._driver.session() as session:
            session.run(
                "MATCH (e:Entity) WHERE e.entity_id IN $ids "
                "WITH collect(e) AS nodes "
                "CALL apoc.refactor.mergeNodes(nodes, {properties: 'combine', mergeRels: true}) "
                "YIELD node RETURN node",
                {"ids": entity_ids},
            )

    def replace_doc_graph(self, doc_id: str, entities: list[dict], relations: list[dict]) -> None:
        """Atomic shadow-rebuild: write to :RebuildShadow, then swap."""
        with self._driver.session() as session:
            tx = session.begin_transaction()
            try:
                # Write new nodes with :RebuildShadow
                for e in entities:
                    tx.run(f"""
                        MERGE (n:Entity:{e['type']}:RebuildShadow {{name: $name}})
                        ON CREATE SET n += $props
                        ON MATCH  SET n += $props
                    """, {"name": e["name"], "props": {k: v for k, v in e.items() if k not in ("name", "type")}})

                # Write new relations
                for r in relations:
                    tx.run(f"""
                        MATCH (a:RebuildShadow {{entity_id: $source}})
                        MATCH (b:RebuildShadow {{entity_id: $target}})
                        MERGE (a)-[rel:{r['type']}]->(b)
                        SET rel += $props
                    """, {"source": r["source_entity_id"], "target": r["target_entity_id"], "props": r})

                # Atomic swap
                tx.run(
                    "MATCH (e {doc_id: $doc_id}) WHERE NOT e:RebuildShadow DETACH DELETE e",
                    {"doc_id": doc_id},
                )
                tx.run(
                    "MATCH (e:RebuildShadow {doc_id: $doc_id}) REMOVE e:RebuildShadow",
                    {"doc_id": doc_id},
                )
                tx.commit()
                logger.info(f"Replaced graph for doc {doc_id}")
            except Exception:
                tx.rollback()
                raise

    def close(self) -> None:
        self._driver.close()
