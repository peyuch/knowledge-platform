"""Entity normalization: dictionary lookup -> vector similarity -> coreference resolution."""

import logging
from typing import Any

import numpy as np
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from core.config import settings
from models.entity_normalization import EntityNormalization
from services.indexing.embedder import Embedder
from common.constants import GRAPHRAG_ENTITY_SIMILARITY_THRESHOLD

logger = logging.getLogger(__name__)


class EntityNormalizer:
    def __init__(self):
        engine = create_engine(settings.database_url_sync)
        self.Session = sessionmaker(bind=engine)
        self._embedder = Embedder()

    def normalize(self, entities: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Normalize entity names: dictionary -> vector -> produce canonical list."""
        if not entities:
            return []

        # 1. Dictionary lookup
        canonicals: dict[str, str] = {}  # alias -> standard_name
        with self.Session() as db:
            names = {e["name"] for e in entities}
            if names:
                rows = db.execute(
                    select(EntityNormalization).where(EntityNormalization.alias.in_(names))
                ).scalars().all()
                for row in rows:
                    canonicals[row.alias] = row.standard_name
                    canonicals.setdefault(row.standard_name, row.standard_name)

        # 2. Apply dictionary mappings
        for e in entities:
            if e["name"] in canonicals:
                e["name"] = canonicals[e["name"]]

        # 3. Vector similarity dedup (within same type)
        entities = self._dedup_by_vector(entities)

        # 4. Build alias list
        alias_map: dict[str, list[str]] = {}
        for e in entities:
            alias_map.setdefault(e["name"], []).append(e.get("original_name", e["name"]))

        return entities

    def _dedup_by_vector(self, entities: list[dict]) -> list[dict]:
        """Deduplicate entities of the same type by embedding cosine similarity."""
        by_type: dict[str, list[int]] = {}
        for i, e in enumerate(entities):
            by_type.setdefault(e["type"], []).append(i)

        names = [e["name"] for e in entities]
        embeddings = self._embedder.encode(names)

        merged = set()
        result = []

        for etype, indices in by_type.items():
            for i in range(len(indices)):
                if indices[i] in merged:
                    continue
                group = [indices[i]]
                for j in range(i + 1, len(indices)):
                    if indices[j] in merged:
                        continue
                    sim = np.dot(embeddings[indices[i]], embeddings[indices[j]])
                    if sim > GRAPHRAG_ENTITY_SIMILARITY_THRESHOLD:
                        group.append(indices[j])
                        merged.add(indices[j])

                canonical = entities[group[0]]
                canonical.setdefault("aliases", [])
                for g in group[1:]:
                    canonical["aliases"].append(entities[g]["name"])
                result.append(canonical)

        return result

    def add_alias(self, standard_name: str, alias: str, entity_type: str) -> None:
        """Add a new alias to the normalization dictionary."""
        with self.Session() as db:
            record = EntityNormalization(
                standard_name=standard_name,
                alias=alias,
                entity_type=entity_type,
            )
            db.add(record)
            db.commit()
