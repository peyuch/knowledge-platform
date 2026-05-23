"""Sentence-transformer wrapper for batch text embedding."""

import logging
import numpy as np
from sentence_transformers import SentenceTransformer

from common.constants import INDEX_EMBEDDING_MODEL
from core.config import settings

logger = logging.getLogger(__name__)


class Embedder:
    def __init__(self, model_name: str = INDEX_EMBEDDING_MODEL):
        self.model = SentenceTransformer(model_name)
        # Warm-up to avoid cold-start latency on first batch
        self.model.encode(["warm up text"])
        logger.info(f"Embedder initialized with model {model_name}")

    def encode(self, texts: list[str]) -> np.ndarray:
        """Batch-encode texts to normalized float32 vectors. Returns (N, 384).

        Vectors are L2-normalized so IP metric in Milvus = COSINE.
        """
        if not texts:
            return np.empty((0, 384), dtype=np.float32)
        vectors = self.model.encode(
            texts,
            normalize_embeddings=True,
            show_progress_bar=False,
            batch_size=settings.index_embed_batch_size,
        )
        return vectors.astype(np.float32)
