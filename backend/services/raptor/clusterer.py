"""UMAP dimensionality reduction + GMM soft clustering for RAPTOR."""

import numpy as np
from umap import UMAP
from sklearn.mixture import GaussianMixture

from common.constants import (
    RAPTOR_MAX_DEPTH,
    RAPTOR_STOP_CLUSTERING_TOKENS,
    RAPTOR_UMAP_N_COMPONENTS,
    RAPTOR_GMM_PROB_THRESHOLD,
    RAPTOR_TARGET_TOKENS_PER_CLUSTER,
)


def soft_cluster(embeddings: np.ndarray, n_components: int) -> np.ndarray:
    """UMAP reduce -> GMM fit -> soft probability matrix (N, K).

    Returns P[i][k] = probability node i belongs to cluster k.
    """
    n = len(embeddings)
    if n < 2:
        return np.ones((n, 1), dtype=np.float32)

    k = max(2, min(n_components, n))

    # Edge case: too few samples for UMAP. Use raw embeddings for GMM.
    if n <= 2 * RAPTOR_UMAP_N_COMPONENTS:
        reduced = embeddings.astype(np.float64)
    else:
        umap_dim = min(RAPTOR_UMAP_N_COMPONENTS, n - 1, k - 1)
        umap_dim = max(2, umap_dim)
        reducer = UMAP(n_components=umap_dim, random_state=42)
        reduced = reducer.fit_transform(embeddings)

    gmm = GaussianMixture(n_components=k, random_state=42)
    gmm.fit(reduced)
    return gmm.predict_proba(reduced)


def assign_clusters(probs: np.ndarray, threshold: float = RAPTOR_GMM_PROB_THRESHOLD) -> list[list[int]]:
    """Convert probability matrix to cluster assignments (soft — one node can belong to multiple clusters)."""
    k = probs.shape[1]
    clusters = [[] for _ in range(k)]
    for i in range(probs.shape[0]):
        for j in range(k):
            if probs[i][j] >= threshold:
                clusters[j].append(i)
    return clusters


def calculate_optimal_k(nodes: list[dict]) -> int:
    """K = total_tokens / 1500, clamped to [2, 10]."""
    total = sum(n["token_count"] for n in nodes)
    return max(2, min(10, int(total / RAPTOR_TARGET_TOKENS_PER_CLUSTER)))


def should_stop_clustering(current_level: int, nodes: list[dict]) -> bool:
    """Check stop conditions: max depth, too few nodes, or total tokens below threshold."""
    if current_level >= RAPTOR_MAX_DEPTH:
        return True
    if len(nodes) < 2:
        return True
    if sum(n["token_count"] for n in nodes) < RAPTOR_STOP_CLUSTERING_TOKENS:
        return True
    return False
