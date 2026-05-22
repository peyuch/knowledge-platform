"""Tests for UMAP+GMM soft clustering."""

import numpy as np
from services.raptor.clusterer import soft_cluster, should_stop_clustering


def test_soft_cluster_returns_probabilities():
    rng = np.random.RandomState(42)
    embeddings = rng.randn(20, 384).astype(np.float32)
    probs = soft_cluster(embeddings, n_components=3)
    assert probs.shape == (20, 3)
    assert np.allclose(probs.sum(axis=1), 1.0, atol=1e-2)


def test_soft_cluster_handles_fewer_points_than_components():
    rng = np.random.RandomState(42)
    embeddings = rng.randn(3, 384).astype(np.float32)
    probs = soft_cluster(embeddings, n_components=5)
    assert probs.shape[0] == 3


def test_should_stop_by_depth():
    nodes = [{"token_count": 2000}, {"token_count": 2000}]
    assert should_stop_clustering(3, nodes) is True
    assert should_stop_clustering(2, nodes) is False


def test_should_stop_by_token_count():
    nodes = [{"token_count": 300}, {"token_count": 400}]
    assert should_stop_clustering(0, nodes) is True


def test_should_stop_by_node_count():
    nodes = [{"token_count": 2000}]
    assert should_stop_clustering(0, nodes) is True
