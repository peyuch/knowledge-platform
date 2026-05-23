"""Tests for the sentence-transformer embedder."""

import pytest
import numpy as np
from services.indexing.embedder import Embedder


@pytest.fixture(scope="module")
def embedder():
    return Embedder()


def test_embedder_batch_encode_returns_correct_dim(embedder):
    texts = ["hello world", "another sentence"]
    vectors = embedder.encode(texts)
    assert vectors.shape == (2, 384)


def test_embedder_vectors_are_normalized(embedder):
    texts = ["test normalization"]
    vectors = embedder.encode(texts)
    norms = np.linalg.norm(vectors, axis=1)
    assert np.allclose(norms, 1.0, atol=1e-4)


def test_embedder_empty_list_returns_empty(embedder):
    vectors = embedder.encode([])
    assert vectors.shape == (0, 384)


def test_embedder_single_text(embedder):
    vectors = embedder.encode(["single"])
    assert vectors.shape == (1, 384)
