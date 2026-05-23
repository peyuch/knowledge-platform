"""Tests for the heading-aware markdown chunker."""

import pytest
from services.ingestion.chunker import chunk_markdown, ChunkDraft


SIMPLE_MD = """\
# Chapter 1
## Section 1.1
This is some content under section 1.1.
More content here.
### Subsection 1.1.1
Detailed content in the subsection.
"""


def test_chunker_returns_list():
    result = chunk_markdown(SIMPLE_MD, doc_id="test-doc")
    assert isinstance(result, list)
    assert len(result) > 0
    assert all(isinstance(c, ChunkDraft) for c in result)


def test_chunker_creates_h1_large_chunk():
    result = chunk_markdown(SIMPLE_MD, doc_id="test-doc")
    h1_chunks = [c for c in result if c.heading_level == "H1"]
    assert len(h1_chunks) >= 1
    assert all(c.granularity == "LARGE" for c in h1_chunks)


def test_chunker_creates_h2_large_chunk():
    result = chunk_markdown(SIMPLE_MD, doc_id="test-doc")
    h2_chunks = [c for c in result if c.heading_level == "H2"]
    assert len(h2_chunks) >= 1
    assert all(c.granularity == "LARGE" for c in h2_chunks)


def test_chunker_creates_h3_small_chunk():
    result = chunk_markdown(SIMPLE_MD, doc_id="test-doc")
    small_chunks = [c for c in result if c.granularity == "SMALL"]
    assert len(small_chunks) > 0


def test_chunker_heading_path_accumulates_correctly():
    result = chunk_markdown(SIMPLE_MD, doc_id="test-doc")
    subsections = [c for c in result if c.heading_level == "H3"]
    for ss in subsections:
        assert "Chapter 1" in ss.heading_path
        assert "Section 1.1" in ss.heading_path


def test_chunker_parent_child_relationship():
    result = chunk_markdown(SIMPLE_MD, doc_id="test-doc")
    for chunk in result:
        if chunk.parent_id is not None:
            parent = next((c for c in result if c.chunk_id == chunk.parent_id), None)
            assert parent is not None


def test_chunker_orphan_text_attached_to_nearest_heading():
    md = """\
# Title
Some orphan text before any subheading.
## Subtopic
More text.
"""
    result = chunk_markdown(md, doc_id="test-doc")
    orphans = [c for c in result if "orphan" in c.content.lower() or "Some orphan" in c.content]
    assert len(orphans) > 0


def test_chunker_table_not_split():
    md = """\
# Report
## Data
| Col A | Col B |
|-------|-------|
| 1     | 2     |
| 3     | 4     |
Some text after table.
"""
    result = chunk_markdown(md, doc_id="test-doc")
    table_chunks = [c for c in result if c.has_table]
    assert len(table_chunks) > 0
    for tc in table_chunks:
        assert "| Col A | Col B |" in tc.content


def test_chunker_content_hash_is_md5_hex():
    result = chunk_markdown(SIMPLE_MD, doc_id="test-doc")
    for c in result:
        assert len(c.content_hash) == 32
        assert all(ch in "0123456789abcdef" for ch in c.content_hash)


def test_chunker_empty_markdown_returns_empty_list():
    result = chunk_markdown("", doc_id="test-doc")
    assert result == []


def test_chunker_no_headings_all_leaf():
    md = "Just some plain text with no headings at all."
    result = chunk_markdown(md, doc_id="test-doc")
    assert len(result) > 0
    for c in result:
        assert c.heading_level == "LEAF"
        assert c.granularity == "SMALL"
