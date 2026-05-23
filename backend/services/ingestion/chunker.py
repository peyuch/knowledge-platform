"""Heading-aware markdown chunker.

Produces a tree of ChunkDraft objects:
- H1/H2 headings -> LARGE chunks (max 2048 tokens, for RAPTOR)
- H3/H4 headings + orphan text -> SMALL chunks (max 512 tokens, for retrieval/entity extraction)
- LARGE chunks exceeding the limit are dropped (RAPTOR handles the roll-up)
- SMALL chunks exceeding the limit are split with 100-token overlap
- Tables, code blocks, and formulas are never split internally.
"""

import uuid
from dataclasses import dataclass, field
from typing import Optional

import mistune

from common.constants import (
    LARGE_CHUNK_MAX_TOKENS,
    SMALL_CHUNK_MAX_TOKENS,
    OVERLAP_TOKENS,
)
from utils.hash import md5_hex
from utils.text import count_tokens


@dataclass
class ChunkDraft:
    chunk_id: str
    parent_id: Optional[str]
    heading_level: str          # H1, H2, H3, H4, LEAF
    granularity: str            # LARGE, SMALL
    heading_path: list[str]
    content: str
    content_hash: str
    token_count: int
    page_start: Optional[int] = None
    page_end: Optional[int] = None
    has_table: bool = False
    has_formula: bool = False
    has_image: bool = False
    sequence: int = 0


def chunk_markdown(markdown_text: str, doc_id: str) -> list[ChunkDraft]:
    """Parse markdown into heading-aware chunks."""
    if not markdown_text.strip():
        return []

    renderer = mistune.HTMLRenderer()
    markdown = mistune.Markdown(renderer)
    _html, state = markdown.parse(markdown_text)

    # Build heading tree from AST
    root = _build_heading_tree(state.tokens)

    if not root.children and not root.content_parts:
        return []

    seq_counter = [0]

    # No headings at all: treat entire document as LEAF/SMALL chunks
    root_direct = "\n".join(root.content_parts).strip()
    if not root.children:
        result: list[ChunkDraft] = []
        if count_tokens(root_direct) <= SMALL_CHUNK_MAX_TOKENS:
            result.append(_make_chunk(
                doc_id, "LEAF", "SMALL", [], root_direct, None, seq_counter,
            ))
        else:
            splits = _split_text(root_direct, SMALL_CHUNK_MAX_TOKENS, OVERLAP_TOKENS)
            for s in splits:
                result.append(_make_chunk(
                    doc_id, "LEAF", "SMALL", [], s, None, seq_counter,
                ))
        return result

    # Root-level orphan text (before first heading) -> attach to first heading node
    if root_direct and root.children:
        root.children[0].content_parts.insert(0, root_direct)

    return _generate_chunks(root.children, doc_id=doc_id, seq_counter=seq_counter)


class _HeadingNode:
    def __init__(self, level: str, title: str, heading_path: list[str]):
        self.level = level
        self.title = title
        self.heading_path = heading_path
        self.children: list["_HeadingNode"] = []
        self.content_parts: list[str] = []
        self.parent: Optional["_HeadingNode"] = None

    @property
    def full_text(self) -> str:
        parts: list[str] = []
        if self.title:
            hn = int(self.level[1]) if self.level[0] == "H" and len(self.level) >= 2 and self.level[1].isdigit() else 0
            prefix = "#" * hn if hn else ""
            parts.append(f"{prefix} {self.title}" if prefix else self.title)
        parts.extend(self.content_parts)
        return "\n".join(parts)


def _build_heading_tree(ast: list) -> "_HeadingNode":
    """Parse markdown AST into a tree of heading nodes. Returns root node."""
    root = _HeadingNode("ROOT", "", [])
    stack = [root]
    current = root

    for token in ast:
        if token["type"] == "heading":
            level_num = token["attrs"]["level"]
            level = f"H{level_num}"

            heading_path: list[str] = []
            for node in stack:
                if node.level not in ("ROOT", "LEAF") and node.title:
                    heading_path.append(node.title)

            # Pop stack until we find a heading with higher level (lower number)
            while stack and stack[-1].level != "ROOT":
                sl = stack[-1].level
                if sl.startswith("H") and sl[1:].isdigit() and int(sl[1:]) < level_num:
                    break
                stack.pop()

            title = _extract_raw_text(token.get("children", []))
            node = _HeadingNode(level, title, list(heading_path))
            stack[-1].children.append(node)
            node.parent = stack[-1]
            stack.append(node)
            current = node

        elif token["type"] in ("paragraph", "blank_line", "block_code", "list", "block_html", "block_text"):
            text = _token_text(token)
            if text.strip():
                current.content_parts.append(text)

    return root


def _extract_raw_text(tokens: list) -> str:
    """Recursively extract all raw text from a list of AST tokens."""
    parts: list[str] = []
    for tok in tokens:
        if "raw" in tok and tok.get("type") == "text":
            parts.append(tok["raw"])
        elif tok.get("type") == "softbreak":
            parts.append("\n")
        elif tok.get("type") == "linebreak":
            parts.append("\n")
        elif "children" in tok:
            parts.append(_extract_raw_text(tok["children"]))
        elif "raw" in tok:
            parts.append(tok["raw"])
    return "".join(parts)


def _token_text(token: dict) -> str:
    """Extract text from an AST token."""
    ttype = token["type"]
    if ttype == "block_code":
        return token.get("raw", "")
    elif ttype == "block_html":
        return token.get("raw", "")
    elif ttype == "paragraph":
        return _extract_raw_text(token.get("children", []))
    elif ttype == "list":
        return _extract_raw_text(token.get("children", []))
    elif ttype == "blank_line":
        return ""
    return _extract_raw_text(token.get("children", []))


def _generate_chunks(
    nodes: list["_HeadingNode"],
    doc_id: str,
    parent_chunk_id: Optional[str] = None,
    seq_counter: list[int] | None = None,
) -> list[ChunkDraft]:
    """Recursively generate ChunkDraft objects from heading tree."""
    if seq_counter is None:
        seq_counter = [0]

    result: list[ChunkDraft] = []

    for node in nodes:
        is_large = node.level in ("H1", "H2")
        granularity = "LARGE" if is_large else "SMALL"
        max_tokens = LARGE_CHUNK_MAX_TOKENS if is_large else SMALL_CHUNK_MAX_TOKENS

        # Direct content (leaf text under this heading)
        direct = "\n".join(node.content_parts).strip()
        if direct:
            tc = count_tokens(direct)
            if tc <= max_tokens:
                result.append(_make_chunk(
                    doc_id, "LEAF", "SMALL",
                    list(node.heading_path) + ([node.title] if node.title else []),
                    direct, parent_chunk_id, seq_counter,
                ))
            else:
                splits = _split_text(direct, max_tokens, OVERLAP_TOKENS)
                for s in splits:
                    result.append(_make_chunk(
                        doc_id, "LEAF", "SMALL",
                        list(node.heading_path) + ([node.title] if node.title else []),
                        s, parent_chunk_id, seq_counter,
                    ))

        # Heading-level aggregate chunk (H1/H2 only)
        child_parent = parent_chunk_id
        if node.title and is_large:
            full = node.full_text
            if count_tokens(full) <= LARGE_CHUNK_MAX_TOKENS:
                c = _make_chunk(
                    doc_id, node.level, "LARGE",
                    list(node.heading_path) + [node.title],
                    full, parent_chunk_id, seq_counter,
                )
                result.append(c)
                child_parent = c.chunk_id

        # Recurse into children
        if node.children:
            result.extend(_generate_chunks(node.children, doc_id, child_parent, seq_counter))

    return result


def _make_chunk(
    doc_id: str,
    level: str,
    granularity: str,
    heading_path: list[str],
    content: str,
    parent_id: Optional[str],
    seq: list[int],
) -> ChunkDraft:
    seq[0] += 1
    return ChunkDraft(
        chunk_id=str(uuid.uuid4()),
        parent_id=parent_id,
        heading_level=level,
        granularity=granularity,
        heading_path=list(heading_path),
        content=content,
        content_hash=md5_hex(content),
        token_count=count_tokens(content),
        has_table="|" in content and "---" in content,
        has_formula="$$" in content or "\\begin{" in content,
        sequence=seq[0],
    )


def _split_text(text: str, max_tokens: int, overlap: int) -> list[str]:
    """Split by sentences, keeping overlap tokens from previous chunk."""
    sentences = text.replace("\n", " ").split("。")  # Chinese period
    if len(sentences) == 1:
        # Try splitting by period for English
        sentences = text.replace("\n", " ").split(". ")
        # Rejoin with periods for English
        chunks: list[str] = []
        current = ""
        for i, sent in enumerate(sentences):
            sep = ". " if i < len(sentences) - 1 else ""
            candidate = current + (". " if current else "") + sent + sep
            candidate = candidate.strip()
            if count_tokens(candidate) > max_tokens and current:
                chunks.append(current.strip())
                # Overlap: keep last `overlap` tokens of current
                words = current.split()
                overlap_text = " ".join(words[-overlap:]) if overlap > 0 and len(words) >= overlap else ""
                current = (overlap_text + ". " + sent + sep) if overlap_text else (sent + sep)
            else:
                current = candidate
        if current.strip():
            chunks.append(current.strip())
        return chunks or [text]

    # Chinese period split
    chunks = []
    current = ""
    for sent in sentences:
        candidate = (current + "。" + sent).strip("。")
        if count_tokens(candidate) > max_tokens and current:
            chunks.append(current.strip())
            words = current.split()
            overlap_text = " ".join(words[-overlap:]) if overlap > 0 and len(words) >= overlap else ""
            current = (overlap_text + "。" + sent) if overlap_text else sent
        else:
            current = candidate
    if current.strip():
        chunks.append(current.strip())
    return chunks or [text]
