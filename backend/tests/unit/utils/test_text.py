"""Tests for text utilities."""

from utils.text import count_tokens


def test_count_tokens_empty_string():
    assert count_tokens("") == 0


def test_count_tokens_english():
    tokens = count_tokens("hello world")
    assert tokens > 0


def test_count_tokens_chinese():
    tokens = count_tokens("你好世界")
    assert tokens > 0


def test_count_tokens_large_text():
    text = "词 " * 1000
    tokens = count_tokens(text)
    assert tokens > 500
