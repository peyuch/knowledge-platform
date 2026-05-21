"""Tests for hash utilities."""

from utils.hash import sha256_hex, md5_hex


def test_sha256_hex_returns_64_chars():
    result = sha256_hex(b"hello")
    assert len(result) == 64
    assert all(c in "0123456789abcdef" for c in result)


def test_sha256_hex_deterministic():
    assert sha256_hex(b"abc") == sha256_hex(b"abc")


def test_sha256_hex_different_inputs_different_hash():
    assert sha256_hex(b"a") != sha256_hex(b"b")


def test_md5_hex_returns_32_chars():
    result = md5_hex("hello world")
    assert len(result) == 32


def test_md5_hex_deterministic():
    assert md5_hex("same text") == md5_hex("same text")
