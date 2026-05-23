"""File and text hashing utilities."""

import hashlib


def sha256_hex(data: bytes) -> str:
    """Compute SHA-256 hash of bytes, return hex string."""
    return hashlib.sha256(data).hexdigest()


def md5_hex(text: str) -> str:
    """Compute MD5 hash of a text string, return hex string."""
    return hashlib.md5(text.encode("utf-8")).hexdigest()
