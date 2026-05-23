"""Text processing utilities."""

import tiktoken

_encoder = tiktoken.get_encoding("cl100k_base")


def count_tokens(text: str) -> int:
    """Count the number of tokens in a text using tiktoken cl100k_base."""
    if not text:
        return 0
    return len(_encoder.encode(text))
