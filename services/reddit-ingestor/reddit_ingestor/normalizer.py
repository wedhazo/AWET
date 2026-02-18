"""Deterministic text normalization and content hashing.

Every function in this module is pure — no I/O, no randomness.
The content_hash algorithm is specified in the Avro schema docs::

    sha256(norm_title + "\\n" + norm_body + "|" + sub + "|" + pid + "|" + ts)
"""
from __future__ import annotations

import hashlib
import re
import unicodedata

# Pre-compiled patterns
_ZERO_WIDTH = re.compile(
    "["
    "\u200b"  # zero-width space
    "\u200c"  # zero-width non-joiner
    "\u200d"  # zero-width joiner
    "\u2060"  # word joiner
    "\ufeff"  # byte order mark / zero-width no-break space
    "\u00ad"  # soft hyphen
    "]+",
)
_MULTI_WS = re.compile(r"[ \t]+")
_MULTI_NL = re.compile(r"\n{3,}")


def normalize_text(text: str) -> str:
    """Normalize a text string deterministically.

    Steps:
        1. NFC Unicode normalization (canonical decomposition + composition).
        2. Strip zero-width / invisible characters.
        3. Collapse runs of horizontal whitespace into a single space.
        4. Collapse 3+ consecutive newlines into 2.
        5. Strip leading/trailing whitespace.
    """
    # 1. Unicode NFC
    text = unicodedata.normalize("NFC", text)
    # 2. Remove zero-width chars
    text = _ZERO_WIDTH.sub("", text)
    # 3. Collapse whitespace (tabs/spaces → single space, per line)
    text = _MULTI_WS.sub(" ", text)
    # 4. Collapse excessive newlines
    text = _MULTI_NL.sub("\n\n", text)
    # 5. Trim
    return text.strip()


def content_hash(
    *,
    title: str,
    body: str,
    subreddit: str,
    post_id: str,
    created_at_utc: str,
) -> str:
    """Compute the deterministic SHA-256 content hash.

    Formula (from schema doc):
        sha256(normalized_title + "\\n" + normalized_body
               + "|" + subreddit + "|" + post_id + "|" + created_at_utc)

    All text inputs are normalized before hashing.
    """
    payload = (
        normalize_text(title)
        + "\n"
        + normalize_text(body)
        + "|"
        + subreddit
        + "|"
        + post_id
        + "|"
        + created_at_utc
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def author_hash(username: str) -> str:
    """SHA-256 of the raw username — no PII stored downstream."""
    return hashlib.sha256(username.encode("utf-8")).hexdigest()
