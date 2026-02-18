"""Cryptographic determinism utilities for the AWET replay engine.

Provides ``compute_stage_hash`` which produces a stable SHA-256 hex digest
for a given payload dict.  The function deep-sorts all dictionary keys
recursively and serialises via ``json.dumps`` with compact separators so
that the output is identical across Python versions, platforms, and process
runs.

No floating-point tolerance hacks, no silent field exclusion.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any


def _deep_sort(obj: Any) -> Any:
    """Return a copy of *obj* with all nested dict keys sorted recursively.

    - ``dict``  → ``dict`` with keys in sorted order, values recursed.
    - ``list``  → ``list`` with each element recursed (order preserved).
    - scalars   → returned as-is.
    """
    if isinstance(obj, dict):
        return {k: _deep_sort(v) for k, v in sorted(obj.items())}
    if isinstance(obj, (list, tuple)):
        return [_deep_sort(item) for item in obj]
    return obj


def compute_stage_hash(payload: dict[str, Any]) -> str:
    """Return a deterministic SHA-256 hex digest for *payload*.

    Guarantees
    ----------
    * Deep-sorts dictionary keys recursively.
    * Serialises with ``json.dumps(..., separators=(",", ":"), sort_keys=True)``.
    * Returns a lowercase 64-character hex string.
    * Deterministic across runs, platforms, and Python versions.

    Parameters
    ----------
    payload:
        Arbitrary JSON-serialisable dictionary.

    Raises
    ------
    TypeError
        If *payload* contains values that cannot be serialised to JSON.
    """
    sorted_payload = _deep_sort(payload)
    canonical = json.dumps(sorted_payload, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
