"""Sharding utilities for mapping QIDs to lakeFS object paths.

All helpers are intentionally internal; do not expose the resulting paths
outside the server. The 2-2-2 sharding scheme keeps bucket/object listings
manageable while maintaining deterministic lookups from a QID.
"""

from __future__ import annotations


def shard_qid(qid: str) -> str:
    """Return the sharded directory prefix for a QID using 2-2-2 padding.

    Args:
        qid: Identifier beginning with ``Q`` followed by digits.

    Returns:
        str: Sharded prefix in the form ``pp/qq/rr/Qxxxx``.
    """
    normalized = qid.upper()
    if not normalized.startswith("Q"):
        raise ValueError("QID must start with 'Q'")
    digits = normalized[1:]
    if not digits.isdigit():
        raise ValueError("QID must contain digits after 'Q'")
    padded = digits.zfill(6)
    return f"{padded[0:2]}/{padded[2:4]}/{padded[4:6]}/{normalized}"


def get_component_path(qid: str, component_id: str, extension: str) -> str:
    """Build the sharded component path (without branch/repo prefixes).

    Args:
        qid: QID of the object.
        component_id: Component identifier (e.g., ``primary``).
        extension: File extension with or without leading dot.

    Returns:
        str: Relative lakeFS path ``pp/qq/rr/Qxxxx/components/<component>.<ext>``.
    """
    ext = extension.lstrip(".")
    prefix = shard_qid(qid)
    return f"{prefix}/components/{component_id}.{ext}" if ext else f"{prefix}/components/{component_id}"


__all__ = ["shard_qid", "get_component_path"]
