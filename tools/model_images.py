"""Resolve GHCR images to immutable references and inspect source revisions."""

import hashlib
import json
import os
import re

import requests

_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
_ACCEPT = ", ".join((
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.docker.distribution.manifest.v2+json",
))


def image_reference(model_image: str, model_digest: str | None) -> str:
    """Build an immutable image reference, rejecting missing or invalid digests.

    Args:
        model_image: Image repository without a tag or digest.
        model_digest: SHA-256 manifest or index digest.

    Returns:
        Repository and digest joined with an at sign.

    Raises:
        ValueError: If the digest is invalid.
    """
    if not _DIGEST.fullmatch(model_digest or ""):
        raise ValueError("model_digest must be sha256: followed by 64 lowercase hex digits")
    return f"{model_image}@{model_digest}"


def _verified_json(response: requests.Response, expected_digest: str | None = None) -> tuple[dict, str]:
    """Decode registry JSON and verify its content-addressed identity.

    Args:
        response: Successful registry response containing raw JSON bytes.
        expected_digest: Digest requested by the caller, if any.

    Returns:
        Parsed document and its computed SHA-256 digest.

    Raises:
        RuntimeError: If the requested or advertised digest mismatches the bytes.
    """
    digest = "sha256:" + hashlib.sha256(response.content).hexdigest()
    advertised = response.headers.get("Docker-Content-Digest")
    if (expected_digest and expected_digest != digest) or (advertised and advertised != digest):
        raise RuntimeError("Registry content does not match the image digest")
    return json.loads(response.content), digest


def resolve_model_image(
    model_image: str, model_tag: str = "latest", model_digest: str | None = None,
) -> dict:
    """Resolve a GHCR tag or digest and read source labels from the pinned image.

    Args:
        model_image: GHCR repository, optionally including an embedded digest.
        model_tag: Tag to resolve when no digest is supplied; blank means latest.
        model_digest: Optional digest taking precedence over the tag.

    Returns:
        Normalized repository, requested tag, resolved digest and source revision.
        The source revision is unavailable when runnable platforms disagree or
        an image lacks the revision label. Attestation manifests are ignored.

    Raises:
        ValueError: If the repository or digest is malformed or contradictory.
        RuntimeError: If image metadata is invalid or its digest mismatches.
        requests.RequestException: If registry access or authentication fails.
    """
    model_image = model_image.strip()
    model_tag = model_tag.strip() or "latest"
    model_digest = model_digest.strip() if model_digest is not None else None
    if "@" in model_image:
        model_image, embedded = model_image.split("@", 1)
        if model_digest is not None and embedded != model_digest:
            raise ValueError("Embedded image digest conflicts with model_digest")
        model_digest = embedded
    if not re.fullmatch(r"ghcr\.io/[a-z0-9]+(?:[._-][a-z0-9]+)*(?:/[a-z0-9]+(?:[._-]+[a-z0-9]+)*)+", model_image):
        raise ValueError("model_image must be a GHCR repository without an image tag")
    if model_digest is not None:
        image_reference(model_image, model_digest)
    elif not re.fullmatch(r"[\w][\w.-]{0,127}", model_tag, flags=re.ASCII):
        raise ValueError("Invalid model_tag")
    repository = model_image.removeprefix("ghcr.io/")
    base_url = f"https://ghcr.io/v2/{repository}"

    with requests.Session() as session:
        token_response = session.get(
            "https://ghcr.io/token",
            params={"service": "ghcr.io", "scope": f"repository:{repository}:pull"},
            auth=(os.environ.get("GHCR_USERNAME", ""), os.environ["GHCR_TOKEN"])
            if os.environ.get("GHCR_TOKEN") else None,
            timeout=30,
        )
        token_response.raise_for_status()
        token = token_response.json().get("token") or token_response.json().get("access_token")
        if not token:
            raise RuntimeError("GHCR returned no registry token")
        session.headers.update({"Authorization": f"Bearer {token}", "Accept": _ACCEPT})

        def fetch(path: str, expected: str | None = None) -> tuple[dict, str]:
            """Read and verify one registry document.

            Args:
                path: Repository-relative manifest or blob endpoint.
                expected: Expected content digest, if specified.

            Returns:
                Parsed registry document and computed digest.
            """
            response = session.get(f"{base_url}/{path}", timeout=30)
            response.raise_for_status()
            return _verified_json(response, expected)

        manifest, digest = fetch(f"manifests/{model_digest or model_tag}", model_digest)

        def revisions(document: dict, depth: int = 0) -> list[str | None]:
            """Read revision labels across runnable manifests in an image index.

            Args:
                document: Image manifest or image index.
                depth: Current index nesting depth.

            Returns:
                Source revision labels, including missing labels as None.

            Raises:
                RuntimeError: If an index is too deeply nested or malformed.
            """
            if depth > 4:
                raise RuntimeError("Image index nesting exceeds supported depth")
            if "manifests" in document:
                result = []
                for descriptor in document["manifests"]:
                    platform = descriptor.get("platform", {})
                    if platform.get("os") == "unknown" or descriptor.get("annotations", {}).get(
                        "vnd.docker.reference.type"
                    ) == "attestation-manifest":
                        continue
                    child_digest = descriptor["digest"]
                    image_reference(model_image, child_digest)
                    child, _ = fetch(f"manifests/{child_digest}", child_digest)
                    result.extend(revisions(child, depth + 1))
                if not result:
                    raise RuntimeError("Image index contains no runnable image manifests")
                return result
            config_digest = document.get("config", {}).get("digest")
            image_reference(model_image, config_digest)
            config, _ = fetch(f"blobs/{config_digest}", config_digest)
            label = ((config.get("config") or {}).get("Labels") or {}).get(
                "org.opencontainers.image.revision"
            )
            # Only report a full Git SHA, never an arbitrary label value.
            return [label if isinstance(label, str) and re.fullmatch(r"[0-9a-fA-F]{40}|[0-9a-fA-F]{64}", label) else None]

        labels = revisions(manifest)
        source_revision = labels[0] if all(label == labels[0] for label in labels) else None
    return {
        "model_image": model_image,
        "model_tag": model_tag,
        "model_digest": digest,
        "model_source_revision": source_revision,
    }
