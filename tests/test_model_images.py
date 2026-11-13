"""Tests for immutable model selection and registry metadata verification."""

import hashlib
import json
from unittest.mock import MagicMock, patch

import pytest
import requests

from tools.model_images import resolve_model_image

IMAGE = "ghcr.io/example/model"
REVISION = "1" * 40


def _document(value):
    """Encode a registry document and calculate its identity.

    Args:
        value: Registry JSON document.

    Returns:
        Response mock and its content digest.
    """
    content = json.dumps(value).encode()
    digest = "sha256:" + hashlib.sha256(content).hexdigest()
    response = MagicMock(content=content, headers={"Docker-Content-Digest": digest})
    return response, digest


def _registry(revision=REVISION, indexed=False):
    """Create fake GHCR token, manifest, index and config responses.

    Args:
        revision: Source label to include; None omits it.
        indexed: Whether to wrap the image with an index and attestation.

    Returns:
        Mock session, immutable image digest and requested endpoint list.
    """
    config, config_digest = _document({"config": {"Labels": {
        "org.opencontainers.image.revision": revision,
    }}})
    manifest, manifest_digest = _document({"schemaVersion": 2, "config": {"digest": config_digest}})
    responses = {
        f"https://ghcr.io/v2/example/model/blobs/{config_digest}": config,
        f"https://ghcr.io/v2/example/model/manifests/{manifest_digest}": manifest,
    }
    top, digest = manifest, manifest_digest
    if indexed:
        top, digest = _document({"schemaVersion": 2, "manifests": [
            {"digest": manifest_digest, "platform": {"os": "linux", "architecture": "amd64"}},
            {"digest": "sha256:" + "0" * 64, "platform": {"os": "unknown", "architecture": "unknown"}},
        ]})
    responses[f"https://ghcr.io/v2/example/model/manifests/latest"] = top
    responses[f"https://ghcr.io/v2/example/model/manifests/{digest}"] = top
    token = MagicMock()
    token.json.return_value = {"token": "test-token"}
    responses["https://ghcr.io/token"] = token
    session = MagicMock()
    session.__enter__.return_value = session
    session.get.side_effect = lambda url, **kwargs: responses[url]
    return session, digest, responses


@pytest.mark.parametrize("indexed", [False, True])
def test_latest_resolves_digest_and_source_from_image_config(indexed):
    """Resolve both single manifests and Buildx indexes without pulling layers.

    Args:
        indexed: Whether the fake image has a Buildx-style index.
    """
    session, digest, _ = _registry(indexed=indexed)
    with patch("tools.model_images.requests.Session", return_value=session):
        result = resolve_model_image(IMAGE)
    assert result == {
        "model_image": IMAGE, "model_tag": "latest", "model_digest": digest,
        "model_source_revision": REVISION,
    }
    assert not any("layers" in call.args[0] for call in session.get.call_args_list)


@pytest.mark.parametrize("embedded", [False, True])
def test_explicit_digest_never_looks_up_latest(embedded):
    """A pinned rerun only reads content addressed manifests and configs.

    Args:
        embedded: Whether the digest is embedded in model_image.
    """
    session, digest, responses = _registry(indexed=True)
    del responses["https://ghcr.io/v2/example/model/manifests/latest"]
    with patch("tools.model_images.requests.Session", return_value=session):
        result = resolve_model_image(
            f"{IMAGE}@{digest}" if embedded else IMAGE,
            model_digest=None if embedded else digest,
        )
    assert result["model_digest"] == digest
    assert result["model_image"] == IMAGE


def test_version_tag_is_resolved_once():
    """Image labels are read through the manifest obtained for the selected tag."""
    session, digest, responses = _registry()
    responses["https://ghcr.io/v2/example/model/manifests/v1"] = responses.pop(
        "https://ghcr.io/v2/example/model/manifests/latest"
    )
    with patch("tools.model_images.requests.Session", return_value=session):
        result = resolve_model_image(IMAGE, "v1")
    assert result["model_tag"] == "v1"
    assert result["model_digest"] == digest


@pytest.mark.parametrize("revision", [None, "latest", "untrusted-label"])
def test_missing_or_invalid_source_is_unavailable(revision):
    """An old image can be pinned without claiming a source Git SHA.

    Args:
        revision: Missing or non-SHA label value.
    """
    session, digest, _ = _registry(revision=revision)
    with patch("tools.model_images.requests.Session", return_value=session):
        result = resolve_model_image(IMAGE)
    assert result["model_digest"] == digest
    assert result["model_source_revision"] is None


def test_registry_failure_is_not_replaced_with_latest():
    """Inaccessible or deleted images stop execution."""
    session, _, _ = _registry()
    session.get.side_effect = requests.HTTPError("Image not found")
    with patch("tools.model_images.requests.Session", return_value=session):
        with pytest.raises(requests.HTTPError):
            resolve_model_image(IMAGE)


def test_digest_mismatch_is_rejected():
    """Unexpected registry bytes cannot silently replace a pinned image."""
    session, digest, responses = _registry()
    forged = "sha256:" + "f" * 64
    responses[f"https://ghcr.io/v2/example/model/manifests/{forged}"] = responses[
        f"https://ghcr.io/v2/example/model/manifests/{digest}"
    ]
    with patch("tools.model_images.requests.Session", return_value=session):
        with pytest.raises(RuntimeError, match="does not match"):
            resolve_model_image(IMAGE, model_digest=forged)


@pytest.mark.parametrize("image,digest", [
    (IMAGE, "sha256:123"),
    (IMAGE, ""),
    (IMAGE, "   "),
    (IMAGE + "@sha256:" + "a" * 64, "sha256:" + "b" * 64),
    ("https://ghcr.io/example/model", None),
    ("ghcr.io/example/model:latest", None),
])
def test_invalid_or_conflicting_references_fail_before_network(image, digest):
    """Malformed inputs must not become registry requests.

    Args:
        image: Invalid or conflicting image reference.
        digest: Invalid or conflicting explicit digest.
    """
    with patch("tools.model_images.requests.Session") as session:
        with pytest.raises(ValueError):
            resolve_model_image(image, model_digest=digest)
    session.assert_not_called()


def test_private_registry_credentials_are_only_used_for_token_exchange(monkeypatch):
    """Configured GHCR credentials are exchanged for a scoped pull token.

    Args:
        monkeypatch: Pytest environment override helper.
    """
    monkeypatch.setenv("GHCR_USERNAME", "example")
    monkeypatch.setenv("GHCR_TOKEN", "secret-test-token")
    session, _, _ = _registry()
    with patch("tools.model_images.requests.Session", return_value=session):
        resolve_model_image(IMAGE)
    token_call = session.get.call_args_list[0]
    assert token_call.kwargs["auth"] == ("example", "secret-test-token")
    assert token_call.kwargs["params"]["scope"] == "repository:example/model:pull"
    assert all("auth" not in call.kwargs for call in session.get.call_args_list[1:])


def test_advertised_digest_is_checked():
    """The registry's advertised identity must match the manifest bytes."""
    session, _, responses = _registry()
    responses["https://ghcr.io/v2/example/model/manifests/latest"].headers = {
        "Docker-Content-Digest": "sha256:" + "0" * 64,
    }
    with patch("tools.model_images.requests.Session", return_value=session):
        with pytest.raises(RuntimeError, match="does not match"):
            resolve_model_image(IMAGE)


def test_platform_disagreement_does_not_claim_a_single_source_revision():
    """Different platform source labels make the source revision unavailable."""
    session, _, responses = _registry(indexed=True)
    config, config_digest = _document({"config": {"Labels": {
        "org.opencontainers.image.revision": "2" * 40,
    }}})
    manifest, manifest_digest = _document({"config": {"digest": config_digest}})
    responses[f"https://ghcr.io/v2/example/model/blobs/{config_digest}"] = config
    responses[f"https://ghcr.io/v2/example/model/manifests/{manifest_digest}"] = manifest
    top = json.loads(responses["https://ghcr.io/v2/example/model/manifests/latest"].content)
    top["manifests"].append({"digest": manifest_digest, "platform": {"os": "linux", "architecture": "arm64"}})
    responses["https://ghcr.io/v2/example/model/manifests/latest"], _ = _document(top)
    with patch("tools.model_images.requests.Session", return_value=session):
        result = resolve_model_image(IMAGE)
    assert result["model_source_revision"] is None


def test_config_digest_is_checked():
    """The source label must come from the configuration pinned by the manifest."""
    session, _, responses = _registry()
    config_url = next(url for url in responses if "/blobs/" in url)
    responses[config_url].content = b'{"config": {"Labels": {}}}'
    with patch("tools.model_images.requests.Session", return_value=session):
        with pytest.raises(RuntimeError, match="does not match"):
            resolve_model_image(IMAGE)
