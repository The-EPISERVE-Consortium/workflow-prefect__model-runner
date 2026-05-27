import json
import re
from unittest.mock import MagicMock, patch

import pytest

from flows.run_model import (
    stage_input,
    submit_and_wait,
    model_pipeline,
    LAKEFS_DATA_REPO,
    LAKEFS_RUN_REPO,
    LAKEFS_BRANCH,
    LAKEFS_ENDPOINT,
)

INPUT_PATH       = "lakefs://data-raw/main/grippeweb/grippeweb-2026-W20.tsv"
RUN_ID           = "model__prediction__grippeweb__baseline-nullmodel-20260527-143022"
MODEL_CONFIG_JSON = '{"horizon_weeks": 4, "n_reference_weeks": 4}'
MODEL_IMAGE      = "ghcr.io/the-episerve-consortium/model__prediction__grippeweb__baseline-nullmodel"
MODEL_TAG        = "v0.1.0"
FAKE_DATA        = b"week\tcases\n2026-W20\t42\n"


def _lakefs_mocks():
    objects_api = MagicMock()
    objects_api.get_object.return_value = FAKE_DATA
    api_client = MagicMock()
    api_client.__enter__ = MagicMock(return_value=api_client)
    api_client.__exit__ = MagicMock(return_value=False)
    return api_client, objects_api


def _k8s_batch_mock(*, succeeded: bool):
    batch_v1 = MagicMock()
    pending = MagicMock(succeeded=None, failed=None)
    terminal = MagicMock(
        succeeded=1 if succeeded else None,
        failed=None if succeeded else 1,
    )
    batch_v1.read_namespaced_job.side_effect = [
        MagicMock(status=pending),
        MagicMock(status=terminal),
    ]
    return batch_v1


# ── run_id format ─────────────────────────────────────────────────────────────

def test_run_id_format():
    with patch("flows.run_model.stage_input"), patch("flows.run_model.submit_and_wait"):
        result = model_pipeline.fn(
            input_path=INPUT_PATH,
            model_image=MODEL_IMAGE,
            model_config=MODEL_CONFIG_JSON,
        )
    # lakefs://model-runs/main/<run-id>/output/  →  index 4
    run_id = result.split("/")[4]
    assert re.match(
        r"^model__prediction__grippeweb__baseline-nullmodel-\d{8}-\d{6}$",
        run_id,
    )


# ── input path parsing ────────────────────────────────────────────────────────

def test_input_path_parsing():
    repo, branch, path = INPUT_PATH.replace("lakefs://", "").split("/", 2)
    assert repo == "data-raw"
    assert branch == "main"
    assert path == "grippeweb/grippeweb-2026-W20.tsv"


# ── stage_input ───────────────────────────────────────────────────────────────

def test_stage_input_calls_get_and_upload():
    api_client, objects_api = _lakefs_mocks()
    with (
        patch("flows.run_model.lakefs_client", return_value=api_client),
        patch("flows.run_model.lakefs_sdk.ObjectsApi", return_value=objects_api),
    ):
        stage_input.fn(input_path=INPUT_PATH, model_config=MODEL_CONFIG_JSON, run_id=RUN_ID)

    objects_api.get_object.assert_called_once_with(
        LAKEFS_DATA_REPO, LAKEFS_BRANCH, "grippeweb/grippeweb-2026-W20.tsv"
    )
    assert objects_api.upload_object.call_count == 2
    paths = [c.args[2] for c in objects_api.upload_object.call_args_list]
    assert f"{RUN_ID}/input/data.tsv" in paths
    assert f"{RUN_ID}/input/config.json" in paths


def test_stage_input_config_uploaded_verbatim():
    api_client, objects_api = _lakefs_mocks()
    with (
        patch("flows.run_model.lakefs_client", return_value=api_client),
        patch("flows.run_model.lakefs_sdk.ObjectsApi", return_value=objects_api),
    ):
        stage_input.fn(input_path=INPUT_PATH, model_config=MODEL_CONFIG_JSON, run_id=RUN_ID)

    config_call = next(
        c for c in objects_api.upload_object.call_args_list
        if c.args[2].endswith("config.json")
    )
    assert config_call.kwargs["content"] == MODEL_CONFIG_JSON.encode()


# ── submit_and_wait ───────────────────────────────────────────────────────────

def test_job_spec():
    batch_v1 = _k8s_batch_mock(succeeded=True)
    with (
        patch("flows.run_model.k8s_config.load_incluster_config"),
        patch("flows.run_model.client.BatchV1Api", return_value=batch_v1),
        patch("time.sleep"),
    ):
        submit_and_wait.fn(run_id=RUN_ID, model_image=MODEL_IMAGE, model_tag=MODEL_TAG)

    job = batch_v1.create_namespaced_job.call_args.kwargs["body"]
    assert job.metadata.name == RUN_ID
    assert job.spec.backoff_limit == 0
    pod = job.spec.template.spec
    assert len(pod.init_containers) == 2
    assert len(pod.containers) == 1
    assert pod.init_containers[0].name == "lakefs-pull"
    assert pod.init_containers[1].name == "model"
    assert pod.init_containers[1].image == f"{MODEL_IMAGE}:{MODEL_TAG}"
    assert pod.containers[0].name == "lakefs-push"
    assert any(v.name == "workdir" for v in pod.volumes)


def test_job_secret_refs():
    batch_v1 = _k8s_batch_mock(succeeded=True)
    with (
        patch("flows.run_model.k8s_config.load_incluster_config"),
        patch("flows.run_model.client.BatchV1Api", return_value=batch_v1),
        patch("time.sleep"),
    ):
        submit_and_wait.fn(run_id=RUN_ID, model_image=MODEL_IMAGE, model_tag=MODEL_TAG)

    job = batch_v1.create_namespaced_job.call_args.kwargs["body"]
    # lakefs-pull carries the credentials for lakectl
    env = job.spec.template.spec.init_containers[0].env
    secret_refs = {
        e.name: e.value_from.secret_key_ref
        for e in env
        if e.value_from and e.value_from.secret_key_ref
    }
    assert set(secret_refs) == {
        "LAKECTL_CREDENTIALS_ACCESS_KEY_ID",
        "LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY",
    }
    for ref in secret_refs.values():
        assert ref.name == "lakefs-credentials"
    assert secret_refs["LAKECTL_CREDENTIALS_ACCESS_KEY_ID"].key == "lakefs-access-key"
    assert secret_refs["LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY"].key == "lakefs-secret-key"


def test_submit_polls_until_succeeded():
    batch_v1 = _k8s_batch_mock(succeeded=True)
    with (
        patch("flows.run_model.k8s_config.load_incluster_config"),
        patch("flows.run_model.client.BatchV1Api", return_value=batch_v1),
        patch("time.sleep"),
    ):
        submit_and_wait.fn(run_id=RUN_ID, model_image=MODEL_IMAGE, model_tag=MODEL_TAG)

    # one pending poll → one succeeded poll → break
    assert batch_v1.read_namespaced_job.call_count == 2


def test_submit_raises_on_failure():
    batch_v1 = _k8s_batch_mock(succeeded=False)
    with (
        patch("flows.run_model.k8s_config.load_incluster_config"),
        patch("flows.run_model.client.BatchV1Api", return_value=batch_v1),
        patch("time.sleep"),
    ):
        with pytest.raises(RuntimeError, match=RUN_ID):
            submit_and_wait.fn(run_id=RUN_ID, model_image=MODEL_IMAGE, model_tag=MODEL_TAG)


# ── model_pipeline ────────────────────────────────────────────────────────────

def test_pipeline_return_path():
    with (
        patch("flows.run_model.stage_input") as mock_stage,
        patch("flows.run_model.submit_and_wait") as mock_submit,
    ):
        result = model_pipeline.fn(
            input_path=INPUT_PATH,
            model_image=MODEL_IMAGE,
            model_config=MODEL_CONFIG_JSON,
        )

    assert result.startswith(f"lakefs://{LAKEFS_RUN_REPO}/{LAKEFS_BRANCH}/")
    assert result.endswith("/output/")
    mock_stage.assert_called_once()
    mock_submit.assert_called_once()
