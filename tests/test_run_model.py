import re
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from flow.run_model import mint_qid, model_pipeline
from tasks.stage_input import stage_input
from tasks.submit_and_wait import submit_and_wait
from tasks.write_metadata import write_metadata, _build_fdo
from tools.lakefs_helpers import LAKEFS_DATA_REPO, LAKEFS_RUN_REPO, LAKEFS_BRANCH
from tools.sharding import shard_qid

INPUT_PATH        = "lakefs://data-raw/main/grippeweb/grippeweb-2026-W20.tsv"
QID               = "Q1748526042817"
RUN_ID            = f"model-runner-{QID.lower()}"
MODEL_CONFIG_JSON = '{"horizon_weeks": 4, "n_reference_weeks": 4}'
MODEL_IMAGE       = "ghcr.io/the-episerve-consortium/model__prediction__grippeweb__baseline-nullmodel"
MODEL_TAG         = "v0.1.0"
FAKE_DATA         = b"week\tcases\n2026-W20\t42\n"


@pytest.fixture(autouse=True)
def mock_logger():
    logger = MagicMock()
    with (
        patch("tasks.stage_input.get_run_logger", return_value=logger),
        patch("tasks.submit_and_wait.get_run_logger", return_value=logger),
    ):
        yield


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    monkeypatch.setenv("LAKEFS_HOST", "https://fake-lakefs/")
    monkeypatch.setenv("LAKEFS_ACCESS_KEY", "fake-access-key")
    monkeypatch.setenv("LAKEFS_SECRET_KEY", "fake-secret-key")


def _lakefs_mocks(*, src_get_error=None, dst_upload_errors=None):
    src_obj = MagicMock()
    if src_get_error:
        src_obj.reader.side_effect = src_get_error
    else:
        src_obj.reader.return_value.read.return_value = FAKE_DATA

    dst_obj = MagicMock()
    if dst_upload_errors is not None:
        dst_obj.upload.side_effect = dst_upload_errors

    src_branch_mock = MagicMock()
    src_branch_mock.object.return_value = src_obj

    dst_branch_mock = MagicMock()
    dst_branch_mock.object.return_value = dst_obj

    src_repo_mock = MagicMock()
    src_repo_mock.branch.return_value = src_branch_mock

    dst_repo_mock = MagicMock()
    dst_repo_mock.branch.return_value = dst_branch_mock

    def repo_factory(name, client=None):
        return src_repo_mock if name == LAKEFS_DATA_REPO else dst_repo_mock

    return repo_factory, src_branch_mock, dst_branch_mock, dst_obj


def _stage_patches(repo_factory):
    return (
        patch("tasks.stage_input.lakefs_client"),
        patch("tasks.stage_input.lakefs.repository", side_effect=repo_factory),
    )


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


# ── mint_qid ──────────────────────────────────────────────────────────────────

def test_mint_qid_format():
    assert re.match(r"^Q\d+$", mint_qid())


def test_mint_qid_unique():
    assert mint_qid() != mint_qid()


# ── run_id / return path ──────────────────────────────────────────────────────

def test_return_path_uses_qid():
    with patch("flow.run_model.stage_input"), patch("flow.run_model.submit_and_wait"), patch("flow.run_model.write_metadata"):
        result = model_pipeline.fn(
            input_path=INPUT_PATH,
            model_image=MODEL_IMAGE,
            config_json=MODEL_CONFIG_JSON,
        )
    # lakefs://model-runs/main/pp/qq/rr/Qxxxx/components/output/
    assert re.search(r"/Q\d+/components/output/$", result)


def test_k8s_job_name_format():
    with patch("flow.run_model.stage_input"), patch("flow.run_model.submit_and_wait") as mock_submit, patch("flow.run_model.write_metadata"):
        model_pipeline.fn(
            input_path=INPUT_PATH,
            model_image=MODEL_IMAGE,
            config_json=MODEL_CONFIG_JSON,
        )
    run_id = mock_submit.call_args.kwargs["run_id"]
    assert re.match(r"^model-runner-q\d+$", run_id)
    assert len(run_id) <= 63


# ── input path parsing ────────────────────────────────────────────────────────

def test_input_path_parsing():
    repo, branch, path = INPUT_PATH.replace("lakefs://", "").split("/", 2)
    assert repo == "data-raw"
    assert branch == "main"
    assert path == "grippeweb/grippeweb-2026-W20.tsv"


# ── stage_input ───────────────────────────────────────────────────────────────

def test_stage_input_raises_when_file_missing():
    repo_factory, _, _, _ = _lakefs_mocks(src_get_error=Exception("404 Not Found"))
    patches = _stage_patches(repo_factory)
    with patches[0], patches[1]:
        with pytest.raises(RuntimeError, match="Failed to read input file from LakeFS"):
            stage_input.fn(input_path=INPUT_PATH, config_json=MODEL_CONFIG_JSON, qid=QID)


def test_stage_input_raises_when_data_upload_fails():
    repo_factory, _, _, _ = _lakefs_mocks(dst_upload_errors=Exception("permission denied"))
    patches = _stage_patches(repo_factory)
    with patches[0], patches[1]:
        with pytest.raises(RuntimeError, match="Failed to stage data.tsv"):
            stage_input.fn(input_path=INPUT_PATH, config_json=MODEL_CONFIG_JSON, qid=QID)


def test_stage_input_raises_when_config_upload_fails():
    repo_factory, _, _, _ = _lakefs_mocks(dst_upload_errors=[None, Exception("permission denied")])
    patches = _stage_patches(repo_factory)
    with patches[0], patches[1]:
        with pytest.raises(RuntimeError, match="Failed to stage config.json"):
            stage_input.fn(input_path=INPUT_PATH, config_json=MODEL_CONFIG_JSON, qid=QID)


def test_stage_input_calls_get_and_upload():
    repo_factory, src_branch_mock, dst_branch_mock, dst_obj = _lakefs_mocks()
    patches = _stage_patches(repo_factory)
    with patches[0], patches[1]:
        stage_input.fn(input_path=INPUT_PATH, config_json=MODEL_CONFIG_JSON, qid=QID)

    src_branch_mock.object.assert_called_once_with("grippeweb/grippeweb-2026-W20.tsv")
    assert dst_obj.upload.call_count == 2
    sharded = shard_qid(QID)
    paths = [c.args[0] for c in dst_branch_mock.object.call_args_list]
    assert f"{sharded}/components/input/data.tsv" in paths
    assert f"{sharded}/components/input/config.json" in paths


def test_stage_input_config_uploaded_verbatim():
    repo_factory, _, dst_branch_mock, dst_obj = _lakefs_mocks()
    patches = _stage_patches(repo_factory)
    with patches[0], patches[1]:
        stage_input.fn(input_path=INPUT_PATH, config_json=MODEL_CONFIG_JSON, qid=QID)

    config_idx = next(
        i for i, c in enumerate(dst_branch_mock.object.call_args_list)
        if "config.json" in c.args[0]
    )
    assert dst_obj.upload.call_args_list[config_idx].kwargs["data"] == MODEL_CONFIG_JSON.encode()


# ── submit_and_wait ───────────────────────────────────────────────────────────

def test_job_spec():
    batch_v1 = _k8s_batch_mock(succeeded=True)
    with (
        patch("tasks.submit_and_wait.k8s_config.load_incluster_config"),
        patch("tasks.submit_and_wait.client.BatchV1Api", return_value=batch_v1),
        patch("time.sleep"),
    ):
        submit_and_wait.fn(run_id=RUN_ID, model_image=MODEL_IMAGE, model_tag=MODEL_TAG, qid=QID)

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
        patch("tasks.submit_and_wait.k8s_config.load_incluster_config"),
        patch("tasks.submit_and_wait.client.BatchV1Api", return_value=batch_v1),
        patch("time.sleep"),
    ):
        submit_and_wait.fn(run_id=RUN_ID, model_image=MODEL_IMAGE, model_tag=MODEL_TAG, qid=QID)

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
        patch("tasks.submit_and_wait.k8s_config.load_incluster_config"),
        patch("tasks.submit_and_wait.client.BatchV1Api", return_value=batch_v1),
        patch("time.sleep"),
    ):
        submit_and_wait.fn(run_id=RUN_ID, model_image=MODEL_IMAGE, model_tag=MODEL_TAG, qid=QID)

    # one pending poll → one succeeded poll → break
    assert batch_v1.read_namespaced_job.call_count == 2


def test_submit_raises_on_stuck_image():
    batch_v1 = MagicMock()
    batch_v1.read_namespaced_job.return_value.status = MagicMock(succeeded=None, failed=None)

    cs = MagicMock()
    cs.name = "model"
    cs.state.waiting.reason = "InvalidImageName"
    cs.state.waiting.message = "invalid reference format"

    core_v1 = MagicMock()
    core_v1.list_namespaced_pod.return_value.items = [
        MagicMock(status=MagicMock(init_container_statuses=[cs], container_statuses=[]))
    ]

    with (
        patch("tasks.submit_and_wait.k8s_config.load_incluster_config"),
        patch("tasks.submit_and_wait.client.BatchV1Api", return_value=batch_v1),
        patch("tasks.submit_and_wait.client.CoreV1Api", return_value=core_v1),
        patch("time.sleep"),
    ):
        with pytest.raises(RuntimeError, match="InvalidImageName"):
            submit_and_wait.fn(run_id=RUN_ID, model_image=MODEL_IMAGE, model_tag=MODEL_TAG, qid=QID)


def test_submit_raises_on_failure():
    batch_v1 = _k8s_batch_mock(succeeded=False)
    with (
        patch("tasks.submit_and_wait.k8s_config.load_incluster_config"),
        patch("tasks.submit_and_wait.client.BatchV1Api", return_value=batch_v1),
        patch("time.sleep"),
    ):
        with pytest.raises(RuntimeError, match=RUN_ID):
            submit_and_wait.fn(run_id=RUN_ID, model_image=MODEL_IMAGE, model_tag=MODEL_TAG, qid=QID)


# ── model_pipeline ────────────────────────────────────────────────────────────

def test_pipeline_return_path():
    with (
        patch("flow.run_model.stage_input") as mock_stage,
        patch("flow.run_model.submit_and_wait") as mock_submit,
        patch("flow.run_model.write_metadata"),
    ):
        result = model_pipeline.fn(
            input_path=INPUT_PATH,
            model_image=MODEL_IMAGE,
            config_json=MODEL_CONFIG_JSON,
        )

    assert result.startswith(f"lakefs://{LAKEFS_RUN_REPO}/{LAKEFS_BRANCH}/")
    assert result.endswith("/components/output/")
    mock_stage.assert_called_once()
    mock_submit.assert_called_once()


# ── _build_fdo ────────────────────────────────────────────────────────────────

import json as _json
from datetime import timezone

_END_TIME = datetime(2026, 6, 3, 12, 0, 0, tzinfo=timezone.utc)

_FILE_ENTITIES = [
    {"@id": "components/input/data.tsv", "@type": "File", "name": "data.tsv", "encodingFormat": "text/tab-separated-values"},
    {"@id": "components/input/config.json", "@type": "File", "name": "config.json", "encodingFormat": "application/json"},
    {"@id": "components/output/forecast.csv", "@type": "File", "name": "forecast.csv", "encodingFormat": "text/csv"},
    {"@id": "components/output/summary.json", "@type": "File", "name": "summary.json", "encodingFormat": "application/json"},
]


def test_fdo_top_level_structure():
    fdo = _json.loads(_build_fdo(QID, MODEL_IMAGE, MODEL_TAG, _END_TIME, _FILE_ENTITIES))
    assert fdo["@id"] == QID
    assert fdo["@type"] == "DigitalObject"
    assert {"schema", "prov", "fdo"} <= {k for ctx in fdo["@context"] if isinstance(ctx, dict) for k in ctx}


def test_fdo_kernel_fields():
    fdo = _json.loads(_build_fdo(QID, MODEL_IMAGE, MODEL_TAG, _END_TIME, _FILE_ENTITIES))
    kernel = fdo["kernel"]
    assert kernel["@id"] == QID
    assert kernel["primaryIdentifier"] == QID
    assert kernel["digitalObjectType"] == "https://schema.org/Dataset"
    assert kernel["modified"] == "2026-06-03T12:00:00Z"


def test_fdo_kernel_components_includes_input_and_output():
    fdo = _json.loads(_build_fdo(QID, MODEL_IMAGE, MODEL_TAG, _END_TIME, _FILE_ENTITIES))
    components = fdo["kernel"]["fdo:hasComponent"]
    assert len(components) == 4
    ids = {c["@id"] for c in components}
    assert "components/input/data.tsv" in ids
    assert "components/input/config.json" in ids
    assert "components/output/forecast.csv" in ids
    assert "components/output/summary.json" in ids
    tsv_comp = next(c for c in components if c["@id"] == "components/input/data.tsv")
    assert tsv_comp["componentId"] == "data.tsv"
    assert tsv_comp["mediaType"] == "text/tab-separated-values"


def test_fdo_kernel_components_empty_when_no_files():
    fdo = _json.loads(_build_fdo(QID, MODEL_IMAGE, MODEL_TAG, _END_TIME, []))
    assert fdo["kernel"]["fdo:hasComponent"] == []


def test_fdo_profile():
    fdo = _json.loads(_build_fdo(QID, MODEL_IMAGE, MODEL_TAG, _END_TIME, _FILE_ENTITIES))
    profile = fdo["profile"]
    assert profile["@type"] == "Dataset"
    assert profile["@id"] == QID
    assert profile["name"] == MODEL_IMAGE.split("/")[-1]
    assert MODEL_TAG in profile["description"]
    assert profile["url"] == MODEL_IMAGE


def test_fdo_provenance():
    fdo = _json.loads(_build_fdo(QID, MODEL_IMAGE, MODEL_TAG, _END_TIME, _FILE_ENTITIES))
    prov = fdo["provenance"]
    assert prov["prov:generatedAtTime"] == "2026-06-03T12:00:00Z"
    assert prov["prov:wasAttributedTo"] == f"{MODEL_IMAGE}:{MODEL_TAG}"


def test_fdo_component_no_media_type_when_unknown():
    entity = {"@id": "components/output/result.bin", "@type": "File", "name": "result.bin"}
    fdo = _json.loads(_build_fdo(QID, MODEL_IMAGE, MODEL_TAG, _END_TIME, [entity]))
    comp = fdo["kernel"]["fdo:hasComponent"][0]
    assert "mediaType" not in comp


# ── write_metadata uploads fdo ────────────────────────────────────────────────

def test_write_metadata_uploads_fdo():
    branch_mock = MagicMock()
    obj_mock = MagicMock()
    branch_mock.object.return_value = obj_mock
    branch_mock.objects.return_value = iter([])

    with (
        patch("tasks.write_metadata.lakefs_client"),
        patch("tasks.write_metadata.lakefs.repository") as mock_repo,
    ):
        mock_repo.return_value.branch.return_value = branch_mock
        write_metadata.fn(
            qid=QID,
            model_image=MODEL_IMAGE,
            model_tag=MODEL_TAG,
            run_start=_END_TIME,
            status="success",
        )

    uploaded_paths = [call.args[0] for call in branch_mock.object.call_args_list]
    sharded = shard_qid(QID)
    assert f"{sharded}/{QID}.fdo.json" in uploaded_paths
