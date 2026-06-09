import re
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from flow.run_model import mint_qid, model_pipeline
from tasks.stage_input import stage_input
from tasks.submit_and_wait import submit_and_wait
from tasks.write_metadata import write_metadata, _build_fdo, mint_model_qid
from tools.lakefs_helpers import LAKEFS_DATA_REPO, LAKEFS_RUN_REPO, LAKEFS_BRANCH
from tools.sharding import shard_qid

INPUT_DATA_FILES = [
    [
        "lakefs://data-processed/main/05/22/57/Q0522578154235/components/SARI-Hospitalisierungsinzidenz.parquet",
        "SARI-Hospitalisierungsinzidenz.parquet",
    ],
    [
        "lakefs://data-processed/main/32/74/12/Q3274128860531/components/GrippeWeb_Daten_des_Wochenberichts.parquet",
        "GrippeWeb_Daten_des_Wochenberichts.parquet",
    ],
]
SINGLE_INPUT_FILE = [INPUT_DATA_FILES[0]]
QID               = "Q1748526042817"
RUN_ID            = f"model-runner-{QID.lower()}"
MODEL_CONFIG_JSON        = '{"horizon_weeks": 4, "n_reference_weeks": 4}'
PREFECT_PAYLOAD_JSON     = '{"model_image": "ghcr.io/example/model", "model_tag": "v1"}'
MODEL_IMAGE       = "ghcr.io/the-episerve-consortium/model__prediction__grippeweb__baseline-nullmodel"
MODEL_TAG         = "v0.1.0"
FAKE_DATA         = b"fake-parquet-bytes"


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
            input_data_files=INPUT_DATA_FILES,
            model_image=MODEL_IMAGE,
            config_json=MODEL_CONFIG_JSON,
        )
    # lakefs://model-runs/main/pp/qq/rr/Qxxxx/components/output/
    assert re.search(r"/Q\d+/components/output/$", result)


def test_k8s_job_name_format():
    with patch("flow.run_model.stage_input"), patch("flow.run_model.submit_and_wait") as mock_submit, patch("flow.run_model.write_metadata"):
        model_pipeline.fn(
            input_data_files=INPUT_DATA_FILES,
            model_image=MODEL_IMAGE,
            config_json=MODEL_CONFIG_JSON,
        )
    run_id = mock_submit.call_args.kwargs["run_id"]
    assert re.match(r"^model-runner-q\d+$", run_id)
    assert len(run_id) <= 63


# ── stage_input ───────────────────────────────────────────────────────────────

HTTP_INPUT_FILE = [["https://example.com/data/input.parquet", "input.parquet"]]


def test_stage_input_downloads_http_uri():
    dst_obj = MagicMock()
    dst_branch_mock = MagicMock()
    dst_branch_mock.object.return_value = dst_obj
    dst_repo_mock = MagicMock()
    dst_repo_mock.branch.return_value = dst_branch_mock

    with (
        patch("tasks.stage_input.lakefs_client"),
        patch("tasks.stage_input.lakefs.repository", return_value=dst_repo_mock),
        patch("tasks.stage_input.requests.get") as mock_get,
    ):
        mock_get.return_value = MagicMock(content=FAKE_DATA, raise_for_status=MagicMock())
        stage_input.fn(input_data_files=HTTP_INPUT_FILE, config_json=MODEL_CONFIG_JSON, prefect_payload_json=PREFECT_PAYLOAD_JSON, qid=QID)

    mock_get.assert_called_once_with("https://example.com/data/input.parquet", timeout=120)
    sharded = shard_qid(QID)
    dst_paths = [c.args[0] for c in dst_branch_mock.object.call_args_list]
    assert f"{sharded}/components/input/input.parquet" in dst_paths


def test_stage_input_raises_when_http_download_fails():
    dst_repo_mock = MagicMock()
    dst_repo_mock.branch.return_value = MagicMock()

    with (
        patch("tasks.stage_input.lakefs_client"),
        patch("tasks.stage_input.lakefs.repository", return_value=dst_repo_mock),
        patch("tasks.stage_input.requests.get", side_effect=Exception("connection refused")),
    ):
        with pytest.raises(RuntimeError, match="Failed to read input file"):
            stage_input.fn(input_data_files=HTTP_INPUT_FILE, config_json=MODEL_CONFIG_JSON, prefect_payload_json=PREFECT_PAYLOAD_JSON, qid=QID)


def test_stage_input_raises_when_file_missing():
    repo_factory, _, _, _ = _lakefs_mocks(src_get_error=Exception("404 Not Found"))
    patches = _stage_patches(repo_factory)
    with patches[0], patches[1]:
        with pytest.raises(RuntimeError, match="Failed to read input file"):
            stage_input.fn(input_data_files=SINGLE_INPUT_FILE, config_json=MODEL_CONFIG_JSON, prefect_payload_json=PREFECT_PAYLOAD_JSON, qid=QID)


def test_stage_input_raises_when_data_upload_fails():
    repo_factory, _, _, _ = _lakefs_mocks(dst_upload_errors=Exception("permission denied"))
    patches = _stage_patches(repo_factory)
    with patches[0], patches[1]:
        with pytest.raises(RuntimeError, match="Failed to stage"):
            stage_input.fn(input_data_files=SINGLE_INPUT_FILE, config_json=MODEL_CONFIG_JSON, prefect_payload_json=PREFECT_PAYLOAD_JSON, qid=QID)


def test_stage_input_raises_when_config_upload_fails():
    # first call (data file) succeeds, second call (config.json) fails
    repo_factory, _, _, _ = _lakefs_mocks(dst_upload_errors=[None, Exception("permission denied")])
    patches = _stage_patches(repo_factory)
    with patches[0], patches[1]:
        with pytest.raises(RuntimeError, match="Failed to stage config.json"):
            stage_input.fn(input_data_files=SINGLE_INPUT_FILE, config_json=MODEL_CONFIG_JSON, prefect_payload_json=PREFECT_PAYLOAD_JSON, qid=QID)


def test_stage_input_raises_when_prefect_config_upload_fails():
    # data file + config.json succeed, config_prefect.json fails
    repo_factory, _, _, _ = _lakefs_mocks(dst_upload_errors=[None, None, Exception("permission denied")])
    patches = _stage_patches(repo_factory)
    with patches[0], patches[1]:
        with pytest.raises(RuntimeError, match="Failed to stage config_prefect.json"):
            stage_input.fn(input_data_files=SINGLE_INPUT_FILE, config_json=MODEL_CONFIG_JSON, prefect_payload_json=PREFECT_PAYLOAD_JSON, qid=QID)


def test_stage_input_calls_get_and_upload():
    repo_factory, src_branch_mock, dst_branch_mock, dst_obj = _lakefs_mocks()
    patches = _stage_patches(repo_factory)
    with patches[0], patches[1]:
        stage_input.fn(input_data_files=INPUT_DATA_FILES, config_json=MODEL_CONFIG_JSON, prefect_payload_json=PREFECT_PAYLOAD_JSON, qid=QID)

    # src: one read per input file
    src_paths_called = [c.args[0] for c in src_branch_mock.object.call_args_list]
    assert "05/22/57/Q0522578154235/components/SARI-Hospitalisierungsinzidenz.parquet" in src_paths_called
    assert "32/74/12/Q3274128860531/components/GrippeWeb_Daten_des_Wochenberichts.parquet" in src_paths_called

    # dst: one upload per input file + config.json + config_prefect.json
    assert dst_obj.upload.call_count == 4
    sharded = shard_qid(QID)
    dst_paths = [c.args[0] for c in dst_branch_mock.object.call_args_list]
    assert f"{sharded}/components/input/SARI-Hospitalisierungsinzidenz.parquet" in dst_paths
    assert f"{sharded}/components/input/GrippeWeb_Daten_des_Wochenberichts.parquet" in dst_paths
    assert f"{sharded}/components/input/config.json" in dst_paths
    assert f"{sharded}/components/input/config_prefect.json" in dst_paths


def test_stage_input_config_uploaded_verbatim():
    repo_factory, _, dst_branch_mock, dst_obj = _lakefs_mocks()
    patches = _stage_patches(repo_factory)
    with patches[0], patches[1]:
        stage_input.fn(input_data_files=SINGLE_INPUT_FILE, config_json=MODEL_CONFIG_JSON, prefect_payload_json=PREFECT_PAYLOAD_JSON, qid=QID)

    config_idx = next(
        i for i, c in enumerate(dst_branch_mock.object.call_args_list)
        if c.args[0].endswith("config.json")
    )
    assert dst_obj.upload.call_args_list[config_idx].kwargs["data"] == MODEL_CONFIG_JSON.encode()


def test_stage_input_prefect_payload_uploaded_verbatim():
    repo_factory, _, dst_branch_mock, dst_obj = _lakefs_mocks()
    patches = _stage_patches(repo_factory)
    with patches[0], patches[1]:
        stage_input.fn(input_data_files=SINGLE_INPUT_FILE, config_json=MODEL_CONFIG_JSON, prefect_payload_json=PREFECT_PAYLOAD_JSON, qid=QID)

    prefect_idx = next(
        i for i, c in enumerate(dst_branch_mock.object.call_args_list)
        if c.args[0].endswith("config_prefect.json")
    )
    assert dst_obj.upload.call_args_list[prefect_idx].kwargs["data"] == PREFECT_PAYLOAD_JSON.encode()


# ── submit_and_wait ───────────────────────────────────────────────────────────

def test_job_spec():
    batch_v1 = _k8s_batch_mock(succeeded=True)
    with (
        patch("tasks.submit_and_wait.k8s_config.load_incluster_config"),
        patch("tasks.submit_and_wait.client.BatchV1Api", return_value=batch_v1),
        patch("tasks.submit_and_wait.client.CoreV1Api", return_value=MagicMock()),
        patch("tasks.submit_and_wait.lakefs_client", return_value=MagicMock()),
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
        patch("tasks.submit_and_wait.client.CoreV1Api", return_value=MagicMock()),
        patch("tasks.submit_and_wait.lakefs_client", return_value=MagicMock()),
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
        patch("tasks.submit_and_wait.client.CoreV1Api", return_value=MagicMock()),
        patch("tasks.submit_and_wait.lakefs_client", return_value=MagicMock()),
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
        patch("tasks.submit_and_wait.client.CoreV1Api", return_value=MagicMock()),
        patch("tasks.submit_and_wait.lakefs_client", return_value=MagicMock()),
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
            input_data_files=INPUT_DATA_FILES,
            model_image=MODEL_IMAGE,
            config_json=MODEL_CONFIG_JSON,
        )

    assert result.startswith(f"lakefs://{LAKEFS_RUN_REPO}/{LAKEFS_BRANCH}/")
    assert result.endswith("/components/output/")
    mock_stage.assert_called_once()
    mock_submit.assert_called_once()


def test_pipeline_empty_model_tag_defaults_to_latest():
    with (
        patch("flow.run_model.stage_input"),
        patch("flow.run_model.submit_and_wait") as mock_submit,
        patch("flow.run_model.write_metadata"),
    ):
        model_pipeline.fn(
            input_data_files=INPUT_DATA_FILES,
            model_image=MODEL_IMAGE,
            model_tag="",
            config_json=MODEL_CONFIG_JSON,
        )

    _, kwargs = mock_submit.call_args
    assert kwargs["model_tag"] == "latest"


# ── _build_fdo ────────────────────────────────────────────────────────────────

import json as _json
from datetime import timezone

_START_TIME = datetime(2026, 6, 3, 11, 0, 0, tzinfo=timezone.utc)
_END_TIME   = datetime(2026, 6, 3, 12, 0, 0, tzinfo=timezone.utc)

_FILE_ENTITIES = [
    {"@id": "components/input/data.tsv", "@type": "File", "name": "data.tsv", "encodingFormat": "text/tab-separated-values"},
    {"@id": "components/input/config.json", "@type": "File", "name": "config.json", "encodingFormat": "application/json"},
    {"@id": "components/output/forecast.csv", "@type": "File", "name": "forecast.csv", "encodingFormat": "text/csv"},
    {"@id": "components/output/summary.json", "@type": "File", "name": "summary.json", "encodingFormat": "application/json"},
]

_INPUT_DATA_FILES = [
    ["lakefs://data-processed/main/aa/bb/cc/Q1111111111111/components/input.parquet", "input.parquet"],
    ["lakefs://data-processed/main/dd/ee/ff/Q2222222222222/components/extra.parquet", "extra.parquet"],
]
_SQL = ["SELECT * FROM df WHERE saison = '26'", ""]


def test_fdo_top_level_structure():
    fdo = _json.loads(_build_fdo(QID, MODEL_IMAGE, MODEL_TAG, _START_TIME, _END_TIME, _FILE_ENTITIES))
    assert fdo["@id"] == QID
    assert fdo["@type"] == "DigitalObject"
    assert {"schema", "prov", "fdo"} <= {k for ctx in fdo["@context"] if isinstance(ctx, dict) for k in ctx}


def test_fdo_kernel_fields():
    fdo = _json.loads(_build_fdo(QID, MODEL_IMAGE, MODEL_TAG, _START_TIME, _END_TIME, _FILE_ENTITIES))
    kernel = fdo["kernel"]
    assert kernel["@id"] == QID
    assert kernel["primaryIdentifier"] == QID
    assert kernel["digitalObjectType"] == "https://schema.org/Dataset"
    assert kernel["modified"] == "2026-06-03T12:00:00Z"


def test_fdo_kernel_components_includes_input_and_output():
    fdo = _json.loads(_build_fdo(QID, MODEL_IMAGE, MODEL_TAG, _START_TIME, _END_TIME, _FILE_ENTITIES))
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
    fdo = _json.loads(_build_fdo(QID, MODEL_IMAGE, MODEL_TAG, _START_TIME, _END_TIME, []))
    assert fdo["kernel"]["fdo:hasComponent"] == []


def test_fdo_profile():
    fdo = _json.loads(_build_fdo(QID, MODEL_IMAGE, MODEL_TAG, _START_TIME, _END_TIME, _FILE_ENTITIES))
    profile = fdo["profile"]
    assert profile["@type"] == "Dataset"
    assert profile["@id"] == QID
    assert profile["name"] == MODEL_IMAGE.split("/")[-1]
    assert MODEL_TAG in profile["description"]
    assert profile["url"] == MODEL_IMAGE


def test_fdo_provenance_is_activity():
    fdo = _json.loads(_build_fdo(QID, MODEL_IMAGE, MODEL_TAG, _START_TIME, _END_TIME, _FILE_ENTITIES))
    assert fdo["prov:wasGeneratedBy"] == {"@id": "#run"}
    prov = fdo["provenance"]
    assert prov["@id"] == "#run"
    assert prov["@type"] == "prov:Activity"
    assert prov["prov:startedAtTime"] == "2026-06-03T11:00:00Z"
    assert prov["prov:endedAtTime"] == "2026-06-03T12:00:00Z"
    assert prov["prov:used"] == []


def test_fdo_provenance_software_agent():
    fdo = _json.loads(_build_fdo(QID, MODEL_IMAGE, MODEL_TAG, _START_TIME, _END_TIME, _FILE_ENTITIES))
    agent = fdo["provenance"]["prov:wasAssociatedWith"]
    assert agent["@type"] == "prov:SoftwareAgent"
    assert agent["@id"] == f"{MODEL_IMAGE}:{MODEL_TAG}"
    assert agent["schema:softwareVersion"] == MODEL_TAG
    assert agent["schema:url"] == MODEL_IMAGE


def test_fdo_prov_used_source_uris():
    fdo = _json.loads(_build_fdo(QID, MODEL_IMAGE, MODEL_TAG, _START_TIME, _END_TIME, _FILE_ENTITIES, input_data_files=_INPUT_DATA_FILES))
    used = fdo["provenance"]["prov:used"]
    assert len(used) == 2
    assert used[0]["@id"] == "https://doip.episerve.zib.de/doip/retrieve/Q1111111111111/input.parquet"
    assert used[0]["@type"] == "prov:Entity"
    assert used[1]["@id"] == "https://doip.episerve.zib.de/doip/retrieve/Q2222222222222/extra.parquet"


def test_fdo_prov_used_includes_sql_when_present():
    fdo = _json.loads(_build_fdo(QID, MODEL_IMAGE, MODEL_TAG, _START_TIME, _END_TIME, _FILE_ENTITIES,
                                  input_data_files=_INPUT_DATA_FILES, data_transformation_sql=_SQL))
    used = fdo["provenance"]["prov:used"]
    assert used[0]["schema:query"] == _SQL[0]
    assert "schema:query" not in used[1]


def test_fdo_prov_used_no_sql_when_not_provided():
    fdo = _json.loads(_build_fdo(QID, MODEL_IMAGE, MODEL_TAG, _START_TIME, _END_TIME, _FILE_ENTITIES, input_data_files=_INPUT_DATA_FILES))
    used = fdo["provenance"]["prov:used"]
    assert all("schema:query" not in entry for entry in used)


def test_fdo_component_no_media_type_when_unknown():
    entity = {"@id": "components/output/result.bin", "@type": "File", "name": "result.bin"}
    fdo = _json.loads(_build_fdo(QID, MODEL_IMAGE, MODEL_TAG, _START_TIME, _END_TIME, [entity]))
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
            input_data_files=_INPUT_DATA_FILES,
        )

    uploaded_paths = [call.args[0] for call in branch_mock.object.call_args_list]
    sharded = shard_qid(QID)
    assert f"{sharded}/components/ro-crate-metadata.json" in uploaded_paths
    assert f"{sharded}/{QID}.fdo.json" in uploaded_paths

    fdo_call = next(c for c in obj_mock.upload.call_args_list if b"fdo:hasComponent" in c.kwargs.get("data", b""))
    fdo = _json.loads(fdo_call.kwargs["data"])
    component_ids = {c["@id"] for c in fdo["kernel"]["fdo:hasComponent"]}
    assert "components/ro-crate-metadata.json" in component_ids


def test_write_metadata_rocrate_uses_model_qid():
    """ro-crate CreateAction.instrument must reference the stable model QID."""
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
            input_data_files=_INPUT_DATA_FILES,
        )

    expected_model_qid = mint_model_qid(MODEL_IMAGE)

    rocrate_call = next(
        c for c in obj_mock.upload.call_args_list
        if b"ro/crate" in c.kwargs.get("data", b"")
    )
    rocrate = _json.loads(rocrate_call.kwargs["data"])
    graph = {node["@id"]: node for node in rocrate["@graph"]}

    run_node = graph["#run"]
    assert run_node["instrument"] == {"@id": expected_model_qid}

    software_node = graph[expected_model_qid]
    assert software_node["@type"] == "SoftwareApplication"
    assert software_node["identifier"] == expected_model_qid


def test_mint_model_qid_deterministic():
    assert mint_model_qid(MODEL_IMAGE) == mint_model_qid(MODEL_IMAGE)


def test_mint_model_qid_format():
    qid = mint_model_qid(MODEL_IMAGE)
    assert re.match(r"^Q\d{13}$", qid)
