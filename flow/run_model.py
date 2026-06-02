import json
import mimetypes
import os
import random
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from prefect import flow, task
from prefect.logging import get_run_logger
from kubernetes import client, config as k8s_config
import lakefs
from lakefs.client import Client
from tools.k8_tools import _check_for_stuck_pods, _collect_pod_logs
from tools.sharding import shard_qid


LAKEFS_DATA_REPO = "data-raw"
LAKEFS_RUN_REPO  = "model-runs"
LAKEFS_BRANCH    = "main"


def mint_qid() -> str:
    return f"Q{int(time.time())}{random.randint(0, 999):03d}"


def lakefs_uri_to_http(uri: str) -> str:
    """
    Convert a lakefs:// URI to the lakeFS HTTP API object URL.

    lakefs://model-runs/main/run-id/input/file.json
    → https://<LAKEFS_HOST>/api/v1/repositories/model-runs/refs/main/objects
      ?path=run-id%2Finput%2Ffile.json&presign=false
    """
    without_scheme = uri[len("lakefs://"):]
    repo, branch, *parts = without_scheme.split("/")
    path = "/".join(parts)
    host = os.environ["LAKEFS_HOST"].rstrip("/")
    return (
        f"{host}/api/v1/repositories/{repo}/refs/{branch}/objects"
        f"?path={quote(path, safe='')}&presign=false"
    )


def _lakefs_client() -> Client:
    return Client(
        host=os.environ["LAKEFS_HOST"],
        username=os.environ["LAKEFS_ACCESS_KEY"],
        password=os.environ["LAKEFS_SECRET_KEY"],
    )


@task
def stage_input(input_path: str, config_json: str, qid: str):
    """
    Copy the input file from data-raw and write config.json into the run path.

    input_path:   e.g. lakefs://data-raw/main/grippeweb/grippeweb-2026-W20.tsv
    config_json: JSON string written verbatim as config.json
    Writes to:
      lakefs://model-runs/main/<qid>/input/data.tsv
      lakefs://model-runs/main/<qid>/input/config.json
    """
    logger = get_run_logger()

    src_repo, src_branch, path = input_path.replace("lakefs://", "").split("/", 2)
    dst_prefix = f"{shard_qid(qid)}/components/input"
    dst_data   = f"lakefs://{LAKEFS_RUN_REPO}/{LAKEFS_BRANCH}/{dst_prefix}/data.tsv"
    dst_config = f"lakefs://{LAKEFS_RUN_REPO}/{LAKEFS_BRANCH}/{dst_prefix}/config.json"

    lc = _lakefs_client()

    logger.info(f"Downloading input: {input_path}")
    try:
        data = (
            lakefs.repository(src_repo, client=lc)
            .branch(src_branch)
            .object(path)
            .reader()
            .read()
        )
        logger.info(f"Downloaded {len(data)} bytes from {input_path}")
    except Exception as e:
        raise RuntimeError(
            f"Failed to read input file from LakeFS: {input_path} — "
            f"make sure the file exists and credentials are correct."
        ) from e

    dst_branch_handle = lakefs.repository(LAKEFS_RUN_REPO, client=lc).branch(LAKEFS_BRANCH)

    logger.info(f"Staging data.tsv -> {dst_data}")
    try:
        dst_branch_handle.object(f"{dst_prefix}/data.tsv").upload(
            data=data, content_type="application/octet-stream"
        )
        logger.info("data.tsv staged successfully")
    except Exception as e:
        raise RuntimeError(f"Failed to stage data.tsv to {dst_data}") from e

    logger.info(f"Staging config.json -> {dst_config}")
    try:
        dst_branch_handle.object(f"{dst_prefix}/config.json").upload(
            data=config_json.encode(), content_type="application/json"
        )
        logger.info("config.json staged successfully")
    except Exception as e:
        raise RuntimeError(f"Failed to stage config.json to {dst_config}") from e


@task
def write_metadata(qid: str, model_image: str, model_tag: str, run_start: datetime, status: str):
    computation_time = int((datetime.now(timezone.utc) - run_start).total_seconds())
    end_time = run_start + timedelta(seconds=computation_time)
    model_name = model_image.split('/')[-1]

    lc = _lakefs_client()
    branch_handle = lakefs.repository(LAKEFS_RUN_REPO, client=lc).branch(LAKEFS_BRANCH)
    file_entities = []
    input_refs = []
    output_refs = []
    sharded = shard_qid(qid)
    prefixes = [f"{sharded}/components/input/"]
    if status == "success":
        prefixes.append(f"{sharded}/components/output/")
    for obj in (o for p in prefixes for o in branch_handle.objects(prefix=p)):
        rel_path = obj.path[len(f"{sharded}/"):]
        mime, _ = mimetypes.guess_type(obj.path)
        entity = {"@id": rel_path, "@type": "File", "name": obj.path.split("/")[-1]}
        if mime:
            entity["encodingFormat"] = mime
        file_entities.append(entity)
        if rel_path.startswith("components/input/"):
            input_refs.append({"@id": rel_path})
        else:
            output_refs.append({"@id": rel_path})

    action_status = (
        "https://schema.org/CompletedActionStatus"
        if status == "success"
        else "https://schema.org/FailedActionStatus"
    )
    software_id = f"#{model_name}"

    metadata = json.dumps({
        "@context": "https://w3id.org/ro/crate/1.1/context",
        "@graph": [
            {
                "@id": "ro-crate-metadata.json",
                "@type": "CreativeWork",
                "conformsTo": [
                    {"@id": "https://w3id.org/ro/crate/1.1"},
                    {"@id": "https://w3id.org/ro/wfrun/process/0.4"},
                ],
                "about": {"@id": "./"},
            },
            {
                "@id": "./",
                "@type": "Dataset",
                "identifier":    qid,
                "name":          model_name,
                "description":   f"Model run of {model_name} (tag: {model_tag})",
                "datePublished": run_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "license":       "unknown",
                "hasPart":       [{"@id": e["@id"]} for e in file_entities],
                "mentions":      [{"@id": "#run"}],
            },
            {
                "@id": "#run",
                "@type": "CreateAction",
                "instrument":   {"@id": software_id},
                "object":       input_refs,
                "result":       output_refs,
                "startTime":    run_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "endTime":      end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "actionStatus": {"@id": action_status},
            },
            {
                "@id": software_id,
                "@type": "SoftwareApplication",
                "name":            model_name,
                "softwareVersion": model_tag,
                "url":             model_image,
            },
            *file_entities,
        ],
    }, indent=2).encode()

    branch_handle \
        .object(f"{sharded}/ro-crate-metadata.json") \
        .upload(data=metadata, content_type="application/json")
    branch_handle.commit(message=f"add ro-crate metadata for {qid}")


@task
def submit_and_wait(run_id: str, model_image: str, model_tag: str, qid: str, namespace: str = "default"):
    """
    Submit a Kubernetes Job with the three-container pattern and wait for completion.

    Pod structure:
      init: lakefs-pull  → downloads qid/input/ from LakeFS → /work/input/
      init: model        → reads /work/input/, writes /work/output/
      container: lakefs-push → uploads /work/output/ → LakeFS qid/output/
    """
    logger = get_run_logger()

    k8s_config.load_incluster_config()
    batch_v1 = client.BatchV1Api()
    core_v1  = client.CoreV1Api()

    lakefs_run_path = f"lakefs://{LAKEFS_RUN_REPO}/{LAKEFS_BRANCH}/{shard_qid(qid)}"
    lakefs_host = os.environ["LAKEFS_HOST"]

    lakefs_env = [
        client.V1EnvVar(name="LAKECTL_SERVER_ENDPOINT_URL", value=lakefs_host),
        client.V1EnvVar(
            name="LAKECTL_CREDENTIALS_ACCESS_KEY_ID",
            value_from=client.V1EnvVarSource(
                secret_key_ref=client.V1SecretKeySelector(
                    name="lakefs-credentials", key="lakefs-access-key"
                )
            ),
        ),
        client.V1EnvVar(
            name="LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY",
            value_from=client.V1EnvVarSource(
                secret_key_ref=client.V1SecretKeySelector(
                    name="lakefs-credentials", key="lakefs-secret-key"
                )
            ),
        ),
    ]

    workdir_mount = client.V1VolumeMount(name="workdir", mount_path="/work")

    job = client.V1Job(
        metadata=client.V1ObjectMeta(name=run_id, namespace=namespace),
        spec=client.V1JobSpec(
            backoff_limit=0,
            ttl_seconds_after_finished=600,
            template=client.V1PodTemplateSpec(
                spec=client.V1PodSpec(
                    restart_policy="Never",
                    init_containers=[
                        client.V1Container(
                            name="lakefs-pull",
                            image="treeverse/lakectl:latest",
                            command=["/bin/sh", "-c"],
                            args=[
                                f"mkdir -p /work/input /work/output && "
                                f"lakectl fs download {lakefs_run_path}/components/input/data.tsv /work/input/data.tsv && "
                                f"lakectl fs download {lakefs_run_path}/components/input/config.json /work/input/config.json"
                            ],
                            env=lakefs_env,
                            volume_mounts=[workdir_mount],
                        ),
                        client.V1Container(
                            name="model",
                            image=f"{model_image}:{model_tag}",
                            volume_mounts=[workdir_mount],
                        ),
                    ],
                    containers=[
                        client.V1Container(
                            name="lakefs-push",
                            image="treeverse/lakectl:latest",
                            command=["/bin/sh", "-c"],
                            args=[
                                f"lakectl fs upload --source /work/output -r {lakefs_run_path}/components/output/ && "
                                f"lakectl commit lakefs://{LAKEFS_RUN_REPO}/{LAKEFS_BRANCH} -m 'model run {run_id}'"
                            ],
                            env=lakefs_env,
                            volume_mounts=[workdir_mount],
                        ),
                    ],
                    volumes=[
                        client.V1Volume(
                            name="workdir",
                            empty_dir=client.V1EmptyDirVolumeSource(),
                        )
                    ],
                )
            ),
        ),
    )

    logger.info(f"Submitting Kubernetes Job: {run_id}")
    batch_v1.create_namespaced_job(namespace=namespace, body=job)
    logger.info("Job submitted, waiting for completion...")

    while True:
        status = batch_v1.read_namespaced_job(name=run_id, namespace=namespace).status
        if status.succeeded:
            logger.info(f"Job {run_id} completed successfully")
            break
        if status.failed:
            pod_logs = _collect_pod_logs(core_v1, run_id, namespace)
            raise RuntimeError(
                f"Job {run_id} failed\n\nPod logs:\n{pod_logs}"
            )
        _check_for_stuck_pods(core_v1, run_id, namespace)
        time.sleep(5)


@flow
def model_pipeline(
    input_path: str,
    model_image: str,
    config_json: str,
    model_tag: str = "latest",
    namespace: str = "default",
):
    """
    Run a model container on Kubernetes, reading input from LakeFS and
    writing output back to LakeFS.

    Args:
        input_path:   LakeFS path to the input TSV,
                      e.g. lakefs://data-raw/main/grippeweb/grippeweb-2026-W20.tsv
        model_image:  GHCR image name,
                      e.g. ghcr.io/the-episerve-consortium/model__prediction__grippeweb__baseline-nullmodel
        config_json: JSON string written verbatim as config.json in the input directory,
                      e.g. {"horizon_weeks": 4, "n_reference_weeks": 4}
        model_tag:    Image tag
        namespace:    Kubernetes namespace
    """
    model_image = model_image.strip()
    model_tag = model_tag.strip()
    run_start = datetime.now(timezone.utc)
    qid = mint_qid()
    run_id = f"model-runner-{qid.lower()}"

    stage_input(input_path=input_path, config_json=config_json, qid=qid)
    status = "failed"
    try:
        submit_and_wait(
            run_id=run_id,
            model_image=model_image,
            model_tag=model_tag,
            qid=qid,
            namespace=namespace,
        )
        status = "success"
    finally:
        write_metadata(
            qid=qid,
            model_image=model_image,
            model_tag=model_tag,
            run_start=run_start,
            status=status,
        )

    return f"lakefs://{LAKEFS_RUN_REPO}/{LAKEFS_BRANCH}/{shard_qid(qid)}/components/output/"
