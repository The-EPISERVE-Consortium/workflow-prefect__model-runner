import os
import re
import time
from datetime import datetime
from prefect import flow, task
from prefect.logging import get_run_logger
from kubernetes import client, config as k8s_config
import lakefs
from lakefs.client import Client


LAKEFS_ENDPOINT  = "https://lake-episerve.zib.de/"
LAKEFS_DATA_REPO = "data-raw"
LAKEFS_RUN_REPO  = "model-runs"
LAKEFS_BRANCH    = "main"


def _lakefs_client() -> Client:
    return Client(
        host=os.environ.get("LAKEFS_HOST", LAKEFS_ENDPOINT),
        username=os.environ["LAKEFS_ACCESS_KEY"],
        password=os.environ["LAKEFS_SECRET_KEY"],
    )


@task
def stage_input(input_path: str, config_json: str, run_id: str):
    """
    Copy the input file from data-raw and write config.json into the run path.

    input_path:   e.g. lakefs://data-raw/main/grippeweb/grippeweb-2026-W20.tsv
    config_json: JSON string written verbatim as config.json
    Writes to:
      lakefs://model-runs/main/<run-id>/input/data.tsv
      lakefs://model-runs/main/<run-id>/input/config.json
    """
    logger = get_run_logger()

    src_repo, src_branch, path = input_path.replace("lakefs://", "").split("/", 2)
    dst_prefix = f"{run_id}/input"
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


def _collect_pod_logs(core_v1: client.CoreV1Api, run_id: str, namespace: str) -> str:
    """Collect logs from all containers of the Job's pod."""
    try:
        pods = core_v1.list_namespaced_pod(
            namespace=namespace,
            label_selector=f"job-name={run_id}",
        ).items
        if not pods:
            return "(no pods found)"
        pod_name = pods[0].metadata.name
        lines = []
        for container in ["lakefs-pull", "model", "lakefs-push"]:
            try:
                log = core_v1.read_namespaced_pod_log(
                    name=pod_name, namespace=namespace,
                    container=container, tail_lines=50,
                )
                lines.append(f"--- {container} ---\n{log}")
            except Exception as e:
                lines.append(f"--- {container} --- (could not retrieve: {e})")
        return "\n".join(lines)
    except Exception as e:
        return f"(could not collect pod logs: {e})"


@task
def submit_and_wait(run_id: str, model_image: str, model_tag: str, namespace: str = "default"):
    """
    Submit a Kubernetes Job with the three-container pattern and wait for completion.

    Pod structure:
      init: lakefs-pull  → downloads run_id/input/ from LakeFS → /work/input/
      init: model        → reads /work/input/, writes /work/output/
      container: lakefs-push → uploads /work/output/ → LakeFS run_id/output/
    """
    logger = get_run_logger()

    k8s_config.load_incluster_config()
    batch_v1 = client.BatchV1Api()
    core_v1  = client.CoreV1Api()

    lakefs_run_path = f"lakefs://{LAKEFS_RUN_REPO}/{LAKEFS_BRANCH}/{run_id}"
    lakefs_host = os.environ.get("LAKEFS_HOST", LAKEFS_ENDPOINT)

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
                                f"lakectl fs download {lakefs_run_path}/input/data.tsv /work/input/data.tsv && "
                                f"lakectl fs download {lakefs_run_path}/input/config.json /work/input/config.json"
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
                                f"find /work/output -type f | while read f; do "
                                f"rel=\"${{f#/work/output/}}\"; "
                                f"lakectl fs upload \"$f\" {lakefs_run_path}/output/\"$rel\"; "
                                f"done"
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
    timestamp = f"{datetime.now():%Y%m%d-%H%M%S}"
    slug = model_image.split('/')[-1]
    slug = re.sub(r'[^a-z0-9-]', '-', slug)   # replace invalid chars
    slug = re.sub(r'-+', '-', slug).strip('-') # collapse and trim hyphens
    slug = slug[:63 - len(timestamp) - 1]      # enforce 63-char limit
    run_id = f"{slug}-{timestamp}"

    stage_input(input_path=input_path, config_json=config_json, run_id=run_id)
    submit_and_wait(
        run_id=run_id,
        model_image=model_image,
        model_tag=model_tag,
        namespace=namespace,
    )

    return f"lakefs://{LAKEFS_RUN_REPO}/{LAKEFS_BRANCH}/{run_id}/output/"
