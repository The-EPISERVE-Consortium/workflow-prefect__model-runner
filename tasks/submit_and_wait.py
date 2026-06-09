import json
import os
import time

from kubernetes import client, config as k8s_config
from prefect import task
from prefect.logging import get_run_logger

import lakefs
from tools.k8_tools import _check_for_stuck_pods, _collect_pod_logs
from tools.lakefs_helpers import LAKEFS_RUN_REPO, LAKEFS_BRANCH, lakefs_client
from tools.sharding import shard_qid

LAKECTL_PYTHON_IMAGE = "ghcr.io/the-episerve-consortium/lakectl-python:latest"


@task
def submit_and_wait(
    run_id: str,
    model_image: str,
    model_tag: str,
    qid: str,
    namespace: str = "default",
    input_data_files: list[list[str]] | None = None,
    data_transformation_sql: list[str] | None = None,
):
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

    pull_spec = []
    for i, (src_uri, filename) in enumerate(input_data_files or []):
        sql = (data_transformation_sql[i] if data_transformation_sql and i < len(data_transformation_sql) else "") or ""
        pull_spec.append([src_uri, filename, sql] if sql else [src_uri, filename])

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
        client.V1EnvVar(name="PULL_SPEC", value=json.dumps(pull_spec)),
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
                            image=LAKECTL_PYTHON_IMAGE,
                            command=["/bin/sh", "-c"],
                            args=[
                                f"mkdir -p /work/input /work/output && "
                                f"lakectl fs download {lakefs_run_path}/components/input/config.json /work/input/config.json && "
                                f"python3 /usr/local/bin/pull.py"
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

    failed = False
    while True:
        status = batch_v1.read_namespaced_job(name=run_id, namespace=namespace).status
        if status.succeeded:
            logger.info(f"Job {run_id} completed successfully")
            break
        if status.failed:
            failed = True
            break
        _check_for_stuck_pods(core_v1, run_id, namespace)
        time.sleep(5)

    pod_logs = _collect_pod_logs(core_v1, run_id, namespace)

    log_path = f"{shard_qid(qid)}/components/output/run.log"
    try:
        lc = lakefs_client()
        lakefs.repository(LAKEFS_RUN_REPO, client=lc) \
            .branch(LAKEFS_BRANCH) \
            .object(log_path) \
            .upload(data=pod_logs.encode(), content_type="text/plain")
        logger.info(f"Pod logs saved to lakefs://{LAKEFS_RUN_REPO}/{LAKEFS_BRANCH}/{log_path}")
    except Exception as e:
        logger.warning(f"Could not save pod logs to LakeFS: {e}")

    if failed:
        logger.error("Pod logs:\n%s", pod_logs)
        model_error = next(
            (line for line in pod_logs.splitlines() if line.startswith("ERROR:")),
            None,
        )
        detail = f": {model_error}" if model_error else ""
        raise RuntimeError(f"Job {run_id} failed{detail}")
