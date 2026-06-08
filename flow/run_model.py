import json
import random
import time
from datetime import datetime, timezone

from prefect import flow
from prefect.runtime import flow_run

from tasks.stage_input import stage_input
from tasks.submit_and_wait import submit_and_wait
from tasks.write_metadata import write_metadata
from tools.lakefs_helpers import LAKEFS_RUN_REPO, LAKEFS_BRANCH
from tools.sharding import shard_qid


def mint_qid() -> str:
    return f"Q{int(time.time())}{random.randint(0, 999):03d}"


@flow
def model_pipeline(
    input_data_files: list[list[str]],
    model_image: str,
    config_json: str,
    model_tag: str = "latest",
    namespace: str = "default",
    data_transformation_sql: list[str] | None = None,
):
    """
    Run a model container on Kubernetes, reading input from LakeFS and
    writing output back to LakeFS.

    Args:
        input_data_files: List of [lakefs_uri, target_filename] pairs,
                          e.g. [["lakefs://data-processed/main/.../file.parquet", "file.parquet"]]
        model_image:  GHCR image name,
                      e.g. ghcr.io/the-episerve-consortium/model__prediction__grippeweb__baseline-nullmodel
        config_json:  JSON string written verbatim as config.json in the input directory,
                      e.g. {"horizon_weeks": 4, "n_reference_weeks": 4}
        model_tag:    Image tag
        namespace:    Kubernetes namespace
        data_transformation_sql: Optional per-file SQL filter applied before staging,
                                 parallel list to input_data_files
    """
    model_image = model_image.strip()
    model_tag = model_tag.strip() or "latest"
    run_start = datetime.now(timezone.utc)
    qid = mint_qid()
    run_id = f"model-runner-{qid.lower()}"

    prefect_payload_json = json.dumps(flow_run.parameters)

    stage_input(
        input_data_files=input_data_files,
        config_json=config_json,
        prefect_payload_json=prefect_payload_json,
        qid=qid,
        data_transformation_sql=data_transformation_sql,
    )
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
            input_data_files=input_data_files,
            data_transformation_sql=data_transformation_sql,
        )

    return f"lakefs://{LAKEFS_RUN_REPO}/{LAKEFS_BRANCH}/{shard_qid(qid)}/components/output/"
