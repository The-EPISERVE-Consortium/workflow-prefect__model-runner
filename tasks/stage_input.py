import lakefs
from prefect import task
from prefect.logging import get_run_logger

from tools.lakefs_helpers import LAKEFS_RUN_REPO, LAKEFS_BRANCH, lakefs_client
from tools.sharding import shard_qid


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

    lc = lakefs_client()

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
