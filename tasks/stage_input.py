import io

import duckdb
import lakefs
import pandas as pd
from prefect import task
from prefect.logging import get_run_logger

from tools.lakefs_helpers import LAKEFS_RUN_REPO, LAKEFS_BRANCH, lakefs_client
from tools.sharding import shard_qid


def _apply_sql(data_bytes: bytes, sql: str) -> bytes:
    df = pd.read_parquet(io.BytesIO(data_bytes))
    conn = duckdb.connect()
    conn.register("df", df)
    result = conn.execute(sql).df()
    buf = io.BytesIO()
    result.to_parquet(buf, index=False)
    return buf.getvalue()


@task
def stage_input(
    input_data_files: list[list[str]],
    config_json: str,
    qid: str,
    data_transformation_sql: list[str] | None = None,
):
    """
    Copy input files from data-processed and write config.json into the run path.

    input_data_files: list of [lakefs_uri, target_filename] pairs
    data_transformation_sql: optional per-file SQL filter applied before staging
    Writes to:
      lakefs://model-runs/main/<sharded-qid>/components/input/<filename>
      lakefs://model-runs/main/<sharded-qid>/components/input/config.json
    """
    logger = get_run_logger()
    lc = lakefs_client()
    dst_prefix = f"{shard_qid(qid)}/components/input"
    dst_branch_handle = lakefs.repository(LAKEFS_RUN_REPO, client=lc).branch(LAKEFS_BRANCH)
    sql_list = data_transformation_sql or []

    for i, (src_uri, filename) in enumerate(input_data_files):
        src_repo, src_branch, path = src_uri.replace("lakefs://", "").split("/", 2)
        sql = sql_list[i] if i < len(sql_list) else ""

        logger.info(f"Downloading input: {src_uri}")
        try:
            data = (
                lakefs.repository(src_repo, client=lc)
                .branch(src_branch)
                .object(path)
                .reader()
                .read()
            )
            logger.info(f"Downloaded {len(data)} bytes from {src_uri}")
        except Exception as e:
            raise RuntimeError(
                f"Failed to read input file from LakeFS: {src_uri} — "
                f"make sure the file exists and credentials are correct."
            ) from e

        if sql:
            logger.info(f"Applying SQL transformation to {filename}")
            try:
                data = _apply_sql(data, sql)
            except Exception as e:
                raise RuntimeError(
                    f"Failed to apply SQL transformation to {filename}: {sql!r}"
                ) from e

        dst_path = f"{dst_prefix}/{filename}"
        logger.info(f"Staging {filename} -> lakefs://{LAKEFS_RUN_REPO}/{LAKEFS_BRANCH}/{dst_path}")
        try:
            dst_branch_handle.object(dst_path).upload(
                data=data, content_type="application/octet-stream"
            )
            logger.info(f"{filename} staged successfully")
        except Exception as e:
            raise RuntimeError(f"Failed to stage {filename} to {dst_path}") from e

    dst_config = f"{dst_prefix}/config.json"
    logger.info(f"Staging config.json -> lakefs://{LAKEFS_RUN_REPO}/{LAKEFS_BRANCH}/{dst_config}")
    try:
        dst_branch_handle.object(dst_config).upload(
            data=config_json.encode(), content_type="application/json"
        )
        logger.info("config.json staged successfully")
    except Exception as e:
        raise RuntimeError(f"Failed to stage config.json to {dst_config}") from e
