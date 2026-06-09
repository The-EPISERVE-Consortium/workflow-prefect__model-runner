import hashlib
import json
import mimetypes
import os
import re
from datetime import datetime, timedelta, timezone

import lakefs
from prefect import task

from tools.lakefs_helpers import LAKEFS_RUN_REPO, LAKEFS_BRANCH, lakefs_client
from tools.sharding import shard_qid

DOIP_BASE_URL = os.getenv("DOIP_BASE_URL", "https://doip.episerve.zib.de")


def _lakefs_uri_to_doip_url(src_uri: str, commit_id: str | None = None) -> str:
    """Convert a lakefs:// URI to a DOIP retrieve URL, appending ?version= when a commit ID is known."""
    path = src_uri[len("lakefs://"):]
    parts = path.split("/")
    qid = None
    component_parts = []
    after_components = False
    for part in parts:
        if re.match(r"^Q\d+$", part, re.IGNORECASE):
            qid = part.upper()
        elif qid and part == "components":
            after_components = True
        elif after_components:
            component_parts.append(part)
    if not qid or not component_parts:
        return src_uri
    component = "/".join(component_parts)
    url = f"{DOIP_BASE_URL}/doip/retrieve/{qid}/{component}"
    if commit_id:
        url += f"?version={commit_id}"
    return url


def mint_model_qid(docker_image: str) -> str:
    """Return a stable QID derived from the docker image URI (tag excluded)."""
    digest = hashlib.sha256(docker_image.encode()).hexdigest()
    return f"Q{int(digest, 16) % 10**13:013d}"


def _build_fdo(
    qid: str,
    model_image: str,
    model_tag: str,
    run_start: datetime,
    end_time: datetime,
    file_entities: list,
    input_data_files: list[list[str]] | None = None,
    data_transformation_sql: list[str] | None = None,
    input_commit_ids: list[str | None] | None = None,
) -> bytes:
    model_name = model_image.split("/")[-1]
    components = []
    for entity in file_entities:
        component = {
            "@id": entity["@id"],
            "componentId": entity["name"],
        }
        if "encodingFormat" in entity:
            component["mediaType"] = entity["encodingFormat"]
        components.append(component)

    sql_list = data_transformation_sql or []
    commit_ids = input_commit_ids or []
    prov_used = []
    for i, (src_uri, _) in enumerate(input_data_files or []):
        commit_id = commit_ids[i] if i < len(commit_ids) else None
        if src_uri.startswith("lakefs://"):
            entry_id = _lakefs_uri_to_doip_url(src_uri, commit_id)
        elif "/doip/retrieve/" in src_uri and commit_id:
            entry_id = f"{src_uri}?version={commit_id}"
        else:
            entry_id = src_uri
        entry = {"@id": entry_id, "@type": "prov:Entity"}
        if i < len(sql_list) and sql_list[i]:
            entry["schema:query"] = sql_list[i]
        prov_used.append(entry)

    return json.dumps({
        "@context": [
            "https://w3id.org/fdo/context/v1",
            {
                "schema": "https://schema.org/",
                "prov": "http://www.w3.org/ns/prov#",
                "fdo": "https://w3id.org/fdo/vocabulary/",
            },
        ],
        "@id": qid,
        "@type": "DigitalObject",
        "prov:wasGeneratedBy": {"@id": "#run"},
        "kernel": {
            "@id": qid,
            "digitalObjectType": "https://schema.org/Dataset",
            "primaryIdentifier": qid,
            "kernelVersion": "v1",
            "immutable": False,
            "modified": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "fdo:hasComponent": components,
        },
        "profile": {
            "@context": "https://schema.org/",
            "@type": "Dataset",
            "@id": qid,
            "name": model_name,
            "description": f"Model run of {model_name} (tag: {model_tag})",
            "url": model_image,
        },
        "provenance": {
            "@id": "#run",
            "@type": "prov:Activity",
            "prov:startedAtTime": run_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "prov:endedAtTime": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "prov:wasAssociatedWith": {
                "@id": f"{model_image}:{model_tag}",
                "@type": "prov:SoftwareAgent",
                "schema:name": model_name,
                "schema:softwareVersion": model_tag,
                "schema:url": model_image,
            },
            "prov:used": prov_used,
        },
    }, indent=2).encode()


@task
def write_metadata(
    qid: str,
    model_image: str,
    model_tag: str,
    run_start: datetime,
    status: str,
    input_data_files: list[list[str]],
    data_transformation_sql: list[str] | None = None,
    input_commit_ids: list[str | None] | None = None,
):
    computation_time = int((datetime.now(timezone.utc) - run_start).total_seconds())
    end_time = run_start + timedelta(seconds=computation_time)
    model_name  = model_image.split('/')[-1]
    model_qid   = mint_model_qid(model_image)

    lc = lakefs_client()
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
                "instrument":   {"@id": model_qid},
                "object":       input_refs,
                "result":       output_refs,
                "startTime":    run_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "endTime":      end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "actionStatus": {"@id": action_status},
            },
            {
                "@id": model_qid,
                "@type": "SoftwareApplication",
                "identifier":      model_qid,
                "name":            model_name,
                "softwareVersion": model_tag,
                "url":             model_image,
            },
            *file_entities,
        ],
    }, indent=2).encode()

    branch_handle \
        .object(f"{sharded}/components/ro-crate-metadata.json") \
        .upload(data=metadata, content_type="application/json")

    file_entities.append({
        "@id": "components/ro-crate-metadata.json",
        "@type": "File",
        "name": "ro-crate-metadata.json",
        "encodingFormat": "application/json",
    })

    fdo = _build_fdo(
        qid=qid,
        model_image=model_image,
        model_tag=model_tag,
        run_start=run_start,
        end_time=end_time,
        file_entities=file_entities,
        input_data_files=input_data_files,
        data_transformation_sql=data_transformation_sql,
        input_commit_ids=input_commit_ids,
    )
    branch_handle \
        .object(f"{sharded}/{qid}.fdo.json") \
        .upload(data=fdo, content_type="application/json")

    branch_handle.commit(message=f"add ro-crate metadata for {qid}")
