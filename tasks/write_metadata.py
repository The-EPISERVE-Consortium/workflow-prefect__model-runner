import json
import mimetypes
from datetime import datetime, timedelta, timezone

import lakefs
from prefect import task

from tools.lakefs_helpers import LAKEFS_RUN_REPO, LAKEFS_BRANCH, lakefs_client
from tools.sharding import shard_qid


def _build_fdo(qid: str, model_image: str, model_tag: str, end_time: datetime, file_entities: list) -> bytes:
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
            "prov:generatedAtTime": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "prov:wasAttributedTo": f"{model_image}:{model_tag}",
        },
    }, indent=2).encode()


@task
def write_metadata(qid: str, model_image: str, model_tag: str, run_start: datetime, status: str):
    computation_time = int((datetime.now(timezone.utc) - run_start).total_seconds())
    end_time = run_start + timedelta(seconds=computation_time)
    model_name = model_image.split('/')[-1]

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

    fdo = _build_fdo(
        qid=qid,
        model_image=model_image,
        model_tag=model_tag,
        end_time=end_time,
        file_entities=file_entities,
    )
    branch_handle \
        .object(f"{sharded}/{qid}.fdo.json") \
        .upload(data=fdo, content_type="application/json")

    branch_handle.commit(message=f"add ro-crate metadata for {qid}")
