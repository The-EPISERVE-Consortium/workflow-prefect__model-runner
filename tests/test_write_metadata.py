"""Unit tests for tasks/write_metadata.py."""

import pytest

from tasks.write_metadata import media_type


@pytest.mark.parametrize(
    "path, expected",
    [
        ("components/input/config.json", "application/json"),
        ("components/output/predictions.tsv", "text/tab-separated-values"),
        ("components/output/run.log", "text/plain"),
        ("components/output/policy.conf", "text/plain"),
        ("components/output/scenario.yaml", "application/yaml"),
        ("components/output/notes.md", "text/markdown"),
        ("components/output/simulation_weekly.parquet", "application/vnd.apache.parquet"),
        ("components/output/snapshot.zip", "application/zip"),
        ("components/output/UPPER.TSV", "text/tab-separated-values"),
    ],
)
def test_media_type_of_known_files(path, expected):
    assert media_type(path) == expected


@pytest.mark.parametrize("path", ["components/output/model.bin", "components/output/noextension", "components/out.d/file"])
def test_every_file_gets_a_media_type(path):
    assert media_type(path) == "application/octet-stream"
