import importlib.util
import os
import pytest

_pull_path = os.path.join(os.path.dirname(__file__), "..", "lakectl-python", "pull.py")
_spec = importlib.util.spec_from_file_location("pull", _pull_path)
pull = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(pull)

_shard_qid = pull._shard_qid
_lakefs_versioned_to_ref = pull._lakefs_versioned_to_ref
_doip_to_lakefs_uri = pull._doip_to_lakefs_uri


def test_shard_qid():
    assert _shard_qid("Q1780951744442") == "17/80/95/Q1780951744442"


def test_lakefs_versioned_to_ref_with_version():
    uri = "lakefs://data-processed/main/aa/bb/cc/Q123/components/input.parquet?version=abc123"
    result = _lakefs_versioned_to_ref(uri)
    assert result == "lakefs://data-processed/abc123/aa/bb/cc/Q123/components/input.parquet"


def test_lakefs_versioned_to_ref_without_version():
    uri = "lakefs://data-processed/main/aa/bb/cc/Q123/components/input.parquet"
    assert _lakefs_versioned_to_ref(uri) == uri


def test_doip_to_lakefs_uri(monkeypatch):
    monkeypatch.setenv("DOIP_LAKEFS_REPO", "data-processed")
    uri = "https://doip.episerve.zib.de/doip/retrieve/Q1780951744442/input.parquet?version=abc123"
    result = _doip_to_lakefs_uri(uri)
    assert result == "lakefs://data-processed/abc123/17/80/95/Q1780951744442/components/input.parquet"


def test_doip_to_lakefs_uri_missing_version():
    uri = "https://doip.episerve.zib.de/doip/retrieve/Q1780951744442/input.parquet"
    with pytest.raises(ValueError, match="no \\?version="):
        _doip_to_lakefs_uri(uri)


def test_doip_urls_are_tried_in_every_repository_that_can_hold_objects(monkeypatch):
    monkeypatch.delenv("DOIP_LAKEFS_REPOS", raising=False)
    monkeypatch.setenv("DOIP_LAKEFS_REPO", "data-processed")
    uris = pull._doip_to_lakefs_uris("https://doip.example/doip/retrieve/Q1792269547104/input.parquet?version=" + "e" * 40)
    assert uris == [f"lakefs://data-processed/{'e' * 40}/17/92/26/Q1792269547104/components/input.parquet",
                    f"lakefs://benchmarks/{'e' * 40}/17/92/26/Q1792269547104/components/input.parquet"]


def test_download_falls_back_to_the_next_repository(monkeypatch, tmp_path):
    import subprocess
    monkeypatch.setattr(pull, "WORK_INPUT", str(tmp_path))
    monkeypatch.delenv("DOIP_LAKEFS_REPOS", raising=False)
    calls = []

    def fake_run(cmd, check):
        calls.append(cmd[3])
        if "data-processed" in cmd[3]:
            raise subprocess.CalledProcessError(1, cmd)
        open(cmd[4], "wb").write(b"x")

    monkeypatch.setattr(pull.subprocess, "run", fake_run)
    pull.download("https://doip.example/doip/retrieve/Q1792269547104/input.parquet?version=" + "e" * 40, "input.parquet")
    assert [c.split("/")[2] for c in calls] == ["data-processed", "benchmarks"]
    monkeypatch.setattr(pull.subprocess, "run", lambda cmd, check: (_ for _ in ()).throw(subprocess.CalledProcessError(1, cmd)))
    import pytest
    with pytest.raises(subprocess.CalledProcessError):
        pull.download("https://doip.example/doip/retrieve/Q1792269547104/input.parquet?version=" + "e" * 40, "input.parquet")
