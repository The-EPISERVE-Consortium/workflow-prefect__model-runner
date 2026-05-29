import pytest
from tools.sharding import shard_qid, get_component_path


def test_shard_qid_basic():
    assert shard_qid("Q1748526042817") == "17/48/52/Q1748526042817"


def test_shard_qid_short_digits_padded():
    # digits < 6 should be zero-padded
    assert shard_qid("Q42") == "00/00/42/Q42"


def test_shard_qid_exactly_6_digits():
    assert shard_qid("Q123456") == "12/34/56/Q123456"


def test_shard_qid_lowercase_normalised():
    assert shard_qid("q1748526042817") == "17/48/52/Q1748526042817"


def test_shard_qid_invalid_no_q():
    with pytest.raises(ValueError, match="must start with 'Q'"):
        shard_qid("1748526042817")


def test_shard_qid_invalid_non_digits():
    with pytest.raises(ValueError, match="must contain digits"):
        shard_qid("Qabc")


def test_get_component_path_with_extension():
    result = get_component_path("Q1748526042817", "primary", "json")
    assert result == "17/48/52/Q1748526042817/components/primary.json"


def test_get_component_path_with_dot_extension():
    result = get_component_path("Q1748526042817", "primary", ".json")
    assert result == "17/48/52/Q1748526042817/components/primary.json"


def test_get_component_path_no_extension():
    result = get_component_path("Q1748526042817", "primary", "")
    assert result == "17/48/52/Q1748526042817/components/primary"
