"""Envelope 序列化契约测试。"""

import json

import pytest

from superlanggraph.core.envelope import ENVELOPE_VERSION, Envelope
from superlanggraph.core.errors import SerializationError


def _sample() -> Envelope:
    return Envelope(
        type="task",
        run_id="run-1",
        payload={"input": {"messages": ["你好"]}},
        task_id="task-1",
        ns=("graph", "node"),
    )


def test_roundtrip_preserves_all_fields() -> None:
    envelope = _sample()
    assert Envelope.from_bytes(envelope.to_bytes()) == envelope


def test_namespace_survives_json_list_conversion() -> None:
    restored = Envelope.from_bytes(_sample().to_bytes())
    assert restored.ns == ("graph", "node")


def test_version_defaults_to_current() -> None:
    assert _sample().version == ENVELOPE_VERSION


def test_rejects_non_json_bytes() -> None:
    with pytest.raises(SerializationError):
        Envelope.from_bytes(b"\xff\xfe not json")


def test_rejects_non_object_json() -> None:
    with pytest.raises(SerializationError):
        Envelope.from_bytes(b'["task"]')


def test_rejects_missing_required_fields() -> None:
    with pytest.raises(SerializationError):
        Envelope.from_bytes(json.dumps({"type": "task"}).encode("utf-8"))


def test_rejects_unknown_message_type() -> None:
    raw = json.dumps({"type": "nope", "run_id": "r", "payload": {}}).encode("utf-8")
    with pytest.raises(SerializationError):
        Envelope.from_bytes(raw)


def test_rejects_non_object_payload() -> None:
    raw = json.dumps({"type": "task", "run_id": "r", "payload": [1]}).encode("utf-8")
    with pytest.raises(SerializationError):
        Envelope.from_bytes(raw)
