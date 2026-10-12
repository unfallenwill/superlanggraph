"""监控事件模型测试：JSON 往返、判别联合分发与非法输入拒绝（§10）。"""

import json
from typing import Any, get_args

import pytest
from pydantic import ValidationError

from superlanggraph.broker.runs import RUN_STATUSES
from superlanggraph.monitoring.events import (
    OUTCOME,
    RUN_STATUS,
    NodeSpanEvent,
    RunLifecycleEvent,
    TokenUsageEvent,
    WorkerHeartbeatEvent,
    parse_monitor_event,
)


def _base(source: str = "driver") -> dict[str, Any]:
    return {"event_id": "ab" * 16, "ts": 1781136000.5, "source": source}


def _lifecycle_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        **_base(),
        "type": "run_lifecycle",
        "run_id": "run-1",
        "thread_id": "th-1",
        "graph_id": "main",
        "status": "running",
    }
    payload.update(overrides)
    return payload


def test_run_lifecycle_json_roundtrip() -> None:
    """model_dump_json 后经 parse_monitor_event 还原，逐字段相等。"""
    event = RunLifecycleEvent(
        **_lifecycle_payload(status="error", error="boom", duration_ms=123.5)
    )
    restored = parse_monitor_event(event.model_dump_json())
    assert restored == event
    assert isinstance(restored, RunLifecycleEvent)
    assert restored.error == "boom"
    assert restored.duration_ms == 123.5


def test_node_span_json_roundtrip() -> None:
    payload: dict[str, Any] = {
        **_base(source="worker"),
        "type": "node_span",
        "run_id": "run-1",
        "thread_id": "th-1",
        "graph_id": "main",
        "task_id": "t-1",
        "node": "chat",
        "queue_ms": 12.5,
        "duration_ms": 340.0,
        "outcome": "error",
        "error": "RuntimeError(x)",
    }
    event = NodeSpanEvent(**payload)
    assert parse_monitor_event(event.model_dump_json()) == event


def test_token_usage_json_roundtrip() -> None:
    payload: dict[str, Any] = {
        **_base(source="worker"),
        "type": "token_usage",
        "run_id": "run-1",
        "thread_id": "th-1",
        "graph_id": "main",
        "node": "chat",
        "input_tokens": 10,
        "output_tokens": 4,
        "total_tokens": 14,
    }
    event = TokenUsageEvent(**payload)
    assert parse_monitor_event(event.model_dump_json()) == event


def test_worker_heartbeat_json_roundtrip() -> None:
    payload: dict[str, Any] = {
        **_base(source="worker"),
        "type": "worker_heartbeat",
        "worker_id": "w-1",
        "inflight": 3,
        "concurrency": 32,
    }
    event = WorkerHeartbeatEvent(**payload)
    assert parse_monitor_event(event.model_dump_json()) == event


def test_roundtrip_accepts_bytes() -> None:
    event = RunLifecycleEvent(**_lifecycle_payload())
    assert parse_monitor_event(event.model_dump_json().encode()) == event


def test_discriminator_dispatches_by_type() -> None:
    """同一 TypeAdapter 按 type 字段分发到具体子类。"""
    payloads = [
        _lifecycle_payload(),
        {
            **_base(),
            "type": "node_span",
            "run_id": "r",
            "thread_id": "t",
            "graph_id": "g",
            "task_id": "k",
            "node": "n",
            "queue_ms": 0.0,
            "duration_ms": 1.0,
            "outcome": "return",
        },
        {
            **_base(),
            "type": "token_usage",
            "run_id": "r",
            "thread_id": "t",
            "graph_id": "g",
            "input_tokens": 1,
            "output_tokens": 2,
            "total_tokens": 3,
        },
        {
            **_base(),
            "type": "worker_heartbeat",
            "worker_id": "w",
            "inflight": 0,
            "concurrency": 0,
        },
    ]
    parsed = [parse_monitor_event(json.dumps(p)) for p in payloads]
    assert [type(event) for event in parsed] == [
        RunLifecycleEvent,
        NodeSpanEvent,
        TokenUsageEvent,
        WorkerHeartbeatEvent,
    ]
    assert [event.type for event in parsed] == [
        "run_lifecycle",
        "node_span",
        "token_usage",
        "worker_heartbeat",
    ]


def test_unknown_type_rejected() -> None:
    with pytest.raises(ValidationError):
        parse_monitor_event(json.dumps({**_lifecycle_payload(), "type": "mystery"}))


def test_illegal_status_rejected() -> None:
    with pytest.raises(ValidationError):
        RunLifecycleEvent(**_lifecycle_payload(status="done"))


def test_illegal_source_rejected() -> None:
    with pytest.raises(ValidationError):
        RunLifecycleEvent(**_lifecycle_payload(source="scheduler"))


def test_illegal_outcome_rejected() -> None:
    payload: dict[str, Any] = {
        **_base(),
        "type": "node_span",
        "run_id": "r",
        "thread_id": "t",
        "graph_id": "g",
        "task_id": "k",
        "node": "n",
        "queue_ms": 0.0,
        "duration_ms": 1.0,
        "outcome": "crash",
    }
    with pytest.raises(ValidationError):
        NodeSpanEvent(**payload)


def test_negative_tokens_rejected() -> None:
    payload: dict[str, Any] = {
        **_base(),
        "type": "token_usage",
        "run_id": "r",
        "thread_id": "t",
        "graph_id": "g",
        "input_tokens": -1,
        "output_tokens": 0,
        "total_tokens": 0,
    }
    with pytest.raises(ValidationError):
        TokenUsageEvent(**payload)


def test_negative_heartbeat_fields_rejected() -> None:
    payload: dict[str, Any] = {
        **_base(),
        "type": "worker_heartbeat",
        "worker_id": "w",
        "inflight": -1,
        "concurrency": 0,
    }
    with pytest.raises(ValidationError):
        WorkerHeartbeatEvent(**payload)


def test_run_status_matches_broker_run_statuses() -> None:
    """分层禁互依：字面量与 broker.runs.RUN_STATUSES 逐项一致（设计 §4 约定）。

    只比集合不比顺序：Literal 与 tuple 的书写顺序本就无语义约束。
    """
    assert set(get_args(RUN_STATUS)) == set(RUN_STATUSES)
    assert len(get_args(RUN_STATUS)) == len(RUN_STATUSES)


def test_outcome_literal_values() -> None:
    assert get_args(OUTCOME) == ("return", "error", "interrupt")
