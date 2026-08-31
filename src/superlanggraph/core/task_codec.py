"""节点任务与执行结果的编解码：driver 与 worker 的共享契约。

任务 payload 是分布式执行的核心数据结构，形状在这里显式定义、两端共用。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, cast

from superlanggraph.core.serde import TypedValue, dumps, loads

OutcomeKind = Literal["return", "error", "interrupt"]


@dataclass(frozen=True, slots=True)
class NodeTask:
    """派发给 worker 的一个节点任务。"""

    task_id: str
    graph_id: str
    node: str
    run_id: str
    thread_id: str
    checkpoint_ns: str
    input: Any
    resume: list[Any] = field(default_factory=list[Any])
    null_resume: Any = None
    resume_map: dict[str, Any] = field(default_factory=dict[str, Any])
    metadata: dict[str, Any] = field(default_factory=dict[str, Any])
    tags: list[str] = field(default_factory=list[str])


@dataclass(frozen=True, slots=True)
class NodeOutcome:
    """worker 对一个节点任务的执行结果。"""

    task_id: str
    kind: OutcomeKind
    value: Any = None
    error_text: str = ""
    resumes: list[Any] = field(default_factory=list[Any])


def encode_task(task: NodeTask) -> dict[str, Any]:
    """任务 → Redis 载荷。"""
    return {
        "task_id": task.task_id,
        "graph_id": task.graph_id,
        "node": task.node,
        "run_id": task.run_id,
        "thread_id": task.thread_id,
        "checkpoint_ns": task.checkpoint_ns,
        "input": dumps(task.input),
        "resume": [dumps(item) for item in task.resume],
        "null_resume": dumps(task.null_resume) if task.null_resume is not None else None,
        "resume_map": {ns_hash: dumps(value) for ns_hash, value in task.resume_map.items()},
        "metadata": task.metadata,
        "tags": task.tags,
    }


def decode_task(payload: dict[str, Any]) -> NodeTask:
    """Redis 载荷 → 任务。形状不符显式报错。"""
    required = (
        "task_id",
        "graph_id",
        "node",
        "run_id",
        "thread_id",
        "checkpoint_ns",
        "input",
    )
    missing = [key for key in required if key not in payload]
    if missing:
        raise ValueError(f"任务载荷缺少字段: {missing}")
    return NodeTask(
        task_id=str(payload["task_id"]),
        graph_id=str(payload["graph_id"]),
        node=str(payload["node"]),
        run_id=str(payload["run_id"]),
        thread_id=str(payload["thread_id"]),
        checkpoint_ns=str(payload["checkpoint_ns"]),
        input=loads(cast_typed(payload["input"])),
        resume=[loads(item) for item in payload.get("resume", [])],
        null_resume=(
            loads(cast_typed(payload["null_resume"]))
            if payload.get("null_resume") is not None
            else None
        ),
        resume_map=_decode_resume_map(payload.get("resume_map")),
        metadata=payload.get("metadata") or {},
        tags=payload.get("tags") or [],
    )


def encode_outcome(outcome: NodeOutcome) -> dict[str, Any]:
    """结果 → Redis 载荷。"""
    return {
        "task_id": outcome.task_id,
        "kind": outcome.kind,
        "value": dumps(outcome.value),
        "error_text": outcome.error_text,
        "resumes": [dumps(item) for item in outcome.resumes],
    }


def decode_outcome(payload: dict[str, Any]) -> NodeOutcome:
    """Redis 载荷 → 结果。"""
    return NodeOutcome(
        task_id=str(payload["task_id"]),
        kind=payload["kind"],
        value=loads(cast_typed(payload["value"])),
        error_text=str(payload.get("error_text", "")),
        resumes=[loads(item) for item in payload.get("resumes", [])],
    )


def _decode_resume_map(raw: Any) -> dict[str, Any]:
    entries = cast("dict[str, list[str]]", raw) if isinstance(raw, dict) else {}
    decoded: dict[str, Any] = {}
    for ns_hash, value in entries.items():
        decoded[str(ns_hash)] = loads(cast_typed(value))
    return decoded


def cast_typed(value: Any) -> TypedValue:
    """断言载荷中的 serde 形状。"""
    if not isinstance(value, list):
        raise ValueError(f"非法的 typed 值: {value!r}")
    typed = cast("list[Any]", value)
    if len(typed) != 2:
        raise ValueError(f"非法的 typed 值: {value!r}")
    return [str(typed[0]), str(typed[1])]
