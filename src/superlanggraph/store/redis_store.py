"""Redis 版 BaseStore：跨线程 KV 存储（KV + 命名空间 + 过滤搜索）。

key 布局：每个 item 一个 Hash ``{prefix}:store:{ns}\\x00{key}``，payload 字段
存 JSON（value + created_at/updated_at）。搜索按 namespace 前缀 SCAN + 内存
过滤（与官方 MemoryStore 语义一致）；语义检索（query）在无向量索引的部署上
显式报错。组合根注入 redis 客户端。
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from typing import Any, Literal, cast

from langgraph.store.base import (
    NOT_PROVIDED,
    BaseStore,
    GetOp,
    Item,
    ListNamespacesOp,
    MatchCondition,
    NotProvided,
    PutOp,
    SearchItem,
    SearchOp,
)
from redis.asyncio import Redis

NamespacePath = tuple[str, ...]

_SEP = "\x00"


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _decode(value: Any) -> Any:
    return value.decode() if isinstance(value, bytes) else value


class RedisStore(BaseStore):
    """实现 store 的 KV / 搜索 / 命名空间能力；Redis 客户端由组合根注入。"""

    def __init__(self, client: Redis, prefix: str = "slg") -> None:
        self._client = client
        self._prefix = prefix

    # ---- 同步接口：异步运行时显式拒绝 ----

    def batch(self, ops: Iterable[Any]) -> list[Any]:
        raise NotImplementedError("RedisStore 仅支持异步接口（abatch 路径）")

    # ---- 异步接口 ----

    async def abatch(self, ops: Iterable[Any]) -> list[Any]:
        results: list[Any] = []
        for op in ops:
            if isinstance(op, GetOp):
                results.extend(await self._do_get([op]))
            elif isinstance(op, SearchOp):
                results.append(await self._do_search(op))
            elif isinstance(op, PutOp):
                await self._do_put(op)
                results.append(None)
            else:
                results.append(None)
        return results

    async def aget(
        self, namespace: tuple[str, ...], key: str, *, refresh_ttl: bool | None = None
    ) -> Item | None:
        found = await self._do_get([GetOp(namespace=namespace, key=key)])
        return found[0] if found else None

    async def asearch(
        self,
        namespace_prefix: tuple[str, ...],
        /,
        *,
        query: str | None = None,
        filter: dict[str, Any] | None = None,
        limit: int = 10,
        offset: int = 0,
        refresh_ttl: bool | None = None,
    ) -> list[SearchItem]:
        return await self._do_search(
            SearchOp(
                namespace_prefix=namespace_prefix,
                filter=filter,
                limit=limit,
                offset=offset,
                query=query,
            )
        )

    async def aput(
        self,
        namespace: tuple[str, ...],
        key: str,
        value: dict[str, Any],
        index: Literal[False] | list[str] | None = None,
        *,
        ttl: float | NotProvided | None = NOT_PROVIDED,
    ) -> None:
        await self._do_put(PutOp(namespace=namespace, key=key, value=value, index=index))

    async def adelete(self, namespace: tuple[str, ...], key: str) -> None:
        await self._client.delete(self._item_key(namespace, key))

    async def alist_namespaces(
        self,
        *,
        prefix: NamespacePath | None = None,
        suffix: NamespacePath | None = None,
        max_depth: int | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[tuple[str, ...]]:
        conditions: list[MatchCondition] = []
        if prefix:
            conditions.append(MatchCondition(match_type="prefix", path=tuple(prefix)))
        if suffix:
            conditions.append(MatchCondition(match_type="suffix", path=tuple(suffix)))
        op = ListNamespacesOp(
            match_conditions=tuple(conditions) if conditions else None,
            max_depth=max_depth,
            limit=limit,
            offset=offset,
        )
        return await self._do_list(op)

    # ---- 内部实现 ----

    async def _do_get(self, ops: list[GetOp]) -> list[Item | None]:
        found: list[Item | None] = []
        for op in ops:
            raw = await self._client.hgetall(self._item_key(op.namespace, op.key))
            found.append(_to_item(op.namespace, op.key, raw))
        return found

    async def _do_put(self, op: PutOp) -> None:
        if op.value is None:
            await self._client.delete(self._item_key(op.namespace, op.key))
            return
        existing = await self._client.hget(self._item_key(op.namespace, op.key), "payload")
        now = _now()
        created = _extract_created(existing) or now
        payload = json.dumps({"value": op.value, "created_at": created, "updated_at": now})
        await self._client.hset(
            self._item_key(op.namespace, op.key),
            mapping={"payload": payload},  # type: ignore[arg-type]
        )

    async def _do_search(self, op: SearchOp) -> list[SearchItem]:
        if op.query:
            raise NotImplementedError("语义检索需要 RediSearch 向量索引，当前部署未启用")
        pattern = f"{self._prefix}:store:*"
        matched: list[SearchItem] = []
        async for raw_key in self._client.scan_iter(match=pattern):  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            key_text = _decode(raw_key)
            body = key_text.rsplit(":", 1)[-1]
            ns_text, _, item_key = body.rpartition(_SEP)
            namespace = tuple(ns_text.split(_SEP)) if ns_text else ()
            if not _prefix_matches(namespace, op.namespace_prefix):
                continue
            raw = await self._client.hgetall(key_text)
            item = _to_search_item(namespace, item_key, raw)
            if item is None:
                continue
            if op.filter and not _matches_filter(item.value, op.filter):
                continue
            matched.append(item)
        matched.sort(key=lambda item: item.updated_at)
        return matched[op.offset : op.offset + op.limit]

    async def _do_list(self, op: ListNamespacesOp) -> list[tuple[str, ...]]:
        pattern = f"{self._prefix}:store:*"
        namespaces: set[tuple[str, ...]] = set()
        async for raw_key in self._client.scan_iter(match=pattern):  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            key_text = _decode(raw_key)
            body = key_text.rsplit(":", 1)[-1]
            ns_text = body.rpartition(_SEP)[0]
            namespace = tuple(ns_text.split(_SEP)) if ns_text else ()
            if not _conditions_match(namespace, op.match_conditions):
                continue
            if op.max_depth is not None:
                namespace = namespace[: op.max_depth]
            namespaces.add(namespace)
        ordered = sorted(namespaces)
        return ordered[op.offset : op.offset + op.limit]

    def _item_key(self, namespace: Sequence[str], key: str) -> str:
        ns_text = _SEP.join(namespace)
        return f"{self._prefix}:store:{ns_text}{_SEP}{key}"


def _extract_created(raw: str | bytes | None) -> str | None:
    if raw is None:
        return None
    text = _decode(raw)
    return json.loads(text).get("created_at")


def _to_item(namespace: tuple[str, ...], key: str, raw: dict[Any, Any]) -> Item | None:
    if not raw:
        return None
    payload_raw = raw.get("payload") or raw.get(b"payload")
    if payload_raw is None:
        return None
    payload = json.loads(_decode(payload_raw))
    return Item(
        value=payload["value"],
        key=key,
        namespace=namespace,
        created_at=payload["created_at"],
        updated_at=payload["updated_at"],
    )


def _to_search_item(namespace: tuple[str, ...], key: str, raw: dict[Any, Any]) -> SearchItem | None:
    item = _to_item(namespace, key, raw)
    if item is None:
        return None
    return SearchItem(
        namespace,
        key,
        item.value,
        item.created_at,
        item.updated_at,
    )


def _prefix_matches(namespace: tuple[str, ...], prefix: Sequence[str]) -> bool:
    return tuple(namespace[: len(prefix)]) == tuple(prefix)


def _conditions_match(
    namespace: tuple[str, ...], conditions: tuple[MatchCondition, ...] | None
) -> bool:
    if not conditions:
        return True
    for condition in conditions:
        path = tuple(condition.path)
        if condition.match_type == "prefix" and not _prefix_matches(namespace, path):
            return False
        if condition.match_type == "suffix" and namespace[-len(path) :] != path:
            return False
    return True


def _matches_filter(value: dict[str, Any], filter: dict[str, Any]) -> bool:
    return all(_nested_get(value, key.split(".")) == expected for key, expected in filter.items())


def _nested_get(value: dict[str, Any], path: list[str]) -> Any:
    current: Any = value
    for part in path:
        if not isinstance(current, dict):
            return None
        current = cast("dict[str, Any]", current)[part]
    return current
