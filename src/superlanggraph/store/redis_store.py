"""Redis 版 BaseStore：跨线程 KV 存储（KV + 命名空间 + 过滤搜索 + 语义检索）。

key 布局：每个 item 一个 Hash ``{prefix}:store:{ns}\\x00{key}``，payload 字段
存 JSON（value + created_at/updated_at），配置语义索引时附加 ``vectors`` 字段
（JSON ``list[list[float]]``，每字段一个向量）。

语义检索：按官方 IndexConfig（dims/embed/fields）对 put 的字段提取文本嵌入；
search(query) 时 query 单向量与 item 多向量做余弦、max-pooling 排序——
与官方 MemoryStore 语义一致。纯 Python 余弦（无 numpy 依赖，P2 规模足够）。
"""

from __future__ import annotations

import inspect
import json
import math
import re
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from typing import Any, Literal, cast

from langgraph.store.base import (
    NOT_PROVIDED,
    BaseStore,
    GetOp,
    IndexConfig,
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
    """实现 store 的 KV / 搜索 / 命名空间 / 语义检索能力；Redis 客户端由组合根注入。"""

    def __init__(
        self,
        client: Redis,
        prefix: str = "slg",
        index: IndexConfig | None = None,
    ) -> None:
        self._client = client
        self._prefix = prefix
        self._index = index
        self._embed_fn = (index or {}).get("embed")

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
        payload: dict[str, Any] = {"value": op.value, "created_at": created, "updated_at": now}

        # 语义索引：store 配置了 index 且本次 put 未显式禁用（index is not False）
        if self._index is not None and op.index is not False:
            fields = (
                list(op.index)
                if isinstance(op.index, list) and op.index
                else list(self._index.get("fields") or ["$"])
            )
            texts = _extract_texts(op.value, tuple(fields))
            vectors = await self._embed_texts(texts)
            if vectors:
                payload["vectors"] = vectors

        await self._client.hset(
            self._item_key(op.namespace, op.key),
            mapping={"payload": json.dumps(payload, ensure_ascii=False)},  # type: ignore[arg-type]
        )

    async def _do_search(self, op: SearchOp) -> list[SearchItem]:
        pattern = f"{self._prefix}:store:*"
        matched: list[tuple[SearchItem, list[list[float]]]] = []
        scoreless: list[SearchItem] = []
        async for raw_key in self._client.scan_iter(match=pattern):  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            key_text = _decode(raw_key)
            body = key_text.rsplit(":", 1)[-1]
            ns_text, _, item_key = body.rpartition(_SEP)
            namespace = tuple(ns_text.split(_SEP)) if ns_text else ()
            if not _prefix_matches(namespace, op.namespace_prefix):
                continue
            raw = await self._client.hgetall(key_text)
            item = _to_item(namespace, item_key, raw)
            if item is None:
                continue
            if op.filter and not _matches_filter(item.value, op.filter):
                continue
            vectors = _extract_vectors(raw)
            search_item = _to_search_item(namespace, item_key, raw, None)
            if search_item is None:
                continue
            vectors = _extract_vectors(raw)
            if op.query and vectors:
                matched.append((search_item, vectors))
            else:
                scoreless.append(search_item)

        if op.query:
            return await self._rank_by_similarity(op, matched, scoreless)

        items = [item for item, _vectors in matched] + scoreless
        items_sorted = sorted(items, key=lambda i: i.updated_at)
        return items_sorted[op.offset : op.offset + op.limit]

    async def _rank_by_similarity(
        self,
        op: SearchOp,
        matched: list[tuple[SearchItem, list[list[float]]]],
        scoreless: list[SearchItem],
    ) -> list[SearchItem]:
        assert op.query is not None
        query_vector = await self._embed_query(op.query)
        if not query_vector:
            return scoreless[op.offset : op.offset + op.limit]
        scored: list[tuple[float, SearchItem]] = []
        for item, vectors in matched:
            best = max((_cosine(query_vector, v) for v in vectors), default=0.0)
            scored.append((best, item))
        scored.sort(key=lambda pair: pair[0], reverse=True)
        ranked = [
            SearchItem(
                item.namespace,
                item.key,
                item.value,
                item.created_at,
                item.updated_at,
                score,
            )
            for score, item in scored
        ]
        return ranked[op.offset : op.offset + op.limit]

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

    async def _embed_texts(self, texts: list[str]) -> list[list[float]]:
        """对字段文本列表生成嵌入；支持 Embeddings 实例、同步/异步函数。"""
        return await _embed_with(self._embed_fn, texts)

    async def _embed_query(self, query: str) -> list[float]:
        vectors = await _embed_with(self._embed_fn, [query])
        return vectors[0] if vectors else []


def _extract_created(raw: str | bytes | None) -> str | None:
    if raw is None:
        return None
    text = _decode(raw)
    return json.loads(text).get("created_at")


def _to_search_item(
    namespace: tuple[str, ...], key: str, raw: dict[Any, Any], score: float | None
) -> SearchItem | None:
    item = _to_item(namespace, key, raw)
    if item is None:
        return None
    return SearchItem(namespace, key, item.value, item.created_at, item.updated_at, score)


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


async def _embed_with(embed: Any, texts: list[str]) -> list[list[float]]:
    """按官方 IndexConfig 的三种形态调用嵌入来源。

    Embeddings 实例走 embed_documents/embed_query；同步/异步函数按
    EmbeddingsFunc/AEmbeddingsFunc 协议直接调用。此函数是嵌入来源
    动态分发的唯一收敛点。
    """
    if hasattr(embed, "embed_documents"):
        return list(cast("list[list[float]]", embed.embed_documents(texts)))
    if hasattr(embed, "embed_query"):
        return [list(cast("list[float]", embed.embed_query(text))) for text in texts]
    if inspect.iscoroutinefunction(embed):
        return list(await embed(texts))
    return list(embed(texts))


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
    current: dict[str, Any] = value
    for part in path:
        if part not in current:
            return None
        current = current[part]
    return current


def _extract_vectors(raw: dict[Any, Any]) -> list[list[float]]:
    """vectors 存在 payload JSON 内部：`{"vectors": [[...], ...]}`。"""
    payload_raw = raw.get("payload") or raw.get(b"payload")
    if payload_raw is None:
        return []
    payload = json.loads(_decode(payload_raw))
    raw_vectors = cast("list[list[float]] | None", payload.get("vectors"))
    if not raw_vectors:
        return []
    return [list(vector) for vector in raw_vectors]


def _extract_texts(value: dict[str, Any], fields: tuple[str, ...]) -> list[str]:
    """按 JSON path（$ / 字段 / 点号嵌套 / [*] 数组通配）提取文本。"""
    texts: list[str] = []
    for field in fields:
        if field == "$":
            texts.append(json.dumps(value, ensure_ascii=False))
            continue
        array_match = re.match(r"^([^[\]]+)\[\*\]\.(.+)$", field)
        if array_match:
            base_path, rest = array_match.group(1), array_match.group(2)
            elements = _nested_get(value, base_path.split("."))
            if isinstance(elements, list):
                typed_elements = cast("list[dict[str, Any]]", elements)
                for element in typed_elements:
                    texts.append(_json_text(_nested_get(element, rest.split("."))))
            continue
        texts.append(_json_text(_nested_get(value, field.split("."))))
    return [text for text in texts if text]


def _json_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def _cosine(a: list[float], b: list[float]) -> float:
    if not a or not b:
        return 0.0
    dot = sum(x * y for x, y in zip(a, b, strict=False))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)
