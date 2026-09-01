# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownParameterType=false, reportUnknownArgumentType=false, reportMissingTypeArgument=false, reportCallIssue=false, reportArgumentType=false
# redis-py 存根类型不完整 + IndexConfig 泛型；集成测试为类型边界，src/ 不受影响。

"""语义检索集成测试：确定性嵌入下验证余弦排序与 score。"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator

import pytest
from redis.asyncio import Redis

from superlanggraph.core.settings import BrokerSettings
from superlanggraph.store.redis_store import RedisStore


class FakeEmbeddings:
    """词表式确定性嵌入：每个新词分配独立维度，同词共向——语义可分且无碰撞。"""

    def __init__(self) -> None:
        self._vocab: dict[str, int] = {}

    def _vec(self, text: str) -> list[float]:
        vec = [0.0] * 64
        for word in text.lower().split():
            idx = self._vocab.setdefault(word, len(self._vocab))
            if idx < 64:
                vec[idx] += 1.0
        norm = sum(x * x for x in vec) ** 0.5
        return [x / norm for x in vec] if norm else vec

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return [self._vec(text) for text in texts]

    def embed_query(self, text: str) -> list[float]:
        return self._vec(text)


@pytest.fixture
async def store() -> AsyncIterator[RedisStore]:
    settings = BrokerSettings()
    client = Redis.from_url(settings.redis_url)
    try:
        await client.ping()
    except Exception:
        pytest.skip("需要本地 Redis：docker compose up -d")
    yield RedisStore(
        client,
        prefix=f"slg-sem-{uuid.uuid4().hex[:8]}",
        index={"dims": 64, "embed": FakeEmbeddings(), "fields": ["text"]},
    )
    await client.aclose()


async def test_semantic_search_ranks_by_similarity(store: RedisStore) -> None:
    ns = ("semantic", uuid.uuid4().hex)
    await store.aput(ns, "cats", {"text": "cats are cute pets"})
    await store.aput(ns, "dogs", {"text": "dogs are loyal pets"})
    await store.aput(ns, "redis", {"text": "redis is a database"})

    results = await store.asearch(ns, query="cute pets", limit=3)
    assert results, "应返回结果"
    assert results[0].key in ("cats", "dogs"), "宠物文档应排最前"
    assert results[-1].key == "redis", "数据库文档应排最后"
    assert results[0].score is not None and results[0].score > 0


async def test_search_without_query_returns_all_filtered(store: RedisStore) -> None:
    ns = ("semantic", uuid.uuid4().hex)
    await store.aput(ns, "a", {"text": "hello world", "kind": "x"})
    await store.aput(ns, "b", {"text": "hello there", "kind": "y"})

    results = await store.asearch(ns)
    assert {item.key for item in results} == {"a", "b"}
    filtered = await store.asearch(ns, filter={"kind": "x"})
    assert [item.key for item in filtered] == ["a"]


async def test_put_with_explicit_index_false_skips_embedding(store: RedisStore) -> None:
    ns = ("semantic", uuid.uuid4().hex)
    await store.aput(ns, "no-embed", {"text": "plain"}, index=False)
    results = await store.asearch(ns, query="plain")
    assert results == [], "显式禁用索引的条目不应被语义搜索命中"
