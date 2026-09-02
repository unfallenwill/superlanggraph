"""``json_body`` 对空 body 与坏 JSON 的容错语义（对齐官方 API）。

用 httpx 的 ASGITransport 直连应用而非 TestClient：后者在未安装 httpx2 的
环境下类型不完整，strict 检查无法通过。
"""

from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
from fastapi import FastAPI, Request

from superlanggraph.server.body import json_body

app = FastAPI()


@app.post("/echo")
async def echo(request: Request) -> dict[str, Any]:
    return {"body": await json_body(request)}


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as c:
        yield c


async def test_empty_body_becomes_empty_dict(client: httpx.AsyncClient) -> None:
    """无 body 的 POST 视为 {}，官方前端探活就是这么发的。"""
    response = await client.post("/echo")
    assert response.status_code == 200
    assert response.json() == {"body": {}}


async def test_malformed_json_returns_400(client: httpx.AsyncClient) -> None:
    """坏 JSON 返回 400，而不是把 500 冒到客户端。"""
    response = await client.post(
        "/echo", content=b"not-json", headers={"content-type": "application/json"}
    )
    assert response.status_code == 400


async def test_valid_json_passthrough(client: httpx.AsyncClient) -> None:
    response = await client.post("/echo", json={"a": 1, "b": [2, 3]})
    assert response.status_code == 200
    assert response.json() == {"body": {"a": 1, "b": [2, 3]}}
