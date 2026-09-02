"""``json_body`` 对空 body 与坏 JSON 的容错语义（对齐官方 API）。"""

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from superlanggraph.server.body import json_body

app = FastAPI()


@app.post("/echo")
async def echo(request: Request) -> dict[str, object]:
    return {"body": await json_body(request)}


client = TestClient(app)


def test_empty_body_becomes_empty_dict():
    """无 body 的 POST 视为 {}，官方前端探活就是这么发的。"""
    response = client.post("/echo")
    assert response.status_code == 200
    assert response.json() == {"body": {}}


def test_malformed_json_returns_400():
    """坏 JSON 返回 400，而不是把 500 冒到客户端。"""
    response = client.post(
        "/echo", content="not-json", headers={"content-type": "application/json"}
    )
    assert response.status_code == 400


def test_valid_json_passthrough():
    response = client.post("/echo", json={"a": 1, "b": [2, 3]})
    assert response.status_code == 200
    assert response.json() == {"body": {"a": 1, "b": [2, 3]}}
