"""请求体解析：对官方 API 的空 body / 坏 JSON 容错语义。

浏览器前端与各类探活客户端常发无 body 的 POST；官方实现按默认值处理，
这里同样不能让 500 冒到客户端。
"""

from __future__ import annotations

import json
from typing import Any

from fastapi import HTTPException, Request


async def json_body(request: Request) -> Any:
    """解析 JSON 请求体：空 body 视为 ``{}``，坏 JSON 返回 400。"""
    raw = await request.body()
    if not raw.strip():
        return {}
    try:
        return json.loads(raw)
    except ValueError as exc:
        raise HTTPException(400, f"请求体不是合法 JSON: {exc}") from exc
