"""图注册表：解析 langgraph.json，加载图，产出两种编译形态。

- 本地形态（worker / server 状态接口使用）：节点函数原样保留；
- 分布式形态（driver 使用）：节点 bound 替换为 RemoteNodeProxy，写入路径仍在本地。

langgraph 的编译图是重泛型类型且对未参数化 StateGraph 推断不完整，
注册表边界统一以 Any 承载（显式 Any 优于传染全仓的 Unknown）。
"""

from __future__ import annotations

import importlib
import importlib.util
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from langgraph.pregel._read import DEFAULT_BOUND, PregelNode

from superlanggraph.core.errors import SlgError
from superlanggraph.core.remote import RemoteNodeProxy


@dataclass(frozen=True, slots=True)
class GraphEntry:
    """langgraph.json 中一条图声明。"""

    graph_id: str
    source: str


class Registry:
    """按 langgraph.json 加载并缓存图。"""

    def __init__(self, config_path: Path) -> None:
        self._config_path = config_path
        self._entries: dict[str, GraphEntry] = {}
        self._load_config()

    def _load_config(self) -> None:
        if not self._config_path.exists():
            raise SlgError(f"找不到图配置: {self._config_path}")
        config = json.loads(self._config_path.read_text(encoding="utf-8"))
        graphs = config.get("graphs", {})
        if not graphs:
            raise SlgError(f"图配置中没有 graphs 声明: {self._config_path}")
        for graph_id, source in graphs.items():
            self._entries[graph_id] = GraphEntry(graph_id=graph_id, source=source)

    @staticmethod
    def _load_object(base: Path, graph_id: str, source: str) -> Any:
        """加载 ``./file.py:attr`` 或 ``module:attr`` 声明的图对象。"""
        target = source.split(":", 1)
        if len(target) != 2:
            raise SlgError(f"图 {graph_id} 的声明缺少 ':属性名' 部分: {source}")
        location, attr = target
        if location.endswith(".py"):
            module = _load_module_from_file(base / location, graph_id)
        else:
            module = importlib.import_module(location)
        obj = getattr(module, attr, None)
        if obj is None:
            raise SlgError(f"图 {graph_id} 的属性不存在: {source}")
        return obj

    def graph_ids(self) -> list[str]:
        return sorted(self._entries)

    def has(self, graph_id: str) -> bool:
        return graph_id in self._entries

    def default_graph_id(self) -> str:
        return self.graph_ids()[0]

    def compile_local(self, graph_id: str, checkpointer: Any = None) -> Any:
        """本地形态：每次独立加载并编译，绝不跨角色共享图对象。

        langgraph 的编译图与 builder 共享底层节点结构，driver 的就地
        proxy 化会污染任何共享引用——隔离加载是正确性的前提。
        """
        entry = self._entries[graph_id]
        obj = self._load_object(self._config_path.parent, graph_id, entry.source)
        return _compile(obj, checkpointer)

    def compile_distributed(self, graph_id: str, executor: Any, checkpointer: Any = None) -> Any:
        """分布式形态：每个真实节点的 bound 替换为 RemoteNodeProxy。"""
        graph = self.compile_local(graph_id, checkpointer)
        for name, node in list(graph.nodes.items()):
            if name.startswith("__") or not isinstance(node, PregelNode):
                continue
            if node.bound is DEFAULT_BOUND:
                continue
            proxy = RemoteNodeProxy(graph_id, name, node.bound, executor)
            graph.nodes[name] = node.copy({"bound": proxy})
        return graph


def _compile(obj: Any, checkpointer: Any) -> Any:
    """未编译图则编译；已编译图直接绑定运行时 checkpointer（Pregel 普通属性）。"""
    if hasattr(obj, "nodes") and hasattr(obj, "astream"):
        if checkpointer is not None:
            obj.checkpointer = checkpointer
        return obj
    return obj.compile(checkpointer=checkpointer)


def _load_module_from_file(path: Path, graph_id: str) -> Any:
    if not path.exists():
        raise SlgError(f"图 {graph_id} 的源文件不存在: {path}")
    module_name = f"slg_graphs_{graph_id}_{abs(hash(str(path)))}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise SlgError(f"无法加载模块: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module
