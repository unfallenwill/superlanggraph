# superlanggraph

基于 LangGraph 的 Agent 项目，Python 3.12 + uv 管理。

# 工作流程

- 任务开始前，先用 firecrawl 或 context7 获取所涉及库 / 框架 / API 的最新文档，再动手写代码，不要依赖可能过时的训练数据知识。
  - 查询库 / 框架 / SDK 文档：优先用 context7（先 `resolve-library-id`，再 `query-docs`）。
  - 抓取网页文档、搜索最新资料：用 firecrawl。
- 已安装的框架可以直接阅读其源码或 API（如 `uv run python -c "import pkg; print(pkg.__file__)"` 定位后阅读），结合在线文档确认实际行为。

# 环境与执行

- 安装依赖一律使用 `uv add <package>`，不要直接用 pip 或手动改 pyproject.toml。
- 所有 Python / 脚本 / 测试命令都通过 `uv run <cmd>` 执行，不用全局 python，也不手动激活虚拟环境。
- MySQL / Redis 等外部服务用 docker compose 启停（`docker compose up -d` / `docker compose down`），应用本身在本地用 uv 跑，不把应用代码打进镜像。
- API key 等敏感信息只放 `.env`，不硬编码进代码，`.env` 不提交到 git。

# 交流约定

- 对话、代码注释、commit message 统一用中文。
