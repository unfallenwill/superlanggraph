# superlanggraph 运行时镜像
# 用户代码（langgraph.json + 图源文件）通过 volume 挂载进容器，
# 与官方 langgraph dev 的挂载模式一致；生产环境可在此基础上 bake 镜像。

FROM python:3.12-slim AS base

WORKDIR /app

# 系统依赖（最小化）
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl && rm -rf /var/lib/apt/lists/*

# 安装 uv + 项目依赖（利用缓存层）
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# 安装项目自身
COPY src/ src/
RUN uv sync --frozen --no-dev

# 用户代码挂载点（langgraph.json + 图源文件）
RUN mkdir -p /app/user
WORKDIR /app/user

# 健康检查
HEALTHCHECK --interval=5s --timeout=3s --retries=10 \
    CMD curl -sf http://127.0.0.1:8000/health || exit 1

# 默认启动单机全角色模式
ENTRYPOINT ["uv", "run", "superlanggraph"]
CMD ["server", "--host", "0.0.0.0", "--config", "/app/user/langgraph.json"]
