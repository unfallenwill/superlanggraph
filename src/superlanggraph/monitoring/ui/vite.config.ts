/**
 * 监控 UI 前端工程配置。
 * - 构建产物输出到后端 static/ 目录（FastAPI StaticFiles 挂载 /monitor/ui）；
 * - base 用相对路径，适配子路径挂载；
 * - dev proxy 仅本地开发用：把 /monitor 转发到本机后端。
 */
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  base: "./",
  build: {
    outDir: "../static",
    emptyOutDir: true,
  },
  server: {
    proxy: {
      "/monitor": "http://localhost:8000",
    },
  },
});
