import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App";

// React 入口引导：挂载 index.html 中的 #root
createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
