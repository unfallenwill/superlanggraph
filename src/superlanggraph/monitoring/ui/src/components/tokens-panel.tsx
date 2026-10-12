/**
 * 令牌面板（对应设计文档 §8.1 #tokens-panel）
 * 数据源：MonitorSnapshot.token_totals（TokenTotals，窗口合计）+ SSE token_usage 帧累加（由上层维护后传入）
 * 纯展示组件：数据一律来自 props，不发请求；布局响应式由组装层负责。
 */
import { Flex, Statistic } from "antd";
import type { TokenTotals } from "../types";

export interface TokensPanelProps {
  /** 窗口令牌合计；未传时以全零渲染（自测用） */
  totals?: TokenTotals;
}

export function TokensPanel({ totals }: TokensPanelProps) {
  const input = totals?.input_tokens ?? 0;
  const output = totals?.output_tokens ?? 0;
  const total = totals?.total_tokens ?? 0;

  const cards = [
    { key: "input", title: "输入", value: input },
    { key: "output", title: "输出", value: output },
    { key: "total", title: "合计", value: total },
  ];

  return (
    <section id="tokens-panel" aria-label="令牌用量">
      <Flex gap="large" wrap="wrap">
        {cards.map(({ key, title, value }) => (
          <Statistic
            key={key}
            title={title}
            value={value}
            // 千分位分组，便于读数
            formatter={(value) => Number(value).toLocaleString("zh-CN")}
          />
        ))}
      </Flex>
    </section>
  );
}
