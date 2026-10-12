/**
 * 分钟序列图：recharts ComposedChart（runs 面积 + errors/tokens 折线），
 * runs/errors 走左轴，tokens 走右轴；antd Card 外框。
 */
import { Card } from "antd";
import {
  Area,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { MinutePoint } from "../types";

const NAME_ZH: Record<string, string> = {
  runs_started: "启动 runs",
  errors: "错误",
  total_tokens: "tokens",
};

function fmtClock(ts: number): string {
  const d = new Date(ts * 1000);
  const p = (x: number) => String(x).padStart(2, "0");
  return `${p(d.getHours())}:${p(d.getMinutes())}`;
}

export default function SeriesChart({ points }: { points: MinutePoint[] }) {
  return (
    <Card title="分钟序列（runs / errors / tokens）" size="small">
      {points.length === 0 ? (
        <div style={{ height: 260, display: "grid", placeItems: "center", color: "#8a919c" }}>
          暂无序列数据
        </div>
      ) : (
        <div style={{ width: "100%", height: 280 }}>
          <ResponsiveContainer>
            <ComposedChart data={points} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
              <CartesianGrid stroke="#30363d" strokeDasharray="3 3" />
              <XAxis
                dataKey="bucket_ts"
                tickFormatter={(v) => fmtClock(Number(v))}
                stroke="#8a919c"
                tick={{ fontSize: 11 }}
                minTickGap={32}
              />
              <YAxis
                yAxisId="count"
                stroke="#8a919c"
                tick={{ fontSize: 11 }}
                width={44}
                allowDecimals={false}
              />
              <YAxis
                yAxisId="tokens"
                orientation="right"
                stroke="#3fb970"
                tick={{ fontSize: 11 }}
                width={56}
              />
              <Tooltip
                labelFormatter={(v) => fmtClock(Number(v))}
                formatter={(value, name) => [
                  Number(value).toLocaleString("zh-CN"),
                  NAME_ZH[String(name)] ?? String(name),
                ]}
                contentStyle={{ background: "#161b22", border: "1px solid #30363d", fontSize: 12 }}
              />
              <Legend formatter={(v) => NAME_ZH[String(v)] ?? String(v)} />
              <Area
                yAxisId="count"
                type="monotone"
                dataKey="runs_started"
                stroke="#4c8dff"
                fill="#4c8dff33"
                strokeWidth={1.5}
                isAnimationActive={false}
              />
              <Line
                yAxisId="count"
                type="monotone"
                dataKey="errors"
                stroke="#f5484d"
                strokeWidth={1.5}
                dot={false}
                isAnimationActive={false}
              />
              <Line
                yAxisId="tokens"
                type="monotone"
                dataKey="total_tokens"
                stroke="#3fb970"
                strokeWidth={1.5}
                dot={false}
                isAnimationActive={false}
              />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      )}
    </Card>
  );
}
