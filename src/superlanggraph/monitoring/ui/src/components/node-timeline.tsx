/**
 * 节点耗时横向条形图：recharts 垂直布局 BarChart，avg/max 双条（已按 total_ms 降序，
 * 截前 12 个节点）；有错误的节点在 avg 条末端以红色 ✕n 标注。antd Card 外框。
 */
import { Card } from "antd";
import {
  Bar,
  BarChart,
  CartesianGrid,
  LabelList,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { NodeStat } from "../types";

const NAME_ZH: Record<string, string> = { avg_ms: "平均", max_ms: "最大" };

type Row = NodeStat & { label: string };

function fmtMs(ms: number): string {
  return ms >= 1000 ? `${(ms / 1000).toFixed(2)}s` : `${Math.round(ms)}ms`;
}

/** avg 条末端标注耗时值；payload.errors > 0 时追加红色 ✕n（props 形状由 recharts 注入） */
function avgLabel(props: any) {
  const { x = 0, y = 0, width = 0, height = 0, value = 0, payload } = props ?? {};
  const row = payload as Row | undefined;
  const hasErr = !!row && row.errors > 0;
  return (
    <text
      x={Number(x) + Number(width) + 6}
      y={Number(y) + Number(height) / 2}
      fill={hasErr ? "#f5484d" : "#c9d1d9"}
      fontSize={11}
      dominantBaseline="middle"
    >
      {fmtMs(Number(value))}
      {hasErr ? ` ✕${row!.errors}` : ""}
    </text>
  );
}

export default function NodeTimeline({ stats }: { stats: NodeStat[] }) {
  const rows: Row[] = stats
    .slice(0, 12)
    .map((s) => ({ ...s, label: `${s.graph_id}.${s.node}` }));
  return (
    <Card title="节点耗时（avg / max，红 = 错误）" size="small">
      {rows.length === 0 ? (
        <div style={{ height: 260, display: "grid", placeItems: "center", color: "#8a919c" }}>
          窗口内无节点执行记录
        </div>
      ) : (
        <div style={{ width: "100%", height: Math.max(220, rows.length * 44 + 40) }}>
          <ResponsiveContainer>
            <BarChart
              layout="vertical"
              data={rows}
              margin={{ top: 8, right: 90, bottom: 0, left: 8 }}
            >
              <CartesianGrid stroke="#30363d" strokeDasharray="3 3" horizontal={false} />
              <XAxis
                type="number"
                stroke="#8a919c"
                tick={{ fontSize: 11 }}
                tickFormatter={(v) => fmtMs(Number(v))}
              />
              <YAxis
                type="category"
                dataKey="label"
                width={170}
                stroke="#8a919c"
                tick={{ fontSize: 11 }}
                tickFormatter={(v) =>
                  String(v).length > 24 ? `${String(v).slice(0, 23)}…` : String(v)
                }
              />
              <Tooltip
                formatter={(value, name) => [
                  fmtMs(Number(value)),
                  NAME_ZH[String(name)] ?? String(name),
                ]}
                labelFormatter={(label, payload) => {
                  const row = payload?.[0]?.payload as Row | undefined;
                  const err = row && row.errors > 0 ? `，✕${row.errors} 次错误` : "";
                  return `${String(label)}（${row ? row.count : "-"} 次${err}）`;
                }}
                contentStyle={{ background: "#161b22", border: "1px solid #30363d", fontSize: 12 }}
              />
              <Legend formatter={(v) => NAME_ZH[String(v)] ?? String(v)} />
              <Bar dataKey="avg_ms" name="avg_ms" fill="#4c8dff" barSize={7} isAnimationActive={false}>
                <LabelList content={avgLabel} />
              </Bar>
              <Bar
                dataKey="max_ms"
                name="max_ms"
                fill="#4c8dff55"
                barSize={7}
                isAnimationActive={false}
              />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}
    </Card>
  );
}
