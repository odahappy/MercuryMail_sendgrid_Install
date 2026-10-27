import { Tooltip, Typography } from 'antd';
import { formatRate } from '../utils/taskDetailFormat.js';

const { Text } = Typography;

const PHASE_SEGMENTS = [
  { key: 'wait_token_ms', label: '令牌', color: '#9ca3af' },
  { key: 'http_request_ms', label: 'HTTP', color: '#3b82f6' },
  { key: 'db_write_ms', label: '写DB', color: '#f97316' },
  { key: 'preprocess_ms', label: '预处理', color: '#22c55e' },
];

function percent(value, total) {
  if (!total) return '0.0';
  return ((value / total) * 100).toFixed(1);
}

export default function TaskPhaseTimingBar({ perf, compact = false }) {
  const values = PHASE_SEGMENTS.map((segment) => ({
    ...segment,
    ms: Number(perf?.[segment.key] || 0),
  }));
  const total = values.reduce((sum, item) => sum + item.ms, 0);

  if (total <= 0) {
    return <Text type="secondary" style={{ fontSize: 11 }}>暂无耗时样本</Text>;
  }

  return (
    <div>
      <div
        style={{
          display: 'flex',
          width: '100%',
          height: compact ? 14 : 24,
          borderRadius: 4,
          overflow: 'hidden',
          background: '#f1f5f9',
        }}
      >
        {values.filter((item) => item.ms > 0).map((item) => (
          <Tooltip
            key={item.key}
            title={`${item.label}：${formatRate(item.ms)} ms（${percent(item.ms, total)}%）`}
          >
            <div
              style={{
                width: `${(item.ms / total) * 100}%`,
                minWidth: item.ms > 0 ? 3 : 0,
                backgroundColor: item.color,
              }}
            />
          </Tooltip>
        ))}
      </div>
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: compact ? 6 : 12, marginTop: compact ? 4 : 8 }}>
        {values.map((item) => (
          <Text key={item.key} style={{ fontSize: compact ? 10 : 12 }}>
            <span
              style={{
                display: 'inline-block',
                width: 6,
                height: 6,
                borderRadius: 2,
                background: item.color,
                marginRight: 3,
              }}
            />
            {item.label} {formatRate(item.ms)}ms
          </Text>
        ))}
      </div>
    </div>
  );
}
