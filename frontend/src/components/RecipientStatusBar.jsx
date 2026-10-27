import { Tooltip, Typography } from 'antd';
import { formatCount } from '../utils/taskDetailFormat.js';

const { Text } = Typography;

const SEGMENTS = [
  { key: 'delivered_final', fields: ['delivered_final', 'delivered'], label: '已送达', color: '#10b981' },
  { key: 'deferred_final', fields: ['deferred_final', 'soft_bounced_or_retrying'], label: '软弹/延迟', color: '#f59e0b' },
  { key: 'hard_bounce_final', fields: ['hard_bounce_final', 'hard_bounced'], label: '硬弹', color: '#ef4444' },
  { key: 'spam_reported_final', fields: ['spam_reported_final'], label: '垃圾投诉', color: '#a855f7' },
  { key: 'waiting_final', fields: ['waiting_final'], label: '等待事件', color: '#64748b' },
  { key: 'unsubscribed', fields: ['unsubscribed'], label: '退订', color: '#94a3b8' },
  { key: 'pending', fields: ['pending'], label: '未处理', color: '#0c8ee6' },
];

function resolveSegmentCount(stats, segment) {
  for (const field of segment.fields) {
    if (stats?.[field] != null) return Number(stats[field] || 0);
  }
  return Number(stats?.[segment.key] || 0);
}

function percent(value, total) {
  if (!total) return '0.0';
  return ((value / total) * 100).toFixed(1);
}

export default function RecipientStatusBar({ stats, compact = false, inline = false }) {
  const total = Number(stats?.total_recipients || 0);
  const values = SEGMENTS.map((segment) => ({
    ...segment,
    count: resolveSegmentCount(stats, segment),
  }));
  const visibleSegments = values.filter((item) => item.count > 0);

  const bar = (
    <div style={{ display: 'flex', width: '100%', height: inline ? 18 : (compact ? 18 : 26), borderRadius: 8, overflow: 'hidden', background: total > 0 ? 'transparent' : '#e8eef5', boxShadow: 'inset 0 1px 2px rgba(15,23,42,0.06)' }}>
      {total > 0 && visibleSegments.length > 0 ? visibleSegments.map((segment) => {
        const segmentPercent = percent(segment.count, total);
        return (
          <Tooltip
            key={segment.key}
            title={`${segment.label}: ${formatCount(segment.count)} (${segmentPercent}%)`}
          >
            <div
              style={{
                width: `${(segment.count / total) * 100}%`,
                minWidth: segment.count > 0 ? 4 : 0,
                backgroundColor: segment.color,
                transition: 'width 0.3s ease',
              }}
            />
          </Tooltip>
        );
      }) : (
        <div style={{ width: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          <Text type="secondary" style={{ fontSize: compact ? 11 : 13 }}>
            {total > 0 ? '暂无分类数据' : '暂无处理数据'}
          </Text>
        </div>
      )}
    </div>
  );

  if (inline) {
    return bar;
  }

  if (compact) {
    return (
      <div className="task-cockpit__stack">
        <Text type="secondary" style={{ fontSize: 11, marginBottom: 4 }}>收件人状态分布</Text>
        {bar}
      </div>
    );
  }

  return (
    <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: 16 }}>
      <Text strong style={{ display: 'block', marginBottom: 8 }}>收件人处理状态分布</Text>
      {bar}
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 16, marginTop: 12 }}>
        {values.map((segment) => (
          <Text key={segment.key}>
            <span style={{ display: 'inline-block', width: 10, height: 10, borderRadius: 2, background: segment.color, marginRight: 6 }} />
            {segment.label} {formatCount(segment.count)}
          </Text>
        ))}
      </div>
    </div>
  );
}
