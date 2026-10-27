import TaskProgressRing from './TaskProgressRing.jsx';
import { formatCount, formatRate } from '../utils/taskDetailFormat.js';

function StatBlock({ label, value, color }) {
  return (
    <div className="task-cockpit__result-stat">
      <strong style={{ color }}>{formatCount(value)}</strong>
      <span>{label}</span>
    </div>
  );
}

export default function TaskOverviewLeft({
  ringPercent,
  statusText,
  stats,
  liveStatus,
  successCount = 0,
  failedCount = 0,
  remainingCount = 0,
  configuredRateLimit,
}) {
  const successSdk = Number(liveStatus?.live_success_count ?? liveStatus?.success_count ?? stats?.success_sdk ?? successCount ?? 0);
  const failedSdk = Number(liveStatus?.live_fail_count ?? liveStatus?.fail_count ?? stats?.failed_sdk ?? failedCount ?? 0);
  const unsentCount = Number(liveStatus ? remainingCount : stats?.unsent_count || 0);
  return (
    <div className="task-cockpit__overview-left">
      <TaskProgressRing
        percent={ringPercent}
        statusText={statusText}
        success={successSdk}
        failed={failedSdk}
        remaining={unsentCount}
        showRingStats
        size={156}
      />

      <div className="task-cockpit__result-panel">
        <TextSectionTitle title="SDK 累计发送结果（每秒刷新）" />
        <div className="task-cockpit__result-stats">
          <StatBlock label="发送成功" value={successSdk} color="#22c55e" />
          <StatBlock label="发送失败" value={failedSdk} color="#ef4444" />
          <StatBlock label="未发送" value={unsentCount} color="#3b82f6" />
        </div>
      </div>
    </div>
  );
}

function TextSectionTitle({ title }) {
  return (
    <div className="task-cockpit__result-panel-title">{title}</div>
  );
}
