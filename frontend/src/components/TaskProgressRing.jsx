import { formatCount } from '../utils/taskDetailFormat.js';

export default function TaskProgressRing({
  percent = 0,
  statusText = '进行中',
  success = 0,
  failed = 0,
  remaining = 0,
  size = 168,
  showRingStats = true,
}) {
  const normalized = Math.max(0, Math.min(100, Number(percent) || 0));
  const stroke = 12;
  const radius = (size - stroke) / 2;
  const circumference = 2 * Math.PI * radius;
  const offset = circumference - (normalized / 100) * circumference;
  const arcColor = normalized >= 100 ? '#10b981' : normalized >= 60 ? '#0c8ee6' : '#06b6d4';

  return (
    <div style={{ textAlign: 'center' }}>
      <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`} role="img" aria-label={`发送进度 ${normalized}%`}>
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          fill="none"
          stroke="#e8eef5"
          strokeWidth={stroke}
        />
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          fill="none"
          stroke={arcColor}
          strokeWidth={stroke}
          strokeLinecap="round"
          strokeDasharray={circumference}
          strokeDashoffset={offset}
          transform={`rotate(-90 ${size / 2} ${size / 2})`}
          style={{ transition: 'stroke-dashoffset 0.4s ease' }}
        />
        <text x="50%" y="46%" textAnchor="middle" dominantBaseline="middle" fontSize="26" fontWeight="700" fill="#111827">
          {normalized}%
        </text>
        <text x="50%" y="62%" textAnchor="middle" dominantBaseline="middle" fontSize="12" fill="#6b7280">
          {statusText}
        </text>
      </svg>
      {showRingStats ? (
        <div className="task-cockpit__ring-stats">
          <div className="task-cockpit__ring-stat">
            <strong style={{ color: '#22c55e' }}>{formatCount(success)}</strong>
            成功
          </div>
          <div className="task-cockpit__ring-stat">
            <strong style={{ color: '#ef4444' }}>{formatCount(failed)}</strong>
            失败
          </div>
          <div className="task-cockpit__ring-stat">
            <strong style={{ color: '#3b82f6' }}>{formatCount(remaining)}</strong>
            剩余
          </div>
        </div>
      ) : null}
    </div>
  );
}
