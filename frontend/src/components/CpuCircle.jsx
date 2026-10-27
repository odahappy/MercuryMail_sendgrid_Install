import { Progress } from 'antd';

export default function CpuCircle({ percent = 0, size = 120 }) {
  const value = Math.max(0, Math.min(100, Number(percent || 0)));
  const strokeColor = value >= 90 ? '#ef4444' : value >= 70 ? '#f59e0b' : '#10b981';
  return (
    <Progress
      type="circle"
      percent={value}
      size={size}
      strokeColor={{
        '0%': strokeColor,
        '100%': value >= 90 ? '#fca5a5' : value >= 70 ? '#fcd34d' : '#6ee7b7',
      }}
      trailColor="rgba(15, 23, 42, 0.06)"
      strokeWidth={8}
      format={(p) => `${Number(p || 0).toFixed(1)}%`}
    />
  );
}
