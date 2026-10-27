import { Typography } from 'antd';
import { formatRate, isTaskRunning } from '../utils/taskDetailFormat.js';

const { Text } = Typography;

export default function TaskSmartAdviceBar({
  taskStatus,
  perf,
  showLowRateWarning,
  performanceAlert,
  suggestedAction,
}) {
  const running = isTaskRunning(taskStatus) && perf?.running !== false;
  const actionText = String(suggestedAction || perf?.suggested_action || '').trim();

  if (performanceAlert?.reason) {
    return (
      <div className="task-cockpit__advice task-cockpit__advice--warn">
        <Text>
          性能告警：{performanceAlert.reason}
          {performanceAlert.actual_rate != null ? `（实际处理 ${formatRate(performanceAlert.actual_rate)} 封/秒）` : ''}
        </Text>
      </div>
    );
  }

  if (running && showLowRateWarning) {
    return (
      <div className="task-cockpit__advice task-cockpit__advice--warn">
        <Text>当前速率低于预期，建议检查代理延迟或提高并发</Text>
      </div>
    );
  }

  if (running && actionText) {
    return (
      <div className="task-cockpit__advice task-cockpit__advice--info">
        <Text>{actionText}</Text>
      </div>
    );
  }

  if (running) {
    return (
      <div className="task-cockpit__advice task-cockpit__advice--ok">
        <Text>✅ 当前速率健康</Text>
      </div>
    );
  }

  return (
    <div className="task-cockpit__advice task-cockpit__advice--idle">
      <Text type="secondary">任务未运行，启动后将显示速率健康状态</Text>
    </div>
  );
}
