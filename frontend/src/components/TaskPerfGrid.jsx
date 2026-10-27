import { Alert, Progress, Tag, Typography } from 'antd';
import TaskPhaseTimingBar from './TaskPhaseTimingBar.jsx';
import { formatCount, formatRate, isTaskRunning } from '../utils/taskDetailFormat.js';

const { Text } = Typography;

const PREFETCH_QUEUE_MAX = 50000;
const WRITE_QUEUE_MAX = 100000;

function latencyColor(ms) {
  const value = Number(ms || 0);
  if (value <= 0) return '#9ca3af';
  if (value < 200) return '#22c55e';
  if (value <= 500) return '#eab308';
  return '#ef4444';
}

function queuePercent(length, max) {
  const value = Number(length || 0);
  const cap = Math.max(1, Number(max || 1));
  return Math.min(100, Math.round((value / cap) * 100));
}

function finiteNumber(value, fallback = 0) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : fallback;
}

function rateText(value, running) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return running ? '计算中' : '—';
  return formatRate(numeric);
}

function MiniRateBars({ samples = [] }) {
  const values = samples.slice(-5);
  const max = Math.max(...values.map((item) => Number(item.rate || 0)), 0.1);
  if (values.length === 0) {
    return <Text type="secondary" style={{ fontSize: 11 }}>暂无趋势</Text>;
  }
  return (
    <div className="task-cockpit__mini-bars">
      {values.map((item, index) => {
        const rate = Number(item.rate || 0);
        const height = Math.max(4, Math.round((rate / max) * 36));
        return (
          <div
            key={`${item.t}-${index}`}
            className="task-cockpit__mini-bar"
            style={{ height }}
            title={`${formatRate(rate)} 封/秒`}
          />
        );
      })}
    </div>
  );
}

function HealthIndicator({ level = 'idle' }) {
  return <span className={`task-cockpit__indicator task-cockpit__indicator--${level}`} />;
}

function resolveProxyHealth(task, perf) {
  if (!task?.use_proxy) {
    return { level: 'ok', label: '直连', latency: '—' };
  }
  const latency = Number(perf?.avg_http_latency_ms || task?.proxy_latency_ms || 0);
  if (latency <= 0) {
    return { level: 'ok', label: '代理已启用', latency: task?.proxy_address || task?.proxy_name || '待发送测速' };
  }
  if (latency < 500) {
    return { level: 'ok', label: '代理正常', latency: `${formatRate(latency)} ms` };
  }
  return { level: 'bad', label: '代理偏慢', latency: `${formatRate(latency)} ms` };
}

function resolveChannelHealth(taskStatus) {
  const status = String(taskStatus || '');
  if (status === 'running') return { level: 'ok', label: '发送中' };
  if (status === 'paused' || status === 'scheduled' || status === 'pending') {
    return { level: 'warn', label: '待命' };
  }
  if (status === 'failed' || status === 'stopped') return { level: 'bad', label: '异常/停止' };
  if (status === 'finished') return { level: 'ok', label: '已完成' };
  return { level: 'idle', label: '草稿' };
}

export default function TaskPerfGrid({ task, taskStatus, perf, stats, rateSamples = [] }) {
  const running = isTaskRunning(taskStatus) && perf?.running !== false;
  const instantRate = finiteNumber(perf?.sdk_realtime_rate ?? perf?.sdk_success_rate_1s ?? perf?.realtime_rate, NaN);
  const sdkSuccessRate = finiteNumber(perf?.sdk_realtime_rate ?? perf?.sdk_success_rate_1s, NaN);
  const currentRate = finiteNumber(perf?.sdk_realtime_rate ?? perf?.sdk_success_rate_1s ?? perf?.current_rate, NaN);
  const successRate = finiteNumber(perf?.sdk_realtime_rate ?? perf?.sdk_success_rate_1s ?? perf?.success_rate ?? currentRate, NaN);
  const realtimeWindow = finiteNumber(perf?.sdk_realtime_window_seconds, 5);
  const processedLastSecond = finiteNumber(perf?.sdk_processed_last_1s ?? currentRate, 0);
  const successLastSecond = finiteNumber(perf?.sdk_success_last_1s ?? successRate, 0);
  const rateLimitSetting = finiteNumber(perf?.rate_limit_setting ?? task?.rate_limit, 0);
  const avgLatency = Number(perf?.avg_http_latency_ms ?? stats?.avg_http_latency_ms ?? 0);
  const httpRps = Number(perf?.http_requests_per_second || 0);
  const maxTheoreticalHttp = Number(perf?.max_theoretical_http_per_sec || 0);
  const activeWorkers = Number(perf?.active_workers ?? stats?.active_workers ?? 0);
  const effectiveConcurrency = Number(perf?.effective_concurrency ?? perf?.configured_concurrency ?? task?.concurrency ?? 0);
  const configuredUserConcurrency = Number(perf?.configured_user_concurrency ?? task?.concurrency ?? 0);
  const prefetchLength = Number(perf?.prefetch_queue_len ?? perf?.queue_prefetch_length ?? stats?.queue_prefetch_length ?? 0);
  const writeLength = Number(perf?.write_queue_len ?? perf?.queue_write_length ?? stats?.queue_write_length ?? 0);
  const writeDropped = Number(perf?.write_dropped || 0);
  const last429 = Number(perf?.last_429_count ?? stats?.last_429_count_per_minute ?? 0);
  const recent429Count2Min = Number(perf?.recent_429_count_2min || 0);
  const pidHistory = Array.isArray(perf?.pid_history) ? perf.pid_history : [];
  const pidLatestReason = String(perf?.pid_latest_reason || perf?.concurrency_reason || '').trim();
  const proxyHealth = resolveProxyHealth(task, perf);
  const channelHealth = resolveChannelHealth(taskStatus);
  const prefetchPct = queuePercent(prefetchLength, PREFETCH_QUEUE_MAX);
  const writePct = queuePercent(writeLength, WRITE_QUEUE_MAX);
  const latencyStyle = { color: latencyColor(avgLatency) };

  return (
    <div className="task-cockpit__grid">
      <div className="task-cockpit__card">
        <div className="task-cockpit__card-label">SDK 实时成功速率（每秒更新）</div>
        <div className="task-cockpit__card-row">
          <div className="task-cockpit__card-value" style={{ color: running ? '#2563eb' : '#9ca3af' }}>
            {running ? rateText(currentRate, running) : '—'}
            <span style={{ fontSize: 13, fontWeight: 500, marginLeft: 4 }}>封/秒</span>
          </div>
          <MiniRateBars samples={rateSamples} />
        </div>
        <Text type="secondary" style={{ fontSize: 11 }}>
          配置 {rateText(rateLimitSetting, false)} 封/秒 · 最近 {formatRate(realtimeWindow)} 秒处理 {formatCount(processedLastSecond)} 封 · 成功 {formatCount(successLastSecond)} 封
          {running && Number.isFinite(instantRate) ? ` · 当前 ${formatRate(instantRate)} 封/秒` : ''}
          {running && Number.isFinite(sdkSuccessRate) ? ` · 成功速率 ${formatRate(sdkSuccessRate)} 封/秒` : ''}
          {running && httpRps > 0 ? ` · HTTP ${formatRate(httpRps)} 次/秒` : ''}
        </Text>
      </div>

      <div className="task-cockpit__card">
        <div className="task-cockpit__card-label">HTTP 延迟</div>
        <div className="task-cockpit__card-row">
          <div className="task-cockpit__card-value" style={latencyStyle}>
            {avgLatency > 0 ? formatRate(avgLatency) : '—'}
            <span style={{ fontSize: 13, fontWeight: 500, marginLeft: 4 }}>ms</span>
          </div>
          <HealthIndicator level={avgLatency <= 0 ? 'idle' : avgLatency < 200 ? 'ok' : avgLatency <= 500 ? 'warn' : 'bad'} />
        </div>
        {maxTheoreticalHttp > 0 ? (
          <Text type="secondary" style={{ fontSize: 11 }}>理论 HTTP 上限 {formatRate(maxTheoreticalHttp)} 次/秒</Text>
        ) : null}
      </div>

      <div className="task-cockpit__card">
        <div className="task-cockpit__card-label">各环节耗时占比</div>
        <TaskPhaseTimingBar perf={perf} compact />
      </div>

      <div className="task-cockpit__card">
        <div className="task-cockpit__card-label">并发与队列</div>
        {task?.auto_concurrency ? (
          <div style={{ fontSize: 13, marginBottom: 6, color: '#2563eb' }}>
            当前自动并发：<strong>{formatCount(Number(perf?.auto_concurrency ?? effectiveConcurrency))}</strong>
            <Tag color="processing" style={{ marginLeft: 8, fontSize: 11 }}>自动调整</Tag>
          </div>
        ) : (
          <div style={{ fontSize: 13, marginBottom: 6, color: '#374151' }}>
            有效并发：<strong>{formatCount(effectiveConcurrency)}</strong>
            {configuredUserConcurrency !== effectiveConcurrency ? <Text type="secondary" style={{ marginLeft: 8, fontSize: 11 }}>用户配置 {formatCount(configuredUserConcurrency)}</Text> : null}
          </div>
        )}
        <div style={{ fontSize: 16, fontWeight: 700, marginBottom: 4 }}>
          {formatCount(activeWorkers)} / {formatCount(effectiveConcurrency)}
          <span style={{ fontSize: 11, fontWeight: 500, color: '#6b7280', marginLeft: 4 }}>活跃/有效</span>
        </div>
        <div className="task-cockpit__queue-bar">
          <div className="task-cockpit__queue-bar-label">
            <span>预取队列</span>
            <span>{formatCount(prefetchLength)} ({prefetchPct}%)</span>
          </div>
          <Progress percent={prefetchPct} showInfo={false} size="small" strokeColor={prefetchPct > 80 ? '#ef4444' : '#3b82f6'} />
        </div>
        <div className="task-cockpit__queue-bar">
          <div className="task-cockpit__queue-bar-label">
            <span>写日志队列</span>
            <span>{formatCount(writeLength)} ({writePct}%){writeDropped > 0 ? ` · 丢弃${formatCount(writeDropped)}` : ''}</span>
          </div>
          <Progress percent={writePct} showInfo={false} size="small" strokeColor={writePct > 80 || writeDropped > 0 ? '#ef4444' : '#6366f1'} />
        </div>
      </div>

      <div className={`task-cockpit__card${last429 > 0 ? ' task-cockpit__429-flash' : ''}`}>
        <div className="task-cockpit__card-label">429 限流</div>
        <div className="task-cockpit__card-value" style={{ color: last429 > 0 ? '#dc2626' : '#111827' }}>
          {formatCount(last429)}
          <span style={{ fontSize: 12, fontWeight: 500, marginLeft: 6, color: '#6b7280' }}>次/分钟</span>
        </div>
        <Text type="secondary" style={{ fontSize: 11 }}>近 2 分钟 {formatCount(recent429Count2Min)} 次</Text>
      </div>

      <div className="task-cockpit__card">
        <div className="task-cockpit__card-label">通道 / 代理健康</div>
        <div className="task-cockpit__card-row" style={{ marginBottom: 6 }}>
          <div style={{ minWidth: 0 }}>
            <div style={{ fontWeight: 600, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              {task?.channel_name || '—'}
            </div>
            <Text type="secondary" style={{ fontSize: 11 }}>{channelHealth.label}</Text>
          </div>
          <HealthIndicator level={channelHealth.level} />
        </div>
        <div className="task-cockpit__card-row">
          <div>
            <Text style={{ fontSize: 11 }}>{proxyHealth.label}</Text>
            <div style={{ fontWeight: 600 }}>{proxyHealth.latency}</div>
          </div>
          <HealthIndicator level={proxyHealth.level} />
        </div>
      </div>

      {task?.auto_concurrency ? (
        <div className="task-cockpit__card" style={{ gridColumn: '1 / -1' }}>
          <div className="task-cockpit__card-label">PID 并发调节器</div>
          {pidLatestReason ? (
            <Alert type="info" showIcon style={{ marginBottom: 8 }} message={pidLatestReason} />
          ) : pidHistory.length === 0 ? (
            <Text type="secondary" style={{ fontSize: 12 }}>暂无调节记录</Text>
          ) : null}
          {pidHistory.length > 0 ? (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 4, marginTop: 8 }}>
              {pidHistory.slice(-8).reverse().map((item, index) => {
                const targetRate = Number(item?.target_rate ?? 0);
                const actualRate = Number(item?.actual_rate ?? 0);
                const concurrency = Number(item?.concurrency ?? 0);
                const outputDelta = Number(item?.output_delta ?? 0);
                const tsKey = String(item?.ts ?? index);
                return (
                  <Text key={`${tsKey}-${index}`} style={{ fontSize: 12 }}>
                    目标 {formatRate(targetRate)} / 实际 {formatRate(actualRate)} 封/秒，并发 {formatCount(concurrency)}（Δ {outputDelta >= 0 ? '+' : ''}{outputDelta}）
                  </Text>
                );
              })}
            </div>
          ) : running ? (
            <Text type="secondary" style={{ fontSize: 12, marginTop: 8 }}>暂无 PID 历史记录</Text>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
