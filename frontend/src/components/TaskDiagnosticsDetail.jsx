import { Alert, Typography } from 'antd';
import { formatCount, formatRate, isTaskRunning } from '../utils/taskDetailFormat.js';

const { Text } = Typography;

export default function TaskDiagnosticsDetail({ taskStatus, perf, stats }) {
  const running = isTaskRunning(taskStatus) && perf?.running !== false;
  const effectiveConcurrency = Number(perf?.effective_concurrency || perf?.configured_concurrency || 0);
  const configuredUserConcurrency = Number(perf?.configured_user_concurrency || 0);
  const autoConcurrencyEnabled = Boolean(perf?.auto_concurrency_enabled);
  const activeWorkers = Number(perf?.active_workers || stats?.active_workers || 0);
  const prefetchLength = Number(perf?.prefetch_queue_len ?? perf?.queue_prefetch_length ?? stats?.queue_prefetch_length ?? 0);
  const writeLength = Number(perf?.write_queue_len ?? perf?.queue_write_length ?? stats?.queue_write_length ?? 0);
  const httpRps = Number(perf?.http_requests_per_second || 0);
  const batchSize = Number(perf?.send_http_batch_size || 50);
  const currentRate = Number(perf?.current_rate || 0);
  const rateLimitSetting = Number(perf?.rate_limit_setting ?? perf?.rate_limit_per_second ?? 0);
  const maxTheoreticalHttp = Number(perf?.max_theoretical_http_per_sec || 0);
  const writeDropped = Number(perf?.write_dropped || 0);
  const prefetchEmptySeconds = Number(perf?.prefetch_queue_empty_seconds || 0);
  const prefetchFetchCount = Number(perf?.prefetch_fetch_count || 0);
  const prefetchTotalRows = Number(perf?.prefetch_total_rows || 0);
  const recent429Count2Min = Number(perf?.recent_429_count_2min || 0);
  const httpRpsRecent = Array.isArray(perf?.http_rps_recent) && perf.http_rps_recent.length > 0
    ? perf.http_rps_recent
    : (httpRps > 0 ? [httpRps] : []);
  const recent429List = Array.isArray(perf?.recent_429_list) ? perf.recent_429_list : [];

  const prefetchEof = Boolean(perf?.prefetch_eof);
  const prefetchFailed = Boolean(perf?.prefetch_failed);
  const activeSends = Number(perf?.active_sends || 0);
  const outstandingWrites = Number(perf?.outstanding_writes || 0);
  const runtimeUptimeSeconds = Number(perf?.runtime_uptime_seconds || 0);
  const backendPrefetchStarvation = Boolean(perf?.prefetch_starvation);
  const prefetchWarn = running
    && backendPrefetchStarvation
    && !prefetchEof
    && !prefetchFailed
    && activeSends > 0
    && outstandingWrites === 0
    && runtimeUptimeSeconds >= 30
    && prefetchEmptySeconds >= 10;
  const writeWarn = writeDropped > 0;
  const rate429Warn = recent429Count2Min >= 3;

  if (!running) {
    return (
      <div className="task-cockpit__diag-strip task-cockpit__diag-strip--idle">
        <Text type="secondary">任务未运行，运行态性能指标将在启动后实时更新。</Text>
      </div>
    );
  }

  return (
    <div className="task-cockpit__diag-strip">
      <div className="task-cockpit__diag-panel">
        <Text strong className="task-cockpit__diag-title">速率与 HTTP</Text>
        <Text>实际 {formatRate(currentRate)} / 配置 {formatRate(rateLimitSetting)} 封/秒</Text>
        <Text type="secondary">
          HTTP {formatRate(httpRps)} 次/秒 · 理论上限 {formatRate(maxTheoreticalHttp)} 次/秒
        </Text>
        <Text type="secondary">
          最近频率：{httpRpsRecent.length > 0 ? `${httpRpsRecent.join(', ')} req/s` : '暂无数据'}
        </Text>
        <Text type="secondary">
          活跃并发 {formatCount(activeWorkers)} / 有效 {formatCount(effectiveConcurrency)} · 用户配置 {formatCount(configuredUserConcurrency)}{autoConcurrencyEnabled ? ' · 自动并发' : ''} · 批量 {formatCount(batchSize)}
        </Text>
      </div>

      <div className="task-cockpit__diag-panel">
        <Text strong className="task-cockpit__diag-title">预取队列</Text>
        {prefetchWarn ? (
          <Alert type="error" showIcon style={{ marginBottom: 4, padding: '2px 8px' }} message="收件人供应不足" />
        ) : null}
        <Text>当前长度 {formatCount(prefetchLength)}</Text>
        <Text type="secondary">
          近 10 秒取库 {formatCount(prefetchFetchCount)} 次 · 共 {formatCount(prefetchTotalRows)} 行
        </Text>
        {prefetchEmptySeconds > 0 ? (
          <Text type="secondary">队列已空 {formatRate(prefetchEmptySeconds)} 秒</Text>
        ) : null}
      </div>

      <div className="task-cockpit__diag-panel">
        <Text strong className="task-cockpit__diag-title">写日志队列</Text>
        {writeWarn ? (
          <Alert type="error" showIcon style={{ marginBottom: 4, padding: '2px 8px' }} message="写入过载，已丢日志" />
        ) : null}
        <Text>当前长度 {formatCount(writeLength)}</Text>
        <Text type="secondary">累计丢弃 {formatCount(writeDropped)} 条</Text>
      </div>

      <div className="task-cockpit__diag-panel">
        <Text strong className="task-cockpit__diag-title">429 时间线</Text>
        {rate429Warn ? (
          <Alert type="warning" showIcon style={{ marginBottom: 4, padding: '2px 8px' }} message="频繁限流" />
        ) : null}
        <Text type="secondary">近 2 分钟 {formatCount(recent429Count2Min)} 次</Text>
        {recent429List.length === 0 ? (
          <Text type="secondary">近 5 分钟无 429</Text>
        ) : (
          recent429List.slice(0, 4).map((item, index) => (
            <Text key={`${item.time}-${index}`} type="secondary" style={{ display: 'block', fontSize: 11 }}>
              {item.time} · 等待 {formatCount(item.retry_after)} 秒
            </Text>
          ))
        )}
      </div>
    </div>
  );
}
