import { getErrorMessage } from '../api/index.js';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useAbortController } from '../hooks/useAbortController.js';
import { useNavigate } from 'react-router-dom';
import {
  Alert,
  Button,
  Card,
  Col,
  Progress,
  Row,
  Skeleton,
  Space,
  Statistic,
  Table,
  Tag,
  Tooltip,
  Typography,
  message,
} from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import { getDashboardStats } from '../api/dashboard.js';
import { subscribeDashboardRefresh } from '../utils/dashboardRefresh.js';
import CpuCircle from '../components/CpuCircle.jsx';
import PageHeader from '../components/PageHeader.jsx';

const { Text } = Typography;
const POLL_MS = 8000;

import { TASK_STATUS_COLORS, resolveTaskStatusLabel } from '../utils/taskDetailFormat.js';

function formatRate(value) {
  const num = Number(value || 0);
  return Number.isInteger(num) ? String(num) : num.toFixed(1);
}

function formatCount(value) {
  return Number(value || 0).toLocaleString('zh-CN');
}

function latencyColor(ms) {
  const value = Number(ms || 0);
  if (value > 1000) return '#ef4444';
  if (value > 500) return '#eab308';
  return '#22c55e';
}

function memoryStrokeColor(percent) {
  const value = Number(percent || 0);
  if (value >= 90) return '#ef4444';
  if (value >= 70) return '#eab308';
  return '#22c55e';
}

function HealthDot({ online }) {
  return (
    <span className={`mm-health-dot ${online ? 'mm-health-dot--online' : 'mm-health-dot--offline'}`} />
  );
}

function parseProgress(progressText) {
  const text = String(progressText || '0/0');
  const [done, total] = text.split('/').map((item) => Number(item || 0));
  if (!total) return 0;
  return Math.min(100, Math.round((done / total) * 100));
}

function isDashboardQuotaEnabled(quota) {
  const value = Number(quota);
  return Number.isFinite(value) && value > 0;
}

export default function Dashboard() {
  const navigate = useNavigate();
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState('');
  const [isPageVisible, setIsPageVisible] = useState(
    typeof document === 'undefined' ? true : document.visibilityState !== 'hidden',
  );
  const { createAbortController, releaseAbortController, cleanupAllControllers } = useAbortController();
  const statsRequestInFlightRef = useRef(false);
  const statsQueuedSilentRefreshRef = useRef(false);

  const loadStats = useCallback(async (silent = false) => {
    if (statsRequestInFlightRef.current) {
      if (silent) statsQueuedSilentRefreshRef.current = true;
      return;
    }
    statsRequestInFlightRef.current = true;
    if (!silent) setLoading(true);
    const controller = createAbortController();
    try {
      const nextStats = await getDashboardStats({ signal: controller.signal });
      setStats((currentStats) => ({ ...(currentStats || {}), ...(nextStats || {}) }));
      setLoadError('');
    } catch (error) {
      if (error?.code === 'ERR_CANCELED') return;
      const messageText = getErrorMessage(error, '加载仪表板失败');
      if (!silent) {
        setLoadError(messageText);
        message.error(messageText);
      } else {
        setLoadError((prev) => prev || '数据加载中，请稍候...');
      }
    } finally {
      statsRequestInFlightRef.current = false;
      if (!silent) setLoading(false);
      releaseAbortController(controller);
      if (statsQueuedSilentRefreshRef.current) {
        statsQueuedSilentRefreshRef.current = false;
        window.setTimeout(() => loadStats(true), 0);
      }
    }
  }, [createAbortController, releaseAbortController]);

  useEffect(() => {
    const handleVisibilityChange = () => {
      const visible = document.visibilityState !== 'hidden';
      setIsPageVisible(visible);
      if (visible) {
        loadStats(true);
      } else {
        cleanupAllControllers();
      }
    };
    document.addEventListener('visibilitychange', handleVisibilityChange);
    return () => {
      document.removeEventListener('visibilitychange', handleVisibilityChange);
    };
  }, [cleanupAllControllers, loadStats]);

  useEffect(() => {
    if (!isPageVisible) return undefined;
    loadStats();
    const timer = window.setInterval(() => loadStats(true), POLL_MS);
    return () => window.clearInterval(timer);
  }, [loadStats, isPageVisible]);

  useEffect(() => {
    return subscribeDashboardRefresh(() => {
      loadStats(true);
    });
  }, [loadStats]);

  const rateRatio = useMemo(() => {
    const serverRatio = Number(stats?.global_rate_achievement_percent);
    if (Number.isFinite(serverRatio) && serverRatio > 0) return Math.min(100, Math.round(serverRatio));
    const actual = Number(stats?.global_current_rate || 0);
    const limit = Number(stats?.global_rate_limit_sum || 0);
    if (limit <= 0) return actual > 0 ? 100 : 0;
    return Math.min(100, Math.round((actual / limit) * 100));
  }, [stats]);

  const theoreticalRate = Number(stats?.global_max_theoretical_rate || 0);

  const quotaRatio = useMemo(() => {
    const sent = Number(stats?.today_api_sent || 0);
    const quota = Number(stats?.today_quota || 0);
    if (quota <= 0) return 0;
    return Math.min(100, Math.round((sent / quota) * 100));
  }, [stats]);

  const showQuotaCard = isDashboardQuotaEnabled(stats?.today_quota);
  const showRateWarning = rateRatio < 80 && Number(stats?.global_rate_limit_sum || 0) > 0;
  const showQuotaWarning = showQuotaCard && quotaRatio >= 90;

  const bannerMessages = useMemo(() => {
    const items = [];
    if (stats?.alert_all_channels_offline) items.push('所有通道离线，请检查通道与 API Key 配置');
    if (stats?.alert_all_proxies_offline) items.push('所有代理离线，发送可能中断');
    if (stats?.alert_global_concurrency_rejected) items.push('全局并发槽已满，新任务启动被拒绝');
    if (stats?.alert_recent_429_burst) items.push('过去 15 分钟内 429 限流次数超过 20 次，请降低并发或增大批次间隔');
    const taskAlerts = (stats?.task_rate_comparison || []).filter((item) => item.alert);
    if (taskAlerts.length > 0) {
      items.push(`${taskAlerts.length} 个任务速率低于目标 80%，请检查 PID 调节与代理延迟`);
    }
    return items;
  }, [stats]);

  const concurrencyRatio = useMemo(() => {
    const occupied = Number(stats?.admitted_workers || 0);
    const limit = Number(stats?.admitted_workers_limit || 0);
    if (limit <= 0) return 0;
    return Math.min(100, Math.round((occupied / limit) * 100));
  }, [stats]);

  const recentColumns = [
    {
      title: '任务名称',
      dataIndex: 'name',
      key: 'name',
      render: (value, row) => <Text strong>{value || `任务${row.id}`}</Text>,
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 110,
      render: (_, row) => (
        <Tag color={TASK_STATUS_COLORS[row.status] || 'default'}>
          {resolveTaskStatusLabel(row)}
        </Tag>
      ),
    },
    {
      title: '实时速率',
      dataIndex: 'current_rate',
      key: 'current_rate',
      width: 110,
      render: (value) => `${formatRate(value)} 封/秒`,
    },
    {
      title: '剩余待发',
      dataIndex: 'pending_count',
      key: 'pending_count',
      width: 100,
      render: (value) => formatCount(value),
    },
    {
      title: '进度',
      key: 'progress',
      render: (_, row) => (
        <div style={{ minWidth: 160 }}>
          <Progress
            percent={parseProgress(row.progress)}
            size="small"
            format={() => row.progress || '0/0'}
          />
        </div>
      ),
    },
  ];

  return (
    <Space direction="vertical" size="large" className="mm-page">
      {bannerMessages.length > 0 ? (
        <Alert className="mm-alert-banner" type="error" showIcon message="全局告警" description={bannerMessages.join('；')} />
      ) : null}

      <PageHeader
        title="仪表盘"
        description="实时展示任务发送、速率、并发、队列和基础设施健康状态。"
        extra={<Button icon={<ReloadOutlined />} loading={loading && !stats} onClick={() => loadStats()}>刷新</Button>}
      />

      {loadError && !stats ? (
        <Alert
          type={loading ? 'info' : 'warning'}
          showIcon
          message={loading ? '数据加载中，请稍候...' : '仪表板数据加载失败'}
          description={loading ? '正在从服务器获取最新统计，大数据量时可能需要几秒钟。' : loadError}
        />
      ) : null}

      {loading && !stats ? (
        <Space direction="vertical" size="large" style={{ width: '100%' }}>
          <Row gutter={[16, 16]}>
            {[1, 2, 3, 4, 5].map((item) => (
              <Col xs={24} sm={12} xl={item <= 4 ? 5 : 4} key={item}>
                <Card><Skeleton active paragraph={{ rows: 2 }} /></Card>
              </Col>
            ))}
          </Row>
          <Row gutter={[16, 16]}>
            <Col xs={24} md={12}><Card><Skeleton active paragraph={{ rows: 4 }} /></Card></Col>
            <Col xs={24} md={12}><Card><Skeleton active paragraph={{ rows: 4 }} /></Card></Col>
          </Row>
          <Row gutter={[16, 16]}>
            {[1, 2, 3].map((item) => (
              <Col xs={24} md={8} key={item}><Card><Skeleton active paragraph={{ rows: 5 }} /></Card></Col>
            ))}
          </Row>
          <Card><Skeleton active paragraph={{ rows: 6 }} /></Card>
        </Space>
      ) : (
        <>
      <Row gutter={[16, 16]} className="mm-dashboard-grid">
        <Col xs={24} sm={12} md={8} xl={6} style={{ display: 'flex' }}>
          <Card
            className="mm-dashboard-card mm-dashboard-card--metric mm-stat-card mm-stat-card--clickable"
            hoverable
            onClick={() => navigate('/tasks?status=running')}
            loading={loading && !stats}
          >
            <div className="mm-dashboard-card__metric-content">
              <Statistic
                title="运行中任务"
                value={stats?.running_tasks ?? '-'}
                suffix={<Text type="secondary">/ {formatCount(stats?.total_tasks || 0)}</Text>}
              />
              <Text className="mm-dashboard-card__note">点击查看运行中任务</Text>
            </div>
          </Card>
        </Col>
        <Col xs={24} sm={12} md={8} xl={6} style={{ display: 'flex' }}>
          <Tooltip title="基于历史发送日志统计，非当前收件人池。">
            <Card className="mm-dashboard-card mm-dashboard-card--metric mm-stat-card" loading={loading && !stats}>
              <div className="mm-dashboard-card__metric-content">
                <Statistic title="发送日志收件人" value={stats ? formatCount(stats.valid_recipients) : '-'} />
                <Text className="mm-dashboard-card__note">基于历史发送日志统计</Text>
              </div>
            </Card>
          </Tooltip>
        </Col>
        <Col xs={24} sm={12} md={8} xl={6} style={{ display: 'flex' }}>
          <Card className="mm-dashboard-card mm-dashboard-card--metric mm-stat-card" loading={loading && !stats}>
            <div className="mm-dashboard-card__metric-content">
              <Statistic title="今日发送成功" value={formatCount(stats?.today_api_sent || 0)} />
              <Text className="mm-dashboard-card__note">基于 SDK 发送日志成功状态</Text>
            </div>
          </Card>
        </Col>
        <Col xs={24} sm={12} md={8} xl={6} style={{ display: 'flex' }}>
          <Card className="mm-dashboard-card mm-dashboard-card--metric mm-stat-card" loading={loading && !stats}>
            <div className="mm-dashboard-card__metric-content">
              <Statistic title="今日发送失败" value={formatCount(stats?.today_api_failed || 0)} />
              <Text className="mm-dashboard-card__note">基于 SDK 发送日志失败状态</Text>
            </div>
          </Card>
        </Col>
        <Col xs={24} sm={12} md={8} xl={6} style={{ display: 'flex' }}>
          <Tooltip title="过去 15 分钟内 SendGrid 429 限流次数">
            <Card className="mm-dashboard-card mm-dashboard-card--metric mm-stat-card" loading={loading && !stats}>
              <div className="mm-dashboard-card__metric-content">
                <Statistic title="429 限流" value={formatCount(stats?.recent_429_count || 0)} suffix="次/15分钟" />
              </div>
            </Card>
          </Tooltip>
        </Col>
        <Col xs={24} sm={12} md={8} xl={6} style={{ display: 'flex' }}>
          <Card className="mm-dashboard-card mm-dashboard-card--metric mm-stat-card" loading={loading && !stats}>
            <div className="mm-dashboard-card__metric-content">
              <Statistic title="全局实时速率" value={stats ? formatRate(stats.global_current_rate) : '-'} suffix="封/秒" />
              <Text className="mm-dashboard-card__note">理论最大 {formatRate(theoreticalRate)} 封/秒</Text>
              <Progress percent={rateRatio} size="small" status={showRateWarning ? 'exception' : 'normal'} showInfo={false} style={{ marginTop: 8 }} />
              {showRateWarning ? <Text type="warning" className="mm-dashboard-card__note">低于配置速率 80%</Text> : null}
            </div>
          </Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]} className="mm-dashboard-grid">
        <Col xs={24} md={12} style={{ display: 'flex' }}>
          <Card className="mm-dashboard-card mm-dashboard-card--panel mm-panel-card" title="全局并发水位" loading={loading && !stats}>
            <div className="mm-dashboard-card__panel-content">
              <Statistic
                title="已准入 Worker / 上限"
                value={`${formatCount(stats?.admitted_workers || 0)} / ${formatCount(stats?.admitted_workers_limit || 0)}`}
              />
              <Text className="mm-dashboard-card__note">HTTP 在飞：{formatCount(stats?.active_http_inflight || 0)}　执行中任务：{formatCount(stats?.executing_tasks || 0)}</Text>
            <Progress
                percent={concurrencyRatio}
                status={concurrencyRatio >= 90 ? 'exception' : 'active'}
                style={{ marginTop: 12 }}
              />
            </div>
          </Card>
        </Col>
        <Col xs={24} md={12} style={{ display: 'flex' }}>
          <Card className="mm-dashboard-card mm-dashboard-card--panel mm-panel-card" title="SenderPool 发送线程池" loading={loading && !stats}>
            <div className="mm-dashboard-card__panel-content">
              <Text>
                活跃线程 {formatCount(stats?.sender_pool_stats?.alive_workers || 0)} / {formatCount(stats?.sender_pool_stats?.workers || 0)}
                <Text type="secondary">（系统线程池，始终就绪）</Text>
              </Text>
              <Text type="secondary">队列深度 {formatCount(stats?.sender_pool_stats?.queue_depth || 0)}，进行中 {formatCount(stats?.sender_pool_stats?.inflight || 0)}</Text>
              {Number(stats?.sender_pool_stats?.dropped || 0) > 0 ? (
                <Alert type="error" showIcon style={{ marginTop: 12 }} message={`发送池已丢弃 ${formatCount(stats.sender_pool_stats.dropped)} 个请求`} />
              ) : null}
            </div>
          </Card>
        </Col>
      </Row>

      {(stats?.task_rate_comparison || []).length > 0 ? (
        <Card className="mm-panel-card" title="运行中任务速率对比" loading={loading && !stats}>
          <Table
            size="small"
            pagination={false}
            rowKey="task_id"
            dataSource={stats.task_rate_comparison}
            columns={[
              { title: '任务 ID', dataIndex: 'task_id', width: 90 },
              { title: '任务名称', dataIndex: 'task_name', ellipsis: true },
              {
                title: '实际 / 目标',
                key: 'rates',
                render: (_, row) => `${formatRate(row.actual_rate)} / ${formatRate(row.target_rate)} 封/秒`,
              },
              {
                title: '达标率',
                dataIndex: 'rate_ratio_percent',
                width: 100,
                render: (value, row) => (
                  <Text type={row.alert ? 'danger' : undefined}>{formatRate(value)}%</Text>
                ),
              },
              { title: '活跃协程', dataIndex: 'active_workers', width: 100, render: (v) => formatCount(v) },
            ]}
          />
        </Card>
      ) : null}

      <Row gutter={[16, 16]} className="mm-dashboard-grid">
        <Col xs={24} md={showQuotaCard ? 12 : 24} style={{ display: 'flex' }}>
          <Card className="mm-dashboard-card mm-dashboard-card--panel mm-panel-card" title="速率达标率" loading={loading && !stats}>
            <div className="mm-dashboard-card__panel-content mm-dashboard-card__centered">
              <div style={{ display: 'flex', justifyContent: 'center' }}>
                <Progress
                  type="dashboard"
                  percent={rateRatio}
                  strokeColor={rateRatio >= 80 ? '#22c55e' : '#eab308'}
                  format={(p) => `${p}%`}
                />
              </div>
              <Text className="mm-dashboard-card__note">实际 {formatRate(stats?.global_current_rate || 0)} / 配置 {formatRate(stats?.global_rate_limit_sum || 0)} 封/秒</Text>
              <Text className="mm-dashboard-card__note">
                {rateRatio >= 80 ? '当前速率达标' : '速率未达标，建议检查代理/并发'}
              </Text>
            </div>
          </Card>
        </Col>
        {showQuotaCard ? (
        <Col xs={24} md={12} style={{ display: 'flex' }}>
          <Card className="mm-dashboard-card mm-dashboard-card--panel mm-panel-card" title="今日配额使用" loading={loading && !stats}>
            <div className="mm-dashboard-card__panel-content">
              <Text>已发送成功 {formatCount(stats?.today_api_sent || 0)} / 配额 {formatCount(stats?.today_quota || 0)}</Text>
              <Progress
                percent={quotaRatio}
                status={showQuotaWarning ? 'exception' : 'active'}
                style={{ marginTop: 12 }}
              />
              {showQuotaWarning ? <Text type="danger" className="mm-dashboard-card__note">配额使用已超过 90%，请注意控制发送量</Text> : null}
            </div>
          </Card>
        </Col>
        ) : null}
      </Row>

      <Row gutter={[16, 16]} className="mm-dashboard-grid">
        <Col xs={24} md={8} style={{ display: 'flex' }}>
          <Card className="mm-dashboard-card mm-dashboard-card--panel mm-panel-card" title="通道健康" loading={loading && !stats}>
            <div className="mm-dashboard-card__panel-content">
              {(stats?.channels_health || []).length === 0 ? (
                <Text type="secondary" className="mm-dashboard-card__note">暂无通道</Text>
              ) : (
                <Space direction="vertical" style={{ width: '100%' }}>
                  {(stats?.channels_health || []).map((item) => (
                    <div key={item.id} style={{ display: 'flex', justifyContent: 'space-between' }}>
                      <Text><HealthDot online={item.online} />{item.name}</Text>
                      <Text style={{ color: latencyColor(item.latency_ms) }}>{item.latency_ms} ms</Text>
                    </div>
                  ))}
                </Space>
              )}
            </div>
          </Card>
        </Col>
        <Col xs={24} md={8} style={{ display: 'flex' }}>
          <Card className="mm-dashboard-card mm-dashboard-card--panel mm-panel-card" title="代理健康" loading={loading && !stats}>
            <div className="mm-dashboard-card__panel-content">
              {(stats?.proxies_health || []).length === 0 ? (
                <Text type="secondary" className="mm-dashboard-card__note">暂无代理</Text>
              ) : (
                <Space direction="vertical" style={{ width: '100%' }}>
                  {(stats?.proxies_health || []).map((item) => (
                    <div key={item.id} style={{ display: 'flex', justifyContent: 'space-between', gap: 8 }}>
                      <Text ellipsis><HealthDot online={item.online} />{item.proxy}</Text>
                      <Text style={{ color: latencyColor(item.latency_ms), whiteSpace: 'nowrap' }}>{item.latency_ms} ms</Text>
                    </div>
                  ))}
                </Space>
              )}
              <Text className="mm-dashboard-card__note mm-dashboard-card__resource-meta">
                平均 {stats?.avg_proxy_latency_ms || 0} ms / 最高 {stats?.proxy_latency_max_ms || 0} ms
              </Text>
            </div>
          </Card>
        </Col>
        <Col xs={24} md={8} style={{ display: 'flex' }}>
          <Card className="mm-dashboard-card mm-dashboard-card--panel mm-panel-card" title="系统资源" loading={loading && !stats}>
            <div className="mm-dashboard-card__panel-content">
              <div className="mm-dashboard-card__resource-layout">
                <div className="mm-dashboard-card__resource-block">
                  <CpuCircle percent={stats?.cpu_percent || 0} />
                  <Text className="mm-dashboard-card__note">CPU 使用率</Text>
                </div>
                <div style={{ flex: 1 }}>
                  <Text>内存使用率</Text>
                  <Progress percent={Number(stats?.memory_percent || 0)} strokeColor={memoryStrokeColor(stats?.memory_percent)} />
                </div>
              </div>
              <Text className="mm-dashboard-card__note mm-dashboard-card__resource-meta">
                最近 15 分钟 429：{formatCount(stats?.recent_429_count || 0)} 次
              </Text>
            </div>
          </Card>
        </Col>
      </Row>

      <Card className="mm-dashboard-card mm-dashboard-card--table mm-panel-card" title="最近任务" bordered={false} loading={loading && !stats}>
        <Table
          rowKey="id"
          size="small"
          columns={recentColumns}
          dataSource={stats?.recent_tasks || []}
          pagination={false}
          locale={{ emptyText: '暂无最近任务。' }}
          onRow={(row) => ({
            onClick: () => navigate(`/tasks/${row.id}`),
            className: 'mm-table-row--clickable',
          })}
        />
      </Card>
        </>
      )}
    </Space>
  );
}
