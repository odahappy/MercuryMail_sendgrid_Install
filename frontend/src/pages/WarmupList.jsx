import { getErrorMessage } from '../api/index.js';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { Button, Card, Popconfirm, Progress, Space, Table, Tag, Typography, message } from 'antd';
import { EditOutlined, PlusOutlined, ReloadOutlined } from '@ant-design/icons';
import { deleteWarmupPlan, getWarmupPlans, pauseWarmupPlan, startWarmupPlan } from '../api/warmup.js';
import PageHeader from '../components/PageHeader.jsx';
import { getWarmupDisplayStatus } from '../utils/warmupStatus.js';

const { Text } = Typography;
const timezoneLabels = {
  'Asia/Shanghai': '北京时间',
  'Asia/Tokyo': '东京时间'
};

function formatWarmupSchedule(plan) {
  const timezone = plan.timezone || 'Asia/Shanghai';
  const label = timezoneLabels[timezone] || timezone;
  return `${plan.scheduled_time || '00:00:00'}（${label}）`;
}

function getTodayProgress(plan) {
  const dailyTarget = Math.max(0, Number(plan?.daily_target ?? plan?.target_today ?? 0));
  const remainingToday = Math.max(0, Number(plan?.remaining_today ?? 0));
  if (dailyTarget <= 0) {
    return { dailyTarget: 0, processedToday: 0, percent: 0 };
  }
  const processedToday = Math.max(0, Math.min(dailyTarget, dailyTarget - remainingToday));
  return {
    dailyTarget,
    processedToday,
    percent: Math.max(0, Math.min(100, Number(((processedToday / dailyTarget) * 100).toFixed(2)))),
  };
}

function getLifetimeProgress(plan) {
  const recipientTotal = Math.max(0, Number(plan?.recipient_total || 0));
  const recipientSent = Math.max(0, Number(plan?.recipient_sent || 0));
  if (recipientTotal <= 0) {
    return { recipientTotal: 0, recipientSent: 0, percent: 0 };
  }
  return {
    recipientTotal,
    recipientSent,
    percent: Math.max(0, Math.min(100, Number(((recipientSent / recipientTotal) * 100).toFixed(2)))),
  };
}

export default function WarmupList() {
  const navigate = useNavigate();
  const refreshTimerRef = useRef(null);
  const plansRequestInFlightRef = useRef(false);
  const isPageVisibleRef = useRef(typeof document === 'undefined' ? true : document.visibilityState !== 'hidden');
  const [plans, setPlans] = useState([]);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [actingId, setActingId] = useState(0);
  const [connectionState, setConnectionState] = useState('idle');
  const totalPages = useMemo(() => Math.max(1, Math.ceil(total / pageSize)), [total, pageSize]);

  const loadPlans = useCallback(async () => {
    if (plansRequestInFlightRef.current) return;
    plansRequestInFlightRef.current = true;
    setLoading(true);
    try {
      const data = await getWarmupPlans(page, pageSize);
      setPlans(data.items || []);
      setTotal(data.total || 0);
    } catch (error) {
      message.error(getErrorMessage(error, '加载养号计划失败'));
    } finally {
      plansRequestInFlightRef.current = false;
      setLoading(false);
    }
  }, [page, pageSize]);

  useEffect(() => {
    const handleVisibility = () => {
      isPageVisibleRef.current = document.visibilityState !== 'hidden';
      setConnectionState(isPageVisibleRef.current ? 'polling' : 'idle');
      if (isPageVisibleRef.current) loadPlans();
    };
    document.addEventListener('visibilitychange', handleVisibility);
    loadPlans();
    setConnectionState('polling');
    const timer = setInterval(() => {
      if (isPageVisibleRef.current) loadPlans();
    }, 5000);
    return () => {
      document.removeEventListener('visibilitychange', handleVisibility);
      clearInterval(timer);
    };
  }, [loadPlans]);

  useEffect(() => () => {
    if (refreshTimerRef.current) window.clearTimeout(refreshTimerRef.current);
  }, []);

  const startPlan = async (plan) => {
    setActingId(plan.id);
    try {
      const result = await startWarmupPlan(plan.id);
      message.success(result?.message || '养号计划已启动');
      await loadPlans();
      if (refreshTimerRef.current) window.clearTimeout(refreshTimerRef.current);
      refreshTimerRef.current = window.setTimeout(loadPlans, 1500);
    } catch (error) {
      message.error(getErrorMessage(error, '启动养号计划失败'));
    } finally {
      setActingId(0);
    }
  };

  const pausePlan = async (plan) => {
    setActingId(plan.id);
    try {
      await pauseWarmupPlan(plan.id);
      message.success('养号计划已暂停');
      await loadPlans();
    } catch (error) {
      message.error(getErrorMessage(error, '暂停养号计划失败'));
    } finally {
      setActingId(0);
    }
  };

  const removePlan = async (plan) => {
    setActingId(plan.id);
    try {
      await deleteWarmupPlan(plan.id);
      message.success('养号计划已删除');
      await loadPlans();
    } catch (error) {
      message.error(getErrorMessage(error, '删除养号计划失败'));
    } finally {
      setActingId(0);
    }
  };

  const columns = [
    { title: '计划名称', dataIndex: 'name', key: 'name', render: (_, plan) => <Link to={`/warmup/${plan.id}`}><Text strong>{plan.name}</Text></Link> },
    { title: '通道', dataIndex: 'channel_name', key: 'channel_name' },
    { title: '状态', dataIndex: 'status', key: 'status', render: (_, plan) => {
      const display = getWarmupDisplayStatus(plan);
      return <Tag color={display.tagColor}>{display.label}</Tag>;
    } },
    {
      title: '进度',
      key: 'progress',
      render: (_, plan) => {
        const todayProgress = getTodayProgress(plan);
        const lifetimeProgress = getLifetimeProgress(plan);
        const runtimePercent = typeof plan.progress === 'number' ? Math.max(0, Math.min(100, plan.progress)) : 0;
        return (
          <Space direction="vertical" size={0} style={{ width: '100%' }}>
            <Progress percent={runtimePercent || todayProgress.percent} size="small" status={plan.running ? 'active' : 'normal'} />
            <Text type="secondary">
              今日已处理 {todayProgress.dailyTarget || 0}；成功 {Number(plan.success_count || 0)} / 失败 {Number(plan.fail_count || 0)}
            </Text>
            <Text type="secondary">
              累计已发送 {lifetimeProgress.recipientSent}/{lifetimeProgress.recipientTotal}
            </Text>
          </Space>
        );
      },
    },
    { title: '发送天数', key: 'total_days', render: (_, plan) => <Space direction="vertical" size={0}><Text>总计 {Number(plan.total_days || 0) > 0 ? `${plan.total_days} 天` : '不限'}</Text><Text type="secondary">已运行 {Number(plan.elapsed_days || 0)} 天；剩余 {plan.remaining_days === null || plan.remaining_days === undefined ? '不限' : `${plan.remaining_days} 天`}</Text></Space> },
    { title: '实时消息', dataIndex: 'last_message', key: 'last_message', ellipsis: true, render: (value, plan) => value || (plan.status === 'finished' ? '养号计划已完成，累计发送天数已达到预设值' : '—') },
    { title: '每日定时', key: 'scheduled_time', render: (_, plan) => <Space direction="vertical" size={0}><Text>{formatWarmupSchedule(plan)}</Text><Text type="secondary">按计划时区本地时间自动启动</Text></Space> },
    {
      title: '操作',
      key: 'actions',
      width: 400,
      render: (_, plan) => <Space wrap><Button size="small" type="primary" loading={actingId === plan.id} disabled={plan.status === 'completed' || plan.running || Number(plan.recipient_unsent || 0) <= 0} onClick={() => startPlan(plan)}>{plan.status === 'paused' ? '恢复' : '启动'}</Button><Button size="small" loading={actingId === plan.id} disabled={plan.status === 'paused' || plan.status === 'completed'} onClick={() => pausePlan(plan)}>暂停</Button><Button size="small" onClick={() => navigate(`/warmup/${plan.id}`)}>详情</Button><Button size="small" icon={<EditOutlined />} disabled={plan.running} onClick={() => navigate(`/warmup/${plan.id}`, { state: { openPlanEditor: true } })}>更改任务</Button><Button size="small" onClick={() => navigate('/logs', { state: { defaultLogType: 'warmup', planId: plan.id } })}>日志</Button><Popconfirm title="确认删除该养号计划？" okText="删除" cancelText="取消" onConfirm={() => removePlan(plan)}><Button size="small" danger loading={actingId === plan.id}>删除</Button></Popconfirm></Space>
    }
  ];

  return <Space direction="vertical" size="large" className="mm-page">
    <PageHeader
      title="AI 养号"
      description="每个计划绑定一个 SendGrid 通道，独立生成文案并按每日策略发送。列表页每 5 秒轮询刷新进度。"
      extra={<Space><Tag color={connectionState === 'polling' ? 'blue' : 'default'}>{connectionState === 'polling' ? '轮询刷新中' : '空闲'}</Tag><Button icon={<ReloadOutlined />} onClick={loadPlans} loading={loading}>刷新</Button><Button type="primary" icon={<PlusOutlined />} onClick={() => navigate('/warmup/create')}>创建计划</Button></Space>}
    />
    <Card bordered={false} styles={{ body: { padding: 0 } }}><Table rowKey="id" bordered loading={loading} columns={columns} dataSource={plans} locale={{ emptyText: '暂无养号计划' }} pagination={{ current: page, pageSize, total, showSizeChanger: true, showTotal: (value) => `共 ${value} 条，第 ${page} / ${totalPages} 页`, onChange: (p, ps) => { setPage(p); setPageSize(ps); } }} /></Card>
  </Space>;
}
