import { getErrorMessage } from '../api/index.js';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useParams, Link, useLocation, useNavigate } from 'react-router-dom';
import { Alert, Breadcrumb, Button, Card, Descriptions, Form, Input, InputNumber, Modal, Progress, Space, Tag, TimePicker, Typography, message } from 'antd';
import { ArrowLeftOutlined, EditOutlined, PlusOutlined, ReloadOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';
import { addWarmupRecipients, getWarmupPlan, pauseWarmupPlan, regenerateWarmupContent, startWarmupPlan, updatePlan, updatePlanContent } from '../api/warmup.js';
import { isShutdownConflict409, runWithShutdownRetry } from '../utils/shutdownRetry.js';
import { getWarmupDisplayStatus } from '../utils/warmupStatus.js';

const AI_GENERATION_FIELDS = ['prompt', 'content_language', 'tone', 'must_include', 'avoid', 'fidelity_mode'];

function buildAiParamsSnapshot(source) {
  return {
    prompt: (source?.prompt || '').trim(),
    content_language: source?.content_language || 'zh',
    tone: source?.tone || '自然日常',
    must_include: (source?.must_include || '').trim(),
    avoid: (source?.avoid || '').trim(),
    fidelity_mode: source?.fidelity_mode || 'strict',
  };
}

function aiGenerationParamsChanged(before, after) {
  return AI_GENERATION_FIELDS.some((field) => before[field] !== after[field]);
}

const { Title, Text, Paragraph } = Typography;
const { TextArea } = Input;
const timezoneLabels = {
  'Asia/Shanghai': '北京时间',
  'Asia/Tokyo': '东京时间',
};

function estimateWarmupDailyTarget(plan) {
  if (!plan) return 0;
  const fromApi = Number(plan.daily_target ?? plan.target_today ?? 0);
  if (fromApi > 0) return fromApi;
  const startCount = Math.max(1, Number(plan.start_count || 1));
  const maxDaily = Math.max(1, Number(plan.max_daily_count || startCount));
  const rate = Math.max(0, Number(plan.daily_increase_rate || 0));
  if (!plan.last_run_date) return Math.min(startCount, maxDaily);
  const lastDaily = Number(plan.last_daily_target || 0);
  const previousTarget = lastDaily >= 1 ? lastDaily : Math.min(startCount, maxDaily);
  return Math.min(Math.max(1, Math.ceil(previousTarget * (1 + rate))), maxDaily);
}

const TIMEZONE_OPTIONS = [
  { value: 'Asia/Shanghai', label: 'Asia/Shanghai 北京时间' },
  { value: 'Asia/Tokyo', label: 'Asia/Tokyo 东京时间' },
];

function formatWarmupSchedule(plan) {
  const timezone = plan?.timezone || 'Asia/Shanghai';
  const label = timezoneLabels[timezone] || timezone;
  return `${plan?.scheduled_time || '00:00:00'}（${label}，${timezone}）`;
}

function getWarmupTodayProgress(plan) {
  const dailyTarget = Math.max(0, Number(plan?.daily_target ?? plan?.target_today ?? 0));
  const remainingToday = Math.max(0, Number(plan?.remaining_today ?? 0));
  if (dailyTarget <= 0) {
    return { dailyTarget: 0, processedToday: 0, remainingToday: 0, percent: 0 };
  }
  const processedToday = Math.max(0, Math.min(dailyTarget, dailyTarget - remainingToday));
  return {
    dailyTarget,
    processedToday,
    remainingToday,
    percent: Math.max(0, Math.min(100, Number(((processedToday / dailyTarget) * 100).toFixed(2)))),
  };
}

function getWarmupLifetimeProgress(plan) {
  const totalRecipients = Math.max(0, Number(plan?.recipient_total || 0));
  const sentRecipients = Math.max(0, Number(plan?.recipient_sent || 0));
  if (totalRecipients <= 0) {
    return { totalRecipients: 0, sentRecipients: 0, percent: 0 };
  }
  return {
    totalRecipients,
    sentRecipients,
    percent: Math.max(0, Math.min(100, Number(((sentRecipients / totalRecipients) * 100).toFixed(2)))),
  };
}

function maskProxy(proxy) {
  if (!proxy?.host || !proxy?.port) return '直连';
  const rawScheme = String(proxy.protocol || proxy.type || 'HTTP').toLowerCase();
  const scheme = rawScheme === 'socks5' ? 'socks5' : 'http';
  const auth = proxy.username ? '***@' : '';
  return `${scheme}://${auth}${proxy.host}:${proxy.port}`;
}

function getPlanProxy(plan) {
  const channelProxy = plan?.channel?.proxy;
  if (channelProxy?.host && channelProxy?.port) {
    return {
      name: channelProxy.name || plan.channel?.proxy_name || '代理',
      address: maskProxy(channelProxy),
    };
  }
  const fallbackAddress = String(plan?.channel?.proxy_address || '').trim();
  if (fallbackAddress && fallbackAddress !== '直连') {
    return {
      name: plan.channel?.proxy_name || '代理',
      address: fallbackAddress,
    };
  }
  return null;
}

export default function WarmupDetail() {
  const { id } = useParams();
  const location = useLocation();
  const navigate = useNavigate();
  const [contentForm] = Form.useForm();
  const [planForm] = Form.useForm();
  const [plan, setPlan] = useState(null);
  const [loading, setLoading] = useState(false);
  const [acting, setActing] = useState(false);
  const [addModalOpen, setAddModalOpen] = useState(false);
  const [contentModalOpen, setContentModalOpen] = useState(false);
  const [planModalOpen, setPlanModalOpen] = useState(false);
  const [recipientText, setRecipientText] = useState('');
  const [addingRecipients, setAddingRecipients] = useState(false);
  const [savingContent, setSavingContent] = useState(false);
  const [savingPlan, setSavingPlan] = useState(false);
  const [contentStale, setContentStale] = useState(false);
  const [resumeRetryExhausted, setResumeRetryExhausted] = useState(false);
  const [secondsToRefresh, setSecondsToRefresh] = useState(5);
  const pollTimerRef = useRef(null);
  const countdownRef = useRef(null);
  const actionRefreshTimerRef = useRef(null);
  const planRequestInFlightRef = useRef(false);
  const isPageVisibleRef = useRef(typeof document === 'undefined' ? true : document.visibilityState !== 'hidden');

  const loadPlan = useCallback(async () => {
    if (planRequestInFlightRef.current) return;
    planRequestInFlightRef.current = true;
    setLoading(true);
    try {
      const planData = await getWarmupPlan(id);
      setPlan(planData);
    } catch (error) {
      message.error(getErrorMessage(error, '加载养号详情失败'));
    } finally {
      planRequestInFlightRef.current = false;
      setLoading(false);
    }
  }, [id]);

  useEffect(() => {
    loadPlan();
    return () => {
      if (pollTimerRef.current) clearInterval(pollTimerRef.current);
      if (countdownRef.current) clearInterval(countdownRef.current);
      if (actionRefreshTimerRef.current) window.clearTimeout(actionRefreshTimerRef.current);
    };
  }, [loadPlan]);

  const shouldPollPlan = Boolean(plan?.running) || plan?.status === 'active';

  useEffect(() => {
    const handleVisibility = () => {
      isPageVisibleRef.current = document.visibilityState !== 'hidden';
      if (isPageVisibleRef.current && shouldPollPlan) loadPlan();
    };
    document.addEventListener('visibilitychange', handleVisibility);
    return () => document.removeEventListener('visibilitychange', handleVisibility);
  }, [loadPlan, shouldPollPlan]);

  useEffect(() => {
    if (location.state?.openPlanEditor && plan && !plan.running && !planModalOpen) {
      openPlanEditor();
      navigate(location.pathname, { replace: true, state: {} });
    }
  }, [location.pathname, location.state, navigate, plan, planModalOpen]);

  useEffect(() => {
    if (pollTimerRef.current) clearInterval(pollTimerRef.current);
    if (countdownRef.current) clearInterval(countdownRef.current);
    if (!plan || !shouldPollPlan) return undefined;

    setSecondsToRefresh(5);
    pollTimerRef.current = setInterval(() => {
      if (!isPageVisibleRef.current) return;
      loadPlan();
      setSecondsToRefresh(5);
    }, 5000);
    countdownRef.current = setInterval(() => {
      setSecondsToRefresh((value) => (value <= 1 ? 5 : value - 1));
    }, 1000);

    return () => {
      if (pollTimerRef.current) clearInterval(pollTimerRef.current);
      if (countdownRef.current) clearInterval(countdownRef.current);
    };
  }, [plan?.id, plan?.running, plan?.status, shouldPollPlan, loadPlan]);

  const runAction = async (action, successText, { useShutdownRetry = false } = {}) => {
    setActing(true);
    if (useShutdownRetry) setResumeRetryExhausted(false);
    try {
      const result = useShutdownRetry
        ? await runWithShutdownRetry(action)
        : await action();
      message.success(result?.message || successText);
      await loadPlan();
      if (actionRefreshTimerRef.current) window.clearTimeout(actionRefreshTimerRef.current);
      actionRefreshTimerRef.current = window.setTimeout(loadPlan, 1500);
    } catch (error) {
      if (useShutdownRetry && isShutdownConflict409(error)) {
        setResumeRetryExhausted(true);
        message.error('任务仍在收尾中，请点击下方刷新后重试');
      } else {
        message.error(getErrorMessage(error, successText.replace('已', '失败：')));
      }
    } finally {
      setActing(false);
    }
  };

  const parseRecipientText = (text) => {
    const seen = new Set();
    return text.split(/[\n,;\s]+/).map((item) => item.trim().toLowerCase()).filter((email) => {
      if (!email || seen.has(email)) return false;
      seen.add(email);
      return true;
    });
  };

  const openContentEditor = () => {
    contentForm.setFieldsValue({
      subject: plan?.content?.subject || '',
      html_body: plan?.content?.html_body || '',
      text_body: plan?.content?.text_body || '',
    });
    setContentModalOpen(true);
  };

  const openPlanEditor = () => {
    planForm.setFieldsValue({
      name: plan?.name || '',
      start_count: plan?.start_count,
      total_days: Number(plan?.total_days || 0),
      daily_increase_rate: plan?.daily_increase_rate,
      max_daily_count: plan?.max_daily_count,
      base_interval_seconds: plan?.base_interval_seconds,
      interval_jitter_seconds: plan?.interval_jitter_seconds,
      scheduled_time: plan?.scheduled_time ? dayjs(plan.scheduled_time, 'HH:mm:ss') : null,
      timezone: plan?.timezone || 'Asia/Shanghai',
      prompt: plan?.prompt || '',
      content_language: plan?.content_language || 'zh',
      tone: plan?.tone || '自然日常',
      must_include: plan?.must_include || '',
      avoid: plan?.avoid || '',
      fidelity_mode: plan?.fidelity_mode || 'strict',
    });
    setPlanModalOpen(true);
  };

  const submitPlan = async () => {
    try {
      const values = await planForm.validateFields();
      const scheduledTime = dayjs.isDayjs(values.scheduled_time) ? values.scheduled_time : dayjs(values.scheduled_time, 'HH:mm:ss');
      const aiParamsBefore = buildAiParamsSnapshot(plan);
      const aiParamsAfter = buildAiParamsSnapshot({
        ...plan,
        prompt: (values.prompt || '').trim(),
        content_language: values.content_language,
        tone: values.tone,
        must_include: (values.must_include || '').trim(),
        avoid: (values.avoid || '').trim(),
        fidelity_mode: values.fidelity_mode,
      });
      setSavingPlan(true);
      const updated = await updatePlan(id, {
        name: values.name.trim(),
        start_count: Number(values.start_count),
        total_days: Number(values.total_days || 0),
        daily_increase_rate: Number(values.daily_increase_rate),
        max_daily_count: Number(values.max_daily_count),
        base_interval_seconds: Number(values.base_interval_seconds),
        interval_jitter_seconds: Number(values.interval_jitter_seconds),
        scheduled_time: scheduledTime.format('HH:mm:ss'),
        timezone: values.timezone,
        prompt: (values.prompt || '').trim(),
        content_language: values.content_language,
        tone: values.tone,
        must_include: (values.must_include || '').trim(),
        avoid: (values.avoid || '').trim(),
        fidelity_mode: values.fidelity_mode,
      });
      setPlan(updated);
      setPlanModalOpen(false);
      const saveHint = updated.run_message
        || (updated.status === 'paused' ? '计划配置已保存，仍处于暂停状态' : '计划配置已保存');
      message.success(saveHint);
      const shouldPromptRegenerate = plan?.content_mode !== 'manual'
        && (updated.content_stale || aiGenerationParamsChanged(aiParamsBefore, aiParamsAfter));
      if (shouldPromptRegenerate) {
        setContentStale(true);
        Modal.confirm({
          title: '文案风格参数已修改',
          content: '文案风格参数已修改，是否立即重新生成文案？',
          okText: '立即重新生成',
          cancelText: '稍后手动生成',
          onOk: async () => {
            try {
              await regenerateWarmupContent(id, {
                prompt: aiParamsAfter.prompt,
                content_language: aiParamsAfter.content_language,
                tone: aiParamsAfter.tone,
                must_include: aiParamsAfter.must_include,
                avoid: aiParamsAfter.avoid,
                fidelity_mode: aiParamsAfter.fidelity_mode,
              });
              message.success('AI 文案已重新生成');
              setContentStale(false);
              await loadPlan();
            } catch (error) {
              message.error(getErrorMessage(error, '重新生成文案失败'));
            }
          },
        });
      }
      await loadPlan();
    } catch (error) {
      if (!error.errorFields) message.error(getErrorMessage(error, '更新养号计划失败'));
    } finally {
      setSavingPlan(false);
    }
  };

  const submitContent = async () => {
    try {
      const values = await contentForm.validateFields();
      const payload = {
        subject: values.subject.trim(),
        html_body: (values.html_body || '').trim(),
        text_body: (values.text_body || '').trim(),
      };
      if (!payload.html_body && !payload.text_body) {
        message.error('HTML 正文和纯文本正文至少填写一个');
        return;
      }
      setSavingContent(true);
      await updatePlanContent(id, payload);
      setContentModalOpen(false);
      message.success('文案已更新，下次发送将使用新文案');
      await loadPlan();
    } catch (error) {
      if (!error.errorFields) message.error(getErrorMessage(error, '更新养号文案失败'));
    } finally {
      setSavingContent(false);
    }
  };

  const submitAddRecipients = async () => {
    const emails = parseRecipientText(recipientText);
    if (!emails.length) {
      message.error('请输入至少一个邮箱，一行一个或用逗号分隔');
      return;
    }
    setAddingRecipients(true);
    try {
      const result = await addWarmupRecipients(id, emails);
      setPlan(result.plan);
      setAddModalOpen(false);
      setRecipientText('');
      const nextHint = result.plan?.status === 'paused'
        ? '计划仍处于暂停状态，确认无误后可点击“恢复发送”。'
        : result.plan?.status === 'scheduled'
          ? '计划仍处于等待定时状态，可手动启动或等待每日自动调度。'
          : result.plan?.status === 'active'
            ? '可点击“立即启动今日发送”，或等待每日自动调度。'
            : '新邮箱已加入后续发送队列。';
      message.success(`${result.message}。${nextHint}`, 8);
      await loadPlan();
    } catch (error) {
      message.error(getErrorMessage(error, '添加养号邮箱失败'));
    } finally {
      setAddingRecipients(false);
    }
  };

  const buildRegeneratePayload = () => {
    let savedParams = {};
    try {
      savedParams = JSON.parse(window.localStorage.getItem(`warmup_ai_params_${id}`) || '{}');
    } catch (error) {
      savedParams = {};
    }
    return {
      prompt: plan?.prompt || savedParams.prompt || '',
      content_language: plan?.content_language || savedParams.content_language || plan?.language || 'zh',
      tone: plan?.tone || savedParams.tone || '自然日常',
      must_include: plan?.must_include || savedParams.must_include || '',
      avoid: plan?.avoid || savedParams.avoid || '',
      fidelity_mode: plan?.fidelity_mode || savedParams.fidelity_mode || 'strict',
    };
  };

  const progressValue = typeof plan?.progress === 'number' ? Math.max(0, Math.min(100, plan.progress)) : 0;
  const contentText = useMemo(() => plan?.content?.text_body || '', [plan]);
  const planProxy = useMemo(() => getPlanProxy(plan), [plan]);
  const displayStatus = useMemo(() => getWarmupDisplayStatus(plan), [plan]);
  const canEdit = Boolean(plan && !plan.running);
  const isPaused = plan?.status === 'paused';
  const startLabel = isPaused ? '恢复发送' : '立即启动今日发送';
  const recipientUnsent = Number(plan?.recipient_unsent || 0);
  const dailyTarget = estimateWarmupDailyTarget(plan);
  const resumeBatchCap = Number(plan?.remaining_today ?? Math.min(recipientUnsent, dailyTarget || recipientUnsent));
  const showResumeQuotaAlert = isPaused && !plan?.running && recipientUnsent > 0 && dailyTarget > 0 && recipientUnsent > dailyTarget;
  const todayProgress = useMemo(() => getWarmupTodayProgress(plan), [plan]);
  const lifetimeProgress = useMemo(() => getWarmupLifetimeProgress(plan), [plan]);

  return <Space direction="vertical" size="large" className="mm-page">
    <Breadcrumb items={[
      { title: <Link to="/warmup">养号计划</Link> },
      { title: plan?.name || '计划详情' },
    ]} />
    <div style={{ display: 'flex', justifyContent: 'space-between', gap: 16, flexWrap: 'wrap' }}>
      <div>
        <Title level={2} style={{ margin: 0 }}>养号计划详情</Title>
        <Text type="secondary">页面每 5 秒自动轮询一次计划详情，实时同步进度、状态和累计结果。</Text>
      </div>
      <Space wrap>
        <Link to="/warmup"><Button icon={<ArrowLeftOutlined />}>返回列表</Button></Link>
        <Button icon={<ReloadOutlined />} onClick={loadPlan} loading={loading}>刷新</Button>
          <Button onClick={() => navigate('/logs', { state: { defaultLogType: 'warmup', planId: Number(id) } })}>查看养号日志</Button>
        <Button icon={<EditOutlined />} disabled={!canEdit} onClick={openPlanEditor}>编辑计划</Button>
        <Button icon={<PlusOutlined />} disabled={!canEdit} onClick={() => setAddModalOpen(true)}>添加邮箱</Button>
        <Space direction="vertical" size={8} align="start">
          <Button type="primary" loading={acting} disabled={!plan || plan.running || Number(plan.recipient_unsent || 0) <= 0} onClick={() => runAction(() => startWarmupPlan(id), isPaused ? '养号计划已恢复发送' : '养号计划已启动', { useShutdownRetry: isPaused })}>{startLabel}</Button>
          {showResumeQuotaAlert ? <Alert
            type="warning"
            showIcon
            style={{ maxWidth: 480 }}
            message={`恢复后本次最多发送今日剩余配额（约 ${resumeBatchCap} 封）`}
            description="如需全部发送，可增加每日上限或等待次日自动发送。"
          /> : null}
        </Space>
        <Button loading={acting} disabled={!plan || plan.status === 'paused' || plan.status === 'completed'} onClick={() => runAction(() => pauseWarmupPlan(id), '养号计划已暂停')}>暂停</Button>
        {plan?.content_mode !== 'manual' && <Button loading={acting} disabled={!canEdit} onClick={() => runAction(async () => {
          const result = await regenerateWarmupContent(id, buildRegeneratePayload());
          setContentStale(false);
          return result;
        }, 'AI 纯文本文案已重新生成')}>重新生成文案</Button>}
      </Space>
    </div>

    {resumeRetryExhausted ? <Alert
      type="warning"
      showIcon
      message="恢复发送未成功"
      description="养号任务可能仍在收尾中，请稍等片刻后点击刷新，再尝试恢复。"
      action={<Button size="small" onClick={() => { setResumeRetryExhausted(false); loadPlan(); }}>刷新</Button>}
    /> : null}

    {isPaused && !plan?.running ? <Alert type="warning" showIcon message="计划已暂停，可继续编辑" description="你可以修改计划配置、文案和邮箱列表；保存后计划仍保持暂停。若修改每日启动时间，系统只会等待新的每日定时时间自动执行，不会立即恢复发送。点击“恢复发送”将从剩余未发送邮箱继续，不会重复发送已完成地址。" /> : null}

    <Alert
      type={plan?.running ? 'success' : 'info'}
      showIcon
      message={plan?.running ? '计划发送中，页面正在轮询更新' : shouldPollPlan ? `当前状态：${displayStatus.label}` : '计划已停止，已暂停自动轮询'}
      description={shouldPollPlan ? `轮询间隔 5 秒，当前倒计时 ${secondsToRefresh}s` : '仅在运行中或启用状态下自动刷新'}
    />

    <Card loading={loading} bordered={false}>
      {plan ? <Space direction="vertical" size="middle" style={{ width: '100%' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 16, flexWrap: 'wrap' }}>
          <Space wrap align="center">
            <Tag color={displayStatus.tagColor}>{displayStatus.label}</Tag>
            {isPaused && !plan?.running ? <Text type="secondary">计划已暂停，您可以编辑配置后重新启动</Text> : null}
            <Text type="secondary">{plan.last_message || '暂无运行消息'}</Text>
          </Space>
          <Text type="secondary">{plan.running ? `下一次刷新倒计时 ${secondsToRefresh}s` : '当前停止刷新'}</Text>
        </div>
        <Card size="small" bordered={false} style={{ background: '#fafafa' }}>
          <Space direction="vertical" size={12} style={{ width: '100%' }}>
            <div>
              <Text strong>本轮/今日进度</Text>
              <Progress percent={progressValue || todayProgress.percent} status={plan.running ? 'active' : 'normal'} />
              <Text type="secondary">
                已处理 {todayProgress.processedToday}/{todayProgress.dailyTarget || dailyTarget || 0}，剩余今日配额 {todayProgress.remainingToday}，成功 {Number(plan?.success_count || 0)}，失败 {Number(plan?.fail_count || 0)}
              </Text>
            </div>
            <div>
              <Text strong>累计完成进度</Text>
              <Progress percent={lifetimeProgress.percent} size="small" />
              <Text type="secondary">
                已发送 {lifetimeProgress.sentRecipients}/{lifetimeProgress.totalRecipients}，未发送 {Number(plan?.recipient_unsent || 0)}
              </Text>
            </div>
          </Space>
        </Card>
        <Descriptions bordered column={2}>
          <Descriptions.Item label="名称">{plan.name}</Descriptions.Item>
          <Descriptions.Item label="状态">
            <Space>
              <Tag color={displayStatus.tagColor}>{displayStatus.label}</Tag>
              {contentStale || plan.content_stale ? <Tag color="warning">文案可能已过时</Tag> : null}
            </Space>
          </Descriptions.Item>
          <Descriptions.Item label="通道">{plan.channel_name}</Descriptions.Item>
          <Descriptions.Item label="代理">{planProxy ? `${planProxy.name}（${planProxy.address}）` : '直连'}</Descriptions.Item>
          <Descriptions.Item label="每日自动启动">{formatWarmupSchedule(plan)}</Descriptions.Item>
          <Descriptions.Item label="计划时区">{timezoneLabels[plan.timezone] || plan.timezone || '北京时间'}（{plan.timezone || 'Asia/Shanghai'}）</Descriptions.Item>
          <Descriptions.Item label="起始/递增/上限">{plan.start_count} / {plan.daily_increase_rate} / {plan.max_daily_count}</Descriptions.Item>
          <Descriptions.Item label="发送天数">总计 {Number(plan.total_days || 0) > 0 ? `${plan.total_days} 天` : '不限'}；已运行 {Number(plan.elapsed_days || 0)} 天；剩余 {plan.remaining_days === null || plan.remaining_days === undefined ? '不限' : `${plan.remaining_days} 天`}</Descriptions.Item>
          <Descriptions.Item label="间隔/抖动">{plan.base_interval_seconds}s / ±{plan.interval_jitter_seconds}s</Descriptions.Item>
          <Descriptions.Item label="今日发送进度">已处理 {todayProgress.processedToday}/{todayProgress.dailyTarget || (dailyTarget || (plan.daily_target ?? plan.target_today) || 0)}，剩余今日配额 {plan.remaining_today ?? resumeBatchCap}，成功 {plan.success_count}，失败 {plan.fail_count}</Descriptions.Item>
          <Descriptions.Item label="累计收件人统计">总人数 {plan.recipient_total}，已发送 {plan.recipient_sent}，未发送 {plan.recipient_unsent}</Descriptions.Item>
          <Descriptions.Item label="上次执行">{plan.last_run_date || '未执行'}</Descriptions.Item>
          <Descriptions.Item label="提示词" span={2}><Paragraph>{plan.prompt}</Paragraph></Descriptions.Item>
        </Descriptions>
      </Space> : <Text type="secondary">暂无数据</Text>}
    </Card>

    <Card
      title={<Space>固定养号文案 {contentStale || plan?.content_stale ? <Tag color="warning">文案可能已过时，建议重新生成</Tag> : null}</Space>}
      bordered={false}
      extra={<Button disabled={!canEdit} onClick={openContentEditor}>编辑文案</Button>}
    >
      {plan?.content ? <Space direction="vertical" style={{ width: '100%' }}>
        <Text strong>主题：{plan.content.subject}</Text>
        {plan.content.html_body ? <div><Text strong>HTML 正文</Text><Paragraph copyable style={{ whiteSpace: 'pre-wrap' }}>{plan.content.html_body}</Paragraph></div> : null}
        {contentText ? <div><Text strong>纯文本正文</Text><Paragraph copyable style={{ whiteSpace: 'pre-wrap' }}>{contentText}</Paragraph></div> : null}
      </Space> : <Text type="secondary">暂无文案，可点击“编辑文案”创建关联文案。</Text>}
    </Card>

    <Modal
      title="编辑养号文案"
      open={contentModalOpen}
      onCancel={() => setContentModalOpen(false)}
      onOk={submitContent}
      confirmLoading={savingContent}
      okText="保存文案"
      cancelText="取消"
      destroyOnHidden
      width={820}
    >
      <Form form={contentForm} layout="vertical">
        <Form.Item name="subject" label="邮件主题" rules={[{ required: true, message: '请输入邮件主题' }]}><Input maxLength={500} /></Form.Item>
        <Form.Item name="html_body" label="HTML 正文"><TextArea rows={10} wrap="soft" style={{ whiteSpace: 'pre-wrap', overflowWrap: 'anywhere', wordBreak: 'break-word' }} placeholder="可选，填写 HTML 邮件正文" /></Form.Item>
        <Form.Item name="text_body" label="纯文本正文"><TextArea rows={8} placeholder="可选，填写纯文本正文；HTML 正文和纯文本正文至少填写一个" /></Form.Item>
      </Form>
    </Modal>

    <Modal
      title="编辑养号计划"
      open={planModalOpen}
      onCancel={() => setPlanModalOpen(false)}
      onOk={submitPlan}
      confirmLoading={savingPlan}
      okText="保存配置"
      cancelText="取消"
      destroyOnHidden
      width={860}
    >
      <Form form={planForm} layout="vertical">
        <Form.Item name="name" label="计划名称" rules={[{ required: true, message: '请输入计划名称' }]}><Input maxLength={160} /></Form.Item>
        <Space wrap align="start">
          <Form.Item name="start_count" label="起始量" rules={[{ required: true }]}><InputNumber min={1} max={100000} /></Form.Item>
          <Form.Item name="total_days" label="总发送天数" tooltip="填写 0 表示不限制天数" rules={[{ required: true, message: '请输入总发送天数' }]}><InputNumber min={0} max={3650} precision={0} addonAfter="天" /></Form.Item>
          <Form.Item name="daily_increase_rate" label="每日递增比例" rules={[{ required: true }]}><InputNumber min={0} max={10} step={0.01} /></Form.Item>
          <Form.Item name="max_daily_count" label="最大日发送量" rules={[{ required: true }]}><InputNumber min={1} max={1000000} /></Form.Item>
          <Form.Item name="base_interval_seconds" label="基础间隔（秒）" rules={[{ required: true }]}><InputNumber min={1} max={86400} /></Form.Item>
          <Form.Item name="interval_jitter_seconds" label="随机抖动（秒）" rules={[{ required: true }]}><InputNumber min={0} max={86400} /></Form.Item>
          <Form.Item name="scheduled_time" label="每日启动时间" rules={[{ required: true, message: '请选择启动时间' }]}><TimePicker format="HH:mm:ss" /></Form.Item>
          <Form.Item name="timezone" label="时区" rules={[{ required: true }]}><Input style={{ width: 220 }} list="warmup-timezone-options" /></Form.Item>
        </Space>
        <datalist id="warmup-timezone-options">
          {TIMEZONE_OPTIONS.map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}
        </datalist>
        {plan?.content_mode !== 'manual' ? <>
          <Form.Item name="content_language" label="文案语言" rules={[{ required: true }]}><Input /></Form.Item>
          <Form.Item name="tone" label="文案风格" rules={[{ required: true }]}><Input /></Form.Item>
          <Form.Item name="fidelity_mode" label="忠实度" rules={[{ required: true }]}><Input /></Form.Item>
          <Form.Item name="must_include" label="必须包含"><TextArea rows={2} /></Form.Item>
          <Form.Item name="avoid" label="必须避免"><TextArea rows={2} /></Form.Item>
          <Form.Item name="prompt" label="专属提示词" rules={[{ required: true, message: '请输入提示词' }]}><TextArea rows={5} /></Form.Item>
        </> : null}
      </Form>
    </Modal>

    <Modal
      title="添加养号邮箱"
      open={addModalOpen}
      onCancel={() => setAddModalOpen(false)}
      onOk={submitAddRecipients}
      confirmLoading={addingRecipients}
      okText="添加"
      cancelText="取消"
      destroyOnHidden
      width={680}
    >
      <Space direction="vertical" style={{ width: '100%' }}>
        <Alert type="info" showIcon message="支持随时追加邮箱" description="系统会按当前计划独立去重：已发送和未发送过的同计划邮箱都会跳过，新邮箱会以未发送状态加入队列。已完成计划追加成功后会重新激活，可手动启动或等待自动调度。" />
        <TextArea rows={10} value={recipientText} onChange={(event) => setRecipientText(event.target.value)} placeholder="new1@example.com\nnew2@example.com" />
      </Space>
    </Modal>

    <div style={{ display: 'flex', justifyContent: 'flex-start' }}>
      <Link to="/warmup"><Button icon={<ArrowLeftOutlined />}>返回养号列表</Button></Link>
    </div>
  </Space>;
}
