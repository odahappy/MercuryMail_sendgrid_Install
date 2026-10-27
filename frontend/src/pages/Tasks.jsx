import { getErrorMessage } from '../api/index.js';
import { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Link, useSearchParams } from 'react-router-dom';
import { Alert, Button, Card, Collapse, DatePicker, Form, Input, InputNumber, Modal, Popconfirm, Row, Col, Select, Space, Switch, Table, Tag, Tooltip, Typography, message } from 'antd';
import { PlusOutlined, ReloadOutlined, ThunderboltOutlined } from '@ant-design/icons';
import { listChannels, measureChannelProxyLatency } from '../api/channels.js';
import { getLists as getRecipientLists } from '../api/recipients.js';
import { batchPauseTasks, batchResumeTasks, batchStartTasks, batchStopTasks, createTask, deleteTask, getTasks, resumeTask, startTask } from '../api/tasks.js';
import { normalizeTaskIds } from '../api/export.js';
import PageHeader from '../components/PageHeader.jsx';
import { isShutdownConflict409, runWithShutdownRetry } from '../utils/shutdownRetry.js';
import { getTemplates } from '../api/templates.js';
import { getPools } from '../api/urls.js';
import { formatProxyLatencySummary, getCachedProxyLatency, setCachedProxyLatency, getConcurrencyRecommendation, isConcurrencyBelowRecommendation, concurrencyWarnInputStyle, buildOptimalConfigAdvice, formatOptimalConfigSummary, SENDGRID_FREE_DAILY_EMAIL_HINT, SENDGRID_HTTP_RPS_SOFT_LIMIT, DEFAULT_HTTP_BATCH_SIZE, RECOMMENDED_CONCURRENCY_FORMULA } from '../utils/proxyLatency.js';
import { TASK_STATUS_COLORS, resolveTaskStatusLabel } from '../utils/taskDetailFormat.js';

const { Text } = Typography;
const TASK_CREATE_DEFAULTS = {
  name: '',
  template_id: undefined,
  recipient_list_id: undefined,
  channel_id: undefined,
  url_pool_id: undefined,
  concurrency: 1,
  auto_concurrency: false,
  sender_rotation_enabled: false,
  unsubscribe_enabled: false,
  send_http_batch_size: DEFAULT_HTTP_BATCH_SIZE,
  rate_limit: 0,
  retry_count: 3,
  monitor_emails: [''],
  monitor_interval: 100,
  scheduled_at: null,
  timezone: 'Asia/Shanghai'
};
const emptyForm = { ...TASK_CREATE_DEFAULTS };
const timezoneOptions = [
  { value: 'Asia/Shanghai', label: '北京时间（Asia/Shanghai）' },
  { value: 'Asia/Tokyo', label: '东京时间（Asia/Tokyo）' }
];
const STARTABLE_STATUSES = new Set(['draft', 'scheduled', 'pending', 'paused', 'stopped', 'failed']);

function getBulkStartDisabledReason(task) {
  const status = String(task?.status || '').toLowerCase();
  if (!STARTABLE_STATUSES.has(status)) return '当前状态不支持批量启动';
  return '';
}

function getBulkActionDisabledReason(task, action) {
  const status = String(task?.status || '').toLowerCase();
  const allowed = action === 'pause' ? status === 'running' : ['running', 'pending', 'paused'].includes(status);
  return allowed ? '' : `任务状态不允许${action === 'pause' ? '暂停' : '停止'}`;
}

function recipientGroupOptionLabel(item) {
  const importing = ['queued', 'processing', 'importing'].includes(String(item.status || '').toLowerCase());
  const suffix = importing ? '，导入中' : '';
  return `${item.name}（有效 ${item.valid_count ?? item.total_count} / 总数 ${item.total_count}${suffix}）`;
}

function isRecipientListSelectable(item) {
  const status = String(item?.status || '').toLowerCase();
  if (['queued', 'processing', 'importing', 'failed'].includes(status)) return false;
  return Number(item?.valid_count ?? item?.total_count ?? 0) > 0;
}

function channelSupportsSenderRotation(channel) {
  const senders = Array.isArray(channel?.sender_emails) ? channel.sender_emails : [];
  const activeSenders = senders.filter((item) => item?.enabled !== false && String(item?.email || '').trim());
  return activeSenders.length > 1;
}

const tablePageSizeOptions = [10, 20, 50];
const TASK_ACTION_DEBOUNCE_MS = 1000;
const TASKS_REFRESH_MS = 3000;
const TASKS_HIDDEN_REFRESH_MS = 30000;
const RUNNING_STATUSES = new Set(['running', 'preparing', 'scheduled']);

const TaskNameCell = memo(function TaskNameCell({ task }) {
  return <Link to={`/tasks/${task.id}`}><Text strong>{task.name}</Text></Link>;
});

const TaskResourcesCell = memo(function TaskResourcesCell({ task }) {
  return (
    <Space direction="vertical" size={0}>
      <Text>模板：{task.template_name}</Text>
      <Text>收件人分组：{task.recipient_list_name}</Text>
      <Text>通道：{task.channel_name}</Text>
      <Text>URL 池：{task.url_pool_name || '未绑定'}</Text>
      <Text>退订：{task.unsubscribe_enabled ? '开启' : '关闭'}</Text>
      <Text>定时启动：{task.scheduled_at ? `${task.scheduled_at.replace('T', ' ').slice(0, 16)}（${task.timezone || 'Asia/Shanghai'}）` : '未设置'}</Text>
    </Space>
  );
});

const TaskStatusCell = memo(function TaskStatusCell({ task }) {
  const status = String(task?.status || '');
  const pauseReason = status === 'paused' ? String(task?.run_message || task?.status_reason || '').trim() : '';
  const statusTag = <Tag color={TASK_STATUS_COLORS[status] || 'default'}>{resolveTaskStatusLabel(task)}</Tag>;
  return pauseReason ? <Tooltip title={pauseReason}><Space direction="vertical" size={0}>{statusTag}<Text type="danger" ellipsis style={{ maxWidth: 220 }}>{pauseReason}</Text></Space></Tooltip> : statusTag;
});

const TaskProgressCell = memo(function TaskProgressCell({ task }) {
  return <Text>成功 {task.sent_count} / 失败 {task.failed_count} / 总数 {task.total_count}</Text>;
});

const TaskActionsCell = memo(function TaskActionsCell({ task, actingId, onStart, onResume, onDelete }) {
  const isActing = actingId === task.id;
  const showResume = task.status === 'paused' || task.status === 'failed' || (task.status === 'stopped' && Number(task.sent_count || 0) > 0);
  const showStart = STARTABLE_STATUSES.has(task.status) && !(task.status === 'stopped' && Number(task.sent_count || 0) > 0);
  return (
    <Space>
      <Link to={`/tasks/${task.id}`}><Button size="small">详情</Button></Link>
      {showResume ? <Button size="small" type="primary" loading={isActing} disabled={Boolean(actingId)} onClick={() => onResume(task)}>继续</Button> : null}
      {showStart ? <Button size="small" type="primary" loading={isActing} disabled={Boolean(actingId)} onClick={() => onStart(task)}>启动</Button> : null}
      <Popconfirm title="确认删除发送任务？" description="删除后任务将从列表隐藏，历史发送日志会保留用于统计、审计和导出。" okText="删除" cancelText="取消" onConfirm={() => onDelete(task)}>
        <Button size="small" danger loading={actingId === task.id} disabled={Boolean(actingId)}>删除</Button>
      </Popconfirm>
    </Space>
  );
});

export default function Tasks() {
  const [searchParams] = useSearchParams();
  const statusFilter = searchParams.get('status') || '';
  const [tasks, setTasks] = useState([]);
  const [templates, setTemplates] = useState([]);
  const [recipientLists, setRecipientLists] = useState([]);
  const [channels, setChannels] = useState([]);
  const [urlPools, setUrlPools] = useState([]);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [modalOpen, setModalOpen] = useState(false);
  const [actingId, setActingId] = useState(0);
  const [bulkAction, setBulkAction] = useState('');
  const [selectedTaskIds, setSelectedTaskIds] = useState([]);
  const [deletingId, setDeletingId] = useState(0);
  const [latencyLoading, setLatencyLoading] = useState(false);
  const [latencyResult, setLatencyResult] = useState(null);
  const [form] = Form.useForm();
  const actionCooldownUntilRef = useRef(0);
  const senderRotationChannelRef = useRef(null);
  const tasksRequestInFlightRef = useRef(false);
  const optionsRequestInFlightRef = useRef(false);
  const isPageVisibleRef = useRef(typeof document === 'undefined' ? true : document.visibilityState !== 'hidden');
  const [isPageVisible, setIsPageVisible] = useState(
    typeof document === 'undefined' ? true : document.visibilityState !== 'hidden',
  );
  const totalPages = useMemo(() => Math.max(1, Math.ceil(total / pageSize)), [total, pageSize]);

  const guardTaskAction = useCallback(() => {
    const now = Date.now();
    if (now < actionCooldownUntilRef.current) return false;
    actionCooldownUntilRef.current = now + TASK_ACTION_DEBOUNCE_MS;
    return true;
  }, []);

  const templateOptions = useMemo(() => templates.map((item) => ({ value: item.id, label: item.name })), [templates]);
  const recipientListOptions = useMemo(() => recipientLists.map((item) => ({
    value: item.id,
    label: recipientGroupOptionLabel(item),
    disabled: !isRecipientListSelectable(item),
  })), [recipientLists]);
  const channelOptions = useMemo(() => channels.map((item) => {
    const unauthorized = String(item.health_status || '').toLowerCase() === 'unauthorized';
    const name = item.name || `通道 #${item.id}`;
    return {
      value: item.id,
      label: unauthorized ? `${name}（API Key 已失效）` : name,
      disabled: unauthorized,
    };
  }), [channels]);
  const urlPoolOptions = useMemo(() => urlPools.map((item) => ({ value: item.id, label: `${item.name}（${item.url_count || 0} 条）` })), [urlPools]);
  const selectedChannelId = Form.useWatch('channel_id', form);
  const selectedChannel = useMemo(
    () => channels.find((item) => Number(item.id) === Number(selectedChannelId)) || null,
    [channels, selectedChannelId],
  );
  const selectedChannelSenderCount = useMemo(() => {
    const senders = Array.isArray(selectedChannel?.sender_emails) ? selectedChannel.sender_emails : [];
    return senders.filter((item) => item?.enabled !== false && String(item?.email || '').trim()).length;
  }, [selectedChannel]);
  const senderRotationAvailable = channelSupportsSenderRotation(selectedChannel);

  const loadTasks = useCallback(async (silent = false) => {
    if (tasksRequestInFlightRef.current) return;
    tasksRequestInFlightRef.current = true;
    if (!silent) setLoading(true);
    try {
      const data = await getTasks(page, pageSize, statusFilter, silent ? { skipNetworkRetry: true } : {});
      setTasks(data.items || []);
      setTotal(data.total || 0);
    } catch (error) {
      if (!silent) message.error(getErrorMessage(error, '加载任务失败'));
    } finally {
      tasksRequestInFlightRef.current = false;
      if (!silent) setLoading(false);
    }
  }, [page, pageSize, statusFilter]);

  const loadOptions = useCallback(async () => {
    if (optionsRequestInFlightRef.current) return;
    optionsRequestInFlightRef.current = true;
    try {
      const [templateData, listData, channelData, urlPoolData] = await Promise.all([getTemplates(1, 100), getRecipientLists(), listChannels({ page: 1, page_size: 100 }), getPools(1, 100)]);
      setTemplates(templateData.items || []);
      setRecipientLists(listData || []);
      setChannels(channelData.items || []);
      setUrlPools(urlPoolData.items || []);
    } catch (error) {
      message.error(getErrorMessage(error, '加载创建任务选项失败'));
    } finally {
      optionsRequestInFlightRef.current = false;
    }
  }, []);

  useEffect(() => { loadTasks(); }, [loadTasks]);
  useEffect(() => { loadOptions(); }, [loadOptions]);

  useEffect(() => {
    const handleVisibilityChange = () => {
      const visible = document.visibilityState !== 'hidden';
      isPageVisibleRef.current = visible;
      setIsPageVisible(visible);
      if (visible) loadTasks(true);
    };
    document.addEventListener('visibilitychange', handleVisibilityChange);
    return () => document.removeEventListener('visibilitychange', handleVisibilityChange);
  }, [loadTasks]);

  const hasActiveTasksOnPage = tasks.some((task) => RUNNING_STATUSES.has(String(task?.status || '').toLowerCase()));

  useEffect(() => {
    const intervalMs = isPageVisible && hasActiveTasksOnPage ? TASKS_REFRESH_MS : TASKS_HIDDEN_REFRESH_MS;
    const timer = window.setInterval(() => {
      if (!isPageVisibleRef.current && intervalMs <= TASKS_REFRESH_MS) return;
      loadTasks(true);
    }, intervalMs);
    return () => window.clearInterval(timer);
  }, [hasActiveTasksOnPage, isPageVisible, loadTasks]);

  const openCreate = () => {
    senderRotationChannelRef.current = null;
    form.setFieldsValue(emptyForm);
    setLatencyResult(null);
    setModalOpen(true);
  };

  useEffect(() => {
    if (!modalOpen) return;
    const normalizedChannelId = selectedChannelId ? Number(selectedChannelId) : null;
    const channelChanged = senderRotationChannelRef.current !== normalizedChannelId;
    senderRotationChannelRef.current = normalizedChannelId;
    if (!normalizedChannelId || !senderRotationAvailable) {
      if (form.getFieldValue('sender_rotation_enabled')) {
        form.setFieldsValue({ sender_rotation_enabled: false });
      }
      return;
    }
    if (channelChanged) {
      form.setFieldsValue({ sender_rotation_enabled: true });
    }
  }, [form, modalOpen, senderRotationAvailable, selectedChannelId]);

  const runProxyLatencyCheck = async () => {
    if (latencyLoading) return;
    const channelId = Number(form.getFieldValue('channel_id'));
    const rateLimit = Number(form.getFieldValue('rate_limit'));
    if (!channelId) {
      message.warning('请先选择发送通道');
      return;
    }
    if (!Number.isFinite(rateLimit) || rateLimit <= 0) {
      message.warning('请先填写大于 0 的速率限制（封/秒）');
      return;
    }
    const batchSize = Number(form.getFieldValue('send_http_batch_size')) || DEFAULT_HTTP_BATCH_SIZE;
    const applyOptimalFields = (result) => {
      if (!result?.success) {
        return;
      }
      const advice = buildOptimalConfigAdvice(rateLimit, result.latency_ms, batchSize);
      const patch = {};
      if (result.recommended_concurrency) {
        patch.concurrency = result.recommended_concurrency;
      }
      if (advice?.recommendedBatchSize) {
        patch.send_http_batch_size = advice.recommendedBatchSize;
      }
      if (Object.keys(patch).length > 0) {
        form.setFieldsValue(patch);
      }
    };
    const cached = getCachedProxyLatency(channelId, rateLimit, batchSize);
    if (cached) {
      setLatencyResult(cached);
      applyOptimalFields(cached);
      message.info(`使用缓存结果：${formatProxyLatencySummary(cached)}`);
      return;
    }
    setLatencyLoading(true);
    try {
      const result = await measureChannelProxyLatency(channelId, rateLimit, batchSize);
      setLatencyResult(result);
      setCachedProxyLatency(channelId, rateLimit, result, batchSize);
      applyOptimalFields(result);
      if (result.success) {
        message.success(formatProxyLatencySummary(result));
      } else {
        message.error(result.message || '代理延迟检测失败');
      }
    } catch (error) {
      message.error(getErrorMessage(error, '代理延迟检测失败'));
    } finally {
      setLatencyLoading(false);
    }
  };

  const submitTask = async () => {
    if (saving) return;
    try {
      const values = await form.validateFields();
      setSaving(true);
      const mergedValues = { ...TASK_CREATE_DEFAULTS, ...values };
      const monitorEmails = (mergedValues.monitor_emails || [])
        .map((item) => String(item || '').trim())
        .filter(Boolean);
      const scheduledAt = mergedValues.scheduled_at && typeof mergedValues.scheduled_at.format === 'function'
        ? mergedValues.scheduled_at.format('YYYY-MM-DDTHH:mm:ss')
        : null;
      const payload = {
        name: String(mergedValues.name || '').trim(),
        template_id: Number(mergedValues.template_id),
        recipient_list_id: Number(mergedValues.recipient_list_id),
        channel_id: Number(mergedValues.channel_id),
        url_pool_id: mergedValues.url_pool_id ? Number(mergedValues.url_pool_id) : null,
        concurrency: Number(mergedValues.concurrency ?? TASK_CREATE_DEFAULTS.concurrency),
        auto_concurrency: Boolean(mergedValues.auto_concurrency),
        sender_rotation_enabled: senderRotationAvailable && Boolean(mergedValues.sender_rotation_enabled),
        unsubscribe_enabled: Boolean(mergedValues.unsubscribe_enabled),
        send_http_batch_size: Number(mergedValues.send_http_batch_size ?? DEFAULT_HTTP_BATCH_SIZE),
        rate_limit: Number(mergedValues.rate_limit ?? TASK_CREATE_DEFAULTS.rate_limit),
        retry_count: Number(mergedValues.retry_count ?? TASK_CREATE_DEFAULTS.retry_count),
        monitor_emails: monitorEmails,
        monitor_interval: Number(mergedValues.monitor_interval ?? TASK_CREATE_DEFAULTS.monitor_interval),
        scheduled_at: scheduledAt,
        timezone: mergedValues.timezone || TASK_CREATE_DEFAULTS.timezone
      };
      const numericChecks = [
        ['批量大小', payload.send_http_batch_size, 1, 1000],
        ['发送速率', payload.rate_limit, 0, 100000],
        ['并发数', payload.concurrency, 1, 1000],
        ['重试次数', payload.retry_count, 0, 10],
        ['监控间隔', payload.monitor_interval, 0, 1000000]
      ];
      const invalidNumeric = numericChecks.find(([, value, min, max]) => !Number.isFinite(value) || value < min || value > max);
      if (!payload.name || !payload.template_id || !payload.recipient_list_id || !payload.channel_id) {
        message.error('请先补全任务名称、模板、收件人池和发送通道');
        return;
      }
      if (invalidNumeric) {
        const [label, , min, max] = invalidNumeric;
        message.error(`${label}必须是 ${min} 到 ${max} 范围内的数字`);
        return;
      }
      const created = await createTask(payload);
      message.success('任务已创建');
      if (created?.concurrency_warning) {
        message.warning(created.concurrency_warning, 8);
      }
      setModalOpen(false);
      form.resetFields();
      await loadTasks();
    } catch (error) {
      if (!error.errorFields) message.error(getErrorMessage(error, '创建任务失败'));
    } finally {
      setSaving(false);
    }
  };

  const runStart = useCallback(async (task) => {
    if (actingId || !guardTaskAction()) return;
    setActingId(task.id);
    try {
      await startTask(task.id);
      message.success('任务已启动');
      await loadTasks();
    } catch (error) {
      message.error(getErrorMessage(error, '启动任务失败'));
    } finally {
      setActingId(0);
    }
  }, [actingId, guardTaskAction, loadTasks]);

  const runResume = useCallback(async (task) => {
    if (actingId || !guardTaskAction()) return;
    setActingId(task.id);
    try {
      await runWithShutdownRetry(() => resumeTask(task.id));
      message.success('任务已继续发送');
      await loadTasks();
    } catch (error) {
      if (isShutdownConflict409(error)) {
        message.error('任务仍在收尾中，请稍后从详情页刷新后重试');
      } else {
        message.error(getErrorMessage(error, '继续发送失败'));
      }
    } finally {
      setActingId(0);
    }
  }, [actingId, guardTaskAction, loadTasks]);

  const runDelete = useCallback(async (task) => {
    if (deletingId) return;
    setDeletingId(task.id);
    try {
      await deleteTask(task.id);
      message.success('任务已删除');
      setSelectedTaskIds((ids) => ids.filter((id) => id !== task.id));
      await loadTasks();
    } catch (error) {
      message.error(getErrorMessage(error, '删除任务失败'));
    } finally {
      setDeletingId(0);
    }
  }, [deletingId, loadTasks]);

  const resumableSelectedTaskIds = useMemo(() => {
    const selectedIds = new Set(normalizeTaskIds(selectedTaskIds));
    return tasks
      .filter((task) => selectedIds.has(Number(task.id)) && ['paused', 'stopped', 'failed'].includes(String(task.status || '').toLowerCase()))
      .map((task) => Number(task.id));
  }, [selectedTaskIds, tasks]);

  const runBulkAction = useCallback((action) => {
    const normalizedIds = action === 'resume' ? resumableSelectedTaskIds : normalizeTaskIds(selectedTaskIds);
    if (!normalizedIds.length || bulkAction || !guardTaskAction()) return;
    const labels = { start: '启动', resume: '继续', pause: '暂停', stop: '停止' };
    const label = labels[action];
    Modal.confirm({
      title: `确定要${label}选中的 ${normalizedIds.length} 个${action === 'resume' ? '暂停' : ''}任务吗？`,
      content: '系统将逐个检查任务状态，非法状态会记录为跳过，不影响其他任务。',
      okText: `确认${label}`,
      cancelText: '取消',
      okButtonProps: { danger: action === 'pause' || action === 'stop' },
      onOk: async () => {
        setBulkAction(action);
        try {
          const actionRequest = { start: batchStartTasks, resume: batchResumeTasks, pause: batchPauseTasks, stop: batchStopTasks }[action];
          const result = await actionRequest(normalizedIds);
          const successCount = result.success?.length || 0;
          const failedItems = result.failed || [];
          const skippedItems = result.skipped || [];
          const summary = [`成功${label} ${successCount} 个`];
          if (failedItems.length) summary.push(`失败 ${failedItems.length} 个：${failedItems.map((item) => `#${item.task_id} ${item.reason}`).join('；')}`);
          if (skippedItems.length) summary.push(`跳过 ${skippedItems.length} 个：${skippedItems.map((item) => `#${item.task_id} ${item.reason}`).join('；')}`);
          if (failedItems.length || skippedItems.length) message.warning(summary.join('，'), 10);
          else message.success(summary[0]);
          setSelectedTaskIds([]);
          await loadTasks();
        } catch (error) {
          message.error(getErrorMessage(error, '服务繁忙，请稍后重试'));
        } finally {
          setBulkAction('');
        }
      },
    });
  }, [bulkAction, guardTaskAction, loadTasks, resumableSelectedTaskIds, selectedTaskIds]);

  const columns = useMemo(() => [
    { title: '名称', dataIndex: 'name', key: 'name', render: (_, task) => <TaskNameCell task={task} /> },
    { title: '资源', key: 'resources', render: (_, task) => <TaskResourcesCell task={task} /> },
    { title: '状态', dataIndex: 'status', key: 'status', width: 110, render: (_, task) => {
      const disabledReason = getBulkStartDisabledReason(task);
      const statusNode = <TaskStatusCell task={task} />;
      return disabledReason ? <Tooltip title={disabledReason}>{statusNode}</Tooltip> : statusNode;
    } },
    { title: '进度', key: 'progress', render: (_, task) => <TaskProgressCell task={task} /> },
    { title: '操作', key: 'actions', width: 240, render: (_, task) => <TaskActionsCell task={task} actingId={actingId || deletingId} onStart={runStart} onResume={runResume} onDelete={runDelete} /> }
  ], [actingId, deletingId, runStart, runResume, runDelete]);

  return <Space direction="vertical" size="large" className="mm-page">
    <PageHeader
      title="发送任务"
      description="不同通道/账号的多个任务可同时发送；同一通道自动独占，避免账号冲突。"
      extra={<Space><Button icon={<ReloadOutlined />} loading={loading} onClick={loadTasks}>刷新</Button><Button type="primary" icon={<PlusOutlined />} onClick={openCreate}>创建任务</Button></Space>}
    />
    <Card bordered={false} styles={{ body: { padding: 0 } }}><Table rowKey="id" bordered loading={loading} columns={columns} dataSource={tasks} rowSelection={{ selectedRowKeys: selectedTaskIds, onChange: setSelectedTaskIds, getCheckboxProps: (task) => ({ disabled: false, title: undefined }) }} title={() => <Space wrap><Button type="primary" loading={bulkAction === 'start'} disabled={!selectedTaskIds.length || Boolean(bulkAction) || Boolean(actingId) || Boolean(deletingId)} onClick={() => runBulkAction('start')}>批量启动</Button><Button type="primary" loading={bulkAction === 'resume'} disabled={!resumableSelectedTaskIds.length || Boolean(bulkAction) || Boolean(actingId) || Boolean(deletingId)} onClick={() => runBulkAction('resume')}>批量继续</Button><Button loading={bulkAction === 'pause'} disabled={!selectedTaskIds.length || Boolean(bulkAction) || Boolean(actingId) || Boolean(deletingId)} onClick={() => runBulkAction('pause')}>批量暂停</Button><Button danger loading={bulkAction === 'stop'} disabled={!selectedTaskIds.length || Boolean(bulkAction) || Boolean(actingId) || Boolean(deletingId)} onClick={() => runBulkAction('stop')}>批量停止</Button><Text type="secondary">已选择 {selectedTaskIds.length} 个任务，其中暂停 {pausedSelectedTaskIds.length} 个</Text></Space>} locale={{ emptyText: '暂无任务。' }} pagination={{ current: page, pageSize, total, showSizeChanger: true, pageSizeOptions: tablePageSizeOptions, showTotal: (value) => `共 ${value} 条，第 ${page} / ${totalPages} 页`, onChange: (p, ps) => { setPage(p); setPageSize(Math.min(ps, 50)); } }} /></Card>
    <Modal title="创建发送任务" open={modalOpen} onCancel={() => setModalOpen(false)} onOk={submitTask} confirmLoading={saving} okText="创建" cancelText="取消" destroyOnHidden width={920}>
      <Form form={form} layout="vertical" initialValues={emptyForm} disabled={saving}>
        <Card size="small" bordered={false} title="基本设置" style={{ marginBottom: 16 }}>
          <Row gutter={16}>
            <Col xs={24} md={12}><Form.Item name="name" label="任务名称" rules={[{ required: true, message: '请输入任务名称' }]}><Input /></Form.Item></Col>
            <Col xs={24} md={12}><Form.Item name="template_id" label="模板" rules={[{ required: true, message: '请选择模板' }]}><Select virtual showSearch optionFilterProp="label" options={templateOptions} /></Form.Item></Col>
            <Col xs={24} md={12}><Form.Item name="recipient_list_id" label="收件人池" rules={[{ required: true, message: '请选择收件人池' }]}><Select virtual showSearch optionFilterProp="label" placeholder="按分组名称选择要发送的收件人池" options={recipientListOptions} /></Form.Item></Col>
            <Col xs={24} md={12}>
              <Form.Item label="发送通道" required>
                <Space.Compact style={{ width: '100%' }}>
                  <Form.Item name="channel_id" noStyle rules={[{ required: true, message: '请选择发送通道' }]}>
                    <Select virtual showSearch optionFilterProp="label" options={channelOptions} style={{ width: 'calc(100% - 148px)' }} />
                  </Form.Item>
                  <Button icon={<ThunderboltOutlined />} loading={latencyLoading} onClick={runProxyLatencyCheck}>检测代理延迟</Button>
                </Space.Compact>
              </Form.Item>
            </Col>
            <Col xs={24} md={8}><Form.Item name="timezone" label="定时时区"><Select options={timezoneOptions} /></Form.Item></Col>
            <Col xs={24} md={16}><Form.Item name="scheduled_at" label="定时启动时间"><DatePicker showTime style={{ width: '100%' }} placeholder="不选择则手动启动" /></Form.Item></Col>
            <Col xs={24}><Form.Item name="url_pool_id" label="URL 池" tooltip="模板含 {{url}} 变量时，启动任务前必须选择 URL 池"><Select allowClear virtual showSearch optionFilterProp="label" placeholder="可选；模板含追踪链接时启动前必选" options={urlPoolOptions} /></Form.Item></Col>
          </Row>
        </Card>

        {latencyResult ? (
          <Alert
            type={latencyResult.success ? 'success' : 'error'}
            showIcon
            message={latencyResult.success ? '延迟检测完成' : '延迟检测失败'}
            description={formatProxyLatencySummary(latencyResult)}
            style={{ marginBottom: 16 }}
          />
        ) : null}

        <Card size="small" bordered={false} title="发送控制" style={{ marginBottom: 16 }}>
          <Form.Item noStyle shouldUpdate={(prev, cur) => prev.rate_limit !== cur.rate_limit || prev.concurrency !== cur.concurrency || prev.send_http_batch_size !== cur.send_http_batch_size || prev.channel_id !== cur.channel_id || prev.auto_concurrency !== cur.auto_concurrency}>
            {({ getFieldValue }) => {
              const rate = Number(getFieldValue('rate_limit') || 0);
              const channelId = getFieldValue('channel_id');
              const batchSize = Number(getFieldValue('send_http_batch_size')) || DEFAULT_HTTP_BATCH_SIZE;
              const latencyMs = latencyResult?.success ? latencyResult.latency_ms : getConcurrencyRecommendation(channelId, rate, batchSize).latencyMs;
              const advice = latencyResult?.success && rate > 0 ? buildOptimalConfigAdvice(rate, latencyMs, batchSize) : null;
              if (!advice) {
                return null;
              }
              const currentBatch = Number(getFieldValue('send_http_batch_size') || 0);
              const currentConcurrency = Number(getFieldValue('concurrency') || 0);
              const httpRpsText = Number.isInteger(advice.estimatedHttpRps) ? String(advice.estimatedHttpRps) : advice.estimatedHttpRps.toFixed(2);
              return (
                <Collapse
                  style={{ marginBottom: 16 }}
                  items={[{
                    key: 'optimal',
                    label: '最优配置建议（基于代理 RTT）',
                    children: (
                      <Space direction="vertical" style={{ width: '100%' }}>
                        <Text>{formatOptimalConfigSummary(advice, rate)}</Text>
                        <Text type="secondary">当前：批量大小 {currentBatch || '-'}，并发 {currentConcurrency || '-'}</Text>
                        {advice.exceedsHttpRpsSoftLimit ? (
                          <Alert type="warning" showIcon message="HTTP 请求频率偏高" description={`预计 ${httpRpsText} 次/秒，超过软上限 ${SENDGRID_HTTP_RPS_SOFT_LIMIT} 次/秒，可能触发 SendGrid 限流。`} />
                        ) : null}
                        {advice.exceedsFreeDailyHint ? (
                          <Alert type="info" showIcon message="免费套餐提示" description={`按当前速率持续 8 小时约 ${advice.sustainedEightHours} 封，参考值约 ${SENDGRID_FREE_DAILY_EMAIL_HINT} 封/天。`} />
                        ) : null}
                      </Space>
                    ),
                  }]}
                />
              );
            }}
          </Form.Item>

          <Row gutter={16}>
            <Col xs={24} md={8}>
              <Form.Item
                name="send_http_batch_size"
                label="批量大小"
                tooltip="SendGrid SDK 每 HTTP 请求携带的收件人数。"
                rules={[{ required: true, message: '请输入批量大小' }]}
              >
                <InputNumber min={1} max={1000} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col xs={24} md={8}>
              <Form.Item name="rate_limit" label="发送速率（封/秒）" tooltip="发送速率上限（封/秒）。启用 SDK 批量时，HTTP 请求频率约为 发送速率 ÷ 批量大小。" rules={[{ required: true, message: '请输入发送速率' }]}>
                <InputNumber min={0} max={100000} step={0.1} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col xs={24} md={8}>
              <Form.Item
                name="auto_concurrency"
                label="启用自动调整并发"
                valuePropName="checked"
                tooltip="开启后系统将根据网络延迟和实际发送速率在运行期动态优化并发数；关闭时使用下方手动并发数。"
              >
                <Switch checkedChildren="开" unCheckedChildren="关" />
              </Form.Item>
            </Col>
            <Col xs={24} md={8}>
              <Form.Item
                name="sender_rotation_enabled"
                label="启用发件人轮询"
                valuePropName="checked"
                tooltip="开启后按批次在通道多个启用发件人间轮换；关闭时始终使用第一个启用发件人（主发件人）。"
                extra={senderRotationAvailable ? `当前通道有 ${selectedChannelSenderCount} 个启用发件人，可按批次轮询。` : '当前通道少于 2 个启用发件人，无法启用轮询；请先在通道管理中添加并启用多个发件人。'}
              >
                <Switch checkedChildren="开" unCheckedChildren="关" disabled={!senderRotationAvailable} />
              </Form.Item>
            </Col>
            <Col xs={24} md={8}>
              <Form.Item
                name="unsubscribe_enabled"
                label="启用退订"
                valuePropName="checked"
                tooltip="开启后发送时注入 List-Unsubscribe 邮件头和 {{unsubscribe_url}} 变量；关闭时不注入退订链接。"
              >
                <Switch checkedChildren="开" unCheckedChildren="关" />
              </Form.Item>
            </Col>
          </Row>

          <Form.Item noStyle shouldUpdate={(prev, cur) => prev.concurrency !== cur.concurrency || prev.rate_limit !== cur.rate_limit || prev.channel_id !== cur.channel_id || prev.auto_concurrency !== cur.auto_concurrency}>
            {({ getFieldValue }) => {
              const rate = Number(getFieldValue('rate_limit') || 0);
              const concurrency = Number(getFieldValue('concurrency') || 1);
              const autoConcurrency = Boolean(getFieldValue('auto_concurrency'));
              const channelId = getFieldValue('channel_id');
              const batchSize = Number(getFieldValue('send_http_batch_size')) || DEFAULT_HTTP_BATCH_SIZE;
              const { recommended, threshold } = getConcurrencyRecommendation(channelId, rate, batchSize);
              const warn = !autoConcurrency && isConcurrencyBelowRecommendation(concurrency, recommended);
              return (
                <>
                  {autoConcurrency ? (
                    <Alert
                      type="info"
                      showIcon
                      style={{ marginBottom: 16 }}
                      message="已启用自动调整并发"
                      description="系统将根据网络延迟和速率自动优化并发数。"
                    />
                  ) : (
                    <Row gutter={16}>
                      <Col xs={24} md={12}>
                        <Form.Item
                          name="concurrency"
                          label="并发数"
                          tooltip={`SDK 批量模型：${RECOMMENDED_CONCURRENCY_FORMULA}；可先检测代理延迟自动填入`}
                          extra={recommended && rate > 0 ? `推荐并发 = ${RECOMMENDED_CONCURRENCY_FORMULA} → ${recommended}${warn ? `（当前值低于推荐值 80%，可能无法达到 ${rate} 封/秒）` : ''}` : `推荐并发公式：${RECOMMENDED_CONCURRENCY_FORMULA}`}
                          rules={[{ required: true, message: '请输入并发数' }]}
                        >
                          <InputNumber min={1} max={1000} style={{ width: '100%', ...(warn ? concurrencyWarnInputStyle : {}) }} />
                        </Form.Item>
                      </Col>
                    </Row>
                  )}
                  {warn ? (
                    <Alert
                      type="warning"
                      showIcon
                      style={{ marginTop: autoConcurrency ? 0 : -8, marginBottom: 0 }}
                      message="并发数可能不足以达到目标速率"
                      description={`当前并发 ${concurrency}，推荐 ${recommended}（80% 阈值为 ${threshold}；${RECOMMENDED_CONCURRENCY_FORMULA}，batch=${batchSize}）`}
                    />
                  ) : null}
                </>
              );
            }}
          </Form.Item>
        </Card>

        <Card size="small" bordered={false} title="监控与高级" style={{ marginBottom: 0 }}>
          <Row gutter={16}>
            <Col xs={24} md={8}><Form.Item name="retry_count" label="重试次数" rules={[{ required: true, message: '请输入重试次数' }]}><InputNumber min={0} max={10} style={{ width: '100%' }} /></Form.Item></Col>
            <Col xs={24} md={8}><Form.Item name="monitor_interval" label="监控间隔" tooltip="常规发送累计成功 N 封后，向每个监控邮箱各发送 1 封回测邮件；设为 0 即关闭监控抽样" rules={[{ required: true, message: '请输入监控间隔' }]}><InputNumber min={0} max={1000000} style={{ width: '100%' }} /></Form.Item></Col>
            <Col xs={24} md={8}>
              <Space direction="vertical" style={{ width: '100%' }} size={4}>
                <Text strong>监控邮箱</Text>
                <Text type="secondary">每次触发时，列表中的每个邮箱各收到 1 封回测邮件。</Text>
              </Space>
            </Col>
          </Row>
          <Form.List name="monitor_emails">
            {(fields, { add, remove }) => (
              <Space direction="vertical" style={{ width: '100%', marginTop: 8 }}>
                {fields.map((field) => (
                  <Space key={field.key} align="baseline" style={{ display: 'flex' }}>
                    <Form.Item {...field} rules={[{ type: 'email', message: '请输入有效邮箱地址' }]} style={{ marginBottom: 8 }}><Input placeholder="monitor@example.com" style={{ width: 360 }} /></Form.Item>
                    <Button onClick={() => remove(field.name)} disabled={fields.length <= 1}>删除</Button>
                  </Space>
                ))}
                <Button type="dashed" icon={<PlusOutlined />} onClick={() => add('')}>新增监控邮箱</Button>
              </Space>
            )}
          </Form.List>
        </Card>
      </Form>
    </Modal>
  </Space>;
}
