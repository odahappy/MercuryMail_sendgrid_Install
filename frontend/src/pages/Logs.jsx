import { getErrorMessage } from '../api/index.js';
import { useAbortController } from '../hooks/useAbortController.js';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useLocation } from 'react-router-dom';
import { Button, Card, DatePicker, Form, Input, Modal, Select, Space, Table, Tag, message } from 'antd';
import { DeleteOutlined, ReloadOutlined, SearchOutlined } from '@ant-design/icons';
import { clearLogs, getLogs, getWarmupLogs } from '../api/logs.js';
import PageHeader from '../components/PageHeader.jsx';
import { getTasks } from '../api/tasks.js';
import { getWarmupPlans } from '../api/warmup.js';
import {
  resolveSdkSendStatusLabel,
  sdkSendStatusColor,
} from '../utils/sendStatusLabels.js';

const STATUS_OPTIONS_MONITOR = [
  { value: 'sent', label: '发送成功' },
  { value: 'failed', label: '发送失败' },
];

const STATUS_OPTIONS_WARMUP = [
  { value: 'sent', label: '发送成功' },
  { value: 'failed', label: '发送失败' },
];

const STATUS_OPTIONS_SIMPLE_SEND = [
  { value: 'sent', label: '发送成功' },
  { value: 'failed', label: '发送失败' },
];

const { RangePicker } = DatePicker;
const CLEAR_LOG_OPTIONS = [
  { type: 'all', label: '全部', buttonLabel: '清空全部' },
  { type: 'regular', label: '普通发送', buttonLabel: '清空普通' },
  { type: 'monitor', label: '监控邮箱', buttonLabel: '清空监控' },
  { type: 'warmup', label: '养号', buttonLabel: '清空养号' },
];

function renderRegularLogFilterSelect() {
  return (
    <Select allowClear placeholder="全部状态">
      <Select.OptGroup label="SDK 发送状态">
        <Select.Option value="">全部</Select.Option>
        <Select.Option value="status:sent">发送成功</Select.Option>
        <Select.Option value="status:failed">发送失败</Select.Option>
      </Select.OptGroup>
    </Select>
  );
}

function formatDate(value) {
  return value ? new Date(value).toLocaleString('zh-CN') : '—';
}

function formatMonitorTriggerLabel(count) {
  const normalized = Number(count || 0);
  if (!Number.isFinite(normalized) || normalized <= 0) return '—';
  return `回测 - ${normalized}封`;
}

function isMonitorLog(item) {
  return item?.log_type === 'monitor' || item?.status === 'monitor_sent' || item?.status === 'monitor_failed';
}

function logTypeTag(item) {
  if (isMonitorLog(item)) {
    return <Tag color="blue">监控邮件</Tag>;
  }
  if (item?.log_type === 'warmup') {
    return <Tag color="cyan">养号邮件</Tag>;
  }
  return <Tag color="green">普通邮件</Tag>;
}

function parseLogFilterValue(filterValue) {
  const normalized = String(filterValue || '').trim();
  if (!normalized) {
    return { status: '' };
  }
  const separatorIndex = normalized.indexOf(':');
  if (separatorIndex <= 0) {
    return { status: '' };
  }
  const kind = normalized.slice(0, separatorIndex);
  const rawValue = normalized.slice(separatorIndex + 1);
  if (kind === 'status') {
    return { status: rawValue };
  }
  return { status: '' };
}

function normalizeRecordedEgress(value) {
  const rawValue = String(value || '').trim();
  if (!rawValue || rawValue === '直连') return '';
  const withoutProtocol = rawValue.replace(/^[a-z][a-z0-9+.-]*:\/\//i, '');
  const withoutAuth = withoutProtocol.includes('@') ? withoutProtocol.split('@').pop() : withoutProtocol;
  const withoutPath = String(withoutAuth || '').split('/')[0].trim();
  const ipv4WithOptionalPort = withoutPath.match(/^(\d{1,3}(?:\.\d{1,3}){3})(?::\d{1,5})?$/);
  if (ipv4WithOptionalPort) return ipv4WithOptionalPort[1];
  return withoutPath;
}

function formatProxyEgress(item) {
  const outboundIp = normalizeRecordedEgress(item?.outbound_ip);
  const egressIp = normalizeRecordedEgress(item?.egress_ip);
  const proxyInfo = String(item?.proxy_info || '').trim();
  const proxyLabel = proxyInfo && proxyInfo !== '直连' ? proxyInfo : '';
  const primaryIp = outboundIp || egressIp;
  if (primaryIp && proxyLabel && primaryIp !== normalizeRecordedEgress(proxyLabel)) return `${primaryIp}（${proxyLabel}）`;
  if (primaryIp) return primaryIp;
  return proxyLabel || '—';
}

export default function Logs() {
  const location = useLocation();
  const [form] = Form.useForm();
  const [tasks, setTasks] = useState([]);
  const [warmupPlans, setWarmupPlans] = useState([]);
  const [logs, setLogs] = useState({ items: [], total: 0, page: 1, page_size: 20 });
  const [selectedLogType, setSelectedLogType] = useState('regular');
  const [filters, setFilters] = useState({
    task_id: '',
    plan_id: '',
    email: '',
    status: '',
    level: '',
    log_kind: '',
    log_type: 'regular',
    start_time: '',
    end_time: '',
  });
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [tasksLoading, setTasksLoading] = useState(false);
  const [warmupPlansLoading, setWarmupPlansLoading] = useState(false);
  const [loading, setLoading] = useState(false);
  const [clearingType, setClearingType] = useState('');
  const [pollingGeneration, setPollingGeneration] = useState(0);
  const { createAbortController, releaseAbortController, cleanupAllControllers } = useAbortController();
  const tasksRequestInFlightRef = useRef(false);
  const warmupPlansRequestInFlightRef = useRef(false);
  const logsRequestInFlightRef = useRef(false);
  const isPageVisibleRef = useRef(typeof document === 'undefined' ? true : document.visibilityState !== 'hidden');
  const isAllSendLogView = selectedLogType === 'all';
  const isMonitorView = selectedLogType === 'monitor';
  const isWarmupView = selectedLogType === 'warmup';
  const isWarmupStyleLogView = isWarmupView || selectedLogType === 'regular';
  const locationPlanId = Number(location.state?.planId || 0);
  const locationDefaultLogType = String(location.state?.defaultLogType || '').trim();

  const loadTasks = useCallback(async () => {
    if (tasksRequestInFlightRef.current) return;
    tasksRequestInFlightRef.current = true;
    const controller = createAbortController();
    setTasksLoading(true);
    try {
      const data = await getTasks(1, 100, '', { signal: controller.signal });
      setTasks(data.items || []);
    } catch (error) {
      if (error?.code === 'ERR_CANCELED') return;
      message.error(getErrorMessage(error, '任务列表加载失败'));
    } finally {
      tasksRequestInFlightRef.current = false;
      releaseAbortController(controller);
      setTasksLoading(false);
    }
  }, [createAbortController, releaseAbortController]);

  const loadWarmupPlans = useCallback(async () => {
    if (warmupPlansRequestInFlightRef.current) return;
    warmupPlansRequestInFlightRef.current = true;
    const controller = createAbortController();
    setWarmupPlansLoading(true);
    try {
      const data = await getWarmupPlans(1, 100, { signal: controller.signal });
      setWarmupPlans(data.items || []);
    } catch (error) {
      if (error?.code === 'ERR_CANCELED') return;
      message.error(getErrorMessage(error, '养号计划列表加载失败'));
    } finally {
      warmupPlansRequestInFlightRef.current = false;
      releaseAbortController(controller);
      setWarmupPlansLoading(false);
    }
  }, [createAbortController, releaseAbortController]);

  const loadLogs = useCallback(async () => {
    if (logsRequestInFlightRef.current) return;
    logsRequestInFlightRef.current = true;
    const controller = createAbortController();
    setLoading(true);
    try {
      const params = { page, page_size: pageSize };
      if (isWarmupView) {
        if (filters.plan_id) {
          const planId = Number(filters.plan_id);
          if (Number.isInteger(planId) && planId > 0) {
            params.plan_id = planId;
          }
        }
        if (filters.email?.trim()) params.email = filters.email.trim();
        if (filters.status) params.status = filters.status;
        if (filters.start_time) params.start_time = filters.start_time;
        if (filters.end_time) params.end_time = filters.end_time;
        const data = await getWarmupLogs(params, { signal: controller.signal });
        setLogs(data);
        return;
      }
      params.log_type = filters.log_type || selectedLogType || 'all';
      if (filters.task_id) {
        const taskId = Number(filters.task_id);
        if (Number.isInteger(taskId) && taskId > 0) {
          params.task_id = taskId;
        }
      }
      if (filters.email?.trim()) params.email = filters.email.trim();
      if (filters.status) params.status = filters.status;
      if (filters.start_time) params.start_time = filters.start_time;
      if (filters.end_time) params.end_time = filters.end_time;
      const data = await getLogs(params, { signal: controller.signal });
      setLogs(data);
    } catch (error) {
      if (error?.code === 'ERR_CANCELED') return;
      message.error(getErrorMessage(error, '日志加载失败'));
    } finally {
      logsRequestInFlightRef.current = false;
      releaseAbortController(controller);
      setLoading(false);
    }
  }, [filters, selectedLogType, page, pageSize, createAbortController, releaseAbortController, isWarmupView]);

  useEffect(() => {
    loadTasks();
  }, [loadTasks]);

  useEffect(() => {
    loadWarmupPlans();
  }, [loadWarmupPlans]);

  useEffect(() => {
    const nextLogType = ['all', 'regular', 'monitor', 'warmup'].includes(locationDefaultLogType)
      ? locationDefaultLogType
      : '';
    if (!nextLogType) return;
    setSelectedLogType(nextLogType);
    const nextPlanId = nextLogType === 'warmup' && locationPlanId > 0 ? locationPlanId : '';
    form.setFieldsValue({
      plan_id: nextPlanId || undefined,
      task_id: undefined,
      log_filter: undefined,
      level: undefined,
      log_kind: undefined,
    });
    setFilters((prev) => ({
      ...prev,
      task_id: '',
      plan_id: nextPlanId,
      status: '',
      level: '',
      log_kind: '',
      log_type: nextLogType,
    }));
    setPage(1);
  }, [form, locationDefaultLogType, locationPlanId]);

  useEffect(() => {
    loadLogs();
  }, [loadLogs]);

  useEffect(() => {
    const handleVisibility = () => {
      isPageVisibleRef.current = document.visibilityState !== 'hidden';
    };
    document.addEventListener('visibilitychange', handleVisibility);
    return () => document.removeEventListener('visibilitychange', handleVisibility);
  }, []);

  useEffect(() => {
    if (isWarmupView || clearingType) return undefined;
    const timer = window.setInterval(() => {
      if (isPageVisibleRef.current) loadLogs();
    }, 15000);
    return () => window.clearInterval(timer);
  }, [isWarmupView, clearingType, pollingGeneration, loadLogs]);

  const handleLogTypeChange = (nextType) => {
    const normalizedType = nextType === 'monitor' ? 'monitor' : nextType === 'warmup' ? 'warmup' : nextType === 'regular' ? 'regular' : 'all';
    setSelectedLogType(normalizedType);
    form.setFieldValue('log_filter', undefined);
    setFilters((prev) => ({
      ...prev,
      log_type: normalizedType,
      plan_id: '',
      status: '',
      level: '',
      log_kind: '',
    }));
    setPage(1);
  };

  const handleSearch = (values) => {
    const range = values.time_range || [];
    const simpleStatusFilter = String(values.log_filter || '').trim();
    const simpleSendStatus = (isMonitorView || isWarmupStyleLogView) && ['sent', 'failed'].includes(simpleStatusFilter)
      ? simpleStatusFilter
      : '';
    const { status: regularStatus } = parseLogFilterValue(
      isMonitorView || isWarmupStyleLogView ? '' : values.log_filter,
    );
    setFilters({
      task_id: values.task_id || '',
      plan_id: values.plan_id || '',
      email: values.email || '',
      status: simpleSendStatus || regularStatus,
      level: values.level || '',
      log_kind: values.log_kind || '',
      log_type: selectedLogType,
      start_time: range[0] ? range[0].format('YYYY-MM-DDTHH:mm:ss') : '',
      end_time: range[1] ? range[1].format('YYYY-MM-DDTHH:mm:ss') : '',
    });
    setPage(1);
  };

  const resetFilters = () => {
    form.resetFields();
    setFilters({
      task_id: '',
      plan_id: '',
      email: '',
      status: '',
      level: '',
      log_kind: '',
      log_type: selectedLogType,
      start_time: '',
      end_time: '',
    });
    setPage(1);
  };

  const executeClearLogs = async (option) => {
    cleanupAllControllers();
    logsRequestInFlightRef.current = false;
    setClearingType(option.type);
    try {
      const result = await clearLogs(option.type);
      message.success(result.message || '日志已清空');
      setLogs({ items: [], total: 0, page: 1, page_size: pageSize });
      setPage(1);
      setPollingGeneration((value) => value + 1);
      window.setTimeout(() => loadLogs(), 0);
    } catch (error) {
      message.error(getErrorMessage(error, `清空${option.label}日志失败`));
    } finally {
      setClearingType('');
    }
  };

  const confirmClearLogs = (option) => {
    Modal.confirm({
      title: `确认清空${option.label}日志？`,
      content: `确定要清空所有${option.label}日志吗？此操作不可恢复。`,
      okText: '确认清空',
      cancelText: '取消',
      okButtonProps: { danger: true },
      onOk: () => executeClearLogs(option),
    });
  };

  const columns = useMemo(() => {
    if (isWarmupStyleLogView) {
      const baseColumns = [
        { title: '发送时间', dataIndex: 'created_at', key: 'created_at', width: 180, render: formatDate },
        { title: isWarmupView ? '任务/计划' : '任务名称', dataIndex: 'task_name', key: 'task_name', width: 180, render: (_, item) => item.task_name || (item.task_id ? `#${item.task_id}` : '养号发送') },
        { title: '邮箱', dataIndex: 'email', key: 'email', width: 240 },
        {
          title: '发件人',
          dataIndex: 'sender_email',
          key: 'sender_email',
          width: 220,
          render: (value) => value || '—',
        },
        {
          title: 'SDK 发送状态',
          dataIndex: 'status',
          key: 'sdk_status',
          width: 130,
          render: (value) => <Tag color={sdkSendStatusColor(value)}>{resolveSdkSendStatusLabel(value)}</Tag>,
        },
        {
          title: '代理出口',
          key: 'proxy_egress',
          width: 180,
          render: (_, item) => formatProxyEgress(item),
        },
        {
          title: 'SDK 错误 / 响应',
          key: 'message',
          ellipsis: true,
          render: (_, item) => item.error_message || item.response_text || item.event_reason || '—',
        },
      ];
      if (isMonitorView) {
        baseColumns.splice(3, 0, {
          title: '触发点',
          dataIndex: 'monitor_trigger_count',
          key: 'monitor_trigger_count',
          width: 130,
          render: formatMonitorTriggerLabel,
        });
      }
      return baseColumns;
    }

    return [
      { title: '发送时间', dataIndex: 'created_at', key: 'created_at', width: 180, render: formatDate },
      { title: '任务名称', dataIndex: 'task_name', key: 'task_name', width: 160, render: (_, item) => item.task_name || (item.task_id ? `#${item.task_id}` : '养号发送') },
      { title: '日志类型', key: 'log_type', width: 110, render: (_, item) => logTypeTag(item) },
      { title: '邮箱', dataIndex: 'email', key: 'email', width: 220 },
      {
        title: '发件人',
        dataIndex: 'sender_email',
        key: 'sender_email',
        width: 200,
        render: (value) => value || '—',
      },
      {
        title: 'SDK 发送状态',
        dataIndex: 'status',
        key: 'sdk_status',
        width: 120,
        render: (value) => <Tag color={sdkSendStatusColor(value)}>{resolveSdkSendStatusLabel(value)}</Tag>,
      },
      {
        title: '代理出口',
        key: 'proxy_egress',
        width: 160,
        render: (_, item) => formatProxyEgress(item),
      },
      {
        title: 'SDK 错误 / 响应',
        key: 'message',
        ellipsis: true,
        render: (_, item) => item.error_message || item.response_text || '—',
      },
    ];
  }, [isWarmupStyleLogView, isWarmupView, isMonitorView]);

  const pageTitle = isWarmupView ? '养号日志' : isMonitorView ? '监控邮箱日志' : isAllSendLogView ? '全部发送日志' : '普通发送日志';
  const pageDescription = isWarmupView
    ? '查看养号计划发送记录，可按计划、邮箱、状态和时间范围筛选。'
    : isMonitorView
      ? '查看监控抽样回测邮件记录，可按任务、监控邮箱、状态和时间范围筛选。'
      : isAllSendLogView
        ? '查看普通任务、监控抽样与养号发送记录；可切换到单独视图进行精确筛选。'
        : '查看普通群发发送记录，显示方式与养号日志一致，可按任务、邮箱、状态和时间范围筛选。';

  return (
    <Space direction="vertical" size="large" className="mm-page">
      <PageHeader
        title={pageTitle}
        description={pageDescription}
        extra={<Button icon={<ReloadOutlined />} onClick={loadLogs} loading={loading}>刷新</Button>}
      />

      <Space>
        <Button
          type={selectedLogType === 'all' ? 'primary' : 'default'}
          onClick={() => handleLogTypeChange('all')}
        >
          全部日志
        </Button>
        <Button
          type={selectedLogType === 'regular' ? 'primary' : 'default'}
          onClick={() => handleLogTypeChange('regular')}
        >
          普通发送日志
        </Button>
        <Button
          type={selectedLogType === 'monitor' ? 'primary' : 'default'}
          onClick={() => handleLogTypeChange('monitor')}
        >
          监控邮箱日志
        </Button>
        <Button
          type={selectedLogType === 'warmup' ? 'primary' : 'default'}
          onClick={() => handleLogTypeChange('warmup')}
        >
          养号日志
        </Button>
      </Space>

      <Card bordered={false}>
        <Form form={form} layout="vertical" onFinish={handleSearch}>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 16 }}>
            {isWarmupView ? (
              <Form.Item name="plan_id" label="养号计划">
                <Select allowClear loading={warmupPlansLoading} placeholder="全部养号计划" options={warmupPlans.map((plan) => ({ value: plan.id, label: plan.name }))} />
              </Form.Item>
            ) : (
              <Form.Item name="task_id" label="任务">
                <Select allowClear loading={tasksLoading} placeholder="全部任务" options={tasks.map((task) => ({ value: task.id, label: task.name }))} />
              </Form.Item>
            )}
            <Form.Item name="email" label={isMonitorView ? '监控邮箱' : '邮箱'}>
              <Input allowClear placeholder="user@example.com" />
            </Form.Item>
            <Form.Item
              name="log_filter"
              label="状态筛选"
            >
              {isWarmupView ? (
                <Select allowClear placeholder="全部状态" options={STATUS_OPTIONS_WARMUP} />
              ) : isMonitorView ? (
                <Select allowClear placeholder="全部状态" options={STATUS_OPTIONS_MONITOR} />
              ) : selectedLogType === 'regular' ? (
                <Select allowClear placeholder="全部状态" options={STATUS_OPTIONS_SIMPLE_SEND} />
              ) : (
                renderRegularLogFilterSelect()
              )}
            </Form.Item>
            <Form.Item name="time_range" label="时间范围">
              <RangePicker showTime style={{ width: '100%' }} />
            </Form.Item>
          </div>
          <Space>
            <Button type="primary" htmlType="submit" icon={<SearchOutlined />} loading={loading}>搜索</Button>
            <Button onClick={resetFilters} disabled={loading || Boolean(clearingType)}>重置筛选</Button>
          </Space>
        </Form>
      </Card>

      <Card
        bordered={false}
        title="日志列表"
        extra={(
          <Space wrap>
            {CLEAR_LOG_OPTIONS.map((option) => (
              <Button
                key={option.type}
                danger
                size="small"
                icon={<DeleteOutlined />}
                loading={clearingType === option.type}
                disabled={Boolean(clearingType) && clearingType !== option.type}
                onClick={() => confirmClearLogs(option)}
              >
                {option.buttonLabel}
              </Button>
            ))}
          </Space>
        )}
        styles={{ body: { padding: 0 } }}
      >
        <Table
          rowKey="id"
          bordered
          loading={loading}
          columns={columns}
          dataSource={logs.items || []}
          locale={{ emptyText: '暂无日志' }}
          pagination={{
            current: page,
            pageSize,
            total: logs.total >= 0 ? logs.total : Math.max(page * pageSize + pageSize, pageSize),
            showSizeChanger: true,
            showTotal: (value) => logs.total >= 0 ? `共 ${value} 条` : '深分页模式：已跳过总数统计以保持流畅',
            onChange: (nextPage, nextPageSize) => {
              setPage(nextPage);
              setPageSize(nextPageSize);
            },
          }}
        />
      </Card>
    </Space>
  );
}
