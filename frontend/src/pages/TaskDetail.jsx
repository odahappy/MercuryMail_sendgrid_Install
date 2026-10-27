import { readBlobErrorDetail } from '../api/export.js';
import { getErrorMessage } from '../api/index.js';
import { useAbortController } from '../hooks/useAbortController.js';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import {
  Alert,
  Button,
  Card,
  Col,
  Collapse,
  DatePicker,
  Empty,
  Form,
  Input,
  InputNumber,
  Modal,
  Row,
  Select,
  Space,
  Switch,
  Tabs,
  Tag,
  Typography,
  message,
} from 'antd';
import {
  ArrowLeftOutlined,
  DownloadOutlined,
  InfoCircleOutlined,
  PauseCircleOutlined,
  PlayCircleOutlined,
  PlusOutlined,
  ReloadOutlined,
  SettingOutlined,
  StopOutlined,
  ThunderboltOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';
import { measureChannelProxyLatency } from '../api/channels.js';
import FailuresTable from '../components/FailuresTable.jsx';
import TaskConfigSummary from '../components/TaskConfigSummary.jsx';
import TaskDiagnosticsDetail from '../components/TaskDiagnosticsDetail.jsx';
import TaskOverviewLeft from '../components/TaskOverviewLeft.jsx';
import TaskPerfGrid from '../components/TaskPerfGrid.jsx';
import TaskSmartAdviceBar from '../components/TaskSmartAdviceBar.jsx';
import {
  downloadBlob,
  exportFailedRecipients,
  exportSuccessRecipients,
  exportUnsetRecipients,
  getTask,
  getTaskAlert,
  getTaskPerf,
  getTaskStats,
  getTaskStatus,
  pauseTask,
  resumeTask,
  startTask,
  stopTask,
  updateTask,
} from '../api/tasks.js';
import { getPools } from '../api/urls.js';
import { isShutdownConflict409, runWithShutdownRetry } from '../utils/shutdownRetry.js';
import {
  TASK_STATUS_COLORS,
  resolveTaskStatusLabel,
  formatRate,
  isTaskRunning,
} from '../utils/taskDetailFormat.js';
import {
  DEFAULT_HTTP_BATCH_SIZE,
  RECOMMENDED_CONCURRENCY_FORMULA,
  SENDGRID_FREE_DAILY_EMAIL_HINT,
  SENDGRID_HTTP_RPS_SOFT_LIMIT,
  buildOptimalConfigAdvice,
  concurrencyWarnInputStyle,
  formatOptimalConfigSummary,
  formatProxyLatencySummary,
  getCachedProxyLatency,
  getConcurrencyRecommendation,
  isConcurrencyBelowRecommendation,
  setCachedProxyLatency,
} from '../utils/proxyLatency.js';
import './TaskDetail.css';

const { Text } = Typography;
const SUCCESS_EXPORT_TOOLTIP = '基于 SendLog 发送成功状态';
const FAILED_EXPORT_TOOLTIP = '基于 SendLog 发送失败状态';
const UNSET_EXPORT_TOOLTIP = '任务关联收件人中，尚未出现在本任务 send_log 的邮箱';
const TERMINAL_STATUSES = new Set(['finished', 'stopped', 'failed']);
const POLL_MS = 1000;
const HIDDEN_POLL_MS = 5000;
const TASK_REFRESH_MS = 15000;
const LOW_RATE_WINDOW_MS = 30000;
const CONTROL_DEBOUNCE_MS = 1000;

const timezoneOptions = [
  { value: 'Asia/Shanghai', label: '北京时间（Asia/Shanghai）' },
  { value: 'Asia/Tokyo', label: '东京时间（Asia/Tokyo）' },
];

function parseScheduledAt(value) {
  if (!value) return null;
  const parsed = dayjs(value);
  return parsed.isValid() ? parsed : null;
}

function parseTaskId(raw) {
  const n = Number(raw);
  if (!Number.isInteger(n) || n <= 0) {
    return null;
  }
  return n;
}

function resolveProgressStatusLabel(status, task) {
  if (task?.status_label) {
    if (String(task.status || '') === 'running') return '发送中';
    return task.status_label;
  }
  const normalized = String(status || '');
  if (normalized === 'running') return '发送中';
  return resolveTaskStatusLabel(normalized);
}

export default function TaskDetail() {
  const { id } = useParams();
  const taskId = useMemo(() => parseTaskId(id), [id]);
  const invalidTaskId = taskId === null;
  const pollTimerRef = useRef(null);
  const taskTimerRef = useRef(null);
  const rateHistoryRef = useRef([]);
  const previousRunningRef = useRef(false);
  const controlCooldownUntilRef = useRef(0);
  const taskRequestInFlightRef = useRef(false);
  const statsRequestInFlightRef = useRef(false);
  const perfRequestInFlightRef = useRef(false);
  const liveStatusRequestInFlightRef = useRef(false);
  const alertRequestInFlightRef = useRef(false);
  const liveStatusFailureCountRef = useRef(0);
  const isPageVisibleRef = useRef(typeof document === 'undefined' ? true : document.visibilityState !== 'hidden');

  const [task, setTask] = useState(null);
  const [stats, setStats] = useState(null);
  const [perf, setPerf] = useState(null);
  const [liveStatus, setLiveStatus] = useState(null);
  const [rateSamples, setRateSamples] = useState([]);
  const [activeTab, setActiveTab] = useState('overview');
  const [loading, setLoading] = useState(false);
  const [controlLoading, setControlLoading] = useState('');
  const [resumeRetryExhausted, setResumeRetryExhausted] = useState(false);
  const [taskNotFound, setTaskNotFound] = useState(false);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [configOpen, setConfigOpen] = useState(false);
  const [settingsSaving, setSettingsSaving] = useState(false);
  const [latencyLoading, setLatencyLoading] = useState(false);
  const [latencyResult, setLatencyResult] = useState(null);
  const [performanceAlert, setPerformanceAlert] = useState(null);
  const [liveStatusRetrying, setLiveStatusRetrying] = useState(false);
  const [exportingType, setExportingType] = useState('');
  const [exportProgressText, setExportProgressText] = useState('');
  const [urlPools, setUrlPools] = useState([]);
  const [isPageVisible, setIsPageVisible] = useState(
    typeof document === 'undefined' ? true : document.visibilityState !== 'hidden',
  );
  const [settingsForm] = Form.useForm();
  const { createAbortController, releaseAbortController } = useAbortController();

  const taskStatus = String(task?.status || 'draft');
  const configuredRateLimit = Number(task?.rate_limit || 0);
  const running = isTaskRunning(taskStatus);
  const terminalReason = String(task?.status_reason || task?.run_message || '').trim();
  const terminalError = String(task?.error_message || '').trim();

  const liveSuccessCount = Number(liveStatus?.live_success_count ?? liveStatus?.success_count ?? stats?.success_sdk ?? task?.sent_count ?? 0);
  const liveFailCount = Number(liveStatus?.live_fail_count ?? liveStatus?.fail_count ?? stats?.failed_sdk ?? task?.failed_count ?? 0);
  const totalRecipients = Number(liveStatus?.total_recipients || stats?.total_recipients || task?.total_count || 0);
  const successCount = liveSuccessCount;
  const failedCount = liveFailCount;
  const processedCount = Math.min(totalRecipients, successCount + failedCount);
  const remainingCount = Math.max(0, totalRecipients - processedCount);
  const ringPercent = totalRecipients > 0
    ? Math.min(100, Math.round(Number(liveStatus?.progress_percent ?? (processedCount / totalRecipients) * 100)))
    : 0;

  const showLowRateWarning = useMemo(() => {
    if (!running || configuredRateLimit <= 0) return false;
    const samples = rateHistoryRef.current;
    if (samples.length < 8) return false;
    const threshold = configuredRateLimit * 0.6;
    return samples.every((item) => item.rate < threshold);
  }, [running, configuredRateLimit, perf?.current_rate, rateSamples]);

  const loadTask = useCallback(async () => {
    if (taskId == null || taskRequestInFlightRef.current) return;
    taskRequestInFlightRef.current = true;
    const controller = createAbortController();
    setLoading(true);
    try {
      const nextTask = await getTask(taskId, { signal: controller.signal });
      setTask((currentTask) => ({ ...(currentTask || {}), ...(nextTask || {}) }));
      setTaskNotFound(false);
    } catch (error) {
      if (error?.code === 'ERR_CANCELED') return;
      if (error?.response?.status === 404) {
        setTask(null);
        setTaskNotFound(true);
      } else {
        message.error(getErrorMessage(error, '加载任务详情失败'));
      }
    } finally {
      taskRequestInFlightRef.current = false;
      releaseAbortController(controller);
      setLoading(false);
    }
  }, [taskId, createAbortController, releaseAbortController]);

  const loadStats = useCallback(async () => {
    if (taskId == null || statsRequestInFlightRef.current) return;
    statsRequestInFlightRef.current = true;
    const controller = createAbortController();
    try {
      const nextStats = await getTaskStats(taskId, { signal: controller.signal });
      setStats((currentStats) => ({ ...(currentStats || {}), ...(nextStats || {}) }));
    } catch (error) {
      if (error?.code === 'ERR_CANCELED') return;
    } finally {
      statsRequestInFlightRef.current = false;
      releaseAbortController(controller);
    }
  }, [taskId, createAbortController, releaseAbortController]);

  const loadPerf = useCallback(async () => {
    if (taskId == null || perfRequestInFlightRef.current) return;
    perfRequestInFlightRef.current = true;
    const controller = createAbortController();
    try {
      const nextPerf = await getTaskPerf(taskId, { signal: controller.signal });
      setPerf((currentPerf) => ({
        ...(currentPerf || {}),
        ...(nextPerf || {}),
      }));
      const rawRate = Number(nextPerf?.current_rate ?? nextPerf?.sdk_current_rate_10s);
      const currentRate = Number.isFinite(rawRate) ? rawRate : 0;
      if (import.meta.env.DEV || String(import.meta.env.VITE_PERF_DEBUG || '').toLowerCase() === 'true') {
        console.info(
          `任务 ${taskId} 速率API: current_rate=${currentRate}封/秒 success_rate=${Number(nextPerf?.success_rate ?? 0)}封/秒 window=${Number(nextPerf?.rate_window_seconds ?? 10)}s success=${Number(nextPerf?.rate_window_success_count ?? 0)}`,
        );
      }
      const now = Date.now();
      const nextSamples = [
        ...rateHistoryRef.current.filter((item) => now - item.t <= LOW_RATE_WINDOW_MS),
        { t: now, rate: currentRate },
      ];
      rateHistoryRef.current = nextSamples;
      setRateSamples(nextSamples.slice(-5));
    } catch (error) {
      if (error?.code === 'ERR_CANCELED') return;
    } finally {
      perfRequestInFlightRef.current = false;
      releaseAbortController(controller);
    }
  }, [taskId, createAbortController, releaseAbortController]);

  const loadLiveStatus = useCallback(async () => {
    if (taskId == null || liveStatusRequestInFlightRef.current) return;
    liveStatusRequestInFlightRef.current = true;
    const controller = createAbortController();
    try {
      const nextStatus = await getTaskStatus(taskId, { signal: controller.signal });
      liveStatusFailureCountRef.current = 0;
      setLiveStatusRetrying(false);
      setLiveStatus((currentStatus) => ({ ...(currentStatus || {}), ...(nextStatus || {}) }));
      const backendRate = Number(nextStatus?.sdk_success_rate_1s ?? nextStatus?.sdk_realtime_rate ?? nextStatus?.current_rate);
      const currentRate = Math.max(0, Number((Number.isFinite(backendRate) ? backendRate : 0).toFixed(2)));
      const now = Date.now();
      const nextSamples = [
        ...rateHistoryRef.current.filter((item) => now - item.t <= LOW_RATE_WINDOW_MS),
        { t: now, rate: currentRate },
      ];
      rateHistoryRef.current = nextSamples;
      setRateSamples(nextSamples.slice(-5));
      setPerf((currentPerf) => ({
        ...(currentPerf || {}),
        current_rate: currentRate,
        sdk_current_rate_1s: Number(nextStatus?.sdk_current_rate_1s ?? nextStatus?.sdk_realtime_processed_rate ?? currentRate),
        sdk_success_rate_1s: currentRate,
        sdk_realtime_rate: currentRate,
        sdk_current_rate_10s: Number(nextStatus?.sdk_current_rate_10s ?? nextStatus?.current_rate_10s ?? currentPerf?.sdk_current_rate_10s ?? 0),
        sdk_processed_last_1s: Number(nextStatus?.sdk_processed_last_1s ?? Math.round(currentRate)),
        sdk_success_last_1s: Number(nextStatus?.sdk_success_last_1s ?? Math.round(nextStatus?.sdk_success_rate_1s ?? 0)),
        success_rate: Number(nextStatus?.success_rate ?? currentPerf?.success_rate ?? currentRate),
        effective_concurrency: Number(nextStatus?.effective_concurrency ?? currentPerf?.effective_concurrency ?? task?.concurrency ?? 0),
        active_workers: Number(nextStatus?.active_workers ?? currentPerf?.active_workers ?? stats?.active_workers ?? 0),
        queue_write_length: Number(nextStatus?.result_queue_size ?? currentPerf?.queue_write_length ?? 0),
        rate_limit_setting: Number(nextStatus?.rate_limit_per_second ?? currentPerf?.rate_limit_setting ?? task?.rate_limit ?? 0),
        send_http_batch_size: Number(nextStatus?.send_http_batch_size ?? currentPerf?.send_http_batch_size ?? task?.send_http_batch_size ?? 50),
        configured_user_concurrency: Number(currentPerf?.configured_user_concurrency ?? task?.concurrency ?? 0),
        running: String(nextStatus?.status || task?.status || '') === 'running',
        sdk_realtime_window_seconds: Number(nextStatus?.sdk_realtime_window_seconds ?? currentPerf?.sdk_realtime_window_seconds ?? 5),
      }));
      setTask((currentTask) => {
        if (!currentTask) return currentTask;
        const nextTaskStatus = String(nextStatus?.status || currentTask.status || '');
        return {
          ...currentTask,
          status: nextTaskStatus || currentTask.status,
          status_label: nextStatus?.status_label || currentTask.status_label,
          run_message: nextStatus?.run_message ?? currentTask.run_message,
          status_reason: nextStatus?.status_reason ?? currentTask.status_reason,
          sent_count: Number(nextStatus?.success_count ?? currentTask.sent_count ?? 0),
          failed_count: Number(nextStatus?.fail_count ?? currentTask.failed_count ?? 0),
          total_count: Number(nextStatus?.total_recipients ?? currentTask.total_count ?? 0),
        };
      });
    } catch (error) {
      if (error?.code === 'ERR_CANCELED') return;
      liveStatusFailureCountRef.current += 1;
      if (liveStatusFailureCountRef.current >= 3) setLiveStatusRetrying(true);
      console.error(`任务 ${taskId} 实时数据加载失败，轮询将继续：${getErrorMessage(error, '网络请求失败')}`);
    } finally {
      liveStatusRequestInFlightRef.current = false;
      releaseAbortController(controller);
    }
  }, [taskId, createAbortController, releaseAbortController]);

  const refreshAll = useCallback(async () => {
    await Promise.all([loadTask(), loadStats(), loadPerf(), loadLiveStatus()]);
  }, [loadTask, loadStats, loadPerf, loadLiveStatus]);

  const clearTimers = useCallback(() => {
    [pollTimerRef, taskTimerRef].forEach((ref) => {
      if (ref.current) {
        window.clearInterval(ref.current);
        ref.current = null;
      }
    });
  }, []);

  useEffect(() => {
    if (invalidTaskId) return undefined;
    loadTask();
    loadStats();
    loadPerf();
    loadLiveStatus();
    return clearTimers;
  }, [invalidTaskId, loadTask, loadStats, loadPerf, loadLiveStatus, clearTimers]);

  useEffect(() => {
    const wasRunning = previousRunningRef.current;
    if (running && !wasRunning) {
      rateHistoryRef.current = [];
      setRateSamples([]);
      setPerformanceAlert(null);
      setPerf(null);
    }
    previousRunningRef.current = running;
  }, [running]);

  useEffect(() => {
    getPools(1, 100)
      .then((data) => setUrlPools(data.items || []))
      .catch(() => setUrlPools([]));
  }, []);

  useEffect(() => {
    const handleVisibility = () => {
      const visible = document.visibilityState !== 'hidden';
      isPageVisibleRef.current = visible;
      setIsPageVisible(visible);
      if (visible && activeTab === 'overview') {
        loadTask();
        loadStats();
        loadPerf();
        loadLiveStatus();
      }
    };
    document.addEventListener('visibilitychange', handleVisibility);
    return () => {
      document.removeEventListener('visibilitychange', handleVisibility);
    };
  }, [activeTab, loadTask, loadStats, loadPerf, loadLiveStatus]);

  useEffect(() => {
    clearTimers();
    if (invalidTaskId || activeTab !== 'overview') {
      return undefined;
    }
    const pollInterval = isPageVisible && running ? POLL_MS : HIDDEN_POLL_MS;
    const taskInterval = isPageVisible ? TASK_REFRESH_MS : HIDDEN_POLL_MS;
    let pollCancelled = false;
    const pollLiveStatus = async () => {
      if (pollCancelled || !isPageVisibleRef.current || activeTab !== 'overview') return;
      await loadLiveStatus();
      if (!pollCancelled && isPageVisibleRef.current && activeTab === 'overview') {
        pollTimerRef.current = window.setTimeout(pollLiveStatus, pollInterval);
      }
    };
    pollTimerRef.current = window.setTimeout(pollLiveStatus, 0);
    if (running) {
      taskTimerRef.current = window.setInterval(() => {
        loadTask();
        loadStats();
        loadPerf();
      }, taskInterval);
    } else {
      taskTimerRef.current = window.setInterval(loadStats, taskInterval);
    }
    return () => {
      pollCancelled = true;
      clearTimers();
    };
  }, [invalidTaskId, taskStatus, running, activeTab, isPageVisible, loadStats, loadPerf, loadLiveStatus, loadTask, clearTimers]);

  useEffect(() => {
    if (taskId == null || !running || !isPageVisible) {
      if (!running) setPerformanceAlert(null);
      return undefined;
    }
    let cancelled = false;
    const loadAlert = async () => {
      if (!isPageVisibleRef.current || alertRequestInFlightRef.current) return;
      alertRequestInFlightRef.current = true;
      const controller = createAbortController();
      try {
        const data = await getTaskAlert(taskId, { signal: controller.signal });
        if (cancelled) return;
        setPerformanceAlert(data && Object.keys(data).length > 0 ? data : null);
      } catch (error) {
        if (error?.code === 'ERR_CANCELED') return;
        if (!cancelled) setPerformanceAlert(null);
      } finally {
        alertRequestInFlightRef.current = false;
        releaseAbortController(controller);
      }
    };
    loadAlert();
    const timer = window.setInterval(loadAlert, 10000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [taskId, running, isPageVisible, createAbortController, releaseAbortController]);

  const canResume = taskStatus === 'paused' || taskStatus === 'failed' || (taskStatus === 'stopped' && Number(stats?.success || task?.sent_count || 0) > 0);
  const exportDisabled = taskStatus === 'draft' || !task;
  const exportBusy = Boolean(exportingType);

  const control = async (action) => {
    const now = Date.now();
    if (controlLoading || now < controlCooldownUntilRef.current) return;
    controlCooldownUntilRef.current = now + CONTROL_DEBOUNCE_MS;
    setControlLoading(action);
    if (action === 'resume') setResumeRetryExhausted(false);
    try {
      if (action === 'start') await startTask(taskId);
      if (action === 'pause') await pauseTask(taskId);
      if (action === 'resume') await runWithShutdownRetry(() => resumeTask(taskId));
      if (action === 'stop') await stopTask(taskId);
      message.success('控制命令已发送');
      await refreshAll();
    } catch (error) {
      if (action === 'resume' && isShutdownConflict409(error)) {
        setResumeRetryExhausted(true);
        message.error('任务仍在收尾中，请点击刷新后重试');
      } else {
        message.error(error?.userMessage || getErrorMessage(error, '控制任务失败'));
      }
    } finally {
      setControlLoading('');
    }
  };

  const runExport = async (type) => {
    if (exportDisabled || exportBusy) return;
    setExportingType(type);
    setExportProgressText('正在生成导出文件，请稍候...');
    try {
      let result;
      const options = { onProgress: (progress) => setExportProgressText(progress?.message || '文件生成中，请稍候...') };
      if (type === 'success') result = await exportSuccessRecipients(taskId, options);
      else if (type === 'failed') result = await exportFailedRecipients(taskId, options);
      else if (type === 'unset') result = await exportUnsetRecipients(taskId, options);
      if (result) {
        downloadBlob(result.blob, result.filename);
        message.success('文件开始下载');
      }
    } catch (error) {
      message.error(error?.message || await readBlobErrorDetail(error, '导出失败，请稍后重试'));
    } finally {
      setExportProgressText('');
      setExportingType('');
    }
  };

  const openSettingsEditor = () => {
    if (!task) return;
    const monitorEmails = Array.isArray(task.monitor_emails) && task.monitor_emails.length > 0 ? task.monitor_emails : [''];
    settingsForm.setFieldsValue({
      url_pool_id: task.url_pool_id || undefined,
      concurrency: task.concurrency || 1,
      auto_concurrency: Boolean(task.auto_concurrency),
      sender_rotation_enabled: Boolean(task.sender_rotation_enabled),
      send_http_batch_size: task.send_http_batch_size || 50,
      rate_limit: Number(task.rate_limit || 0),
      retry_count: task.retry_count || 0,
      monitor_interval: task.monitor_interval != null ? task.monitor_interval : 100,
      monitor_emails: monitorEmails,
      scheduled_at: parseScheduledAt(task.scheduled_at),
      timezone: task.timezone || 'Asia/Shanghai',
    });
    setLatencyResult(null);
    setSettingsOpen(true);
  };

  const runProxyLatencyCheck = async () => {
    if (!task?.channel_id || latencyLoading) return;
    const rateLimit = Number(settingsForm.getFieldValue('rate_limit'));
    if (!Number.isFinite(rateLimit) || rateLimit <= 0) {
      message.warning('请先填写大于 0 的速率限制（封/秒）');
      return;
    }
    const channelId = Number(task.channel_id);
    const batchSize = Number(settingsForm.getFieldValue('send_http_batch_size')) || DEFAULT_HTTP_BATCH_SIZE;
    const applyOptimalFields = (result) => {
      if (!result?.success) return;
      const advice = buildOptimalConfigAdvice(rateLimit, result.latency_ms, batchSize);
      const patch = {};
      if (result.recommended_concurrency) patch.concurrency = result.recommended_concurrency;
      if (advice?.recommendedBatchSize) patch.send_http_batch_size = advice.recommendedBatchSize;
      if (Object.keys(patch).length > 0) settingsForm.setFieldsValue(patch);
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
      message[result.success ? 'success' : 'error'](result.success ? formatProxyLatencySummary(result) : (result.message || '代理延迟检测失败'));
    } catch (error) {
      message.error(getErrorMessage(error, '代理延迟检测失败'));
    } finally {
      setLatencyLoading(false);
    }
  };

  const saveSettings = async () => {
    if (!task) return;
    try {
      const values = await settingsForm.validateFields();
      const monitorEmails = (values.monitor_emails || []).map((item) => String(item || '').trim()).filter(Boolean);
      setSettingsSaving(true);
      const updatedTask = await updateTask(taskId, {
        name: task.name,
        template_id: task.template_id,
        recipient_list_id: task.recipient_list_id,
        channel_id: task.channel_id,
        url_pool_id: values.url_pool_id ? Number(values.url_pool_id) : null,
        concurrency: Number(values.concurrency),
        auto_concurrency: Boolean(values.auto_concurrency),
        sender_rotation_enabled: Boolean(values.sender_rotation_enabled),
        batch_size: task.batch_size || 100,
        send_http_batch_size: Number(values.send_http_batch_size),
        rate_limit: Number(values.rate_limit),
        retry_count: Number(values.retry_count),
        monitor_emails: monitorEmails,
        monitor_interval: Number(values.monitor_interval),
        unsubscribe_enabled: Boolean(task.unsubscribe_enabled),
        scheduled_at: values.scheduled_at ? values.scheduled_at.format('YYYY-MM-DDTHH:mm:ss') : null,
        timezone: values.timezone || 'Asia/Shanghai',
      });
      setTask(updatedTask);
      setSettingsOpen(false);
      message.success(values.scheduled_at ? '发送设置与定时启动已保存' : '发送设置已保存，定时启动已取消');
      if (updatedTask?.concurrency_warning) message.warning(updatedTask.concurrency_warning, 8);
      await refreshAll();
    } catch (error) {
      if (!error.errorFields) message.error(getErrorMessage(error, '保存发送设置失败'));
    } finally {
      setSettingsSaving(false);
    }
  };

  const topBar = (
    <div className="task-cockpit__topbar">
      <div className="task-cockpit__title">
        <h1 title={task?.name || '任务详情'}>{task?.name || '任务详情'}</h1>
        <Tag color={TASK_STATUS_COLORS[taskStatus] || 'default'}>
          {resolveTaskStatusLabel(task)}
        </Tag>
      </div>
      <div className="task-cockpit__actions">
        {taskStatus !== 'paused' && taskStatus !== 'failed' && !(taskStatus === 'stopped' && canResume) ? (
          <Button
            type="primary"
            size="small"
            icon={<PlayCircleOutlined />}
            loading={controlLoading === 'start'}
            disabled={Boolean(controlLoading) || taskStatus === 'running'}
            onClick={() => control('start')}
          >
            启动
          </Button>
        ) : null}
        <Button
          size="small"
          icon={<PauseCircleOutlined />}
          loading={controlLoading === 'pause'}
          disabled={Boolean(controlLoading) || taskStatus !== 'running'}
          onClick={() => control('pause')}
        >
          暂停
        </Button>
        {canResume ? (
          <Button
            type="primary"
            ghost
            size="small"
            icon={<PlayCircleOutlined />}
            loading={controlLoading === 'resume'}
            disabled={Boolean(controlLoading)}
            onClick={() => control('resume')}
          >
            恢复
          </Button>
        ) : null}
        <Button
          danger
          size="small"
          icon={<StopOutlined />}
          loading={controlLoading === 'stop'}
          disabled={Boolean(controlLoading)}
          onClick={() => control('stop')}
        >
          终止
        </Button>
        <Button
          size="small"
          icon={<DownloadOutlined />}
          loading={exportingType === 'success'}
          disabled={exportDisabled || (exportBusy && exportingType !== 'success')}
          title={SUCCESS_EXPORT_TOOLTIP}
          onClick={() => runExport('success')}
        >
          导出 SDK 发送成功
        </Button>
        <Button
          size="small"
          icon={<DownloadOutlined />}
          loading={exportingType === 'failed'}
          disabled={exportDisabled || (exportBusy && exportingType !== 'failed')}
          title={FAILED_EXPORT_TOOLTIP}
          onClick={() => runExport('failed')}
        >
          导出 SDK 发送失败
        </Button>
        <Button
          size="small"
          icon={<DownloadOutlined />}
          loading={exportingType === 'unset'}
          disabled={exportDisabled || (exportBusy && exportingType !== 'unset')}
          title={UNSET_EXPORT_TOOLTIP}
          onClick={() => runExport('unset')}
        >
          导出未发送
        </Button>
        {(taskStatus === 'paused' || taskStatus === 'scheduled' || taskStatus === 'stopped') ? (
          <Button size="small" icon={<SettingOutlined />} disabled={Boolean(controlLoading)} onClick={openSettingsEditor}>
            设置
          </Button>
        ) : null}
        <Button size="small" icon={<InfoCircleOutlined />} onClick={() => setConfigOpen(true)}>配置</Button>
        <Button size="small" icon={<ReloadOutlined />} loading={loading} onClick={refreshAll}>刷新</Button>
        <Link to="/tasks"><Button size="small" icon={<ArrowLeftOutlined />}>返回</Button></Link>
      </div>
    </div>
  );

  if (invalidTaskId) {
    return (
      <div className="task-cockpit">
        <div className="task-cockpit__topbar">
          <div className="task-cockpit__title">
            <h1>任务详情</h1>
          </div>
          <Link to="/tasks"><Button size="small" icon={<ArrowLeftOutlined />}>返回</Button></Link>
        </div>
        <Empty description="任务 ID 无效">
          <Link to="/tasks"><Button type="primary">返回任务列表</Button></Link>
        </Empty>
      </div>
    );
  }

  if (taskNotFound && !loading) {
    return (
      <div className="task-cockpit">
        <div className="task-cockpit__topbar">
          <div className="task-cockpit__title">
            <h1>任务详情</h1>
          </div>
          <Link to="/tasks"><Button size="small" icon={<ArrowLeftOutlined />}>返回</Button></Link>
        </div>
        <Empty description="任务不存在或已被删除">
          <Link to="/tasks"><Button type="primary">返回任务列表</Button></Link>
        </Empty>
      </div>
    );
  }

  const overviewTab = (
    <div className="task-cockpit__overview">
      {resumeRetryExhausted ? (
        <Alert type="warning" showIcon banner message="恢复发送未成功，请刷新后重试" />
      ) : null}
      {liveStatusRetrying ? (
        <Alert type="warning" showIcon banner message="数据加载失败，正在重试…" style={{ marginBottom: 12 }} />
      ) : null}
      {liveStatus?.stalled ? (
        <Alert type="warning" showIcon banner message="任务可能已卡住" description="任务心跳超过 30 秒未更新，实时轮询仍在继续。" style={{ marginBottom: 12 }} />
      ) : null}

      {TERMINAL_STATUSES.has(taskStatus) && (terminalReason || terminalError) ? (
        <Alert
          type={taskStatus === 'failed' ? 'error' : taskStatus === 'stopped' ? 'warning' : 'success'}
          showIcon
          message={taskStatus === 'failed' ? '任务失败原因' : taskStatus === 'stopped' ? '任务停止原因' : '任务完成原因'}
          description={terminalReason || terminalError}
          style={{ marginBottom: 12 }}
        />
      ) : null}

      <div className="task-cockpit__main">
        <TaskOverviewLeft
          ringPercent={ringPercent}
          statusText={resolveProgressStatusLabel(liveStatus?.status || taskStatus, task)}
          stats={stats}
          liveStatus={liveStatus}
          successCount={successCount}
          failedCount={failedCount}
          remainingCount={remainingCount}
          configuredRateLimit={configuredRateLimit}
        />
        <TaskPerfGrid
          task={task}
          taskStatus={taskStatus}
          perf={perf}
          stats={stats}
          rateSamples={rateSamples}
        />
      </div>

      <TaskDiagnosticsDetail
        taskStatus={taskStatus}
        perf={perf}
        stats={stats}
      />

      <TaskSmartAdviceBar
        taskStatus={taskStatus}
        perf={perf}
        showLowRateWarning={showLowRateWarning}
        performanceAlert={performanceAlert}
      />
    </div>
  );

  const failuresTab = (
    <FailuresTable taskId={taskId} active={activeTab === 'failures'} />
  );

  return (
    <div className="task-cockpit mm-page mm-task-detail">
      {topBar}
      {exportProgressText ? (
        <Alert style={{ marginBottom: 12 }} type="info" showIcon message={exportProgressText} />
      ) : null}

      <Tabs
        className="task-cockpit__tabs"
        activeKey={activeTab}
        onChange={setActiveTab}
        items={[
          { key: 'overview', label: '实时总览', children: overviewTab },
          { key: 'failures', label: '失败明细', children: failuresTab },
        ]}
      />

      <Modal title="任务配置摘要" open={configOpen} onCancel={() => setConfigOpen(false)} footer={null} width={760} destroyOnHidden>
        <TaskConfigSummary task={task} flat />
      </Modal>

      <Modal title="编辑发送设置" open={settingsOpen} onCancel={() => setSettingsOpen(false)} onOk={saveSettings} confirmLoading={settingsSaving} okText="保存设置" cancelText="取消" destroyOnHidden width={920}>
        <Form form={settingsForm} layout="vertical" disabled={settingsSaving}>
          <Space style={{ marginBottom: 16 }}>
            <Text type="secondary">通道：{task?.channel_name || '-'}</Text>
            <Button icon={<ThunderboltOutlined />} loading={latencyLoading} onClick={runProxyLatencyCheck}>检测代理延迟</Button>
          </Space>
          {latencyResult ? (
            <Alert
              type={latencyResult.success ? 'success' : 'error'}
              showIcon
              message={latencyResult.success ? '延迟检测完成' : '延迟检测失败'}
              description={formatProxyLatencySummary(latencyResult)}
              style={{ marginBottom: 16 }}
            />
          ) : null}
          <Form.Item name="url_pool_id" label="URL 池" tooltip="模板含 {{url}} 变量时，启动任务前必须选择 URL 池">
            <Select allowClear virtual showSearch optionFilterProp="label" placeholder="可选；模板含追踪链接时启动前必选" options={urlPools.map((pool) => ({ value: pool.id, label: pool.name }))} />
          </Form.Item>
          <Card size="small" bordered={false} title="发送控制" style={{ marginBottom: 16 }}>
            <Form.Item noStyle shouldUpdate>
              {({ getFieldValue }) => {
                const rate = Number(getFieldValue('rate_limit') || 0);
                const concurrency = Number(getFieldValue('concurrency') || 1);
                const autoConcurrency = Boolean(getFieldValue('auto_concurrency'));
                const batchSize = Number(getFieldValue('send_http_batch_size')) || DEFAULT_HTTP_BATCH_SIZE;
                const { recommended, threshold } = getConcurrencyRecommendation(task?.channel_id, rate, batchSize);
                const warn = !autoConcurrency && isConcurrencyBelowRecommendation(concurrency, recommended);
                const advice = latencyResult?.success && rate > 0 ? buildOptimalConfigAdvice(rate, latencyResult.latency_ms, batchSize) : null;
                return (
                  <>
                    {advice ? (
                      <Collapse
                        style={{ marginBottom: 16 }}
                        items={[{
                          key: 'optimal',
                          label: '最优配置建议（基于代理 RTT）',
                          children: (
                            <Space direction="vertical" style={{ width: '100%' }}>
                              <Text>{formatOptimalConfigSummary(advice, rate)}</Text>
                              <Text type="secondary">当前：批量大小 {batchSize || '-'}，并发 {concurrency || '-'}</Text>
                              {advice.exceedsHttpRpsSoftLimit ? (
                                <Alert type="warning" showIcon message="HTTP 请求频率偏高" description={`预计超过软上限 ${SENDGRID_HTTP_RPS_SOFT_LIMIT} 次/秒。`} />
                              ) : null}
                              {advice.exceedsFreeDailyHint ? (
                                <Alert type="info" showIcon message="免费套餐提示" description={`持续 8 小时约 ${advice.sustainedEightHours} 封（参考 ${SENDGRID_FREE_DAILY_EMAIL_HINT} 封/天）。`} />
                              ) : null}
                            </Space>
                          ),
                        }]}
                      />
                    ) : null}
                    <Form.Item
                      name="auto_concurrency"
                      label="启用自动调整并发"
                      valuePropName="checked"
                      tooltip="开启后系统将根据网络延迟和实际发送速率在运行期动态优化并发数；修改后需停止任务并重新启动才生效。"
                    >
                      <Switch checkedChildren="开" unCheckedChildren="关" />
                    </Form.Item>
                    <Form.Item
                      name="sender_rotation_enabled"
                      label="发件人轮询"
                      valuePropName="checked"
                      tooltip="开启后按批次在通道多个启用发件人间轮换；关闭时使用第一个启用发件人。"
                    >
                      <Switch checkedChildren="开" unCheckedChildren="关" />
                    </Form.Item>
                    {autoConcurrency ? (
                      <Alert
                        type="info"
                        showIcon
                        style={{ marginBottom: 16 }}
                        message="已启用自动调整并发"
                        description="系统将根据网络延迟和速率自动优化并发数；手动并发数字段仅供参考。"
                      />
                    ) : null}
                    {warn && rate > 0 ? (
                      <Alert type="warning" showIcon style={{ marginBottom: 16 }} message="并发数可能不足以达到目标速率" description={`当前并发 ${concurrency}，推荐 ${recommended}（80% 阈值 ${threshold}；${RECOMMENDED_CONCURRENCY_FORMULA}）`} />
                    ) : null}
                    <Row gutter={16}>
                      <Col xs={24} md={8}>
                        <Form.Item name="concurrency" label="并发数" rules={[{ required: true, message: '请输入并发数' }]}>
                          <InputNumber min={1} max={1000} disabled={autoConcurrency} style={{ width: '100%', ...(warn ? concurrencyWarnInputStyle : {}), ...(autoConcurrency ? { opacity: 0.65 } : {}) }} />
                        </Form.Item>
                      </Col>
                      <Col xs={24} md={8}>
                        <Form.Item name="rate_limit" label="发送速率（封/秒）" rules={[{ required: true, message: '请输入发送速率' }]}>
                          <InputNumber min={0} max={100000} step={0.1} style={{ width: '100%' }} />
                        </Form.Item>
                      </Col>
                      <Col xs={24} md={8}>
                        <Form.Item name="send_http_batch_size" label="批量大小" rules={[{ required: true, message: '请输入批量大小' }]}>
                          <InputNumber min={1} max={1000} style={{ width: '100%' }} />
                        </Form.Item>
                      </Col>
                      <Col xs={24} md={8}>
                        <Form.Item name="retry_count" label="重试次数" rules={[{ required: true, message: '请输入重试次数' }]}>
                          <InputNumber min={0} max={10} style={{ width: '100%' }} />
                        </Form.Item>
                      </Col>
                      <Col xs={24} md={8}>
                        <Form.Item
                          name="monitor_interval"
                          label="监控间隔"
                          tooltip="常规发送累计成功 N 封后，向每个监控邮箱各发送 1 封回测邮件；设为 0 即关闭监控抽样"
                          rules={[{ required: true, message: '请输入监控间隔' }]}
                        >
                          <InputNumber min={0} max={1000000} style={{ width: '100%' }} />
                        </Form.Item>
                      </Col>
                      <Col xs={24} md={8}>
                        <Form.Item name="timezone" label="定时时区" rules={[{ required: true, message: '请选择时区' }]}>
                          <Select options={timezoneOptions} />
                        </Form.Item>
                      </Col>
                      <Col xs={24} md={16}>
                        <Form.Item name="scheduled_at" label="定时启动时间">
                          <DatePicker showTime={{ format: 'HH:mm' }} format="YYYY-MM-DD HH:mm" style={{ width: '100%' }} placeholder="不选择则取消定时" />
                        </Form.Item>
                      </Col>
                    </Row>
                  </>
                );
              }}
            </Form.Item>
          </Card>

          <Card size="small" bordered={false} title="监控与高级" style={{ marginBottom: 0 }}>
            <Form.List name="monitor_emails">
              {(fields, { add, remove }) => (
                <Space direction="vertical" style={{ width: '100%' }}>
                  <Text strong>监控邮箱</Text>
                  <Text type="secondary">每次触发时，列表中的每个邮箱各收到 1 封回测邮件。</Text>
                  {fields.map((field) => (
                    <Space key={field.key} align="baseline">
                      <Form.Item {...field} rules={[{ type: 'email', message: '请输入有效邮箱地址' }]} style={{ marginBottom: 8 }}>
                        <Input placeholder="monitor@example.com" style={{ width: 360 }} />
                      </Form.Item>
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
    </div>
  );
}
