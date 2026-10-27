export const IMPORT_PROGRESS_STATUS_LABELS = {

  queued: '排队中',

  processing: '处理中',

  completed: '已完成',

  failed: '失败',

};



export const TASK_STATUS_LABELS = {

  draft: '草稿',

  scheduled: '等待中',

  pending: '等待中',

  running: '运行中',

  paused: '已暂停',

  finished: '已完成',

  completed: '已完成',

  stopped: '已终止',

  aborted: '已终止',

  failed: '已失败',

  cancelled: '已取消',

};



export const TASK_STATUS_COLORS = {

  draft: 'default',

  scheduled: 'orange',

  pending: 'orange',

  running: 'success',

  paused: 'blue',

  finished: 'success',

  completed: 'success',

  stopped: 'error',

  aborted: 'error',

  failed: 'error',

  cancelled: 'default',

};



/** SendLog / Event Poller 投递与发送结果状态（仅展示用） */

export const SEND_RESULT_STATUS_LABELS = {

  sent: '发送成功',

  success: '发送成功',

  failed: '发送失败',

  delivered: '已送达',

  bounced: '硬弹',

  hard_bounce: '硬弹',

  soft_bounce: '软弹',

  blocked: '被阻止',

  deferred: '延迟',

  spamreported: '垃圾邮件投诉',

  spam_report: '垃圾邮件投诉',

  retrying: '重试中',

  pending: '待发送',

  monitor_sent: '监控成功',

  monitor_failed: '监控失败',

  queued: '排队中',

  processing: '处理中',

};



export function resolveTaskStatusLabel(taskOrStatus) {

  if (taskOrStatus && typeof taskOrStatus === 'object') {

    if (taskOrStatus.status_label) return taskOrStatus.status_label;

    return TASK_STATUS_LABELS[taskOrStatus.status] || '未知';

  }

  return TASK_STATUS_LABELS[taskOrStatus] || '未知';

}



export function resolveSendResultStatusLabel(status) {

  const normalized = String(status || '').trim().toLowerCase();

  if (!normalized) return '—';

  return SEND_RESULT_STATUS_LABELS[normalized] || '未知';

}



export function formatCount(value) {

  const num = Number(value || 0);

  if (!Number.isFinite(num)) return '0';

  return num.toLocaleString('zh-CN');

}



export function formatRate(value) {

  const num = Number(value || 0);

  if (!Number.isFinite(num)) return '0.0';

  return num.toFixed(1);

}



export function formatDateTime(value) {

  if (!value) return '-';

  const parsed = new Date(value);

  if (Number.isNaN(parsed.getTime())) return String(value);

  return parsed.toLocaleString('zh-CN');

}



export function resolveTaskStatus(task, perf) {

  return String(task?.status || perf?.status || 'draft');

}



export function isTaskRunning(status) {

  return status === 'running';

}



export function renderTaskStatusTagProps(taskOrStatus) {

  const status = typeof taskOrStatus === 'object' ? String(taskOrStatus?.status || 'draft') : String(taskOrStatus || 'draft');

  return {

    status,

    label: resolveTaskStatusLabel(taskOrStatus),

    color: TASK_STATUS_COLORS[status] || 'default',

  };

}


