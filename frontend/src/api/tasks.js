import api, { exportApi, getErrorMessage, requestApi, requestExportApi } from './index.js';
import { notifyDashboardRefresh } from '../utils/dashboardRefresh.js';
import {
  downloadBlob,
  readBlobErrorDetail,
  resolveExportDownload,
  runSmartBlobExport,
} from './export.js';

const EXPORT_FAIL_MESSAGE = '导出失败，请稍后重试';

export async function getTasks(page = 1, pageSize = 20, status = '', config = {}) {
  const params = { page, page_size: pageSize };
  if (status) params.status = status;
  const response = await requestApi({ method: 'get', url: '/tasks', ...config, params });
  return response.data;
}

export async function getTask(id, config = {}) {
  const response = await requestApi({ method: 'get', url: `/tasks/${id}`, ...config });
  return response.data;
}

export async function createTask(payload) {
  const response = await api.post('/tasks', payload);
  return response.data;
}

export async function updateTask(id, payload) {
  const response = await api.put(`/tasks/${id}`, payload);
  return response.data;
}

export async function getTaskStats(taskId, config = {}) {
  const response = await requestApi({ method: 'get', url: `/tasks/${taskId}/stats`, ...config });
  return response.data;
}

export async function getTaskPerf(taskId, config = {}) {
  const response = await requestApi({ method: 'get', url: `/tasks/${taskId}/perf`, ...config });
  return response.data;
}

export async function getTaskStatus(taskId, config = {}) {
  const response = await requestApi({
    method: 'get',
    url: `/tasks/${taskId}/live-status`,
    timeout: 5000,
    ...config,
    params: {
      ...(config.params || {}),
      _ts: Date.now(),
    },
  });
  return response.data;
}

export async function getTaskFailures(taskId, page = 1, pageSize = 20) {
  const response = await api.get(`/tasks/${taskId}/failures`, {
    params: {
      page,
      page_size: pageSize,
    },
  });
  return response.data;
}

export async function getTaskAlert(id, config = {}) {
  const response = await requestApi({ method: 'get', url: `/tasks/${id}/alert`, ...config });
  return response.data;
}

export { downloadBlob };

async function runTaskExportRequest(request) {
  try {
    return await request();
  } catch (error) {
    const detail = error instanceof Error && error.message
      ? error.message
      : await readBlobErrorDetail(error, EXPORT_FAIL_MESSAGE);
    throw new Error(detail);
  }
}

export async function exportUnsetRecipients(taskId, options = {}) {
  return runTaskExportRequest(() => runSmartBlobExport(
    (config = {}) => requestExportApi({
      method: 'get',
      url: `/tasks/${taskId}/export/unset`,
      responseType: 'blob',
      timeout: 600000,
      ...config,
    }),
    `task_${taskId}_unset.txt`,
    options.onProgress,
  ));
}

export async function exportSuccessRecipients(taskId, options = {}) {
  return runTaskExportRequest(() => runSmartBlobExport(
    (config = {}) => requestExportApi({
      method: 'get',
      url: `/tasks/${taskId}/export/success`,
      responseType: 'blob',
      timeout: 600000,
      ...config,
    }),
    `task_${taskId}_success.txt`,
    options.onProgress,
  ));
}

export async function exportFailedRecipients(taskId, options = {}) {
  return runTaskExportRequest(() => runSmartBlobExport(
    (config = {}) => requestExportApi({
      method: 'get',
      url: `/tasks/${taskId}/export/failed`,
      responseType: 'blob',
      timeout: 600000,
      ...config,
    }),
    `task_${taskId}_failed.txt`,
    options.onProgress,
  ));
}

export async function startTask(id) {
  try {
    const response = await requestApi({ method: 'post', url: `/tasks/${id}/start` });
    return response.data;
  } catch (error) {
    error.userMessage = getErrorMessage(error, '启动任务失败');
    throw error;
  }
}

export async function bulkStartTasks(taskIds) {
  const response = await api.post('/tasks/batch-start', { task_ids: taskIds });
  return response.data;
}

export async function batchStartTasks(taskIds) {
  return bulkStartTasks(taskIds);
}

export async function batchPauseTasks(taskIds) {
  const response = await api.post('/tasks/batch-pause', { task_ids: taskIds });
  return response.data;
}

export async function batchResumeTasks(taskIds) {
  const response = await api.post('/tasks/batch-resume', { task_ids: taskIds });
  return response.data;
}

export async function batchStopTasks(taskIds) {
  const response = await api.post('/tasks/batch-stop', { task_ids: taskIds });
  return response.data;
}

export async function pauseTask(id) {
  const response = await api.post(`/tasks/${id}/pause`);
  return response.data;
}

export async function resumeTask(id) {
  const response = await api.post(`/tasks/${id}/resume`);
  return response.data;
}

export async function stopTask(id) {
  const response = await api.post(`/tasks/${id}/stop`);
  return response.data;
}

export async function deleteTask(id) {
  const response = await api.delete(`/tasks/${id}`);
  notifyDashboardRefresh();
  return response.data;
}
