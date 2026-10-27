import api, { exportApi, getErrorMessage, requestApi, requestExportApi } from './index.js';

const EXPORT_FAIL_MESSAGE = '导出失败，请稍后重试';
const ASYNC_EXPORT_POLL_MS = 3000;
const ASYNC_EXPORT_MAX_POLLS = 1200;

function sleep(ms) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

function resolveContentType(response) {
  return String(
    response?.headers?.['content-type'] || response?.headers?.['Content-Type'] || '',
  ).toLowerCase();
}

function isBlobLike(value) {
  return value instanceof Blob || Object.prototype.toString.call(value) === '[object Blob]';
}

async function readResponsePayload(response) {
  const data = response?.data;
  if (isBlobLike(data)) {
    const text = await data.text();
    return text ? JSON.parse(text) : {};
  }
  if (typeof data === 'string') {
    return data ? JSON.parse(data) : {};
  }
  if (data && typeof data === 'object') {
    return data;
  }
  return {};
}

export async function readBlobErrorDetail(error, fallback = EXPORT_FAIL_MESSAGE) {
  const data = error?.response?.data;
  if (isBlobLike(data)) {
    try {
      const text = await data.text();
      if (!text.trim()) {
        return fallback;
      }
      const parsed = JSON.parse(text);
      return getErrorMessage({ response: { data: parsed } }, fallback);
    } catch {
      return fallback;
    }
  }
  return getErrorMessage(error, fallback);
}

export async function pollAsyncExport(exportId, onProgress) {
  const normalizedId = String(exportId || '').trim();
  if (!normalizedId) {
    throw new Error(EXPORT_FAIL_MESSAGE);
  }
  for (let attempt = 0; attempt < ASYNC_EXPORT_MAX_POLLS; attempt += 1) {
    if (attempt > 0) {
      await sleep(ASYNC_EXPORT_POLL_MS);
    }
    const statusResponse = await requestApi({ method: 'get', url: `/export/async/${normalizedId}` });
    const statusData = statusResponse.data || {};
    if (typeof onProgress === 'function') {
      const percent = Number(statusData.progress_percent || 0);
      const progressText = Number.isFinite(percent) && percent > 0
        ? `正在生成导出文件... 已完成 ${Math.min(100, Math.max(0, Math.round(percent)))}%`
        : '正在生成导出文件，请稍候...';
      onProgress({
        export_id: normalizedId,
        status: statusData.status || 'processing',
        progress_percent: Number.isFinite(percent) ? percent : 0,
        message: progressText,
      });
    }
    if (statusData.status === 'completed') {
      const downloadResponse = await requestExportApi({
        method: 'get',
        url: `/export/async/${normalizedId}/download`,
        responseType: 'blob',
        timeout: 600000,
      });
      return resolveExportDownload(
        downloadResponse,
        statusData.filename || timestampExportFilename('export'),
      );
    }
    if (statusData.status === 'failed') {
      throw new Error(statusData.error || EXPORT_FAIL_MESSAGE);
    }
  }
  throw new Error('导出超时，请稍后重试');
}

export async function runSmartBlobExport(requestFn, fallbackFilename, onProgress) {
  try {
    const response = await requestFn();
    const contentType = resolveContentType(response);
    if (contentType.includes('application/json')) {
      let payload;
      try {
        payload = await readResponsePayload(response);
      } catch {
        throw new Error(EXPORT_FAIL_MESSAGE);
      }
      if (!payload?.export_id) {
        throw new Error(getErrorMessage({ response: { data: payload } }, EXPORT_FAIL_MESSAGE));
      }
      return pollAsyncExport(payload.export_id, onProgress);
    }
    return resolveExportDownload(response, fallbackFilename);
  } catch (error) {
    if (error?.response) {
      const detail = await readBlobErrorDetail(error, EXPORT_FAIL_MESSAGE);
      throw new Error(detail);
    }
    throw error;
  }
}

async function runBlobExport(request, fallback = EXPORT_FAIL_MESSAGE) {
  try {
    return await request();
  } catch (error) {
    const detail = error instanceof Error && error.message
      ? error.message
      : await readBlobErrorDetail(error, fallback);
    throw new Error(detail);
  }
}

export function normalizeTaskIds(taskIds) {
  if (!Array.isArray(taskIds)) {
    return [];
  }
  const seen = new Set();
  return taskIds
    .map((id) => Number(id))
    .filter((id) => Number.isInteger(id) && id > 0 && !seen.has(id) && seen.add(id));
}

export async function listExportTasks(window) {
  const response = await requestApi({ method: 'get', url: '/export/tasks', params: { window } });
  return response.data;
}

export async function getExportTasks(window) {
  return listExportTasks(window);
}

export function parseExportFilename(contentDisposition, fallback) {
  const header = String(contentDisposition || '');
  const utf8Match = header.match(/filename\*=UTF-8''([^;]+)/i);
  if (utf8Match?.[1]) {
    try {
      return decodeURIComponent(utf8Match[1]);
    } catch {
      return fallback;
    }
  }
  const plainMatch = header.match(/filename="?([^";]+)"?/i);
  return plainMatch?.[1] || fallback;
}

export function resolveExportDownload(response, fallbackFilename) {
  const disposition = response.headers?.['content-disposition']
    || response.headers?.['Content-Disposition']
    || '';
  return {
    blob: response.data,
    filename: parseExportFilename(disposition, fallbackFilename),
  };
}

export async function exportValidRecipients(source, options = {}) {
  return runBlobExport(() => runSmartBlobExport(
    () => requestExportApi({
      method: 'get',
      url: '/export/valid-recipients',
      params: { source },
      responseType: 'blob',
      timeout: 600000,
    }),
    timestampExportFilename('valid_recipients_all'),
    options.onProgress,
  ));
}

export async function exportValidRecipientsHot(taskIds, type, options = {}) {
  const normalizedIds = normalizeTaskIds(taskIds);
  if (!normalizedIds.length) {
    throw new Error('请至少选择一个有效任务');
  }
  return runBlobExport(() => runSmartBlobExport(
    () => requestExportApi({
      method: 'post',
      url: '/export/valid-recipients/hot',
      data: { task_ids: normalizedIds, type },
      responseType: 'blob',
      timeout: 600000,
    }),
    timestampExportFilename('valid_recipients_hot'),
    options.onProgress,
  ));
}

export async function exportValidRecipientsArchive(taskIds, type, options = {}) {
  const normalizedIds = normalizeTaskIds(taskIds);
  if (!normalizedIds.length) {
    throw new Error('请至少选择一个有效任务');
  }
  return runBlobExport(() => runSmartBlobExport(
    () => requestExportApi({
      method: 'post',
      url: '/export/valid-recipients/archive',
      data: { task_ids: normalizedIds, type },
      responseType: 'blob',
      timeout: 600000,
    }),
    timestampExportFilename('valid_recipients_archive'),
    options.onProgress,
  ));
}

export async function exportGroupRecipients(groupId, fallbackFilename, options = {}) {
  return runBlobExport(() => runSmartBlobExport(
    () => requestExportApi({
      method: 'get',
      url: `/export/group/${groupId}`,
      responseType: 'blob',
      timeout: 600000,
    }),
    fallbackFilename || timestampExportFilename(`group_${groupId}`),
    options.onProgress,
  ));
}

export function downloadBlob(blob, filename) {
  const url = window.URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  link.style.display = 'none';
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.URL.revokeObjectURL(url);
}

export function timestampExportFilename(prefix) {
  const now = new Date();
  const pad = (value) => String(value).padStart(2, '0');
  const stamp = [
    now.getUTCFullYear(),
    pad(now.getUTCMonth() + 1),
    pad(now.getUTCDate()),
    '_',
    pad(now.getUTCHours()),
    pad(now.getUTCMinutes()),
    pad(now.getUTCSeconds()),
  ].join('');
  return `${prefix}_${stamp}.txt`;
}
