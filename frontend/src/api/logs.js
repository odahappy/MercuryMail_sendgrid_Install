import { requestApi } from './index.js';

/** 面向用户的操作日志中需隐藏的类型（导出操作不留痕） */
export const USER_OPERATION_LOG_EXCLUDED_CATEGORIES = ['export'];

function isExportOperationLogEntry(item) {
  if (!item || typeof item !== 'object') return false;
  const category = String(item.category || item.action_category || item.operation_type || '').trim().toLowerCase();
  if (USER_OPERATION_LOG_EXCLUDED_CATEGORIES.includes(category)) return true;
  const action = String(item.action || item.action_label || item.summary || '').trim();
  return /导出/.test(action);
}

/** 过滤掉导出类操作日志，供操作/审计日志列表默认使用 */
export function filterUserOperationLogs(items = []) {
  if (!Array.isArray(items)) return [];
  return items.filter((item) => !isExportOperationLogEntry(item));
}

export async function getLogs(params = {}, config = {}) {
  const response = await requestApi({ method: 'get', url: '/logs', ...config, params });
  return response.data || {};
}

export async function getWarmupLogs(params = {}, config = {}) {
  const response = await requestApi({ method: 'get', url: '/logs', ...config, params: { ...params, log_type: 'warmup' } });
  return response.data || {};
}

export async function clearLogs(logType) {
  const response = await requestApi({ method: 'delete', url: '/logs', params: { type: logType } });
  return response.data || {};
}
