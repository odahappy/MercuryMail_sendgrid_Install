import { IMPORT_ACTIVE_STATUSES, IMPORT_FINISHED_STATUSES } from '../constants/importStatus.js';

const STORAGE_KEY = 'mercury_recipient_import_tasks';
const MAX_STORED_TASKS = 20;

function normalizeTask(raw) {
  const taskId = String(raw?.taskId || raw?.task_id || '').trim();
  const listId = Number(raw?.listId || raw?.list_id || 0);
  const poolName = String(raw?.poolName || raw?.pool_name || raw?.name || '').trim();
  if (!taskId || !listId) return null;
  return {
    taskId,
    listId,
    poolName,
    startedAt: Number(raw?.startedAt || raw?.started_at || Date.now()),
  };
}

export function loadStoredImportTasks() {
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    const parsed = JSON.parse(raw || '[]');
    if (!Array.isArray(parsed)) return [];
    return parsed.map(normalizeTask).filter(Boolean).slice(0, MAX_STORED_TASKS);
  } catch {
    return [];
  }
}

export function saveStoredImportTasks(tasks) {
  const normalized = (tasks || []).map(normalizeTask).filter(Boolean).slice(0, MAX_STORED_TASKS);
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify(normalized));
}

export function addStoredImportTask(task) {
  const normalized = normalizeTask(task);
  if (!normalized) return;
  const tasks = loadStoredImportTasks().filter((item) => item.taskId !== normalized.taskId);
  tasks.unshift(normalized);
  saveStoredImportTasks(tasks);
}

export function removeStoredImportTask(taskId) {
  const normalizedId = String(taskId || '').trim();
  if (!normalizedId) return;
  saveStoredImportTasks(loadStoredImportTasks().filter((item) => item.taskId !== normalizedId));
}

export function normalizeImportStatus(status) {
  return String(status || '').trim().toLowerCase();
}

export function isImportTaskActive(status) {
  return IMPORT_ACTIVE_STATUSES.includes(normalizeImportStatus(status));
}

export function isImportTaskFinished(status) {
  return IMPORT_FINISHED_STATUSES.includes(normalizeImportStatus(status));
}
