import { message } from 'antd';
import { getErrorMessage } from '../api/index.js';

const SHUTDOWN_RETRY_DELAY_MS = 2000;
const SHUTDOWN_MAX_RETRIES = 5;

function sleep(ms) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

export function isShutdownConflict409(error) {
  if (error?.response?.status !== 409) return false;
  const detail = getErrorMessage(error, '');
  return /收尾|正在运行|养号任务已在运行|养号计划正在运行/i.test(detail);
}

export async function runWithShutdownRetry(action, options = {}) {
  const {
    onRetrying,
    maxRetries = SHUTDOWN_MAX_RETRIES,
    retryDelayMs = SHUTDOWN_RETRY_DELAY_MS,
  } = options;
  let lastError = null;
  for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
    try {
      return await action();
    } catch (error) {
      lastError = error;
      if (!isShutdownConflict409(error) || attempt >= maxRetries) {
        throw error;
      }
      if (attempt === 0) {
        message.info('任务正在收尾中，请稍候...');
      }
      onRetrying?.(attempt + 1, maxRetries);
      await sleep(retryDelayMs);
    }
  }
  throw lastError;
}
