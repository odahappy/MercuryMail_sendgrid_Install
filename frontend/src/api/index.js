import axios from 'axios';

const NETWORK_RETRY_DELAY_MS = 800;
const MAX_NETWORK_RETRIES = 2;
const API_TIMEOUT_MS = 30000;
const EXPORT_TIMEOUT_MS = 600000;
const UPLOAD_TIMEOUT_MS = 180000;
const ABORT_ERROR_MESSAGE = '请求已取消';
const routeAbortControllers = new Set();

export function cancelRouteRequests() {
  for (const controller of routeAbortControllers) {
    try {
      controller.abort(ABORT_ERROR_MESSAGE);
    } catch {
      // ignore
    }
  }
  routeAbortControllers.clear();
}

function shouldAutoCancelOnNavigation(config) {
  const method = String(config?.method || 'get').toLowerCase();
  if (method !== 'get') return false;
  if (config?.signal || config?.skipRouteCancel) return false;
  const url = String(config?.url || '');
  if (url.includes('/export/')) return false;
  return true;
}

function trackRouteAbortController(config) {
  if (!shouldAutoCancelOnNavigation(config)) return config;
  const controller = new AbortController();
  routeAbortControllers.add(controller);
  config.signal = controller.signal;
  config.__routeAbortController = controller;
  return config;
}

function untrackRouteAbortController(config) {
  const controller = config?.__routeAbortController;
  if (controller) {
    routeAbortControllers.delete(controller);
    delete config.__routeAbortController;
  }
}

function sleep(ms, signal) {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(new DOMException(ABORT_ERROR_MESSAGE, 'AbortError'));
      return;
    }
    const timer = window.setTimeout(() => {
      signal?.removeEventListener?.('abort', onAbort);
      resolve();
    }, ms);
    function onAbort() {
      window.clearTimeout(timer);
      reject(new DOMException(ABORT_ERROR_MESSAGE, 'AbortError'));
    }
    signal?.addEventListener?.('abort', onAbort, { once: true });
  });
}

function getStoredToken() {
  return window.localStorage.getItem('mercury_token');
}

function getStoredTokenExpireAt() {
  return Number(window.localStorage.getItem('mercury_token_expire_at') || 0);
}

function clearStoredAuth() {
  window.localStorage.removeItem('mercury_token');
  window.localStorage.removeItem('mercury_token_expire_at');
}

const ERROR_TEXT_MAP = new Map([
  ['密码已更改，请重新登录', '密码已更改，请重新登录'],
  ['Incorrect username or password', '密码错误'],
  ['Not authenticated', '请先登录'],
  ['Invalid authentication token', '登录状态已失效，请重新登录'],
  ['Invalid administrator identity', '管理员身份校验失败，请重新登录'],
  ['Task not found', '任务不存在'],
  ['Channel not found', '通道不存在'],
  ['Proxy not found', '代理不存在'],
  ['Recipient list not found', '分组不存在'],
  ['Template not found', '模板不存在'],
  ['URL pool not found', 'URL 池不存在'],
  ['User not found', '用户不存在'],
  ['Internal Server Error', '服务器内部错误，请稍后重试'],
  ['Network Error', '网络连接失败，请检查后重试'],
  ['Connection timeout', '连接超时，请稍后重试'],
  ['Unauthorized', '未授权访问'],
  ['Invalid API Key', 'API Key 无效'],
  ['Authenticated user is not authorized to send mail', '当前 API Key 无权发送邮件，请检查 Mail Send 权限和发件人身份验证'],
  ['通道认证失败，已自动暂停', '通道认证失败，任务已自动暂停'],
  ['Rate limit exceeded', '请求过于频繁，请稍后重试'],
  ['Permission denied', '权限不足'],
  ['Validation error', '数据校验失败'],
  ['Export failed', '导出失败，请稍后重试'],
  ['File not found', '文件不存在'],
  ['WRITER_BATCH_SIZE must be between 1 and 5000', 'WRITER_BATCH_SIZE 必须在 1 到 5000 之间'],
  ['URL cannot be empty', 'URL 不能为空'],
  ['URL host cannot be empty', 'URL 主机名不能为空'],
  ['URL 仅支持 http 或 https', 'URL 仅支持 http 或 https'],
  ['URL pool cache is closed', 'URL 池缓存已关闭'],
  ['Send failed', '发送失败'],
  ['HTTP Error', 'HTTP 错误'],
  ['Transport Error', '传输错误'],
  ['MercuryMail model registry is incomplete', 'MercuryMail 模型注册表不完整，无法启动'],
  ['Request failed with status code 401', '认证失败，请检查登录状态、API Key 权限或发件人地址'],
  ['Request failed with status code 403', '没有权限执行此操作，请检查 API Key 权限或发件人地址'],
  ['Request failed with status code 404', '请求的资源不存在'],
  ['Request failed with status code 422', '提交的数据格式不正确，请检查后重试'],
  ['Request failed with status code 429', '操作过于频繁，请稍后重试'],
  ['Request failed with status code 500', '服务器内部错误，请稍后重试'],
  ['Request failed with status code 503', '服务暂时不可用，请稍后重试']
]);

function translateErrorText(value, fallback, status) {
  const text = String(value || '').trim();
  if (!text) return fallback || '操作失败，请稍后重试';
  if (status === 404) {
    if (/分组|模板|任务|通道|代理|URL 池|用户|收件人池|不存在|已过期/i.test(text)) return text;
    return text || '请求的资源不存在';
  }
  if (ERROR_TEXT_MAP.has(text)) return ERROR_TEXT_MAP.get(text);
  if (/[^\x00-\x7F]/.test(text)) return text;
  if (/^Request failed with status code \d+$/.test(text)) return fallback || '请求失败，请稍后重试';
  if (/timeout|timed out|ECONNABORTED/i.test(text)) return '请求超时，请检查服务是否繁忙或网络是否稳定';
  if (/Network Error/i.test(text)) return '网络连接失败，请检查后重试';
  if (/Failed to fetch|ERR_NETWORK|ERR_CONNECTION_REFUSED/i.test(text)) return '无法连接到后端服务，请确认服务已启动并可访问';
  if (/loading chunk|chunk load/i.test(text)) return '前端资源加载失败，请刷新页面后重试';
  if (/database|db|pool/i.test(text)) return '数据库暂时繁忙，请稍后重试';
  if (/redis/i.test(text)) return 'Redis 服务暂时不可用，请稍后重试';
  if (/initializ|starting|service unavailable/i.test(text)) return '服务正在初始化或暂时不可用，请稍后刷新重试';
  if (/proxy.*failed|failed.*proxy/i.test(text)) return '连接代理失败，请检查代理配置';
  if (/network/i.test(text)) return '网络连接失败，请检查后重试';
  if (/error/i.test(text)) return fallback || '操作失败，请稍后重试';
  if (/^[\x00-\x7F]+$/.test(text) && /[A-Za-z]/.test(text)) return fallback || '操作失败，请稍后重试';
  return text;
}

function extractErrorDetail(error) {
  const responseData = error?.response?.data;
  if (!responseData) return '';
  const detail = responseData.detail;
  if (typeof detail === 'string') return detail.trim();
  if (Array.isArray(detail)) {
    return detail
      .map((item) => {
        if (typeof item === 'string') return item.trim();
        if (item && typeof item === 'object') return String(item.msg || item.message || '').trim();
        return String(item || '').trim();
      })
      .filter(Boolean)
      .join('；');
  }
  if (detail && typeof detail === 'object') {
    return String(detail.msg || detail.message || '').trim();
  }
  if (typeof responseData.message === 'string') return responseData.message.trim();
  return '';
}

export function getErrorMessage(error, fallback) {
  const detail = extractErrorDetail(error);
  const status = Number(error?.response?.status || 0);
  if (detail) return translateErrorText(detail, fallback, status);
  if (typeof error?.message === 'string') return translateErrorText(error.message, fallback, status);
  return fallback || '操作失败，请稍后重试';
}

function isTokenRequestUsable() {
  const token = getStoredToken();
  const expireAt = getStoredTokenExpireAt();
  return Boolean(token && expireAt && Date.now() < expireAt);
}

function redirectToLogin() {
  if (window.location.pathname !== '/login') {
    const current = `${window.location.pathname}${window.location.search}${window.location.hash}`;
    if (!window.sessionStorage.getItem('mercury_auth_redirecting')) {
      window.sessionStorage.setItem('mercury_auth_redirecting', '1');
      window.location.assign(`/login?redirect=${encodeURIComponent(current)}`);
    }
  } else {
    window.sessionStorage.removeItem('mercury_auth_redirecting');
  }
}

const api = axios.create({
  baseURL: '/api',
  timeout: API_TIMEOUT_MS,
  headers: {
    'Content-Type': 'application/json'
  }
});

const exportApi = axios.create({
  baseURL: '/api',
  timeout: EXPORT_TIMEOUT_MS,
  headers: {
    'Content-Type': 'application/json'
  }
});

const uploadApi = axios.create({
  baseURL: '/api',
  timeout: UPLOAD_TIMEOUT_MS,
  headers: {
    'Content-Type': 'application/json'
  }
});

function prepareMultipartUploadHeaders(headers) {
  if (!headers) return;
  if (typeof headers.delete === 'function') {
    headers.delete('Content-Type');
    headers.delete('content-type');
    return;
  }
  delete headers['Content-Type'];
  delete headers['content-type'];
}

function attachAuthInterceptors(instance, { retryNetworkGet = false, routeCancelableGet = true } = {}) {
  instance.interceptors.request.use((config) => {
    if (routeCancelableGet) {
      trackRouteAbortController(config);
    }
    if (typeof FormData !== 'undefined' && config.data instanceof FormData) {
      prepareMultipartUploadHeaders(config.headers);
    }
    const token = getStoredToken();
    if (token && isTokenRequestUsable()) {
      config.headers.Authorization = `Bearer ${token}`;
    } else if (token) {
      clearStoredAuth();
    }
    return config;
  });

  instance.interceptors.response.use(
    (response) => {
      untrackRouteAbortController(response.config);
      return response;
    },
    async (error) => {
      const config = error.config || {};
      untrackRouteAbortController(config);
      const method = (config.method || 'get').toLowerCase();
      const isCanceled = error.code === 'ERR_CANCELED' || error.message === ABORT_ERROR_MESSAGE;
      if (isCanceled) {
        error.detail = ABORT_ERROR_MESSAGE;
        return Promise.reject(error);
      }
      const isNetworkError = !error.response;
      config.__networkRetryCount = config.__networkRetryCount || 0;

      if (
        retryNetworkGet
        && !config.skipNetworkRetry
        && isNetworkError
        && method === 'get'
        && config.__networkRetryCount < MAX_NETWORK_RETRIES
      ) {
        config.__networkRetryCount += 1;
        await sleep(NETWORK_RETRY_DELAY_MS * config.__networkRetryCount, config.signal);
        return instance(config);
      }

      if (isNetworkError && String(config.url || '').includes('/export/')) {
        const timeoutLike = error.code === 'ECONNABORTED' || /timeout/i.test(String(error.message || ''));
        if (timeoutLike) {
          error.message = '连接超时，请稍后重试';
        }
      }

      if (error.response && error.response.status === 401 && !config.skipAuthRedirect) {
        clearStoredAuth();
        redirectToLogin();
      }
      error.detail = extractErrorDetail(error);
      return Promise.reject(error);
    }
  );
}

function createAbortableRequest(instance) {
  return async function request(config = {}) {
    return instance(config);
  };
}

attachAuthInterceptors(api, { retryNetworkGet: true, routeCancelableGet: true });
attachAuthInterceptors(exportApi, { routeCancelableGet: false });
attachAuthInterceptors(uploadApi, { retryNetworkGet: true, routeCancelableGet: false });

export const requestApi = createAbortableRequest(api);
export const requestExportApi = createAbortableRequest(exportApi);
export const requestUploadApi = createAbortableRequest(uploadApi);
export { exportApi, uploadApi };
export default api;
