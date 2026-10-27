const CACHE_TTL_MS = 5 * 60 * 1000;
const cache = new Map();

/** SendGrid 免费套餐参考：每日约 100 封；持续高速发送需升级套餐 */
export const SENDGRID_FREE_DAILY_EMAIL_HINT = 100;
/** HTTP 请求频率软上限（次/秒），超过可能触发平台限流 */
export const SENDGRID_HTTP_RPS_SOFT_LIMIT = 10;
/** 未填写 HTTP 批量大小时用于推荐并发计算的默认值 */
export const DEFAULT_HTTP_BATCH_SIZE = 50;
/** SDK 批量发送推荐并发公式说明 */
export const RECOMMENDED_CONCURRENCY_FORMULA = 'max(2, ceil((rate_limit / batch_size) × (RTT + 0.1s) × 1.5))';

function cacheKey(channelId, rateLimit, batchSize) {
  return `${channelId}:${Number(rateLimit)}:${Number(batchSize)}`;
}

export function getCachedProxyLatency(channelId, rateLimit, batchSize = DEFAULT_HTTP_BATCH_SIZE) {
  const entry = cache.get(cacheKey(channelId, rateLimit, batchSize));
  if (!entry || entry.expiresAt <= Date.now()) {
    return null;
  }
  return entry.data;
}

export function setCachedProxyLatency(channelId, rateLimit, data, batchSize = DEFAULT_HTTP_BATCH_SIZE) {
  cache.set(cacheKey(channelId, rateLimit, batchSize), {
    data,
    expiresAt: Date.now() + CACHE_TTL_MS,
  });
}

export function formatProxyLatencySummary(result) {
  if (!result?.success) {
    return result?.message || '代理延迟检测失败';
  }
  const routeLabel = result.proxy_used ? '代理' : '直连';
  const parts = [`${routeLabel}延迟 ${result.latency_ms} ms`];
  if (result.recommended_concurrency) {
    parts.push(`推荐并发 ${result.recommended_concurrency}`);
    parts.push(`公式：${RECOMMENDED_CONCURRENCY_FORMULA}`);
  }
  return parts.join('，');
}

export function computeRecommendedConcurrency(rateLimit, latencyMs = 1500, batchSize = DEFAULT_HTTP_BATCH_SIZE) {
  const rate = Number(rateLimit || 0);
  if (rate <= 0) {
    return null;
  }
  const batch = Math.max(1, Number(batchSize) || DEFAULT_HTTP_BATCH_SIZE);
  const rttSeconds = Math.max(0.1, Number(latencyMs || 1500) / 1000);
  const httpRps = rate / batch;
  return Math.max(4, Math.ceil(httpRps * (rttSeconds + 0.1) * 1.5));
}

/**
 * 基于速率、代理 RTT 与 HTTP 批量大小计算 SDK 批量发送最优配置建议。
 * 目标 HTTP 频率约 1 次/秒：推荐 batch_size ≈ rate_limit。
 */
export function buildOptimalConfigAdvice(rateLimit, latencyMs = 1500, batchSize = DEFAULT_HTTP_BATCH_SIZE) {
  const rate = Number(rateLimit || 0);
  if (!Number.isFinite(rate) || rate <= 0) {
    return null;
  }
  const rttMs = Math.max(100, Number(latencyMs || 1500));
  const rttSeconds = rttMs / 1000;
  const effectiveBatch = Math.max(1, Number(batchSize) || DEFAULT_HTTP_BATCH_SIZE);
  const recommendedConcurrency = computeRecommendedConcurrency(rate, rttMs, effectiveBatch);
  const recommendedBatchSize = Math.min(1000, Math.max(1, Math.round(rate)));
  const estimatedHttpRps = rate / recommendedBatchSize;
  const sustainedEightHours = rate * 3600 * 8;
  const exceedsFreeDailyHint = sustainedEightHours > SENDGRID_FREE_DAILY_EMAIL_HINT;
  const exceedsHttpRpsSoftLimit = estimatedHttpRps > SENDGRID_HTTP_RPS_SOFT_LIMIT;
  return {
    rttMs,
    rttSeconds,
    effectiveBatch,
    recommendedConcurrency,
    recommendedBatchSize,
    estimatedHttpRps,
    exceedsFreeDailyHint,
    exceedsHttpRpsSoftLimit,
    sustainedEightHours: Math.round(sustainedEightHours),
  };
}

export function formatOptimalConfigSummary(advice, rateLimit) {
  if (!advice) {
    return '请先填写大于 0 的速率限制并检测代理延迟';
  }
  const rate = Number(rateLimit || 0);
  const httpRpsText = Number.isInteger(advice.estimatedHttpRps)
    ? String(advice.estimatedHttpRps)
    : advice.estimatedHttpRps.toFixed(2);
  return [
    `代理 RTT ${advice.rttMs} ms`,
    `计算批量 ${advice.effectiveBatch}`,
    `推荐 HTTP 批量大小 ${advice.recommendedBatchSize}`,
    `推荐并发 ${advice.recommendedConcurrency}（${RECOMMENDED_CONCURRENCY_FORMULA}）`,
    `预计 HTTP 频率 ${httpRpsText} 次/秒（${rate} 封/秒 ÷ ${advice.recommendedBatchSize}）`,
  ].join('；');
}

export function getConcurrencyRecommendation(channelId, rateLimit, batchSize = DEFAULT_HTTP_BATCH_SIZE) {
  const rate = Number(rateLimit || 0);
  if (rate <= 0) {
    return { recommended: null, threshold: null, latencyMs: null };
  }
  const normalizedBatch = Math.max(1, Number(batchSize) || DEFAULT_HTTP_BATCH_SIZE);
  const cached = channelId ? getCachedProxyLatency(channelId, rate, normalizedBatch) : null;
  const latencyMs = cached?.latency_ms || 1500;
  const recommended = cached?.recommended_concurrency || computeRecommendedConcurrency(rate, latencyMs, normalizedBatch);
  const threshold = recommended ? Math.max(1, Math.floor(recommended * 0.8)) : null;
  return { recommended, threshold, latencyMs, cached };
}

export function isConcurrencyBelowRecommendation(concurrency, recommended) {
  const normalized = Number(concurrency || 0);
  const rec = Number(recommended || 0);
  if (!rec) {
    return false;
  }
  const threshold = Math.max(1, Math.floor(rec * 0.8));
  return normalized < threshold;
}

export const concurrencyWarnInputStyle = {
  borderColor: '#fa8c16',
  boxShadow: '0 0 0 2px rgba(250, 140, 22, 0.2)',
};
