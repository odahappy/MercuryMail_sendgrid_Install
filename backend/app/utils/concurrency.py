import math

TASK_CONCURRENCY_MAX = 1000
RTT_HEADROOM = 1.2
MIN_RECOMMENDED_CONCURRENCY = 2


def calculate_recommended_concurrency(
    rate_limit: float,
    latency_ms: int,
    send_http_batch_size: int = 1,
) -> int:
    """基于 SDK 批量发送模型推荐任务并发数：HTTP 频率 = rate_limit / batch_size。"""
    rate = max(0.0, float(rate_limit or 0))
    if rate <= 0:
        return MIN_RECOMMENDED_CONCURRENCY
    batch = max(1, int(send_http_batch_size or 1))
    latency_seconds = max(0.001, float(latency_ms) / 1000.0)
    http_rps = rate / float(batch)
    recommended = math.ceil(http_rps * latency_seconds * RTT_HEADROOM)
    return max(MIN_RECOMMENDED_CONCURRENCY, min(int(recommended), TASK_CONCURRENCY_MAX))
