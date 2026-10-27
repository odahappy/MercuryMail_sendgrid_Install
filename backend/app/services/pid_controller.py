"""发送并发控制器。

控制策略由三层组成：任务级 GCRA 令牌限速、基于吞吐的自适应 PID、以及
资源压力的柔性门禁。PID 只决定期望并发，实际并发由任务本地动态并发限制器执行。
"""

from __future__ import annotations

import logging
import math
import os
from collections import deque
from dataclasses import dataclass, field
from time import monotonic

logger = logging.getLogger(__name__)


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


PID_INTERVAL_SECONDS = max(1.0, _float_env("PID_INTERVAL_SECONDS", 5.0))
PID_HISTORY_MAX_POINTS = 240
PID_KP = _float_env("PID_KP", 0.55)
PID_KI = _float_env("PID_KI", 0.08)
PID_KD = _float_env("PID_KD", 0.12)
PID_INTEGRAL_CLAMP = max(1.0, _float_env("PID_INTEGRAL_CLAMP", 120.0))
PID_DEAD_BAND_RATIO = max(0.0, _float_env("PID_DEAD_BAND_RATIO", 0.015))
PID_MAX_STEP_RATIO = max(0.05, _float_env("PID_MAX_STEP_RATIO", 0.30))
PID_MIN_STEP_LIMIT = max(1, _int_env("PID_MIN_STEP_LIMIT", 1))
PID_MIN_CONCURRENCY = max(1, _int_env("PID_MIN_CONCURRENCY", 2))
PID_429_REDUCE_RATIO = min(0.95, max(0.20, _float_env("PID_429_REDUCE_RATIO", 0.70)))
PID_429_COOLDOWN_SECONDS = max(1.0, _float_env("PID_429_COOLDOWN_SECONDS", 8.0))
PID_DOWNSCALE_COOLDOWN_SECONDS = max(1.0, _float_env("PID_DOWNSCALE_COOLDOWN_SECONDS", 8.0))
PID_RAMP_UP_MAX_STEP = max(1, _int_env("PID_RAMP_UP_MAX_STEP", 12))
PID_NON_429_DOWNSCALE_MAX_STEP = max(1, _int_env("PID_NON_429_DOWNSCALE_MAX_STEP", 8))
PID_OUTLIER_HIGH_RATIO = max(1.2, _float_env("PID_OUTLIER_HIGH_RATIO", 2.5))
PID_FILTER_WINDOW_POINTS = max(2, _int_env("PID_FILTER_WINDOW_POINTS", 5))
PID_GATE_WRITE_QUEUE_DEPTH = max(1000, _int_env("PID_GATE_WRITE_QUEUE_DEPTH", 30000))

PREFETCH_QUEUE_LOW_THRESHOLD = 2000
PREFETCH_QUEUE_RECOVER_THRESHOLD = 10000
PREFETCH_LOW_CYCLES_REQUIRED = 2
PREFETCH_RATE_GAP_RATIO = 0.98
PREFETCH_LIMIT_BOOST_RATIO = 4.0
PREFETCH_LIMIT_MAX_MULTIPLIER = 8.0
WRITE_QUEUE_HIGH_WATERMARK = 10000
WRITE_QUEUE_CRITICAL_WATERMARK = 30000
WRITE_QUEUE_RECOVER_WATERMARK = 5000
WRITE_BACKLOG_REDUCE_RATIO = 0.75
PID_EXTREME_SLOT_WAIT_P95_MS = 5000.0


@dataclass
class PidSample:
    timestamp: float
    target_rate: float
    actual_rate: float
    concurrency: int
    output_delta: float
    reason: str = ""


@dataclass
class QueueAwareConcurrencyState:
    prefetch_low_cycles: int = 0
    prefetch_limit_boosted: bool = False
    write_backlog_active: bool = False
    write_backlog_critical: bool = False
    pressure_score: float = 0.0
    emergency_cycles: int = 0
    latency_guard_active: bool = False
    latency_recovery_cycles: int = 0
    near_target_cycles: int = 0
    under_target_cycles: int = 0
    reservation_saturated_cycles: int = 0
    last_feedforward_target: int = 0


@dataclass
class IncrementalPidController:
    kp: float = PID_KP
    ki: float = PID_KI
    kd: float = PID_KD
    min_concurrency: int = PID_MIN_CONCURRENCY
    max_concurrency: int = 1000
    _integral: float = 0.0
    _previous_error: float = 0.0
    _last_eval_at: float = 0.0
    _last_update_at: float = 0.0
    _last_valid_actual_rate: float = 0.0
    _recent_valid_rates: deque[float] = field(default_factory=lambda: deque(maxlen=PID_FILTER_WINDOW_POINTS))
    _history: deque[PidSample] = field(default_factory=lambda: deque(maxlen=PID_HISTORY_MAX_POINTS))
    _last_429_downscale_at: float = 0.0
    _last_downscale_at: float = 0.0
    _emergency_cycles: int = 0

    def update_gains(self, *, kp: float | None = None, ki: float | None = None, kd: float | None = None) -> None:
        if kp is not None:
            self.kp = max(0.0, float(kp))
        if ki is not None:
            self.ki = max(0.0, float(ki))
        if kd is not None:
            self.kd = max(0.0, float(kd))

    def record_429_event(self) -> None:
        self._integral *= 0.25
        self._previous_error = 0.0
        self._last_429_downscale_at = monotonic()

    def evaluate(self, *, target_rate: float, actual_rate: float, current_concurrency: int, recent_429_count: int = 0, force: bool = False, task_id: int | None = None) -> tuple[int, str] | None:
        now = monotonic()
        if not force and self._last_eval_at and now - self._last_eval_at < PID_INTERVAL_SECONDS:
            return None
        self._last_eval_at = now
        target = max(0.0, float(target_rate or 0))
        current = max(self.min_concurrency, min(self.max_concurrency, int(current_concurrency or self.min_concurrency)))
        if target <= 0:
            return None
        actual = self._filter_actual_rate(target, actual_rate)
        if (
            recent_429_count > 0
            and now - self._last_429_downscale_at >= PID_429_COOLDOWN_SECONDS
        ):
            reduced = max(
                self.min_concurrency,
                math.floor(current * PID_429_REDUCE_RATIO),
            )
            self.record_429_event()
            if reduced < current:
                reason = (
                    f"检测到服务端限流，安全降低并发 {current} -> {reduced}"
                    f"（近期限流={recent_429_count}）"
                )
                return self._result(
                    now,
                    target,
                    actual,
                    current,
                    reduced,
                    reason,
                    task_id,
                )
        error = target - actual
        error_ratio = error / max(target, 1.0)
        dead_band = target * PID_DEAD_BAND_RATIO
        if abs(error) <= dead_band:
            self._integral *= 0.9
            self._previous_error = error
            return None
        dt = max(0.25, now - self._last_update_at) if self._last_update_at else max(0.25, PID_INTERVAL_SECONDS)
        derivative = (error - self._previous_error) / dt
        candidate_integral = max(-PID_INTEGRAL_CLAMP, min(PID_INTEGRAL_CLAMP, self._integral + error * dt))
        raw_delta = self.kp * error_ratio * max(1.0, current) + self.ki * candidate_integral / max(target, 1.0) + self.kd * derivative / max(target, 1.0)
        max_step = max(PID_MIN_STEP_LIMIT, math.ceil(max(1, current) * PID_MAX_STEP_RATIO))
        if error > 0:
            delta = min(max_step, PID_RAMP_UP_MAX_STEP, max(1, math.ceil(raw_delta)))
            if error_ratio > 0.35:
                self._emergency_cycles += 1
                delta = min(max_step, max(delta, min(PID_RAMP_UP_MAX_STEP, 2 + self._emergency_cycles)))
            else:
                self._emergency_cycles = 0
        else:
            self._emergency_cycles = 0
            if now - self._last_downscale_at < PID_DOWNSCALE_COOLDOWN_SECONDS:
                self._previous_error = error
                return None
            delta = max(-PID_NON_429_DOWNSCALE_MAX_STEP, min(-1, math.floor(raw_delta)))
            self._last_downscale_at = now
        proposed = current + delta
        saturated = proposed <= self.min_concurrency or proposed >= self.max_concurrency
        if not saturated or (error * delta < 0):
            self._integral = candidate_integral
        else:
            self._integral *= 0.8
        self._previous_error = error
        self._last_update_at = now
        proposed = max(self.min_concurrency, min(self.max_concurrency, proposed))
        reason = f"自适应 PID：目标={target:.1f}，实际={actual:.1f}，误差={error:+.1f}，并发 {current} -> {proposed}"
        return self._result(now, target, actual, current, proposed, reason, task_id)

    def _result(self, now: float, target: float, actual: float, current: int, proposed: int, reason: str, task_id: int | None) -> tuple[int, str] | None:
        self._append_history(now, target, actual, proposed, proposed - current, reason)
        if proposed == current:
            return None
        logger.info("PID 调节 task_id=%s %s", task_id or "", reason)
        return proposed, reason

    def _filter_actual_rate(self, target: float, actual_rate: float) -> float:
        actual = max(0.0, float(actual_rate or 0))
        if actual > target * PID_OUTLIER_HIGH_RATIO and self._last_valid_actual_rate > 0:
            actual = self._last_valid_actual_rate
        self._last_valid_actual_rate = actual
        self._recent_valid_rates.append(actual)
        ordered = sorted(self._recent_valid_rates)
        return ordered[len(ordered) // 2] if len(ordered) >= 3 else actual

    def _append_history(self, timestamp: float, target: float, actual: float, concurrency: int, output_delta: float, reason: str) -> None:
        self._history.append(PidSample(round(timestamp, 3), round(target, 2), round(actual, 2), int(concurrency), round(output_delta, 2), reason))

    def history_points(self, limit: int = 60) -> list[dict[str, object]]:
        return [sample.__dict__.copy() for sample in list(self._history)[-max(1, int(limit)):]]

    def latest_reason(self) -> str:
        return str(self._history[-1].reason or "") if self._history else ""


def apply_queue_aware_concurrency_adjustment(*, tune_result: tuple[int, str] | None, current_concurrency: int, target_rate: float, actual_rate: float, prefetch_queue_depth: int, write_queue_depth: int, state: QueueAwareConcurrencyState, base_prefetch_limit: int, enabled: bool = True) -> tuple[tuple[int, str] | None, QueueAwareConcurrencyState, int]:
    base = max(1, int(base_prefetch_limit or 1))
    effective_limit = min(int(base * PREFETCH_LIMIT_MAX_MULTIPLIER), int(base * (PREFETCH_LIMIT_BOOST_RATIO if state.prefetch_limit_boosted else 1)))
    if not enabled:
        return tune_result, state, effective_limit
    queue_depth = max(0, int(prefetch_queue_depth or 0))
    write_depth = max(0, int(write_queue_depth or 0))
    target = max(0.0, float(target_rate or 0))
    actual = max(0.0, float(actual_rate or 0))
    if write_depth >= WRITE_QUEUE_CRITICAL_WATERMARK:
        state.write_backlog_critical = True
        state.write_backlog_active = True
        state.pressure_score = min(1.0, state.pressure_score + 0.25)
        reduced = max(
            PID_MIN_CONCURRENCY,
            math.floor(int(current_concurrency) * WRITE_BACKLOG_REDUCE_RATIO),
        )
        return (
            (
                (reduced, f"写入队列严重积压（深度={write_depth}），降低并发")
                if reduced < current_concurrency
                else None
            ),
            state,
            effective_limit,
        )
    if write_depth >= WRITE_QUEUE_HIGH_WATERMARK:
        state.write_backlog_active = True
    elif write_depth <= WRITE_QUEUE_RECOVER_WATERMARK:
        state.write_backlog_active = False
    state.write_backlog_critical = False
    state.prefetch_low_cycles = state.prefetch_low_cycles + 1 if queue_depth < PREFETCH_QUEUE_LOW_THRESHOLD and actual < target * PREFETCH_RATE_GAP_RATIO else 0
    if state.prefetch_low_cycles >= PREFETCH_LOW_CYCLES_REQUIRED:
        state.prefetch_limit_boosted = True
        effective_limit = min(int(base * PREFETCH_LIMIT_MAX_MULTIPLIER), int(base * PREFETCH_LIMIT_BOOST_RATIO))
    elif queue_depth > PREFETCH_QUEUE_RECOVER_THRESHOLD:
        state.prefetch_limit_boosted = False
    state.pressure_score = max(0.0, state.pressure_score * 0.7)
    return tune_result, state, effective_limit


def refine_pid_concurrency_target(*, pid_result: tuple[int, str] | None, current_concurrency: int, target_rate: float, actual_rate: float, prefetch_queue_depth: int, write_queue_depth: int, queue_state: QueueAwareConcurrencyState, base_prefetch_limit: int, send_http_batch_size: int = 50, sender_queue_wait_p95_ms: float = 0.0, db_claim_p95_ms: float = 0.0, local_concurrency_wait_p95_ms: float = 0.0, http_p95_ms: float = 0.0, rate_limiter_reserved_ahead_seconds: float = 0.0, sender_alive_workers: int = 0, sender_workers: int = 0, task_id: int | None = None, enable_queue_aware: bool = True) -> tuple[tuple[int, str] | None, QueueAwareConcurrencyState, int]:
    current = max(PID_MIN_CONCURRENCY, int(current_concurrency))
    http_p95 = max(0.0, float(http_p95_ms or 0.0))
    slot_p95 = max(0.0, float(local_concurrency_wait_p95_ms or 0.0))
    if slot_p95 > PID_EXTREME_SLOT_WAIT_P95_MS:
        queue_state.latency_guard_active = True
        queue_state.latency_recovery_cycles = 0
        logger.warning(
            "任务 %s 本地并发许可等待偏高（P95=%.0fms，HTTP P95=%.0fms），该等待说明并发已饱和，不阻止 Worker 扩容",
            task_id or "",
            slot_p95,
            http_p95,
        )
    else:
        queue_state.latency_guard_active = False
        queue_state.latency_recovery_cycles = 0
    result, state, limit = apply_queue_aware_concurrency_adjustment(tune_result=pid_result, current_concurrency=current_concurrency, target_rate=target_rate, actual_rate=actual_rate, prefetch_queue_depth=prefetch_queue_depth, write_queue_depth=write_queue_depth, state=queue_state, base_prefetch_limit=base_prefetch_limit, enabled=enable_queue_aware)
    target = max(0.0, float(target_rate or 0.0))
    actual = max(0.0, float(actual_rate or 0.0))
    reserved_ahead = max(0.0, float(rate_limiter_reserved_ahead_seconds or 0.0))
    # 前馈并发由目标速率和 HTTP P95 直接估算，PID 只修正模型误差，避免长期只靠积分扩容。
    feedforward_target = max(PID_MIN_CONCURRENCY, math.ceil(target * max(0.05, http_p95 / 1000.0) * 1.20)) if target > 0 else PID_MIN_CONCURRENCY
    state.last_feedforward_target = feedforward_target
    near_target = target > 0 and actual >= target * 0.94
    state.near_target_cycles = state.near_target_cycles + 1 if near_target else 0
    state.under_target_cycles = state.under_target_cycles + 1 if target > 0 and actual < target * 0.90 else 0
    reservation_saturated = reserved_ahead >= 3.0
    state.reservation_saturated_cycles = state.reservation_saturated_cycles + 1 if reservation_saturated else 0
    if state.near_target_cycles >= 3 and current > feedforward_target + 2:
        proposed = max(feedforward_target, current - min(PID_NON_429_DOWNSCALE_MAX_STEP, max(1, math.ceil(current * 0.10))))
        result = (proposed, f"稳态回收：连续接近目标且当前并发高于前馈需求 {feedforward_target}，并发 {current} -> {proposed}")
    elif state.reservation_saturated_cycles >= 2 and current > feedforward_target and actual >= target * 0.85:
        proposed = max(feedforward_target, current - max(1, math.ceil(current * 0.08)))
        result = (proposed, f"预约队列回收：限速预约 {reserved_ahead:.1f}s，降低过量并发 {current} -> {proposed}")
    elif result is not None and result[0] > current and reservation_saturated:
        result = None
    minimum_upscale_depth = max(1, current * max(1, int(send_http_batch_size or 1)) * 2)
    if result is not None and result[0] > current and int(prefetch_queue_depth or 0) <= minimum_upscale_depth:
        result = None
    if result is None and target_rate > 0 and actual_rate < target_rate * 0.80 and not state.write_backlog_critical and int(prefetch_queue_depth or 0) > minimum_upscale_depth:
        state.emergency_cycles += 1
        if state.emergency_cycles >= 3:
            proposed = max(
                current_concurrency + 2,
                math.ceil(current_concurrency * 1.20),
            )
            if proposed > current_concurrency:
                result = (
                    proposed,
                    f"应急模式：连续 {state.emergency_cycles} 个调节周期低于目标80%，并发 {current_concurrency} -> {proposed}",
                )
                state.emergency_cycles = 0
    elif result is not None and actual_rate >= target_rate * 0.80:
        state.emergency_cycles = 0
    if result is None:
        return None, state, limit
    proposed, reason = result
    current = int(current_concurrency)
    if proposed > current:
        # 只计算实际活跃的压力源；未启用/未采样的 0 值不是“无压力”样本，
        # 否则单个满载 SenderPool 会被五个不存在的压力源稀释而继续无效扩容。
        pressures: list[float] = []
        if prefetch_queue_depth < 1000 and float(db_claim_p95_ms or 0) > 0:
            pressures.append(min(1.0, float(db_claim_p95_ms) / 5000.0))
        if write_queue_depth > 0:
            pressures.append(min(1.0, float(write_queue_depth) / PID_GATE_WRITE_QUEUE_DEPTH))
        if sender_workers and sender_alive_workers < sender_workers:
            pressures.append(1.0)
        score = (sum(pressures) / len(pressures)) if pressures else 0.0
        state.pressure_score = score
        severe_rate_gap = target_rate > 0 and actual_rate < target_rate * 0.80
        if score >= 0.85:
            if severe_rate_gap:
                proposed = min(proposed, current + 2)
                reason = f"应急模式：实际速率低于目标80%且无429，小幅试探增加2并发；{reason}"
                state.emergency_cycles += 1
            else:
                return None, state, limit
        elif score > 0.25:
            step = max(1, math.ceil((proposed - current) * (1.0 - score)))
            proposed = min(proposed, current + step)
            reason = f"柔性资源门禁 score={score:.2f}，小步试探；{reason}"
    return (proposed, reason), state, limit


def create_task_pid_controller(*, max_concurrency: int) -> IncrementalPidController:
    return IncrementalPidController(max_concurrency=max(PID_MIN_CONCURRENCY, int(max_concurrency)), min_concurrency=PID_MIN_CONCURRENCY)
