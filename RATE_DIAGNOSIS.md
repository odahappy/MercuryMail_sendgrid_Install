# MercuryMail 发送速率诊断与处置流程

## 结论摘要

任务 120/121 的 11～15 封/秒不是收件人总量不足导致。队列深度长期为 1800～7500 时仍只有 4～5 个活跃 Worker，结合约 10 秒的 Worker 取数等待，最可能的主瓶颈是 **有效并发未扩至满足 RTT 的前馈并发**，并伴随 Redis 协调不可用使 PID 扩容/Worker 计数失效。若 SendGrid/代理 HTTP RTT 约为 0.3～0.45 秒，4～5 个单封发送 Worker 的理论上限恰好约为 11～15 封/秒。

通道 45 的 401 不是任务 120/121 的直接限速器，但旧逻辑没有完整取得同通道任务且未立即停止 Runner，会让已暂停任务短时间继续占用 Worker、通道租约和 SenderPool，从而间接挤压其他任务。SenderPool 固定 24 Worker 在多任务和高 RTT 场景也会形成二级排队瓶颈。

## 根因优先级

1. **最可能：Worker/HTTP 并发不足。** 50 封/秒所需并发约为 `ceil(目标速率 × HTTP P95 延迟 / HTTP 批量大小) + 余量`。单封、P95 1 秒时至少需要约 50～60 个 HTTP 并发；4～5 个 Worker 无法达到目标。
2. **高概率：Redis 故障阻断 PID 扩容。** Worker 计数及跨实例槽位依赖 Redis。旧的本地降级会产生错误的可用槽位视图，并可能让扩缩容状态漂移。修复后短暂故障只冻结并发：保留既有 Worker，拒绝新增扩容，恢复后立即同步本实例槽位。
3. **高概率：SenderPool 容量/排队。** 多任务共用 24 个同步 SDK 执行线程时，HTTP RTT 上升会迅速耗尽线程。启动任务时现按全局活跃 Worker 加余量扩至至少 64，受 `sender_pool_max_workers`（默认 128）约束。
4. **中概率：通道槽位公平份额不足。** 多任务共享同一通道时按权重分配全局槽位；需观察 `channel_slot_wait_ms_p95`。该值持续大于 500ms 表示通道容量或代理连接池是瓶颈。
5. **中概率：预取间歇停滞。** 深度日志可能是任务级/全局快照，不能单凭深度排除某个 Worker 的队列空窗。重点观察 `prefetch_queue_empty_seconds`、领取 P95、连续空结果及完整性守卫。当前实现低于低水位持续补充，完整性守卫独立低频运行。
6. **低概率：数据库异常。** 给定日志无明显预取慢查询或数据库异常，因此不是首要方向，但仍需用领取 P95 和连接池水位验证。
7. **间接影响：通道 45 的 401 残留资源。** 修复后所有 `running/starting/pending/scheduled` 任务统一暂停，写入中文原因，停止活跃 Runner，并释放通道租约；健康恢复后只恢复由认证故障暂停的任务，沿持久化游标继续。

## 标准诊断流程

### 1. 确认速率口径

- 对比用户配置速率、任务独立限速器的有效速率、最近 10/30/60 秒成功数。
- 排除监控邮件、重试、退订和永久失败是否计入不同口径。
- 多任务分别计算，禁止把通道总速率误当单任务速率。

### 2. 计算理论并发

- 读取 SendGrid HTTP P50/P95/P99 与批量大小。
- 计算 `所需并发 = ceil(target_rps × http_p95_seconds / batch_size) + 20% 余量`。
- 对比用户并发、PID 目标、有效并发、活跃 Worker、HTTP inflight 和通道公平份额。
- 若活跃 Worker 明显低于所需并发，先查 Redis 扩容门禁、全局槽位和 PID gate。

### 3. 按流水线逐段定位

1. **预取段**：`prefetch_q`、领取查询 P95/P99、空队列持续秒数、每 10 秒领取次数/行数。
2. **Worker 段**：活跃 Worker、等待收件人 P95、局部批次数。队列深但等待高时检查是否读取了不同任务队列，或事件循环被同步工作阻塞。
3. **限速段**：令牌等待 P95、每任务 limiter 实例及其配置值。令牌等待接近目标节拍属于正常；远高于节拍说明 limiter 状态或公平份额异常。
4. **通道段**：通道槽位等待 P95/P99、租约份额、代理连接池上限。
5. **SenderPool 段**：队列等待 P95、队列深度、alive/inflight、投递未知数量。
6. **HTTP 段**：P50/P95/P99、429 比例、401/403、5xx、代理握手/连接复用。
7. **写入段**：结果队列、flush P99、数据库连接池。写入背压会反馈降低预取批次。

### 4. Redis 专项检查

- 检查连接池耗尽、服务端 `maxclients`、网络丢包、慢命令及 CPU。
- 应用池默认普通/锁/后台分别至少 64，连接与命令超时默认 2 秒，15 秒健康检查，超时重试。
- 每次关键协调操作最多立即尝试 3 次；失败后不得新增本地槽位，也不得缩减现有 Worker。
- 观察 `redis_coordination_mode=hold_existing_reject_scale_out`。恢复后应出现“任务 Worker 计数和本实例槽位已快速同步”。
- 如果故障持续，应修复 Redis 基础设施；应用不会用本地分配伪装可用性。

### 5. 401 专项检查

- 确认 SendGrid SDK 响应来源，避免把代理/WAF 伪造响应误判为账号错误。
- 对确认的 401/403，查询同通道所有可执行任务是否都变为 `paused`，`run_message` 是否为“通道认证失败，已自动暂停”。
- 确认 Runner、Worker 槽位、通道租约在清理周期内释放。
- 健康探测成功后，仅恢复该原因暂停的任务；核对 `last_claimed_row_number` 不回退，未提交批次由回滚保护重新领取。

## 50 封/秒验收标准

- 连续 10 分钟的 60 秒窗口速率达到目标的 95% 以上，且无持续 429。
- 活跃 Worker 不低于基于 HTTP P95 算出的需求，SenderPool 排队 P95 小于 100ms。
- 通道槽位等待 P95 小于 200ms；预取队列不出现超过 2 秒的非 EOF 空窗。
- Redis 短暂中断期间既有 HTTP inflight/Worker 数不下降；扩容被明确拒绝；恢复后 20 秒内完成槽位同步并重新允许 PID 扩容。
- 401 通道的全部关联任务均暂停且不再占用通道租约，恢复后从持久化游标继续。

## 生产配置建议

- `REDIS_MAX_CONNECTIONS=64`（多任务高峰可提高到 128）
- `REDIS_LOCK_MAX_CONNECTIONS=64`
- `REDIS_BACKGROUND_MAX_CONNECTIONS=64`
- `REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS=2`
- `REDIS_SOCKET_TIMEOUT_SECONDS=2`
- `REDIS_HEALTH_CHECK_INTERVAL_SECONDS=15`
- `SENDER_POOL_WORKERS` 留空以动态计算，或在确认 CPU/文件描述符容量后设为 64～128
- `SENDER_POOL_MAX_WORKERS=128`
- `TASK_HTTP_BATCH_CONCURRENCY_CAP=64`，高 RTT 且连接池充足时可提高

所有发送仍经项目的 SendGrid SDK 管理器执行；代理绑定、响应来源识别与投递未知保护未被绕过。

## 二次深度审计补充（2026-08-11）

二次复核发现并修复了四个隐藏问题：

1. **Redis 故障门禁此前只在一次 Worker 读取失败时生效。** 新任务仍可能重复等待 Redis，且其他 Redis 操作成功不会统一解除门禁。现已加入进程级协调可用状态：任一关键协调操作连续失败即冻结启动/扩容，任一关键操作成功即解除门禁并记录中文恢复日志。
2. **SenderPool 动态扩容没有同步扩大 asyncio 信号量。** 线程数虽从 24/32 增至 64，但旧信号量容量不变，实际并发仍可能卡在扩容前数值。现扩容时同步释放新增许可；这比单纯增加线程数更直接地解释“配置已提高但速率不变”。
3. **SenderPool 排队指标误把 HTTP 执行时间算作队列等待。** 原 `queue_wait_ms` 是整个 `run_sync` 耗时，会错误触发 PID 排队门禁。现由 SenderPool 在真正取得槽位时回传等待毫秒数，将排队和 HTTP RTT 分离。
4. **401 恢复存在旧 Runner 未退出即重启的竞态。** 健康探测可能在协作式清理完成前恢复任务，导致 `start_task_runner` 返回失败后 Redis 暂停标记却已被删除。现只在旧 Runner 已退出后恢复；仍待退出或启动失败的任务保留暂停标记和 `unauthorized` 状态，下一轮继续恢复。

另修正了两项诊断/语义问题：速率告警现在使用运行时有效速率（含通道公平份额），避免按原始配置产生假告警；通用 403 不再自动视为账号级鉴权失效，只有标准账号认证错误才暂停整个通道，发件人身份或 Mail Send scope 配置错误仍按任务级配置错误处理。

## 三次深挖：扩容阻断链路（2026-08-11）

本轮逐段审计了“提出扩容 → 目标裁剪 → 取得全局槽位 → 创建 Worker → SDK/通道实际并发”的完整链路，确认并修复如下高风险阻断点：

1. **PID 最大并发曾被容量预检值反向锁死。** `verify_task_capacity()` 的 `required_http_inflight` 是按 `rate / batch × RTT × 1.5` 的最低 HTTP 在途估计，不是扩容上限。它被用于 `create_task_pid_controller(max_concurrency=...)` 后，PID 即使发现 HTTP P95 上升或实际速率不足，也不能提出更高目标。现在 PID 上限改用任务硬上限（受全局上限、任务配置和 HTTP 管线保护约束），且至少不低于任务已分配的初始并发和前馈下限。
2. **PID 调度协程错误依赖 `auto_concurrency` 开关。** 即便启用 `ENABLE_PID_CONCURRENCY_TUNING`，固定并发任务不会创建自适应协程，导致在启动 RTT 估计偏低、运行期 HTTP 延迟变高时完全无法补偿。现在只要 PID 功能开关启用，所有任务都会运行 PID；固定模式仍保留任务配置作为硬上限，自动模式可使用计算出的安全硬上限。
3. **自动模式的 RTT 动态扩容仍被表单初始并发限制。** 自动模式默认表单并发常为 1，旧路径将其作为 `configured_cap`，动态 RTT 探测即使识别到更高延迟也无法增长。现在自动模式使用 `calculate_task_concurrency_hard_cap()`，固定模式才严格使用用户值上限。
4. **扩容目标裁剪可能把目标提升到高于硬上限的前馈下限。** 旧的 `max(floor, min(target, hard_cap))` 在 floor 大于 hard cap 时会越过硬上限；随后失败难以解释。现改为先保证不突破硬上限，并明确记录“目标速率所需并发下限超过当前硬上限”的中文容量阻断原因。
5. **PID 压力评分把未采样资源当作 0 压力。** 单个资源已满载时，平均到六个固定项会被稀释，导致错误扩容；反向看，排队指标异常时也难以定位。现只对真正存在采样值的资源评分，未采样项目不参与平均。
6. **扩容失败没有统一暴露最终原因。** 现在运行时指标增加 `pid_proposed_concurrency`、`pid_proposed_reason`、`pid_last_no_scale_reason` 与 `concurrency_capacity_block_reason`，可区分 PID 未提议、硬上限、Redis 门禁、全局槽位不足和 Worker 未实际创建。

### 扩容门禁检查顺序

1. 运行时有效速率是否低于 `rate_limit_per_second`；注意它已包含同通道任务的公平份额。
2. PID 是否提出 `pid_proposed_concurrency`；没有时查看 `pid_last_no_scale_reason`。
3. 目标是否被任务硬上限、全局上限或容量下限冲突裁剪；查看 `concurrency_capacity_block_reason`。
4. Redis 协调是否可用；不可用时只保留现有 Worker，绝不本地扩容。
5. Redis Lua 全局槽位是否分配成功；失败查看 `pid_scale_up_last_error`。
6. Worker supervisor 是否实际创建对应数目的 Task；比较 `admitted_workers`、`effective_concurrency` 和 `pid_proposed_concurrency`。
7. SenderPool 线程和信号量是否不小于活跃 SDK 并发；检查 `sender_pool_workers`、`sender_pool_alive_workers`、`sender_pool_inflight`、`sender_pool_queue_wait_ms_p95`。
8. 通道槽位是否成为真实瓶颈；检查 `channel_send_slot_parallel` 与 `channel_slot_wait_ms_p95`。

对于 50 封/秒单封任务，初始 RTT 估计不足不是安全理由去永久限制扩容：应由上述门禁逐层说明具体资源不足，而不是静默维持在 4～5 个 Worker。
