# MercuryMail 最终架构验证计划

## 前置条件

1. 执行 `compact_outcomes`、`task_stats_snapshots` 和唯一索引迁移。
2. 为测试任务准备至少 100 万个有效且互不重复的收件人。
3. 固定 `SEND_HTTP_BATCH_SIZE=50`，关闭运行中批次自动修改逻辑。
4. 记录容器、PostgreSQL、Redis、代理和 SendGrid 基线指标。

```bash
docker compose up -d
docker compose ps
docker stats --no-stream
psql "$DATABASE_URL" -c "SELECT indexname FROM pg_indexes WHERE indexname IN ('ux_compact_outcomes_task_recipient','ux_task_stats_snapshots_task_id');"
```

## 通用观测命令

```bash
docker compose logs -f app | grep -E "任务状态快照|CompletionAuditor|最小结果账本|详细日志队列|429|PID|批次"
```

```sql
SELECT task_id, status, current_rate, target_rate, success_count, failed_count,
       admitted_workers, active_http_inflight, proxy_rtt_ms, updated_at
FROM task_stats_snapshots
ORDER BY task_id;
```

```sql
SELECT task_id,
       COUNT(*) FILTER (WHERE status = 'success' AND log_type = 'regular') AS success_count,
       COUNT(*) FILTER (WHERE status IN ('failed','bounced') AND log_type = 'regular') AS failed_count
FROM compact_outcomes
GROUP BY task_id
ORDER BY task_id;
```

## 一、单任务稳定测试

配置：目标 50 封/秒，HTTP 批次 50，持续 30 分钟。任务收件人不少于 100,000。

```bash
watch -n 10 'curl -fsS http://localhost/api/dashboard/stats | jq ".task_rate_comparison"'
```

每 10 秒导出一次 `task_stats_snapshots.current_rate`，计算全部窗口的 P50 与 P95。

验收：

- 10 秒窗口速率 P50 不低于 49/s。
- 10 秒窗口速率 P95 不低于 48/s。
- 除最后一批外，发送批次均为 50。
- 前 10 分钟与后 10 分钟平均速率差异不超过 3%。
- `compact_outcomes` 记录数等于任务成功数加失败数。
- 日志不存在运行中批次变更、Writer 控制任务失败或预取大表扫描。

预期日志：

```text
任务 <id> Writer 已启动：最小结果账本队列容量=100000，详细日志队列容量=500000
任务 <id> keyset 预取与 CompletionAuditor 已启动
任务 <id> CompletionAuditor 确认完成
```

## 二、多任务隔离测试

同时启动：

- A：健康代理，50/s。
- B：注入高延迟代理，50/s。
- C：50/s，代理中断 20 秒后恢复。

```bash
curl -X POST http://localhost/api/tasks/A/start
curl -X POST http://localhost/api/tasks/B/start
curl -X POST http://localhost/api/tasks/C/start
```

代理故障注入使用测试代理或网络规则；恢复后立即持续观测快照。

验收：

- A 的 10 秒窗口 P50 不低于 49/s，故障期间下降不超过 5%。
- B 的 PID 自动降低 `admitted_workers`，进程和任务不崩溃。
- C 中断期间速率允许为 0；恢复后 60 秒内重新接近目标速率。
- C 的 `compact_outcomes(task_id, recipient_id)` 无重复。
- 三个任务的固定批次配置互不改变。

重复检查：

```sql
SELECT task_id, recipient_id, COUNT(*)
FROM compact_outcomes
WHERE task_id IN (:a, :b, :c) AND log_type = 'regular'
GROUP BY task_id, recipient_id
HAVING COUNT(*) > 1;
```

预期返回 0 行。

## 三、Redis 故障测试

任务稳定运行后停止 Redis 60 秒：

```bash
docker compose stop redis
sleep 60
docker compose start redis
```

验收：

- 所有发送任务持续运行。
- PID 根据本地 runtime 正常调节。
- 不因 Redis 异常暂停、失败或降低目标速率。
- Redis 恢复后缓存自动更新。
- 允许出现限频中文缓存告警，不允许出现“全局槽位不足”或“Redis 许可失败”。

## 四、数据库与仪表盘压力测试

运行发送任务时并发请求仪表盘：

```bash
seq 1 10000 | xargs -P 100 -I{} curl -fsS -o /dev/null http://localhost/api/dashboard/stats
```

同时观察 PostgreSQL：

```sql
SELECT state, wait_event_type, wait_event, COUNT(*)
FROM pg_stat_activity
WHERE datname = current_database()
GROUP BY state, wait_event_type, wait_event;
```

确认仪表盘SQL：

```sql
SELECT query, calls, total_exec_time
FROM pg_stat_statements
WHERE query ILIKE '%send_logs%'
  AND query ILIKE '%dashboard%'
ORDER BY total_exec_time DESC;
```

验收：

- 仪表盘API只读内存缓存；后台刷新只查 `tasks`、`task_stats_snapshots`、通道和代理小表。
- 不出现 `send_logs`、`send_log_archive` 或 `recipients` 的仪表盘聚合。
- 发送速率相对无仪表盘压力基线下降不超过 3%。
- 数据库连接池无耗尽，API无5xx突增。
- 快照缺失时返回空数据或上一次缓存，不触发实时聚合。

## 五、长时间稳定性测试

3 个任务持续运行 2 小时，每 60 秒采样：

```bash
while true; do
  date -Is
  docker stats --no-stream
  curl -fsS http://localhost/api/dashboard/stats | jq '{rate:.global_current_rate,workers:.admitted_workers,http:.active_http_inflight}'
  sleep 60
done | tee stability-2h.log
```

容器内FD检查：

```bash
docker compose exec app sh -c 'ls /proc/1/fd | wc -l'
```

验收：

- RSS在预热后无持续线性增长；最后30分钟与稳定期基线差异不超过10%。
- FD无持续增长，波动后能回落。
- PostgreSQL连接数无持续增长，不超过配置池上限。
- 每个任务后半小时平均速率相对前半小时下降不超过3%。
- 关键账本队列无丢弃；详细日志满时只出现每60秒一次的中文告警。

## 六、异常与恢复补充测试

发送中重启应用：

```bash
docker compose restart app
```

验收：

- 从持久游标和 `compact_outcomes` 恢复。
- 唯一索引防止重复账本记录。
- CompletionAuditor发现缺口后只补发缺口。
- 任务最终进入 `finished`。

最终一致性：

```sql
SELECT t.id, t.sent_count, t.failed_count,
       COUNT(*) FILTER (WHERE o.status = 'success' AND o.log_type = 'regular') AS ledger_success,
       COUNT(*) FILTER (WHERE o.status IN ('failed','bounced') AND o.log_type = 'regular') AS ledger_failed
FROM tasks t
LEFT JOIN compact_outcomes o ON o.task_id = t.id
WHERE t.id IN (:task_ids)
GROUP BY t.id, t.sent_count, t.failed_count;
```

每个任务必须满足 `sent_count = ledger_success` 且 `failed_count = ledger_failed`。
