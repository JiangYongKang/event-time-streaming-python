# 事件时间与水位线语义说明

本文档说明本服务的核心概念、处理规则、拒绝原因与可恢复性保证，并给出本地验证步骤。

## 1. 概念

- **事件时间（event time）**：事件负载自带的时间戳 `event_time`（epoch 毫秒），
  与到达服务的处理时间（ingestion processing time）无关。所有窗口归属都以事件时间为准。
- **滚动窗口（tumbling window）**：固定大小、不重叠。窗口分配为
  `start = floor(event_time / window_size_ms) * window_size_ms`，
  区间为左闭右开 **`[start, start + window_size_ms)`**。
- **水位线（watermark, 记作 `w`）**：一个“不再期待更早事件”的进度断言。它有两种推进方式：
  - 启发式：`w = max_observed_event_time - STREAM_MAX_OOO_MS`，
    即默认容忍 `STREAM_MAX_OOO_MS` 的乱序；
  - 显式：`POST /watermark {"watermark": W}`（可由上游 source / 定时器 / 测试驱动）。
- **允许迟到时间（allowed lateness, 记作 `L`）**：窗口关闭后，迟到事件仍可纠正结果的额外时长。

## 2. 窗口生命周期

对窗口右边界（exclusive end）`we` 与当前水位线 `w`：

| 条件 | 状态 | 行为 |
| --- | --- | --- |
| `w < we` | **OPEN** | 正常聚合 |
| `we <= w <= we + L` | **CLOSED** | 已最终化并写入 `outputs`；迟到事件仍接受，结果被**纠正**（同一事务刷新 `outputs`） |
| `w > we + L` | **PURGED** | 工作状态被清除；迟到事件以 `LATE_EVENT_TOO_OLD` 拒绝；已最终化 `outputs` 永久保留 |

- 水位线**越过边界**（`w >= we`）时窗口即关闭，并持久化一条最终化输出。
- 边界取值约定：`w == we` 表示窗口关闭（该时间点及以后不再作为正常数据），
  但在 `w <= we + L` 期间仍接受迟到纠正；`w == we + L` 仍接受，`w == we + L + 1` 起拒绝。

## 3. 水位线单调性与回退

- 显式请求将水位线设为**小于当前值**时，抛出 `WATERMARK_REGRESSION`（HTTP 409），
  状态保持不变；设为**相等值**是幂等 no-op。
- 启发式水位线只增不减；晚到的乱序事件不会把水位线往回拉。
- 每次推进都在处理事务中与窗口关闭/清理一起原子持久化（`kv` 表），
  因此重启后水位线不变，回退在重启后同样被拒绝。

## 4. 迟到与重复数据处理规则

### 4.1 迟到事件

1. 先以“该事件影响水位线之前”的当前水位线 `old_wm` 判定其窗口状态；
2. PURGED（`old_wm > we + L`）→ 逐事件 **拒绝**，状态 `REJECTED`，
   `reject_code=LATE_EVENT_TOO_OLD`，消息含 `event_time/window_end/allowed_lateness/watermark`，
   该事件**不**写入去重表、**不**计数；
3. OPEN / CLOSED → **接受**。其中 CLOSED 表示这是一次迟到纠正：
   更新窗口并在**同一事务**内刷新已最终化的 `outputs`。

注意启发式水位线会随较大的事件时间前移，因此可接受的“先到晩时间戳、后到早时间戳”
幅度受 `STREAM_MAX_OOO_MS + STREAM_ALLOWED_LATENESS_MS` 约束；
若希望完全手动控制，可把 `STREAM_MAX_OOO_MS=0` 并用 `/watermark` 显式推进。

### 4.2 重复投递（幂等）

- 每个被**接受**的 `event_id` 在聚合同一事务内写入 `events` 表（主键即 `event_id`）。
- 再次到达（HTTP 重投、批次内重复、重启后重放）返回 `DUPLICATE`，绝不第二次计数；
  即使重复请求携带不同 payload，也以首次确认为准。
- 被拒绝（迟到超限）的事件不会被记住为“已接受”，再次投递仍按迟到规则重新判定。

### 4.3 批次原子性

- `POST /events/batch` 的整批在**单个 SQLite 事务**内处理：
  去重判定、计数、水位线推进、窗口关闭/清理、最终化输出全部一起提交。
- 批次内重复逐元素折叠为 `DUPLICATE`，不会导致整批失败；
  处理过程中任何异常都会 `ROLLBACK`，不留部分结果
  （见 `tests/test_dedup_recovery.py::test_batch_failure_leaves_no_partial_result`）。

### 4.4 重启恢复

- 提交点：`PRAGMA journal_mode=WAL` + `PRAGMA synchronous=FULL`。
- 重启后从 `kv` 读回水位线、从 `windows` 读回工作窗口、从 `outputs` 读回已确认结果。
- 已确认结果在重启后保持一致；重启后重放历史批次全部命中 `DUPLICATE`。

## 5. 有界缓冲、并发与背压

- 单个有界 `queue.Queue(maxsize=STREAM_QUEUE_CAPACITY)` + 单个工作线程。
- 所有持久化状态变更在工作线程上、持 SQLite 事务锁执行，因此同键并发更新串行化，
  不会因竞态丢失计数（`tests/test_concurrency_backpressure.py` 用 8 线程验证）。
- 摄入分两阶段，错误原因可区分：
  1. **非阻塞入队**：缓冲区满立即抛 `QUEUE_FULL`（HTTP 503，背压，请稍后重试），
     调用方不会被无限挂起，内存占用以队列容量为上界；
  2. **限时等待处理完成**：已入队但未在 `STREAM_PROCESSING_TIMEOUT_MS` 内完成 →
     `PROCESSING_TIMEOUT`（HTTP 503）。
- `/state/queue` 暴露当前队列大小、容量、累计入队数与累计背压拒绝数。

## 6. 拒绝原因码汇总

| code | 触发条件 | HTTP |
| --- | --- | --- |
| `INVALID_EVENT` | 空 event_id/key、负 event_time，或请求体校验失败 | 400 / 422 |
| `WATERMARK_REGRESSION` | 显式水位线小于当前值 | 409 |
| `LATE_EVENT_TOO_OLD` | 事件晚于 `window_end + allowed_lateness` | 409（批次内逐事件返回，不拖垮整批） |
| `QUEUE_FULL` | 有界摄取队列已满 | 503 |
| `PROCESSING_TIMEOUT` | 已入队但处理超时 | 503 |

## 7. 本地验证步骤

```bash
uv sync --group dev
uv run pytest -s                       # 打印水位线、窗口区间与拒绝原因
```

手工端到端：

```bash
STREAM_WINDOW_SIZE_MS=10000 STREAM_ALLOWED_LATENESS_MS=5000 \
  uv run uvicorn main:app --port 8000

# 1) 正常 + 乱序
curl -s -X POST localhost:8000/events -d '{"event_id":"1","key":"k","event_time":8000}' -H 'content-type: application/json'
curl -s -X POST localhost:8000/events -d '{"event_id":"2","key":"k","event_time":1000}' -H 'content-type: application/json'
# 2) 推进水位线关闭 [0,10000)
curl -s -X POST localhost:8000/watermark -d '{"watermark":10000}' -H 'content-type: application/json'
# 3) 回退 -> 409 WATERMARK_REGRESSION
curl -s -i -X POST localhost:8000/watermark -d '{"watermark":9000}' -H 'content-type: application/json'
# 4) 迟到但在容忍内 -> 接受并纠正
curl -s -X POST localhost:8000/events -d '{"event_id":"3","key":"k","event_time":2000}' -H 'content-type: application/json'
# 5) 越过迟到上限后再迟到 -> 批次内 LATE_EVENT_TOO_OLD
curl -s -X POST localhost:8000/watermark -d '{"watermark":15001}' -H 'content-type: application/json'
curl -s -X POST localhost:8000/events -d '{"event_id":"4","key":"k","event_time":500}' -H 'content-type: application/json'
# 6) 查看最终结果
curl -s localhost:8000/state/outputs
```

复现背压（小容量 + 人工处理延迟）：

```bash
STREAM_QUEUE_CAPACITY=1 STREAM_SIMULATED_PROCESS_DELAY_MS=300 \
  uv run uvicorn main:app --port 8000
# 并发打几条请求即可观察到 503 QUEUE_FULL
```
