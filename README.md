# 事件时间流式聚合服务（event-time-streaming-python）

一个可本地验证的事件时间（event-time）流式聚合服务，基于 FastAPI 实现。
在**乱序、迟到、重复投递、并发摄取、服务重启、落盘暂时故障**等场景下，
聚合结果确定、可解释，拒绝原因可区分；支持**按事件标识查询处理结果**
（客户端可安全重试）与**可重试摄取**（暂时性失败自动、有节奏地重试）。

## 1. 核心概念与语义

### 1.1 事件时间窗口

* 每个事件携带 `event_time_ms`（毫秒，非负），按其事件时间而非处理时间聚合。
* 窗口为固定大小的**滚动窗口**，左闭右开：`[start_ms, end_ms)`，
  `start_ms = floor(event_time_ms / window_size_ms) * window_size_ms`。
* 每个窗口维护 `count`（事件数）与 `total`（`value` 之和）。

### 1.2 水位线（Watermark）

* 水位线 `W` 表示“系统认为不会再收到 `event_time < W` 的事件”。
* **水位线单调不回退**：调用 `POST /watermark` 传入更小或相等的值不会
  改变当前水位线，响应中 `advanced=false`。
* 初始水位线为 `-1`（表示尚无时间信息，任何非负事件都不算迟到）。

### 1.3 窗口关闭

当 `W >= window.end_ms + allowed_lateness_ms` 时，窗口**永久关闭**，
关闭动作在推进水位线时触发并返回 `closed_windows`。关闭后的窗口不再
接受任何事件。

### 1.4 迟到事件处理规则

对事件时间 `t`、当前水位线 `W`：

| 情况 | 判定 | 处理 |
| --- | --- | --- |
| `t >= W` | 准时 | 接受，`late=false` |
| `W - allowed_lateness_ms <= t < W` | 迟到但在宽限期内 | 接受，`late=true` |
| `t < W - allowed_lateness_ms`，或目标窗口已关闭 | 迟到且超宽限 | **拒绝**，`reason=LATE_EVENT` |

拒绝与“接受但迟到”通过 `accepted` 与 `late` 字段明确区分，拒绝原因
`LATE_EVENT` 与其他拒绝原因可区分。

### 1.5 重复与幂等

* 每个事件有全局唯一 `event_id`；已处理过的 `event_id` 再次到达时
  返回 `accepted=false, reason=DUPLICATE_EVENT`，不重复计数。
* 去重集合随状态快照持久化，**重启后重复投递依旧被拒绝**。

### 1.6 持久化与重启恢复

* 每次接受事件 / 推进水位线后，将完整状态（水位线、窗口、已见事件 ID）
  写入状态文件：先写同目录临时文件 + `fsync`，再 `os.replace` 原子替换。
* 因此批次写入要么完整生效、要么保持上一份快照，**不会留下部分结果**。
* 重启时从该文件恢复：水位线不回退、已确认的聚合结果与窗口关闭状态保持一致。

### 1.7 并发与背压

* 摄取入口为**有界队列**（容量 `max_pending_events`），由固定数量工作线程
  消费；聚合器内部以锁保护计数与去重集合，**同一键的并发更新不会丢计数**。
* 队列已满时事件被**立即非阻塞拒绝**，`reason=BUFFER_FULL`（背压），
  不会无限占用内存。
* 服务停止期间提交返回 `reason=BUSY`，等待处理超时同样返回 `BUSY`。
* **同一 `event_id` 的并发提交**共享同一个在途处理：只入队一次，并发
  调用方拿到语义一致的结果（同在途时结果完全相同；在处理完成后到达的
  重复提交得到 `DUPLICATE_EVENT`），不会出现含义不清的结果，且只计一次。

### 1.8 按事件标识查询（安全重试）

* 服务为每个处理过的 `event_id` 记录**首个确定性结果**（决策台账），
  随状态快照持久化，重启后仍可查询：
  - `ACCEPTED`：已接受并计入窗口；
  - `REJECTED`：终态拒绝（如 `LATE_EVENT`），重发也不会成功；
  - `PENDING`：暂时性失败，已放入重试暂存区，等待自动重试。
* 客户端超时/未收到响应时，先 `GET /events/{event_id}`：
  - `ACCEPTED` → 不要重发（已统计进窗口）；
  - `REJECTED` → 终态，按原因处理，重发无意义；
  - `PENDING` → 等待自动重试，稍后再查；
  - `404` → 服务从未处理过该标识，可以放心重发。
* 重复投递不会覆盖台账中已记录的终态结果（例如已 `ACCEPTED` 的事件
  再次投递返回 `DUPLICATE_EVENT`，查询结果仍是 `ACCEPTED`）。

### 1.9 可重试摄取（暂时性失败自动重试）

* 失败被明确分为两类，原因标签稳定：
  - **暂时性**（`BUFFER_FULL`、`BUSY`）：容量/处理能力暂时不足，
    过一会儿重发很可能成功；
  - **终态**（`LATE_EVENT`、`DUPLICATE_EVENT`、`INVALID`）：
    重发也不会成功，直接进入台账，不占用暂存区。
* 开启可重试摄取（`ETS_MAX_RETRY_EVENTS > 0`）后，提交时遇到
  `BUFFER_FULL` 不直接丢弃，而是放入**有界重试暂存区**，响应中
  `retry_scheduled=true`；维护线程按 `ETS_RETRY_INTERVAL_MS` 的节奏
  自动重试，直到得出终态（接受或终态拒绝）。
* 暂存区容量有上限（`max_retry_events`），不会无限占内存；暂存区也满时，
  事件以原暂时性原因（`retry_scheduled=false`）返回，由客户端自行重试。
* 暂存记录随快照持久化，**重启后自动恢复并继续重试**；重试依赖幂等
  去重，**不会重复计数**（同一事件重试多次也只计一次）。
* 默认 `max_retry_events=0`：可重试摄取关闭，`BUFFER_FULL` 直接返回，
  行为与上一版完全一致。

### 1.10 落盘暂时失败的自愈

* 持久化采用全量快照 + 原子替换；写盘暂时失败（如磁盘故障）时：
  - 摄取链路**不停摆**：事件仍在内存中正常接受/拒绝，结果照常返回；
  - 磁盘上**不会出现只生效一半的状态**：失败时保留上一份完整快照；
  - 服务进入降级状态（`GET /health` 中 `storage_degraded=true`），
    维护线程每个周期自动重试落盘，**恢复后自愈**，无需重启。
* 故障期间若服务重启：恢复到最后一份完整快照，已确认（已落盘）的
  结果保持一致、不重复计数；仅内存确认、尚未落盘的结果不在其列。

### 1.11 拒绝原因一览（`RejectReason`）

| reason | 含义 | 类型 |
| --- | --- | --- |
| `LATE_EVENT` | 迟到超过宽限或窗口已关闭 | 终态 |
| `DUPLICATE_EVENT` | `event_id` 重复，幂等忽略 | 终态 |
| `INVALID` | 字段非法（HTTP 层通常表现为 422） | 终态 |
| `BUFFER_FULL` | 有界缓冲已满，触发背压拒绝 | 暂时性（可暂存重试） |
| `BUSY` | 服务停止中或处理超时 | 暂时性（客户端可查询后重发） |

## 2. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/events` | 提交事件，返回接受/拒绝结果与当前水位线 |
| `GET` | `/events/{event_id}` | 按标识查询处理结果（`ACCEPTED`/`REJECTED`/`PENDING`），未处理过返回 404 |
| `POST` | `/watermark` | 推进水位线，返回新关闭的窗口 |
| `GET` | `/windows?key=&closed=` | 查询窗口聚合结果 |
| `GET` | `/health` | 健康检查（水位线、缓冲占用、重试暂存占用、落盘是否降级） |

事件请求体：

```json
{ "event_id": "e1", "key": "k", "event_time_ms": 1500, "value": 1 }
```

事件响应（新增 `retry_scheduled` 字段）：

```json
{ "accepted": false, "watermark_ms": 2000, "late": false,
  "reason": "BUFFER_FULL", "retry_scheduled": true, "closed_windows": [] }
```

按标识查询响应：

```json
{ "event_id": "e1", "status": "ACCEPTED", "reason": null,
  "late": false, "watermark_ms": 2000 }
```

## 3. 配置项（环境变量）

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `ETS_WINDOW_SIZE_MS` | `1000` | 窗口长度（毫秒） |
| `ETS_ALLOWED_LATENESS_MS` | `500` | 允许迟到宽限（毫秒） |
| `ETS_MAX_PENDING_EVENTS` | `1000` | 摄取有界队列容量 |
| `ETS_INGEST_WORKERS` | `2` | 摄取工作线程数 |
| `ETS_STATE_PATH` | `data/state.json` | 状态快照文件路径 |
| `ETS_MAX_RETRY_EVENTS` | `0` | 重试暂存区容量；`0` 表示关闭可重试摄取（与上一版行为一致） |
| `ETS_RETRY_INTERVAL_MS` | `200` | 自动重试 / 落盘自愈的周期（毫秒） |

## 4. 本地验证

### 4.1 运行测试

```bash
uv sync
uv run pytest -q          # 静默
uv run pytest -s          # 打印水位线、窗口区间、拒绝原因日志
```

测试覆盖：

* `tests/test_watermark_windows.py`：乱序、迟到接受/拒绝、水位线回退、
  窗口关闭、按 key 独立关闭；
* `tests/test_dedup_recovery.py`：重复投递只计一次、重启后状态一致且
  重复仍被拒绝、关闭状态持久化；
* `tests/test_concurrency_bounded.py`：同键 200 事件并发不丢计数、
  缓冲超限 `BUFFER_FULL` 背压拒绝且原因可区分、停止后 `BUSY`；
* `tests/test_api.py`：端到端 HTTP 流程与重启恢复；
* `tests/test_event_status_query.py`：按标识查询（接受/终态拒绝/未知）、
  决策跨重启保留、重复投递不覆盖原结果、HTTP 查询支持安全重试；
* `tests/test_retryable_ingest.py`：暂时性失败入暂存区并自动重试成功、
  终态拒绝不进暂存区、暂存区有界、待重试记录跨重启恢复；
* `tests/test_concurrent_same_event.py`：同一 `event_id` 并发提交只计
  一次且结果语义一致；
* `tests/test_storage_failure_recovery.py`：落盘暂时失败时服务不停摆、
  不留半生效状态、恢复后自愈、故障期间重启结果仍一致。

### 4.2 启动服务并手工验证

```bash
ETS_STATE_PATH=/tmp/ets-demo/state.json \
  uv run uvicorn main:app --port 18099
```

另一终端：

```bash
# 乱序投递
curl -s -X POST localhost:18099/events -H 'Content-Type: application/json' \
  -d '{"event_id":"e1","key":"k","event_time_ms":1500}'
curl -s -X POST localhost:18099/events -H 'Content-Type: application/json' \
  -d '{"event_id":"e0","key":"k","event_time_ms":100}'
# 重复投递 -> DUPLICATE_EVENT
curl -s -X POST localhost:18099/events -H 'Content-Type: application/json' \
  -d '{"event_id":"e1","key":"k","event_time_ms":1500}'
# 推进水位线到 2000（关闭 [0,1000)，因为 end+grace=1500 <= 2000）
curl -s -X POST localhost:18099/watermark -H 'Content-Type: application/json' \
  -d '{"watermark_ms":2000}'
# 回退尝试 -> advanced=false, watermark 保持 2000
curl -s -X POST localhost:18099/watermark -H 'Content-Type: application/json' \
  -d '{"watermark_ms":100}'
# 宽限内迟到接受(late=true)；超宽限拒绝 LATE_EVENT
curl -s -X POST localhost:18099/events -H 'Content-Type: application/json' \
  -d '{"event_id":"e3","key":"k","event_time_ms":1700}'
curl -s -X POST localhost:18099/events -H 'Content-Type: application/json' \
  -d '{"event_id":"e4","key":"k","event_time_ms":1400}'
# 查询结果
curl -s 'localhost:18099/windows?key=k'
```

重启服务（保持同一 `ETS_STATE_PATH`）后再次 `GET /windows` 与重放旧
`event_id`，可验证结果一致、重复依旧被拒绝。

### 4.3 验证按标识查询与可重试摄取

```bash
# 开启可重试摄取（暂存区容量 100，重试周期 200ms）
ETS_STATE_PATH=/tmp/ets-demo/state.json \
ETS_MAX_RETRY_EVENTS=100 ETS_RETRY_INTERVAL_MS=200 \
  uv run uvicorn main:app --port 18099
```

```bash
# 提交后按标识查询处理结果（客户端超时后可先查再决定是否重发）
curl -s -X POST localhost:18099/events -H 'Content-Type: application/json' \
  -d '{"event_id":"q1","key":"k","event_time_ms":100}'
curl -s localhost:18099/events/q1        # -> status=ACCEPTED
curl -s localhost:18099/events/unknown   # -> 404，可放心重发

# 健康检查可观察重试暂存区占用与落盘降级状态
curl -s localhost:18099/health
# -> {"buffer_size":0,"retry_buffer_size":0,"storage_degraded":false,...}
```

模拟落盘故障（如把 `ETS_STATE_PATH` 指向只读目录后恢复可写），期间
`POST /events` 仍正常返回、`storage_degraded=true`；恢复可写后下一个
周期自动自愈，`storage_degraded` 回到 `false`，重启后结果一致。

## 5. 与上一版行为的兼容范围

* **默认配置完全兼容**：`ETS_MAX_RETRY_EVENTS` 默认为 `0`，可重试摄取
  关闭，`BUFFER_FULL`/`BUSY` 的拒绝行为与上一版一致。
* **状态文件向后兼容**：上一版生成的快照缺少 `decisions` /
  `pending_retries` 字段，加载时按空处理；本版保存的快照包含新增字段。
* **接口兼容**：`POST /events` 响应新增 `retry_scheduled` 字段（旧客户端
  可忽略）；新增 `GET /events/{event_id}`；`GET /health` 新增
  `retry_buffer_size`、`storage_degraded` 字段。
* **语义不变**：水位线单调、窗口关闭、迟到宽限、幂等去重、原子快照等
  原有语义均未改变；同一 `event_id` 并发提交从“后到的请求可能超时”
  修正为“共享在途结果”，结果更明确。

## 6. 代码结构

```
app/
  config.py      # 配置（环境变量）
  models.py      # 领域模型、RejectReason、决策台账与暂存记录类型
  storage.py     # 原子化快照存储（重启恢复，含台账与待重试记录）
  aggregator.py  # 事件时间窗口 / 水位线 / 迟到 / 去重 / 决策台账 / 落盘降级
  ingest.py      # 有界队列、工作线程、在途去重、重试暂存区与维护线程
  service.py     # FastAPI 装配与路由
main.py          # 入口
tests/           # 语义、并发、持久化、查询、重试与故障恢复测试
```
