# 事件时间流式聚合服务（event-time-streaming-python）

一个可本地验证的事件时间（event-time）流式聚合服务，基于 FastAPI 实现。
在**乱序、迟到、重复投递、并发摄取、服务重启**等场景下，聚合结果确定、
可解释，拒绝原因可区分。在此之上还提供：

* **按标识查询处理结果**：客户端超时/失败后按 `event_id` 查询当时的
  接受/拒绝结果（重启后依然可查），放心重试而不必盲目重发；
* **可重试摄取**：暂时性失败（容量占满、服务忙、落盘暂不可用）进入
  有界暂存区自动、有节奏地重试，与终态拒绝明确区分，不丢结果、不
  重复计数，落盘故障恢复后自愈。

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
* **同一 `event_id` 的并发提交会被合并**：只入队、只计数一次，所有并发
  调用共享同一个处理结果——要么都算接受、要么都算重复
  （`DUPLICATE_EVENT`），不会返回含义不清的结果。

### 1.8 按标识查询处理结果（安全重试）

* 每个被处理过的 `event_id`（接受或终态拒绝）都会记入**结果台账**，并随
  状态快照一起原子持久化，**重启后依旧可查**。
* 客户端超时/失败后可先 `GET /events/{event_id}`：
  - `status=ACCEPTED`：已被接受并统计进窗口，**不要重发**；
  - `status=REJECTED`：已被终态拒绝（含原因），重发也不会成功；
  - `status=PENDING`：在可重试暂存区中等待自动重试，稍后再查；
  - `404`：从未见过该标识，可放心首次提交。
* 重复投递不会覆盖台账中原始的 `ACCEPTED` 结果。

### 1.9 可重试摄取（暂时性失败自动重试）

* 拒绝原因分为两类，响应中用 `retryable` 字段明确区分：
  - **暂时性（`retryable=true`）**：`BUFFER_FULL`、`BUSY`、
    `STORE_UNAVAILABLE`——过一会儿重发很可能成功；
  - **终态（`retryable=false`）**：`LATE_EVENT`、`DUPLICATE_EVENT`、
    `INVALID`——重发也不会成功。
* 暂时性失败的事件进入**有界暂存区**（容量 `max_retry_events`），由后台
  线程按固定节奏（`retry_interval_ms`）自动重试；暂存区满时暂时性拒绝
  原样返回客户端，不无限占用内存。
* 暂存区内容随状态快照一起原子持久化，**重启后继续自动重试**；重试基于
  `event_id` 幂等，**不会重复计数**（重试命中已接受事件时得到
  `DUPLICATE_EVENT` 并出区）。
* 暂存期间客户端手动重发同一标识：一次接受、其余重复，仍只计一次。

### 1.10 落盘暂时失败与自愈

* 接受事件时落盘失败：**内存变更整体回滚**（计数、去重集合、台账都不
  生效，不会出现“只生效一半”），返回 `STORE_UNAVAILABLE`
  （`retryable=true`），事件进入暂存区稍后自动重试；**服务不停摆**。
* 推进水位线时落盘失败：水位线在内存中保持（单调性优先），标记为
  “待刷盘”；之后任何一次成功写入或后台重试周期的 `flush` 都会把
  **完整快照**重新落盘，实现自愈。可通过 `GET /health` 的
  `store_healthy` 观察。
* 快照写入是“临时文件 + `fsync` + 原子替换”，故障期间磁盘上始终保留
  上一份完整快照；恢复后已确认结果在重启前后保持一致。

### 1.11 拒绝原因一览（`RejectReason`）

| reason | 含义 | retryable |
| --- | --- | --- |
| `LATE_EVENT` | 迟到超过宽限或窗口已关闭 | 否（终态） |
| `DUPLICATE_EVENT` | `event_id` 重复，幂等忽略 | 否（终态） |
| `INVALID` | 字段非法（HTTP 层通常表现为 422） | 否（终态） |
| `BUFFER_FULL` | 有界缓冲已满，触发背压拒绝 | 是（自动重试） |
| `BUSY` | 服务停止中或处理超时 | 是（自动重试） |
| `STORE_UNAVAILABLE` | 落盘暂时失败，内存变更已回滚 | 是（自动重试） |

## 2. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/events` | 提交事件，返回接受/拒绝结果、当前水位线与 `retryable` 标签 |
| `GET` | `/events/{event_id}` | 按标识查询处理结果（ACCEPTED/REJECTED/PENDING/404） |
| `POST` | `/watermark` | 推进水位线，返回新关闭的窗口 |
| `GET` | `/windows?key=&closed=` | 查询窗口聚合结果 |
| `GET` | `/health` | 健康检查（水位线、缓冲占用、暂存区占用、落盘健康） |

事件请求体：

```json
{ "event_id": "e1", "key": "k", "event_time_ms": 1500, "value": 1 }
```

按标识查询响应示例：

```json
{ "event_id": "e1", "status": "ACCEPTED", "watermark_ms": 2000,
  "late": false, "reason": null, "retryable": false }
```

## 3. 配置项（环境变量）

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `ETS_WINDOW_SIZE_MS` | `1000` | 窗口长度（毫秒） |
| `ETS_ALLOWED_LATENESS_MS` | `500` | 允许迟到宽限（毫秒） |
| `ETS_MAX_PENDING_EVENTS` | `1000` | 摄取有界队列容量 |
| `ETS_INGEST_WORKERS` | `2` | 摄取工作线程数 |
| `ETS_STATE_PATH` | `data/state.json` | 状态快照文件路径 |
| `ETS_MAX_RETRY_EVENTS` | `500` | 可重试暂存区容量上限 |
| `ETS_RETRY_INTERVAL_MS` | `200` | 暂存区自动重试间隔（毫秒） |

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
* `tests/test_outcome_query.py`：按标识查询接受/拒绝结果、重复不覆盖
  原始接受、重启后仍可查、HTTP 安全重试流程；
* `tests/test_retry_ingest.py`：`BUFFER_FULL`/`BUSY` 自动暂存与重试、
  终态拒绝不进暂存区、暂存区有界、重启后恢复重试、暂存期间手动重发
  只计一次；
* `tests/test_concurrent_same_id.py`：同一标识并发提交只计一次且结果
  语义一致（无含义不清的结果）；
* `tests/test_storage_failure.py`：落盘失败回滚、磁盘上无半生效状态、
  恢复后自愈且重启前后结果一致；
* `tests/test_api.py`：端到端 HTTP 流程与重启恢复。

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
# 按标识查询处理结果（客户端超时后安全重试的依据）
curl -s 'localhost:18099/events/e1'
# 健康检查：含水印、缓冲/暂存区占用、落盘健康
curl -s 'localhost:18099/health'
```

重启服务（保持同一 `ETS_STATE_PATH`）后再次 `GET /windows`、
`GET /events/e1` 与重放旧 `event_id`，可验证结果一致、重复依旧被拒绝、
台账与暂存区内容恢复。

## 5. 兼容范围

* 状态快照格式向后兼容：旧版本（无 `outcomes` / `retry_events` 字段）的
  快照文件可直接加载，缺失部分按空处理。
* 原有接口（`POST /events`、`POST /watermark`、`GET /windows`、
  `GET /health`）的请求/响应字段只增不减：`POST /events` 响应新增
  `retryable` 字段，`GET /health` 新增 `retry_buffer_size` 与
  `store_healthy` 字段；原有字段语义不变。
* 拒绝原因集合新增 `STORE_UNAVAILABLE`；原有五种原因的含义与触发条件
  不变。`BUFFER_FULL`/`BUSY` 现在会额外进入暂存区自动重试——若客户端
  原本自行重试，幂等语义保证不会重复计数。

## 6. 代码结构

```
app/
  config.py      # 配置（环境变量）
  models.py      # 领域模型、RejectReason 与可重试分类、EventOutcome
  storage.py     # 原子化快照存储（窗口/台账/暂存区，重启恢复）
  aggregator.py  # 事件时间窗口 / 水位线 / 迟到 / 去重 / 台账 / 落盘自愈
  retry.py       # 有界可重试暂存区
  ingest.py      # 有界队列、工作线程、背压、同 id 合并、自动重试
  service.py     # FastAPI 装配与路由
main.py          # 入口
tests/           # 语义、并发、持久化、查询、重试与端到端测试
```
