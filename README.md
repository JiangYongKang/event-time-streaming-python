# 事件时间流式聚合服务（event-time-streaming-python）

一个可本地验证的事件时间（event-time）流式聚合服务，基于 FastAPI 实现。
在**乱序、迟到、重复投递、并发摄取、服务重启**等场景下，聚合结果确定、
可解释，拒绝原因可区分。

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

### 1.8 拒绝原因一览（`RejectReason`）

| reason | 含义 |
| --- | --- |
| `LATE_EVENT` | 迟到超过宽限或窗口已关闭 |
| `DUPLICATE_EVENT` | `event_id` 重复，幂等忽略 |
| `BUFFER_FULL` | 有界缓冲已满，触发背压拒绝 |
| `BUSY` | 服务停止中或处理超时 |
| `INVALID` | 字段非法（HTTP 层通常表现为 422） |

## 2. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/events` | 提交事件，返回接受/拒绝结果与当前水位线 |
| `POST` | `/watermark` | 推进水位线，返回新关闭的窗口 |
| `GET` | `/windows?key=&closed=` | 查询窗口聚合结果 |
| `GET` | `/health` | 健康检查（水位线、缓冲占用） |

事件请求体：

```json
{ "event_id": "e1", "key": "k", "event_time_ms": 1500, "value": 1 }
```

## 3. 配置项（环境变量）

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `ETS_WINDOW_SIZE_MS` | `1000` | 窗口长度（毫秒） |
| `ETS_ALLOWED_LATENESS_MS` | `500` | 允许迟到宽限（毫秒） |
| `ETS_MAX_PENDING_EVENTS` | `1000` | 摄取有界队列容量 |
| `ETS_INGEST_WORKERS` | `2` | 摄取工作线程数 |
| `ETS_STATE_PATH` | `data/state.json` | 状态快照文件路径 |

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
```

重启服务（保持同一 `ETS_STATE_PATH`）后再次 `GET /windows` 与重放旧
`event_id`，可验证结果一致、重复依旧被拒绝。

## 5. 代码结构

```
app/
  config.py      # 配置（环境变量）
  models.py      # 领域模型与 RejectReason
  storage.py     # 原子化快照存储（重启恢复）
  aggregator.py  # 事件时间窗口 / 水位线 / 迟到 / 去重核心
  ingest.py      # 有界队列、工作线程、背压拒绝
  service.py     # FastAPI 装配与路由
main.py          # 入口
tests/           # 语义、并发、持久化与端到端测试
```
