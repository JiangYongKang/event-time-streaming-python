# Event-Time Streaming Aggregation (Python / FastAPI)

一套**本地可验证**的事件时间（event-time）流式聚合服务：按事件时间划分滚动窗口、
用单调水位线推进与关闭窗口，对乱序 / 迟到 / 重复 / 并发投递给出**确定且可解释**的结果。

- 事件时间窗口聚合，水位线（watermark）越过窗口边界即关闭窗口
- 水位线严格单调；回退请求被拒绝并返回可区分原因 `WATERMARK_REGRESSION`
- 迟到事件：允许迟到窗口内接受并**纠正**已最终化结果；超过迟到上限拒绝为
  `LATE_EVENT_TOO_OLD`
- 幂等：同一 `event_id` 重复投递（含批次内重复、重启后重放）绝不重复计数
- 原子批次：一批事件在单事务内提交，失败整体回滚，不留部分结果
- 持久化恢复：SQLite（WAL + synchronous=FULL）落盘，重启后水位线与已确认结果一致
- 有界摄取队列：容量超限立即背压拒绝 `QUEUE_FULL`；处理过慢返回
  `PROCESSING_TIMEOUT`；单工作线程串行提交，同键并发更新不丢计数

详细语义见 [docs/SEMANTICS.md](docs/SEMANTICS.md)。

## 快速开始

```bash
uv sync --group dev          # 安装运行与测试依赖
uv run uvicorn main:app --reload
# 或: uv run fastapi dev main.py
```

发送事件（epoch 毫秒时间戳）：

```bash
curl -s -X POST localhost:8000/events -H 'content-type: application/json' \
  -d '{"event_id":"e1","key":"user-1","event_time":1000}'
```

推进水位线（显式）：

```bash
curl -s -X POST localhost:8000/watermark -H 'content-type: application/json' \
  -d '{"watermark":10000}'
```

查看状态：

```bash
curl -s localhost:8000/state/watermark
curl -s localhost:8000/state/windows     # 工作中的窗口
curl -s localhost:8000/state/outputs     # 已最终化窗口（结果）
curl -s localhost:8000/state/queue       # 有界队列与背压计数
```

批量摄入（原子提交）：

```bash
curl -s -X POST localhost:8000/events/batch -H 'content-type: application/json' \
  -d '{"events":[
    {"event_id":"b1","key":"user-1","event_time":2000},
    {"event_id":"b2","key":"user-1","event_time":8000}
  ]}'
```

## 配置项（环境变量）

| 变量 | 默认值 | 含义 |
| --- | --- | --- |
| `STREAM_DB_PATH` | `stream_agg.db` | SQLite 文件路径（测试用临时文件） |
| `STREAM_WINDOW_SIZE_MS` | `10000` | 滚动窗口大小（毫秒），区间 `[start, end)` |
| `STREAM_ALLOWED_LATENESS_MS` | `5000` | 窗口关闭后仍可纠正的迟到容忍（毫秒） |
| `STREAM_MAX_OOO_MS` | `2000` | 启发式水位线相对最大观测事件时间的滞后 |
| `STREAM_QUEUE_CAPACITY` | `1024` | 有界摄取队列容量（背压阈值） |
| `STREAM_PROCESSING_TIMEOUT_MS` | `5000` | 入队后等待处理完成的超时（毫秒） |
| `STREAM_SIMULATED_PROCESS_DELAY_MS` | 未设置 | 人工注入处理延迟，便于本地复现背压/超时 |

## 本地验证

```bash
uv run pytest -s                      # 全部场景（-s 查看水位线/窗口/拒绝日志）
uv run pytest tests/test_late.py -s   # 只看迟到语义
```

测试覆盖：乱序聚合、迟到接受/拒绝、水位线回退、重复投递（含批次内与重启重放）、
批次失败回滚、重启恢复一致性、同键并发不丢计数、队列满背压与处理超时。
日志会打印**当前水位线、窗口区间 `[start,end)`、窗口状态以及拒绝原因码与说明**。

可观测的拒绝原因码：

| code | HTTP | 含义 |
| --- | --- | --- |
| `LATE_EVENT_TOO_OLD` | 409（批次内逐事件） | 晚于 `window_end + allowed_lateness` |
| `WATERMARK_REGRESSION` | 409 | 试图将水位线回退 |
| `QUEUE_FULL` | 503 | 有界队列已满，背压：稍后重试 |
| `PROCESSING_TIMEOUT` | 503 | 已入队但处理未在超时内完成 |
| `INVALID_EVENT` | 400/422 | 非法事件字段 |
