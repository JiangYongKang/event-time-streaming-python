"""HTTP 服务装配与路由。"""

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.aggregator import EventTimeAggregator
from app.config import Settings, get_settings
from app.ingest import IngestService
from app.models import Event
from app.storage import StateStore


class EventIn(BaseModel):
    event_id: str = Field(min_length=1)
    key: str = Field(min_length=1)
    event_time_ms: int = Field(ge=0)
    value: int = 1


class WatermarkIn(BaseModel):
    watermark_ms: int = Field(ge=0)


def _window_dto(agg) -> dict:
    return {
        "key": agg.window.key,
        "interval": agg.window.interval(),
        "start_ms": agg.window.start_ms,
        "end_ms": agg.window.end_ms,
        "count": agg.count,
        "total": agg.total,
        "closed": agg.closed,
    }


def _result_dto(result, closed=None) -> dict:
    return {
        "accepted": result.accepted,
        "watermark_ms": result.watermark_ms,
        "late": result.late,
        "reason": result.reason.value if result.reason else None,
        "retryable": result.retryable,
        "closed_windows": [
            {"key": w.key, "interval": w.interval(),
             "start_ms": w.start_ms, "end_ms": w.end_ms}
            for w in (closed or result.closed_windows)
        ],
    }


def _outcome_dto(outcome) -> dict:
    return {
        "event_id": outcome.event_id,
        "status": outcome.status.value,
        "watermark_ms": outcome.watermark_ms,
        "late": outcome.late,
        "reason": outcome.reason.value if outcome.reason else None,
        "retryable": outcome.retryable,
    }


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    store = StateStore(settings.state_path)
    aggregator = EventTimeAggregator(settings, store=store)
    ingest = IngestService(aggregator, settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        ingest.start()
        yield
        ingest.stop()

    app = FastAPI(
        title="event-time-streaming",
        version="0.1.0",
        lifespan=lifespan,
    )

    @app.get("/")
    def read_root() -> dict:
        return {"service": "event-time-streaming", "status": "ok"}

    @app.post("/events")
    def post_event(payload: EventIn) -> dict:
        event = Event(
            event_id=payload.event_id,
            key=payload.key,
            event_time_ms=payload.event_time_ms,
            value=payload.value,
        )
        result = ingest.submit(event)
        return _result_dto(result)

    @app.post("/watermark")
    def post_watermark(payload: WatermarkIn) -> dict:
        before = aggregator.watermark_ms
        closed = aggregator.advance_watermark(payload.watermark_ms)
        return {
            "watermark_ms": aggregator.watermark_ms,
            "advanced": payload.watermark_ms > before,
            "closed_windows": [
                {"key": w.key, "interval": w.interval(),
                 "start_ms": w.start_ms, "end_ms": w.end_ms}
                for w in closed
            ],
        }

    @app.get("/windows")
    def get_windows(key: str | None = None, closed: bool | None = None) -> dict:
        snap = aggregator.snapshot()
        windows = [
            _window_dto(w)
            for w in snap.windows
            if (key is None or w.window.key == key)
            and (closed is None or w.closed == closed)
        ]
        return {"watermark_ms": snap.watermark_ms, "windows": windows}

    @app.get("/events/{event_id}")
    def get_event_outcome(event_id: str) -> dict:
        """按 event_id 查询处理结果，供客户端超时/失败后安全重试。

        * 200 + status=ACCEPTED/REJECTED：该标识已有终态结果，不要重发；
        * 200 + status=PENDING：在暂存区等待自动重试，稍后再查；
        * 404：从未见过该标识，可以放心首次提交。
        """
        outcome = ingest.query_event(event_id)
        if outcome is None:
            raise HTTPException(status_code=404, detail="event_id not found")
        return _outcome_dto(outcome)

    @app.get("/health")
    def health() -> dict:
        return {
            "status": "ok",
            "watermark_ms": aggregator.watermark_ms,
            "buffer_size": ingest.buffer_size(),
            "retry_buffer_size": ingest.retry_buffer_size(),
            "store_healthy": aggregator.store_healthy,
        }

    # 暴露给测试直接操作
    app.state.aggregator = aggregator
    app.state.ingest = ingest
    app.state.settings = settings
    return app
