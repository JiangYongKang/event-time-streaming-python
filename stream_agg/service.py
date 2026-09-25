"""FastAPI app wiring.

HTTP status mapping for the distinguishable reject reasons::

    INVALID_EVENT        -> 400
    WATERMARK_REGRESSION -> 409
    LATE_EVENT_TOO_OLD   -> 409 (per event; never fails a whole batch)
    QUEUE_FULL           -> 503 (back-pressure, retry later)
    PROCESSING_TIMEOUT   -> 503
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from stream_agg.config import Config
from stream_agg.engine import StreamEngine
from stream_agg.errors import IngestError, RejectCode
from stream_agg.models import Event

_HTTP_STATUS = {
    RejectCode.INVALID_EVENT: 400,
    RejectCode.WATERMARK_REGRESSION: 409,
    RejectCode.LATE_EVENT_TOO_OLD: 409,
    RejectCode.QUEUE_FULL: 503,
    RejectCode.PROCESSING_TIMEOUT: 503,
}


class EventIn(BaseModel):
    event_id: str = Field(..., min_length=1)
    key: str = Field(..., min_length=1)
    event_time: int = Field(..., ge=0)
    value: int = 1


class BatchIn(BaseModel):
    events: list[EventIn] = Field(..., min_length=1)


class WatermarkIn(BaseModel):
    watermark: int = Field(..., ge=0)


def create_app(config: Config | None = None) -> FastAPI:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    cfg = config or Config.from_env()
    engine = StreamEngine(cfg)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engine.start()
        try:
            yield
        finally:
            engine.stop()

    app = FastAPI(title="event-time-streaming", lifespan=lifespan)
    app.state.engine = engine

    @app.exception_handler(IngestError)
    async def _ingest_error_handler(request, exc: IngestError):  # noqa: ANN001
        return JSONResponse(
            status_code=_HTTP_STATUS[exc.code],
            content={"error": exc.to_dict()},
        )

    @app.get("/")
    def read_root() -> dict:
        return {
            "service": "event-time-streaming",
            "watermark": engine.watermark,
            "config": {
                "window_size_ms": cfg.window_size_ms,
                "allowed_lateness_ms": cfg.allowed_lateness_ms,
                "max_out_of_orderness_ms": cfg.max_out_of_orderness_ms,
                "queue_capacity": cfg.queue_capacity,
            },
        }

    @app.post("/events")
    def post_event(event: EventIn) -> dict:
        result = engine.ingest(
            Event(
                event_id=event.event_id,
                key=event.key,
                event_time=event.event_time,
                value=event.value,
            )
        )
        return result.__dict__

    @app.post("/events/batch")
    def post_batch(batch: BatchIn) -> dict:
        events = [
            Event(
                event_id=e.event_id,
                key=e.key,
                event_time=e.event_time,
                value=e.value,
            )
            for e in batch.events
        ]
        result = engine.ingest_batch(events)
        return {
            "accepted": result.accepted,
            "duplicates": result.duplicates,
            "rejected": result.rejected,
            "watermark": result.watermark,
            "results": [r.__dict__ for r in result.results],
        }

    @app.post("/watermark")
    def post_watermark(body: WatermarkIn) -> dict:
        new_wm = engine.advance_watermark(body.watermark)
        return {"watermark": new_wm}

    @app.get("/state/watermark")
    def get_watermark() -> dict:
        return {"watermark": engine.watermark}

    @app.get("/state/windows")
    def get_windows() -> dict:
        return {
            "watermark": engine.watermark,
            "windows": [w.__dict__ | {"state": w.state.value} for w in engine.windows()],
        }

    @app.get("/state/outputs")
    def get_outputs() -> dict:
        return {
            "watermark": engine.watermark,
            "outputs": [
                w.__dict__ | {"state": w.state.value} for w in engine.outputs()
            ],
        }

    @app.get("/state/queue")
    def get_queue() -> dict:
        return engine.queue_stats()

    return app
