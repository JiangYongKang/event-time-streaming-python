"""End-to-end HTTP semantics and distinguishable error status mapping."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from stream_agg.service import create_app
from stream_agg.config import Config


@pytest.fixture
def client(db_path):
    app = create_app(
        Config(
            db_path=db_path,
            window_size_ms=10_000,
            allowed_lateness_ms=5_000,
            max_out_of_orderness_ms=0,
            queue_capacity=4,
            processing_timeout_ms=2_000,
        )
    )
    with TestClient(app) as c:
        yield c


def test_accept_duplicate_late_regression_over_http(client):
    r = client.post("/events", json={"event_id": "e1", "key": "k", "event_time": 1000})
    assert r.status_code == 200 and r.json()["status"] == "ACCEPTED"

    r = client.post("/events", json={"event_id": "e1", "key": "k", "event_time": 1000})
    assert r.status_code == 200 and r.json()["status"] == "DUPLICATE"

    # close the window
    assert client.post("/watermark", json={"watermark": 10000}).json() == {
        "watermark": 10000
    }

    # late but within allowed lateness: accepted
    r = client.post("/events", json={"event_id": "late", "key": "k", "event_time": 500})
    assert r.json()["status"] == "ACCEPTED"

    # regression -> 409 + WATERMARK_REGRESSION
    r = client.post("/watermark", json={"watermark": 1})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "WATERMARK_REGRESSION"

    # purge then late -> per-event rejection inside a 200 batch result
    client.post("/watermark", json={"watermark": 15001})
    r = client.post(
        "/events/batch",
        json={"events": [{"event_id": "too-late", "key": "k", "event_time": 500}]},
    )
    assert r.status_code == 200
    item = r.json()["results"][0]
    assert item["status"] == "REJECTED"
    assert item["reject_code"] == "LATE_EVENT_TOO_OLD"


def test_invalid_event_is_400(client):
    r = client.post(
        "/events", json={"event_id": "", "key": "k", "event_time": 1}
    )
    assert r.status_code == 422  # pydantic validation before engine

    r = client.post("/events/batch", json={"events": []})
    assert r.status_code == 422


def test_state_endpoints_report_watermark_windows_outputs(client):
    client.post("/events", json={"event_id": "e1", "key": "k", "event_time": 1000})
    client.post("/watermark", json={"watermark": 10000})

    assert client.get("/state/watermark").json()["watermark"] == 10000
    outputs = client.get("/state/outputs").json()["outputs"]
    assert outputs[0]["window_start"] == 0 and outputs[0]["count"] == 1
    queue = client.get("/state/queue").json()
    assert queue["capacity"] == 4
