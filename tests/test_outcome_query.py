"""按 event_id 查询处理结果：客户端超时/失败后可安全重试。"""

import pytest
from conftest import log_line
from fastapi.testclient import TestClient

from app.aggregator import EventTimeAggregator
from app.models import Event, OutcomeStatus, RejectReason
from app.service import create_app
from app.storage import StateStore


def test_query_accepted_and_rejected_outcomes(settings):
    agg = EventTimeAggregator(settings)
    agg.add_event(Event("q-ok", "k", 100, value=2))
    agg.advance_watermark(2000)  # 宽限到 1500
    agg.add_event(Event("q-late", "k", 1000))  # 超宽限 -> LATE_EVENT

    ok = agg.query_event("q-ok")
    late = agg.query_event("q-late")
    log_line(f"outcome q-ok: {ok}")
    log_line(f"outcome q-late: {late}")
    assert ok.status == OutcomeStatus.ACCEPTED and ok.reason is None
    assert late.status == OutcomeStatus.REJECTED
    assert late.reason == RejectReason.LATE_EVENT
    assert late.retryable is False  # 终态拒绝：重发也不会成功

    # 从未见过的标识
    assert agg.query_event("q-unknown") is None


def test_query_after_duplicate_keeps_original_accept(settings):
    agg = EventTimeAggregator(settings)
    agg.add_event(Event("q-dup", "k", 100))
    dup = agg.add_event(Event("q-dup", "k", 100))
    log_line(f"duplicate submit reason={dup.reason}")
    assert dup.reason == RejectReason.DUPLICATE_EVENT

    outcome = agg.query_event("q-dup")
    log_line(f"outcome after duplicate: status={outcome.status}")
    # 查询要如实反映“已被接受并统计”，而不是被重复投递覆盖
    assert outcome.status == OutcomeStatus.ACCEPTED


def test_outcome_survives_restart(settings):
    store = StateStore(settings.state_path)
    agg1 = EventTimeAggregator(settings, store=store)
    agg1.add_event(Event("q-restart", "k", 100))
    agg1.advance_watermark(2000)
    agg1.add_event(Event("q-restart-late", "k", 1000))
    log_line("state persisted; simulating restart")

    agg2 = EventTimeAggregator(settings, store=store)
    ok = agg2.query_event("q-restart")
    late = agg2.query_event("q-restart-late")
    log_line(f"after restart: q-restart={ok.status} q-restart-late={late.status}/{late.reason}")
    assert ok.status == OutcomeStatus.ACCEPTED
    assert late.status == OutcomeStatus.REJECTED
    assert late.reason == RejectReason.LATE_EVENT


@pytest.fixture()
def client(settings):
    app = create_app(settings)
    with TestClient(app) as c:
        yield c


def test_query_endpoint_safe_retry_flow(client):
    # 客户端提交后“超时没收到响应”，按标识查询确认真实结果
    r = client.post(
        "/events",
        json={"event_id": "http-q1", "key": "k", "event_time_ms": 1500},
    ).json()
    log_line(f"submit result: {r}")
    assert r["accepted"] and r["retryable"] is False

    outcome = client.get("/events/http-q1").json()
    log_line(f"query outcome: {outcome}")
    assert outcome["status"] == "ACCEPTED"
    assert outcome["retryable"] is False

    # 重发同一标识 -> 重复，查询结果仍是 ACCEPTED（语义不被覆盖）
    dup = client.post(
        "/events",
        json={"event_id": "http-q1", "key": "k", "event_time_ms": 1500},
    ).json()
    log_line(f"re-submit reason={dup['reason']} retryable={dup['retryable']}")
    assert dup["reason"] == "DUPLICATE_EVENT" and dup["retryable"] is False
    assert client.get("/events/http-q1").json()["status"] == "ACCEPTED"

    # 未知标识 -> 404，客户端可放心首次提交
    missing = client.get("/events/never-seen")
    log_line(f"unknown id -> {missing.status_code}")
    assert missing.status_code == 404


def test_query_endpoint_reflects_terminal_rejection(client):
    client.post("/watermark", json={"watermark_ms": 2000})
    r = client.post(
        "/events",
        json={"event_id": "http-q2", "key": "k", "event_time_ms": 1000},
    ).json()
    log_line(f"late reject: reason={r['reason']} retryable={r['retryable']}")
    assert r["reason"] == "LATE_EVENT" and r["retryable"] is False

    outcome = client.get("/events/http-q2").json()
    log_line(f"query rejected outcome: {outcome}")
    assert outcome["status"] == "REJECTED"
    assert outcome["reason"] == "LATE_EVENT"
    assert outcome["retryable"] is False
