"""按事件标识查询处理结果：客户端超时/失败后可安全重试。"""

import pytest
from conftest import log_line
from fastapi.testclient import TestClient

from app.aggregator import EventTimeAggregator
from app.models import DecisionStatus, Event, RejectReason
from app.service import create_app
from app.storage import StateStore


def test_query_unknown_event_returns_none(settings):
    agg = EventTimeAggregator(settings)
    assert agg.query_event("never-seen") is None


def test_accepted_event_queryable(settings):
    agg = EventTimeAggregator(settings)
    r = agg.add_event(Event("q-1", "k", 100, value=2))
    assert r.accepted

    d = agg.query_event("q-1")
    log_line(f"query q-1: status={d.status} late={d.late} watermark={d.watermark_ms}")
    assert d.status == DecisionStatus.ACCEPTED
    assert d.reason is None


def test_rejected_event_queryable_with_reason(settings):
    agg = EventTimeAggregator(settings)
    agg.add_event(Event("q-2", "k", 1200))
    agg.advance_watermark(2000)  # 宽限到 1500
    r = agg.add_event(Event("q-3", "k", 1000))
    log_line(f"late reject: reason={r.reason}")
    assert r.reason == RejectReason.LATE_EVENT

    d = agg.query_event("q-3")
    log_line(f"query q-3: status={d.status} reason={d.reason}")
    assert d.status == DecisionStatus.REJECTED
    assert d.reason == RejectReason.LATE_EVENT


def test_duplicate_does_not_overwrite_original_decision(settings):
    agg = EventTimeAggregator(settings)
    agg.add_event(Event("q-4", "k", 100))
    agg.add_event(Event("q-4", "k", 100))  # 重复投递
    d = agg.query_event("q-4")
    log_line(f"query q-4 after duplicate: status={d.status}")
    # 查询反映的是“当时被接受”这一事实，而不是后来的 DUPLICATE_EVENT
    assert d.status == DecisionStatus.ACCEPTED


def test_decisions_survive_restart(settings):
    store = StateStore(settings.state_path)
    agg1 = EventTimeAggregator(settings, store=store)
    agg1.add_event(Event("q-5", "k", 100))
    agg1.advance_watermark(2000)
    agg1.add_event(Event("q-6", "k", 100))  # 超宽限 -> LATE_EVENT

    agg2 = EventTimeAggregator(settings, store=store)
    d_ok = agg2.query_event("q-5")
    d_bad = agg2.query_event("q-6")
    log_line(
        f"after restart: q-5={d_ok.status} q-6={d_bad.status}/{d_bad.reason}"
    )
    assert d_ok.status == DecisionStatus.ACCEPTED
    assert d_bad.status == DecisionStatus.REJECTED
    assert d_bad.reason == RejectReason.LATE_EVENT


@pytest.fixture()
def client(settings):
    app = create_app(settings)
    with TestClient(app) as c:
        yield c


def test_http_query_supports_safe_client_retry(client):
    # 客户端提交后（假设当时超时没收到响应），先按标识查询再决定是否重发
    r = client.post(
        "/events",
        json={"event_id": "h-1", "key": "k", "event_time_ms": 100, "value": 5},
    )
    assert r.status_code == 200

    q = client.get("/events/h-1")
    log_line(f"http query h-1: {q.json()}")
    assert q.status_code == 200
    assert q.json()["status"] == "ACCEPTED"
    # 已确认接受 -> 客户端不再重发，窗口只计一次
    win = client.get("/windows?key=k").json()["windows"][0]
    assert win["count"] == 1 and win["total"] == 5

    missing = client.get("/events/h-404")
    log_line(f"http query missing -> {missing.status_code}")
    assert missing.status_code == 404
    # 查不到 -> 客户端可以放心重发
