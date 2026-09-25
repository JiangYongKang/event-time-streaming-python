"""端到端 HTTP API 测试：乱序 -> 水位线 -> 窗口关闭 -> 迟到拒绝 -> 重启恢复。"""

import pytest
from fastapi.testclient import TestClient

from app.service import create_app

from conftest import log_line


@pytest.fixture()
def client(settings):
    app = create_app(settings)
    with TestClient(app) as c:
        yield c


def _post(client, eid, key, t, value=1):
    return client.post(
        "/events",
        json={"event_id": eid, "key": key, "event_time_ms": t, "value": value},
    )


def test_full_event_time_flow(client):
    # 1) 乱序投递两个窗口的事件
    r1 = _post(client, "e1", "k", 1500).json()
    r2 = _post(client, "e0", "k", 100).json()
    r3 = _post(client, "e2", "k", 1200).json()
    log_line(f"ingest: {r1} / {r2} / {r3}")
    assert all(x["accepted"] for x in (r1, r2, r3))

    # 2) 重复投递
    dup = _post(client, "e1", "k", 1500).json()
    log_line(f"duplicate rejected reason={dup['reason']}")
    assert dup["accepted"] is False and dup["reason"] == "DUPLICATE_EVENT"

    # 3) 水位线推进（不回退）
    wm = client.post("/watermark", json={"watermark_ms": 2000}).json()
    log_line(f"watermark -> {wm['watermark_ms']} closed={wm['closed_windows']}")
    assert wm["watermark_ms"] == 2000
    rollback = client.post("/watermark", json={"watermark_ms": 100}).json()
    log_line(f"rollback attempt -> {rollback['watermark_ms']} advanced={rollback['advanced']}")
    assert rollback["watermark_ms"] == 2000 and rollback["advanced"] is False

    # 4) 迟到但在宽限内接受，超出宽限拒绝
    late_ok = _post(client, "e3", "k", 1700).json()
    late_bad = _post(client, "e4", "k", 1400).json()
    log_line(f"late-within-grace: {late_ok}; late-beyond-grace: {late_bad}")
    assert late_ok["accepted"] is True and late_ok["late"] is True
    assert late_bad["accepted"] is False and late_bad["reason"] == "LATE_EVENT"

    # 5) 水位线越过 end+grace，窗口关闭
    closed = client.post("/watermark", json={"watermark_ms": 2500}).json()
    intervals = [w["interval"] for w in closed["closed_windows"]]
    log_line(f"watermark=2500 closed windows={intervals}")
    assert "[1000, 2000)" in intervals

    post_close = _post(client, "e5", "k", 1500).json()
    log_line(f"post-close reject reason={post_close['reason']}")
    assert post_close["reason"] == "LATE_EVENT"

    # 6) 查询窗口结果
    windows = client.get("/windows?key=k").json()
    log_line(f"snapshot watermark={windows['watermark_ms']} windows={windows['windows']}")
    w1 = [w for w in windows["windows"] if w["start_ms"] == 1000][0]
    assert w1["count"] == 3 and w1["closed"] is True


def test_restart_recovery_over_http(settings):
    with TestClient(create_app(settings)) as c1:
        assert _post(c1, "r1", "k", 100).status_code == 200
    # 重启：同 state_path 新建 app
    with TestClient(create_app(settings)) as c2:
        dup = _post(c2, "r1", "k", 100).json()
        log_line(f"after restart duplicate reason={dup['reason']}")
        assert dup["reason"] == "DUPLICATE_EVENT"
        windows = c2.get("/windows").json()
        log_line(f"after restart windows={windows['windows']}")
        assert windows["windows"][0]["count"] == 1


def test_invalid_event_rejected(client):
    r = client.post(
        "/events",
        json={"event_id": "", "key": "k", "event_time_ms": 100},
    )
    assert r.status_code == 422
