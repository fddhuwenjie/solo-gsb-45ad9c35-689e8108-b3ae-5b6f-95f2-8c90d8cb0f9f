"""设备批次链 API 测试：链式追加、摘要计算、幂等重放、409 冲突与并发不分叉。"""
import hashlib
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from fastapi.testclient import TestClient

from app.batches import canonical_payload, compute_digest
from app.db import BatchConflict, Database
from app.main import create_app
from app.models import DeviceBatchIn


@pytest.fixture()
def client(tmp_path):
    app = create_app(db_path=str(tmp_path / "test.db"))
    with TestClient(app) as c:
        yield c


def make_batch(sequence, previous_digest, batch_id=None, n_obs=3, **overrides):
    body = {
        "batch_id": batch_id or f"batch-{sequence:04d}",
        "sequence": sequence,
        "previous_digest": previous_digest,
        "observations": [
            {"captured_at": 1_700_000_000.0 + 10.0 * i, "local_id": f"obs-{i:04d}"}
            for i in range(n_obs)
        ],
        "tolerance": 0.5,
        "max_drift": 5e-4,
        "max_missed": 3,
        "max_false": 3,
    }
    body.update(overrides)
    return body


def post_batch(client, device_id, body):
    return client.post(f"/field-devices/{device_id}/batches", json=body)


def db_append(db, device_id, body):
    req = DeviceBatchIn(**body)
    digest = compute_digest(canonical_payload(device_id, req))
    return db.append_batch(device_id, req, digest=digest)


def db_append_status(db, device_id, body):
    try:
        return db_append(db, device_id, body)["status"]
    except BatchConflict:
        return "conflict"


# ---------------- 追加与摘要 ----------------

def test_first_batch_accepted_and_server_side_digest(client):
    body = make_batch(1, "")
    r = post_batch(client, "dev-1", body)
    assert r.status_code == 201
    resp = r.json()
    assert resp["status"] == "accepted"

    # 服务端摘要：排除摘要字段，UTF-8 键序紧凑 JSON 的 SHA-256
    payload = canonical_payload("dev-1", DeviceBatchIn(**body))
    assert "previous_digest" not in payload and "digest" not in payload
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    expected = hashlib.sha256(blob.encode("utf-8")).hexdigest()
    assert resp["batch"]["digest"] == expected == compute_digest(payload)

    batch = resp["batch"]
    assert batch["batch_id"] == body["batch_id"]
    assert batch["sequence"] == 1 and batch["previous_digest"] == ""
    assert batch["observation_count"] == 3
    assert batch["status"] == "accepted" and batch["replay_count"] == 0
    assert (batch["tolerance"], batch["max_drift"]) == (0.5, 5e-4)
    assert (batch["max_missed"], batch["max_false"]) == (3, 3)

    cursor = resp["cursor"]
    assert cursor["head_sequence"] == 1 and cursor["head_digest"] == expected
    assert cursor["head_batch_id"] == body["batch_id"]
    assert cursor["next_sequence"] == 2
    assert cursor["expected_previous_digest"] == expected
    assert cursor["last_status"] == "accepted"


def test_chain_appends_via_cursor(client):
    digest = ""
    for seq in (1, 2, 3):
        r = post_batch(client, "dev-1", make_batch(seq, digest))
        assert r.status_code == 201
        resp = r.json()
        assert resp["batch"]["previous_digest"] == digest
        digest = resp["batch"]["digest"]
        assert resp["cursor"]["head_sequence"] == seq

    cursor = client.get("/field-devices/dev-1/cursor").json()
    assert cursor["head_sequence"] == 3 and cursor["head_digest"] == digest
    assert cursor["next_sequence"] == 4
    assert cursor["expected_previous_digest"] == digest
    assert cursor["last_status"] == "accepted"

    rows = client.get("/field-devices/dev-1/batches").json()
    assert rows["count"] == 3
    assert [b["sequence"] for b in rows["batches"]] == [1, 2, 3]
    assert all(b["status"] == "accepted" for b in rows["batches"])

    detail = client.get("/field-devices/dev-1/batches/batch-0001").json()
    assert detail["sequence"] == 1 and len(detail["observations"]) == 3
    assert detail["observations"][0]["local_id"] == "obs-0000"
    assert detail["observations"][1]["captured_at"] == 1_700_000_010.0


def test_first_batch_must_start_chain(client):
    # 首批 previous_digest 必须为空
    r = post_batch(client, "dev-1", make_batch(1, "0" * 64))
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["reason"] == "stale_chain_head"
    assert detail["expected"] == {"sequence": 1, "previous_digest": ""}

    # 首批序号必须为 1
    r = post_batch(client, "dev-1", make_batch(2, ""))
    assert r.status_code == 409
    assert r.json()["detail"]["expected"]["sequence"] == 1

    # 冲突不落库、不建链
    cursor = client.get("/field-devices/dev-1/cursor").json()
    assert cursor["head_sequence"] == 0 and cursor["next_sequence"] == 1
    assert client.get("/field-devices/dev-1/batches").json()["count"] == 0


def test_stale_chain_head_returns_409_with_expected(client):
    head = post_batch(client, "dev-1", make_batch(1, "")).json()["batch"]["digest"]

    # previous_digest 未引用当前链头
    r = post_batch(client, "dev-1", make_batch(2, "0" * 64))
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["reason"] == "stale_chain_head"
    assert detail["expected"] == {"sequence": 2, "previous_digest": head}
    assert detail["got"]["previous_digest"] == "0" * 64

    # 序号跳跃
    r = post_batch(client, "dev-1", make_batch(5, head))
    assert r.status_code == 409
    assert r.json()["detail"]["expected"]["sequence"] == 2

    # 冲突不推进链头、不落库
    cursor = client.get("/field-devices/dev-1/cursor").json()
    assert cursor["head_sequence"] == 1 and cursor["head_digest"] == head
    assert client.get("/field-devices/dev-1/batches").json()["count"] == 1


# ---------------- 幂等重放 ----------------

def test_identical_replay_returns_original_result(client):
    body = make_batch(1, "")
    original = post_batch(client, "dev-1", body).json()
    assert original["status"] == "accepted"

    r = post_batch(client, "dev-1", body)
    assert r.status_code == 200
    replayed = r.json()
    assert replayed["status"] == "replayed"
    # 返回原结果：摘要与接收时间不变
    assert replayed["batch"]["digest"] == original["batch"]["digest"]
    assert replayed["batch"]["received_at"] == original["batch"]["received_at"]
    assert replayed["batch"]["replay_count"] == 1
    # 链头不变，游标标记最近一次接收为重放
    assert replayed["cursor"]["head_sequence"] == 1
    assert replayed["cursor"]["last_status"] == "replayed"
    assert replayed["cursor"]["last_batch_id"] == body["batch_id"]

    # 重复重放只累计次数，不新增记录
    r = post_batch(client, "dev-1", body)
    assert r.status_code == 200 and r.json()["batch"]["replay_count"] == 2
    rows = client.get("/field-devices/dev-1/batches").json()
    assert rows["count"] == 1 and rows["batches"][0]["replay_count"] == 2
    cursor = client.get("/field-devices/dev-1/cursor").json()
    assert cursor["head_sequence"] == 1 and cursor["last_status"] == "replayed"


def test_replay_old_batch_after_head_advanced(client):
    digest, bodies = "", []
    for seq in (1, 2, 3):
        body = make_batch(seq, digest)
        digest = post_batch(client, "dev-1", body).json()["batch"]["digest"]
        bodies.append(body)

    # 断网恢复后重发最早未确认的批次：返回原结果，链头不变
    r = post_batch(client, "dev-1", bodies[0])
    assert r.status_code == 200
    resp = r.json()
    assert resp["status"] == "replayed"
    assert resp["batch"]["sequence"] == 1
    assert resp["cursor"]["head_sequence"] == 3
    assert resp["cursor"]["next_sequence"] == 4
    assert client.get("/field-devices/dev-1/batches").json()["count"] == 3


# ---------------- 409 冲突 ----------------

def test_same_batch_id_different_payload_conflict(client):
    body = make_batch(1, "")
    original = post_batch(client, "dev-1", body).json()

    # 同 batch_id，观测变化
    r = post_batch(client, "dev-1", make_batch(1, "", n_obs=4))
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["reason"] == "payload_mismatch"
    assert detail["conflict"]["field"] == "batch_id"
    assert detail["conflict"]["existing"]["digest"] == original["batch"]["digest"]
    assert detail["expected"]["sequence"] == 2  # 链头期望值一并返回

    # 同 batch_id，序号变化同样算载荷变化
    r = post_batch(client, "dev-1", make_batch(2, "", batch_id=body["batch_id"]))
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "payload_mismatch"

    assert client.get("/field-devices/dev-1/batches").json()["count"] == 1


def test_sequence_taken_by_other_batch_conflict(client):
    post_batch(client, "dev-1", make_batch(1, ""))
    # 新 batch_id 但序号 1 已被占用
    r = post_batch(client, "dev-1", make_batch(1, "", batch_id="batch-other"))
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["reason"] == "payload_mismatch"
    assert detail["conflict"]["field"] == "sequence"
    assert detail["conflict"]["existing"]["batch_id"] == "batch-0001"


# ---------------- 请求校验 ----------------

def test_batch_validation_errors(client):
    # 观测数量：0 或 >100 → 422
    assert post_batch(client, "d", make_batch(1, "", n_obs=0)).status_code == 422
    assert post_batch(client, "d", make_batch(1, "", n_obs=101)).status_code == 422

    # 观测必须按 (captured_at, local_id) 严格递增
    body = make_batch(1, "")
    body["observations"] = [
        {"captured_at": 2.0, "local_id": "a"},
        {"captured_at": 1.0, "local_id": "b"},
    ]
    assert post_batch(client, "d", body).status_code == 422
    body["observations"] = [
        {"captured_at": 1.0, "local_id": "a"},
        {"captured_at": 1.0, "local_id": "a"},
    ]
    assert post_batch(client, "d", body).status_code == 422

    # captured_at 相同、local_id 递增 → 合法
    body["observations"] = [
        {"captured_at": 1.0, "local_id": "a"},
        {"captured_at": 1.0, "local_id": "b"},
    ]
    assert post_batch(client, "d", body).status_code == 201

    # previous_digest 必须是空或 64 位小写十六进制
    assert post_batch(client, "d2", make_batch(1, "xyz")).status_code == 422
    assert post_batch(client, "d2", make_batch(1, "A" * 64)).status_code == 422

    # 阈值校验同 /judge
    assert post_batch(client, "d3", make_batch(1, "", tolerance=0)).status_code == 422
    assert post_batch(client, "d3", make_batch(1, "", max_drift=1)).status_code == 422
    assert post_batch(client, "d3", make_batch(1, "", max_missed=-1)).status_code == 422
    assert post_batch(client, "d3", make_batch(1, "", max_false=-1)).status_code == 422
    assert post_batch(client, "d3", make_batch(0, "")).status_code == 422


def test_cursor_and_queries_for_unknown_device(client):
    cursor = client.get("/field-devices/ghost/cursor").json()
    assert cursor["head_sequence"] == 0 and cursor["head_digest"] == ""
    assert cursor["next_sequence"] == 1 and cursor["expected_previous_digest"] == ""
    assert cursor["last_status"] is None and cursor["last_batch_id"] is None

    rows = client.get("/field-devices/ghost/batches").json()
    assert rows["count"] == 0 and rows["batches"] == []
    assert client.get("/field-devices/ghost/batches/nope").status_code == 404


def test_chains_are_isolated_per_device(client):
    post_batch(client, "dev-a", make_batch(1, ""))
    post_batch(client, "dev-b", make_batch(1, ""))  # 相同 batch_id 跨设备不冲突
    a = client.get("/field-devices/dev-a/cursor").json()
    b = client.get("/field-devices/dev-b/cursor").json()
    assert a["head_sequence"] == 1 and b["head_sequence"] == 1
    assert a["head_digest"] != b["head_digest"]  # 摘要含 device_id


def test_batches_are_stored_not_judged(client):
    post_batch(client, "dev-1", make_batch(1, ""))
    detail = client.get("/field-devices/dev-1/batches/batch-0001").json()
    assert detail["tolerance"] == 0.5 and detail["max_missed"] == 3
    # 批次上传不产生判定记录
    assert client.get("/judgments").json()["count"] == 0


# ---------------- 并发：不分叉、重放幂等 ----------------

def test_concurrent_appends_never_fork(tmp_path):
    db = Database(str(tmp_path / "c.db"))
    head_digest = db_append(db, "dev-1", make_batch(1, ""))["batch"]["digest"]

    # 8 个并发批次引用同一链头、竞争同一序号
    bodies = [make_batch(2, head_digest, batch_id=f"race-{i}") for i in range(8)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda b: db_append_status(db, "dev-1", b), bodies))

    assert results.count("accepted") == 1
    assert results.count("conflict") == 7
    cursor = db.get_device_cursor("dev-1")
    assert cursor["head_sequence"] == 2  # 链头只推进一次：无分叉
    assert len(db.list_device_batches("dev-1")) == 2


def test_concurrent_identical_replay_is_idempotent(tmp_path):
    db = Database(str(tmp_path / "c.db"))
    body = make_batch(1, "")
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: db_append_status(db, "dev-1", body), range(8)))
    assert results.count("accepted") == 1
    assert results.count("replayed") == 7
    assert len(db.list_device_batches("dev-1")) == 1
    assert db.get_device_cursor("dev-1")["head_sequence"] == 1
