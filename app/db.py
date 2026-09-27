"""SQLite 持久层：不可变特征库版本、判定记录与设备批次链。

特征库以版本为单位不可变存储：

- ``library_versions``：版本元数据，状态机 draft → active → historical；
- ``library_version_aids``：每个版本的完整航标快照（版本不可变，可复算差异）；
- ``library_state``：单行表，指向当前激活版本。

设备批次链（每台设备一条仅追加链）：

- ``device_batches``：批次记录，(device_id, batch_id) 与 (device_id, sequence) 唯一；
- ``device_chain_state``：每设备单行链头（head_sequence/head_digest）与最近一次
  接收事件（accepted/replayed），校验、追加与链头更新在同一事务内完成。

首次启动时把旧版平铺 ``aids`` 表整体迁移为初始激活版本（source='migration'），
并给 ``judgments`` 表补充 ``library_version`` 列；迁移在单个事务内完成。
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS library_versions (
    version_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    status       TEXT NOT NULL CHECK (status IN ('draft', 'active', 'historical')),
    base_version INTEGER REFERENCES library_versions(version_id),
    source       TEXT NOT NULL DEFAULT 'api',
    created_at   REAL NOT NULL,
    activated_at REAL
);
CREATE TABLE IF NOT EXISTS library_version_aids (
    version_id INTEGER NOT NULL REFERENCES library_versions(version_id),
    aid_id     TEXT NOT NULL,
    period     REAL NOT NULL,
    flashes    TEXT NOT NULL,
    PRIMARY KEY (version_id, aid_id)
);
CREATE TABLE IF NOT EXISTS library_state (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    version_id INTEGER NOT NULL REFERENCES library_versions(version_id)
);
CREATE TABLE IF NOT EXISTS judgments (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at      REAL NOT NULL,
    status          TEXT NOT NULL,
    best_aid_id     TEXT,
    matched         INTEGER NOT NULL DEFAULT 0,
    missed          INTEGER NOT NULL DEFAULT 0,
    false_count     INTEGER NOT NULL DEFAULT 0,
    drift           REAL,
    library_version INTEGER,
    request         TEXT NOT NULL,
    response        TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS device_batches (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id         TEXT NOT NULL,
    batch_id          TEXT NOT NULL,
    sequence          INTEGER NOT NULL,
    previous_digest   TEXT NOT NULL,
    digest            TEXT NOT NULL,
    observation_count INTEGER NOT NULL,
    tolerance         REAL NOT NULL,
    max_drift         REAL NOT NULL,
    max_missed        INTEGER NOT NULL,
    max_false         INTEGER NOT NULL,
    observations      TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'accepted' CHECK (status IN ('accepted')),
    replay_count      INTEGER NOT NULL DEFAULT 0,
    received_at       REAL NOT NULL,
    last_replayed_at  REAL,
    UNIQUE (device_id, batch_id),
    UNIQUE (device_id, sequence)
);
CREATE TABLE IF NOT EXISTS device_chain_state (
    device_id      TEXT PRIMARY KEY,
    head_sequence  INTEGER NOT NULL,
    head_digest    TEXT NOT NULL,
    head_batch_id  TEXT NOT NULL,
    last_status    TEXT NOT NULL CHECK (last_status IN ('accepted', 'replayed')),
    last_batch_id  TEXT NOT NULL,
    updated_at     REAL NOT NULL
);
"""


class VersionNotFound(Exception):
    """引用的版本不存在。"""


class VersionStateError(Exception):
    """版本状态不允许该操作（如激活非草稿、以草稿为基线）。"""


class VersionConflict(Exception):
    """激活时草稿的 base_version 已不再是当前激活版本（乐观并发冲突）。"""


class BatchConflict(Exception):
    """批次与设备链冲突（载荷变化或链头过期）；detail 为结构化的 409 响应体。"""

    def __init__(self, detail: dict):
        super().__init__(detail["message"])
        self.detail = detail


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _diff_aids(base_aids: list[dict], new_aids: list[dict]) -> dict:
    """按航标编号比较两个快照，给出增/删/改（period 或 flashes 不同即为改）。"""
    base = {a["aid_id"]: a for a in base_aids}
    new = {a["aid_id"]: a for a in new_aids}
    return {
        "added": sorted(k for k in new if k not in base),
        "removed": sorted(k for k in base if k not in new),
        "modified": sorted(
            k for k in new.keys() & base.keys()
            if base[k]["period"] != new[k]["period"] or base[k]["flashes"] != new[k]["flashes"]
        ),
    }


class Database:
    """按操作开启连接的轻量封装：串行写由锁保证，多步写包在单个事务内。"""

    def __init__(self, path: str):
        self._path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        with closing(self._connect()) as conn:
            conn.executescript(SCHEMA)
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._migrate(conn)
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    @contextmanager
    def _write_tx(self):
        """串行化的单事务写：BEGIN IMMEDIATE … COMMIT，异常时 ROLLBACK。"""
        with self._lock, closing(self._connect()) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    # ---------------- 首次迁移 ----------------

    def _migrate(self, conn) -> None:
        tables = {
            r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if "aids" in tables:  # 旧版平铺特征库 → 初始激活版本
            rows = conn.execute(
                "SELECT aid_id, period, flashes FROM aids ORDER BY aid_id"
            ).fetchall()
            now = time.time()
            cur = conn.execute(
                "INSERT INTO library_versions (status, base_version, source, created_at, activated_at) "
                "VALUES ('active', NULL, 'migration', ?, ?)",
                (now, now),
            )
            vid = int(cur.lastrowid)
            conn.executemany(
                "INSERT INTO library_version_aids (version_id, aid_id, period, flashes) "
                "VALUES (?, ?, ?, ?)",
                [(vid, r["aid_id"], r["period"], r["flashes"]) for r in rows],
            )
            conn.execute("INSERT INTO library_state (id, version_id) VALUES (1, ?)", (vid,))
            conn.execute("DROP TABLE aids")
        cols = {r[1] for r in conn.execute("PRAGMA table_info(judgments)")}
        if "library_version" not in cols:
            conn.execute("ALTER TABLE judgments ADD COLUMN library_version INTEGER")

    # ---------------- 特征库版本 ----------------

    @staticmethod
    def _aids_of(conn, version_id: int) -> list[dict]:
        rows = conn.execute(
            "SELECT aid_id, period, flashes FROM library_version_aids "
            "WHERE version_id = ? ORDER BY aid_id",
            (version_id,),
        ).fetchall()
        return [
            {"aid_id": r["aid_id"], "period": r["period"], "flashes": json.loads(r["flashes"])}
            for r in rows
        ]

    def _version_meta(self, conn, row) -> dict:
        vid = row["version_id"]
        base = row["base_version"]
        count = conn.execute(
            "SELECT COUNT(*) AS c FROM library_version_aids WHERE version_id = ?", (vid,)
        ).fetchone()["c"]
        diff = None
        if base is not None:
            diff = _diff_aids(self._aids_of(conn, base), self._aids_of(conn, vid))
        return {
            "version_id": vid,
            "status": row["status"],
            "base_version": base,
            "source": row["source"],
            "aid_count": count,
            "created_at": _iso(row["created_at"]),
            "activated_at": _iso(row["activated_at"]) if row["activated_at"] is not None else None,
            "diff": diff,
        }

    def create_version(self, aids, *, base_version: int, source: str = "api") -> dict:
        """保存完整特征库为草稿版本；基线可以是当前激活版本，也可以是历史版本。"""
        rows = [(a.aid_id, a.period, json.dumps(a.flashes)) for a in aids]
        with self._write_tx() as conn:
            base = conn.execute(
                "SELECT status FROM library_versions WHERE version_id = ?", (base_version,)
            ).fetchone()
            if base is None:
                raise VersionNotFound(f"base_version {base_version} not found")
            if base["status"] == "draft":
                raise VersionStateError(
                    f"base_version {base_version} is still a draft; "
                    "base must be an activated (current or historical) version"
                )
            cur = conn.execute(
                "INSERT INTO library_versions (status, base_version, source, created_at) "
                "VALUES ('draft', ?, ?, ?)",
                (base_version, source, time.time()),
            )
            vid = int(cur.lastrowid)
            conn.executemany(
                "INSERT INTO library_version_aids (version_id, aid_id, period, flashes) "
                "VALUES (?, ?, ?, ?)",
                [(vid, aid_id, period, flashes) for aid_id, period, flashes in rows],
            )
        return self.get_version(vid)

    def activate_version(self, version_id: int) -> dict:
        """单事务激活：确认草稿的 base_version 仍是当前激活版本，然后原子切换。"""
        with self._write_tx() as conn:
            row = conn.execute(
                "SELECT * FROM library_versions WHERE version_id = ?", (version_id,)
            ).fetchone()
            if row is None:
                raise VersionNotFound(f"version {version_id} not found")
            if row["status"] != "draft":
                raise VersionStateError(
                    f"version {version_id} is {row['status']}; only a draft can be activated"
                )
            state = conn.execute("SELECT version_id FROM library_state WHERE id = 1").fetchone()
            current = state["version_id"] if state is not None else None
            if row["base_version"] != current:
                raise VersionConflict(
                    f"base_version {row['base_version']} is no longer the active version "
                    f"(currently active: {current}); re-create the draft on top of the "
                    "current active version and retry"
                )
            now = time.time()
            conn.execute(
                "UPDATE library_versions SET status = 'historical' WHERE status = 'active'"
            )
            conn.execute(
                "UPDATE library_versions SET status = 'active', activated_at = ? "
                "WHERE version_id = ?",
                (now, version_id),
            )
            conn.execute(
                "INSERT INTO library_state (id, version_id) VALUES (1, ?) "
                "ON CONFLICT(id) DO UPDATE SET version_id = excluded.version_id",
                (version_id,),
            )
        return self.get_version(version_id)

    def publish_import(self, aids, *, replace: bool) -> tuple[int, dict]:
        """/library/import：基于当前激活版本合并/替换，单事务创建并直接激活新版本。"""
        with self._write_tx() as conn:
            state = conn.execute("SELECT version_id FROM library_state WHERE id = 1").fetchone()
            base_id = state["version_id"] if state is not None else None
            merged: dict[str, dict] = {}
            if not replace and base_id is not None:
                for a in self._aids_of(conn, base_id):
                    merged[a["aid_id"]] = a
            for a in aids:
                merged[a.aid_id] = {
                    "aid_id": a.aid_id,
                    "period": a.period,
                    "flashes": list(a.flashes),
                }
            now = time.time()
            conn.execute(
                "UPDATE library_versions SET status = 'historical' WHERE status = 'active'"
            )
            cur = conn.execute(
                "INSERT INTO library_versions (status, base_version, source, created_at, activated_at) "
                "VALUES ('active', ?, 'import', ?, ?)",
                (base_id, now, now),
            )
            vid = int(cur.lastrowid)
            conn.executemany(
                "INSERT INTO library_version_aids (version_id, aid_id, period, flashes) "
                "VALUES (?, ?, ?, ?)",
                [
                    (vid, a["aid_id"], a["period"], json.dumps(a["flashes"]))
                    for a in merged.values()
                ],
            )
            conn.execute(
                "INSERT INTO library_state (id, version_id) VALUES (1, ?) "
                "ON CONFLICT(id) DO UPDATE SET version_id = excluded.version_id",
                (vid,),
            )
        return len(aids), self.get_version(vid)

    def list_versions(self) -> list[dict]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM library_versions ORDER BY version_id"
            ).fetchall()
            return [self._version_meta(conn, r) for r in rows]

    def get_version(self, version_id: int) -> dict | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM library_versions WHERE version_id = ?", (version_id,)
            ).fetchone()
            return self._version_meta(conn, row) if row is not None else None

    def get_version_aids(self, version_id: int) -> list[dict]:
        with closing(self._connect()) as conn:
            return self._aids_of(conn, version_id)

    def get_active_snapshot(self) -> tuple[int | None, list[dict]]:
        """/judge 请求开始时固定一次：同一读事务内取激活版本号与完整快照。"""
        with closing(self._connect()) as conn:
            try:
                conn.execute("BEGIN")
                state = conn.execute(
                    "SELECT version_id FROM library_state WHERE id = 1"
                ).fetchone()
                vid = state["version_id"] if state is not None else None
                aids = self._aids_of(conn, vid) if vid is not None else []
            finally:
                conn.rollback()  # 只读事务，释放读锁
        return vid, aids

    # ---------------- 判定记录 ----------------

    def save_judgment(
        self,
        *,
        status: str,
        best_aid_id: str | None,
        matched: int,
        missed: int,
        false_count: int,
        drift: float | None,
        library_version: int | None,
        request: dict,
        response: dict,
    ) -> int:
        with self._write_tx() as conn:
            cur = conn.execute(
                "INSERT INTO judgments "
                "(created_at, status, best_aid_id, matched, missed, false_count, drift, "
                "library_version, request, response) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    time.time(),
                    status,
                    best_aid_id,
                    matched,
                    missed,
                    false_count,
                    drift,
                    library_version,
                    json.dumps(request, ensure_ascii=False),
                    json.dumps(response, ensure_ascii=False),
                ),
            )
            return int(cur.lastrowid)

    def list_judgments(self, limit: int = 50, offset: int = 0) -> list[dict]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT id, created_at, status, best_aid_id, matched, missed, false_count, "
                "drift, library_version "
                "FROM judgments ORDER BY id DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
        return [
            {
                "id": r["id"],
                "created_at": _iso(r["created_at"]),
                "status": r["status"],
                "best_aid_id": r["best_aid_id"],
                "matched": r["matched"],
                "missed": r["missed"],
                "false": r["false_count"],
                "drift": r["drift"],
                "library_version": r["library_version"],
            }
            for r in rows
        ]

    def get_judgment(self, judgment_id: int) -> dict | None:
        with closing(self._connect()) as conn:
            r = conn.execute(
                "SELECT id, created_at, status, best_aid_id, matched, missed, false_count, "
                "drift, library_version, request, response "
                "FROM judgments WHERE id = ?",
                (judgment_id,),
            ).fetchone()
        if r is None:
            return None
        return {
            "id": r["id"],
            "created_at": _iso(r["created_at"]),
            "status": r["status"],
            "best_aid_id": r["best_aid_id"],
            "matched": r["matched"],
            "missed": r["missed"],
            "false": r["false_count"],
            "drift": r["drift"],
            "library_version": r["library_version"],
            "request": json.loads(r["request"]),
            "response": json.loads(r["response"]),
        }

    # ---------------- 设备批次链 ----------------

    @staticmethod
    def _expected_of(state) -> dict:
        """设备链当前期望的下一批序号与前序摘要（无链状态时为首批）。"""
        if state is None:
            return {"sequence": 1, "previous_digest": ""}
        return {
            "sequence": state["head_sequence"] + 1,
            "previous_digest": state["head_digest"],
        }

    @staticmethod
    def _conflict_detail(
        device_id: str, state, req, previous_digest: str,
        *, reason: str, message: str, conflict: dict | None = None,
    ) -> dict:
        detail = {
            "error": "batch_conflict",
            "reason": reason,
            "message": message,
            "device_id": device_id,
            "expected": Database._expected_of(state),
            "got": {
                "batch_id": req.batch_id,
                "sequence": req.sequence,
                "previous_digest": previous_digest,
            },
        }
        if conflict is not None:
            detail["conflict"] = conflict
        return detail

    @staticmethod
    def _touch_chain_state(conn, device_id: str, status: str, batch_id: str, now: float) -> None:
        """更新设备链最近一次接收事件（重放不推进链头）。"""
        conn.execute(
            "UPDATE device_chain_state SET last_status = ?, last_batch_id = ?, updated_at = ? "
            "WHERE device_id = ?",
            (status, batch_id, now, device_id),
        )

    def _cursor_of(self, conn, device_id: str) -> dict:
        state = conn.execute(
            "SELECT * FROM device_chain_state WHERE device_id = ?", (device_id,)
        ).fetchone()
        if state is None:
            return {
                "device_id": device_id,
                "head_sequence": 0,
                "head_digest": "",
                "head_batch_id": None,
                "next_sequence": 1,
                "expected_previous_digest": "",
                "last_status": None,
                "last_batch_id": None,
                "updated_at": None,
            }
        return {
            "device_id": device_id,
            "head_sequence": state["head_sequence"],
            "head_digest": state["head_digest"],
            "head_batch_id": state["head_batch_id"],
            "next_sequence": state["head_sequence"] + 1,
            "expected_previous_digest": state["head_digest"],
            "last_status": state["last_status"],
            "last_batch_id": state["last_batch_id"],
            "updated_at": _iso(state["updated_at"]),
        }

    @staticmethod
    def _batch_record(row, *, include_observations: bool = False) -> dict:
        record = {
            "device_id": row["device_id"],
            "batch_id": row["batch_id"],
            "sequence": row["sequence"],
            "previous_digest": row["previous_digest"],
            "digest": row["digest"],
            "observation_count": row["observation_count"],
            "tolerance": row["tolerance"],
            "max_drift": row["max_drift"],
            "max_missed": row["max_missed"],
            "max_false": row["max_false"],
            "status": row["status"],
            "replay_count": row["replay_count"],
            "received_at": _iso(row["received_at"]),
            "last_replayed_at": (
                _iso(row["last_replayed_at"]) if row["last_replayed_at"] is not None else None
            ),
        }
        if include_observations:
            record["observations"] = json.loads(row["observations"])
        return record

    def append_batch(self, device_id: str, req, *, digest: str) -> dict:
        """单事务追加设备批次：幂等重放判定、链头校验、插入记录并推进链头。

        相同 batch_id/sequence 且载荷一致的重放返回原记录（status='replayed'）；
        载荷变化或链头过期抛出 BatchConflict（detail 含链头期望值）。
        BEGIN IMMEDIATE 串行化并发写，并发提交不会形成分叉。
        """
        previous_digest = req.previous_digest or ""
        observations = [
            {"captured_at": o.captured_at, "local_id": o.local_id} for o in req.observations
        ]
        now = time.time()
        with self._write_tx() as conn:
            state = conn.execute(
                "SELECT * FROM device_chain_state WHERE device_id = ?", (device_id,)
            ).fetchone()

            by_batch = conn.execute(
                "SELECT * FROM device_batches WHERE device_id = ? AND batch_id = ?",
                (device_id, req.batch_id),
            ).fetchone()
            if by_batch is not None:
                if by_batch["digest"] == digest and by_batch["previous_digest"] == previous_digest:
                    # 相同载荷重放：不追加记录，累计重放次数并返回原结果
                    conn.execute(
                        "UPDATE device_batches SET replay_count = replay_count + 1, "
                        "last_replayed_at = ? WHERE id = ?",
                        (now, by_batch["id"]),
                    )
                    self._touch_chain_state(conn, device_id, "replayed", req.batch_id, now)
                    row = conn.execute(
                        "SELECT * FROM device_batches WHERE id = ?", (by_batch["id"],)
                    ).fetchone()
                    result = {
                        "status": "replayed",
                        "batch": self._batch_record(row),
                        "cursor": self._cursor_of(conn, device_id),
                    }
                else:
                    raise BatchConflict(
                        self._conflict_detail(
                            device_id, state, req, previous_digest,
                            reason="payload_mismatch",
                            message=(
                                f"batch_id {req.batch_id!r} already exists on device "
                                f"{device_id!r} with a different payload"
                            ),
                            conflict={
                                "field": "batch_id",
                                "existing": {
                                    "batch_id": by_batch["batch_id"],
                                    "sequence": by_batch["sequence"],
                                    "digest": by_batch["digest"],
                                    "previous_digest": by_batch["previous_digest"],
                                },
                            },
                        )
                    )
            else:
                by_seq = conn.execute(
                    "SELECT * FROM device_batches WHERE device_id = ? AND sequence = ?",
                    (device_id, req.sequence),
                ).fetchone()
                if by_seq is not None:
                    # 序号已被其他 batch_id 占用（载荷一致必然 batch_id 也一致，已命中上一分支）
                    raise BatchConflict(
                        self._conflict_detail(
                            device_id, state, req, previous_digest,
                            reason="payload_mismatch",
                            message=(
                                f"sequence {req.sequence} on device {device_id!r} is already "
                                f"taken by batch_id {by_seq['batch_id']!r}"
                            ),
                            conflict={
                                "field": "sequence",
                                "existing": {
                                    "batch_id": by_seq["batch_id"],
                                    "sequence": by_seq["sequence"],
                                    "digest": by_seq["digest"],
                                    "previous_digest": by_seq["previous_digest"],
                                },
                            },
                        )
                    )
                expected = self._expected_of(state)
                if (
                    req.sequence != expected["sequence"]
                    or previous_digest != expected["previous_digest"]
                ):
                    raise BatchConflict(
                        self._conflict_detail(
                            device_id, state, req, previous_digest,
                            reason="stale_chain_head",
                            message=(
                                f"batch does not extend the current chain head of device "
                                f"{device_id!r}: expected sequence {expected['sequence']} "
                                f"with previous_digest {expected['previous_digest']!r}"
                            ),
                        )
                    )
                conn.execute(
                    "INSERT INTO device_batches "
                    "(device_id, batch_id, sequence, previous_digest, digest, "
                    "observation_count, tolerance, max_drift, max_missed, max_false, "
                    "observations, status, replay_count, received_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'accepted', 0, ?)",
                    (
                        device_id, req.batch_id, req.sequence, previous_digest, digest,
                        len(observations), req.tolerance, req.max_drift, req.max_missed,
                        req.max_false, json.dumps(observations, ensure_ascii=False), now,
                    ),
                )
                conn.execute(
                    "INSERT INTO device_chain_state "
                    "(device_id, head_sequence, head_digest, head_batch_id, last_status, "
                    "last_batch_id, updated_at) VALUES (?, ?, ?, ?, 'accepted', ?, ?) "
                    "ON CONFLICT(device_id) DO UPDATE SET "
                    "head_sequence = excluded.head_sequence, "
                    "head_digest = excluded.head_digest, "
                    "head_batch_id = excluded.head_batch_id, "
                    "last_status = 'accepted', "
                    "last_batch_id = excluded.last_batch_id, "
                    "updated_at = excluded.updated_at",
                    (device_id, req.sequence, digest, req.batch_id, req.batch_id, now),
                )
                row = conn.execute(
                    "SELECT * FROM device_batches WHERE device_id = ? AND batch_id = ?",
                    (device_id, req.batch_id),
                ).fetchone()
                result = {
                    "status": "accepted",
                    "batch": self._batch_record(row),
                    "cursor": self._cursor_of(conn, device_id),
                }
        return result

    def get_device_cursor(self, device_id: str) -> dict:
        """设备批次链游标；设备尚无批次时返回初始游标（首批期望值）。"""
        with closing(self._connect()) as conn:
            return self._cursor_of(conn, device_id)

    def list_device_batches(
        self, device_id: str, *, limit: int = 50, offset: int = 0
    ) -> list[dict]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM device_batches WHERE device_id = ? "
                "ORDER BY sequence LIMIT ? OFFSET ?",
                (device_id, limit, offset),
            ).fetchall()
            return [self._batch_record(r) for r in rows]

    def get_device_batch(self, device_id: str, batch_id: str) -> dict | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM device_batches WHERE device_id = ? AND batch_id = ?",
                (device_id, batch_id),
            ).fetchone()
            if row is None:
                return None
            return self._batch_record(row, include_observations=True)
