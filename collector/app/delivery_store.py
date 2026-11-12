"""投递记录存储：独立 SQLite（deliveries.db），与任务库 collector.db 分离。

投递是「至少一次」语义：
- pending   待确认（含正在投递、响应丢失、网络失败、退避等待中）；
- confirmed 已收到任务号 / SHA-256 / 记录数三者全吻合的回执；
- rejected  接收端明确拒绝（校验失败、异字节冲突等），人工排查后可重新发起。

任务号上有 UNIQUE 约束，配合进程内锁，保证重复或并发点击复用同一条投递。
"""
from __future__ import annotations

import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import config

PENDING = "pending"
CONFIRMED = "confirmed"
REJECTED = "rejected"
DELIVERY_STATES = (PENDING, CONFIRMED, REJECTED)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS deliveries (
    id             TEXT PRIMARY KEY,
    task_id        TEXT NOT NULL UNIQUE,
    status         TEXT NOT NULL,
    file_sha256    TEXT NOT NULL,
    record_count   INTEGER NOT NULL,
    attempts       INTEGER NOT NULL DEFAULT 0,
    last_error     TEXT NOT NULL DEFAULT '',
    receipt_id     TEXT,
    receipt_json   TEXT,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    next_attempt_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_deliveries_status ON deliveries(status);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class DeliveryStore:
    def __init__(self, db_path: str | Path | None = None):
        config.DATA_DIR.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            str(db_path or config.DELIVERY_DB_PATH), check_same_thread=False
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def get_for_task(self, task_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM deliveries WHERE task_id = ?", (task_id,)
            ).fetchone()
            return dict(row) if row else None

    def create_if_absent(self, task_id: str, file_sha256: str,
                         record_count: int) -> tuple[dict, bool]:
        """幂等创建。返回 (投递记录, 是否新建)；并发/重复点击复用同一行。"""
        with self._lock:
            existing = self.get_for_task(task_id)
            if existing is not None:
                return existing, False
            delivery_id = uuid.uuid4().hex
            now = _now()
            try:
                self._conn.execute(
                    "INSERT INTO deliveries (id, task_id, status, file_sha256, record_count,"
                    " created_at, updated_at, next_attempt_at)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (delivery_id, task_id, PENDING, file_sha256, record_count,
                     now, now, now),
                )
                self._conn.commit()
            except sqlite3.IntegrityError:
                # 并发点击的数据库层兜底：任务号 UNIQUE，回退到已存在的那一条。
                self._conn.rollback()
                return self.get_for_task(task_id), False
            return self.get_for_task(task_id), True

    def list_all(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM deliveries ORDER BY created_at DESC, id DESC"
            ).fetchall()
            return [dict(r) for r in rows]

    def mark_attempt(self, delivery_id: str, error: str, next_attempt_at: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE deliveries SET attempts = attempts + 1, last_error = ?,"
                " updated_at = ?, next_attempt_at = ? WHERE id = ?",
                (error, _now(), next_attempt_at, delivery_id),
            )
            self._conn.commit()

    def mark_confirmed(self, delivery_id: str, receipt_id: str, receipt_json: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE deliveries SET status = ?, receipt_id = ?, receipt_json = ?,"
                " last_error = '', updated_at = ?, next_attempt_at = NULL WHERE id = ?",
                (CONFIRMED, receipt_id, receipt_json, _now(), delivery_id),
            )
            self._conn.commit()

    def mark_rejected(self, delivery_id: str, error: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE deliveries SET status = ?, last_error = ?, updated_at = ?,"
                " next_attempt_at = NULL WHERE id = ?",
                (REJECTED, error, _now(), delivery_id),
            )
            self._conn.commit()

    def reopen(self, task_id: str) -> dict | None:
        """把被拒绝的投递重新置为待确认（文件未变，仍复用同一投递）。"""
        with self._lock:
            row = self.get_for_task(task_id)
            if row is None:
                return None
            now = _now()
            self._conn.execute(
                "UPDATE deliveries SET status = ?, last_error = '', updated_at = ?,"
                " next_attempt_at = ? WHERE id = ?",
                (PENDING, now, now, row["id"]),
            )
            self._conn.commit()
            return self.get_for_task(task_id)

    def due_pending(self, now_iso: str) -> list[dict[str, Any]]:
        """所有到达退避时间的待确认投递，FIFO。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM deliveries WHERE status = ?"
                " AND (next_attempt_at IS NULL OR next_attempt_at <= ?)"
                " ORDER BY created_at ASC, id ASC",
                (PENDING, now_iso),
            ).fetchall()
            return [dict(r) for r in rows]

    def has_pending(self) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM deliveries WHERE status = ? LIMIT 1", (PENDING,)
            ).fetchone()
            return row is not None

    def close(self) -> None:
        with self._lock:
            self._conn.close()
