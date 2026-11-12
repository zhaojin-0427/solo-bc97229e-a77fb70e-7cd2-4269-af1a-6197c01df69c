"""质量库接收服务核心逻辑。

刻意不依赖任何 Web 框架：Flask 层（app.py）与标准库测试都调用同一套
process_delivery()，保证「容器里跑的」和「测试里验的」是同一份代码。

幂等规则（按任务号去重）：
- 同任务 + 同字节（SHA-256 相同）：不重复入库，返回第一次的原回执；
- 同任务 + 异字节：拒绝（409），不写入任何数据；
- 新任务：按采集端原规则校验 CSV，通过后在一个事务内写入
  回执（deliveries）与全部明细（records，含任务号与原记录序号），
  明细行不去重、不建唯一约束，重复行分别保留。
"""
from __future__ import annotations

import csv
import hashlib
import io
import os
import sqlite3
import tempfile
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote

from validation import validate_csv

SCHEMA = """
CREATE TABLE IF NOT EXISTS deliveries (
    receipt_id    TEXT PRIMARY KEY,
    task_id       TEXT NOT NULL UNIQUE,
    file_sha256   TEXT NOT NULL,
    record_count  INTEGER NOT NULL,
    line          TEXT NOT NULL,
    date          TEXT NOT NULL,
    payload_size  INTEGER NOT NULL,
    created_at    TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS records (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id        TEXT NOT NULL,
    receipt_id     TEXT NOT NULL,
    record_seq     INTEGER NOT NULL,
    line           TEXT NOT NULL,
    date           TEXT NOT NULL,
    sample_no      TEXT NOT NULL,
    measured_value TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_records_task ON records(task_id);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class DeliveryMode:
    """演示开关：入库后是否主动断开响应（模拟响应丢失）。"""

    def __init__(self, drop: bool = False):
        self._lock = threading.Lock()
        self._drop_after_persist = drop

    @property
    def drop_after_persist(self) -> bool:
        with self._lock:
            return self._drop_after_persist

    def set(self, drop: bool) -> None:
        with self._lock:
            self._drop_after_persist = bool(drop)


class QualityStore:
    """独立 SQLite：receipts + 全部明细。所有访问经同一把锁串行化。"""

    def __init__(self, db_path: str | Path | None = None):
        path = Path(db_path or os.environ.get("QUALITYDB_DB", "/data/quality.db"))
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def find_delivery(self, task_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM deliveries WHERE task_id = ?", (task_id,)
            ).fetchone()
            return dict(row) if row else None

    def insert_delivery(self, *, task_id: str, sha256: str, line: str, date: str,
                        rows: list[tuple[str, str, str, str]], payload_size: int) -> dict:
        """一个事务写回执 + 全部明细；重复行原样分别保留。"""
        receipt = {
            "receipt_id": uuid.uuid4().hex,
            "task_id": task_id,
            "file_sha256": sha256,
            "record_count": len(rows),
            "received_at": _now(),
        }
        with self._lock:
            self._conn.execute(
                "INSERT INTO deliveries (receipt_id, task_id, file_sha256, record_count,"
                " line, date, payload_size, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (receipt["receipt_id"], task_id, sha256, len(rows), line, date,
                 payload_size, receipt["received_at"]),
            )
            self._conn.executemany(
                "INSERT INTO records (task_id, receipt_id, record_seq, line, date,"
                " sample_no, measured_value) VALUES (?, ?, ?, ?, ?, ?, ?)",
                [(task_id, receipt["receipt_id"], seq, rline, rdate, sample, value)
                 for seq, (rline, rdate, sample, value) in enumerate(rows, start=1)],
            )
            self._conn.commit()
        return receipt

    def list_deliveries(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM deliveries ORDER BY created_at DESC, receipt_id DESC"
            ).fetchall()
            return [dict(r) for r in rows]

    def get_records(self, task_id: str) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM records WHERE task_id = ? ORDER BY record_seq ASC, id ASC",
                (task_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    def close(self) -> None:
        with self._lock:
            self._conn.close()


@dataclass
class DeliveryResult:
    status: int
    body: dict
    # 仅「新入库且处于断开模式」为 True：调用方应在提交响应前断开连接。
    drop_connection: bool = False


def _err(status: int, code: str, message: str) -> DeliveryResult:
    return DeliveryResult(status, {"ok": False, "error": code, "message": message})


def _parse_rows(payload: bytes) -> list[tuple[str, str, str, str]]:
    """按与校验相同的口径（utf-8-sig、容忍末尾空行）解析明细。"""
    text = payload.decode("utf-8-sig")
    reader = csv.reader(io.StringIO(text))
    next(reader)  # 表头已在校验阶段确认
    rows: list[tuple[str, str, str, str]] = []
    for row in reader:
        if not row or all(not cell.strip() for cell in row):
            continue
        rows.append(tuple(cell.strip() for cell in row))  # type: ignore[arg-type]
    return rows


def process_delivery(store: QualityStore, mode: DeliveryMode,
                     headers: dict[str, str], payload: bytes,
                     drop_socket=None) -> DeliveryResult:
    """处理一次投递请求。headers 的键不区分大小写。

    drop_socket: 可选的零参回调，执行后断开底层 TCP 连接。
    """
    h = {k.lower(): v for k, v in headers.items()}
    task_id = (h.get("x-task-id") or "").strip()
    line = unquote((h.get("x-task-line") or "").strip())
    day = unquote((h.get("x-task-date") or "").strip())
    sha_header = (h.get("x-file-sha256") or "").strip().lower()
    count_header = (h.get("x-record-count") or "").strip()

    if not task_id:
        return _err(400, "missing_task_id", "缺少任务号头 X-Task-Id")
    if not line or not day:
        return _err(400, "missing_task_meta", "缺少任务产线/日期头")
    if not sha_header:
        return _err(400, "missing_sha256", "缺少文件 SHA-256 头")
    try:
        declared_count = int(count_header)
        if declared_count < 0:
            raise ValueError
    except ValueError:
        return _err(400, "bad_record_count", "记录数头 X-Record-Count 必须是非负整数")

    actual_sha = hashlib.sha256(payload).hexdigest()
    if actual_sha != sha_header:
        return _err(400, "sha256_mismatch",
                    "文件字节 SHA-256 与申报值不符，拒绝入库")

    # 幂等判定：同任务号已存在时，同字节回放原回执，异字节直接拒绝。
    with store.lock:
        existing = store.find_delivery(task_id)
    if existing is not None:
        if existing["file_sha256"] == actual_sha:
            return DeliveryResult(200, {
                "ok": True,
                "duplicate": True,
                "receipt": {
                    "receipt_id": existing["receipt_id"],
                    "task_id": existing["task_id"],
                    "file_sha256": existing["file_sha256"],
                    "record_count": existing["record_count"],
                    "received_at": existing["created_at"],
                },
            })
        return _err(409, "content_conflict",
                    f"任务 {task_id} 已投递过不同字节的文件，拒绝覆盖入库")

    # 新任务：落临时文件后复用采集端的同一套校验规则。
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        tmp.write(payload)
        tmp_path = tmp.name
    try:
        result = validate_csv(tmp_path, line, day)
        if not result.ok:
            return _err(400, "validation_failed", f"CSV 校验未通过：{result.message}")
        if result.row_count != declared_count:
            return _err(400, "record_count_mismatch",
                        f"申报记录数 {declared_count} 与实际 {result.row_count} 不符")
        rows = _parse_rows(payload)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    try:
        receipt = store.insert_delivery(
            task_id=task_id, sha256=actual_sha, line=line, date=day,
            rows=rows, payload_size=len(payload),
        )
    except sqlite3.IntegrityError:
        # 并发兜底：检查与落库之间另一请求已写入同任务（UNIQUE 约束拦截）。
        existing = store.find_delivery(task_id)
        if existing is not None and existing["file_sha256"] == actual_sha:
            return DeliveryResult(200, {
                "ok": True, "duplicate": True,
                "receipt": {
                    "receipt_id": existing["receipt_id"],
                    "task_id": existing["task_id"],
                    "file_sha256": existing["file_sha256"],
                    "record_count": existing["record_count"],
                    "received_at": existing["created_at"],
                },
            })
        return _err(409, "content_conflict",
                    f"任务 {task_id} 已存在投递且字节不同，拒绝覆盖入库")

    # 演示模式：入库成功后断开连接，让采集端经历「响应丢失 → 待确认 → 重试」。
    if mode.drop_after_persist and drop_socket is not None:
        try:
            drop_socket()
        except OSError:
            pass
        return DeliveryResult(201, {"ok": True, "dropped": True, "receipt": receipt},
                              drop_connection=True)

    return DeliveryResult(201, {"ok": True, "duplicate": False, "receipt": receipt})
