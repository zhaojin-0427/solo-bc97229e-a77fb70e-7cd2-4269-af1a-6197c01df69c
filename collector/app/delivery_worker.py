"""投递 worker：独立后台线程，与采集 worker 完全分离。

关键约束：
- 网络等待只发生在本线程，既不持有任务库（TaskStore）锁，也不占用门户会话，
  原采集、查询、下载不受影响；
- 投递是至少一次语义：任何「没拿到可确认回执」的情况都保持 pending，
  用原任务号、原文件字节重试，退避到期自动再投；
- 只有回执的任务号 / SHA-256 / 记录数三者全吻合才置 confirmed；
- 进程重启后 pending 记录自然到期被本线程接续，不漏投。
接收端的任务号唯一约束保证重复投递不会重复入库。
"""
from __future__ import annotations

import csv
import json
import logging
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import config
from .delivery_client import (
    DeliveryRejected,
    DeliveryTransportError,
    post_delivery,
    sha256_file,
    verify_receipt,
)
from .delivery_store import DeliveryStore
from .models import TaskStore

log = logging.getLogger("collector.delivery")


def count_csv_records(path: str | Path) -> int:
    """与校验口径一致：跳过表头与末尾空行，统计数据记录数。"""
    count = 0
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh)
        next(reader, None)
        for row in reader:
            if row and any(cell.strip() for cell in row):
                count += 1
    return count


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class DeliveryWorker(threading.Thread):
    def __init__(self, delivery_store: DeliveryStore, task_store: TaskStore):
        super().__init__(name="delivery-worker", daemon=True)
        self.deliveries = delivery_store
        self.tasks = task_store
        self._wake = threading.Event()
        # 注意不能命名为 _stop：threading.Thread 自身有同名内部方法。
        self._stop_event = threading.Event()

    def notify(self) -> None:
        self._wake.set()

    def shutdown(self) -> None:
        self._stop_event.set()
        self._wake.set()

    def run(self) -> None:
        log.info("投递 worker 已启动，目标质量库 %s", config.QUALITYDB_URL)
        while not self._stop_event.is_set():
            # 退避期间也能被新投递立即唤醒；最长一个轮询周期自然醒来。
            self._wake.wait(config.POLL_INTERVAL)
            self._wake.clear()
            try:
                due = self.deliveries.due_pending(_utcnow().isoformat(timespec="seconds"))
            except Exception:
                log.exception("读取待确认投递失败")
                continue
            for delivery in due:
                if self._stop_event.is_set():
                    break
                self._process(delivery)

    # ------------------------------------------------------------------
    def _process(self, delivery: dict) -> None:
        delivery_id = delivery["task_id"]
        # 读取任务信息：任务库锁只在这一下本地查询期间持有，网络 IO 在锁外。
        task = self.tasks.get_task(delivery_id)
        if task is None or not task.get("published_file"):
            self.deliveries.mark_rejected(
                delivery["id"], "关联任务或已发布文件不存在，无法投递"
            )
            return
        csv_path = config.PUBLISHED_DIR / task["published_file"]
        if not csv_path.exists():
            self.deliveries.mark_rejected(delivery["id"], f"发布文件缺失：{csv_path.name}")
            return

        # 原文件字节 + 原任务号：每次重试都是同一份内容。
        try:
            payload = csv_path.read_bytes()
            actual_sha = sha256_file(csv_path)
        except OSError as exc:
            self._schedule_retry(delivery, f"读取发布文件失败：{exc}")
            return
        if actual_sha != delivery["file_sha256"]:
            self.deliveries.mark_rejected(
                delivery["id"], "发布文件在投递前发生变化（SHA-256 不一致），拒绝投递"
            )
            return

        try:
            envelope = post_delivery(
                base_url=config.QUALITYDB_URL,
                task_id=task["id"],
                line=task["line"],
                date=task["date"],
                payload=payload,
                file_sha256=actual_sha,
                record_count=delivery["record_count"],
            )
        except DeliveryTransportError as exc:
            # 响应丢失/超时/断连：对端可能已入库，保持待确认，原文件重试。
            log.warning("任务 %s 投递响应不可确认，保持待确认：%s", task["id"], exc)
            self._schedule_retry(delivery, str(exc))
            return
        except DeliveryRejected as exc:
            log.warning("任务 %s 被质量库拒绝：%s", task["id"], exc.message)
            self.deliveries.mark_rejected(delivery["id"], exc.message)
            return

        receipt = envelope.get("receipt") or {}
        ok, message = verify_receipt(
            receipt,
            task_id=task["id"],
            file_sha256=actual_sha,
            record_count=delivery["record_count"],
        )
        if not ok:
            # 回执对不上：不确认，按待确认重试，让接收端幂等逻辑给出可信回执。
            log.warning("任务 %s 回执校验未通过：%s", task["id"], message)
            self._schedule_retry(delivery, f"回执校验未通过：{message}")
            return

        self.deliveries.mark_confirmed(
            delivery["id"],
            receipt_id=receipt.get("receipt_id"),
            receipt_json=json.dumps(receipt, ensure_ascii=False, sort_keys=True),
        )
        log.info(
            "任务 %s 投递成功（%s，累计失败重试 %s 次，重复投递=%s）",
            task["id"], message, delivery["attempts"], envelope.get("duplicate"),
        )

    def _schedule_retry(self, delivery: dict, error: str) -> None:
        # 指数退避：min * 2**attempts，封顶 max。
        backoff = min(
            config.DELIVERY_BACKOFF_MIN * (2 ** delivery["attempts"]),
            config.DELIVERY_BACKOFF_MAX,
        )
        next_at = (_utcnow() + timedelta(seconds=backoff)).isoformat(timespec="seconds")
        self.deliveries.mark_attempt(delivery["id"], error, next_at)
