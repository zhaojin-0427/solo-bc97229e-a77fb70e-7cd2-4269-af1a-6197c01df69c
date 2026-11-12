"""投递链路端到端测试（纯标准库，无需 Flask / 浏览器）。

覆盖：
1. 正常投递：明细（含任务号、原记录序号）与回执写入质量库独立 SQLite；
2. 同任务同字节重试：回放原回执，不重复入库；重复行分别保留；
3. 同任务异字节：409 拒绝；
4. 接收端入库后断开响应：采集端保持待确认 → 原文件自动重试 → 成功且只入库一次；
5. 两端「重启」接续：pending 落库后新建 store/worker，仍能确认，不漏不重；
6. 回执任务号/SHA-256/记录数不符：不确认，保持待确认重试，修正后成功；
7. 校验失败的 CSV 被接收端拒绝（rejected）；
8. 并发/重复发起投递：复用同一条投递（任务号 UNIQUE）。

质量库用 wsgiref + 真实 TCP socket 起服务，断开模式直接关 socket，
与容器中 Flask 开发服务器的行为一致。
"""
import importlib
import json
import os
import shutil
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from wsgiref.simple_server import WSGIRequestHandler, make_server

ROOT = Path(__file__).resolve().parents[1]

# --- 质量库模块（顶层包名 validation/receiver，无 Flask 依赖）-------------
QROOT = str(ROOT / "qualitydb")
sys.path.insert(0, QROOT)
import validation as q_validation  # noqa: E402
import receiver as q_receiver  # noqa: E402

# --- 采集端配置必须在 import app 之前注入 --------------------------------
TMP = Path(tempfile.mkdtemp(prefix="delivery-e2e-"))
os.environ["DATA_DIR"] = str(TMP / "collector-data")
os.environ["QUALITYDB_DB"] = str(TMP / "quality-data" / "quality.db")
os.environ["QUALITYDB_URL"] = "http://127.0.0.1:8131"  # 占位，起服务后覆盖
os.environ["POLL_INTERVAL"] = "0.1"
os.environ["DELIVERY_BACKOFF_MIN"] = "0.2"
os.environ["DELIVERY_BACKOFF_MAX"] = "1"
os.environ["DELIVERY_TIMEOUT_SECONDS"] = "5"

sys.path.insert(0, str(ROOT / "collector"))
from app import config  # noqa: E402
from app.delivery_client import post_delivery, sha256_file, verify_receipt  # noqa: E402
from app.delivery_store import CONFIRMED, PENDING, REJECTED, DeliveryStore  # noqa: E402
from app.delivery_worker import DeliveryWorker, count_csv_records  # noqa: E402

GOOD_CSV = (
    "﻿产线,日期,样品号,测量值\n"
    "A线,2026-11-10,A-001,12.34\n"
    "A线,2026-11-10,A-002,56.78\n"
    "A线,2026-11-10,A-002,56.78\n"   # 与上一行完全重复：应分别保留
    "A线,2026-11-10,A-003,90.00\n"
)
BAD_CSV = (
    "产线,日期,样品号,测量值\n"
    "A线,2026-11-10,A-001,12.34\n"
    "X线,2026-11-10,X-002,56.78\n"   # 产线不符
)


class _ServerMode:
    """接收端演示模式：normal / drop / bad_receipt。"""

    def __init__(self):
        self.lock = threading.Lock()
        self.mode = "normal"

    def set(self, mode):
        with self.lock:
            self.mode = mode

    def get(self):
        with self.lock:
            return self.mode


def _build_wsgi_app(store, mode):
    def application(environ, start_response):
        if environ.get("PATH_INFO") != "/api/deliveries" or environ.get("REQUEST_METHOD") != "POST":
            start_response("404 Not Found", [("Content-Type", "application/json")])
            return [b'{"ok": false}']
        length = int(environ.get("CONTENT_LENGTH") or 0)
        payload = environ["wsgi.input"].read(length)
        headers = {k[5:].replace("_", "-"): v for k, v in environ.items()
                   if k.startswith("HTTP_")}

        def drop_socket():
            # 生产 Werkzeug 与本测试 handler 都把 socket 放在 werkzeug.socket。
            sock = environ.get("werkzeug.socket")
            if sock is None:
                raise OSError("no socket available")
            sock.shutdown(2)
            sock.close()

        # 断连模式仅对「新入库」生效（与真实 Flask 壳一致）。
        drop_mode = q_receiver.DeliveryMode(mode.get() == "drop")
        result = q_receiver.process_delivery(
            store, drop_mode, headers, payload, drop_socket=drop_socket,
        )
        if result.drop_connection:
            # 模拟 Flask 壳：连接已关，不再写响应。
            return [b""]
        if mode.get() == "bad_receipt" and result.status in (200, 201):
            # 篡改回执三要素，测试采集端拒绝确认。
            receipt = result.body["receipt"]
            result.body["receipt"] = dict(receipt, record_count=receipt["record_count"] + 999)
        body = json.dumps(result.body, ensure_ascii=False).encode("utf-8")
        start_response(f"{result.status} Mock", [("Content-Type", "application/json")])
        return [body]

    return application


class _QuietWSGIHandler(WSGIRequestHandler):
    def get_environ(self):
        # WSGIRequestHandler 经 get_environ 构造 environ；注入与生产 Werkzeug
        # 同名的 socket 键，使「入库后断连」演示在测试中同样可行。
        environ = super().get_environ()
        environ["werkzeug.socket"] = self.request
        return environ

    def handle(self):
        try:
            super().handle()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass


class FakeTaskStore:
    """采集端任务库的最小替身：只提供 worker 用到的 get_task。"""

    def __init__(self, tasks):
        self._tasks = tasks

    def get_task(self, task_id):
        return self._tasks.get(task_id)


class DeliveryE2ETest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.qstore = q_receiver.QualityStore(os.environ["QUALITYDB_DB"])
        cls.mode = _ServerMode()
        cls.httpd = make_server(
            "127.0.0.1", 8131, _build_wsgi_app(cls.qstore, cls.mode),
            handler_class=_QuietWSGIHandler,
        )
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()
        config.QUALITYDB_URL = "http://127.0.0.1:8131"
        # 等待端口就绪
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                urllib.request.urlopen("http://127.0.0.1:8131/", timeout=1)
            except urllib.error.HTTPError:
                break
            except OSError:
                time.sleep(0.1)

        cls.published = config.PUBLISHED_DIR
        cls.published.mkdir(parents=True, exist_ok=True)
        cls.good_path = cls.published / "A线_2026-11-10_good.csv"
        cls.good_path.write_text(GOOD_CSV, encoding="utf-8")
        cls.bad_path = cls.published / "A线_2026-11-10_bad.csv"
        cls.bad_path.write_text(BAD_CSV, encoding="utf-8")
        cls.good_sha = sha256_file(cls.good_path)
        cls.good_count = count_csv_records(cls.good_path)
        assert cls.good_count == 4

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.qstore.close()
        shutil.rmtree(TMP, ignore_errors=True)

    def setUp(self):
        self.mode.set("normal")
        self.delivery_db = TMP / f"deliveries-{self.id().split('.')[-1]}.db"
        self.dstore = DeliveryStore(self.delivery_db)
        self.worker = None

    def tearDown(self):
        if self.worker is not None:
            self.worker.shutdown()
            self.worker.join(timeout=5)
        self.dstore.close()

    # ------------------------------------------------------------------
    def _task(self, task_id, path):
        return FakeTaskStore({
            task_id: {
                "id": task_id,
                "line": "A线",
                "date": "2026-11-10",
                "status": "published",
                "published_file": path.name,
            }
        })

    def _start_worker(self, tasks):
        self.worker = DeliveryWorker(self.dstore, tasks)
        self.worker.start()

    def _wait_status(self, task_id, status, timeout=20):
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            row = self.dstore.get_for_task(task_id)
            last = row["status"] if row else None
            if row and row["status"] == status:
                return row
            time.sleep(0.1)
        raise AssertionError(f"等待 {status} 超时，当前 {last}")

    def _raw_post(self, task_id, payload, sha, count):
        return post_delivery(
            base_url=config.QUALITYDB_URL, task_id=task_id, line="A线",
            date="2026-11-10", payload=payload, file_sha256=sha, record_count=count,
        )

    # ------------------------------------------------------------------
    def test_01_full_delivery_persists_records_and_receipt(self):
        task_id = "task-full-0001"
        delivery, created = self.dstore.create_if_absent(
            task_id, self.good_sha, self.good_count
        )
        self.assertTrue(created)
        self._start_worker(self._task(task_id, self.good_path))
        self.worker.notify()

        row = self._wait_status(task_id, CONFIRMED)
        self.assertTrue(row["receipt_id"])
        # attempts 记录失败/重试次数：首投即成功时为 0。
        self.assertEqual(row["attempts"], 0)

        # 质量库侧：回执 + 全部明细，重复行分别保留，序号从 1 连续。
        d = self.qstore.find_delivery(task_id)
        self.assertIsNotNone(d)
        self.assertEqual(d["file_sha256"], self.good_sha)
        self.assertEqual(d["record_count"], 4)
        records = self.qstore.get_records(task_id)
        self.assertEqual(len(records), 4)  # 含一条完全重复行，不去重
        self.assertEqual([r["record_seq"] for r in records], [1, 2, 3, 4])
        self.assertEqual(records[1]["sample_no"], records[2]["sample_no"])
        self.assertTrue(all(r["task_id"] == task_id for r in records))
        self.assertTrue(all(r["receipt_id"] == d["receipt_id"] for r in records))

    def test_02_same_bytes_replay_receipt_no_duplicate_insert(self):
        task_id = "task-idem-0002"
        payload = self.good_path.read_bytes()
        first = self._raw_post(task_id, payload, self.good_sha, self.good_count)
        self.assertFalse(first["duplicate"])
        receipt_1 = first["receipt"]["receipt_id"]

        # 完全相同的任务号与字节再投：回放原回执，状态 200、duplicate=True
        second = self._raw_post(task_id, payload, self.good_sha, self.good_count)
        self.assertTrue(second["duplicate"])
        self.assertEqual(second["receipt"]["receipt_id"], receipt_1)

        # 明细仍是第一次的 4 条，没有重复入库
        self.assertEqual(len(self.qstore.get_records(task_id)), 4)
        self.assertEqual(len([d for d in self.qstore.list_deliveries()
                              if d["task_id"] == task_id]), 1)

    def test_03_different_bytes_same_task_rejected_409(self):
        task_id = "task-conflict-0003"
        self._raw_post(task_id, self.good_path.read_bytes(), self.good_sha, self.good_count)
        other = GOOD_CSV.replace("90.00", "91.11").encode("utf-8")
        import hashlib
        other_sha = hashlib.sha256(other).hexdigest()
        from app.delivery_client import DeliveryRejected
        with self.assertRaises(DeliveryRejected) as ctx:
            self._raw_post(task_id, other, other_sha, self.good_count)
        self.assertEqual(ctx.exception.status, 409)
        # 原数据保持不变
        d = self.qstore.find_delivery(task_id)
        self.assertEqual(d["file_sha256"], self.good_sha)
        self.assertEqual(len(self.qstore.get_records(task_id)), 4)

    def test_04_drop_after_persist_then_automatic_retry(self):
        task_id = "task-drop-0004"
        self.dstore.create_if_absent(task_id, self.good_sha, self.good_count)
        # 第一次入库后断开响应；重试时同字节回放原回执（断连只对新入库生效）。
        self.mode.set("drop")
        self._start_worker(self._task(task_id, self.good_path))
        self.worker.notify()

        row = self._wait_status(task_id, CONFIRMED, timeout=25)
        self.assertGreaterEqual(row["attempts"], 1, "响应丢失后应至少重试一次")
        self.assertTrue(row["receipt_id"])
        # 只入库一次：回执 1 条、明细 4 条
        self.assertEqual(len(self.qstore.get_records(task_id)), 4)
        self.assertEqual(
            len([d for d in self.qstore.list_deliveries() if d["task_id"] == task_id]), 1
        )

    def test_05_restart_resumes_pending_without_loss_or_duplicate(self):
        task_id = "task-restart-0005"
        self.dstore.create_if_absent(task_id, self.good_sha, self.good_count)
        self.mode.set("drop")
        self._start_worker(self._task(task_id, self.good_path))
        self.worker.notify()

        # 等到至少发生一次尝试并处于待确认（响应丢失），随后「重启」采集端。
        deadline = time.time() + 15
        while time.time() < deadline:
            row = self.dstore.get_for_task(task_id)
            if row["attempts"] >= 1 and row["status"] == PENDING:
                break
            time.sleep(0.1)
        else:
            self.fail("未观察到响应丢失后的 pending 状态")
        self.worker.shutdown()
        self.worker.join(timeout=5)
        self.worker = None
        self.dstore.close()

        # 模拟进程重启：用同一个 deliveries.db 新建 store 与 worker。
        self.mode.set("normal")
        new_store = DeliveryStore(self.delivery_db)
        self.addCleanup(new_store.close)
        new_worker = DeliveryWorker(new_store, self._task(task_id, self.good_path))
        new_worker.start()
        self.addCleanup(new_worker.shutdown)
        new_worker.notify()

        deadline = time.time() + 15
        while time.time() < deadline:
            row = new_store.get_for_task(task_id)
            if row["status"] == CONFIRMED:
                break
            time.sleep(0.1)
        else:
            self.fail("重启后 pending 投递未被接续确认")
        self.assertEqual(len(self.qstore.get_records(task_id)), 4)
        self.assertEqual(
            len([d for d in self.qstore.list_deliveries() if d["task_id"] == task_id]), 1
        )

    def test_06_bad_receipt_not_confirmed_then_succeeds(self):
        task_id = "task-badreceipt-0006"
        self.dstore.create_if_absent(task_id, self.good_sha, self.good_count)
        self.mode.set("bad_receipt")
        self._start_worker(self._task(task_id, self.good_path))
        self.worker.notify()

        # 回执记录数不符：必须保持待确认，不得确认成功。
        time.sleep(2)
        row = self.dstore.get_for_task(task_id)
        self.assertEqual(row["status"], PENDING)
        self.assertIn("回执", row["last_error"])
        self.assertGreaterEqual(row["attempts"], 1)

        # 接收端恢复正常后，重试拿到可信回执并确认。
        self.mode.set("normal")
        self._wait_status(task_id, CONFIRMED, timeout=15)
        self.assertEqual(len(self.qstore.get_records(task_id)), 4)

    def test_07_invalid_csv_is_rejected(self):
        task_id = "task-badcsv-0007"
        bad_sha = sha256_file(self.bad_path)
        bad_count = count_csv_records(self.bad_path)
        self.dstore.create_if_absent(task_id, bad_sha, bad_count)
        self._start_worker(self._task(task_id, self.bad_path))
        self.worker.notify()

        row = self._wait_status(task_id, REJECTED)
        self.assertIn("校验", row["last_error"])
        self.assertIsNone(self.qstore.find_delivery(task_id))  # 拒绝不入库

    def test_08_concurrent_create_reuses_single_delivery(self):
        task_id = "task-concurrent-0008"
        results = []

        def create():
            row, created = self.dstore.create_if_absent(
                task_id, self.good_sha, self.good_count
            )
            results.append(created)

        threads = [threading.Thread(target=create) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(sum(results), 1)  # 只有一个线程真正建行
        self.assertEqual(
            len([d for d in self.dstore.list_all() if d["task_id"] == task_id]), 1
        )

    def test_11_rejected_then_reopen_confirmed_reuses_row(self):
        # 先因坏文件被明确拒绝；重新发起（复用同一行）后投递成功。
        task_id = "task-reopen-0011"
        bad_sha = sha256_file(self.bad_path)
        bad_count = count_csv_records(self.bad_path)
        self.dstore.create_if_absent(task_id, bad_sha, bad_count)
        self._start_worker(self._task(task_id, self.bad_path))
        self.worker.notify()
        self._wait_status(task_id, REJECTED)
        rejected_row = self.dstore.get_for_task(task_id)
        self.worker.shutdown()
        self.worker.join(timeout=5)
        self.worker = None

        # 重新发起：同一投递行回到 pending（文件问题修复后用同一任务再投）。
        reopened = self.dstore.reopen(task_id)
        self.assertEqual(reopened["status"], PENDING)
        self.assertEqual(reopened["id"], rejected_row["id"])  # 复用同一行
        # 模拟文件已被替换为正确内容，更新其指纹并换用正确文件。
        with self.dstore.lock:
            self.dstore._conn.execute(
                "UPDATE deliveries SET file_sha256 = ?, record_count = ? WHERE id = ?",
                (self.good_sha, self.good_count, reopened["id"]),
            )
            self.dstore._conn.commit()
        self._start_worker(self._task(task_id, self.good_path))
        self.worker.notify()
        row = self._wait_status(task_id, CONFIRMED)
        self.assertTrue(row["receipt_id"])
        self.assertEqual(len(self.qstore.get_records(task_id)), 4)
        # 整个生命周期只有一条投递记录
        self.assertEqual(
            len([d for d in self.dstore.list_all() if d["task_id"] == task_id]), 1
        )

    def test_09_receipt_verification_rules(self):
        base = {"receipt_id": "r1", "task_id": "t", "file_sha256": "h",
                "record_count": 3}
        ok, _ = verify_receipt(dict(base), task_id="t", file_sha256="h", record_count=3)
        self.assertTrue(ok)
        for mutated in (
            dict(base, task_id="other"),
            dict(base, file_sha256="other"),
            dict(base, record_count=9),
            dict(base, receipt_id=""),
        ):
            ok, _ = verify_receipt(mutated, task_id="t", file_sha256="h", record_count=3)
            self.assertFalse(ok)

    def test_10_receiver_uses_same_validation_rules_as_collector(self):
        # 两端 validation.py 必须逐字节一致，规则同源。
        collector_rule = (ROOT / "collector" / "app" / "validation.py").read_bytes()
        receiver_rule = (ROOT / "qualitydb" / "validation.py").read_bytes()
        self.assertEqual(collector_rule, receiver_rule)
        self.assertEqual(q_validation.CSV_HEADER, ["产线", "日期", "样品号", "测量值"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
