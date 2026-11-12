"""采集服务 Web 层：任务创建、状态查看、取消、重试、下载，以及质量库投递。"""
from __future__ import annotations

import json
import logging
import re
import urllib.error
import urllib.request
from datetime import date

from flask import (
    Flask,
    abort,
    jsonify,
    redirect,
    render_template,
    request,
    send_from_directory,
    url_for,
)

from . import config
from .delivery_store import CONFIRMED, PENDING, REJECTED, DeliveryStore
from .delivery_worker import DeliveryWorker, count_csv_records
from .delivery_client import sha256_file
from .models import FINAL_STATES, TaskStore
from .worker import Worker

log = logging.getLogger("collector.web")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
LINE_RE = re.compile(r"^[一-龥A-Za-z0-9_-]{1,20}$")

# 投递状态中文展示
DELIVERY_LABELS = {
    PENDING: "待确认",
    CONFIRMED: "投递成功",
    REJECTED: "已拒绝",
}


def create_app() -> Flask:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    app = Flask(__name__)
    store = TaskStore()
    worker = Worker(store)
    delivery_store = DeliveryStore()
    delivery_worker = DeliveryWorker(delivery_store, store)

    # 上次进程遗留的未完成采集任务统一转为可重试失败态；
    # pending 投递无需改动——重启后由投递 worker 自动接续（不漏投）。
    recovered = store.recover_stale()
    if recovered:
        log.warning("启动恢复：%s 个中断采集任务标记为 failed 可重试", recovered)
    worker.start()
    delivery_worker.start()
    app.extensions["store"] = store
    app.extensions["worker"] = worker
    app.extensions["delivery_store"] = delivery_store
    app.extensions["delivery_worker"] = delivery_worker

    # ---- 页面 -----------------------------------------------------------
    @app.get("/")
    def index_page():
        tasks = store.list_tasks()
        return render_template(
            "index.html",
            tasks=tasks,
            portal_url=config.PORTAL_URL,
            final_states=FINAL_STATES,
            delivery_count=len(delivery_store.list_all()),
        )

    @app.get("/deliveries")
    def deliveries_page():
        tasks = store.list_tasks()
        deliveries = {d["task_id"]: d for d in delivery_store.list_all()}
        # 质量库侧明细（代理查询，失败不阻塞页面渲染）。
        quality: dict[str, dict] = {}
        errors: dict[str, str] = {}
        for task_id in deliveries:
            data, err = _fetch_quality(task_id)
            if err:
                errors[task_id] = err
            elif data:
                quality[task_id] = data
        return render_template(
            "deliveries.html",
            tasks=tasks,
            deliveries=deliveries,
            quality=quality,
            quality_errors=errors,
            labels=DELIVERY_LABELS,
            qualitydb_url=config.QUALITYDB_URL,
        )

    # ---- 采集任务 API ---------------------------------------------------
    @app.post("/api/tasks")
    def create_task():
        line = (request.form.get("line") or "").strip()
        day = (request.form.get("date") or "").strip()
        if not LINE_RE.match(line):
            return _bad_request("产线格式无效（1-20 位中文/字母/数字/横线/下划线）")
        if not DATE_RE.match(day):
            return _bad_request("日期格式无效，应为 YYYY-MM-DD")
        try:
            date.fromisoformat(day)
        except ValueError:
            return _bad_request("日期不是有效日历日期")
        task = store.create_task(line, day)
        worker.notify()
        if request.accept_mimetypes.accept_json and not request.form.get("html"):
            return jsonify(task), 201
        return redirect(url_for("index_page"))

    @app.get("/api/tasks")
    def list_tasks():
        return jsonify(store.list_tasks())

    @app.get("/api/tasks/<task_id>")
    def get_task(task_id: str):
        task = store.get_task(task_id)
        if task is None:
            abort(404)
        return jsonify(task)

    @app.post("/api/tasks/<task_id>/cancel")
    def cancel_task(task_id: str):
        ok, message = store.request_cancel(task_id)
        if not ok:
            return _bad_request(message)
        worker.notify()
        if request.form.get("html"):
            return redirect(url_for("index_page"))
        return jsonify({"ok": True, "message": message})

    @app.post("/api/tasks/<task_id>/retry")
    def retry_task(task_id: str):
        new_task = store.retry(task_id)
        if new_task is None:
            return _bad_request("仅失败或已取消的任务可以重试")
        worker.notify()
        if request.form.get("html"):
            return redirect(url_for("index_page"))
        return jsonify(new_task), 201

    @app.get("/tasks/<task_id>/download")
    def download_task(task_id: str):
        task = store.get_task(task_id)
        if task is None:
            abort(404)
        if task["status"] != "published" or not task["published_file"]:
            abort(409, description="任务尚未发布，没有可下载文件")
        # send_from_directory 会拦截路径穿越，文件名来自服务端生成也更安全。
        return send_from_directory(
            config.PUBLISHED_DIR, task["published_file"], as_attachment=True
        )

    # ---- 质量库投递 API -------------------------------------------------
    @app.post("/api/tasks/<task_id>/deliver")
    def create_delivery(task_id: str):
        task = store.get_task(task_id)
        if task is None:
            return _bad_request("任务不存在", 404)
        if task["status"] != "published" or not task["published_file"]:
            return _bad_request("仅已发布的任务可以投递质量库")
        csv_path = config.PUBLISHED_DIR / task["published_file"]
        if not csv_path.exists():
            return _bad_request("已发布 CSV 文件缺失，无法投递")

        existing = delivery_store.get_for_task(task_id)
        if existing is not None:
            # 重复/并发点击：复用同一条投递，不重复建行。
            if existing["status"] == REJECTED:
                # 被拒绝过的允许人工重新发起（仍复用同一行、同一文件）。
                existing = delivery_store.reopen(task_id)
                delivery_worker.notify()
            result = {"ok": True, "reused": True, "delivery": existing}
        else:
            file_sha = sha256_file(csv_path)
            record_count = count_csv_records(csv_path)
            delivery, created = delivery_store.create_if_absent(
                task_id, file_sha, record_count
            )
            delivery_worker.notify()
            result = {"ok": True, "reused": not created, "delivery": delivery}

        if request.form.get("html"):
            return redirect(url_for("deliveries_page"))
        return jsonify(result), 200 if result["reused"] else 201

    @app.get("/api/deliveries")
    def list_deliveries():
        return jsonify(delivery_store.list_all())

    @app.get("/api/deliveries/<task_id>")
    def get_delivery(task_id: str):
        delivery = delivery_store.get_for_task(task_id)
        if delivery is None:
            abort(404)
        return jsonify(delivery)

    @app.get("/api/deliveries/<task_id>/quality-records")
    def quality_records(task_id: str):
        """代理读取质量库中该任务的明细，前端不直连质量库。"""
        data, err = _fetch_quality(task_id)
        if err:
            return jsonify({"ok": False, "error": err}), 502
        if data is None:
            return jsonify({"ok": False, "error": "质量库中暂无该任务的投递"}), 404
        return jsonify({"ok": True, **data})

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}, 200

    return app


def _fetch_quality(task_id: str) -> tuple[dict | None, str | None]:
    """短超时代理 GET 质量库明细；网络等待不持任何任务库锁。"""
    url = config.QUALITYDB_URL.rstrip("/") + f"/api/deliveries/{task_id}"
    try:
        with urllib.request.urlopen(url, timeout=3) as resp:
            payload = resp.read()
        return json.loads(payload.decode("utf-8")), None
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None, None
        return None, f"质量库返回 HTTP {exc.code}"
    except (urllib.error.URLError, OSError, ValueError) as exc:
        return None, f"质量库暂不可达：{exc.__class__.__name__}"


def _bad_request(message: str, status: int = 400):
    return (jsonify({"ok": False, "error": message}), status)
