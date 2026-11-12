"""本地质量库接收服务（Flask 壳）。

核心逻辑全部在 receiver.py（框架无关）；本文件只负责 HTTP 适配、
演示开关页面与质量库明细查询。

入口/接口：
- POST /api/deliveries          投递 CSV（字节为 body，任务元数据走请求头）
- GET  /api/deliveries          回执列表
- GET  /api/deliveries/<task>   该任务在质量库中的全部明细
- GET  /admin                   演示控制台：切换「入库后断开响应」
- GET  /healthz                 健康检查
"""
from __future__ import annotations

import os

from flask import Flask, jsonify, render_template, request
from werkzeug.serving import WSGIRequestHandler

from receiver import DeliveryMode, QualityStore, process_delivery

app = Flask(__name__)
app.secret_key = os.environ.get("QUALITYDB_SECRET_KEY", "qualitydb-demo-secret")

DB_PATH = os.environ.get("QUALITYDB_DB", "/data/quality.db")
store = QualityStore(DB_PATH)
mode = DeliveryMode(os.environ.get("QUALITYDB_DROP_AFTER_PERSIST", "0") == "1")


class _ConnectionDropped(Exception):
    """已在底层断开连接，无需再写 HTTP 响应。"""


def _drop_environ_socket() -> None:
    """断开当前请求的底层 TCP 连接（仅开发服务器环境可用）。"""
    socket = request.environ.get("werkzeug.socket") or request.environ.get("gunicorn.socket")
    if socket is None:
        raise OSError("当前运行环境拿不到底层 socket，无法模拟断连")
    socket.shutdown(2)  # SHUT_RDWR
    socket.close()


@app.errorhandler(_ConnectionDropped)
def _on_dropped(_exc):
    # 连接已断开，返回的响应写不出去；吞掉即可，避免无意义的错误日志。
    return ("", 204)


@app.post("/api/deliveries")
def deliver():
    payload = request.get_data(cache=False)
    result = process_delivery(
        store, mode, dict(request.headers), payload,
        drop_socket=_drop_environ_socket,
    )
    if result.drop_connection:
        # 底层连接已关，直接抛异常走 errorhandler，不再尝试写响应。
        raise _ConnectionDropped()
    return jsonify(result.body), result.status


@app.get("/api/deliveries")
def list_deliveries():
    return jsonify(store.list_deliveries())


@app.get("/api/deliveries/<task_id>")
def get_delivery(task_id: str):
    delivery = store.find_delivery(task_id)
    if delivery is None:
        return jsonify({"ok": False, "error": "not_found",
                        "message": f"质量库中没有任务 {task_id} 的投递"}), 404
    return jsonify({"ok": True, "delivery": delivery,
                    "records": store.get_records(task_id)})


@app.route("/admin", methods=["GET", "POST"])
def admin():
    if request.method == "POST":
        mode.set(request.form.get("drop_after_persist") == "on")
    return render_template(
        "admin.html",
        drop=mode.drop_after_persist,
        deliveries=store.list_deliveries(),
    )


@app.get("/healthz")
def healthz():
    return {"status": "ok"}, 200


# 断连演示时 Werkzeug 会记录 BrokenPipe，把噪音压下去。
class _QuietHandler(WSGIRequestHandler):
    def log_exception(self, exc_info):  # pragma: no cover - 仅日志行为
        if issubclass(exc_info[0], (BrokenPipeError, ConnectionResetError, _ConnectionDropped)):
            return
        super().log_exception(exc_info)


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8100")),
        request_handler=_QuietHandler,
    )
