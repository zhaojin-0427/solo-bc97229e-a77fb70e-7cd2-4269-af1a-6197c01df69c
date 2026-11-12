"""投递 HTTP 客户端：仅用标准库 urllib，不引入新依赖。

区分两类失败：
- DeliveryTransportError：连接/超时/响应读取失败（含接收端入库后断连），
  此时接收端可能已入库，采集端必须保持「待确认」并用原任务、原文件重试；
- DeliveryRejected：接收端给出明确的非 2xx 响应（校验失败、异字节冲突等），
  属于永久性拒绝，投递置为 rejected 等待人工处理。
"""
from __future__ import annotations

import hashlib
import http.client
import json
import socket
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import quote

from . import config

# 对端在写响应前断开（含「入库后断连」演示）会表现为 BadStatusLine /
# RemoteDisconnected，它不是 HTTP 错误响应，必须按响应丢失可重试处理。
_TRANSPORT_ERRORS = (
    urllib.error.URLError,
    http.client.HTTPException,
    socket.timeout,
    TimeoutError,
    OSError,
)


class DeliveryTransportError(Exception):
    """没有拿到可确认的 HTTP 响应（断连、超时、连接拒绝等）。"""


class DeliveryRejected(Exception):
    """接收端明确拒绝（非 2xx），携带状态码与消息。"""

    def __init__(self, status: int, message: str):
        super().__init__(f"HTTP {status}: {message}")
        self.status = status
        self.message = message


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def post_delivery(*, base_url: str, task_id: str, line: str, date: str,
                  payload: bytes, file_sha256: str, record_count: int,
                  timeout: float | None = None) -> dict:
    """向质量库投递一次，返回接收端响应信封（dict）。"""
    url = base_url.rstrip("/") + "/api/deliveries"
    headers = {
        "Content-Type": "text/csv; charset=utf-8",
        "X-Task-Id": task_id,
        # 中文产线经百分号编码放进 ASCII 头，接收端 unquote 还原。
        "X-Task-Line": quote(line),
        "X-Task-Date": quote(date),
        "X-File-Sha256": file_sha256,
        "X-Record-Count": str(record_count),
    }
    req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(
            req, timeout=timeout or config.DELIVERY_TIMEOUT_SECONDS
        ) as resp:
            body = resp.read()
            status = resp.status
    except urllib.error.HTTPError as exc:
        # 接收端有明确响应：解析错误信封，归为永久性拒绝。
        try:
            envelope = json.loads(exc.read().decode("utf-8"))
            message = envelope.get("message") or envelope.get("error") or str(exc)
        except Exception:
            message = str(exc)
        raise DeliveryRejected(exc.code, message) from exc
    except _TRANSPORT_ERRORS as exc:
        # 拿不到响应：对端可能已入库，必须可重试，绝不能当失败终结。
        raise DeliveryTransportError(f"网络/响应异常：{exc.__class__.__name__}: {exc}") from exc

    try:
        envelope = json.loads(body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise DeliveryTransportError(f"响应不是合法 JSON：{exc}") from exc
    if status not in (200, 201) or not envelope.get("ok"):
        raise DeliveryRejected(status, str(envelope))
    return envelope


def verify_receipt(receipt: dict, *, task_id: str, file_sha256: str,
                   record_count: int) -> tuple[bool, str]:
    """只有任务号、文件 SHA-256、记录数三者全吻合才算投递成功。"""
    if not isinstance(receipt, dict):
        return False, "回执结构无效"
    if receipt.get("task_id") != task_id:
        return False, "回执任务号与投递任务不符"
    if receipt.get("file_sha256") != file_sha256:
        return False, "回执 SHA-256 与投递文件不符"
    if receipt.get("record_count") != record_count:
        return False, f"回执记录数 {receipt.get('record_count')} 与本地 {record_count} 不符"
    if not receipt.get("receipt_id"):
        return False, "回执缺少 receipt_id"
    return True, "回执三要素（任务号/SHA-256/记录数）全部吻合"
