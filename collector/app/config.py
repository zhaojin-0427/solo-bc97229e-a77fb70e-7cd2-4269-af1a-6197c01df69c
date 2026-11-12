"""采集服务配置（全部可通过环境变量覆盖）。"""
from __future__ import annotations

import os
from pathlib import Path


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


# 门户与浏览器
PORTAL_URL = _env("PORTAL_URL", "http://portal:8000")
PORTAL_USERNAME = _env("PORTAL_USERNAME", "admin")
PORTAL_PASSWORD = _env("PORTAL_PASSWORD", "instrument123")
# 留空则本地启动浏览器；容器内指向独立 Chromium 的 CDP 端点。
BROWSER_CDP_URL = _env("BROWSER_CDP_URL", "http://browser:9222")
HEADLESS = _env("HEADLESS", "1") == "1"

# 存储
DATA_DIR = Path(_env("DATA_DIR", "/data"))
DB_PATH = DATA_DIR / "collector.db"
PUBLISHED_DIR = DATA_DIR / "published"
WORK_DIR = DATA_DIR / "work"

# 调度
POLL_INTERVAL = float(_env("POLL_INTERVAL", "1.0"))
OPERATION_TIMEOUT_MS = int(_env("OPERATION_TIMEOUT_MS", "30000"))
# 导出轮询间隔，取消信号按此粒度生效。
DOWNLOAD_TICK_SECONDS = float(_env("DOWNLOAD_TICK_SECONDS", "0.25"))
NAV_TIMEOUT_MS = int(_env("NAV_TIMEOUT_MS", "15000"))

# 质量库 CSV 表头（单一事实来源在 validation.CSV_HEADER，这里再导出保持兼容）
from .validation import CSV_HEADER  # noqa: E402

# 质量库接收服务（投递目标）
QUALITYDB_URL = _env("QUALITYDB_URL", "http://qualitydb:8100")
# 投递 HTTP 超时：网络等待发生在独立投递线程，绝不占任务库锁/门户会话
DELIVERY_TIMEOUT_SECONDS = float(_env("DELIVERY_TIMEOUT_SECONDS", "10"))
# 投递重试的初始与最大退避（秒），失败后指数退避
DELIVERY_BACKOFF_MIN = float(_env("DELIVERY_BACKOFF_MIN", "2"))
DELIVERY_BACKOFF_MAX = float(_env("DELIVERY_BACKOFF_MAX", "30"))
DELIVERY_DB_PATH = DATA_DIR / "deliveries.db"
