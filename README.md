# 质检数据采集与质量库投递系统

质检数据工程师的每日检测明细采集工具：在网页上填写产线、日期后，后台通过
Playwright 实际操作旧仪器门户完成筛选与 CSV 导出，校验通过后发布；再通过
「质量库投递页」把已发布 CSV 投递到本地质量库接收服务，供报告系统读取，
支持状态查看、取消、失败重试、结果下载与投递追踪。

## 架构

```
┌──────────┐   HTTP    ┌──────────────┐   CDP    ┌──────────┐
│  portal  │ ◄──────── │  collector   │ ───────► │ browser  │
│ 模拟门户  │  筛选/导出 │ Flask+双worker│  9222    │ Chromium │
└──────────┘           └──────┬───────┘          └──────────┘
                              │ HTTP 投递（独立线程，不占任务库锁/门户会话）
                              ▼
                       ┌──────────────┐
                       │  qualitydb   │
                       │ 质量库接收服务 │
                       └──────────────┘
                       /data（collector：SQLite+CSV；qualitydb：独立 SQLite）
```

- **portal**：本地模拟的旧仪器门户。账号全局仅允许一个会话（新登录踢掉旧会话），
  支持按产线+日期筛选、导出 CSV，并可在「演示控制台」切换两种故障模式：
  延迟导出（约 8 秒）、返回错误产线文件。
- **collector**：采集服务。Flask 网页 + 两个相互独立的后台线程：
  - 采集 worker：任务排队（FIFO），同一时刻只有一个任务占用门户会话，
    筛选到下载全程不交叉；取消执行中任务先停浏览器、删除未发布文件，
    落盘后才到达的「迟到下载」一律不发布；
  - 投递 worker：把已发布 CSV 投递给质量库。**网络等待只发生在该线程，
    既不持有任务库锁也不占用门户会话**，原采集、查询、下载照常可用；
  - 发布前校验：CSV 表头必须是 `产线,日期,样品号,测量值`，且**每一条**记录的
    产线、日期都与请求吻合，否则整单拒绝发布。
- **qualitydb**：本地质量库接收服务（独立 SQLite）。
  - 按与采集端**同一份**校验规则（`validation.py` 构建时复制）校验 CSV；
  - 通过后在一个事务内写入回执（`deliveries`）与全部明细（`records`，
    含任务号与原记录序号）；明细不去重、不建唯一约束，**重复行分别保留**；
  - 任务号幂等：同任务同字节（SHA-256 相同）回放**原回执**、不重复入库；
    同任务异字节返回 **409 拒绝**、不写入；
  - `/admin` 可切换「入库后断开响应」，用于演示响应丢失后的自动接续。
- **browser**：独立 Chromium 容器，通过 CDP（9222）供采集服务连接。

### 投递的可靠性语义（至少一次 + 接收端幂等）

- 采集端投递记录落在**独立的 `deliveries.db`**，任务号 UNIQUE；重复或并发
  点击复用同一条投递，不会重复建行。
- 只有收到 **任务号、文件 SHA-256、记录数三者全吻合**（且带 receipt_id）的
  回执才置「投递成功」；任一不符都保持「待确认」继续重试。
- 连接拒绝、超时、对端入库后断连等「拿不到可确认响应」的情况，一律保持
  待确认，用**原任务号、原文件字节**按指数退避自动重试。
- 采集端或接收端重启后，待确认投递到期自动接续；接收端的任务号唯一约束
  保证重试只回放原回执，**不漏投、不重复入库**。

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

| 入口 | 地址 | 说明 |
| --- | --- | --- |
| 采集服务 | http://localhost:8080 | 创建任务、查看状态、取消/重试/下载 |
| 质量库投递页 | http://localhost:8080/deliveries | 投递已发布任务、查看投递状态/错误/质量库明细 |
| 模拟门户 | http://localhost:8000 | 账号 `admin` / `instrument123` |
| 门户演示控制台 | http://localhost:8000/admin | 切换延迟导出、错误产线文件 |
| 质量库接收服务 | http://localhost:8100 | 回执列表与明细查询 |
| 质量库演示控制台 | http://localhost:8100/admin | 切换「入库后断开响应」 |

### 演示流程

1. **正常采集**：采集服务首页选产线、日期 → 提交采集 → 状态从
   `排队中 → 执行中 → 已发布`，点击「下载 CSV」取回质量库文件。
2. **取消执行中任务**：门户控制台开启「延迟导出」→ 提交任务 → 执行中点「取消」
   → 任务变为「已取消」，未发布文件被删除，下载接口返回 409。
3. **错误数据拦截**：门户控制台开启「返回错误产线文件」→ 提交任务 →
   任务「失败」，消息提示产线不符、拒绝发布 → 关闭开关后点「重试」→ 发布成功。
4. **单会话排队**：连续提交多个任务，观察它们依次执行、互不交叉。
5. **投递质量库**：打开「质量库投递页」→ 对已发布任务点「投递到质量库」→
   状态从「待确认」变「投递成功」，页面展示回执与质量库中的全部明细
   （含原记录序号，重复行分别保留）。重复或连点按钮只复用同一条投递。
6. **响应丢失自动接续（重点演示）**：在质量库控制台开启「入库后断开响应」
   → 投递一个新任务 → 采集端首次请求在对端入库后连接被断开，状态保持
   「待确认」并自动用原文件重试 → 接收端按任务号识别为同字节，回放原回执
   → 采集端确认成功；质量库中该任务始终只有 1 条回执、明细不重复。
7. **异字节拒绝**：同一任务号若文件字节变化（SHA-256 不同），接收端返回
   409 拒绝，采集端标记「已拒绝」并展示原因，不会覆盖原入库数据。

## API

采集服务（8080）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/tasks` | 创建任务，表单字段 `line`、`date`（YYYY-MM-DD） |
| GET | `/api/tasks` | 任务列表（JSON） |
| GET | `/api/tasks/<id>` | 单个任务状态 |
| POST | `/api/tasks/<id>/cancel` | 取消（排队中立即取消；执行中先停浏览器再释放会话） |
| POST | `/api/tasks/<id>/retry` | 重试失败/已取消任务，返回新任务 |
| GET | `/tasks/<id>/download` | 下载已发布 CSV（未发布返回 409） |
| POST | `/api/tasks/<id>/deliver` | 对已发布任务发起/复用质量库投递（幂等，并发点击复用同一条） |
| GET | `/api/deliveries` | 全部投递记录（含状态、尝试次数、错误、回执） |
| GET | `/api/deliveries/<id>` | 单条投递记录 |
| GET | `/api/deliveries/<id>/quality-records` | 代理查询质量库中该任务的明细 |
| GET | `/deliveries` | 投递管理网页 |
| GET | `/healthz` | 健康检查 |

任务状态机：`queued → running → published / failed`，
`queued/running → cancelling → cancelled`；`failed`、`cancelled` 可重试。

投递状态机：`pending（待确认，含退避重试）→ confirmed（回执三要素吻合）`；
接收端明确拒绝时进入 `rejected`，可在页面「重新发起」（仍复用同一行、同一文件）。

质量库接收服务（8100）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/deliveries` | 投递 CSV：字节为 body，任务号/产线/日期/SHA-256/记录数走请求头 |
| GET | `/api/deliveries` | 回执列表 |
| GET | `/api/deliveries/<task_id>` | 该任务回执 + 全部明细（含原记录序号） |
| GET/POST | `/admin` | 演示控制台：切换「入库后断开响应」 |
| GET | `/healthz` | 健康检查 |

## 本地开发（不用 Docker）

```bash
pip install -r portal/requirements.txt -r collector/requirements.txt \
            -r qualitydb/requirements.txt
python -m playwright install chromium

# 终端 1：门户
python portal/app.py                      # http://127.0.0.1:8000

# 终端 2：质量库接收服务
QUALITYDB_DB=./data/quality.db python qualitydb/app.py   # http://127.0.0.1:8100

# 终端 3：采集服务（BROWSER_CDP_URL 留空则本地启动浏览器）
PORTAL_URL=http://127.0.0.1:8000 QUALITYDB_URL=http://127.0.0.1:8100 \
  BROWSER_CDP_URL= DATA_DIR=./data \
  python -m app                           # 在 collector/ 目录下，http://127.0.0.1:8080
```

## 测试

```bash
python -m unittest discover -s tests -v
```

- `tests/test_validation.py`：发布校验（表头/产线/日期/空文件等 7 例）
- `tests/test_portal.py`：门户单会话踢出、导出、故障开关（7 例，需 Flask）
- `tests/test_integration.py`：真实浏览器端到端（5 例，需 Flask + Playwright）
- `tests/test_delivery.py`：投递链路端到端（10 例，**纯标准库即可运行**）——
  正常入库与重复行保留、同字节回放原回执、异字节 409、入库后断连自动重试、
  重启接续不漏不重、回执三要素校验、坏 CSV 拒绝、并发点击复用单条投递、
  两端校验规则同源。

## 配置项（环境变量）

采集服务：

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `PORTAL_URL` | `http://portal:8000` | 门户地址 |
| `PORTAL_USERNAME` / `PORTAL_PASSWORD` | `admin` / `instrument123` | 门户账号 |
| `BROWSER_CDP_URL` | `http://browser:9222` | 浏览器 CDP 端点，留空则本地启动 |
| `QUALITYDB_URL` | `http://qualitydb:8100` | 质量库接收服务地址 |
| `DATA_DIR` | `/data` | SQLite 与 CSV 存储目录（任务库与投递库均在此） |
| `DELIVERY_TIMEOUT_SECONDS` | `10` | 单次投递 HTTP 超时（仅作用于投递线程） |
| `DELIVERY_BACKOFF_MIN` / `DELIVERY_BACKOFF_MAX` | `2` / `30` | 投递失败指数退避上下限（秒） |
| `OPERATION_TIMEOUT_MS` | `30000` | 浏览器单步/下载等待超时 |
| `HEADLESS` | `1` | 本地启动浏览器时是否无头 |

质量库接收服务：

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `QUALITYDB_DB` | `/data/quality.db` | 独立 SQLite 路径（回执 + 明细） |
| `QUALITYDB_DROP_AFTER_PERSIST` | `0` | 启动即开启「入库后断开响应」演示模式 |

门户：

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `PORTAL_DELAY_EXPORT` | `0` | 启动时开启延迟导出 |
| `PORTAL_BAD_FILE` | `0` | 启动时开启错误产线文件 |
| `PORTAL_EXPORT_DELAY` | `8` | 延迟导出秒数 |

## 数据存储

- 采集端：`collector.db`（任务）、`deliveries.db`（投递，任务号 UNIQUE）、
  `published/`（已发布 CSV），均在 `DATA_DIR` 下；两个库分离，投递网络等待
  不阻塞任务库。
- 质量库：`quality.db` 中 `deliveries`（回执，任务号 UNIQUE）与 `records`
  （全部明细，含任务号、回执号、原记录序号，不去重）。
