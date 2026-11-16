# 实时流数据处理中心

面向实时数据处理团队的流式计算与管理中心。覆盖流数据源接入、流处理算子编排、窗口计算配置、状态后端管理、检查点恢复、反压监控、延迟追踪与流作业运维的完整实时计算链路。

这是一个前后端分离的管理平台：前端 React + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 React + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 流数据源接入 | `streamin` | 流数据源 | 源编号、源名称、消息队列 |
| 流处理算子 | `operator` | 算子定义 | 算子编号、算子名称、输入流 |
| 窗口计算配置 | `window` | 窗口定义 | 窗口编号、数据流名称、窗口类型 |
| 状态后端管理 | `state` | 后端配置 | 配置编号、后端类型、存储路径 |
| 检查点恢复 | `checkpoint` | 检查点记录 | 记录编号、流作业名、检查点ID |
| 反压监控面板 | `backpress` | 反压指标 | 指标编号、流作业名、反压级别 |
| 延迟追踪管理 | `latency` | 延迟记录 | 记录编号、流作业名、端到端延迟 |
| 流作业运维 | `jobops` | 作业实例 | 实例编号、作业名称、运行节点 |
| 流数据输出端 | `sink` | 输出配置 | 配置编号、输出类型、目标地址 |
| 流数据格式定义 | `schema` | 格式定义 | 定义编号、流名称、字段名称 |
| 水位线管理 | `watermark` | 水位线配置 | 配置编号、流名称、时间字段 |
| 并行度调优 | `parallel` | 调优记录 | 记录编号、流算子名、当前并行度 |
| 流作业指标采集 | `metric` | 指标采集器 | 采集器编号、作业名称、指标名称 |
| 流告警规则 | `alarm` | 告警规则 | 规则编号、监控指标、告警条件 |
| 自定义函数库 | `udf` | 函数定义 | 函数编号、函数名称、输入参数 |
| 流处理日志中心 | `log` | 日志记录 | 记录编号、流作业名、日志级别 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.tsx`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 过期快照批量清理与状态快照数据清单

状态后端管理（`state`）额外负责过期快照的批量清理。清理口径：

- 先读 STAT 状态快照与 CHEC 检查点引用，校验引用集合后再写清单、删目录。
- 被可恢复检查点（含仍停留在恢复待办里的已过期检查点）引用的存储路径整组保留，
  受保护大小计入状态快照数据清单，状态后端的状态大小不减。
- 同一路径由清理登记（`_state_cleanup`）保证并发清理只成功一次。
- 引用集合、删除结果、受保护大小与失败项在同一事务里写入状态快照数据清单；
  失败项保留原目录，`/retry` 只从未完成路径继续。
- 跨链路冲突（已登记删除的目录仍被恢复待办引用）只登记失败项并保留目录。
- 重复提交只接受一份记录，返回既有清单；引用集合已释放时允许重新清理。
- 消费侧通过 `/api/state/manifests/consumers` 查看、`/sync` 推进版本标识。

相关接口：

| 接口 | 说明 |
| --- | --- |
| `POST /api/state/manifests/cleanup` | 批量清理；提交 `state_ids`/`状态快照`、`检查点`、`提交编号`，`预演` 只计算不落库 |
| `GET /api/state/manifests` | 状态快照数据清单列表 |
| `GET /api/state/manifests/{id}` | 单份清单明细（删除结果、保护项、受保护大小、失败项、整组进度） |
| `POST /api/state/manifests/{id}/retry` | 失败续跑，只处理未完成路径 |
| `POST /api/state/manifests/{id}/sync` | 跨链路同步同一份清单并更新消费侧版本标识 |
| `GET /api/state/manifests/consumers` | 消费侧版本标识 |

验收样例：`STAT-0001`、`STAT-0002` 与 `CHEC-0003` 一起清理时，只删除无引用的
`orders-archive/ckpt-099` 目录，被 `CHEC-0001` 引用的 1.2GB 快照仍保留并计入状态大小，
清单受保护大小为 1.2GB。

测试：`cd backend && .venv/bin/python -m unittest discover -s tests -v`

领域：数据工程
子方向：数仓与数据湖
