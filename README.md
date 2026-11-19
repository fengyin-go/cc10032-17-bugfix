# 实时计算服务中心

面向实时数据分析场景，提供流式计算定义、窗口聚合、策略告警和状态管理的实时数据平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
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
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 流式源端 | `stream_source` | 数据源配置 | 源端名称、消息队列、Topic名称 |
| 窗口配置 | `window_config` | 窗口策略 | 策略名称、窗口类型、窗口大小 |
| 计算算子 | `calc_operator` | 算子定义 | 算子名称、算子类型、输入字段 |
| 状态存储 | `state_store` | 状态配置 | 配置名称、后端类型、状态键模式 |
| 水位追踪 | `watermark_track` | 水位记录 | 水位名称、数据源、当前水位 |
| 告警策略 | `alert_policy` | 告警规则 | 规则名称、监控指标、触发条件 |
| 滑动指标 | `sliding_index` | 指标计算 | 指标名称、计算窗口、聚合函数 |
| 流关联 | `join_stream` | 关联配置 | 配置名称、左流源、右流源 |
| 结果落地 | `sink_output` | 输出配置 | 配置名称、输出类型、目标地址 |
| 补数任务 | `backfill_job` | 补数作业 | 作业名称、目标指标、补数范围 |
| CEP规则 | `cep_rule` | 事件序列 | 序列名称、事件类型、匹配顺序 |
| 吞吐管理 | `throughput_mgr` | 吞吐配置 | 配置名称、数据源、最大QPS |
| 检查点 | `checkpoint_mgr` | 检查点配置 | 配置名称、间隔时间、超时时长 |
| 故障恢复 | `restore_plan` | 恢复方案 | 方案名称、任务范围、恢复模式 |
| 指标血缘 | `metric_lineage` | 血缘记录 | 记录ID、指标名称、上游源端 |
| 迟到处理 | `late_handler` | 处理策略 | 策略名称、允许延迟、迟到动作 |
| 任务拓扑 | `task_topology` | 拓扑配置 | 拓扑名称、算子链、并行度 |
| 数据采样 | `data_sampler` | 采样策略 | 策略名称、采样方式、采样比例 |
| 调试追踪 | `debug_trace` | 调试会话 | 会话名称、目标算子、采样数据 |
| 聚合落库 | `aggregate_sink` | 聚合任务 | 任务名称、源指标集、聚合粒度 |

## 序号17：分层采样与调试复核

数据采样模块的 `apply_sampling` 是分层采样的唯一入口，调试追踪模块消费同一条
采样批次，两条链路共用一份分层样本数据和样本报告。

- **每层至少一份**：先给每个非空分层保底 1 条，再按比例分配剩余配额，尾部分层
  不会再被整体截断清空。
- **上限不足时两种取舍**：`cap_policy=expand`（默认）把有效上限扩到分层数；
  `cap_policy=compress` 保持上限不变、允许总量超出上限（`limit_overrun`）并记录。
  两种策略产出不同结果版本（`issue17.expand.v1` / `issue17.compress.v1`）。
- **截断台账**：`_sample_report` 记录每层可用/入选/损失、`truncated_strata`、
  `truncated_total`、`cap_expanded`、`duplicates_dropped`，写入批次时一次性落库。
- **不重复累计**：事件按 `event_id`（缺失时内容指纹）全局去重；同一策略、同一数据、
  同一参数生成同一幂等键，重复/并发调用只生效一次；写库失败回滚到采样前版本。
- **读取源事件**：`read_events_from_source` 按源端名读取事件；源端异常断连/已下线
  时拒绝读取，避免用不完整数据产出样本报告。
- **修复台账**：每次批次生效或幂等命中都会记一条台账（`/api/sampling/ledger`），
  其他模块据此同步更新；策略列表不再混入运行期采样批次（修复详情页显示错位）。
- **调试复核**：调试会话通过 `batch_id` 绑定批次，`check_sample_coverage` 同时核对
  样本报告与 `_sampled_data`。任一层缺失、或报告与样本数量对不上，`complete` /
  `analyze` 返回 409 并阻止生成异常结论，不会把未抽样层当作正常。

接口（固定前缀，注册在 `/api/{module}` 之前）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/sampling/sample` | 执行采样，可带 `method/group_key/ratio/max_samples/cap_policy/idempotency_key` |
| GET | `/api/sampling/report` | 样本报告（可带 `batch_id`） |
| GET | `/api/sampling/strata` | 每层入选数量、截断损失、是否扩容 |
| GET | `/api/sampling/batches` | 最近采样批次 |
| POST | `/api/sampling/source-events` | 从流式源端读取采样事件（断连/下线时 409） |
| GET | `/api/sampling/ledger` | 序号17 修复台账（生效版本、幂等命中、回滚基线） |
| POST | `/api/tracing/attach` | 会话绑定采样批次 |
| GET | `/api/tracing/coverage` | 分层覆盖复核（`session=`） |
| POST | `/api/tracing/complete` | 生成异常结论，覆盖缺失时 409 |
| POST | `/api/tracing/analyze` | 异常模式分析，覆盖缺失时 409 |

前端：数据采样页内嵌「分层样本复核」面板（每层入选数量、截断损失、扩容/压缩提示），
调试追踪页内嵌「调试异常结论复核」面板（缺失分层高亮、阻止提示、结论明细）。
采样页面板还可直接生成演示事件并执行分层采样，用来复现“上限不足”的两种取舍。

回归测试：`backend/tests/test_issue17_sampling.py`。

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数据分析与数据应用
