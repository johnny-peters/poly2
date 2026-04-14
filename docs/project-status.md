# poly2 项目现状梳理

更新时间：2026-04-14

本文基于当前仓库代码（`src`、`config`、`README`、`docs`）整理，目标是明确项目当前可用能力、正在推进方向和后续计划。

## 一、已实现的功能

### 1) 核心交易引擎骨架

- 已具备完整主流程：`行情快照 -> 策略信号 -> 风控校验 -> 执行下单 -> 持仓更新`。
- 已实现 `TradingEngine` + `EngineRunner` 分层，支持批量快照处理和执行汇总输出。
- 支持循环运行模式，主程序可按 `FETCH_INTERVAL` 周期性扫描并执行。

### 2) 策略框架与已落地策略

- 已实现策略抽象（`Strategy` trait）和策略注册中心（`StrategyRegistry`）。
- 已落地 `ArbitrageStrategy`（套利策略）：
  - 触发逻辑基于 `YES_ask + NO_ask + cost_buffer < 1 - min_profit_threshold`。
  - 可配置最小利润阈值、滑点缓冲、最小流动性、下单规模。
- 已提供 TODO 占位策略机制，未实现策略不会进入调度执行。

### 3) 风控能力

- 已实现风控引擎 `RiskEngine`，覆盖三类硬约束：
  - 单市场最大仓位限制（`max_position_pct`）
  - 单笔最大交易限制（`max_single_trade_pct`）
  - 日内亏损限制（`daily_loss_limit_pct`）
- 风控在策略信号进入执行前生效，未通过即拦截。

### 4) 执行与回报

- 已实现多种执行客户端：
  - `MockExecutionClient`（本地模拟）
  - `RetryingExecutionClient`（统一重试包装）
  - `PolymarketHttpExecutionClient`（HTTP 下单、状态轮询、部分成交汇总）
  - `EventingExecutionClient`（广播执行事件）
- 已支持：
  - 多 action 并发提交
  - 订单状态归一（Pending/Partial/Filled/Rejected）
  - 成交增量与汇总事件输出

### 5) 持仓与持久化

- 已实现 `PositionManager`：
  - 持仓变更
  - 已实现/未实现 PnL 计算
  - 费用累计
- 已实现持久化：
  - 订单 JSONL 追加日志（`append_order_record`）
  - 持仓快照读写（`load_positions` / `save_positions`）

### 6) 运维与工具能力

- 已实现 `healthcheck` 命令，覆盖：
  - CLOB HTTP 连通
  - WS TCP 探测
  - Polygon RPC/区块/USDC 合约代码检查
- 已实现 `scan-arb` 命令：
  - 拉取市场并筛选活跃标的
  - 计算套利边际并输出候选 Top N
- 已提供 Ubuntu 部署文档和 systemd 常驻运行说明。

## 二、正在实现中的功能（已具备基础，仍在完善）

### 1) 实盘执行链路完善

- HTTP 实盘执行已接通，但仍偏“工程可跑”状态，离稳定生产化还有完善空间：
  - API 签名流程目前以可注入 provider / 环境变量方式实现，需进一步标准化。
  - 对不同交易所返回结构与异常场景的兼容仍在持续打磨。

### 2) 风险后处理与退出机制

- `EngineRunner` 已实现止盈/止损触发平仓逻辑（基于持仓与最新 bid 计算）。
- 当前退出逻辑在 runner 层实现，后续可继续演进为更独立的“退出策略模块”。

### 3) 可观测性雏形到体系化

- 当前已有基础日志输出、执行事件与统计汇总。
- 监控指标、告警链路、统一仪表盘仍处在从“有信息”到“可运营”的建设阶段。

## 三、未来计划实现的功能

### 1) 其他策略落地

当前已预留并注册为 TODO 的策略方向：

- `ProbabilityTrading`：外部概率（如 AI/模型）与市场价格偏差交易。
- `MarketMaking`：双边挂单与库存管理。
- `LatencyArbitrage`：事件驱动的低延迟套利。

### 2) 更完整的数据层

- 按架构文档目标，后续应补齐更实时的数据采集与本地 orderbook 维护能力（而不仅是当前扫描式入口）。

### 3) 生产级监控与告警

- 指标体系（PnL、延迟、失败率、API 健康）和告警策略（连续失败、风控熔断）需要产品化落地。

### 4) 测试与稳定性工程

- 持续完善集成测试、异常回放、重放与压测，提升实盘前验证强度。

## 四、当前结论（简版）

- 项目已完成“可运行的交易引擎骨架 + 套利策略 MVP + 基础风控执行闭环”。
- 多策略体系和生产运维体系的扩展点已留好，下一阶段重点是：**补齐 TODO 策略与生产化能力**。

