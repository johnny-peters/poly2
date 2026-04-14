# poly2 盈利可行性审查（执行版）

更新时间：2026-04-14

本文将“盈利可行性审查计划”落地为可执行标准，目标是回答两个问题：

1. 现阶段 `poly2` 是否已经具备稳定盈利能力？
2. 如何用 14 天小额实盘给出可复现的 Go / No-Go 结论？

## 1) 证据基线（当前能力边界）

### 1.1 项目定位与策略现状

- 项目定位为交易引擎骨架，而非已验证盈利的成品系统：`README` 明确建议小资金先验证。
- 当前仅 `arbitrage` 启用，其他策略（`probability_trading` / `market_making` / `latency_arbitrage`）均未启用。
- 已实现套利触发公式：`YES_ask + NO_ask + total_cost_buffer < 1 - min_profit_threshold`。

对应证据：

- `README.md`
- `config/default.yaml`
- `src/strategy/arbitrage.rs`
- `docs/project-status.md`

### 1.2 风控与执行闭环能力（有，但仍是 MVP）

- 风控硬约束已落地：最大仓位、单笔上限、日损限制。
- 执行层具备：重试、状态轮询、部分成交处理、事件输出。
- 运行器具备：同市场冷却、止盈/止损触发平仓、持仓更新。

对应证据：

- `config/default.yaml`
- `src/runner.rs`
- `src/execution.rs`
- `src/main.rs`

### 1.3 可观测性与盈利证明能力（当前短板）

当前已有日志与持仓持久化能力，但“盈利证明”仍缺统一口径：

- 订单日志：支持 JSONL 逐笔追加（`ORDER_LOG_PATH`）。
- 持仓快照：支持读写（`POSITIONS_PATH`）。
- 循环汇总：会打印 `realized_notional`、`fees`、`unrealized_pnl`，但缺标准化日结报表。

对应证据：

- `src/persistence.rs`
- `src/main.rs`
- `src/runner.rs`

## 2) 结论（针对本金约 100，目标月净收益 >= 3%）

在当前实现阶段，不能直接认定“可稳定盈利”。主要原因：

1. 策略单一（仅套利）且市场条件依赖高，机会密度不稳定。
2. 本金较小，固定摩擦成本（手续费、滑点、失败重试、网络质量）占比偏高。
3. 尚无连续多日、统一口径的净收益证明数据。

因此当前最优动作不是放大交易，而是先用严格指标跑完 14 天验证窗口。

## 3) 统一净收益口径（必须先统一再评估）

## 3.1 基础定义

- `GrossPnl`（毛收益）：由平仓收益 + 到期兑付收益组成（不含费用）。
- `Fees`（费用）：交易费、链上相关费用、第三方接口费用分摊。
- `SlippageCost`（滑点成本）：理论成交价与实际成交均价偏差。
- `FailureCost`（失败成本）：重试、超时、拒单导致的额外损失。
- `NetPnl`（净收益）：`GrossPnl - Fees - SlippageCost - FailureCost`。

## 3.2 日收益与风险指标

- 日净收益率：`DailyNetReturn = NetPnl_day / StartCapital_day`。
- 胜率：`WinRate = 正净收益交易日数 / 总交易日数`。
- 盈亏比：`ProfitFactor = 盈利日净收益总和 / |亏损日净收益总和|`。
- 最大回撤：基于日净值序列计算 `MaxDrawdown`。
- 执行成功率：`FilledOrPartialReports / AllReports`。

## 3.3 最小数据采集规范（按日落盘）

建议新增一个日级 CSV 或 JSON 报表，字段最少包含：

- `date`
- `start_capital`
- `gross_pnl`
- `fees`
- `slippage_cost`
- `failure_cost`
- `net_pnl`
- `daily_net_return`
- `max_drawdown_to_date`
- `report_count`
- `filled_or_partial_count`

说明：当前项目已有 `ORDER_LOG_PATH` 与 `POSITIONS_PATH`，可以先以这两类数据作为日结来源，手动或脚本汇总均可。

## 4) 14 天小额实盘验证 SOP

## 4.1 目标与约束

- 验证目标：确认策略在真实交易摩擦下是否存在可持续正边际。
- 资金约束：本金约 100 时，单笔规模必须保守（避免冲击和费用吞噬）。
- 策略约束：仅启用 `arbitrage`，暂不混用其他 TODO 策略。

## 4.2 启动前检查（Day 0）

1. 运行健康检查：
   - `cargo run -- healthcheck --strict`
2. 确认关键配置：
   - `TRADING_ENABLED=true`
   - `ORDER_LOG_PATH` 已设置
   - `POSITIONS_PATH` 已设置
   - `SCAN_MIN_MINUTES_TO_SETTLE` 设置为保守值（建议 >= 30）
3. 确认风控阈值不过激：
   - `max_position_pct`
   - `max_single_trade_pct`
   - `daily_loss_limit_pct`

## 4.3 日常执行（Day 1 ~ Day 14）

每日执行流程保持一致：

1. 启动交易循环：`cargo run`
2. 运行时记录：
   - `order_report`
   - `order_summary`
   - 失败日志（scan / execute / forced_exit）
3. 日终汇总：
   - 计算 `gross_pnl / fees / slippage_cost / failure_cost / net_pnl`
   - 计算 `daily_net_return`
   - 更新累计净值与最大回撤

## 4.4 风险刹车规则（硬性）

任一条件触发即进入观察或暂停：

- 连续 3 天 `daily_net_return <= 0`
- 7 天滚动中位日收益 `<= 0`
- 执行成功率显著下降（例如 < 70%）
- 出现连续基础设施异常（RPC / CLOB / WS）

## 4.5 Go / No-Go 判定

14 天结束后按以下标准判断：

- Go（可继续迭代/小幅放大）：
  - 至少 10 天净收益为正；
  - 7 天滚动中位日收益 > 0；
  - 最大回撤在可承受范围（建议不高于风控目标）。
- No-Go（先修系统再谈盈利）：
  - 净收益不稳定、成本持续吞噬边际；
  - 或执行质量不足（大量失败/部分成交劣化）。

## 5) 针对当前仓库的优先改进项（盈利前置）

按优先级从高到低：

1. 先把日级净收益报表固化（统一口径，避免“看上去盈利”）。
2. 强化执行质量监控（成交率、拒单率、重试成本）。
3. 在套利策略稳定后，再逐步开放 `probability_trading` 或 `market_making` 做策略分散。

---

简结：`poly2` 当前具备“可交易与可风控的工程基础”，但尚未形成“可证明的稳定盈利系统”。对“本金约 100、月净收益 >= 3%”目标，最现实路径是先完成 14 天统一口径验证，再决定是否继续投入或扩展策略。
