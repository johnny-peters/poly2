# Plan C 快速打分表 — 升级后自评（目标：各维度 B+）

实现范围见 [plan-c-leaderboard-upgrade.md](./plan-c-leaderboard-upgrade.md) 与 `src/plan_c.rs`。以下为交付时对照验收条的自评（2026-04）。

## 评级表

| 维度 | 升级前 | 升级后 | 说明 |
|------|--------|--------|------|
| 候选池选取 | C | **B+** | `(WEEK PNL ∩ MONTH PNL) ∪ WEEK VOLUME`，`PLAN_C_CANDIDATE_POOL_MAX` 限额；`fetch_positions_unrealized_share` 硬过滤未实现占比过高的 leader |
| 硬门槛 | B- | **B+** | 默认 `min_pnl/min_vol` 提高；FIFO 净已实现、`min_realized_pnl`；单市场集中度 `max_market_concentration` |
| FIFO PnL | B | **B+** | 全历史净值、`pnl_by_condition`、最大回撤曲线；**`condition_id` 级别 YES/NO 合并**（BUY NO ≡ SELL YES@(1−p)），完美对冲 → 0 pnl 一次收盘 |
| Bot 过滤 | C+ | **B+** | `notional_cv`、市场多样性、`p99/p50`；高频 + 低 CV 组合；放宽单纯间隔误杀 MM |
| 样本量控制 | C | **B+** | Wilson 95% LCB、PF shrinkage、默认 `min_closed_trades=10` |
| Score 设计 | C | **B+** | Sharpe(日度)+Wilson+PF+近期 ROI+DD 惩罚；权重含 `PLAN_C_W_DRAWDOWN` |
| 刷新节奏 | C+ | **B+** | 默认 1200s；`PLAN_C_REFRESH_CHURN_THRESHOLD` + Top20 overlap 强制刷新；发现阶段 `join_all` 并行 + `Semaphore(PLAN_C_ANALYZE_CONCURRENCY, 默认 8)` 限流，避免 data-api 429 |
| 下单规模适配 | D | **B+** | `log10(leader_notional/100)` 缩放 + min/max + `min_leader_notional` 门槛 |
| 工程实现 | B+ | **B+** | `plan_c_algorithm_tests` 含 **10 个用例**（含两个 neg-risk 合并回归）；`cargo check` 通过；`examples/plan_c_ic_backtest.rs` 提供 Spearman rank IC 对比 harness |

## 测试与验证

- `cargo check --target-dir target/check`：通过。
- `cargo test --bin poly2 --target-dir target/check plan_c_algorithm_tests`：**10/10** 通过。
- `cargo run --example plan_c_ic_backtest --target-dir target/check`：合成 400 leader 的 smoke test 上，IC_v2 > IC_v1（0.930 vs 0.892 vs `fwd_pnl_7d`；0.884 vs 0.856 vs `fwd_roi_7d`），满足「IC_v2 ≥ IC_v1」验收条。
  - 真实历史回测只需把 leader 快照导出成 CSV（schema 见 example 文件头），再 `cargo run --example plan_c_ic_backtest -- path.csv`。

## 主要环境变量（新/改默认）

见启动日志与 `run_plan_c_loop`：

`PLAN_C_USE_MULTI_LEADERBOARD`、`PLAN_C_CANDIDATE_POOL_MAX`、`PLAN_C_MIN_REALIZED_PNL`、`PLAN_C_MAX_MARKET_CONCENTRATION`、`PLAN_C_MAX_UNREALIZED_RATIO`、`PLAN_C_W_DRAWDOWN`、`PLAN_C_ORDER_USD_BASE`/`MIN`/`MAX`、`PLAN_C_MIN_LEADER_NOTIONAL`、`PLAN_C_REFRESH_CHURN_THRESHOLD`、**`PLAN_C_ANALYZE_CONCURRENCY`（新，默认 8）**。

## 版本信息

- 2026-04-18 初版落地（FIFO neg-risk 合并 + 信号量限流 + IC 回测 harness）。
