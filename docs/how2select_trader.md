以下是 Plan C 当前筛选"跟单账号"的完整流程（代码基本集中在 `src/plan_c.rs` 的 `fetch_leaderboard_candidate_pool` + `discover_qualified_traders` + `analyze_trader_full` 三个函数里）：

## 1. 候选池构建（来源 = Polymarket 官方排行榜）

入口 `fetch_leaderboard_candidate_pool`（`src/plan_c.rs` 约 1764–1804 行）：

- 并行拉取三张榜单（默认开启 `PLAN_C_USE_MULTI_LEADERBOARD=true`）：
  - WEEK PNL
  - MONTH PNL
  - WEEK VOL
- 组池逻辑：`(WEEK PNL ∩ MONTH PNL) ∪ WEEK VOL`，再去重、按 `PLAN_C_CANDIDATE_POOL_MAX`（默认 120）截断。
- 如果关闭多榜单开关，则退化为单榜单：`PLAN_C_LEADERBOARD_PERIOD`（默认 `weekly`）+ `PNL`，条目数 `PLAN_C_LEADERBOARD_LIMIT`（默认 50）。
- 另外单独抓一份 WEEK PNL Top‑20 作为 `top20_snapshot`（用于后续 churn 比较、不是筛选条件本身）。

## 2. 硬门槛（一级过滤，仅看排行榜字段）

在 `discover_qualified_traders` 里对每个候选并行执行（并发上限 `PLAN_C_ANALYZE_CONCURRENCY`，默认 8）。入口硬门槛（约 2420 行）：

- `pnl >= PLAN_C_MIN_PNL`（默认 2000 USDC）
- `volume >= PLAN_C_MIN_VOLUME`（默认 50000 USDC）

不通过直接丢弃，不再发起后续 API。

## 3. 拉交易流水后做 FIFO + 仓位体检（二级过滤）

用 `fetch_user_trades` 拿最多 500 条近似交易，再做：

- **FIFO 实际已实现净盈亏**（`fifo_pnl_aggregate`）：
  - `net_realized >= PLAN_C_MIN_REALIZED_PNL`（默认 500）——榜单 pnl 常含未平仓浮盈，这里只认已平仓部分。
- **单市场集中度**（`max_single_market_concentration`）：
  - 单 conditionId 的 |pnl| / |net_realized| ≤ `PLAN_C_MAX_MARKET_CONCENTRATION`（默认 0.60），防"一个市场赌出来的神仙"。
- **未实现浮盈占比**（`fetch_positions_unrealized_share`）：
  - 浮动市值 / (已实现 + 浮动) ≤ `PLAN_C_MAX_UNREALIZED_RATIO`（默认 0.70），防未平仓账面党。

## 4. 行为分析 / 机器人过滤（三级过滤）

`analyze_trader_full`（约 2135–2293 行）在 `PLAN_C_ANALYSIS_HOURS`（默认 24h）窗口内统计行为，`is_bot` 命中任一条件就淘汰：

- 窗口内交易数超过 `PLAN_C_MAX_TRADES_IN_WINDOW`（默认 200）
- 平均间隔 <60s 且 notional CV < `PLAN_C_MAX_NOTIONAL_CV`（默认 0.15）且交易数 >10（典型 MM bot）
- 交易数 >30 且平均间隔 < `PLAN_C_MIN_AVG_INTERVAL_SECS`（默认 120s）
- 交易数 >20 且间隔标准差 < `PLAN_C_MIN_INTERVAL_STDDEV`（默认 10s）—— 机械等距
- 交易数 >50 且活跃小时占比 > `PLAN_C_MAX_ACTIVE_HOUR_RATIO`（默认 0.9）—— 7×24 全天在线
- 交易数 >25 且市场多样性 < `PLAN_C_MIN_MARKET_DIVERSITY`（默认 0.10）—— 死磕单市场
- 仓位尺寸 `p99/p50 > PLAN_C_MAX_SIZE_P99_P50`（默认 20）—— 巨尾，可能做市或刷量

## 5. 质量评分 + 最低分门槛（四级过滤）

调用 `compute_score_v2`（约 2103 行）做加权评分：

- 组件：Sharpe 7d、Wilson 胜率下界、profit factor（shrinkage 后）、recent ROI、drawdown penalty
- 权重来自环境变量：`PLAN_C_W_ROI / W_WIN_RATE / W_PROFIT_FACTOR / W_RECENT_ROI / W_DRAWDOWN`（默认 0.25/0.25/0.20/0.15/0.15）
- 门槛：
  - `score >= PLAN_C_MIN_COPY_SCORE`（默认 0.45）
  - 若 `PLAN_C_REQUIRE_CLOSED_TRADES=true`（默认），还要求 FIFO 已闭合对数 ≥ `PLAN_C_MIN_CLOSED_TRADES`（默认 10）—— 防止"只有浮盈、没真落袋"的假高手

## 5.5 持续盈利优先（最高排序优先级）

在通过 `min_copy_score` 等门槛之后，对同一批 `trades` 再按配置的天数窗口（默认 7 / 30 / 90 天）各跑一次 `fifo_pnl_aggregate(trades, Some(cutoff))`。**每个窗口**必须同时满足：

- FIFO 已实现净盈亏 `net_realized > PLAN_C_PERSIST_MIN_NET_PER_WINDOW`（默认 `0`）
- 窗口内闭合对数 `total >= PLAN_C_PERSIST_MIN_CLOSES_PER_WINDOW`（默认 `1`）

全部窗口都满足则 `persistent_profit = true`，否则为 `false`。**不淘汰**，只影响排序：合格者按 `persistent_profit` 降序（`true` 在前），再按 `score` 降序（`src/plan_c.rs` 中 `discover_qualified_traders` 排序逻辑）。

环境变量：`PLAN_C_PERSIST_WINDOWS_DAYS`（逗号分隔，默认 `7,30,90`）、`PLAN_C_PERSIST_MIN_NET_PER_WINDOW`、`PLAN_C_PERSIST_MIN_CLOSES_PER_WINDOW`。

**限制**：`fetch_user_trades` 当前 `limit=500`，成交极活跃的 leader 的长窗口可能被截断为「最近 500 笔」而非严格日历窗口。

## 6. 排序 + 进入跟单池

通过全部过滤的 `QualifiedTrader` 先按持续盈利标签、再按 `score` 降序排列，作为本轮 leader 列表。后续下单阶段还会再结合：

- 单领导者最多并存仓位数 `PLAN_C_MAX_PER_TRADER`（默认 2）
- 全局最多仓位数 `PLAN_C_MAX_POSITIONS`（默认 5）
- 领导者挂单时间新鲜度 `PLAN_C_MAX_TRADE_AGE_SECS`（默认 180s）
- 领导者单笔名义金额 `PLAN_C_MIN_LEADER_NOTIONAL`（默认 100 USDC）
- 市场关键字黑名单 `PLAN_C_BLOCKED_KEYWORDS`（默认屏蔽 esports 类）
- 最大买入价 `PLAN_C_MAX_BUY_PRICE`（默认 0.90）

这些"下单端"规则虽不算"筛账号"，但决定了合格 leader 的哪些动作才会被真正 copy。

## 7. Solo‑leader 模式（`PLAN_C_SOLO_LEADER_MODE=true`）

在完成 §1–§5.5 的标准流水线之后，如果开启了"独占领导者"模式，则额外执行以下裁剪：

1. **候选池**：沿用 `discover_qualified_traders` 返回的合格列表（已通过硬门槛 + FIFO + bot 过滤 + 评分门槛）。
2. **首选条件**：只在 `persistent_profit == true` 的子集中挑选（多窗口 FIFO 净值均为正，见 §5.5）。
3. **排序键**：按 `ROI = pnl / volume` 降序（即 leaderboard 期的"盈利率"）；打平时按 `score` 降序兜底。
4. **回退**：若没有 `persistent_profit` 命中，则退回到完整合格池中按 ROI 取 Top‑1（并打印 warn 日志），避免静默空跑。
5. **结果**：`qualified_traders` 裁剪到长度 **1**，下游 copy‑buy / leader‑exit 等逻辑全部沿用既有 trader‑driven 代码路径，**严格只跟随这一个账号**。

配套参数：

- `PLAN_C_SOLO_LEADER_MODE`（默认 `false`）：总开关。
- `PLAN_C_SOLO_LEADER_POLL_SECS`（默认 `5`）：该模式下 `poll_secs` 的覆盖值。每 5 秒拉一次这位 leader 的最新成交，用于驱动 copy‑buy / leader‑exit。
- `PLAN_C_MAX_PER_TRADER`：在 solo 模式下会被自动抬升到 `PLAN_C_MAX_POSITIONS`，否则 2 仓位/人 的上限会把"单一账号"锁死成只能同时持有两个仓位。

领导者重选的节奏仍走 `PLAN_C_REFRESH_LEADERS_SECS`（默认 1200s）+ WEEK PNL top‑20 churn 强制刷新；换句话说：5 秒级的刷新只作用于"跟随这位 leader 的最新成交"，而不是 5 秒就切一次人。

## 一句话总结

Plan C 的筛账号流水线是：**三张榜单取并交集 → 硬 PnL/Vol 门槛 → FIFO 已实现盈亏 + 市场集中度 + 未实现占比 → 行为反机器人 7 连测 → 加权评分 + 最低闭合对数**，全通过后打上多窗口持续盈利标签，**先按持续盈利、再按综合分**排序供跟单环节使用。若开启 `PLAN_C_SOLO_LEADER_MODE=true`，则在此之后再裁剪为"持续盈利 + ROI 最高"的单一账号，并以 5 秒节奏跟单。所有阈值都在 `.env`（`PLAN_C_*`）里可调，`src/.env` 当前值就是线上生效值。
