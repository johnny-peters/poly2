以下是 Plan C 当前筛选"跟单账号"的完整流程（代码基本集中在 `src/plan_c.rs` 的 `fetch_leaderboard_candidate_pool` + `discover_qualified_traders` + `analyze_trader_full` 三个函数里）。

**2026-04 改版**：原来的 FIFO 净已实现 / 市场集中度 / 未实现占比 / min_copy_score / require_closed_trades 五条硬门槛全部停用，统一切换为下方 §2（五条用户指定硬门槛）+ §3（反机器人七连测）。

## 1. 候选池构建（来源 = Polymarket 官方排行榜）

入口 `fetch_leaderboard_candidate_pool`（`src/plan_c.rs` 约 1764–1804 行）：

- 并行拉取三张榜单（默认开启 `PLAN_C_USE_MULTI_LEADERBOARD=true`）：
  - WEEK PNL
  - MONTH PNL
  - WEEK VOL
- 组池逻辑：`(WEEK PNL ∩ MONTH PNL) ∪ WEEK VOL`，再去重、按 `PLAN_C_CANDIDATE_POOL_MAX`（默认 120）截断。
- 如果关闭多榜单开关，则退化为单榜单：`PLAN_C_LEADERBOARD_PERIOD`（默认 `weekly`）+ `PNL`，条目数 `PLAN_C_LEADERBOARD_LIMIT`（默认 50）。
- 另外单独抓一份 WEEK PNL Top‑20 作为 `top20_snapshot`（用于后续 churn 比较、不是筛选条件本身）。

## 2. 五条硬门槛（新版扫描模式）

在 `discover_qualified_traders` 里对每个候选并行执行（并发上限 `PLAN_C_ANALYZE_CONCURRENCY`，默认 8）。按下列顺序短路检查，任一不通过立刻丢弃，不再发起后续 API：

1. **Overall PnL 门槛** — 来自 leaderboard entry：`pnl >= PLAN_C_MIN_PNL`（默认 20000，USD）。
2. **交易量门槛** — `volume >= PLAN_C_MIN_VOLUME`（默认 0，事实上关闭；保留 env 以便需要时再启用）。
3. **Active Positions 门槛** — 调 `data-api.polymarket.com/positions?sizeThreshold=0.01`，计数满足 `size>0.01 && curPrice>0 && !redeemable` 的条目（见 `count_active_open_positions`），要求 ≥ `PLAN_C_MIN_ACTIVE_POSITIONS`（默认 2）。排除了已结算等待赎回的"僵尸仓位"。
4. **地址 pUSD 余额门槛** — 用 `fetch_pusd_balance_multi`（`src/healthcheck.rs`，原名 `fetch_usdc_balance_multi`，2026-04-28 V2 cutover 后改名）在 `PLAN_C_RPC_URLS` 列表上做多 RPC 故障转移，查询 `PLAN_C_COLLATERAL_ADDRESS`（旧 alias `PLAN_C_USDC_ADDRESS` 仍可用）余额 ≥ `PLAN_C_MIN_ADDRESS_PUSD`（旧 alias `PLAN_C_MIN_ADDRESS_USDC`，默认 20000 pUSD）。任一候选的 RPC 全部失败即判不通过（保守策略，避免跟单到没有实际资金的钱包）。
5. **Win Rate 门槛** — 调 `fetch_user_trades(limit=500)` → `fifo_pnl_aggregate(trades, None)`，计算 `win_rate_raw = wins / total`，要求 `win_rate_raw >= PLAN_C_MIN_WIN_RATE`（默认 0.80）**且** `total >= PLAN_C_MIN_CLOSED_TRADES`（默认 10，作为最小样本量守卫，防止 1/1 = 100% 蒙混）。

**市场不限**：旧版 `PLAN_C_MAX_MARKET_CONCENTRATION` 的单市场集中度限制已移除——任何市场组合都被允许通过候选阶段（下单阶段仍有 `PLAN_C_BLOCKED_KEYWORDS` 关键词黑名单在，与这里无关）。

## 3. 行为分析 / 机器人过滤

`analyze_trader_full`（约 2135–2293 行）在 `PLAN_C_ANALYSIS_HOURS`（默认 24h）窗口内统计行为，`is_bot` 命中任一条件就淘汰：

- 窗口内交易数超过 `PLAN_C_MAX_TRADES_IN_WINDOW`（默认 200）
- 平均间隔 <60s 且 notional CV < `PLAN_C_MAX_NOTIONAL_CV`（默认 0.15）且交易数 >10（典型 MM bot）
- 交易数 >30 且平均间隔 < `PLAN_C_MIN_AVG_INTERVAL_SECS`（默认 120s）
- 交易数 >20 且间隔标准差 < `PLAN_C_MIN_INTERVAL_STDDEV`（默认 10s）—— 机械等距
- 交易数 >50 且活跃小时占比 > `PLAN_C_MAX_ACTIVE_HOUR_RATIO`（默认 0.9）—— 7×24 全天在线
- 交易数 >25 且市场多样性 < `PLAN_C_MIN_MARKET_DIVERSITY`（默认 0.10）—— 死磕单市场
- 仓位尺寸 `p99/p50 > PLAN_C_MAX_SIZE_P99_P50`（默认 20）—— 巨尾，可能做市或刷量

## 4. 持续盈利标签（仅用于排序，不淘汰）

在通过 §2+§3 之后，对同一批 `trades` 再按配置的天数窗口（默认 7 / 30 / 90 天）各跑一次 `fifo_pnl_aggregate(trades, Some(cutoff))`。**每个窗口**必须同时满足：

- FIFO 已实现净盈亏 `net_realized > PLAN_C_PERSIST_MIN_NET_PER_WINDOW`（默认 `0`）
- 窗口内闭合对数 `total >= PLAN_C_PERSIST_MIN_CLOSES_PER_WINDOW`（默认 `1`）

全部窗口都满足则 `persistent_profit = true`，否则为 `false`。**不淘汰**，只影响排序：合格者按 `persistent_profit` 降序（`true` 在前），再按 `score` 降序。

环境变量：`PLAN_C_PERSIST_WINDOWS_DAYS`（逗号分隔，默认 `7,30,90`）、`PLAN_C_PERSIST_MIN_NET_PER_WINDOW`、`PLAN_C_PERSIST_MIN_CLOSES_PER_WINDOW`。

**限制**：`fetch_user_trades` 当前 `limit=500`，成交极活跃的 leader 的长窗口可能被截断为「最近 500 笔」而非严格日历窗口。

## 5. 旧版门槛现状（全部已停用）

下列原来的过滤条件不再参与淘汰，仅 env 键保留为兼容读取：

| 旧门槛 env | 旧默认值 | 现状 |
| - | - | - |
| `PLAN_C_MIN_REALIZED_PNL` | 500 | 已停用（FIFO 净已实现不再硬过滤） |
| `PLAN_C_MAX_MARKET_CONCENTRATION` | 1.0 | 已停用（"市场不限"） |
| `PLAN_C_MAX_UNREALIZED_RATIO` | 0.70 | 已停用 |
| `PLAN_C_MIN_COPY_SCORE` | 0.45 | 已停用（score 仍算、仅用于排序 / 日志） |
| `PLAN_C_REQUIRE_CLOSED_TRADES` | true | 已停用 |
| `PLAN_C_MIN_CLOSED_TRADES` | 10 | **仍在用**，语义改为 Win Rate 门槛的最小样本量下限 |

`compute_score_v2` 依旧输出 `score`、Sharpe、Wilson WR、profit factor、recent ROI、drawdown penalty，这些值会打印到 QUALIFIED 日志中以便 ops 对账，但不参与决策。

## 6. 排序 + 进入跟单池

通过 §2+§3 过滤的 `QualifiedTrader` 先按 `persistent_profit` 降序、再按 `score` 降序，作为本轮 leader 列表。后续下单阶段还会再结合：

- 单领导者最多并存仓位数 `PLAN_C_MAX_PER_TRADER`（默认 2）
- 全局最多仓位数 `PLAN_C_MAX_POSITIONS`（默认 5）
- 领导者挂单时间新鲜度 `PLAN_C_MAX_TRADE_AGE_SECS`（默认 180s）
- 领导者单笔名义金额 `PLAN_C_MIN_LEADER_NOTIONAL`（默认 100 pUSD；2026-04-28 V2 cutover 前为 USDC.e）
- 市场关键字黑名单 `PLAN_C_BLOCKED_KEYWORDS`（默认屏蔽 esports 类）
- 最大买入价 `PLAN_C_MAX_BUY_PRICE`（默认 0.90）

这些"下单端"规则虽不算"筛账号"，但决定了合格 leader 的哪些动作才会被真正 copy。

## 7. Solo‑leader 模式（`PLAN_C_SOLO_LEADER_MODE=true`）

在完成 §1–§4 的标准流水线之后，如果开启了"独占领导者"模式，则额外执行以下裁剪：

1. **候选池**：沿用 `discover_qualified_traders` 返回的合格列表（已通过 §2 + §3）。
2. **首选条件**：只在 `persistent_profit == true` 的子集中挑选（多窗口 FIFO 净值均为正，见 §4）。
3. **排序键**：按 `ROI = pnl / volume` 降序（即 leaderboard 期的"盈利率"）；打平时按 `score` 降序兜底。
4. **回退**：若没有 `persistent_profit` 命中，则退回到完整合格池中按 ROI 取 Top‑1（并打印 warn 日志），避免静默空跑。
5. **结果**：`qualified_traders` 裁剪到长度 **1**，下游 copy‑buy / leader‑exit 等逻辑全部沿用既有 trader‑driven 代码路径，**严格只跟随这一个账号**。

配套参数：

- `PLAN_C_SOLO_LEADER_MODE`（默认 `false`）：总开关。
- `PLAN_C_SOLO_LEADER_POLL_SECS`（默认 `5`）：该模式下 `poll_secs` 的覆盖值。每 5 秒拉一次这位 leader 的最新成交，用于驱动 copy‑buy / leader‑exit。
- `PLAN_C_MAX_PER_TRADER`：在 solo 模式下会被自动抬升到 `PLAN_C_MAX_POSITIONS`，否则 2 仓位/人 的上限会把"单一账号"锁死成只能同时持有两个仓位。

领导者重选的节奏仍走 `PLAN_C_REFRESH_LEADERS_SECS`（默认 1200s）+ WEEK PNL top‑20 churn 强制刷新；换句话说：5 秒级的刷新只作用于"跟随这位 leader 的最新成交"，而不是 5 秒就切一次人。

## §0. 手动模式（`USER_ADDRESSES`）

若在 `.env` 中配置了非空的 `USER_ADDRESSES`（单地址、逗号分隔或 JSON 数组，见 `src/.env` 注释），则 **§1–§4 的榜单发现与筛选** 以及 **§7 的 `PLAN_C_SOLO_LEADER_MODE` 独占领导者裁剪** 全部跳过：跟单池直接等于你列出的地址，等价于「你已自行选好账号，只把地址交给跟单引擎」。`PLAN_C_POLL_SECS` 等轮询节奏仍生效；下单端的时效、名义金额、买价上限、黑词、仓位上限等规则不受影响。

## 一句话总结

Plan C 扫描模式的筛账号流水线是：**三张榜单取并交集 → 五条硬门槛（Overall PnL / 交易量 / Active Positions / 地址 USDC 余额 / FIFO 原始胜率）→ 反机器人七连测**，全通过后打上多窗口持续盈利标签，**先按持续盈利、再按综合分**排序供跟单环节使用。若开启 `PLAN_C_SOLO_LEADER_MODE=true`，则在此之后再裁剪为"持续盈利 + ROI 最高"的单一账号，并以 5 秒节奏跟单。所有阈值都在 `.env`（`PLAN_C_*`）里可调，`src/.env` 当前值就是线上生效值。
