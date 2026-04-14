# poly2 快速上手

`poly2` 是一个用 Rust 编写的策略交易引擎骨架，当前包含：

- 策略注册与执行框架（含 `ArbitrageStrategy` 和占位 `TodoStrategy`）
- 风控引擎（仓位/单笔/日内损失限制）
- 执行客户端抽象与重试机制
- 健康检查与简单套利机会扫描工具

## 环境要求

- Rust 1.75+（建议安装最新 stable）
- 可用网络（`healthcheck` 与 `scan-arb` 会访问外部接口）

## 1 分钟启动

在项目根目录执行：

```bash
cargo build
cargo run
```

默认运行会读取：

- 配置文件：`config/default.yaml`
- 环境变量文件：`src/.env`

运行成功后会看到类似输出：

- `execution target=...`
- `run summary: processed_snapshots=...`
- `tracked markets after run=...`

## 常用命令

### 1) 默认运行（本地样例快照）

```bash
cargo run
```

说明：执行一次样例 market snapshot，并输出执行汇总。

### 2) 健康检查

```bash
cargo run -- healthcheck
```

严格模式（会额外执行启动前 preflight 校验）：

```bash
cargo run -- healthcheck --strict
```

会输出：

- `healthcheck_passed=true/false`
- 每项检查结果（HTTP、WS TCP、Polygon RPC、USDC 合约代码等）

### 3) 扫描套利候选

```bash
cargo run -- scan-arb
```

会输出：

- `arb_candidates_count=...`
- Top N 候选（edge、sum、price、slug、question）

## 配置说明

### `config/default.yaml`

核心配置分三块：

- `strategies`：策略开关与参数（如 `arbitrage.enabled`）
- `risk`：风控参数（`max_position_pct` 等）
- `execution`：执行与网络参数（HTTP/WS URL、重试、超时）
  - 包含 `scan_min_minutes_to_settle`（扫描时过滤临近结算市场，默认 30 分钟）

### `src/.env`

用于覆盖部分运行参数和注入敏感信息（如私钥/API key）。

当前代码中会读取的关键覆盖项：

- `CLOB_WS_URL` -> 覆盖 `execution.polymarket_ws_url`
- `CLOB_HTTP_URL` -> 覆盖 `execution.polymarket_http_url`
- `NETWORK_RETRY_LIMIT` 或 `RETRY_LIMIT` -> 覆盖 `execution.max_retries`
- `REQUEST_TIMEOUT_MS` -> 覆盖 `execution.timeout_secs`
- `STATUS_POLL_INTERVAL_MS` -> 覆盖 `execution.status_poll_interval_ms`
- `PM_SIGNATURE_TYPE` -> Polymarket 账户签名模式（`eoa|proxy|gnosis_safe` 或 `0|1|2`）
- `PM_FUNDER` -> 可选，指定 Polymarket 资金地址（不填时在 `proxy/gnosis_safe` 模式下会按私钥自动推导）
- `SCAN_MIN_MINUTES_TO_SETTLE` -> 扫描时过滤“距离结算不足 N 分钟”的市场（默认 30）
- `TRADING_ENABLED` -> `false` 时仅扫描不下单（支持 `true/false/1/0/on/off`）
- `TOTAL_CAPITAL_USD` -> 风控总资金（优先用于实盘）
- `TOTAL_CAPITAL_CMD` -> 通过外部命令输出风控总资金（首行必须是正数）
- `CAPITAL_REFRESH_LOOPS` -> 每 N 轮刷新一次风控总资金（默认 10）
- `MIN_TOTAL_CAPITAL_USD` -> preflight 允许的最小总资金（默认 1）
- `MIN_ORDER_SIZE_USD` -> preflight 校验套利 `order_size` 下限（默认 1）
- `CB_CONSECUTIVE_FAILURE_LIMIT` -> 连续失败达到该值触发熔断（默认 3）
- `CB_WINDOW_LOOPS` -> 失败率统计窗口（按 loop 数，默认 20）
- `CB_MAX_FAILURE_RATE` -> 窗口失败率达到该阈值触发熔断（0~1，默认 0.5）
- `CB_COOLDOWN_LOOPS` -> 熔断后跳过执行的 loop 数（默认 5）

健康检查还会读取：

- `RPC_URL`
- `USDC_CONTRACT_ADDRESS`

## 示例：只改最少配置

1. 编辑 `config/default.yaml`，确认 `execution` 中 URL 合理。  
2. 编辑 `src/.env`，至少设置：
   - `CLOB_HTTP_URL`
   - `CLOB_WS_URL`
   - `RPC_URL`
3. 运行：

```bash
cargo run -- healthcheck
```

若 `healthcheck_passed=true`，再执行：

```bash
cargo run -- scan-arb
```

## 项目结构（简版）

- `src/main.rs`：程序入口与命令分发
- `src/config.rs`：YAML + `.env` 配置加载与覆盖
- `src/engine.rs` / `src/runner.rs`：引擎与运行器
- `src/risk.rs`：风控逻辑
- `src/execution.rs`：执行客户端与重试
- `src/healthcheck.rs`：连通性检查与套利扫描
- `src/strategy/`：策略实现与注册
- `examples/event_stream.rs`：事件流执行示例

## 运行示例程序

```bash
cargo run --example event_stream
```

## 盈利验证文档

- `docs/profitability-feasibility.md`：盈利可行性审查（证据基线、净收益口径、Go/No-Go 标准）
- `docs/pilot-14d-checklist.md`：14 天小额实盘验证清单
- `docs/templates/daily-profit-report.csv`：日级收益记录模板
- `scripts/generate_daily_profit_report.py`：从 `ORDER_LOG_PATH` + `POSITIONS_PATH` 半自动生成/更新日报

日报脚本默认可无参运行（默认读取 `data/orders.jsonl`、`data/positions.json`、`start_capital=100`）：

```bash
python scripts/generate_daily_profit_report.py
```

## 常见问题

- Q: 为什么 `scan-arb` 没有候选？  
  A: 市场状态过滤较严格（active/accepting_orders 等），且只有 `price_a + price_b < 1` 才会入选。

- Q: `healthcheck` 失败怎么办？  
  A: 先检查 `src/.env` 中的 `RPC_URL`、`CLOB_HTTP_URL`、`CLOB_WS_URL`，再确认网络可访问对应域名。

- Q: 可以直接用于实盘吗？  
  A: 当前更偏工程骨架与验证工具，建议先在小资金/模拟环境验证风控与执行细节。
