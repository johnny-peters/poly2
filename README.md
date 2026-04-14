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
- `SCAN_MIN_MINUTES_TO_SETTLE` -> 扫描时过滤“距离结算不足 N 分钟”的市场（默认 30）

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

## 常见问题

- Q: 为什么 `scan-arb` 没有候选？  
  A: 市场状态过滤较严格（active/accepting_orders 等），且只有 `price_a + price_b < 1` 才会入选。

- Q: `healthcheck` 失败怎么办？  
  A: 先检查 `src/.env` 中的 `RPC_URL`、`CLOB_HTTP_URL`、`CLOB_WS_URL`，再确认网络可访问对应域名。

- Q: 可以直接用于实盘吗？  
  A: 当前更偏工程骨架与验证工具，建议先在小资金/模拟环境验证风控与执行细节。
