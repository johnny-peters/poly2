use anyhow::{anyhow, Context};
use chrono::Utc;
use futures::future::join_all;
use rust_decimal::{Decimal, RoundingStrategy};
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::{self, Duration};

use poly2::{ExecutionClient, ExecutionStatus, RiskEngine, Side, StrategyContext, StrategyId};

use crate::{
    append_round_record, fetch_clob_best_bid, print_execution_report, read_decimal_env_or,
    read_f64_env_or, read_u64_env_or, resolve_runtime_var,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExitMode {
    LeaderWithSafety,
    LeaderOnly,
    LocalOnly,
}

fn parse_exit_mode(raw: Option<String>) -> ExitMode {
    let normalized = raw
        .unwrap_or_else(|| "leader_with_safety".to_string())
        .trim()
        .to_ascii_lowercase();
    match normalized.as_str() {
        "leader_with_safety" | "leader-with-safety" | "leader_safety" | "default" | "" => {
            ExitMode::LeaderWithSafety
        }
        "leader_only" | "leader-only" | "leader" => ExitMode::LeaderOnly,
        "local_only" | "local-only" | "local" => ExitMode::LocalOnly,
        other => {
            eprintln!(
                "plan_c: warn: PLAN_C_EXIT_MODE='{}' not recognised, using leader_with_safety",
                other
            );
            ExitMode::LeaderWithSafety
        }
    }
}

fn mode_has_local_safety(mode: ExitMode) -> bool {
    matches!(mode, ExitMode::LeaderWithSafety | ExitMode::LocalOnly)
}

fn mode_has_leader_exit(mode: ExitMode) -> bool {
    matches!(mode, ExitMode::LeaderWithSafety | ExitMode::LeaderOnly)
}

/// Build a conservative sell size that is guaranteed to be <= the actual
/// on-chain balance for a position, even when the local `pos.size` tracks the
/// on-chain size up to a few micro-units of precision drift.
///
/// This prevents the Polymarket CLOB from rejecting a close with
/// `not enough balance / allowance` errors such as
/// `balance: 14007060, order amount: 14010000` (observed off-by-<=0.003 share)
/// by flooring to 2dp and shaving a single 0.01-share "dust" margin.
/// The worst-case loss is 0.01 × sell_price (< $0.01 per exit), which is
/// negligible compared with repeatedly failing to exit a position.
fn conservative_sell_size(raw: Decimal) -> Decimal {
    let min_size = Decimal::new(1, 2); // 0.01 shares
    let floored = raw.round_dp_with_strategy(2, RoundingStrategy::ToZero);
    let shaved = floored - min_size;
    if shaved <= Decimal::ZERO {
        // Position too small to safely shave — fall back to the floored value
        // and let the exchange decide; at that size we prefer trying over giving up.
        floored.max(min_size)
    } else {
        shaved
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
#[allow(dead_code)]
struct LeaderboardEntry {
    #[serde(default)]
    rank: Option<String>,
    #[serde(default, alias = "proxyWallet")]
    proxy_wallet: Option<String>,
    #[serde(default, alias = "userName")]
    user_name: Option<String>,
    #[serde(default)]
    vol: Option<f64>,
    #[serde(default)]
    pnl: Option<f64>,
    #[serde(default, alias = "profileImage")]
    profile_image: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[allow(dead_code)]
struct TradeRecord {
    #[serde(default, alias = "proxyWallet")]
    proxy_wallet: Option<String>,
    #[serde(default)]
    side: Option<String>,
    #[serde(default)]
    asset: Option<String>,
    #[serde(default, alias = "conditionId")]
    condition_id: Option<String>,
    #[serde(default)]
    size: Option<f64>,
    #[serde(default)]
    price: Option<f64>,
    #[serde(default)]
    timestamp: Option<i64>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    slug: Option<String>,
    #[serde(default, alias = "outcomeIndex")]
    outcome_index: Option<i64>,
    #[serde(default)]
    outcome: Option<String>,
    #[serde(default, alias = "usdcSize")]
    usdc_size: Option<f64>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct QualifiedTrader {
    address: String,
    user_name: String,
    pnl: f64,
    volume: f64,
    avg_interval_secs: f64,
    trade_count: usize,
    /// Return on turnover for leaderboard period: `pnl / volume` (diagnostic).
    roi: f64,
    /// Sharpe-like score from last-7d daily realized PnL (see `analyze_trader_full`).
    sharpe_7d: f64,
    /// Net realized / buy notional in last `recent_window_hours`.
    ret_7d: f64,
    win_rate: f64,
    profit_factor: f64,
    recent_roi: f64,
    max_drawdown: f64,
    score: f64,
}

#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
struct TraderAnalysis {
    avg_interval_secs: f64,
    trade_count: usize,
    interval_stddev: f64,
    active_hour_ratio: f64,
    /// Raw WR from FIFO closes (wins / total); kept for logging only.
    win_rate_raw: f64,
    /// Wilson 95% lower bound on WR.
    wilson_wr_lcb: f64,
    /// Bayesian-shrunk profit factor.
    profit_factor: f64,
    /// Fixed-formula recent ROI: net 7d / buy notional 7d.
    recent_roi: f64,
    sharpe_7d: f64,
    ret_7d: f64,
    max_drawdown: f64,
    /// Net realized PnL from all FIFO-closed trades in history (gp - gl).
    net_realized_pnl: f64,
    notional_cv: f64,
    market_diversity: f64,
    size_p99_p50: f64,
    /// Number of FIFO-closed trades over the full trade history.
    /// `0` means we could not pair any BUY→SELL, so win_rate / profit_factor
    /// are meaningless zeros, not real statistics.
    closed_trade_count: usize,
    is_bot: bool,
}

/// Lifecycle state of a copied position. Active positions occupy a slot in the
/// `max_positions` budget; Resolved/RedeemFailed positions do not — they are
/// waiting to be redeemed on-chain and should not block new entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum PositionStatus {
    Active,
    /// Market has settled on-chain; we're waiting for (or about to perform) redeem.
    Resolved,
    /// Previous redeem attempt failed; retry with exponential-ish backoff.
    RedeemFailed,
}

impl Default for PositionStatus {
    fn default() -> Self {
        PositionStatus::Active
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct CopiedPosition {
    trader: String,
    market_id: String,
    token_id: String,
    side: Side,
    entry_price: Decimal,
    size: Decimal,
    opened_at: i64,
    leader_buy_ts: i64,
    /// Lifecycle status — only Active positions count toward `max_positions`.
    status: PositionStatus,
    /// Final settlement price (0 or 1 USDC per share) if known.
    resolved_price: Option<Decimal>,
    /// Consecutive rounds where `fetch_clob_best_bid` failed for this token.
    /// When it crosses `clob_fail_threshold` we treat the market as closed/resolved.
    clob_fail_count: u32,
    /// How many times redeem has been attempted without success.
    redeem_retry_count: u32,
    /// Unix ts of the last redeem attempt (for backoff).
    last_redeem_attempt: i64,
    /// Cached negRisk flag so we route redeem to the correct contract.
    neg_risk: Option<bool>,
    /// Human-readable market title (used for the local history ledger).
    market_title: String,
    /// Human-readable leader name (used for the local history ledger).
    trader_name: String,
}

// ----------------------------------------------------------------------------
// Persistence & on-chain reconciliation
// ----------------------------------------------------------------------------

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct PersistedPosition {
    trader: String,
    market_id: String,
    token_id: String,
    side: String, // "Yes" | "No"
    entry_price: String,
    size: String,
    opened_at: i64,
    leader_buy_ts: i64,
    #[serde(default)]
    status: PositionStatus,
    #[serde(default)]
    resolved_price: Option<String>,
    #[serde(default)]
    clob_fail_count: u32,
    #[serde(default)]
    redeem_retry_count: u32,
    #[serde(default)]
    last_redeem_attempt: i64,
    #[serde(default)]
    neg_risk: Option<bool>,
    #[serde(default)]
    market_title: String,
    #[serde(default)]
    trader_name: String,
}

fn side_to_persisted(s: &Side) -> &'static str {
    match s {
        Side::Yes => "Yes",
        Side::No => "No",
    }
}

fn side_from_persisted(s: &str) -> Side {
    match s.trim() {
        "No" | "no" | "NO" => Side::No,
        _ => Side::Yes,
    }
}

impl PersistedPosition {
    fn from_position(p: &CopiedPosition) -> Self {
        Self {
            trader: p.trader.clone(),
            market_id: p.market_id.clone(),
            token_id: p.token_id.clone(),
            side: side_to_persisted(&p.side).to_string(),
            entry_price: p.entry_price.to_string(),
            size: p.size.to_string(),
            opened_at: p.opened_at,
            leader_buy_ts: p.leader_buy_ts,
            status: p.status,
            resolved_price: p.resolved_price.map(|d| d.to_string()),
            clob_fail_count: p.clob_fail_count,
            redeem_retry_count: p.redeem_retry_count,
            last_redeem_attempt: p.last_redeem_attempt,
            neg_risk: p.neg_risk,
            market_title: p.market_title.clone(),
            trader_name: p.trader_name.clone(),
        }
    }

    fn to_position(&self) -> Option<CopiedPosition> {
        let entry_price = Decimal::from_str(self.entry_price.trim()).ok()?;
        let size = Decimal::from_str(self.size.trim()).ok()?;
        let resolved_price = self
            .resolved_price
            .as_deref()
            .and_then(|s| Decimal::from_str(s.trim()).ok());
        Some(CopiedPosition {
            trader: self.trader.clone(),
            market_id: self.market_id.clone(),
            token_id: self.token_id.clone(),
            side: side_from_persisted(&self.side),
            entry_price,
            size,
            opened_at: self.opened_at,
            leader_buy_ts: self.leader_buy_ts,
            status: self.status,
            resolved_price,
            clob_fail_count: self.clob_fail_count,
            redeem_retry_count: self.redeem_retry_count,
            last_redeem_attempt: self.last_redeem_attempt,
            neg_risk: self.neg_risk,
            market_title: self.market_title.clone(),
            trader_name: self.trader_name.clone(),
        })
    }
}

/// A buy order that was accepted by the exchange but not immediately filled.
/// Lives in its own bucket (separate from `open_positions`) until:
///   * it fills on the book (promoted to an Active `CopiedPosition`), or
///   * it times out and we cancel (after which any partial fill is promoted), or
///   * the exchange reports it as rejected/cancelled (dropped outright).
/// Counts toward `max_positions` and per-trader caps so we don't oversubscribe.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct PendingBuy {
    order_id: String,
    trader: String,
    market_id: String,
    token_id: String,
    side: Side,
    /// Limit price we submitted — becomes `entry_price` on promotion.
    price: Decimal,
    /// Remaining requested size (initially == submitted size, decays as partial fills).
    size: Decimal,
    /// Cumulative matched amount as of the last exchange poll.
    matched_cum: Decimal,
    leader_buy_ts: i64,
    submitted_at: i64,
    last_checked_at: i64,
    market_title: String,
    trader_name: String,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct PersistedPendingBuy {
    order_id: String,
    trader: String,
    market_id: String,
    token_id: String,
    side: String,
    price: String,
    size: String,
    #[serde(default)]
    matched_cum: String,
    leader_buy_ts: i64,
    submitted_at: i64,
    #[serde(default)]
    last_checked_at: i64,
    #[serde(default)]
    market_title: String,
    #[serde(default)]
    trader_name: String,
}

impl PersistedPendingBuy {
    fn from_pending(p: &PendingBuy) -> Self {
        Self {
            order_id: p.order_id.clone(),
            trader: p.trader.clone(),
            market_id: p.market_id.clone(),
            token_id: p.token_id.clone(),
            side: side_to_persisted(&p.side).to_string(),
            price: p.price.to_string(),
            size: p.size.to_string(),
            matched_cum: p.matched_cum.to_string(),
            leader_buy_ts: p.leader_buy_ts,
            submitted_at: p.submitted_at,
            last_checked_at: p.last_checked_at,
            market_title: p.market_title.clone(),
            trader_name: p.trader_name.clone(),
        }
    }

    fn to_pending(&self) -> Option<PendingBuy> {
        let price = Decimal::from_str(self.price.trim()).ok()?;
        let size = Decimal::from_str(self.size.trim()).ok()?;
        let matched_cum = Decimal::from_str(self.matched_cum.trim()).unwrap_or(Decimal::ZERO);
        Some(PendingBuy {
            order_id: self.order_id.clone(),
            trader: self.trader.clone(),
            market_id: self.market_id.clone(),
            token_id: self.token_id.clone(),
            side: side_from_persisted(&self.side),
            price,
            size,
            matched_cum,
            leader_buy_ts: self.leader_buy_ts,
            submitted_at: self.submitted_at,
            last_checked_at: self.last_checked_at,
            market_title: self.market_title.clone(),
            trader_name: self.trader_name.clone(),
        })
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct PlanCState {
    #[serde(default)]
    positions: Vec<PersistedPosition>,
    #[serde(default)]
    pending_buys: Vec<PersistedPendingBuy>,
    #[serde(default)]
    last_seen_trades: HashMap<String, i64>,
    #[serde(default)]
    daily_pnl: String,
    /// UTC calendar date (YYYY-MM-DD) that `daily_pnl` was accrued on.
    /// Empty on legacy state files — in that case we conservatively start
    /// fresh on the next load (daily_pnl reset to 0).
    #[serde(default)]
    daily_pnl_date: String,
    #[serde(default)]
    saved_at: String,
}

/// UTC calendar date formatted as `YYYY-MM-DD`. Used to decide when a new
/// trading day has started and the `daily_pnl` risk counter should reset.
fn current_utc_date_str() -> String {
    Utc::now().date_naive().to_string()
}

fn save_plan_c_state(
    path: &Path,
    positions: &[CopiedPosition],
    pending_buys: &[PendingBuy],
    last_seen_trades: &HashMap<String, i64>,
    daily_pnl: Decimal,
    daily_pnl_date: &str,
) {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                eprintln!("plan_c: failed to create state dir {}: {e}", parent.display());
                return;
            }
        }
    }
    let state = PlanCState {
        positions: positions.iter().map(PersistedPosition::from_position).collect(),
        pending_buys: pending_buys
            .iter()
            .map(PersistedPendingBuy::from_pending)
            .collect(),
        last_seen_trades: last_seen_trades.clone(),
        daily_pnl: daily_pnl.to_string(),
        daily_pnl_date: daily_pnl_date.to_string(),
        saved_at: Utc::now().to_rfc3339(),
    };
    match serde_json::to_string_pretty(&state) {
        Ok(json) => {
            let tmp = path.with_extension("json.tmp");
            if let Err(e) = std::fs::write(&tmp, &json) {
                eprintln!(
                    "plan_c: failed to write state tmp {}: {e}",
                    tmp.display()
                );
                return;
            }
            if let Err(e) = std::fs::rename(&tmp, path) {
                // On Windows rename onto existing file may fail; fall back to direct write.
                if let Err(e2) = std::fs::write(path, &json) {
                    eprintln!(
                        "plan_c: failed to persist state to {}: rename err={e}, write err={e2}",
                        path.display()
                    );
                }
            }
        }
        Err(e) => eprintln!("plan_c: failed to serialise state: {e}"),
    }
}

// ----------------------------------------------------------------------------
// Local trade history ledger (CSV) — append on buy, finalise on close.
//
// Columns (in order):
//   event, trader, side, shares, buy_price, buy_cost, buy_time,
//   sell_price, profit, sell_time
//
// Rows are identified by the tuple (trader, event, side, shares, buy_price,
// buy_time) which is unique in practice: even if a trader repeats the same
// market at the same price, opened_at is second-precision so the formatted
// buy-time differs between separate positions.
// ----------------------------------------------------------------------------

const HISTORY_HEADERS: &[&str] = &[
    "event",
    "trader",
    "side",
    "shares",
    "buy_price",
    "buy_cost",
    "buy_time",
    "sell_price",
    "profit",
    "sell_time",
];

/// Old Chinese header set — if we find this on disk we rewrite it to English
/// in place so downstream tooling sees consistent column names.
const HISTORY_HEADERS_LEGACY_CN: &[&str] = &[
    "事件名",
    "跟单账号",
    "YES/NO",
    "份额",
    "买入单价",
    "买入总成本",
    "买入时间",
    "卖出单价",
    "盈利",
    "卖出时间",
];

fn history_log_path(dotenv_path: &Path) -> std::path::PathBuf {
    let raw = resolve_runtime_var("PLAN_C_HISTORY_PATH", dotenv_path)
        .unwrap_or_else(|| "data/plan_c_history.csv".to_string());
    std::path::PathBuf::from(raw)
}

fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        let escaped = s.replace('"', "\"\"");
        format!("\"{}\"", escaped)
    } else {
        s.to_string()
    }
}

fn format_ts_human(ts: i64) -> String {
    chrono::DateTime::<Utc>::from_timestamp(ts, 0)
        .map(|d| d.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| ts.to_string())
}

fn ensure_history_file(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let needs_header = match std::fs::metadata(path) {
        Ok(m) => m.len() == 0,
        Err(_) => true,
    };
    if needs_header {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        use std::io::Write as _;
        writeln!(f, "{}", HISTORY_HEADERS.join(","))?;
        return Ok(());
    }

    // Migrate legacy Chinese headers to English in place, preserving all data
    // rows. Only runs once — second time around the header already matches.
    let raw = std::fs::read_to_string(path)?;
    let mut lines: Vec<String> = raw.lines().map(|s| s.to_string()).collect();
    let legacy_header = HISTORY_HEADERS_LEGACY_CN.join(",");
    if !lines.is_empty() && lines[0] == legacy_header {
        lines[0] = HISTORY_HEADERS.join(",");
        let tmp = path.with_extension("csv.tmp");
        let body = lines.join("\n") + "\n";
        std::fs::write(&tmp, body.as_bytes())?;
        if std::fs::rename(&tmp, path).is_err() {
            // Windows may fail to rename over an existing file; fall back.
            std::fs::write(path, lines.join("\n") + "\n")?;
        }
        eprintln!(
            "plan_c: history: migrated legacy CN headers → EN in {}",
            path.display()
        );
    }
    Ok(())
}

fn append_history_open(
    path: &Path,
    event: &str,
    trader_name: &str,
    side: &Side,
    size: Decimal,
    buy_price: Decimal,
    buy_ts: i64,
) {
    if let Err(e) = ensure_history_file(path) {
        eprintln!("plan_c: history: cannot prep ledger {}: {e}", path.display());
        return;
    }
    let side_str = side_to_persisted(side);
    let total_cost = (buy_price * size).round_dp(4);
    let row = vec![
        csv_escape(event),
        csv_escape(trader_name),
        csv_escape(side_str),
        size.to_string(),
        buy_price.to_string(),
        total_cost.to_string(),
        csv_escape(&format_ts_human(buy_ts)),
        String::new(),
        String::new(),
        String::new(),
    ];
    match std::fs::OpenOptions::new().append(true).open(path) {
        Ok(mut f) => {
            use std::io::Write as _;
            if let Err(e) = writeln!(f, "{}", row.join(",")) {
                eprintln!(
                    "plan_c: history: append failed on {}: {e}",
                    path.display()
                );
            }
        }
        Err(e) => eprintln!(
            "plan_c: history: open-for-append failed on {}: {e}",
            path.display()
        ),
    }
}

/// Update the most recent still-open row that matches the given position,
/// filling in sell_price / profit / sell_time. Matching key: (trader_name,
/// event_name, side, size, buy_price, buy_time). If no open row matches, a
/// warning is printed but the close still goes through — the ledger just
/// misses one row.
fn update_history_close(
    path: &Path,
    event: &str,
    trader_name: &str,
    side: &Side,
    size: Decimal,
    buy_price: Decimal,
    buy_ts: i64,
    sell_price: Decimal,
    profit: Decimal,
    sell_ts: i64,
) {
    if !path.exists() {
        eprintln!(
            "plan_c: history: ledger {} missing on close — no row to update",
            path.display()
        );
        return;
    }
    let raw = match std::fs::read_to_string(path) {
        Ok(r) => r,
        Err(e) => {
            eprintln!(
                "plan_c: history: read failed for {}: {e}",
                path.display()
            );
            return;
        }
    };
    let mut lines: Vec<String> = raw.lines().map(|s| s.to_string()).collect();
    if lines.is_empty() {
        return;
    }

    let side_str = side_to_persisted(side);
    let buy_time_str = format_ts_human(buy_ts);
    let buy_price_str = buy_price.to_string();
    let size_str = size.to_string();
    let expected_event = event.to_string();
    let expected_trader = trader_name.to_string();

    let mut matched: Option<usize> = None;
    // Walk backwards to target the most recent open row (handles repeat buys).
    for (i, line) in lines.iter().enumerate().rev() {
        if i == 0 {
            break; // header
        }
        let cols = parse_csv_row(line);
        if cols.len() < HISTORY_HEADERS.len() {
            continue;
        }
        if !cols[9].is_empty() {
            continue; // already closed
        }
        if cols[0] == expected_event
            && cols[1] == expected_trader
            && cols[2] == side_str
            && cols[3] == size_str
            && cols[4] == buy_price_str
            && cols[6] == buy_time_str
        {
            matched = Some(i);
            break;
        }
    }

    let Some(idx) = matched else {
        eprintln!(
            "plan_c: history: no open row to close for trader={} event=\"{}\" side={} size={} price={} buy_time={}",
            trader_name, event, side_str, size, buy_price, buy_time_str
        );
        return;
    };

    let mut cols = parse_csv_row(&lines[idx]);
    while cols.len() < HISTORY_HEADERS.len() {
        cols.push(String::new());
    }
    cols[7] = sell_price.to_string();
    cols[8] = profit.to_string();
    cols[9] = csv_escape(&format_ts_human(sell_ts));
    // Rebuild the row, re-escaping text columns (0,1,2,6,9) whose values may
    // legitimately contain commas. Numerics never need escaping.
    let mut rebuilt: Vec<String> = Vec::with_capacity(cols.len());
    for (col_idx, v) in cols.into_iter().enumerate() {
        if matches!(col_idx, 0 | 1 | 2 | 6 | 9) {
            rebuilt.push(csv_escape(&v));
        } else {
            rebuilt.push(v);
        }
    }
    lines[idx] = rebuilt.join(",");

    let tmp = path.with_extension("csv.tmp");
    let mut out = lines.join("\n");
    out.push('\n');
    if let Err(e) = std::fs::write(&tmp, out.as_bytes()) {
        eprintln!(
            "plan_c: history: write tmp failed {}: {e}",
            tmp.display()
        );
        return;
    }
    if let Err(e) = std::fs::rename(&tmp, path) {
        // Windows rename onto existing sometimes fails; fall back to direct write.
        if let Err(e2) = std::fs::write(path, lines.join("\n") + "\n") {
            eprintln!(
                "plan_c: history: persist failed ({}): rename err={e}, write err={e2}",
                path.display()
            );
        }
    }
}

/// Minimal CSV row parser — handles `"..."` quoted fields with `""` escapes.
/// Adequate for our simple ledger format (no embedded newlines in rows).
fn parse_csv_row(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if in_quotes {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    cur.push('"');
                } else {
                    in_quotes = false;
                }
            } else {
                cur.push(c);
            }
        } else if c == ',' {
            out.push(std::mem::take(&mut cur));
        } else if c == '"' && cur.is_empty() {
            in_quotes = true;
        } else {
            cur.push(c);
        }
    }
    out.push(cur);
    out
}

fn load_plan_c_state(path: &Path) -> PlanCState {
    let Ok(raw) = std::fs::read_to_string(path) else {
        return PlanCState::default();
    };
    match serde_json::from_str::<PlanCState>(&raw) {
        Ok(state) => state,
        Err(e) => {
            eprintln!(
                "plan_c: failed to parse state file {}: {e} — starting with empty state",
                path.display()
            );
            PlanCState::default()
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
#[allow(dead_code)]
struct OnchainPosition {
    #[serde(default)]
    asset: Option<String>,
    #[serde(default, alias = "conditionId")]
    condition_id: Option<String>,
    #[serde(default)]
    size: Option<f64>,
    #[serde(default, alias = "avgPrice")]
    avg_price: Option<f64>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    outcome: Option<String>,
    #[serde(default)]
    redeemable: Option<bool>,
    #[serde(default, alias = "curPrice")]
    cur_price: Option<f64>,
    /// Realized PnL when present (Polymarket data-api).
    #[serde(default, alias = "cashPnl")]
    cash_pnl: Option<f64>,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[allow(dead_code)]
struct GammaMarket {
    #[serde(default, alias = "conditionId")]
    condition_id: Option<String>,
    #[serde(default, alias = "negRisk")]
    neg_risk: Option<bool>,
    #[serde(default)]
    closed: Option<bool>,
}

/// Reconcile every outstanding pending buy order with the exchange.
///
/// Returns `true` if state changed and should be persisted.
///
/// For each pending order:
/// * poll `get_order_status` to learn the current fill
/// * if fully filled (or any fill on Filled status) → promote to Active
/// * if partially filled on the book → keep waiting unless aged out;
///   when aged out we cancel and promote the partial
/// * if rejected/cancelled → drop outright
/// * if still pending and under timeout → keep waiting, bump last_checked_at
async fn sweep_pending_buys<C>(
    execution: &C,
    pending_buys: &mut Vec<PendingBuy>,
    open_positions: &mut Vec<CopiedPosition>,
    now_ts: i64,
    pending_timeout_secs: i64,
    max_positions: usize,
    history_path: &Path,
) -> bool
where
    C: ExecutionClient + ?Sized,
{
    if pending_buys.is_empty() {
        return false;
    }
    let mut state_dirty = false;
    let mut to_remove: Vec<usize> = Vec::new();

    for (idx, pb) in pending_buys.iter_mut().enumerate() {
        let (status, matched_cum) = match execution.get_order_status(&pb.order_id).await {
            Ok(snapshot) => snapshot,
            Err(e) => {
                eprintln!(
                    "plan_c: pending status poll failed (order={}): {e} — will retry next round",
                    pb.order_id
                );
                pb.last_checked_at = now_ts;
                continue;
            }
        };
        pb.last_checked_at = now_ts;
        let matched_delta = (matched_cum - pb.matched_cum).max(Decimal::ZERO);
        if matched_delta > Decimal::ZERO {
            pb.matched_cum = matched_cum;
            state_dirty = true;
        }

        let age_secs = now_ts - pb.submitted_at;
        let timed_out = pending_timeout_secs > 0 && age_secs >= pending_timeout_secs;

        match status {
            ExecutionStatus::Filled => {
                let filled = if matched_cum > Decimal::ZERO {
                    matched_cum
                } else {
                    pb.size
                };
                promote_pending_to_active(
                    pb,
                    filled,
                    now_ts,
                    open_positions,
                    max_positions,
                    history_path,
                );
                to_remove.push(idx);
                state_dirty = true;
            }
            ExecutionStatus::PartiallyFilled => {
                if timed_out {
                    // Cancel the remainder; anything that matched gets promoted.
                    match execution.cancel_order(&pb.order_id).await {
                        Ok(()) => {
                            // Re-poll to pick up any last-second fill between our
                            // read and the cancel landing on the book.
                            let final_matched = match execution
                                .get_order_status(&pb.order_id)
                                .await
                            {
                                Ok((_, m)) => m.max(matched_cum),
                                Err(_) => matched_cum,
                            };
                            if final_matched > Decimal::ZERO {
                                promote_pending_to_active(
                                    pb,
                                    final_matched,
                                    now_ts,
                                    open_positions,
                                    max_positions,
                                    history_path,
                                );
                                println!(
                                    "plan_c: pending CANCELLED (partial fill) — order={} market={} matched={} (of {}) after {}s",
                                    pb.order_id, pb.market_id, final_matched, pb.size, age_secs
                                );
                            } else {
                                println!(
                                    "plan_c: pending CANCELLED (no fill) — order={} market={} after {}s",
                                    pb.order_id, pb.market_id, age_secs
                                );
                            }
                        }
                        Err(e) => {
                            eprintln!(
                                "plan_c: cancel failed for order={}: {e} — leaving pending in place",
                                pb.order_id
                            );
                            // Don't mark for removal; retry next round.
                            continue;
                        }
                    }
                    to_remove.push(idx);
                    state_dirty = true;
                } else {
                    // Wait a bit more to see if it fully fills.
                    if matched_delta > Decimal::ZERO {
                        println!(
                            "plan_c: pending PARTIAL — order={} market={} matched={}/{} age={}s",
                            pb.order_id, pb.market_id, matched_cum, pb.size, age_secs
                        );
                    }
                }
            }
            ExecutionStatus::Pending => {
                if timed_out {
                    match execution.cancel_order(&pb.order_id).await {
                        Ok(()) => {
                            let final_matched = match execution
                                .get_order_status(&pb.order_id)
                                .await
                            {
                                Ok((_, m)) => m.max(matched_cum),
                                Err(_) => matched_cum,
                            };
                            if final_matched > Decimal::ZERO {
                                promote_pending_to_active(
                                    pb,
                                    final_matched,
                                    now_ts,
                                    open_positions,
                                    max_positions,
                                    history_path,
                                );
                                println!(
                                    "plan_c: pending TIMED OUT (last-moment fill) — order={} market={} matched={} after {}s",
                                    pb.order_id, pb.market_id, final_matched, age_secs
                                );
                            } else {
                                println!(
                                    "plan_c: pending TIMED OUT (no fill) — order={} market={} after {}s",
                                    pb.order_id, pb.market_id, age_secs
                                );
                            }
                        }
                        Err(e) => {
                            eprintln!(
                                "plan_c: cancel failed for order={}: {e} — leaving pending in place",
                                pb.order_id
                            );
                            continue;
                        }
                    }
                    to_remove.push(idx);
                    state_dirty = true;
                }
            }
            ExecutionStatus::Rejected => {
                if matched_cum > Decimal::ZERO {
                    // Rare: cancelled after partial fill. Promote what we got.
                    promote_pending_to_active(
                        pb,
                        matched_cum,
                        now_ts,
                        open_positions,
                        max_positions,
                        history_path,
                    );
                    println!(
                        "plan_c: pending REJECTED with partial fill — order={} market={} matched={}",
                        pb.order_id, pb.market_id, matched_cum
                    );
                } else {
                    println!(
                        "plan_c: pending REJECTED/CANCELLED — order={} market={}",
                        pb.order_id, pb.market_id
                    );
                }
                to_remove.push(idx);
                state_dirty = true;
            }
        }
    }

    // Remove promoted/rejected entries from the tail so indices stay valid.
    to_remove.sort_unstable_by(|a, b| b.cmp(a));
    for idx in to_remove {
        pending_buys.remove(idx);
    }

    state_dirty
}

/// Promote a pending buy (with a known filled size) into an Active position
/// on `open_positions`. Mirrors the accounting rounding used elsewhere in the
/// buy path so local SL/TP stay consistent after promotion.
fn promote_pending_to_active(
    pb: &PendingBuy,
    filled_size: Decimal,
    now_ts: i64,
    open_positions: &mut Vec<CopiedPosition>,
    max_positions: usize,
    history_path: &Path,
) {
    // Same 97% safety trim we apply on direct fills.
    let actual_size = (filled_size * Decimal::new(97, 2)).round_dp(2);
    let actual_size = if actual_size <= Decimal::ZERO {
        filled_size
    } else {
        actual_size
    };
    open_positions.push(CopiedPosition {
        trader: pb.trader.clone(),
        market_id: pb.market_id.clone(),
        token_id: pb.token_id.clone(),
        side: pb.side.clone(),
        entry_price: pb.price,
        size: actual_size,
        opened_at: now_ts,
        leader_buy_ts: pb.leader_buy_ts,
        status: PositionStatus::Active,
        resolved_price: None,
        clob_fail_count: 0,
        redeem_retry_count: 0,
        last_redeem_attempt: 0,
        neg_risk: None,
        market_title: pb.market_title.clone(),
        trader_name: pb.trader_name.clone(),
    });
    append_history_open(
        history_path,
        &pb.market_title,
        &pb.trader_name,
        &pb.side,
        actual_size,
        pb.price,
        now_ts,
    );
    let active_after = open_positions
        .iter()
        .filter(|p| matches!(p.status, PositionStatus::Active))
        .count();
    println!(
        "plan_c: pending PROMOTED — trader={} market={} price={} size={} (filled={}) — {}/{} active",
        pb.trader, pb.market_id, pb.price, actual_size, filled_size, active_after, max_positions
    );
}

/// Fetch the `negRisk` flag for a condition (market) via Polymarket's gamma
/// API. Returns `None` if the market is not found or the field is missing.
/// Defaults caller-side to `false` are safer — standard CTF.redeemPositions
/// is always valid to call, it just produces no collateral if the user holds
/// no standard positions.
async fn fetch_market_neg_risk(
    http: &reqwest::Client,
    condition_id: &str,
) -> anyhow::Result<Option<bool>> {
    let url = format!(
        "https://gamma-api.polymarket.com/markets?condition_ids={}",
        condition_id
    );
    let resp = http
        .get(&url)
        .send()
        .await
        .with_context(|| format!("gamma fetch failed for {condition_id}"))?;
    if !resp.status().is_success() {
        return Err(anyhow!(
            "gamma fetch failed for {}: status={}",
            condition_id,
            resp.status()
        ));
    }
    let markets: Vec<GammaMarket> = resp.json().await.context("gamma parse failed")?;
    Ok(markets.into_iter().next().and_then(|m| m.neg_risk))
}

async fn build_redeem_client(
    dotenv_path: &Path,
    rpc_url: &str,
    ctf_addr_raw: &str,
    neg_risk_addr_raw: &str,
    usdc_addr_raw: &str,
    proxy_wallet: Option<&str>,
) -> anyhow::Result<crate::plan_c_redeem::RedeemClient> {
    use alloy::primitives::Address;

    let private_key = resolve_runtime_var("POLYMARKET_PRIVATE_KEY", dotenv_path)
        .or_else(|| resolve_runtime_var("PRIVATE_KEY", dotenv_path))
        .ok_or_else(|| {
            anyhow!("missing private key: set POLYMARKET_PRIVATE_KEY or PRIVATE_KEY")
        })?;

    let holder = proxy_wallet.ok_or_else(|| {
        anyhow!("PROXY_WALLET / PM_FUNDER is required for redeem (that's where the CTF tokens live)")
    })?;
    let position_holder: Address = holder
        .trim()
        .parse()
        .with_context(|| format!("invalid PROXY_WALLET address '{holder}'"))?;

    let ctf_address: Address = ctf_addr_raw
        .trim()
        .parse()
        .with_context(|| format!("invalid PLAN_C_CTF_ADDRESS '{ctf_addr_raw}'"))?;
    let neg_risk_adapter: Address = neg_risk_addr_raw
        .trim()
        .parse()
        .with_context(|| format!("invalid PLAN_C_NEG_RISK_ADAPTER '{neg_risk_addr_raw}'"))?;
    let usdc_address: Address = usdc_addr_raw
        .trim()
        .parse()
        .with_context(|| format!("invalid PLAN_C_USDC_ADDRESS '{usdc_addr_raw}'"))?;

    crate::plan_c_redeem::RedeemClient::new(
        rpc_url,
        &private_key,
        position_holder,
        ctf_address,
        neg_risk_adapter,
        usdc_address,
    )
    .await
}

async fn fetch_onchain_positions_once(
    http: &reqwest::Client,
    wallet: &str,
) -> anyhow::Result<Vec<OnchainPosition>> {
    let url = format!(
        "https://data-api.polymarket.com/positions?user={}&sizeThreshold=0.01",
        wallet
    );
    let resp = http
        .get(&url)
        .send()
        .await
        .with_context(|| format!("positions request failed for {}", wallet))?;
    if !resp.status().is_success() {
        return Err(anyhow!(
            "positions request failed for {}: status={}",
            wallet,
            resp.status()
        ));
    }
    let text = resp
        .text()
        .await
        .context("positions body read failed")?;
    let positions: Vec<OnchainPosition> = serde_json::from_str(&text).with_context(|| {
        format!(
            "positions parse failed, body={}",
            text.chars().take(200).collect::<String>()
        )
    })?;
    Ok(positions)
}

/// Resilient wrapper around `fetch_onchain_positions_once`.
///
/// The Polymarket data API occasionally returns a transient `5xx` or slow responses;
/// when that happens we'd rather retry than skip the reconciliation entirely (the
/// caller would silently proceed with stale cached state). Retries use short
/// exponential backoff so startup isn't noticeably delayed in the common case.
async fn fetch_onchain_positions(
    http: &reqwest::Client,
    wallet: &str,
) -> anyhow::Result<Vec<OnchainPosition>> {
    const MAX_ATTEMPTS: u32 = 3;
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 1..=MAX_ATTEMPTS {
        match fetch_onchain_positions_once(http, wallet).await {
            Ok(v) => return Ok(v),
            Err(e) => {
                if attempt < MAX_ATTEMPTS {
                    let backoff_ms = 500u64 * 2u64.pow(attempt - 1);
                    eprintln!(
                        "plan_c: fetch_onchain_positions attempt {}/{} failed: {e} — retrying in {}ms",
                        attempt, MAX_ATTEMPTS, backoff_ms
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                }
                last_err = Some(e);
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow!("fetch_onchain_positions: exhausted retries")))
}

fn f64_to_decimal(v: f64) -> Option<Decimal> {
    // Use a string formatted to a fixed precision to avoid scientific notation
    // and to tolerate floating-point noise. `normalize()` strips insignificant
    // trailing zeros so the resulting Decimal stringifies back as "11.11" rather
    // than "11.11000000" — the latter fails Polymarket's "Maximum lot size is 2
    // decimals" validation when adopted positions are later sold.
    if !v.is_finite() {
        return None;
    }
    Decimal::from_str(&format!("{:.8}", v)).ok().map(|d| d.normalize())
}

/// Reconcile locally cached positions with truth from the Polymarket data API.
/// - Cached AND on-chain → keep cache (metadata is better) but clamp size downward
///   to the on-chain value (so we don't try to sell more than we actually own).
/// - Cached but not on-chain → drop (was sold/redeemed outside this bot).
/// - On-chain but not cached → adopt as "orphan" position (no trader metadata;
///   only local SL/TP can manage it).
fn reconcile_positions(
    cached: Vec<CopiedPosition>,
    onchain: &[OnchainPosition],
    history_path: &Path,
) -> Vec<CopiedPosition> {
    let mut onchain_map: HashMap<String, &OnchainPosition> = HashMap::new();
    for p in onchain {
        let Some(asset) = p.asset.as_deref().map(str::trim) else {
            continue;
        };
        if asset.is_empty() {
            continue;
        }
        if p.size.unwrap_or(0.0) <= 0.0 {
            continue;
        }
        onchain_map.insert(asset.to_string(), p);
    }

    let mut reconciled: Vec<CopiedPosition> = Vec::new();
    for mut pos in cached {
        match onchain_map.remove(&pos.token_id) {
            Some(on) => {
                let onchain_size = on.size.unwrap_or(0.0);
                if let Some(onchain_dec) = f64_to_decimal(onchain_size) {
                    if onchain_dec > Decimal::ZERO && onchain_dec < pos.size {
                        println!(
                            "plan_c: reconcile — shrinking cached size for market={} token={} from {} to {} (on-chain)",
                            pos.market_id,
                            short_token(&pos.token_id),
                            pos.size,
                            onchain_dec
                        );
                        pos.size = onchain_dec;
                    }
                }
                // Market has settled on-chain — mark for redeem instead of trying
                // to sell on a closed orderbook.
                if on.redeemable.unwrap_or(false)
                    && matches!(pos.status, PositionStatus::Active)
                {
                    let final_px = on
                        .cur_price
                        .and_then(f64_to_decimal)
                        .map(|d| d.round_dp(4));
                    println!(
                        "plan_c: reconcile — market RESOLVED, marking for redeem: market={} token={} final_px={:?}",
                        pos.market_id,
                        short_token(&pos.token_id),
                        final_px
                    );
                    pos.status = PositionStatus::Resolved;
                    pos.resolved_price = final_px;
                }
                reconciled.push(pos);
            }
            None => {
                println!(
                    "plan_c: reconcile — dropping cached position (no longer on-chain): market={} token={} size={} status={:?}",
                    pos.market_id,
                    short_token(&pos.token_id),
                    pos.size,
                    pos.status,
                );
            }
        }
    }

    for (asset, p) in onchain_map {
        if p.redeemable.unwrap_or(false) {
            // Already resolved — bot shouldn't try to sell it.
            continue;
        }
        let cid = p
            .condition_id
            .as_deref()
            .map(str::trim)
            .unwrap_or("")
            .to_string();
        if cid.is_empty() {
            continue;
        }
        let size_dec = match f64_to_decimal(p.size.unwrap_or(0.0)) {
            Some(v) if v > Decimal::ZERO => v,
            _ => continue,
        };
        let entry_dec = f64_to_decimal(p.avg_price.unwrap_or(0.5))
            .unwrap_or_else(|| Decimal::new(50, 2))
            .round_dp(2);
        let title = p.title.as_deref().unwrap_or(&cid);
        let outcome = p.outcome.as_deref().unwrap_or("?");
        println!(
            "plan_c: reconcile — adopting on-chain position as orphan: \"{}\" outcome={} token={} size={} avg={} (local SL/TP only)",
            title,
            outcome,
            short_token(&asset),
            size_dec,
            entry_dec
        );
        let adopt_ts = Utc::now().timestamp();
        let title_owned = title.to_string();
        append_history_open(
            history_path,
            &title_owned,
            "orphan",
            &Side::Yes,
            size_dec,
            entry_dec,
            adopt_ts,
        );
        reconciled.push(CopiedPosition {
            trader: String::new(),
            market_id: cid,
            token_id: asset,
            side: Side::Yes,
            entry_price: entry_dec,
            size: size_dec,
            opened_at: adopt_ts,
            leader_buy_ts: 0,
            status: PositionStatus::Active,
            resolved_price: None,
            clob_fail_count: 0,
            redeem_retry_count: 0,
            last_redeem_attempt: 0,
            neg_risk: None,
            market_title: title_owned,
            trader_name: "orphan".to_string(),
        });
    }

    reconciled
}

fn short_token(token_id: &str) -> String {
    let end = 12.min(token_id.len());
    format!("{}…", &token_id[..end])
}

/// Jaccard-like overlap of two top-N address lists (both treated as sets, denom = 20).
fn leaderboard_top_overlap(prev: &[String], new: &[String]) -> f64 {
    if prev.is_empty() && new.is_empty() {
        return 1.0;
    }
    let a: std::collections::HashSet<&str> = prev.iter().map(|s| s.as_str()).collect();
    let b: std::collections::HashSet<&str> = new.iter().map(|s| s.as_str()).collect();
    let inter = a.intersection(&b).count();
    inter as f64 / 20.0
}

fn normalize_leaderboard_period(raw: &str) -> &'static str {
    match raw.trim().to_ascii_uppercase().as_str() {
        "DAY" | "DAILY" | "D" => "DAY",
        "WEEK" | "WEEKLY" | "W" => "WEEK",
        "MONTH" | "MONTHLY" | "M" => "MONTH",
        "ALL" | "A" => "ALL",
        _ => "WEEK",
    }
}

async fn fetch_leaderboard(
    http: &reqwest::Client,
    limit: u64,
    period: &str,
    order_by: &str,
) -> anyhow::Result<Vec<LeaderboardEntry>> {
    let api_period = normalize_leaderboard_period(period);
    let clamped_limit = limit.min(50);
    let mut all_entries = Vec::new();
    let mut offset: u64 = 0;

    while all_entries.len() < limit as usize {
        let remaining = limit.saturating_sub(all_entries.len() as u64);
        let batch = clamped_limit.min(remaining).max(1);
        if remaining == 0 {
            break;
        }
        let url = format!(
            "https://data-api.polymarket.com/v1/leaderboard?limit={}&offset={}&timePeriod={}&orderBy={}",
            batch, offset, api_period, order_by
        );
        let resp = http
            .get(&url)
            .send()
            .await
            .context("leaderboard request failed")?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "leaderboard request failed: status={} body={}",
                status,
                &body[..body.len().min(300)]
            ));
        }
        let entries: Vec<LeaderboardEntry> =
            resp.json().await.context("leaderboard parse failed")?;
        let count = entries.len();
        all_entries.extend(entries);
        if count < batch as usize {
            break;
        }
        offset += count as u64;
    }

    Ok(all_entries)
}

/// Build pool: (WEEK PNL ∩ MONTH PNL) ∪ WEEK VOL, dedup, cap `max_candidates`.
///
/// Note on `orderBy`: Polymarket data-api only accepts `PNL` or `VOL`
/// (case-insensitive) — anything else (incl. the literal `VOLUME`) returns
/// HTTP 400, which as of 2026-04 would silently collapse the entire pool
/// through the caller's `unwrap_or_default()`.
async fn fetch_leaderboard_candidate_pool(
    http: &reqwest::Client,
    per_list_limit: u64,
    max_candidates: usize,
) -> anyhow::Result<Vec<LeaderboardEntry>> {
    let wp = fetch_leaderboard(http, per_list_limit, "WEEK", "PNL").await?;
    let mp = fetch_leaderboard(http, per_list_limit, "MONTH", "PNL").await?;
    let wv = fetch_leaderboard(http, per_list_limit, "WEEK", "VOL").await?;

    let mut month_addrs: std::collections::HashSet<String> = std::collections::HashSet::new();
    for e in &mp {
        if let Some(a) = &e.proxy_wallet {
            if !a.trim().is_empty() {
                month_addrs.insert(a.trim().to_lowercase());
            }
        }
    }

    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut pool: Vec<LeaderboardEntry> = Vec::new();

    for e in wp {
        if let Some(a) = &e.proxy_wallet {
            let al = a.trim().to_lowercase();
            if month_addrs.contains(&al) && seen.insert(al.clone()) {
                pool.push(e);
            }
        }
    }
    for e in wv {
        if let Some(a) = &e.proxy_wallet {
            let al = a.trim().to_lowercase();
            if seen.insert(al) {
                pool.push(e);
            }
        }
    }

    pool.truncate(max_candidates);
    Ok(pool)
}

async fn fetch_user_trades(
    http: &reqwest::Client,
    user_address: &str,
    limit: u64,
) -> anyhow::Result<Vec<TradeRecord>> {
    let url = format!(
        "https://data-api.polymarket.com/trades?user={}&limit={}",
        user_address, limit
    );
    let resp = http
        .get(&url)
        .send()
        .await
        .with_context(|| format!("trades request failed for {}", user_address))?;
    if !resp.status().is_success() {
        return Err(anyhow!(
            "trades request failed for {}: status={}",
            user_address,
            resp.status()
        ));
    }
    let trades: Vec<TradeRecord> = resp.json().await.context("trades parse failed")?;
    Ok(trades)
}

fn stddev(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let var = values
        .iter()
        .map(|v| {
            let d = v - mean;
            d * d
        })
        .sum::<f64>()
        / (values.len() as f64);
    var.sqrt()
}

fn wilson_lower_bound(wins: usize, n: usize, z: f64) -> f64 {
    if n == 0 {
        return 0.0;
    }
    let phat = wins as f64 / n as f64;
    let z2 = z * z;
    let nf = n as f64;
    let denom = 1.0 + z2 / nf;
    let center = phat + z2 / (2.0 * nf);
    let inner = phat * (1.0 - phat) / nf + z2 / (4.0 * nf * nf);
    let margin = z * inner.sqrt();
    ((center - margin) / denom).clamp(0.0, 1.0)
}

fn profit_factor_shrinkage(gp: f64, gl: f64, alpha: f64, beta: f64) -> f64 {
    (gp + alpha) / (gl + beta).max(1e-9)
}

fn max_drawdown_from_equity_events(events: &[(i64, f64)]) -> f64 {
    if events.is_empty() {
        return 0.0;
    }
    let mut sorted = events.to_vec();
    sorted.sort_by_key(|(ts, _)| *ts);
    let mut cum = 0.0f64;
    let mut peak = 0.0f64;
    let mut max_dd = 0.0f64;
    for (_, pnl) in &sorted {
        cum += pnl;
        if cum > peak {
            peak = cum;
        }
        let dd = peak - cum;
        if dd > max_dd {
            max_dd = dd;
        }
    }
    max_dd
}

/// Aggregate FIFO-closed PnL per `(condition_id, outcome token)`.
/// `count_cutoff`: only closes with `timestamp >= cutoff` contribute to win/factor stats;
/// equity curve for DD always uses *all* closes (pass full agg with `count_cutoff == None` for DD).
#[derive(Debug, Default, Clone)]
struct FifoPnlAgg {
    wins: usize,
    total: usize,
    gross_profit: f64,
    gross_loss: f64,
    max_drawdown: f64,
    net_realized: f64,
    pnl_by_condition: HashMap<String, f64>,
    equity_events: Vec<(i64, f64)>,
}

/// FIFO P&L aggregate, **collapsing neg-risk YES/NO pairs** within each
/// `condition_id` to a single "YES"-unit ledger.
///
/// Why: in Polymarket neg-risk markets some leaders routinely hedge by holding
/// YES and NO of the same event at the same time; the old per-asset grouping
/// counted each side as a separate ledger, which double-counts wins/losses and
/// distorts PF/WR. The canonical YES is the asset with the most trades inside
/// the condition; non-canonical trades are translated:
/// * BUY  NO @ p  ≡ SELL YES @ (1-p)
/// * SELL NO @ p  ≡ BUY  YES @ (1-p)
///
/// After collapsing, a perfectly hedged leader produces `total=0` closes — their
/// WR/PF/Score correctly degrade to "no useful signal".
fn fifo_pnl_aggregate(trades: &[TradeRecord], count_cutoff: Option<i64>) -> FifoPnlAgg {
    #[derive(Default, Clone)]
    struct Lot {
        size: f64,
        price: f64,
    }

    let cutoff_ts = count_cutoff.unwrap_or(i64::MIN);

    // Step 1: group by condition_id only.
    let mut by_cond: HashMap<String, Vec<&TradeRecord>> = HashMap::new();
    for t in trades {
        let cid = match &t.condition_id {
            Some(v) if !v.trim().is_empty() => v.trim().to_string(),
            _ => continue,
        };
        if t.asset.as_deref().map(|s| s.trim().is_empty()).unwrap_or(true) {
            continue;
        }
        by_cond.entry(cid).or_default().push(t);
    }

    let mut wins = 0usize;
    let mut total = 0usize;
    let mut gross_profit = 0.0f64;
    let mut gross_loss = 0.0f64;
    let mut pnl_by_condition: HashMap<String, f64> = HashMap::new();
    let mut equity_events: Vec<(i64, f64)> = Vec::new();

    for (cid, mut items) in by_cond {
        items.sort_by_key(|t| t.timestamp.unwrap_or(0));

        // Step 2: pick canonical YES asset = most-traded asset in this condition.
        // Deterministic tiebreak: lexicographic smaller asset id.
        let mut asset_counts: HashMap<String, usize> = HashMap::new();
        for t in &items {
            if let Some(a) = t.asset.as_deref() {
                let key = a.trim().to_string();
                if !key.is_empty() {
                    *asset_counts.entry(key).or_insert(0) += 1;
                }
            }
        }
        // Canonical YES = most-traded asset; tiebreak by lexicographically smaller id.
        let yes_asset: String = asset_counts
            .into_iter()
            .max_by(|a, b| a.1.cmp(&b.1).then_with(|| b.0.cmp(&a.0)))
            .map(|(k, _)| k)
            .unwrap_or_default();

        let mut inventory: Vec<Lot> = Vec::new();
        for t in items {
            let side = t.side.as_deref().unwrap_or("");
            let price = t.price.unwrap_or(0.0);
            let size = t.size.unwrap_or(0.0).abs();
            if size <= 0.0 {
                continue;
            }
            let asset = t.asset.as_deref().unwrap_or("").trim();
            let is_yes = !yes_asset.is_empty() && asset == yes_asset;

            // Translate BUY/SELL NO to the canonical YES side.
            let (canon_buy, canon_price) = match (is_yes, side.to_ascii_uppercase().as_str()) {
                (true, "BUY") => (true, price),
                (true, "SELL") => (false, price),
                (false, "BUY") => (false, 1.0 - price),
                (false, "SELL") => (true, 1.0 - price),
                _ => continue,
            };

            if canon_buy {
                inventory.push(Lot {
                    size,
                    price: canon_price,
                });
            } else {
                let mut remaining = size;
                let mut pnl = 0.0f64;
                let mut matched = false;
                while remaining > 0.0 && !inventory.is_empty() {
                    let lot = inventory.first_mut().expect("inventory not empty");
                    let take = remaining.min(lot.size);
                    pnl += (canon_price - lot.price) * take;
                    lot.size -= take;
                    remaining -= take;
                    matched = true;
                    if lot.size <= 1e-9 {
                        inventory.remove(0);
                    }
                }
                if !matched {
                    continue;
                }
                let ts = t.timestamp.unwrap_or(0);
                equity_events.push((ts, pnl));
                if ts < cutoff_ts {
                    continue;
                }
                total += 1;
                // Avoid classifying sub-cent float noise (e.g. from a perfectly
                // hedged YES/NO leg) as a "win" — 1e-6 USD is below settlement
                // precision on any real trade.
                if pnl > 1e-6 {
                    wins += 1;
                    gross_profit += pnl;
                } else if pnl < -1e-6 {
                    gross_loss += -pnl;
                }
                *pnl_by_condition.entry(cid.clone()).or_insert(0.0) += pnl;
            }
        }
    }

    let net_realized = gross_profit - gross_loss;
    let max_drawdown = if count_cutoff.is_none() {
        max_drawdown_from_equity_events(&equity_events)
    } else {
        0.0
    };

    FifoPnlAgg {
        wins,
        total,
        gross_profit,
        gross_loss,
        max_drawdown,
        net_realized,
        pnl_by_condition,
        equity_events,
    }
}

fn buy_notional_since(trades: &[TradeRecord], since_ts: i64, now_ts: i64) -> f64 {
    trades
        .iter()
        .filter_map(|t| {
            if !t.side.as_deref().unwrap_or("").eq_ignore_ascii_case("BUY") {
                return None;
            }
            let ts = t.timestamp?;
            if ts < since_ts || ts > now_ts {
                return None;
            }
            let sz = t.size.unwrap_or(0.0).abs();
            let px = t.price.unwrap_or(0.0);
            Some(sz * px)
        })
        .sum()
}

fn retention_norm(x: f64, lo: f64, hi: f64) -> f64 {
    if (hi - lo).abs() < 1e-12 {
        return 0.0;
    }
    ((x - lo) / (hi - lo)).clamp(0.0, 1.0)
}

fn sharpe_daily_from_events(equity: &[(i64, f64)], window_start: i64, now_ts: i64) -> f64 {
    let mut daily: HashMap<i64, f64> = HashMap::new();
    for (ts, pnl) in equity {
        if *ts < window_start || *ts > now_ts {
            continue;
        }
        let day = *ts / 86_400;
        *daily.entry(day).or_insert(0.0) += pnl;
    }
    let vals: Vec<f64> = daily.into_values().collect();
    if vals.len() < 2 {
        return 0.0;
    }
    let m = vals.iter().sum::<f64>() / vals.len() as f64;
    let sd = stddev(&vals);
    if sd < 1e-9 {
        return 0.0;
    }
    m / sd
}

fn percentile_ratio(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 1.0;
    }
    let idx = (((sorted.len() as f64 - 1.0) * p).round() as usize).min(sorted.len() - 1);
    sorted[idx]
}

/// Wilson LCB, shrunk PF, `ret_norm` from Sharpe~7d, recent ROI norm, drawdown penalty.
#[allow(clippy::too_many_arguments)]
fn compute_score_v2(
    sharpe_7d: f64,
    wilson_wr: f64,
    profit_factor_shrunk: f64,
    recent_roi: f64,
    dd_penalty: f64,
    w_ret: f64,
    w_wr: f64,
    w_pf: f64,
    w_rr: f64,
    w_dd: f64,
) -> f64 {
    let ret_norm = retention_norm(sharpe_7d.clamp(-3.0, 6.0), -3.0, 6.0);
    let pf_norm = profit_factor_shrunk.clamp(0.0, 8.0) / 8.0;
    let rr_norm = retention_norm(recent_roi.clamp(-0.75, 1.5), -0.75, 1.5);
    w_ret * ret_norm
        + w_wr * wilson_wr.clamp(0.0, 1.0)
        + w_pf * pf_norm
        + w_rr * rr_norm
        - w_dd * dd_penalty.clamp(0.0, 1.0)
}

fn max_single_market_concentration(agg: &FifoPnlAgg) -> f64 {
    let den = agg.net_realized.abs().max(1.0);
    agg.pnl_by_condition
        .values()
        .map(|v| v.abs())
        .fold(0.0f64, f64::max)
        / den
}

#[allow(clippy::too_many_arguments)]
fn analyze_trader_full(
    trades: &[TradeRecord],
    analysis_hours: u64,
    min_avg_interval_secs: f64,
    max_trades_in_window: usize,
    min_interval_stddev: f64,
    max_active_hour_ratio: f64,
    recent_window_hours: u64,
    max_notional_cv: f64,
    min_market_diversity: f64,
    max_size_p99_p50: f64,
    pf_alpha: f64,
    pf_beta: f64,
) -> TraderAnalysis {
    let now = Utc::now().timestamp();
    let cutoff = now - (analysis_hours as i64 * 3600);
    let recent_cutoff = now - (recent_window_hours as i64 * 3600);
    let sharpe_window_start = now - (7_i64 * 3600 * 24);

    let full = fifo_pnl_aggregate(trades, None);
    let recent_cut = fifo_pnl_aggregate(trades, Some(recent_cutoff));
    let net_recent = recent_cut.gross_profit - recent_cut.gross_loss;
    let buy_nom = buy_notional_since(trades, recent_cutoff, now).max(1.0);
    let recent_roi = net_recent / buy_nom;

    let wilson_wr_lcb = wilson_lower_bound(full.wins, full.total, 1.96);
    let pf_raw = profit_factor_shrinkage(full.gross_profit, full.gross_loss, pf_alpha, pf_beta);

    let sharpe_7d = sharpe_daily_from_events(&full.equity_events, sharpe_window_start, now);

    let mut recent_ts: Vec<i64> = trades
        .iter()
        .filter_map(|t| t.timestamp)
        .filter(|&ts| ts >= cutoff)
        .collect();
    recent_ts.sort();

    let trade_count = recent_ts.len();

    let empty_behavioral = TraderAnalysis {
        avg_interval_secs: f64::MAX,
        trade_count: 0,
        interval_stddev: 0.0,
        active_hour_ratio: 0.0,
        win_rate_raw: if full.total > 0 {
            full.wins as f64 / full.total as f64
        } else {
            0.0
        },
        wilson_wr_lcb,
        profit_factor: pf_raw,
        recent_roi,
        sharpe_7d,
        ret_7d: recent_roi,
        max_drawdown: full.max_drawdown,
        net_realized_pnl: full.net_realized,
        notional_cv: 1.0,
        market_diversity: 0.0,
        size_p99_p50: 1.0,
        closed_trade_count: full.total,
        is_bot: false,
    };

    if trade_count == 0 {
        return empty_behavioral;
    }

    let (avg_interval_secs, interval_stddev) = if recent_ts.len() < 2 {
        (f64::MAX, 0.0)
    } else {
        let intervals: Vec<f64> = recent_ts
            .windows(2)
            .map(|w| (w[1] - w[0]) as f64)
            .collect();
        let avg = intervals.iter().sum::<f64>() / intervals.len() as f64;
        (avg, stddev(&intervals))
    };

    let mut active_hours: std::collections::HashSet<i64> = std::collections::HashSet::new();
    for ts in &recent_ts {
        active_hours.insert(ts / 3600);
    }
    let denom_hours = (analysis_hours.min(24)).max(1) as f64;
    let active_hour_ratio = (active_hours.len() as f64) / denom_hours;

    // Buy notionals & sizes in analysis window
    let mut notionals: Vec<f64> = Vec::new();
    let mut sizes: Vec<f64> = Vec::new();
    let mut uniq_cid: std::collections::HashSet<String> = std::collections::HashSet::new();
    for t in trades {
        let ts = match t.timestamp {
            Some(x) if x >= cutoff => x,
            _ => continue,
        };
        let _ = ts;
        if let Some(cid) = t.condition_id.as_deref() {
            if !cid.trim().is_empty() {
                uniq_cid.insert(cid.trim().to_string());
            }
        }
        if t.side.as_deref().unwrap_or("").eq_ignore_ascii_case("BUY") {
            let sz = t.size.unwrap_or(0.0).abs();
            let px = t.price.unwrap_or(0.0);
            notionals.push(sz * px);
            sizes.push(sz);
        }
    }
    let mean_n = notionals.iter().sum::<f64>() / notionals.len().max(1) as f64;
    let notional_cv = if mean_n > 1e-9 && notionals.len() > 1 {
        stddev(&notionals) / mean_n
    } else {
        1.0
    };
    let market_diversity = if trade_count > 0 {
        uniq_cid.len() as f64 / trade_count as f64
    } else {
        0.0
    };
    let mut sorted_sz = sizes;
    sorted_sz.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p50 = percentile_ratio(&sorted_sz, 0.5);
    let p99 = percentile_ratio(&sorted_sz, 0.99);
    let size_p99_p50 = if p50 > 1e-9 { p99 / p50 } else { 1.0 };

    let win_rate_raw = if full.total > 0 {
        full.wins as f64 / full.total as f64
    } else {
        0.0
    };

    // Bot filter v2 — avoid rejecting MM on interval alone.
    let is_bot = (trade_count > max_trades_in_window)
        || (avg_interval_secs < 60.0 && notional_cv < max_notional_cv && trade_count > 10)
        || (trade_count > 30 && avg_interval_secs < min_avg_interval_secs)
        || ((interval_stddev < min_interval_stddev) && (trade_count > 20))
        || ((active_hour_ratio > max_active_hour_ratio) && (trade_count > 50))
        || (trade_count > 25 && market_diversity < min_market_diversity)
        || (size_p99_p50 > max_size_p99_p50);

    TraderAnalysis {
        avg_interval_secs,
        trade_count,
        interval_stddev,
        active_hour_ratio,
        win_rate_raw,
        wilson_wr_lcb,
        profit_factor: pf_raw,
        recent_roi,
        sharpe_7d,
        ret_7d: recent_roi,
        max_drawdown: full.max_drawdown,
        net_realized_pnl: full.net_realized,
        notional_cv,
        market_diversity,
        size_p99_p50,
        closed_trade_count: full.total,
        is_bot,
    }
}

async fn fetch_positions_unrealized_share(
    http: &reqwest::Client,
    wallet: &str,
) -> anyhow::Result<Option<f64>> {
    let positions = fetch_onchain_positions_once(http, wallet).await?;
    let mut numer = 0.0f64;
    let mut denom = 0.0f64;
    for p in &positions {
        let s = p.size.unwrap_or(0.0).abs();
        let avg = p.avg_price.unwrap_or(0.0);
        let cur = p.cur_price.unwrap_or(avg);
        let cash = p.cash_pnl.unwrap_or(0.0);
        let m2m = s * (cur - avg);
        numer += m2m.abs();
        denom += cash.abs() + m2m.abs();
    }
    if denom < 1e-3 {
        return Ok(None);
    }
    Ok(Some(numer / denom))
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::cognitive_complexity)]
async fn discover_qualified_traders(
    http: &reqwest::Client,
    use_multi_leaderboard: bool,
    per_list_limit: u64,
    candidate_pool_max: usize,
    leaderboard_limit: u64,
    period: &str,
    min_pnl: f64,
    min_volume: f64,
    min_realized_pnl: f64,
    max_market_concentration: f64,
    max_unrealized_ratio: f64,
    analysis_hours: u64,
    min_avg_interval_secs: f64,
    max_trades_in_window: usize,
    min_interval_stddev: f64,
    max_active_hour_ratio: f64,
    recent_window_hours: u64,
    max_notional_cv: f64,
    min_market_diversity: f64,
    max_size_p99_p50: f64,
    pf_alpha: f64,
    pf_beta: f64,
    w_ret: f64,
    w_wr: f64,
    w_pf: f64,
    w_rr: f64,
    w_dd: f64,
    min_copy_score: f64,
    require_closed_trades: bool,
    min_closed_trades: usize,
    max_concurrency: usize,
) -> (Vec<QualifiedTrader>, Vec<String>) {
    let http_c = http.clone();
    // Rate-limit outbound data-api calls. Too high → 429s and slow tails;
    // too low → long wall-clock during discovery. 8 is a conservative default.
    let semaphore = Arc::new(Semaphore::new(max_concurrency.max(1)));

    let top20_snapshot: Vec<String> =
        match fetch_leaderboard(&http_c, 20, "WEEK", "PNL").await {
            Ok(v) => v
                .into_iter()
                .filter_map(|e| e.proxy_wallet.map(|a| a.trim().to_lowercase()))
                .take(20)
                .collect(),
            Err(_) => Vec::new(),
        };

    let entries: Vec<LeaderboardEntry> = if use_multi_leaderboard {
        // Do NOT silently `unwrap_or_default()` here — a 400/500 on any of the
        // three sub-fetches has historically shown up as "leaderboard returned
        // 0 entries" and cost an operator hours of confusion. Log and move on.
        match fetch_leaderboard_candidate_pool(&http_c, per_list_limit, candidate_pool_max).await {
            Ok(v) => v,
            Err(err) => {
                eprintln!("plan_c: fetch_leaderboard_candidate_pool failed: {err}");
                Vec::new()
            }
        }
    } else {
        match fetch_leaderboard(&http_c, leaderboard_limit, period, "PNL").await {
            Ok(e) => e,
            Err(err) => {
                eprintln!("plan_c: fetch_leaderboard failed: {err}");
                return (Vec::new(), top20_snapshot);
            }
        }
    };

    let mode = if use_multi_leaderboard {
        "multi_pool"
    } else {
        "single_period"
    };
    println!(
        "plan_c: leaderboard returned {} entries (mode={} period={} min_pnl={} min_vol={})",
        entries.len(),
        mode,
        period,
        min_pnl,
        min_volume
    );

    let mut futs = Vec::new();
    for entry in entries {
        let http = http_c.clone();
        let sem = semaphore.clone();
        futs.push(async move {
            let address = match entry.proxy_wallet {
                Some(ref a) if !a.trim().is_empty() => a.trim().to_string(),
                _ => {
                    return None;
                }
            };
            let pnl = entry.pnl.unwrap_or(0.0);
            let volume = entry.vol.unwrap_or(0.0);
            let user_name = entry
                .user_name
                .clone()
                .unwrap_or_else(|| address[..10.min(address.len())].to_string());

            if pnl < min_pnl || volume < min_volume {
                return None;
            }

            // Hold the permit for the remainder of this trader's HTTP work;
            // release automatically on drop when this future resolves. This
            // caps simultaneous trades-API + positions-API calls.
            let _permit = match sem.acquire_owned().await {
                Ok(p) => p,
                Err(_) => return None,
            };

            let trades = match fetch_user_trades(&http, &address, 500).await {
                Ok(t) => t,
                Err(err) => {
                    eprintln!("plan_c: fetch_trades failed for {address}: {err}");
                    return None;
                }
            };

            let full = fifo_pnl_aggregate(&trades, None);
            if full.net_realized < min_realized_pnl {
                println!(
                    "plan_c: FILTERED (low realized FIFO net) {} net={:.0} < {:.0}",
                    user_name, full.net_realized, min_realized_pnl
                );
                return None;
            }
            let conc = max_single_market_concentration(&full);
            if conc > max_market_concentration {
                println!(
                    "plan_c: FILTERED (market concentration) {} max_cid_share={:.2} > {:.2}",
                    user_name, conc, max_market_concentration
                );
                return None;
            }

            if let Ok(Some(share)) = fetch_positions_unrealized_share(&http, &address).await {
                if share > max_unrealized_ratio {
                    println!(
                        "plan_c: FILTERED (unrealized share) {} share={:.2} > {:.2}",
                        user_name, share, max_unrealized_ratio
                    );
                    return None;
                }
            }

            let analysis = analyze_trader_full(
                &trades,
                analysis_hours,
                min_avg_interval_secs,
                max_trades_in_window,
                min_interval_stddev,
                max_active_hour_ratio,
                recent_window_hours,
                max_notional_cv,
                min_market_diversity,
                max_size_p99_p50,
                pf_alpha,
                pf_beta,
            );

            if analysis.is_bot {
                println!(
                    "plan_c: FILTERED (bot-like) {} (pnl={:.0} vol={:.0} trades={} avg_interval={:.0}s div={:.2} ncv={:.2})",
                    user_name,
                    pnl,
                    volume,
                    analysis.trade_count,
                    analysis.avg_interval_secs,
                    analysis.market_diversity,
                    analysis.notional_cv
                );
                return None;
            }

            let dd_penalty =
                (full.max_drawdown / full.net_realized.abs().max(1.0)).clamp(0.0, 1.0);
            let score = compute_score_v2(
                analysis.sharpe_7d,
                analysis.wilson_wr_lcb,
                analysis.profit_factor,
                analysis.recent_roi,
                dd_penalty,
                w_ret,
                w_wr,
                w_pf,
                w_rr,
                w_dd,
            );

            let interval_display = if analysis.avg_interval_secs > 1e30 {
                "n/a".to_string()
            } else {
                format!("{:.0}s", analysis.avg_interval_secs)
            };

            if require_closed_trades && analysis.closed_trade_count < min_closed_trades {
                println!(
                    "plan_c: FILTERED (insufficient stats) {} (score={:.2} closed={} < {})",
                    user_name, score, analysis.closed_trade_count, min_closed_trades
                );
                return None;
            }
            if score < min_copy_score {
                println!(
                    "plan_c: FILTERED (low score) {} score={:.2} < {:.2} wr_lcb={:.2} pf={:.2} rr={:.2}",
                    user_name,
                    score,
                    min_copy_score,
                    analysis.wilson_wr_lcb,
                    analysis.profit_factor,
                    analysis.recent_roi,
                );
                return None;
            }

            println!(
                "plan_c: QUALIFIED {} (score={:.2} sharpe7d={:.2} wr_lcb={:.2} pf={:.2} rr={:.2} dd_pen={:.2} closed={} pnl={:.0} vol={:.0} trades={} avg={})",
                user_name,
                score,
                analysis.sharpe_7d,
                analysis.wilson_wr_lcb,
                analysis.profit_factor,
                analysis.recent_roi,
                dd_penalty,
                analysis.closed_trade_count,
                pnl,
                volume,
                analysis.trade_count,
                interval_display
            );

            let roi = if volume > 0.0 { pnl / volume } else { 0.0 };
            Some(QualifiedTrader {
                address,
                user_name,
                pnl,
                volume,
                avg_interval_secs: analysis.avg_interval_secs,
                trade_count: analysis.trade_count,
                roi,
                sharpe_7d: analysis.sharpe_7d,
                ret_7d: analysis.ret_7d,
                win_rate: analysis.wilson_wr_lcb,
                profit_factor: analysis.profit_factor,
                recent_roi: analysis.recent_roi,
                max_drawdown: full.max_drawdown,
                score,
            })
        });
    }

    let raw: Vec<Option<QualifiedTrader>> = join_all(futs).await;
    let mut candidates: Vec<QualifiedTrader> = raw.into_iter().flatten().collect();
    candidates.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));

    println!(
        "plan_c: discovery complete — qualified={} (parallel pool)",
        candidates.len()
    );

    (candidates, top20_snapshot)
}

pub(crate) async fn run_plan_c_loop<C>(
    context: &StrategyContext,
    risk_engine: &RiskEngine,
    execution: &C,
    dotenv_path: &Path,
) where
    C: ExecutionClient,
{
    let leaderboard_limit = read_u64_env_or(dotenv_path, "PLAN_C_LEADERBOARD_LIMIT", 50);
    let period_raw = resolve_runtime_var("PLAN_C_LEADERBOARD_PERIOD", dotenv_path)
        .unwrap_or_else(|| "weekly".to_string());
    let period = period_raw.trim().to_string();
    let min_pnl = read_f64_env_or(dotenv_path, "PLAN_C_MIN_PNL", 2000.0);
    let min_volume = read_f64_env_or(dotenv_path, "PLAN_C_MIN_VOLUME", 50000.0);
    let min_realized_pnl = read_f64_env_or(dotenv_path, "PLAN_C_MIN_REALIZED_PNL", 500.0);
    let max_market_concentration =
        read_f64_env_or(dotenv_path, "PLAN_C_MAX_MARKET_CONCENTRATION", 0.60);
    let max_unrealized_ratio = read_f64_env_or(dotenv_path, "PLAN_C_MAX_UNREALIZED_RATIO", 0.70);
    let use_multi_leaderboard =
        crate::read_bool_env_or(dotenv_path, "PLAN_C_USE_MULTI_LEADERBOARD", true);
    let candidate_pool_max = read_u64_env_or(dotenv_path, "PLAN_C_CANDIDATE_POOL_MAX", 120) as usize;
    let max_notional_cv = read_f64_env_or(dotenv_path, "PLAN_C_MAX_NOTIONAL_CV", 0.15);
    let min_market_diversity = read_f64_env_or(dotenv_path, "PLAN_C_MIN_MARKET_DIVERSITY", 0.10);
    let max_size_p99_p50 = read_f64_env_or(dotenv_path, "PLAN_C_MAX_SIZE_P99_P50", 20.0);
    let pf_alpha = read_f64_env_or(dotenv_path, "PLAN_C_PF_ALPHA", 50.0);
    let pf_beta = read_f64_env_or(dotenv_path, "PLAN_C_PF_BETA", 50.0);
    let refresh_churn_threshold =
        read_f64_env_or(dotenv_path, "PLAN_C_REFRESH_CHURN_THRESHOLD", 0.30);
    // Keep default conservative to avoid 429s on the data-api; bump to 16 on
    // a warm network. Set to 1 to effectively serialise (regression harness).
    let analyze_concurrency =
        read_u64_env_or(dotenv_path, "PLAN_C_ANALYZE_CONCURRENCY", 8) as usize;
    let analysis_hours = read_u64_env_or(dotenv_path, "PLAN_C_ANALYSIS_HOURS", 24);
    let min_avg_interval_secs =
        read_f64_env_or(dotenv_path, "PLAN_C_MIN_AVG_INTERVAL_SECS", 120.0);
    let max_trades_in_window =
        read_u64_env_or(dotenv_path, "PLAN_C_MAX_TRADES_IN_WINDOW", 200) as usize;
    let min_interval_stddev =
        read_f64_env_or(dotenv_path, "PLAN_C_MIN_INTERVAL_STDDEV", 10.0);
    let max_active_hour_ratio =
        read_f64_env_or(dotenv_path, "PLAN_C_MAX_ACTIVE_HOUR_RATIO", 0.9);
    let recent_window_hours =
        read_u64_env_or(dotenv_path, "PLAN_C_RECENT_WINDOW_HOURS", 168);
    let max_trade_age_secs = read_u64_env_or(dotenv_path, "PLAN_C_MAX_TRADE_AGE_SECS", 180) as i64;
    let w_ret = read_f64_env_or(dotenv_path, "PLAN_C_W_ROI", 0.25);
    let w_wr = read_f64_env_or(dotenv_path, "PLAN_C_W_WIN_RATE", 0.25);
    let w_pf = read_f64_env_or(dotenv_path, "PLAN_C_W_PROFIT_FACTOR", 0.20);
    let w_rr = read_f64_env_or(dotenv_path, "PLAN_C_W_RECENT_ROI", 0.15);
    let w_dd = read_f64_env_or(dotenv_path, "PLAN_C_W_DRAWDOWN", 0.15);
    let order_usd_base = if let Some(s) = resolve_runtime_var("PLAN_C_ORDER_USD_BASE", dotenv_path) {
        Decimal::from_str(s.trim()).unwrap_or_else(|_| Decimal::from(5_u32))
    } else {
        read_decimal_env_or(dotenv_path, "PLAN_C_ORDER_USD", Decimal::from(5_u32))
    };
    let order_usd_min =
        read_decimal_env_or(dotenv_path, "PLAN_C_ORDER_USD_MIN", Decimal::from(3_u32));
    let order_usd_max =
        read_decimal_env_or(dotenv_path, "PLAN_C_ORDER_USD_MAX", Decimal::from(25_u32));
    let min_leader_notional =
        read_f64_env_or(dotenv_path, "PLAN_C_MIN_LEADER_NOTIONAL", 100.0);
    let poll_secs = read_u64_env_or(dotenv_path, "PLAN_C_POLL_SECS", 30);
    let stop_loss_pct = read_decimal_env_or(
        dotenv_path,
        "PLAN_C_STOP_LOSS_PCT",
        Decimal::from_str("0.15").unwrap_or(Decimal::ZERO),
    );
    let take_profit_pct = read_decimal_env_or(
        dotenv_path,
        "PLAN_C_TAKE_PROFIT_PCT",
        Decimal::from_str("0.30").unwrap_or(Decimal::ZERO),
    );
    let max_positions = read_u64_env_or(dotenv_path, "PLAN_C_MAX_POSITIONS", 5) as usize;
    // After this many seconds, a pending (not-yet-filled) buy order is cancelled
    // to free up its slot. Set to 0 to disable auto-cancel (orders stay GTC).
    let pending_timeout_secs =
        read_u64_env_or(dotenv_path, "PLAN_C_PENDING_TIMEOUT_SECS", 60) as i64;
    // Do not copy-buy at prices >= this cap. At very high prices the upside is
    // too thin to cover fees + slippage even on a winning settlement.
    // Default 0.90 — set to 1.0 to disable the cap.
    let max_buy_price = read_decimal_env_or(
        dotenv_path,
        "PLAN_C_MAX_BUY_PRICE",
        Decimal::new(90, 2),
    );
    // Minimum fraction of account equity (cash + allocated capital) that must
    // stay in USDC after any new buy. Prevents the bot from spending the
    // wallet down to zero and leaves headroom for fees / redeem gas.
    // Default 0.20 — set to 0 to disable the reserve check.
    let min_cash_reserve_pct = read_decimal_env_or(
        dotenv_path,
        "PLAN_C_MIN_CASH_RESERVE_PCT",
        Decimal::new(20, 2),
    );
    // After this many consecutive failed `fetch_clob_best_bid` calls, treat the
    // market as closed/resolved so the position stops occupying a slot.
    let clob_fail_threshold =
        read_u64_env_or(dotenv_path, "PLAN_C_CLOB_FAIL_THRESHOLD", 5) as u32;
    // On-chain redeem configuration (Phase 2).
    let auto_redeem = resolve_runtime_var("PLAN_C_AUTO_REDEEM", dotenv_path)
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false);
    let rpc_url = resolve_runtime_var("PLAN_C_RPC_URL", dotenv_path)
        .unwrap_or_else(|| crate::plan_c_redeem::DEFAULT_RPC_URL.to_string());
    // Optional comma-separated list of fallback RPC URLs — the main `rpc_url` is tried
    // first, then each fallback in order. Use this to ride out transient 5xx / throttling
    // on any single provider. Example:
    //   PLAN_C_RPC_URLS = 'https://polygon-rpc.com, https://poly.api.pocket.network, https://polygon.llamarpc.com'
    let extra_rpc_urls: Vec<String> =
        resolve_runtime_var("PLAN_C_RPC_URLS", dotenv_path)
            .map(|raw| {
                raw.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default();
    // Deduplicate: primary + fallbacks, preserving order.
    let mut rpc_urls_all: Vec<String> = Vec::new();
    rpc_urls_all.push(rpc_url.clone());
    for u in &extra_rpc_urls {
        if !rpc_urls_all.iter().any(|existing| existing == u) {
            rpc_urls_all.push(u.clone());
        }
    }
    // When the on-chain USDC balance fetch fails *and* the cash-reserve guard is enabled,
    // should we skip copy buys this round (safe) or proceed without the check (loose)?
    // Default: true (skip) — lining up with what users intuitively expect from a
    // "minimum reserve" guard.
    let reserve_block_on_fail =
        crate::read_bool_env_or(dotenv_path, "PLAN_C_RESERVE_BLOCK_ON_FAIL", true);
    let ctf_addr_raw = resolve_runtime_var("PLAN_C_CTF_ADDRESS", dotenv_path)
        .unwrap_or_else(|| crate::plan_c_redeem::DEFAULT_CTF_ADDRESS.to_string());
    let neg_risk_addr_raw = resolve_runtime_var("PLAN_C_NEG_RISK_ADAPTER", dotenv_path)
        .unwrap_or_else(|| crate::plan_c_redeem::DEFAULT_NEG_RISK_ADAPTER.to_string());
    let usdc_addr_raw = resolve_runtime_var("PLAN_C_USDC_ADDRESS", dotenv_path)
        .unwrap_or_else(|| crate::plan_c_redeem::DEFAULT_USDC_ADDRESS.to_string());
    let redeem_max_retries =
        read_u64_env_or(dotenv_path, "PLAN_C_REDEEM_MAX_RETRIES", 5) as u32;
    let redeem_backoff_secs =
        read_u64_env_or(dotenv_path, "PLAN_C_REDEEM_BACKOFF_SECS", 300) as i64;
    let buy_slippage = read_decimal_env_or(
        dotenv_path,
        "PLAN_C_BUY_SLIPPAGE",
        Decimal::from_str("0.02").unwrap_or(Decimal::ZERO),
    );
    let max_per_trader = read_u64_env_or(dotenv_path, "PLAN_C_MAX_PER_TRADER", 2) as usize;
    let refresh_leaders_secs = read_u64_env_or(dotenv_path, "PLAN_C_REFRESH_LEADERS_SECS", 1200);
    let exit_mode = parse_exit_mode(resolve_runtime_var("PLAN_C_EXIT_MODE", dotenv_path));
    let leader_sell_max_age_secs =
        read_u64_env_or(dotenv_path, "PLAN_C_LEADER_SELL_MAX_AGE_SECS", 300) as i64;

    // Quality gates layered on top of the bot-filter: (方案 3)
    //   * PLAN_C_MIN_COPY_SCORE     — hard floor on composite score (default 0.35)
    //   * PLAN_C_REQUIRE_CLOSED_TRADES — require some FIFO-closed history (default true)
    //   * PLAN_C_MIN_CLOSED_TRADES  — how many closed pairs are "some" (default 3)
    // Previously score was only used for priority sorting, so any non-bot
    // account with a stale fresh BUY could grab a slot regardless of how
    // meaningless its wr/pf/recent_roi were.
    let min_copy_score = read_f64_env_or(dotenv_path, "PLAN_C_MIN_COPY_SCORE", 0.45);
    let require_closed_trades =
        crate::read_bool_env_or(dotenv_path, "PLAN_C_REQUIRE_CLOSED_TRADES", true);
    let min_closed_trades =
        read_u64_env_or(dotenv_path, "PLAN_C_MIN_CLOSED_TRADES", 10) as usize;

    println!("plan_c: leaderboard_limit={leaderboard_limit} candidate_pool_max={candidate_pool_max} period={period} multi_lb={use_multi_leaderboard}");
    println!("plan_c: min_pnl={min_pnl} min_volume={min_volume} min_realized_pnl={min_realized_pnl} max_mkt_conc={max_market_concentration} max_unreal={max_unrealized_ratio}");
    println!("plan_c: analysis_hours={analysis_hours} min_avg_interval={min_avg_interval_secs}s max_trades_in_window={max_trades_in_window}");
    println!("plan_c: bot_filters min_interval_stddev={min_interval_stddev} max_active_hour_ratio={max_active_hour_ratio} max_ncv={max_notional_cv} min_div={min_market_diversity} max_p99p50={max_size_p99_p50}");
    println!("plan_c: recent_window_hours={recent_window_hours} max_trade_age_secs={max_trade_age_secs}");
    println!("plan_c: score_weights w_ret={w_ret} w_win_rate={w_wr} w_profit_factor={w_pf} w_recent_roi={w_rr} w_dd={w_dd}");
    println!(
        "plan_c: order_usd base={} min={} max={} min_leader_notional={} poll_secs={}",
        order_usd_base,
        order_usd_min,
        order_usd_max,
        min_leader_notional,
        poll_secs
    );
    println!("plan_c: stop_loss={stop_loss_pct} take_profit={take_profit_pct} max_positions={max_positions}");
    println!("plan_c: buy_slippage={buy_slippage} max_per_trader={max_per_trader} refresh_leaders={refresh_leaders_secs}s");
    println!(
        "plan_c: exit_mode={:?} leader_sell_max_age_secs={leader_sell_max_age_secs}",
        exit_mode
    );
    println!(
        "plan_c: quality_gates min_copy_score={:.2} require_closed_trades={} min_closed_trades={}",
        min_copy_score, require_closed_trades, min_closed_trades
    );
    println!(
        "plan_c: auto_redeem={} rpc_url={} redeem_max_retries={} redeem_backoff_secs={}",
        auto_redeem, rpc_url, redeem_max_retries, redeem_backoff_secs
    );
    println!(
        "plan_c: clob_fail_threshold={} (rounds before marking market Resolved)",
        clob_fail_threshold
    );
    println!(
        "plan_c: pending_timeout_secs={} (0 = disabled; otherwise auto-cancel unfilled buys after N seconds)",
        pending_timeout_secs
    );
    println!(
        "plan_c: max_buy_price={} (skip copy-buys at or above this price — fee/slippage floor)",
        max_buy_price
    );
    println!(
        "plan_c: min_cash_reserve_pct={} (keep >= this fraction of equity as free USDC; 0 = disabled)",
        min_cash_reserve_pct
    );
    println!(
        "plan_c: reserve_block_on_fail={} rpc_urls_total={} (primary={}; fallbacks={})",
        reserve_block_on_fail,
        rpc_urls_all.len(),
        rpc_url,
        extra_rpc_urls.len()
    );

    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(30))
        .build()
        .expect("plan_c: http client build failed");

    // ------------------------------------------------------------------
    // State persistence & on-chain reconciliation
    // ------------------------------------------------------------------
    let state_path_str = resolve_runtime_var("PLAN_C_STATE_PATH", dotenv_path)
        .unwrap_or_else(|| "data/plan_c_state.json".to_string());
    let state_path_buf = std::path::PathBuf::from(&state_path_str);
    let state_path = state_path_buf.as_path();
    let history_path_buf = history_log_path(dotenv_path);
    let history_path = history_path_buf.as_path();
    // Resolve the *funding* wallet — where USDC and positions actually live.
    //   * Polymarket gnosis_safe users: a Safe proxy (PM_FUNDER) holds the funds;
    //     PROXY_WALLET is just the EOA that signs orders on behalf of the Safe and
    //     has a zero USDC balance. We MUST query PM_FUNDER for balance/positions.
    //   * EOA / proxy-magic users: PROXY_WALLET == funding wallet.
    //
    // Decision tree:
    //   PLAN_C_FUNDING_WALLET        — explicit override (highest priority)
    //   else if PM_SIGNATURE_TYPE is gnosis_safe / safe / 2
    //                                 → PM_FUNDER first, PROXY_WALLET fallback
    //   else                          → PROXY_WALLET first, PM_FUNDER fallback
    let sig_type_raw = resolve_runtime_var("PM_SIGNATURE_TYPE", dotenv_path)
        .map(|s| s.trim().to_ascii_lowercase())
        .unwrap_or_default();
    let is_gnosis_safe = matches!(
        sig_type_raw.as_str(),
        "2" | "gnosis_safe" | "gnosissafe" | "safe"
    );
    let explicit_funding = resolve_runtime_var("PLAN_C_FUNDING_WALLET", dotenv_path)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let proxy_wallet: Option<String> = if let Some(w) = explicit_funding {
        Some(w)
    } else if is_gnosis_safe {
        resolve_runtime_var("PM_FUNDER", dotenv_path)
            .or_else(|| resolve_runtime_var("PROXY_WALLET", dotenv_path))
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    } else {
        resolve_runtime_var("PROXY_WALLET", dotenv_path)
            .or_else(|| resolve_runtime_var("PM_FUNDER", dotenv_path))
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    };
    println!(
        "plan_c: state_path={} history_path={} funding_wallet={} (sig_type={}, source={})",
        state_path.display(),
        history_path.display(),
        proxy_wallet.as_deref().unwrap_or("(none)"),
        if sig_type_raw.is_empty() { "(unset)" } else { &sig_type_raw },
        if is_gnosis_safe { "PM_FUNDER" } else { "PROXY_WALLET" }
    );

    // ------------------------------------------------------------------
    // Build the redeem client if auto-redeem is enabled.
    // ------------------------------------------------------------------
    let redeem_client: Option<crate::plan_c_redeem::RedeemClient> = if auto_redeem {
        match build_redeem_client(
            dotenv_path,
            &rpc_url,
            &ctf_addr_raw,
            &neg_risk_addr_raw,
            &usdc_addr_raw,
            proxy_wallet.as_deref(),
        )
        .await
        {
            Ok(c) => {
                println!(
                    "plan_c: redeem client ready — position_holder={:#x}",
                    c.position_holder()
                );
                Some(c)
            }
            Err(e) => {
                eprintln!(
                    "plan_c: auto_redeem enabled but client construction failed: {e} — resolved positions will still release slots but won't be redeemed"
                );
                None
            }
        }
    } else {
        None
    };

    let persisted = load_plan_c_state(state_path);
    let mut open_positions: Vec<CopiedPosition> = persisted
        .positions
        .iter()
        .filter_map(PersistedPosition::to_position)
        .collect();
    let mut pending_buys: Vec<PendingBuy> = persisted
        .pending_buys
        .iter()
        .filter_map(PersistedPendingBuy::to_pending)
        .collect();
    let mut last_seen_trades: HashMap<String, i64> = persisted.last_seen_trades.clone();
    let loaded_daily_pnl = Decimal::from_str(persisted.daily_pnl.trim()).ok();
    let mut daily_pnl = loaded_daily_pnl.unwrap_or(context.risk.daily_pnl);

    // Roll over the daily-loss risk counter whenever we cross into a new UTC
    // calendar day. Historically `daily_pnl` was persisted forever, so a single
    // bad day would permanently trip the RiskEngine's daily_loss_limit and
    // block every subsequent entry until the counter crept back above the
    // threshold via take-profits. Now we tag the counter with the UTC date it
    // belongs to and reset it on day boundary (both at load-time and during
    // the main loop).
    let today_str = current_utc_date_str();
    let saved_date = persisted.daily_pnl_date.trim();
    let mut daily_pnl_date = if saved_date.is_empty() {
        // Legacy state file without a date tag. Conservatively treat the
        // stored pnl as "stale" and start today fresh.
        if daily_pnl != Decimal::ZERO {
            println!(
                "plan_c: state file has no daily_pnl_date — treating cached daily_pnl={daily_pnl} as stale and resetting to 0 for {today_str}"
            );
            daily_pnl = Decimal::ZERO;
        }
        today_str.clone()
    } else if saved_date == today_str {
        saved_date.to_string()
    } else {
        println!(
            "plan_c: daily_pnl roll-over at load — saved_date={saved_date} today={today_str}; resetting daily_pnl from {daily_pnl} to 0"
        );
        daily_pnl = Decimal::ZERO;
        today_str.clone()
    };
    {
        let active = open_positions
            .iter()
            .filter(|p| matches!(p.status, PositionStatus::Active))
            .count();
        let resolved = open_positions.len() - active;
        println!(
            "plan_c: restored {} position(s) (active={} resolved_or_failed={}) + {} pending buy(s), {} trader watermark(s), daily_pnl={} (date={})",
            open_positions.len(),
            active,
            resolved,
            pending_buys.len(),
            last_seen_trades.len(),
            daily_pnl,
            daily_pnl_date
        );
    }

    // Reconcile with on-chain truth (if a wallet is configured).
    //
    // If on-chain returns 0 positions while the cache still has live Active ones,
    // this can be either (a) those positions really were sold/redeemed between runs
    // or (b) the data API briefly returned a stale/empty payload. `reconcile_keep_on_empty`
    // lets ops pick the safe-but-stale behaviour (keep cache) over the aggressive
    // default (drop everything not confirmed on-chain).
    let reconcile_keep_on_empty =
        crate::read_bool_env_or(dotenv_path, "PLAN_C_RECONCILE_KEEP_ON_EMPTY", false);
    if let Some(wallet) = proxy_wallet.as_deref() {
        match fetch_onchain_positions(&http, wallet).await {
            Ok(onchain) => {
                println!(
                    "plan_c: fetched {} on-chain position(s) for reconciliation",
                    onchain.len()
                );
                let cached_active = open_positions
                    .iter()
                    .filter(|p| matches!(p.status, PositionStatus::Active))
                    .count();
                if onchain.is_empty() && cached_active > 0 && reconcile_keep_on_empty {
                    eprintln!(
                        "plan_c: on-chain returned 0 positions but cache has {} Active — keeping cache (PLAN_C_RECONCILE_KEEP_ON_EMPTY=true). If those positions really were settled manually, set the flag to false or edit the state file.",
                        cached_active
                    );
                } else {
                    if onchain.is_empty() && cached_active > 0 {
                        eprintln!(
                            "plan_c: WARNING — on-chain returned 0 positions while cache has {} Active. Dropping cache (default). Set PLAN_C_RECONCILE_KEEP_ON_EMPTY=true to override if this looks wrong.",
                            cached_active
                        );
                    }
                    open_positions =
                        reconcile_positions(open_positions, &onchain, history_path);
                    println!(
                        "plan_c: reconciled open_positions={} (of which {} are orphans with no trader metadata)",
                        open_positions.len(),
                        open_positions.iter().filter(|p| p.trader.is_empty()).count()
                    );
                }
            }
            Err(e) => {
                eprintln!(
                    "plan_c: on-chain reconciliation skipped (fetch failed after retries): {e} — proceeding with cached state only"
                );
            }
        }
    } else {
        eprintln!(
            "plan_c: PROXY_WALLET/PM_FUNDER not configured — skipping on-chain reconciliation"
        );
    }

    // Persist the reconciled state so the file reflects reality on next restart.
    save_plan_c_state(
        state_path,
        &open_positions,
        &pending_buys,
        &last_seen_trades,
        daily_pnl,
        &daily_pnl_date,
    );

    let mut qualified_traders: Vec<QualifiedTrader> = Vec::new();
    let mut last_leader_refresh: i64 = 0;
    let mut last_top20_snapshot: Vec<String> = Vec::new();
    let round_log_path = Path::new("data/plan_c_rounds.jsonl");

    let mut loop_idx: u64 = 0;
    loop {
        loop_idx += 1;
        let now = Utc::now().timestamp();

        // Roll over daily_pnl whenever the UTC date advances while the bot
        // is running. Without this, a bot that stays up across midnight would
        // keep yesterday's losses on the books and stay blocked by the
        // RiskEngine's daily_loss_limit until the counter organically recovers.
        {
            let today_str = current_utc_date_str();
            if today_str != daily_pnl_date {
                println!(
                    "plan_c: daily_pnl roll-over — previous_date={} today={} (resetting daily_pnl from {} to 0)",
                    daily_pnl_date, today_str, daily_pnl
                );
                daily_pnl = Decimal::ZERO;
                daily_pnl_date = today_str;
                save_plan_c_state(
                    state_path,
                    &open_positions,
                    &pending_buys,
                    &last_seen_trades,
                    daily_pnl,
                    &daily_pnl_date,
                );
            }
        }

        let mut force_refresh = false;
        if !last_top20_snapshot.is_empty() && !qualified_traders.is_empty() {
            if let Ok(snap_entries) = fetch_leaderboard(&http, 20, "WEEK", "PNL").await {
                let new_snap: Vec<String> = snap_entries
                    .into_iter()
                    .filter_map(|e| e.proxy_wallet.map(|a| a.trim().to_lowercase()))
                    .take(20)
                    .collect();
                let overlap = leaderboard_top_overlap(&last_top20_snapshot, &new_snap);
                if overlap < 1.0 - refresh_churn_threshold {
                    println!(
                        "plan_c: WEEK PNL top-20 churn (overlap={:.2} < {:.2}) — forcing leader refresh",
                        overlap,
                        1.0 - refresh_churn_threshold
                    );
                    force_refresh = true;
                }
            }
        }

        if force_refresh
            || now - last_leader_refresh >= refresh_leaders_secs as i64
            || qualified_traders.is_empty()
        {
            println!(
                "\n================ plan_c round #{loop_idx} — refreshing leaders ================\n"
            );
            let (q, top20) = discover_qualified_traders(
                &http,
                use_multi_leaderboard,
                leaderboard_limit,
                candidate_pool_max,
                leaderboard_limit,
                &period,
                min_pnl,
                min_volume,
                min_realized_pnl,
                max_market_concentration,
                max_unrealized_ratio,
                analysis_hours,
                min_avg_interval_secs,
                max_trades_in_window,
                min_interval_stddev,
                max_active_hour_ratio,
                recent_window_hours,
                max_notional_cv,
                min_market_diversity,
                max_size_p99_p50,
                pf_alpha,
                pf_beta,
                w_ret,
                w_wr,
                w_pf,
                w_rr,
                w_dd,
                min_copy_score,
                require_closed_trades,
                min_closed_trades,
                analyze_concurrency,
            )
            .await;
            qualified_traders = q;
            last_top20_snapshot = top20;
            last_leader_refresh = now;

            if qualified_traders.is_empty() {
                println!("plan_c: no qualified traders found, retrying in 5 minutes");
                time::sleep(Duration::from_secs(300)).await;
                continue;
            }
        }

        // Fetch trades once per round for the union of:
        // - all qualified traders (entry decisions)
        // - traders that currently have open positions (exit decisions)
        let mut leader_addrs: Vec<String> = Vec::new();
        for t in &qualified_traders {
            leader_addrs.push(t.address.clone());
        }
        for p in &open_positions {
            // Orphan positions (reconciled from chain) have no trader — skip them
            // to avoid wasted trades-API calls; they can only be managed by local SL/TP.
            if !p.trader.is_empty() {
                leader_addrs.push(p.trader.clone());
            }
        }
        leader_addrs.sort();
        leader_addrs.dedup();

        let mut trades_map: HashMap<String, Vec<TradeRecord>> = HashMap::new();
        for addr in &leader_addrs {
            // 50 is enough to see recent BUY/SELL events without hammering the API.
            match fetch_user_trades(&http, addr, 50).await {
                Ok(trades) => {
                    trades_map.insert(addr.clone(), trades);
                }
                Err(err) => {
                    eprintln!("plan_c: fetch_trades failed for {addr}: {err}");
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        // Initialise last_seen watermark for newly qualified traders from the fetched trades.
        // This prevents copying historical buys when a trader is first discovered.
        for trader in &qualified_traders {
            if last_seen_trades.contains_key(&trader.address) {
                continue;
            }
            let latest_ts = trades_map
                .get(&trader.address)
                .and_then(|v| v.iter().filter_map(|t| t.timestamp).max())
                .unwrap_or(now);
            last_seen_trades.insert(trader.address.clone(), latest_ts);
        }

        // ------------------------------------------------------------------
        // Pending-buy sweep: reconcile every outstanding limit order with the
        // exchange. Orders that filled (fully or partially) are promoted to
        // Active positions; orders that timed out are cancelled and anything
        // that did match is promoted. Rejected/cancelled orders are dropped.
        // ------------------------------------------------------------------
        let pending_sweep_state_dirty = sweep_pending_buys(
            execution,
            &mut pending_buys,
            &mut open_positions,
            now,
            pending_timeout_secs,
            max_positions,
            history_path,
        )
        .await;
        if pending_sweep_state_dirty {
            save_plan_c_state(
                state_path,
                &open_positions,
                &pending_buys,
                &last_seen_trades,
                daily_pnl,
                &daily_pnl_date,
            );
        }

        let mut closed_indices = Vec::new();
        for (idx, pos) in open_positions.iter_mut().enumerate() {
            // Resolved / RedeemFailed positions are no longer tradable on CLOB;
            // they are handled by the redeem pipeline below (Phase 2).
            if !matches!(pos.status, PositionStatus::Active) {
                continue;
            }

            let current_bid = match fetch_clob_best_bid(&pos.token_id).await {
                Ok(p) => {
                    pos.clob_fail_count = 0;
                    p
                }
                Err(_) => {
                    pos.clob_fail_count = pos.clob_fail_count.saturating_add(1);
                    if clob_fail_threshold > 0
                        && pos.clob_fail_count >= clob_fail_threshold
                    {
                        println!(
                            "plan_c: market price unreachable for {} rounds — marking RESOLVED for redeem: market={} token={}",
                            pos.clob_fail_count,
                            pos.market_id,
                            short_token(&pos.token_id),
                        );
                        pos.status = PositionStatus::Resolved;
                    }
                    continue;
                }
            };

            let pnl_pct = if pos.entry_price > Decimal::ZERO {
                (current_bid - pos.entry_price) / pos.entry_price
            } else {
                Decimal::ZERO
            };

            // 1) Leader exit (SELL) detection (optional by exit mode)
            if mode_has_leader_exit(exit_mode) {
                let now_ts = Utc::now().timestamp();
                let leader_sold = trades_map
                    .get(&pos.trader)
                    .map(|trades| {
                        trades.iter().any(|t| {
                            let is_sell = t
                                .side
                                .as_deref()
                                .map(|s| s.eq_ignore_ascii_case("SELL"))
                                .unwrap_or(false);
                            if !is_sell {
                                return false;
                            }
                            let ts = t.timestamp.unwrap_or(0);
                            if ts <= pos.leader_buy_ts {
                                return false;
                            }
                            if leader_sell_max_age_secs > 0
                                && (now_ts - ts) > leader_sell_max_age_secs
                            {
                                return false;
                            }
                            t.asset
                                .as_deref()
                                .map(|a| a.trim().eq(pos.token_id.as_str()))
                                .unwrap_or(false)
                        })
                    })
                    .unwrap_or(false);

                if leader_sold {
                    println!(
                        "plan_c: LEADER EXIT — market={} bid={} entry={} pnl={:.2}%",
                        pos.market_id,
                        current_bid,
                        pos.entry_price,
                        pnl_pct * Decimal::from(100_u32)
                    );

                    let sell_price =
                        (current_bid - buy_slippage).max(Decimal::new(1, 2)).round_dp(2);
                    let sell_size = conservative_sell_size(pos.size);
                    let signal = poly2::StrategySignal {
                        strategy_id: StrategyId::ProbabilityTrading,
                        market_id: pos.market_id.clone(),
                        yes_token_id: Some(pos.token_id.clone()),
                        no_token_id: None,
                        actions: vec![poly2::OrderIntent {
                            side: pos.side.clone(),
                            price: sell_price,
                            size: sell_size,
                            sell: true,
                            gtd_expiration: None,
                        }],
                        state: poly2::StrategyState::Implemented,
                    };
                    match execution.submit(&signal).await {
                        Ok(report) => {
                            print_execution_report(&report);
                            let round_pnl = (current_bid - pos.entry_price) * sell_size;
                            daily_pnl += round_pnl;
                            append_round_record(
                                round_log_path,
                                loop_idx,
                                &pos.market_id,
                                format!("copy_{}", pos.trader),
                                pos.entry_price,
                                current_bid,
                                sell_size,
                                round_pnl,
                                "leader_sell",
                            );
                            update_history_close(
                                history_path,
                                &pos.market_title,
                                &pos.trader_name,
                                &pos.side,
                                sell_size,
                                pos.entry_price,
                                pos.opened_at,
                                current_bid,
                                round_pnl,
                                Utc::now().timestamp(),
                            );
                            closed_indices.push(idx);
                        }
                        Err(e) => {
                            // IMPORTANT: keep the position in local state so the next poll
                            // retries the exit. Historical bug: we used to drop the position
                            // here unconditionally, producing "ghost" positions that stayed
                            // on-chain but were invisible to the bot.
                            eprintln!(
                                "plan_c: sell (leader exit) failed — keeping position for retry: {e}"
                            );
                        }
                    }
                    // Skip local SL/TP this round regardless of leader-exit outcome:
                    //   - on success, the position was just closed;
                    //   - on failure, we don't want to immediately fire a second sell order
                    //     in the same poll; next round will re-detect and retry cleanly.
                    continue;
                }
            }

            // 2) Local SL/TP safety net (optional by exit mode).
            //    Orphan positions (reconciled from chain with no trader) always need
            //    local safety — otherwise nobody is watching them.
            let local_safety_active = mode_has_local_safety(exit_mode) || pos.trader.is_empty();
            if local_safety_active && pnl_pct <= -stop_loss_pct {
                println!(
                    "plan_c: STOP LOSS — market={} bid={} entry={} pnl={:.2}%",
                    pos.market_id, current_bid, pos.entry_price,
                    pnl_pct * Decimal::from(100_u32)
                );

                let sell_price = (current_bid - buy_slippage).max(Decimal::new(1, 2)).round_dp(2);
                let sell_size = conservative_sell_size(pos.size);
                let signal = poly2::StrategySignal {
                    strategy_id: StrategyId::ProbabilityTrading,
                    market_id: pos.market_id.clone(),
                    yes_token_id: Some(pos.token_id.clone()),
                    no_token_id: None,
                    actions: vec![poly2::OrderIntent {
                        side: pos.side.clone(),
                        price: sell_price,
                        size: sell_size,
                        sell: true,
                        gtd_expiration: None,
                    }],
                    state: poly2::StrategyState::Implemented,
                };
                match execution.submit(&signal).await {
                    Ok(report) => {
                        print_execution_report(&report);
                        let round_pnl = (current_bid - pos.entry_price) * sell_size;
                        daily_pnl += round_pnl;
                        append_round_record(
                            round_log_path, loop_idx, &pos.market_id,
                            format!("copy_{}", pos.trader),
                            pos.entry_price, current_bid, sell_size, round_pnl, "stop_loss",
                        );
                        update_history_close(
                            history_path,
                            &pos.market_title,
                            &pos.trader_name,
                            &pos.side,
                            sell_size,
                            pos.entry_price,
                            pos.opened_at,
                            current_bid,
                            round_pnl,
                            Utc::now().timestamp(),
                        );
                        closed_indices.push(idx);
                    }
                    Err(e) => {
                        // Keep the position for a retry on the next poll instead
                        // of dropping it and leaving a ghost on-chain.
                        eprintln!(
                            "plan_c: sell (stop loss) failed — keeping position for retry: {e}"
                        );
                    }
                }
            } else if local_safety_active && pnl_pct >= take_profit_pct {
                println!(
                    "plan_c: TAKE PROFIT — market={} bid={} entry={} pnl={:.2}%",
                    pos.market_id, current_bid, pos.entry_price,
                    pnl_pct * Decimal::from(100_u32)
                );

                let sell_price = (current_bid - buy_slippage).max(Decimal::new(1, 2)).round_dp(2);
                let sell_size = conservative_sell_size(pos.size);
                let signal = poly2::StrategySignal {
                    strategy_id: StrategyId::ProbabilityTrading,
                    market_id: pos.market_id.clone(),
                    yes_token_id: Some(pos.token_id.clone()),
                    no_token_id: None,
                    actions: vec![poly2::OrderIntent {
                        side: pos.side.clone(),
                        price: sell_price,
                        size: sell_size,
                        sell: true,
                        gtd_expiration: None,
                    }],
                    state: poly2::StrategyState::Implemented,
                };
                match execution.submit(&signal).await {
                    Ok(report) => {
                        print_execution_report(&report);
                        let round_pnl = (current_bid - pos.entry_price) * sell_size;
                        daily_pnl += round_pnl;
                        append_round_record(
                            round_log_path, loop_idx, &pos.market_id,
                            format!("copy_{}", pos.trader),
                            pos.entry_price, current_bid, sell_size, round_pnl, "take_profit",
                        );
                        update_history_close(
                            history_path,
                            &pos.market_title,
                            &pos.trader_name,
                            &pos.side,
                            sell_size,
                            pos.entry_price,
                            pos.opened_at,
                            current_bid,
                            round_pnl,
                            Utc::now().timestamp(),
                        );
                        closed_indices.push(idx);
                    }
                    Err(e) => {
                        // Keep the position for a retry on the next poll instead
                        // of dropping it and leaving a ghost on-chain.
                        eprintln!(
                            "plan_c: sell (take profit) failed — keeping position for retry: {e}"
                        );
                    }
                }
            }
        }
        let any_closed = !closed_indices.is_empty();
        closed_indices.sort_unstable_by(|a, b| b.cmp(a));
        for idx in closed_indices {
            open_positions.remove(idx);
        }
        if any_closed {
            save_plan_c_state(
                state_path,
                &open_positions,
                &pending_buys,
                &last_seen_trades,
                daily_pnl,
                &daily_pnl_date,
            );
        }

        // ------------------------------------------------------------------
        // Redeem loop: settle any on-chain positions whose market has resolved.
        // Positions marked Resolved or RedeemFailed do not occupy a slot, but
        // they also never earn back their locked collateral until redeemed.
        // ------------------------------------------------------------------
        if let Some(redeem) = &redeem_client {
            let now_ts = Utc::now().timestamp();
            let mut redeemed_indices = Vec::new();
            for (idx, pos) in open_positions.iter_mut().enumerate() {
                if !matches!(
                    pos.status,
                    PositionStatus::Resolved | PositionStatus::RedeemFailed
                ) {
                    continue;
                }
                if pos.status == PositionStatus::RedeemFailed
                    && now_ts - pos.last_redeem_attempt < redeem_backoff_secs
                {
                    continue;
                }
                if redeem_max_retries > 0 && pos.redeem_retry_count >= redeem_max_retries {
                    // Stop retrying; surface so the operator can intervene.
                    continue;
                }

                let condition_id = match crate::plan_c_redeem::parse_b256(&pos.market_id) {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!(
                            "plan_c: redeem skipped — invalid conditionId '{}': {e}",
                            pos.market_id
                        );
                        pos.status = PositionStatus::RedeemFailed;
                        pos.redeem_retry_count =
                            pos.redeem_retry_count.saturating_add(1);
                        pos.last_redeem_attempt = now_ts;
                        continue;
                    }
                };

                // Confirm on-chain that the market has actually reported.
                match redeem.is_resolved(condition_id).await {
                    Ok(true) => {}
                    Ok(false) => {
                        // The data-api said redeemable but the CTF hasn't been
                        // reported yet; back off and try next round.
                        pos.last_redeem_attempt = now_ts;
                        continue;
                    }
                    Err(e) => {
                        eprintln!(
                            "plan_c: is_resolved RPC failed for market={}: {e}",
                            pos.market_id
                        );
                        pos.last_redeem_attempt = now_ts;
                        continue;
                    }
                }

                // Determine negRisk flag lazily and cache on the position.
                if pos.neg_risk.is_none() {
                    match fetch_market_neg_risk(&http, &pos.market_id).await {
                        Ok(flag) => {
                            pos.neg_risk = Some(flag.unwrap_or(false));
                        }
                        Err(e) => {
                            eprintln!(
                                "plan_c: gamma lookup failed for {}: {e} — assuming standard CTF",
                                pos.market_id
                            );
                            pos.neg_risk = Some(false);
                        }
                    }
                }
                let is_neg_risk = pos.neg_risk.unwrap_or(false);

                pos.last_redeem_attempt = now_ts;
                let redeem_result = if is_neg_risk {
                    redeem.redeem_neg_risk(condition_id).await
                } else {
                    redeem.redeem_standard(condition_id).await
                };

                match redeem_result {
                    Ok(tx_hash) => {
                        let final_price = pos.resolved_price.unwrap_or(Decimal::ZERO);
                        let round_pnl = (final_price - pos.entry_price) * pos.size;
                        daily_pnl += round_pnl;
                        append_round_record(
                            round_log_path,
                            loop_idx,
                            &pos.market_id,
                            format!("copy_{}", pos.trader),
                            pos.entry_price,
                            final_price,
                            pos.size,
                            round_pnl,
                            if is_neg_risk { "redeemed_neg_risk" } else { "redeemed" },
                        );
                        update_history_close(
                            history_path,
                            &pos.market_title,
                            &pos.trader_name,
                            &pos.side,
                            pos.size,
                            pos.entry_price,
                            pos.opened_at,
                            final_price,
                            round_pnl,
                            Utc::now().timestamp(),
                        );
                        println!(
                            "plan_c: REDEEMED ({}) market={} tx=0x{:x} final_px={} entry={} size={} pnl={}",
                            if is_neg_risk { "neg_risk" } else { "standard" },
                            pos.market_id,
                            tx_hash,
                            final_price,
                            pos.entry_price,
                            pos.size,
                            round_pnl,
                        );
                        redeemed_indices.push(idx);
                    }
                    Err(e) => {
                        pos.status = PositionStatus::RedeemFailed;
                        pos.redeem_retry_count =
                            pos.redeem_retry_count.saturating_add(1);
                        eprintln!(
                            "plan_c: redeem FAILED (attempt {}/{}) market={}: {e}",
                            pos.redeem_retry_count,
                            redeem_max_retries,
                            pos.market_id
                        );
                        if redeem_max_retries > 0
                            && pos.redeem_retry_count >= redeem_max_retries
                        {
                            eprintln!(
                                "plan_c: redeem GIVE UP for market={} after {} attempts — manual intervention required",
                                pos.market_id, pos.redeem_retry_count
                            );
                        }
                    }
                }
            }
            let any_redeemed = !redeemed_indices.is_empty();
            redeemed_indices.sort_unstable_by(|a, b| b.cmp(a));
            for idx in redeemed_indices {
                open_positions.remove(idx);
            }
            if any_redeemed {
                save_plan_c_state(
                    state_path,
                    &open_positions,
                    &pending_buys,
                    &last_seen_trades,
                    daily_pnl,
                    &daily_pnl_date,
                );
            }
        }

        let active_count = open_positions
            .iter()
            .filter(|p| matches!(p.status, PositionStatus::Active))
            .count()
            + pending_buys.len();
        if active_count < max_positions {
            // Step 1: collect pending BUY signals. DO NOT advance last_seen here —
            // we only advance it per-trader once a signal has actually been decided on.
            let collection_now = Utc::now().timestamp();
            let stale_cutoff = collection_now - max_trade_age_secs;
            let mut pending: Vec<(f64, usize, TradeRecord)> = Vec::new();
            // Per-trader max ts we will consider advancing last_seen to, populated as
            // we decide on each pending signal below.
            let mut advance_ts: HashMap<String, i64> = HashMap::new();

            // Pre-fetch USDC balance once per round so we can enforce the cash
            // reserve across multiple copy buys placed in the same iteration.
            // Enabled only when a wallet is configured AND the reserve pct > 0.
            // `remaining_cash` is decremented locally as we place orders below.
            let reserve_enabled = min_cash_reserve_pct > Decimal::ZERO
                && proxy_wallet.as_deref().map(|w| !w.is_empty()).unwrap_or(false);
            let mut reserve_fetch_failed = false;
            let mut remaining_cash: Option<Decimal> = if reserve_enabled {
                let wallet = proxy_wallet.as_deref().unwrap_or("");
                let rpc_refs: Vec<&str> =
                    rpc_urls_all.iter().map(|s| s.as_str()).collect();
                match poly2::fetch_usdc_balance_multi(&rpc_refs, &usdc_addr_raw, wallet).await {
                    Ok(bal) => {
                        println!(
                            "plan_c: cash_reserve check — wallet_usdc={} reserve_pct={} (rpc_urls_tried={})",
                            bal, min_cash_reserve_pct, rpc_refs.len()
                        );
                        Some(bal)
                    }
                    Err(e) => {
                        reserve_fetch_failed = true;
                        if reserve_block_on_fail {
                            eprintln!(
                                "plan_c: cash reserve fetch failed across {} rpc(s): {e} — BLOCKING copy-buys this round (set PLAN_C_RESERVE_BLOCK_ON_FAIL=false to override)",
                                rpc_refs.len()
                            );
                        } else {
                            eprintln!(
                                "plan_c: cash reserve fetch failed across {} rpc(s): {e} — proceeding WITHOUT reserve check this round",
                                rpc_refs.len()
                            );
                        }
                        None
                    }
                }
            } else {
                None
            };
            let block_buys_this_round = reserve_fetch_failed && reserve_block_on_fail;

            // Funnel diagnostics — track why candidate buys do / don't make it into
            // the `pending` pool so an operator can see at a glance whether the bot
            // is just waiting for fresh activity or misconfigured.
            let mut funnel_no_trades: usize = 0;
            let mut funnel_seen_trades: usize = 0;
            let mut funnel_not_buy: usize = 0;
            let mut funnel_already_seen: usize = 0;
            let mut funnel_stale: usize = 0;
            let mut funnel_fresh_buys: usize = 0;
            let mut newest_fresh_age: Option<i64> = None;
            let mut oldest_stale_age: Option<i64> = None;

            for (idx, trader) in qualified_traders.iter().enumerate() {
                let last_ts = last_seen_trades.get(&trader.address).copied().unwrap_or(0);
                let Some(trades) = trades_map.get(&trader.address) else {
                    funnel_no_trades += 1;
                    continue;
                };
                funnel_seen_trades += trades.len();

                for t in trades.iter() {
                    let ts = t.timestamp.unwrap_or(0);
                    let is_buy = t
                        .side
                        .as_deref()
                        .map(|s| s.eq_ignore_ascii_case("BUY"))
                        .unwrap_or(false);
                    if !is_buy {
                        funnel_not_buy += 1;
                        continue;
                    }
                    if ts <= last_ts {
                        funnel_already_seen += 1;
                        continue;
                    }
                    if ts < stale_cutoff {
                        funnel_stale += 1;
                        let age = collection_now - ts;
                        oldest_stale_age = Some(oldest_stale_age.map_or(age, |o| o.max(age)));
                        // Too old — discard and advance the watermark so we don't
                        // keep seeing it next round.
                        let e = advance_ts.entry(trader.address.clone()).or_insert(0);
                        if ts > *e {
                            *e = ts;
                        }
                        continue;
                    }
                    funnel_fresh_buys += 1;
                    let age = collection_now - ts;
                    newest_fresh_age = Some(newest_fresh_age.map_or(age, |o| o.min(age)));
                    pending.push((trader.score, idx, t.clone()));
                }
            }

            println!(
                "plan_c: signal_funnel traders={} no_trades_fetched={} trades_scanned={} not_buy={} already_seen={} stale_>{}s={}{} fresh_buys={}{} → pending={}",
                qualified_traders.len(),
                funnel_no_trades,
                funnel_seen_trades,
                funnel_not_buy,
                funnel_already_seen,
                max_trade_age_secs,
                funnel_stale,
                oldest_stale_age
                    .map(|a| format!(" (oldest {}s)", a))
                    .unwrap_or_default(),
                funnel_fresh_buys,
                newest_fresh_age
                    .map(|a| format!(" (youngest {}s)", a))
                    .unwrap_or_default(),
                pending.len(),
            );

            // Step 2: prioritise by trader score (descending). Ties preserve insertion
            // order which roughly follows leaderboard order from step 1.
            pending.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

            // Step 2b: de-correlate — at most one pending BUY per `condition_id` (best score kept).
            let mut seen_cid: std::collections::HashSet<String> = std::collections::HashSet::new();
            let mut unique_pending: Vec<(f64, usize, TradeRecord)> = Vec::new();
            for triple in pending {
                let cid_opt = triple
                    .2
                    .condition_id
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty());
                if let Some(cid) = cid_opt {
                    if !seen_cid.insert(cid.to_string()) {
                        continue;
                    }
                }
                unique_pending.push(triple);
            }
            pending = unique_pending;

            // Cash-reserve safety: if we couldn't confirm free USDC and the operator
            // opted in to strict blocking, drop every candidate for this round.
            // Signals stay un-marked so they can be reconsidered once RPC recovers
            // (until they age out of the `max_trade_age_secs` window).
            if block_buys_this_round && !pending.is_empty() {
                println!(
                    "plan_c: DROPPING {} pending copy-buy candidate(s) this round due to cash-reserve RPC failure",
                    pending.len()
                );
                pending.clear();
            }

            // Step 3: process in priority order, honouring capacity limits. Any signal
            // that we decide on (copied OR skipped for a non-capacity reason) advances
            // the per-trader watermark. Signals skipped purely due to capacity are left
            // to be re-considered next round (or eventually aged out by max_trade_age).
            for (score, trader_idx, trade) in pending {
                let trader = &qualified_traders[trader_idx];
                let trade_ts = trade.timestamp.unwrap_or(0);

                let mut mark_processed = |addr: &str, ts: i64| {
                    let e = advance_ts.entry(addr.to_string()).or_insert(0);
                    if ts > *e {
                        *e = ts;
                    }
                };

                let active_now = open_positions
                    .iter()
                    .filter(|p| matches!(p.status, PositionStatus::Active))
                    .count()
                    + pending_buys.len();
                if active_now >= max_positions {
                    // Capacity-bound skip: leave watermark untouched so we re-pick
                    // it up next round (if it hasn't aged out).
                    continue;
                }
                let per_trader_now = open_positions
                    .iter()
                    .filter(|p| {
                        matches!(p.status, PositionStatus::Active)
                            && p.trader == trader.address
                    })
                    .count()
                    + pending_buys
                        .iter()
                        .filter(|p| p.trader == trader.address)
                        .count();
                if per_trader_now >= max_per_trader {
                    continue;
                }

                let token_id = match &trade.asset {
                    Some(a) if !a.trim().is_empty() => a.trim().to_string(),
                    _ => {
                        mark_processed(&trader.address, trade_ts);
                        continue;
                    }
                };
                let market_id = match &trade.condition_id {
                    Some(c) if !c.trim().is_empty() => c.trim().to_string(),
                    _ => {
                        mark_processed(&trader.address, trade_ts);
                        continue;
                    }
                };
                let trade_price =
                    Decimal::from_str(&trade.price.unwrap_or(0.5).to_string())
                        .unwrap_or(Decimal::new(50, 2));

                if open_positions.iter().any(|p| {
                    matches!(p.status, PositionStatus::Active)
                        && p.market_id == market_id
                        && p.token_id == token_id
                }) || pending_buys
                    .iter()
                    .any(|p| p.market_id == market_id && p.token_id == token_id)
                {
                    mark_processed(&trader.address, trade_ts);
                    continue;
                }

                let buy_price = (trade_price + buy_slippage).round_dp(2);
                if buy_price >= Decimal::ONE || buy_price <= Decimal::ZERO {
                    mark_processed(&trader.address, trade_ts);
                    continue;
                }
                // Reject high-priced copies: even a winning settlement at 1.00
                // struggles to cover maker/taker fees + slippage here.
                // Check both the leader's fill price AND our own (slipped) bid.
                if trade_price >= max_buy_price || buy_price >= max_buy_price {
                    let leader_name = trader.user_name.as_str();
                    let market_title = trade.title.as_deref().unwrap_or(&market_id);
                    println!(
                        "plan_c: SKIP (price too high) — trader={} market=\"{}\" leader_price={} our_bid={} cap={}",
                        leader_name, market_title, trade_price, buy_price, max_buy_price
                    );
                    mark_processed(&trader.address, trade_ts);
                    continue;
                }

                let leader_notional_f = trade
                    .usdc_size
                    .unwrap_or(0.0)
                    .max(trade.size.unwrap_or(0.0).abs() * trade.price.unwrap_or(0.0));
                if leader_notional_f < min_leader_notional {
                    println!(
                        "plan_c: SKIP (leader notional {:.2} < min {:.2}) — trader={}",
                        leader_notional_f, min_leader_notional, trader.user_name
                    );
                    mark_processed(&trader.address, trade_ts);
                    continue;
                }

                let scale_raw = (leader_notional_f / 100.0).log10().clamp(0.5, 3.0);
                let scale_dec = Decimal::from_str(&format!("{scale_raw:.6}"))
                    .unwrap_or(Decimal::ONE);
                let mut order_usd_this = (order_usd_base * scale_dec).round_dp(2);
                if order_usd_this < order_usd_min {
                    order_usd_this = order_usd_min;
                }
                if order_usd_this > order_usd_max {
                    order_usd_this = order_usd_max;
                }

                let size = (order_usd_this / buy_price).round_dp(2);
                if size < Decimal::from(1_u32) {
                    mark_processed(&trader.address, trade_ts);
                    continue;
                }

                // Cash-reserve guard: do not let free USDC fall below
                // `reserve_pct * total_equity`, where total_equity = free_cash
                // + capital already allocated to Active positions / pending buys.
                // We use the signed-order notional (buy_price * size) as the
                // cash outflow estimate — close enough for a soft rail.
                if let Some(cash) = remaining_cash {
                    let order_notional = (buy_price * size).round_dp(6);
                    let allocated: Decimal = open_positions
                        .iter()
                        .filter(|p| matches!(p.status, PositionStatus::Active))
                        .map(|p| p.entry_price * p.size)
                        .sum::<Decimal>()
                        + pending_buys
                            .iter()
                            .map(|p| p.price * p.size)
                            .sum::<Decimal>();
                    let total_equity = cash + allocated;
                    let reserve_floor = (total_equity * min_cash_reserve_pct).round_dp(6);
                    let cash_after = cash - order_notional;
                    if cash_after < reserve_floor {
                        let market_title = trade.title.as_deref().unwrap_or(&market_id);
                        println!(
                            "plan_c: SKIP (cash reserve) — trader={} market=\"{}\" cash={} need>={} after_order={} (equity={} reserve_pct={})",
                            trader.user_name,
                            market_title,
                            cash,
                            reserve_floor,
                            cash_after,
                            total_equity,
                            min_cash_reserve_pct,
                        );
                        // Do NOT mark_processed: cash may be freed later (e.g. a
                        // settlement / sell) so this trade should be reconsidered
                        // next round, provided it hasn't aged out.
                        continue;
                    }
                }

                let trade_title = trade.title.as_deref().unwrap_or(&market_id);
                let trade_outcome = trade.outcome.as_deref().unwrap_or("?");
                let age_secs = (collection_now - trade_ts).max(0);

                println!(
                    "plan_c: COPY BUY — trader={} (score={:.2} rot={:.4} sharpe7d={:.2} wr_lcb={:.2} pf={:.2} recent_roi={:.2} max_dd={:.0}) market=\"{}\" outcome={} order_usd={} price={} size={} age={}s (leader pnl={:.0} leader_notional=${:.0})",
                    trader.user_name,
                    score,
                    trader.roi,
                    trader.sharpe_7d,
                    trader.win_rate,
                    trader.profit_factor,
                    trader.recent_roi,
                    trader.max_drawdown,
                    trade_title,
                    trade_outcome,
                    order_usd_this,
                    buy_price,
                    size,
                    age_secs,
                    trader.pnl,
                    leader_notional_f
                );

                let mut risk_ctx = context.clone();
                risk_ctx.risk.daily_pnl = daily_pnl;
                let signal = poly2::StrategySignal {
                    strategy_id: StrategyId::ProbabilityTrading,
                    market_id: market_id.clone(),
                    yes_token_id: Some(token_id.clone()),
                    no_token_id: None,
                    actions: vec![poly2::OrderIntent {
                        side: Side::Yes,
                        price: buy_price,
                        size,
                        sell: false,
                        gtd_expiration: None,
                    }],
                    state: poly2::StrategyState::Implemented,
                };
                if !risk_engine.allow(&signal, &risk_ctx) {
                    println!("plan_c: BLOCKED by risk engine (daily_pnl={daily_pnl})");
                    mark_processed(&trader.address, trade_ts);
                    continue;
                }

                // Either the order goes through or errors out — either way the trade
                // has been "acted upon", so mark it processed to avoid retry loops.
                mark_processed(&trader.address, trade_ts);

                match execution.submit(&signal).await {
                    Ok(report) => {
                        print_execution_report(&report);
                        let now_submit = Utc::now().timestamp();
                        let fill_size: Decimal = report
                            .fills
                            .iter()
                            .filter(|f| !f.sell)
                            .map(|f| f.size)
                            .sum();
                        // Deduct the order's notional from our local cash tracker
                        // so later buys in the same round see the reduced balance
                        // without us having to re-query on-chain every time.
                        if let Some(cash) = remaining_cash.as_mut() {
                            *cash -= (buy_price * size).round_dp(6);
                        }

                        // Case A: exchange reports (partially) filled right away — promote
                        // to an Active position immediately.
                        if matches!(
                            report.status,
                            ExecutionStatus::Filled | ExecutionStatus::PartiallyFilled
                        ) {
                            let actual_size = if fill_size > Decimal::ZERO {
                                (fill_size * Decimal::new(97, 2)).round_dp(2)
                            } else {
                                size
                            };
                            let market_title_owned = trade_title.to_string();
                            let trader_name_owned = trader.user_name.clone();
                            open_positions.push(CopiedPosition {
                                trader: trader.address.clone(),
                                market_id: market_id.clone(),
                                token_id: token_id.clone(),
                                side: Side::Yes,
                                entry_price: buy_price,
                                size: actual_size,
                                opened_at: now_submit,
                                leader_buy_ts: trade_ts,
                                status: PositionStatus::Active,
                                resolved_price: None,
                                clob_fail_count: 0,
                                redeem_retry_count: 0,
                                last_redeem_attempt: 0,
                                neg_risk: None,
                                market_title: market_title_owned.clone(),
                                trader_name: trader_name_owned.clone(),
                            });
                            append_history_open(
                                history_path,
                                &market_title_owned,
                                &trader_name_owned,
                                &Side::Yes,
                                actual_size,
                                buy_price,
                                now_submit,
                            );
                            let active_after = open_positions
                                .iter()
                                .filter(|p| matches!(p.status, PositionStatus::Active))
                                .count();
                            println!(
                                "plan_c: position opened — {}/{} active now (pending={})",
                                active_after,
                                max_positions,
                                pending_buys.len()
                            );
                            save_plan_c_state(
                                state_path,
                                &open_positions,
                                &pending_buys,
                                &last_seen_trades,
                                daily_pnl,
                                &daily_pnl_date,
                            );

                        // Case B: order accepted as a resting limit order (Pending) —
                        // register it so subsequent fills are tracked rather than lost.
                        } else if matches!(report.status, ExecutionStatus::Pending) {
                            if let Some(order_id) =
                                report.order_ids.first().cloned()
                            {
                                pending_buys.push(PendingBuy {
                                    order_id,
                                    trader: trader.address.clone(),
                                    market_id: market_id.clone(),
                                    token_id: token_id.clone(),
                                    side: Side::Yes,
                                    price: buy_price,
                                    size,
                                    matched_cum: fill_size,
                                    leader_buy_ts: trade_ts,
                                    submitted_at: now_submit,
                                    last_checked_at: now_submit,
                                    market_title: trade_title.to_string(),
                                    trader_name: trader.user_name.clone(),
                                });
                                println!(
                                    "plan_c: order PENDING on book — market={} price={} size={} pending_buys={}",
                                    market_id,
                                    buy_price,
                                    size,
                                    pending_buys.len()
                                );
                                save_plan_c_state(
                                    state_path,
                                    &open_positions,
                                    &pending_buys,
                                    &last_seen_trades,
                                    daily_pnl,
                                    &daily_pnl_date,
                                );
                            } else {
                                eprintln!(
                                    "plan_c: order Pending but no order_id returned — untracked! market={} price={} size={}",
                                    market_id, buy_price, size
                                );
                            }

                        // Case C: Rejected — just log it.
                        } else {
                            eprintln!(
                                "plan_c: buy order rejected — market={} price={} size={} status={:?}",
                                market_id, buy_price, size, report.status
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!("plan_c: buy order failed: {e}");
                    }
                }
            }

            // Step 4: finalise last_seen advances based on what we actually decided on.
            for (addr, ts) in advance_ts {
                let entry = last_seen_trades.entry(addr).or_insert(0);
                if ts > *entry {
                    *entry = ts;
                }
            }
        }

        // End-of-iteration fallback save: captures last_seen_trades / daily_pnl
        // updates even when positions didn't change this round.
        save_plan_c_state(
            state_path,
            &open_positions,
            &pending_buys,
            &last_seen_trades,
            daily_pnl,
            &daily_pnl_date,
        );

        let active_cnt = open_positions
            .iter()
            .filter(|p| matches!(p.status, PositionStatus::Active))
            .count();
        let resolved_cnt = open_positions
            .iter()
            .filter(|p| matches!(p.status, PositionStatus::Resolved))
            .count();
        let redeem_failed_cnt = open_positions
            .iter()
            .filter(|p| matches!(p.status, PositionStatus::RedeemFailed))
            .count();
        println!(
            "[{}] plan_c: poll #{loop_idx} done — positions={}/{} pending={} resolved={} redeem_failed={} daily_pnl={} tracking={} traders — sleeping {}s",
            chrono::Local::now().format("%m/%d %H:%M:%S"),
            active_cnt,
            max_positions,
            pending_buys.len(),
            resolved_cnt,
            redeem_failed_cnt,
            daily_pnl,
            qualified_traders.len(),
            poll_secs
        );
        time::sleep(Duration::from_secs(poll_secs)).await;
    }
}

#[cfg(test)]
mod plan_c_algorithm_tests {
    use super::*;

    fn tr(
        side: &str,
        cid: &str,
        asset: &str,
        price: f64,
        size: f64,
        ts: i64,
    ) -> TradeRecord {
        TradeRecord {
            proxy_wallet: None,
            side: Some(side.to_string()),
            asset: Some(asset.to_string()),
            condition_id: Some(cid.to_string()),
            size: Some(size),
            price: Some(price),
            timestamp: Some(ts),
            title: None,
            slug: None,
            outcome_index: None,
            outcome: None,
            usdc_size: None,
        }
    }

    #[test]
    fn wilson_lcb_below_point_estimate() {
        let w = wilson_lower_bound(8, 10, 1.96);
        assert!(w < 0.8, "LCB should be conservative: {w}");
        assert!(w > 0.3);
    }

    #[test]
    fn fifo_aggregate_counts_one_close() {
        let trades = vec![
            tr("BUY", "c1", "a1", 0.40, 100.0, 1000),
            tr("SELL", "c1", "a1", 0.60, 100.0, 1100),
        ];
        let a = fifo_pnl_aggregate(&trades, None);
        assert_eq!(a.total, 1);
        assert_eq!(a.wins, 1);
        assert!((a.gross_profit - 20.0).abs() < 1e-6);
        assert!(a.max_drawdown >= 0.0);
    }

    #[test]
    fn profit_factor_shrinkage_stable() {
        let p = profit_factor_shrinkage(100.0, 50.0, 50.0, 50.0);
        assert!((p - 1.5).abs() < 0.01);
    }

    #[test]
    fn compute_score_v2_in_range() {
        let s = compute_score_v2(1.0, 0.5, 2.0, 0.1, 0.2, 0.25, 0.25, 0.2, 0.15, 0.15);
        assert!(s > 0.0 && s <= 1.5, "score={s}");
    }

    #[test]
    fn max_drawdown_from_events_detects_loss() {
        let ev = vec![(1_i64, 10.0), (2, -30.0), (3, 5.0)];
        let dd = max_drawdown_from_equity_events(&ev);
        assert!(dd >= 20.0);
    }

    #[test]
    fn buy_notional_since_sums_buys() {
        let t = vec![
            tr("BUY", "c", "a", 0.5, 10.0, 2000),
            tr("SELL", "c", "a", 0.7, 10.0, 2100),
        ];
        let n = buy_notional_since(&t, 1990, 3000);
        assert!((n - 5.0).abs() < 1e-6);
    }

    #[test]
    fn leaderboard_overlap_identical() {
        let a = vec!["x".to_string(), "y".to_string()];
        let b = vec!["x".to_string(), "y".to_string()];
        assert!((leaderboard_top_overlap(&a, &b) - 0.1).abs() < 1e-9);
    }

    #[test]
    fn retention_norm_clamp() {
        assert!((retention_norm(0.0, -1.0, 1.0) - 0.5).abs() < 1e-9);
    }

    /// Neg-risk hedged position collapses to zero-pnl closure instead of being
    /// counted as two independent trades.
    #[test]
    fn fifo_neg_risk_perfect_hedge_nets_zero() {
        // Make `yes_a` the canonical YES by giving it more trades (majority).
        let trades = vec![
            tr("BUY", "c1", "yes_a", 0.30, 100.0, 1000),
            tr("BUY", "c1", "no_a", 0.70, 100.0, 1100), // -> canon SELL YES @ 0.30
            tr("SELL", "c1", "yes_a", 0.30, 1.0, 1200), // filler so yes_a majority
        ];
        let a = fifo_pnl_aggregate(&trades, None);
        assert_eq!(a.wins, 0, "hedge is not a win: gp={} gl={}", a.gross_profit, a.gross_loss);
        assert!(a.gross_profit.abs() < 1e-6);
        assert!(a.gross_loss.abs() < 1e-6);
        // NO BUY closes the YES lot entirely → the later filler SELL YES has
        // no inventory to match, so exactly one closure is recorded at pnl=0.
        assert_eq!(a.total, 1);
    }

    /// Regression: data-api rejects `VOLUME` with HTTP 400. We must query
    /// using the literal `VOL`. This test pins the constant string inside
    /// `fetch_leaderboard_candidate_pool` so a future "clarifying rename"
    /// back to `VOLUME` fails loudly in CI instead of silently emptying the
    /// candidate pool in production.
    #[test]
    fn leaderboard_candidate_pool_uses_vol_orderby() {
        let src = include_str!("plan_c.rs");
        assert!(
            src.contains("fetch_leaderboard(http, per_list_limit, \"WEEK\", \"VOL\")"),
            "candidate pool must use orderBy=VOL (data-api rejects VOLUME)"
        );
        assert!(
            !src.contains("\"WEEK\", \"VOLUME\""),
            "orderBy=VOLUME is rejected by polymarket data-api with HTTP 400"
        );
    }

    /// Cross-asset: BUY YES then SELL NO flips to BUY YES; a later SELL YES
    /// closes both lots with a single closure counted once.
    #[test]
    fn fifo_neg_risk_cross_asset_profit() {
        let trades = vec![
            tr("BUY", "c2", "yes_a", 0.30, 100.0, 1000),
            tr("SELL", "c2", "no_a", 0.20, 100.0, 1050), // -> canon BUY YES @ 0.80
            tr("SELL", "c2", "yes_a", 0.50, 150.0, 1200),
        ];
        let a = fifo_pnl_aggregate(&trades, None);
        assert_eq!(a.total, 1, "single SELL trade = single closure");
        // pnl = (0.50-0.30)*100 + (0.50-0.80)*50 = 20 - 15 = 5
        assert!((a.gross_profit - 5.0).abs() < 1e-6, "gp={}", a.gross_profit);
        assert!(a.gross_loss.abs() < 1e-6);
        assert_eq!(a.wins, 1);
    }
}
