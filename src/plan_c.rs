use anyhow::{anyhow, Context};
use chrono::Utc;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
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
    roi: f64,
    win_rate: f64,
    profit_factor: f64,
    recent_roi: f64,
    score: f64,
}

#[derive(Debug, Clone, Default)]
struct TraderAnalysis {
    avg_interval_secs: f64,
    trade_count: usize,
    interval_stddev: f64,
    active_hour_ratio: f64,
    win_rate: f64,
    profit_factor: f64,
    recent_roi: f64,
    is_bot: bool,
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
        }
    }

    fn to_position(&self) -> Option<CopiedPosition> {
        let entry_price = Decimal::from_str(self.entry_price.trim()).ok()?;
        let size = Decimal::from_str(self.size.trim()).ok()?;
        Some(CopiedPosition {
            trader: self.trader.clone(),
            market_id: self.market_id.clone(),
            token_id: self.token_id.clone(),
            side: side_from_persisted(&self.side),
            entry_price,
            size,
            opened_at: self.opened_at,
            leader_buy_ts: self.leader_buy_ts,
        })
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct PlanCState {
    #[serde(default)]
    positions: Vec<PersistedPosition>,
    #[serde(default)]
    last_seen_trades: HashMap<String, i64>,
    #[serde(default)]
    daily_pnl: String,
    #[serde(default)]
    saved_at: String,
}

fn save_plan_c_state(
    path: &Path,
    positions: &[CopiedPosition],
    last_seen_trades: &HashMap<String, i64>,
    daily_pnl: Decimal,
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
        last_seen_trades: last_seen_trades.clone(),
        daily_pnl: daily_pnl.to_string(),
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
}

async fn fetch_onchain_positions(
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
    let positions: Vec<OnchainPosition> =
        resp.json().await.context("positions parse failed")?;
    Ok(positions)
}

fn f64_to_decimal(v: f64) -> Option<Decimal> {
    // Use a string formatted to a fixed precision to avoid scientific notation
    // and to tolerate floating-point noise.
    if !v.is_finite() {
        return None;
    }
    Decimal::from_str(&format!("{:.8}", v)).ok()
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
                reconciled.push(pos);
            }
            None => {
                println!(
                    "plan_c: reconcile — dropping cached position (no longer on-chain): market={} token={} size={}",
                    pos.market_id,
                    short_token(&pos.token_id),
                    pos.size
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
        reconciled.push(CopiedPosition {
            trader: String::new(),
            market_id: cid,
            token_id: asset,
            side: Side::Yes,
            entry_price: entry_dec,
            size: size_dec,
            opened_at: Utc::now().timestamp(),
            leader_buy_ts: 0,
        });
    }

    reconciled
}

fn short_token(token_id: &str) -> String {
    let end = 12.min(token_id.len());
    format!("{}…", &token_id[..end])
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
) -> anyhow::Result<Vec<LeaderboardEntry>> {
    let api_period = normalize_leaderboard_period(period);
    let clamped_limit = limit.min(50);
    let mut all_entries = Vec::new();
    let mut offset: u64 = 0;

    while all_entries.len() < limit as usize {
        let batch = clamped_limit.min(limit.saturating_sub(offset));
        if batch == 0 {
            break;
        }
        let url = format!(
            "https://data-api.polymarket.com/v1/leaderboard?limit={}&offset={}&timePeriod={}&orderBy=PNL",
            batch, offset, api_period
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

#[allow(clippy::too_many_arguments)]
fn compute_score(
    roi: f64,
    win_rate: f64,
    profit_factor: f64,
    recent_roi: f64,
    w_roi: f64,
    w_wr: f64,
    w_pf: f64,
    w_rr: f64,
) -> f64 {
    let roi_norm = roi.clamp(0.0, 5.0) / 5.0;
    let wr_norm = win_rate.clamp(0.0, 1.0);
    let pf_norm = profit_factor.clamp(0.0, 5.0) / 5.0;
    // recent_roi reasonable range ~ [-1, +1]; remap to [0, 1] with clipping.
    let rr_norm = (recent_roi + 1.0).clamp(0.0, 2.0) / 2.0;

    (w_roi * roi_norm) + (w_wr * wr_norm) + (w_pf * pf_norm) + (w_rr * rr_norm)
}

/// Walk all trades in chronological order, grouping by (condition_id, asset),
/// and match BUY/SELL lots FIFO. Each SELL event produces one "closed trade"
/// and a P&L (using per-share price). Returns (wins, total, gross_profit, gross_loss).
fn fifo_pnl_stats(trades: &[TradeRecord], since_ts: Option<i64>) -> (usize, usize, f64, f64) {
    #[derive(Default)]
    struct Lot {
        size: f64,
        price: f64,
    }

    let mut grouped: HashMap<(String, String), Vec<&TradeRecord>> = HashMap::new();
    for t in trades {
        let cid = match &t.condition_id {
            Some(v) if !v.trim().is_empty() => v.trim().to_string(),
            _ => continue,
        };
        let asset = match &t.asset {
            Some(v) if !v.trim().is_empty() => v.trim().to_string(),
            _ => continue,
        };
        grouped.entry((cid, asset)).or_default().push(t);
    }

    let mut wins = 0usize;
    let mut total = 0usize;
    let mut gross_profit = 0.0f64;
    let mut gross_loss = 0.0f64;

    for (_, mut items) in grouped {
        items.sort_by_key(|t| t.timestamp.unwrap_or(0));
        let mut inventory: Vec<Lot> = Vec::new();
        for t in items {
            let side = t.side.as_deref().unwrap_or("");
            let price = t.price.unwrap_or(0.0);
            let size = t.size.unwrap_or(0.0).abs();
            if size <= 0.0 {
                continue;
            }
            if side.eq_ignore_ascii_case("BUY") {
                inventory.push(Lot { size, price });
            } else if side.eq_ignore_ascii_case("SELL") {
                let mut remaining = size;
                let mut pnl = 0.0f64;
                let mut matched = false;
                while remaining > 0.0 && !inventory.is_empty() {
                    let lot = inventory.first_mut().expect("inventory not empty");
                    let take = remaining.min(lot.size);
                    pnl += (price - lot.price) * take;
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
                // Only count trades closed after since_ts (None = all time).
                if let Some(cutoff) = since_ts {
                    if t.timestamp.unwrap_or(0) < cutoff {
                        continue;
                    }
                }
                total += 1;
                if pnl > 0.0 {
                    wins += 1;
                    gross_profit += pnl;
                } else if pnl < 0.0 {
                    gross_loss += -pnl;
                }
            }
        }
    }

    (wins, total, gross_profit, gross_loss)
}

fn analyze_trader_full(
    trades: &[TradeRecord],
    analysis_hours: u64,
    min_avg_interval_secs: f64,
    max_trades_in_window: usize,
    min_interval_stddev: f64,
    max_active_hour_ratio: f64,
    recent_window_hours: u64,
) -> TraderAnalysis {
    let now = Utc::now().timestamp();
    let cutoff = now - (analysis_hours as i64 * 3600);

    let mut recent_ts: Vec<i64> = trades
        .iter()
        .filter_map(|t| t.timestamp)
        .filter(|&ts| ts >= cutoff)
        .collect();
    recent_ts.sort();

    let trade_count = recent_ts.len();
    if trade_count == 0 {
        return TraderAnalysis {
            avg_interval_secs: f64::MAX,
            trade_count: 0,
            interval_stddev: 0.0,
            active_hour_ratio: 0.0,
            win_rate: 0.0,
            profit_factor: 0.0,
            recent_roi: 0.0,
            is_bot: false,
        };
    }

    // --- frequency stats ---
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

    // --- FIFO-based win rate / profit factor over all available trades ---
    let (wins, total, gross_profit, gross_loss) = fifo_pnl_stats(trades, None);
    let win_rate = if total > 0 {
        wins as f64 / total as f64
    } else {
        0.0
    };
    let profit_factor = if gross_loss > 0.0 {
        gross_profit / gross_loss
    } else if gross_profit > 0.0 {
        10.0
    } else {
        0.0
    };

    // --- Recent ROI over a longer window (default 7 days) ---
    // Closed trades within [now - recent_window_hours, now] are counted.
    // Buys that have not yet been closed are conservatively ignored.
    let recent_cutoff = now - (recent_window_hours as i64 * 3600);
    let (_, _, recent_profit, recent_loss) = fifo_pnl_stats(trades, Some(recent_cutoff));
    let net_recent_pnl = recent_profit - recent_loss;
    let recent_gross_cost = recent_profit.max(0.0) + recent_loss.max(0.0);
    let recent_roi = if recent_gross_cost > 0.0 {
        net_recent_pnl / recent_gross_cost
    } else {
        0.0
    };

    let is_bot = (trade_count > max_trades_in_window)
        || (avg_interval_secs < min_avg_interval_secs)
        || ((interval_stddev < min_interval_stddev) && (trade_count > 20))
        || ((active_hour_ratio > max_active_hour_ratio) && (trade_count > 50));

    TraderAnalysis {
        avg_interval_secs,
        trade_count,
        interval_stddev,
        active_hour_ratio,
        win_rate,
        profit_factor,
        recent_roi,
        is_bot,
    }
}

#[allow(clippy::too_many_arguments)]
async fn discover_qualified_traders(
    http: &reqwest::Client,
    leaderboard_limit: u64,
    period: &str,
    min_pnl: f64,
    min_volume: f64,
    analysis_hours: u64,
    min_avg_interval_secs: f64,
    max_trades_in_window: usize,
    min_interval_stddev: f64,
    max_active_hour_ratio: f64,
    recent_window_hours: u64,
    w_roi: f64,
    w_wr: f64,
    w_pf: f64,
    w_rr: f64,
) -> Vec<QualifiedTrader> {
    let entries = match fetch_leaderboard(http, leaderboard_limit, period).await {
        Ok(e) => e,
        Err(err) => {
            eprintln!("plan_c: fetch_leaderboard failed: {err}");
            return Vec::new();
        }
    };

    println!(
        "plan_c: leaderboard returned {} entries (period={}, min_pnl={}, min_vol={})",
        entries.len(),
        period,
        min_pnl,
        min_volume
    );

    let mut candidates = Vec::new();
    let mut filtered_pnl = 0;
    let mut filtered_vol = 0;
    let mut filtered_bot = 0;
    let mut filtered_no_addr = 0;
    let mut analyzed = 0;

    for entry in &entries {
        let address = match &entry.proxy_wallet {
            Some(a) if !a.trim().is_empty() => a.trim().to_string(),
            _ => {
                filtered_no_addr += 1;
                continue;
            }
        };
        let pnl = entry.pnl.unwrap_or(0.0);
        let volume = entry.vol.unwrap_or(0.0);
        let user_name = entry
            .user_name
            .clone()
            .unwrap_or_else(|| address[..10.min(address.len())].to_string());

        if pnl < min_pnl {
            filtered_pnl += 1;
            continue;
        }
        if volume < min_volume {
            filtered_vol += 1;
            continue;
        }

        analyzed += 1;
        let trades = match fetch_user_trades(http, &address, 500).await {
            Ok(t) => t,
            Err(err) => {
                eprintln!("plan_c: fetch_trades failed for {address}: {err}");
                continue;
            }
        };

        let analysis = analyze_trader_full(
            &trades,
            analysis_hours,
            min_avg_interval_secs,
            max_trades_in_window,
            min_interval_stddev,
            max_active_hour_ratio,
            recent_window_hours,
        );

        if analysis.is_bot {
            filtered_bot += 1;
            println!(
                "plan_c: FILTERED (bot-like) {} (pnl={:.0} vol={:.0} trades={} avg_interval={:.0}s stddev={:.1} active_hr={:.2})",
                user_name,
                pnl,
                volume,
                analysis.trade_count,
                analysis.avg_interval_secs,
                analysis.interval_stddev,
                analysis.active_hour_ratio
            );
            continue;
        }

        let roi = if volume > 0.0 { pnl / volume } else { 0.0 };
        let score = compute_score(
            roi,
            analysis.win_rate,
            analysis.profit_factor,
            analysis.recent_roi,
            w_roi,
            w_wr,
            w_pf,
            w_rr,
        );
        let interval_display = if analysis.avg_interval_secs > 1e30 {
            "n/a".to_string()
        } else {
            format!("{:.0}s", analysis.avg_interval_secs)
        };
        println!(
            "plan_c: QUALIFIED {} (score={:.2} roi={:.2} wr={:.2} pf={:.2} recent_roi={:.2} pnl={:.0} vol={:.0} trades={} avg_interval={} stddev={:.1} active_hr={:.2})",
            user_name,
            score,
            roi,
            analysis.win_rate,
            analysis.profit_factor,
            analysis.recent_roi,
            pnl,
            volume,
            analysis.trade_count,
            interval_display,
            analysis.interval_stddev,
            analysis.active_hour_ratio
        );
        candidates.push(QualifiedTrader {
            address,
            user_name,
            pnl,
            volume,
            avg_interval_secs: analysis.avg_interval_secs,
            trade_count: analysis.trade_count,
            roi,
            win_rate: analysis.win_rate,
            profit_factor: analysis.profit_factor,
            recent_roi: analysis.recent_roi,
            score,
        });

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    candidates.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));

    println!(
        "plan_c: discovery complete — total={} no_addr={} low_pnl={} low_vol={} analyzed={} bot_like={} qualified={}",
        entries.len(),
        filtered_no_addr,
        filtered_pnl,
        filtered_vol,
        analyzed,
        filtered_bot,
        candidates.len()
    );

    candidates
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
    let min_pnl = read_f64_env_or(dotenv_path, "PLAN_C_MIN_PNL", 500.0);
    let min_volume = read_f64_env_or(dotenv_path, "PLAN_C_MIN_VOLUME", 5000.0);
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
    let w_roi = read_f64_env_or(dotenv_path, "PLAN_C_W_ROI", 0.30);
    let w_wr = read_f64_env_or(dotenv_path, "PLAN_C_W_WIN_RATE", 0.25);
    let w_pf = read_f64_env_or(dotenv_path, "PLAN_C_W_PROFIT_FACTOR", 0.20);
    let w_rr = read_f64_env_or(dotenv_path, "PLAN_C_W_RECENT_ROI", 0.25);
    let order_usd = read_decimal_env_or(dotenv_path, "PLAN_C_ORDER_USD", Decimal::from(5_u32));
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
    let buy_slippage = read_decimal_env_or(
        dotenv_path,
        "PLAN_C_BUY_SLIPPAGE",
        Decimal::from_str("0.02").unwrap_or(Decimal::ZERO),
    );
    let max_per_trader = read_u64_env_or(dotenv_path, "PLAN_C_MAX_PER_TRADER", 2) as usize;
    let refresh_leaders_secs = read_u64_env_or(dotenv_path, "PLAN_C_REFRESH_LEADERS_SECS", 3600);
    let exit_mode = parse_exit_mode(resolve_runtime_var("PLAN_C_EXIT_MODE", dotenv_path));
    let leader_sell_max_age_secs =
        read_u64_env_or(dotenv_path, "PLAN_C_LEADER_SELL_MAX_AGE_SECS", 300) as i64;

    println!("plan_c: leaderboard_limit={leaderboard_limit} period={period}");
    println!("plan_c: min_pnl={min_pnl} min_volume={min_volume}");
    println!("plan_c: analysis_hours={analysis_hours} min_avg_interval={min_avg_interval_secs}s max_trades_in_window={max_trades_in_window}");
    println!("plan_c: bot_filters min_interval_stddev={min_interval_stddev} max_active_hour_ratio={max_active_hour_ratio}");
    println!("plan_c: recent_window_hours={recent_window_hours} max_trade_age_secs={max_trade_age_secs}");
    println!("plan_c: score_weights w_roi={w_roi} w_win_rate={w_wr} w_profit_factor={w_pf} w_recent_roi={w_rr}");
    println!("plan_c: order_usd={order_usd} poll_secs={poll_secs}");
    println!("plan_c: stop_loss={stop_loss_pct} take_profit={take_profit_pct} max_positions={max_positions}");
    println!("plan_c: buy_slippage={buy_slippage} max_per_trader={max_per_trader} refresh_leaders={refresh_leaders_secs}s");
    println!(
        "plan_c: exit_mode={:?} leader_sell_max_age_secs={leader_sell_max_age_secs}",
        exit_mode
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
    let proxy_wallet = resolve_runtime_var("PROXY_WALLET", dotenv_path)
        .or_else(|| resolve_runtime_var("PM_FUNDER", dotenv_path))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    println!(
        "plan_c: state_path={} proxy_wallet={}",
        state_path.display(),
        proxy_wallet.as_deref().unwrap_or("(none)")
    );

    let persisted = load_plan_c_state(state_path);
    let mut open_positions: Vec<CopiedPosition> = persisted
        .positions
        .iter()
        .filter_map(PersistedPosition::to_position)
        .collect();
    let mut last_seen_trades: HashMap<String, i64> = persisted.last_seen_trades.clone();
    let loaded_daily_pnl = Decimal::from_str(persisted.daily_pnl.trim()).ok();
    let mut daily_pnl = loaded_daily_pnl.unwrap_or(context.risk.daily_pnl);
    println!(
        "plan_c: restored {} position(s), {} trader watermark(s), daily_pnl={}",
        open_positions.len(),
        last_seen_trades.len(),
        daily_pnl
    );

    // Reconcile with on-chain truth (if a wallet is configured).
    if let Some(wallet) = proxy_wallet.as_deref() {
        match fetch_onchain_positions(&http, wallet).await {
            Ok(onchain) => {
                println!(
                    "plan_c: fetched {} on-chain position(s) for reconciliation",
                    onchain.len()
                );
                open_positions = reconcile_positions(open_positions, &onchain);
                println!(
                    "plan_c: reconciled open_positions={} (of which {} are orphans with no trader metadata)",
                    open_positions.len(),
                    open_positions.iter().filter(|p| p.trader.is_empty()).count()
                );
            }
            Err(e) => {
                eprintln!(
                    "plan_c: on-chain reconciliation skipped (fetch failed): {e} — proceeding with cached state only"
                );
            }
        }
    } else {
        eprintln!(
            "plan_c: PROXY_WALLET/PM_FUNDER not configured — skipping on-chain reconciliation"
        );
    }

    // Persist the reconciled state so the file reflects reality on next restart.
    save_plan_c_state(state_path, &open_positions, &last_seen_trades, daily_pnl);

    let mut qualified_traders: Vec<QualifiedTrader> = Vec::new();
    let mut last_leader_refresh: i64 = 0;
    let round_log_path = Path::new("data/plan_c_rounds.jsonl");

    let mut loop_idx: u64 = 0;
    loop {
        loop_idx += 1;
        let now = Utc::now().timestamp();

        if now - last_leader_refresh >= refresh_leaders_secs as i64 || qualified_traders.is_empty()
        {
            println!(
                "\n================ plan_c round #{loop_idx} — refreshing leaders ================\n"
            );
            qualified_traders = discover_qualified_traders(
                &http,
                leaderboard_limit,
                &period,
                min_pnl,
                min_volume,
                analysis_hours,
                min_avg_interval_secs,
                max_trades_in_window,
                min_interval_stddev,
                max_active_hour_ratio,
                recent_window_hours,
                w_roi,
                w_wr,
                w_pf,
                w_rr,
            )
            .await;
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

        let mut closed_indices = Vec::new();
        for (idx, pos) in open_positions.iter().enumerate() {
            let current_bid = match fetch_clob_best_bid(&pos.token_id).await {
                Ok(p) => p,
                Err(_) => continue,
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
                    let signal = poly2::StrategySignal {
                        strategy_id: StrategyId::ProbabilityTrading,
                        market_id: pos.market_id.clone(),
                        yes_token_id: Some(pos.token_id.clone()),
                        no_token_id: None,
                        actions: vec![poly2::OrderIntent {
                            side: pos.side.clone(),
                            price: sell_price,
                            size: pos.size,
                            sell: true,
                            gtd_expiration: None,
                        }],
                        state: poly2::StrategyState::Implemented,
                    };
                    match execution.submit(&signal).await {
                        Ok(report) => {
                            print_execution_report(&report);
                            let round_pnl = (current_bid - pos.entry_price) * pos.size;
                            daily_pnl += round_pnl;
                            append_round_record(
                                round_log_path,
                                loop_idx,
                                &pos.market_id,
                                format!("copy_{}", pos.trader),
                                pos.entry_price,
                                current_bid,
                                pos.size,
                                round_pnl,
                                "leader_sell",
                            );
                        }
                        Err(e) => eprintln!("plan_c: sell (leader exit) failed: {e}"),
                    }
                    closed_indices.push(idx);
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
                let signal = poly2::StrategySignal {
                    strategy_id: StrategyId::ProbabilityTrading,
                    market_id: pos.market_id.clone(),
                    yes_token_id: Some(pos.token_id.clone()),
                    no_token_id: None,
                    actions: vec![poly2::OrderIntent {
                        side: pos.side.clone(),
                        price: sell_price,
                        size: pos.size,
                        sell: true,
                        gtd_expiration: None,
                    }],
                    state: poly2::StrategyState::Implemented,
                };
                match execution.submit(&signal).await {
                    Ok(report) => {
                        print_execution_report(&report);
                        let round_pnl = (current_bid - pos.entry_price) * pos.size;
                        daily_pnl += round_pnl;
                        append_round_record(
                            round_log_path, loop_idx, &pos.market_id,
                            format!("copy_{}", pos.trader),
                            pos.entry_price, current_bid, pos.size, round_pnl, "stop_loss",
                        );
                    }
                    Err(e) => eprintln!("plan_c: sell (stop loss) failed: {e}"),
                }
                closed_indices.push(idx);
            } else if local_safety_active && pnl_pct >= take_profit_pct {
                println!(
                    "plan_c: TAKE PROFIT — market={} bid={} entry={} pnl={:.2}%",
                    pos.market_id, current_bid, pos.entry_price,
                    pnl_pct * Decimal::from(100_u32)
                );

                let sell_price = (current_bid - buy_slippage).max(Decimal::new(1, 2)).round_dp(2);
                let signal = poly2::StrategySignal {
                    strategy_id: StrategyId::ProbabilityTrading,
                    market_id: pos.market_id.clone(),
                    yes_token_id: Some(pos.token_id.clone()),
                    no_token_id: None,
                    actions: vec![poly2::OrderIntent {
                        side: pos.side.clone(),
                        price: sell_price,
                        size: pos.size,
                        sell: true,
                        gtd_expiration: None,
                    }],
                    state: poly2::StrategyState::Implemented,
                };
                match execution.submit(&signal).await {
                    Ok(report) => {
                        print_execution_report(&report);
                        let round_pnl = (current_bid - pos.entry_price) * pos.size;
                        daily_pnl += round_pnl;
                        append_round_record(
                            round_log_path, loop_idx, &pos.market_id,
                            format!("copy_{}", pos.trader),
                            pos.entry_price, current_bid, pos.size, round_pnl, "take_profit",
                        );
                    }
                    Err(e) => eprintln!("plan_c: sell (take profit) failed: {e}"),
                }
                closed_indices.push(idx);
            }
        }
        let any_closed = !closed_indices.is_empty();
        closed_indices.sort_unstable_by(|a, b| b.cmp(a));
        for idx in closed_indices {
            open_positions.remove(idx);
        }
        if any_closed {
            save_plan_c_state(state_path, &open_positions, &last_seen_trades, daily_pnl);
        }

        if open_positions.len() < max_positions {
            // Step 1: collect pending BUY signals. DO NOT advance last_seen here —
            // we only advance it per-trader once a signal has actually been decided on.
            let collection_now = Utc::now().timestamp();
            let stale_cutoff = collection_now - max_trade_age_secs;
            let mut pending: Vec<(f64, usize, TradeRecord)> = Vec::new();
            // Per-trader max ts we will consider advancing last_seen to, populated as
            // we decide on each pending signal below.
            let mut advance_ts: HashMap<String, i64> = HashMap::new();

            for (idx, trader) in qualified_traders.iter().enumerate() {
                let last_ts = last_seen_trades.get(&trader.address).copied().unwrap_or(0);
                let Some(trades) = trades_map.get(&trader.address) else {
                    continue;
                };

                for t in trades.iter() {
                    let ts = t.timestamp.unwrap_or(0);
                    let is_buy = t
                        .side
                        .as_deref()
                        .map(|s| s.eq_ignore_ascii_case("BUY"))
                        .unwrap_or(false);
                    if ts <= last_ts || !is_buy {
                        continue;
                    }
                    if ts < stale_cutoff {
                        // Too old — discard and advance the watermark so we don't
                        // keep seeing it next round.
                        let e = advance_ts.entry(trader.address.clone()).or_insert(0);
                        if ts > *e {
                            *e = ts;
                        }
                        continue;
                    }
                    pending.push((trader.score, idx, t.clone()));
                }
            }

            // Step 2: prioritise by trader score (descending). Ties preserve insertion
            // order which roughly follows leaderboard order from step 1.
            pending.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

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

                if open_positions.len() >= max_positions {
                    // Capacity-bound skip: leave watermark untouched so we re-pick
                    // it up next round (if it hasn't aged out).
                    continue;
                }
                let per_trader_now = open_positions
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

                if open_positions
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
                let size = (order_usd / buy_price).round_dp(2);
                if size < Decimal::from(1_u32) {
                    mark_processed(&trader.address, trade_ts);
                    continue;
                }

                let trade_title = trade.title.as_deref().unwrap_or(&market_id);
                let trade_outcome = trade.outcome.as_deref().unwrap_or("?");
                let age_secs = (collection_now - trade_ts).max(0);

                println!(
                    "plan_c: COPY BUY — trader={} (score={:.2} roi={:.2} wr={:.2} pf={:.2} recent_roi={:.2}) market=\"{}\" outcome={} price={} size={} age={}s (leader pnl={:.0})",
                    trader.user_name,
                    score,
                    trader.roi,
                    trader.win_rate,
                    trader.profit_factor,
                    trader.recent_roi,
                    trade_title,
                    trade_outcome,
                    buy_price,
                    size,
                    age_secs,
                    trader.pnl
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
                        if matches!(
                            report.status,
                            ExecutionStatus::Filled | ExecutionStatus::PartiallyFilled
                        ) {
                            let fill_size: Decimal = report
                                .fills
                                .iter()
                                .filter(|f| !f.sell)
                                .map(|f| f.size)
                                .sum();
                            let actual_size = if fill_size > Decimal::ZERO {
                                (fill_size * Decimal::new(97, 2)).round_dp(2)
                            } else {
                                size
                            };
                            open_positions.push(CopiedPosition {
                                trader: trader.address.clone(),
                                market_id: market_id.clone(),
                                token_id: token_id.clone(),
                                side: Side::Yes,
                                entry_price: buy_price,
                                size: actual_size,
                                opened_at: Utc::now().timestamp(),
                                leader_buy_ts: trade_ts,
                            });
                            println!("plan_c: position opened — {} open now", open_positions.len());
                            save_plan_c_state(
                                state_path,
                                &open_positions,
                                &last_seen_trades,
                                daily_pnl,
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
        save_plan_c_state(state_path, &open_positions, &last_seen_trades, daily_pnl);

        println!(
            "[{}] plan_c: poll #{loop_idx} done — positions={}/{} daily_pnl={} tracking={} traders — sleeping {}s",
            chrono::Local::now().format("%m/%d %H:%M:%S"),
            open_positions.len(),
            max_positions,
            daily_pnl,
            qualified_traders.len(),
            poll_secs
        );
        time::sleep(Duration::from_secs(poll_secs)).await;
    }
}
