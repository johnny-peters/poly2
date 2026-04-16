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
    read_u64_env_or, resolve_runtime_var,
};

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
        let batch = clamped_limit.min(limit - offset);
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

fn analyze_trader(
    trades: &[TradeRecord],
    analysis_hours: u64,
    min_avg_interval_secs: f64,
    max_trades_in_window: usize,
) -> Option<(f64, usize)> {
    let now = Utc::now().timestamp();
    let cutoff = now - (analysis_hours as i64 * 3600);

    let mut recent: Vec<i64> = trades
        .iter()
        .filter_map(|t| t.timestamp)
        .filter(|&ts| ts >= cutoff)
        .collect();
    recent.sort();

    if recent.len() < 2 {
        return Some((f64::MAX, recent.len()));
    }

    if recent.len() > max_trades_in_window {
        return None;
    }

    let intervals: Vec<f64> = recent
        .windows(2)
        .map(|w| (w[1] - w[0]) as f64)
        .collect();
    let avg_interval = intervals.iter().sum::<f64>() / intervals.len() as f64;

    if avg_interval < min_avg_interval_secs {
        return None;
    }

    Some((avg_interval, recent.len()))
}

async fn discover_qualified_traders(
    http: &reqwest::Client,
    leaderboard_limit: u64,
    period: &str,
    min_pnl: f64,
    min_volume: f64,
    analysis_hours: u64,
    min_avg_interval_secs: f64,
    max_trades_in_window: usize,
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
    let mut filtered_hf = 0;
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

        match analyze_trader(
            &trades,
            analysis_hours,
            min_avg_interval_secs,
            max_trades_in_window,
        ) {
            Some((avg_interval, trade_count)) => {
                let interval_display = if avg_interval > 1e30 { "n/a".to_string() } else { format!("{:.0}s", avg_interval) };
                println!(
                    "plan_c: QUALIFIED {} (pnl={:.0} vol={:.0} trades={} avg_interval={})",
                    user_name, pnl, volume, trade_count, interval_display
                );
                candidates.push(QualifiedTrader {
                    address,
                    user_name,
                    pnl,
                    volume,
                    avg_interval_secs: avg_interval,
                    trade_count,
                });
            }
            None => {
                filtered_hf += 1;
                println!(
                    "plan_c: FILTERED (high-freq bot) {} (pnl={:.0} vol={:.0})",
                    user_name, pnl, volume
                );
            }
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    candidates.sort_by(|a, b| b.pnl.partial_cmp(&a.pnl).unwrap_or(std::cmp::Ordering::Equal));

    println!(
        "plan_c: discovery complete — total={} no_addr={} low_pnl={} low_vol={} analyzed={} high_freq={} qualified={}",
        entries.len(),
        filtered_no_addr,
        filtered_pnl,
        filtered_vol,
        analyzed,
        filtered_hf,
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
    let min_pnl = read_decimal_env_or(dotenv_path, "PLAN_C_MIN_PNL", Decimal::from(500_u32));
    let min_volume = read_decimal_env_or(dotenv_path, "PLAN_C_MIN_VOLUME", Decimal::from(5000_u32));
    let analysis_hours = read_u64_env_or(dotenv_path, "PLAN_C_ANALYSIS_HOURS", 24);
    let min_avg_interval_secs = read_decimal_env_or(
        dotenv_path,
        "PLAN_C_MIN_AVG_INTERVAL_SECS",
        Decimal::from(120_u32),
    );
    let max_trades_in_window =
        read_u64_env_or(dotenv_path, "PLAN_C_MAX_TRADES_IN_WINDOW", 200) as usize;
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

    println!("plan_c: leaderboard_limit={leaderboard_limit} period={period}");
    println!("plan_c: min_pnl={min_pnl} min_volume={min_volume}");
    println!("plan_c: analysis_hours={analysis_hours} min_avg_interval={min_avg_interval_secs}s max_trades_in_window={max_trades_in_window}");
    println!("plan_c: order_usd={order_usd} poll_secs={poll_secs}");
    println!("plan_c: stop_loss={stop_loss_pct} take_profit={take_profit_pct} max_positions={max_positions}");
    println!("plan_c: buy_slippage={buy_slippage} max_per_trader={max_per_trader} refresh_leaders={refresh_leaders_secs}s");

    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(30))
        .build()
        .expect("plan_c: http client build failed");

    let mut qualified_traders: Vec<QualifiedTrader> = Vec::new();
    let mut last_leader_refresh: i64 = 0;
    let mut open_positions: Vec<CopiedPosition> = Vec::new();
    let mut daily_pnl = context.risk.daily_pnl;
    let mut last_seen_trades: HashMap<String, i64> = HashMap::new();
    let round_log_path = Path::new("data/plan_c_rounds.jsonl");

    let min_pnl_f64 = min_pnl.to_string().parse::<f64>().unwrap_or(500.0);
    let min_volume_f64 = min_volume.to_string().parse::<f64>().unwrap_or(5000.0);
    let min_avg_interval_f64 = min_avg_interval_secs
        .to_string()
        .parse::<f64>()
        .unwrap_or(120.0);

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
                min_pnl_f64,
                min_volume_f64,
                analysis_hours,
                min_avg_interval_f64,
                max_trades_in_window,
            )
            .await;
            last_leader_refresh = now;

            if qualified_traders.is_empty() {
                println!("plan_c: no qualified traders found, retrying in 5 minutes");
                time::sleep(Duration::from_secs(300)).await;
                continue;
            }

            for trader in &qualified_traders {
                if !last_seen_trades.contains_key(&trader.address) {
                    let latest_ts = match fetch_user_trades(&http, &trader.address, 1).await {
                        Ok(t) => t.first().and_then(|t| t.timestamp).unwrap_or(now),
                        Err(_) => now,
                    };
                    last_seen_trades.insert(trader.address.clone(), latest_ts);
                }
            }
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

            if pnl_pct <= -stop_loss_pct {
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
            } else if pnl_pct >= take_profit_pct {
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
        closed_indices.sort_unstable_by(|a, b| b.cmp(a));
        for idx in closed_indices {
            open_positions.remove(idx);
        }

        if open_positions.len() < max_positions {
            for trader in &qualified_traders {
                if open_positions.len() >= max_positions {
                    break;
                }

                let per_trader_count = open_positions
                    .iter()
                    .filter(|p| p.trader == trader.address)
                    .count();
                if per_trader_count >= max_per_trader {
                    continue;
                }

                let last_ts = last_seen_trades
                    .get(&trader.address)
                    .copied()
                    .unwrap_or(0);
                let trades = match fetch_user_trades(&http, &trader.address, 20).await {
                    Ok(t) => t,
                    Err(err) => {
                        eprintln!("plan_c: fetch_trades failed for {}: {err}", trader.user_name);
                        continue;
                    }
                };

                let new_buys: Vec<&TradeRecord> = trades
                    .iter()
                    .filter(|t| {
                        let ts = t.timestamp.unwrap_or(0);
                        let is_buy = t
                            .side
                            .as_deref()
                            .map(|s| s.eq_ignore_ascii_case("BUY"))
                            .unwrap_or(false);
                        ts > last_ts && is_buy
                    })
                    .collect();

                if let Some(latest_ts) = trades.first().and_then(|t| t.timestamp) {
                    if latest_ts > last_ts {
                        last_seen_trades.insert(trader.address.clone(), latest_ts);
                    }
                }

                for trade in new_buys {
                    if open_positions.len() >= max_positions {
                        break;
                    }
                    let per_trader_now = open_positions
                        .iter()
                        .filter(|p| p.trader == trader.address)
                        .count();
                    if per_trader_now >= max_per_trader {
                        break;
                    }

                    let token_id = match &trade.asset {
                        Some(a) if !a.trim().is_empty() => a.trim().to_string(),
                        _ => continue,
                    };
                    let market_id = match &trade.condition_id {
                        Some(c) if !c.trim().is_empty() => c.trim().to_string(),
                        _ => continue,
                    };
                    let trade_price = Decimal::from_str(
                        &trade.price.unwrap_or(0.5).to_string(),
                    )
                    .unwrap_or(Decimal::new(50, 2));

                    if open_positions
                        .iter()
                        .any(|p| p.market_id == market_id && p.token_id == token_id)
                    {
                        continue;
                    }

                    let buy_price = (trade_price + buy_slippage).round_dp(2);
                    if buy_price >= Decimal::ONE || buy_price <= Decimal::ZERO {
                        continue;
                    }
                    let size = (order_usd / buy_price).round_dp(2);
                    if size < Decimal::from(1_u32) {
                        continue;
                    }

                    let trade_title = trade.title.as_deref().unwrap_or(&market_id);
                    let trade_outcome = trade.outcome.as_deref().unwrap_or("?");

                    println!(
                        "plan_c: COPY BUY — trader={} market=\"{}\" outcome={} price={} size={} (leader pnl={:.0})",
                        trader.user_name, trade_title, trade_outcome, buy_price, size, trader.pnl
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
                        continue;
                    }

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
                                });
                                println!(
                                    "plan_c: position opened — {} open now",
                                    open_positions.len()
                                );
                            }
                        }
                        Err(e) => {
                            eprintln!("plan_c: buy order failed: {e}");
                        }
                    }
                }

                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }

        println!(
            "plan_c: poll #{loop_idx} done — positions={}/{} daily_pnl={} tracking={} traders — sleeping {}s",
            open_positions.len(),
            max_positions,
            daily_pnl,
            qualified_traders.len(),
            poll_secs
        );
        time::sleep(Duration::from_secs(poll_secs)).await;
    }
}
