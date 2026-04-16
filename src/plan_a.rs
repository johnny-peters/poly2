use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::path::Path;
use std::str::FromStr;
use tokio::time::{self, Duration};

use poly2::{
    discover_btc_candle_market, ExecutionClient, ExecutionStatus, RiskEngine, Side,
    StrategyContext, StrategyId,
};

use crate::{
    append_round_record, fetch_clob_best_bid, print_execution_report, read_decimal_env_or,
    read_u64_env_or, spawn_ws_price_feed, WsPriceCache,
};

const BTC_CANDLE_SELL_POLL_MS: u64 = 1000;
const BTC_CANDLE_SELL_POLL_SECS: i64 = 5;
const BTC_CANDLE_MAX_SELL_ATTEMPTS: u32 = 3;

#[derive(Debug)]
pub(crate) struct BtcCandleSellResult {
    pub exit_price: Decimal,
    pub matched_shares: Decimal,
    pub fully_exited: bool,
}

fn btc_candle_exit_signal(
    market_id: &str,
    up_token_id: &str,
    down_token_id: &str,
    exit_side: Side,
    price: Decimal,
    size: Decimal,
) -> poly2::StrategySignal {
    poly2::StrategySignal {
        strategy_id: StrategyId::ProbabilityTrading,
        market_id: market_id.to_string(),
        yes_token_id: Some(up_token_id.to_string()),
        no_token_id: Some(down_token_id.to_string()),
        actions: vec![poly2::OrderIntent {
            side: exit_side,
            price,
            size,
            sell: true,
            gtd_expiration: None,
        }],
        state: poly2::StrategyState::Implemented,
    }
}

pub(crate) async fn execute_sell_with_retry<C: ExecutionClient + ?Sized>(
    execution: &C,
    ws_cache: &WsPriceCache,
    market_id: &str,
    up_token_id: &str,
    down_token_id: &str,
    exit_side: Side,
    bought_token_id: &str,
    mut remaining: Decimal,
    fallback_ask: Decimal,
) -> BtcCandleSellResult {
    let min_tick = Decimal::new(1, 2);
    let mut total_matched = Decimal::ZERO;
    let mut total_quote = Decimal::ZERO;

    for attempt in 0..BTC_CANDLE_MAX_SELL_ATTEMPTS {
        if remaining <= Decimal::ZERO {
            break;
        }

        let step = min_tick * Decimal::from(attempt);
        let mut base = ws_cache.bid_for(bought_token_id).await;
        if base <= Decimal::ZERO {
            if let Ok(b) = fetch_clob_best_bid(bought_token_id).await {
                base = b;
            }
        }
        if base <= Decimal::ZERO {
            base = fallback_ask;
        }
        let sell_price = (base - step).max(min_tick).round_dp(2);

        let signal = btc_candle_exit_signal(
            market_id,
            up_token_id,
            down_token_id,
            exit_side.clone(),
            sell_price,
            remaining.round_dp(2),
        );

        let report = match execution.submit(&signal).await {
            Ok(r) => r,
            Err(e) => {
                let err_msg = e.to_string();
                eprintln!(
                    "btc_candle: sell submit failed (attempt {}/{}): {err_msg}",
                    attempt + 1,
                    BTC_CANDLE_MAX_SELL_ATTEMPTS
                );
                if err_msg.contains("not enough balance") {
                    if let Some(bal_str) = err_msg
                        .split("balance: ")
                        .nth(1)
                        .and_then(|s| s.split(|c: char| !c.is_ascii_digit()).next())
                    {
                        if let Ok(bal_raw) = bal_str.parse::<u64>() {
                            let actual = Decimal::new(bal_raw as i64, 6);
                            if actual > Decimal::ZERO && actual < remaining {
                                println!(
                                    "btc_candle: balance={} < remaining={}, adjusting sell size",
                                    actual, remaining
                                );
                                remaining = (actual * Decimal::new(99, 2)).round_dp(2);
                            }
                        }
                    }
                    if remaining <= Decimal::ZERO {
                        break;
                    }
                }
                continue;
            }
        };
        print_execution_report(&report);

        let mut matched_cum = Decimal::ZERO;
        let mut st = report.status.clone();

        let fill_from_report: Decimal = report
            .fills
            .iter()
            .filter(|f| f.sell)
            .map(|f| f.size)
            .sum();

        if let Some(oid) = report.order_ids.first() {
            let (s0, m0) = execution
                .get_order_status(oid)
                .await
                .unwrap_or((report.status.clone(), Decimal::ZERO));
            st = s0;
            matched_cum = m0;
        } else if matches!(report.status, ExecutionStatus::Filled) && fill_from_report > Decimal::ZERO {
            matched_cum = fill_from_report;
            st = ExecutionStatus::Filled;
        } else if matches!(report.status, ExecutionStatus::Filled) {
            matched_cum = remaining;
            st = ExecutionStatus::Filled;
        }

        if fill_from_report > matched_cum {
            matched_cum = fill_from_report;
        }

        if let Some(oid) = report.order_ids.first() {
            if matches!(
                st,
                ExecutionStatus::Pending | ExecutionStatus::PartiallyFilled
            ) {
                let poll_deadline = Utc::now().timestamp() + BTC_CANDLE_SELL_POLL_SECS;
                loop {
                    time::sleep(Duration::from_millis(BTC_CANDLE_SELL_POLL_MS)).await;
                    match execution.get_order_status(oid).await {
                        Ok((s, m)) => {
                            st = s;
                            matched_cum = m;
                            if matches!(st, ExecutionStatus::Filled) {
                                break;
                            }
                        }
                        Err(e) => eprintln!("btc_candle: sell poll error: {e}"),
                    }
                    if Utc::now().timestamp() >= poll_deadline {
                        break;
                    }
                }
            }

            if matches!(
                st,
                ExecutionStatus::Pending | ExecutionStatus::PartiallyFilled
            ) {
                match execution.cancel_order(oid).await {
                    Ok(()) => {
                        if let Ok((_, m_after)) = execution.get_order_status(oid).await {
                            if m_after > matched_cum {
                                matched_cum = m_after;
                            }
                        }
                    }
                    Err(e) => eprintln!("btc_candle: sell cancel failed: {e}"),
                }
            }
        }

        let filled = matched_cum.min(remaining).max(Decimal::ZERO);
        if filled > Decimal::ZERO {
            total_matched += filled;
            total_quote += filled * sell_price;
            remaining -= filled;
        }

        if remaining <= Decimal::new(1, 2) {
            remaining = Decimal::ZERO;
            break;
        }
    }

    let exit_price = if total_matched > Decimal::ZERO {
        total_quote / total_matched
    } else {
        Decimal::ZERO
    };
    let fully_exited = remaining <= Decimal::new(1, 2);

    BtcCandleSellResult {
        exit_price,
        matched_shares: total_matched,
        fully_exited,
    }
}

pub(crate) async fn run_btc_candle_loop<C>(
    _interval_secs: u64,
    context: &StrategyContext,
    risk_engine: &RiskEngine,
    execution: &C,
    dotenv_path: &Path,
) where
    C: ExecutionClient,
{
    let base_order_usd = read_decimal_env_or(dotenv_path, "BTC_CANDLE_ORDER_USD", Decimal::from(2_u32));
    let entry_threshold = read_decimal_env_or(
        dotenv_path,
        "BTC_CANDLE_ENTRY_THRESHOLD",
        Decimal::from_str("0.72").unwrap_or(Decimal::from(72_u32) / Decimal::from(100_u32)),
    );
    let base_stop_loss = read_decimal_env_or(
        dotenv_path,
        "BTC_CANDLE_STOP_LOSS",
        Decimal::from_str("0.50").unwrap_or(Decimal::from(1_u32) / Decimal::from(2_u32)),
    );
    let take_profit_price = read_decimal_env_or(
        dotenv_path,
        "BTC_CANDLE_TAKE_PROFIT",
        Decimal::from_str("0.84").unwrap_or(Decimal::from(84_u32) / Decimal::from(100_u32)),
    );
    let poll_ms = read_u64_env_or(dotenv_path, "BTC_CANDLE_POLL_MS", 1000);
    let max_entry_sum = read_decimal_env_or(
        dotenv_path,
        "BTC_CANDLE_MAX_ENTRY_SUM",
        Decimal::from_str("1.02").unwrap_or(Decimal::ONE),
    );
    let max_spread = read_decimal_env_or(
        dotenv_path,
        "BTC_CANDLE_MAX_SPREAD",
        Decimal::from_str("0.10").unwrap_or(Decimal::ONE),
    );
    let early_exit_secs: i64 = read_u64_env_or(dotenv_path, "BTC_CANDLE_EARLY_EXIT_SECS", 45) as i64;
    let near_certain = read_decimal_env_or(
        dotenv_path,
        "BTC_CANDLE_NEAR_CERTAIN",
        Decimal::from_str("0.95").unwrap_or(Decimal::ONE),
    );
    let max_consec_losses = read_u64_env_or(dotenv_path, "BTC_CANDLE_MAX_CONSEC_LOSSES", 3);
    let loss_cooldown_secs = read_u64_env_or(dotenv_path, "BTC_CANDLE_LOSS_COOLDOWN_SECS", 600);
    let signal_boost_threshold = read_decimal_env_or(
        dotenv_path,
        "BTC_CANDLE_SIGNAL_BOOST_THRESHOLD",
        Decimal::from_str("0.80").unwrap_or(Decimal::ONE),
    );
    let signal_boost_multiplier = read_decimal_env_or(
        dotenv_path,
        "BTC_CANDLE_SIGNAL_BOOST_MULTIPLIER",
        Decimal::from_str("1.5").unwrap_or(Decimal::ONE),
    );
    let buy_slippage = read_decimal_env_or(
        dotenv_path,
        "BTC_CANDLE_BUY_SLIPPAGE",
        Decimal::from_str("0.02").unwrap_or(Decimal::ZERO),
    );
    let exit_max_ask_sum = read_decimal_env_or(
        dotenv_path,
        "BTC_CANDLE_EXIT_MAX_ASK_SUM",
        Decimal::from_str("1.50").unwrap_or(Decimal::from(2_u32)),
    );
    let exit_max_spread = read_decimal_env_or(
        dotenv_path,
        "BTC_CANDLE_EXIT_MAX_SPREAD",
        Decimal::from_str("0.30").unwrap_or(Decimal::ONE),
    );
    let exit_max_bid_jump = read_decimal_env_or(
        dotenv_path,
        "BTC_CANDLE_EXIT_MAX_BID_JUMP",
        Decimal::from_str("0.25").unwrap_or(Decimal::ONE),
    );

    println!(
        "btc_candle: base_order_usd={}, entry_threshold={}, base_stop_loss={}, take_profit={}, poll_ms={}",
        base_order_usd, entry_threshold, base_stop_loss, take_profit_price, poll_ms
    );
    println!(
        "btc_candle: max_entry_sum={}, max_spread={}, early_exit_secs={}, near_certain={}",
        max_entry_sum, max_spread, early_exit_secs, near_certain
    );
    println!(
        "btc_candle: max_consec_losses={}, loss_cooldown_secs={}, signal_boost_threshold={}, signal_boost_multiplier={}",
        max_consec_losses, loss_cooldown_secs, signal_boost_threshold, signal_boost_multiplier
    );

    let mut daily_pnl = context.risk.daily_pnl;
    let mut consecutive_losses: u64 = 0;
    let mut total_rounds: u64 = 0;
    let mut winning_rounds: u64 = 0;
    let round_log_path = Path::new("data/btc_candle_rounds.jsonl");

    let mut loop_idx: u64 = 0;
    let mut first_round = true;
    loop {
        loop_idx += 1;

        let now_secs = Utc::now().timestamp();
        let current_window = now_secs - (now_secs.rem_euclid(300));

        let (window_ts, close_ts) = if first_round {
            first_round = false;
            println!(
                "\n================ btc5m round #{} (immediate start) ================\n\
                 now={} current_window_ts={}",
                loop_idx,
                Utc::now().to_rfc3339(),
                current_window,
            );
            (current_window, current_window + 300)
        } else {
            let next_window = current_window + 300;
            let sleep_to_boundary = (next_window - now_secs).max(0) as u64;
            println!(
                "\n================ btc5m round #{} waiting for window ================\n\
                 now={} next_window_ts={} sleeping={}s",
                loop_idx,
                Utc::now().to_rfc3339(),
                next_window,
                sleep_to_boundary
            );
            if sleep_to_boundary > 0 {
                time::sleep(Duration::from_secs(sleep_to_boundary)).await;
            }
            (next_window, next_window + 300)
        };

        let market = match discover_btc_candle_market(window_ts).await {
            Ok(m) => m,
            Err(err) => {
                eprintln!("btc_candle: market discovery failed: {err}");
                time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };
        println!(
            "btc_candle: market_id={} slug={} up_token={} down_token={}",
            market.market_id, market.market_slug, market.up_token_id, market.down_token_id
        );

        if consecutive_losses >= max_consec_losses {
            println!(
                "btc_candle: {} consecutive losses >= max {}, cooling down {}s",
                consecutive_losses, max_consec_losses, loss_cooldown_secs
            );
            time::sleep(Duration::from_secs(loss_cooldown_secs)).await;
            consecutive_losses = 0;
            continue;
        }

        let ws_cache = WsPriceCache::new(&market.up_token_id, &market.down_token_id);
        let (ws_cancel_tx, ws_cancel_rx) = tokio::sync::watch::channel(false);
        let ws_handle = spawn_ws_price_feed(ws_cache.clone(), ws_cancel_rx);
        time::sleep(Duration::from_millis(1500)).await;

        let mut bought_side: Option<poly2::Side> = None;
        let mut bought_token_id = String::new();
        let mut bought_size = Decimal::ZERO;
        let mut entry_price = Decimal::ZERO;

        let mut entry_skip_remaining: i32 = {
            let nanos = Utc::now().timestamp_subsec_nanos() as u64;
            let pid = std::process::id() as u64;
            let mut h = nanos.wrapping_mul(6364136223846793005).wrapping_add(pid);
            h ^= h >> 17;
            (h % 3) as i32
        };
        let mut last_skip_at: Option<tokio::time::Instant> = None;
        println!("btc_candle: entry_skip_remaining={} (will skip first {} triggers)", entry_skip_remaining, entry_skip_remaining);

        'entry: loop {
            let now = Utc::now().timestamp();
            if now >= close_ts - 10 {
                println!("btc_candle: window closing soon, no entry this round");
                break 'entry;
            }

            let up_val = ws_cache.up_ask().await;
            let down_val = ws_cache.down_ask().await;

            println!(
                "btc_candle poll: up_ask={} down_ask={} sum={} threshold={} remaining={}s",
                up_val, down_val, up_val + down_val, entry_threshold, close_ts - now
            );

            let (trigger_side, trigger_price, trigger_token) =
                if up_val >= entry_threshold && up_val >= down_val {
                    (poly2::Side::Yes, up_val, market.up_token_id.clone())
                } else if down_val >= entry_threshold {
                    (poly2::Side::No, down_val, market.down_token_id.clone())
                } else {
                    time::sleep(Duration::from_millis(poll_ms)).await;
                    continue 'entry;
                };

            if entry_skip_remaining > 0 {
                entry_skip_remaining -= 1;
                last_skip_at = Some(tokio::time::Instant::now());
                println!(
                    "btc_candle: SKIP entry signal (side={:?} ask={}), remaining skips={}",
                    trigger_side, trigger_price, entry_skip_remaining
                );
                time::sleep(Duration::from_millis(poll_ms)).await;
                continue 'entry;
            }

            if let Some(skipped_at) = last_skip_at {
                let elapsed = skipped_at.elapsed();
                let cooldown = Duration::from_secs(10);
                if elapsed < cooldown {
                    let wait = cooldown - elapsed;
                    println!(
                        "btc_candle: post-skip cooldown, waiting {:.1}s before entry",
                        wait.as_secs_f64()
                    );
                    time::sleep(wait).await;
                }
                last_skip_at = None;
            }

            if trigger_price <= Decimal::ZERO {
                time::sleep(Duration::from_millis(poll_ms)).await;
                continue 'entry;
            }

            let ask_sum = up_val + down_val;
            if ask_sum > max_entry_sum {
                println!(
                    "btc_candle: SKIP — ask sum {} > max_entry_sum {}, market charging premium",
                    ask_sum, max_entry_sum
                );
                time::sleep(Duration::from_millis(poll_ms)).await;
                continue 'entry;
            }

            let trigger_bid = ws_cache.bid_for(&trigger_token).await;
            let spread = trigger_price - trigger_bid;
            if trigger_bid > Decimal::ZERO && spread > max_spread {
                println!(
                    "btc_candle: SKIP — bid-ask spread {} (ask={} bid={}) > max_spread {}",
                    spread, trigger_price, trigger_bid, max_spread
                );
                time::sleep(Duration::from_millis(poll_ms)).await;
                continue 'entry;
            }

            let buy_price = (trigger_price + buy_slippage).round_dp(2);
            let buy_cap = (entry_threshold + max_spread).round_dp(2);

            if buy_price > buy_cap {
                println!(
                    "btc_candle: ask {} + slippage {} = {} > cap {} (threshold {} + spread {}), waiting for pullback",
                    trigger_price, buy_slippage, buy_price, buy_cap, entry_threshold, max_spread
                );
                time::sleep(Duration::from_millis(poll_ms)).await;
                continue 'entry;
            }

            let order_usd = if trigger_price >= signal_boost_threshold {
                let boosted = (base_order_usd * signal_boost_multiplier).round_dp(2);
                println!("btc_candle: strong signal ({} >= {}), boosted order_usd {} -> {}", trigger_price, signal_boost_threshold, base_order_usd, boosted);
                boosted
            } else {
                base_order_usd
            };

            let min_size = Decimal::from(5_u32);
            let size = (order_usd / buy_price).round_dp(2).max(min_size);

            let mut risk_ctx = context.clone();
            risk_ctx.risk.daily_pnl = daily_pnl;
            let candidate_signal = poly2::StrategySignal {
                strategy_id: StrategyId::ProbabilityTrading,
                market_id: market.market_id.clone(),
                yes_token_id: Some(market.up_token_id.clone()),
                no_token_id: Some(market.down_token_id.clone()),
                actions: vec![poly2::OrderIntent {
                    side: trigger_side.clone(),
                    price: buy_price,
                    size,
                    sell: false,
                    gtd_expiration: None,
                }],
                state: poly2::StrategyState::Implemented,
            };
            if !risk_engine.allow(&candidate_signal, &risk_ctx) {
                println!(
                    "btc_candle: BLOCKED by risk engine (daily_pnl={}, capital={})",
                    daily_pnl, risk_ctx.risk.total_capital
                );
                break 'entry;
            }

            println!(
                "btc_candle: ENTRY triggered! side={:?} buy_price={} ask={} size={} (order_usd={}) bid={} spread={}",
                trigger_side, buy_price, trigger_price, size, order_usd, trigger_bid, spread
            );

            match execution.submit(&candidate_signal).await {
                Ok(report) => {
                    print_execution_report(&report);
                    let mut final_status = report.status.clone();
                    let order_id_for_poll = report.order_ids.first().cloned();
                    let mut last_matched = Decimal::ZERO;

                    if matches!(
                        final_status,
                        ExecutionStatus::Pending | ExecutionStatus::PartiallyFilled
                    ) {
                        if let Some(ref oid) = order_id_for_poll {
                            println!("btc_candle: order not fully filled yet, polling up to 5s...");
                            let poll_deadline = Utc::now().timestamp() + 5;
                            loop {
                                time::sleep(Duration::from_millis(1000)).await;
                                match execution.get_order_status(oid).await {
                                    Ok((s, m)) => {
                                        println!("btc_candle: poll status={:?} matched={}", s, m);
                                        final_status = s;
                                        last_matched = m;
                                        if matches!(
                                            final_status,
                                            ExecutionStatus::Filled | ExecutionStatus::Rejected
                                        ) {
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("btc_candle: poll error: {e}");
                                    }
                                }
                                if Utc::now().timestamp() >= poll_deadline {
                                    break;
                                }
                            }

                            if matches!(final_status, ExecutionStatus::Pending) {
                                println!("btc_candle: still pending after 5s, cancelling order {oid}");
                                match execution.cancel_order(oid).await {
                                    Ok(()) => {
                                        println!("btc_candle: cancel accepted, re-checking status...");
                                        if let Ok((s, m)) = execution.get_order_status(oid).await {
                                            println!("btc_candle: post-cancel status={:?} matched={}", s, m);
                                            final_status = s;
                                            last_matched = last_matched.max(m);
                                        }
                                    }
                                    Err(e) => eprintln!("btc_candle: cancel failed: {e}"),
                                }
                            }
                        }
                    } else if let Some(ref oid) = order_id_for_poll {
                        if let Ok((_, m)) = execution.get_order_status(oid).await {
                            last_matched = m;
                        }
                    }

                    match final_status {
                        ExecutionStatus::Filled | ExecutionStatus::PartiallyFilled => {
                            bought_side = Some(trigger_side);
                            bought_token_id = trigger_token;
                            entry_price = buy_price;
                            let fill_size_sum: Decimal =
                                report.fills.iter().filter(|f| !f.sell).map(|f| f.size).sum();
                            let from_fills_or_api = last_matched.max(fill_size_sum).min(size);
                            let raw_filled = if from_fills_or_api > Decimal::ZERO {
                                from_fills_or_api
                            } else {
                                size
                            };
                            bought_size = (raw_filled * Decimal::new(97, 2)).round_dp(2);
                            println!(
                                "btc_candle: adjusted bought_size={} (matched={} requested={})",
                                bought_size, raw_filled, size
                            );
                        }
                        _ => {
                            let current_ask = ws_cache.ask_for(&trigger_token).await;
                            if current_ask > buy_price + max_spread {
                                println!(
                                    "btc_candle: buy not filled (status={:?}), ask {} too far from buy_price {}, skipping entry",
                                    final_status, current_ask, buy_price
                                );
                                break 'entry;
                            }
                            println!(
                                "btc_candle: buy order not filled (status={:?}), ask {} still near buy_price {}, retrying",
                                final_status, current_ask, buy_price
                            );
                            time::sleep(Duration::from_millis(poll_ms)).await;
                            continue 'entry;
                        }
                    }
                }
                Err(err) => {
                    eprintln!("btc_candle: buy execution failed: {err}");
                    break 'entry;
                }
            }
            break 'entry;
        }

        let mut tp_order_id: Option<String> = None;
        if bought_side.is_some() && bought_size > Decimal::ZERO {
            let tp_expiration = DateTime::<Utc>::from_timestamp(close_ts, 0)
                .unwrap_or_else(|| Utc::now() + chrono::Duration::minutes(5));
            let mut tp_size = (bought_size * Decimal::new(97, 2)).round_dp(2);
            let min_order = Decimal::from(5_u32);
            if tp_size < min_order {
                tp_size = min_order.min(bought_size);
            }

            let mut tp_placed = false;
            for tp_attempt in 0..2_u32 {
                let tp_signal = poly2::StrategySignal {
                    strategy_id: StrategyId::ProbabilityTrading,
                    market_id: market.market_id.clone(),
                    yes_token_id: Some(market.up_token_id.clone()),
                    no_token_id: Some(market.down_token_id.clone()),
                    actions: vec![poly2::OrderIntent {
                        side: bought_side.clone().unwrap(),
                        price: take_profit_price,
                        size: tp_size,
                        sell: true,
                        gtd_expiration: Some(tp_expiration),
                    }],
                    state: poly2::StrategyState::Implemented,
                };
                match execution.submit(&tp_signal).await {
                    Ok(report) => {
                        tp_order_id = report.order_ids.first().cloned();
                        println!(
                            "btc_candle: GTD TP order placed — id={:?} price={} size={} expires={}",
                            tp_order_id, take_profit_price, tp_size,
                            tp_expiration.to_rfc3339()
                        );
                        if matches!(report.status, ExecutionStatus::Filled) {
                            println!("btc_candle: GTD TP order filled immediately!");
                        }
                        tp_placed = true;
                        break;
                    }
                    Err(e) => {
                        let err_msg = e.to_string();
                        if tp_attempt == 0 && err_msg.contains("not enough balance") {
                            if let Some(bal_str) = err_msg
                                .split("balance: ")
                                .nth(1)
                                .and_then(|s| s.split(|c: char| !c.is_ascii_digit()).next())
                            {
                                if let Ok(bal_raw) = bal_str.parse::<u64>() {
                                    let actual = Decimal::new(bal_raw as i64, 6);
                                    if actual > Decimal::ZERO {
                                        tp_size = (actual * Decimal::new(99, 2)).round_dp(2);
                                        println!(
                                            "btc_candle: GTD TP balance={}, retrying with size={}",
                                            actual, tp_size
                                        );
                                        continue;
                                    }
                                }
                            }
                        }
                        eprintln!("btc_candle: failed to place GTD TP order: {e}, will use app-level TP");
                        break;
                    }
                }
            }
            if !tp_placed {
                eprintln!("btc_candle: GTD TP order not placed after retries, relying on app-level TP/SL");
            }
        }

        let mut exit_price = Decimal::ZERO;
        let mut exit_reason = "none";
        let mut pnl_shares = bought_size;

        if let Some(ref exit_side) = bought_side {
            println!(
                "btc_candle: holding {:?} entry={}, SL={}, TP={}",
                exit_side, entry_price, base_stop_loss, take_profit_price
            );
            let mut last_known_bid = Decimal::ZERO;

            'exit: loop {
                if let Some(ref oid) = tp_order_id {
                    match execution.get_order_status(oid).await {
                        Ok((ExecutionStatus::Filled, matched)) => {
                            println!(
                                "btc_candle: GTD TP order FILLED by exchange! matched={} TP={}",
                                matched, take_profit_price
                            );
                            exit_price = take_profit_price;
                            exit_reason = "take_profit_gtd";
                            if matched > Decimal::ZERO {
                                pnl_shares = matched;
                            }
                            break 'exit;
                        }
                        Ok((ExecutionStatus::PartiallyFilled, matched)) => {
                            if matched > Decimal::ZERO && matched >= bought_size {
                                println!(
                                    "btc_candle: GTD TP order fully matched={} (bought_size={}), treating as filled",
                                    matched, bought_size
                                );
                                exit_price = take_profit_price;
                                exit_reason = "take_profit_gtd";
                                pnl_shares = matched;
                                break 'exit;
                            }
                        }
                        _ => {}
                    }
                }

                let now = Utc::now().timestamp();
                let remaining = close_ts - now;

                if remaining <= early_exit_secs {
                    let current_bid = ws_cache.bid_for(&bought_token_id).await;
                    let current_ask = ws_cache.ask_for(&bought_token_id).await;
                    let early_spread = current_ask - current_bid;
                    let early_bid_jump = if last_known_bid > Decimal::ZERO && current_bid > Decimal::ZERO {
                        (current_bid - last_known_bid).abs()
                    } else {
                        Decimal::ZERO
                    };
                    let early_up = ws_cache.up_ask().await;
                    let early_down = ws_cache.down_ask().await;
                    let early_ask_sum = early_up + early_down;

                    let early_data_ok = early_ask_sum <= exit_max_ask_sum
                        && !(current_bid > Decimal::ZERO && early_spread > exit_max_spread)
                        && early_bid_jump <= exit_max_bid_jump;

                    if !early_data_ok {
                        println!(
                            "btc_candle: early exit STALE DATA (ask_sum={} spread={} bid_jump={}), skipping early decisions",
                            early_ask_sum, early_spread, early_bid_jump
                        );
                    } else if current_ask >= near_certain {
                        println!(
                            "btc_candle: window closing in {}s, price {} >= near_certain {}, letting resolve",
                            remaining, current_ask, near_certain
                        );
                        if let Some(ref oid) = tp_order_id {
                            let _ = execution.cancel_order(oid).await;
                            println!("btc_candle: cancelled GTD TP order (near_certain resolve)");
                        }
                        exit_reason = "resolved_near_certain";
                        exit_price = Decimal::ONE;
                        break 'exit;
                    }

                    if early_data_ok && current_bid > entry_price {
                        println!(
                            "btc_candle: EARLY PROFIT EXIT — {}s left, bid {} > entry {}, selling to lock profit",
                            remaining, current_bid, entry_price
                        );
                        if let Some(ref oid) = tp_order_id {
                            let _ = execution.cancel_order(oid).await;
                            println!("btc_candle: cancelled GTD TP order before early exit sell");
                        }
                        exit_reason = "early_profit_exit";

                        let sell_out = execute_sell_with_retry(
                            execution,
                            &ws_cache,
                            &market.market_id,
                            &market.up_token_id,
                            &market.down_token_id,
                            exit_side.clone(),
                            &bought_token_id,
                            bought_size,
                            current_ask,
                        )
                        .await;
                        exit_price = sell_out.exit_price;
                        if sell_out.matched_shares > Decimal::ZERO {
                            pnl_shares = sell_out.matched_shares;
                        }
                        if !sell_out.fully_exited {
                            eprintln!(
                                "btc_candle: early exit sell incomplete matched={} remaining expected≈{}",
                                sell_out.matched_shares, bought_size
                            );
                        }
                        break 'exit;
                    }

                    if remaining <= 5 {
                        let resolve_bid = if early_data_ok { current_bid } else { last_known_bid };
                        println!("btc_candle: window closing, bid {} (reliable={}) <= entry {}, letting resolve", resolve_bid, early_data_ok, entry_price);
                        if let Some(ref oid) = tp_order_id {
                            let _ = execution.cancel_order(oid).await;
                            println!("btc_candle: cancelled GTD TP order (window closing)");
                        }
                        exit_reason = "resolved_at_loss";
                        exit_price = resolve_bid.max(Decimal::ZERO);
                        break 'exit;
                    }
                }

                let up_val = ws_cache.up_ask().await;
                let down_val = ws_cache.down_ask().await;
                let current_ask = if bought_token_id == market.up_token_id {
                    up_val
                } else {
                    down_val
                };
                let current_bid = ws_cache.bid_for(&bought_token_id).await;

                let ask_sum = up_val + down_val;
                let bid_ask_spread = current_ask - current_bid;
                let bid_jump = if last_known_bid > Decimal::ZERO && current_bid > Decimal::ZERO {
                    (current_bid - last_known_bid).abs()
                } else {
                    Decimal::ZERO
                };

                let data_reliable = if ask_sum > exit_max_ask_sum {
                    println!(
                        "btc_candle exit: STALE DATA — ask_sum={} > {} (up={} down={}), skipping TP/SL",
                        ask_sum, exit_max_ask_sum, up_val, down_val
                    );
                    false
                } else if current_bid > Decimal::ZERO && bid_ask_spread > exit_max_spread {
                    println!(
                        "btc_candle exit: WIDE SPREAD — spread={} > {} (ask={} bid={}), skipping TP/SL",
                        bid_ask_spread, exit_max_spread, current_ask, current_bid
                    );
                    false
                } else if bid_jump > exit_max_bid_jump {
                    println!(
                        "btc_candle exit: BID JUMP — |{} - {}| = {} > {}, skipping TP/SL",
                        current_bid, last_known_bid, bid_jump, exit_max_bid_jump
                    );
                    false
                } else {
                    true
                };

                if current_bid > Decimal::ZERO && data_reliable {
                    last_known_bid = current_bid;
                }

                println!(
                    "btc_candle exit: up_ask={} down_ask={} held_ask={} held_bid={} SL={} TP={} reliable={} remaining={}s",
                    up_val, down_val, current_ask, current_bid, base_stop_loss, take_profit_price, data_reliable, remaining
                );

                let trigger = if !data_reliable {
                    false
                } else if current_bid > Decimal::ZERO && current_bid <= base_stop_loss {
                    println!("btc_candle: STOP LOSS triggered at bid={} (SL={})", current_bid, base_stop_loss);
                    exit_reason = "stop_loss";
                    true
                } else if current_ask <= base_stop_loss && current_bid <= Decimal::ZERO {
                    println!("btc_candle: STOP LOSS triggered at ask={} (SL={}, bid unavailable)", current_ask, base_stop_loss);
                    exit_reason = "stop_loss";
                    true
                } else if current_bid > Decimal::ZERO && current_bid >= take_profit_price {
                    println!("btc_candle: TAKE PROFIT triggered at bid={} (TP={})", current_bid, take_profit_price);
                    exit_reason = "take_profit";
                    true
                } else {
                    false
                };

                if trigger {
                    if exit_reason == "take_profit" {
                        if let Some(ref oid) = tp_order_id {
                            if let Ok((st, matched)) = execution.get_order_status(oid).await {
                                if matches!(st, ExecutionStatus::Filled) || matched >= bought_size {
                                    println!(
                                        "btc_candle: GTD TP order already filled (matched={}), skipping manual sell",
                                        matched
                                    );
                                    exit_price = take_profit_price;
                                    exit_reason = "take_profit_gtd";
                                    if matched > Decimal::ZERO {
                                        pnl_shares = matched;
                                    }
                                    break 'exit;
                                }
                            }
                        }
                    }

                    if let Some(ref oid) = tp_order_id {
                        let _ = execution.cancel_order(oid).await;
                        println!("btc_candle: cancelled GTD TP order before {} sell", exit_reason);
                    }

                    let ws_bid = ws_cache.bid_for(&bought_token_id).await;
                    let sell_price = if ws_bid > Decimal::ZERO {
                        ws_bid
                    } else {
                        fetch_clob_best_bid(&bought_token_id).await.unwrap_or(current_ask)
                    };
                    let sell_price = if sell_price > Decimal::ZERO {
                        sell_price
                    } else {
                        current_ask
                    };

                    println!(
                        "btc_candle: SELL side={:?} initial bid-based price≈{} size={}",
                        exit_side, sell_price, bought_size
                    );

                    let sell_out = execute_sell_with_retry(
                        execution,
                        &ws_cache,
                        &market.market_id,
                        &market.up_token_id,
                        &market.down_token_id,
                        exit_side.clone(),
                        &bought_token_id,
                        bought_size,
                        current_ask,
                    )
                    .await;
                    exit_price = sell_out.exit_price;
                    if sell_out.matched_shares > Decimal::ZERO {
                        pnl_shares = sell_out.matched_shares;
                    }
                    if !sell_out.fully_exited {
                        eprintln!(
                            "btc_candle: sell incomplete after retries (matched={}, exit_reason={})",
                            sell_out.matched_shares, exit_reason
                        );
                    }
                    break 'exit;
                }

                time::sleep(Duration::from_millis(poll_ms)).await;
            }
        }

        if bought_side.is_some() {
            let round_pnl = (exit_price - entry_price) * pnl_shares;
            let is_resolved = exit_reason.starts_with("resolved_");
            if !is_resolved {
                daily_pnl += round_pnl;
            } else {
                println!(
                    "btc_candle: round settled by market resolution ({}), estimated pnl={} NOT counted toward daily_pnl",
                    exit_reason, round_pnl
                );
            }
            total_rounds += 1;
            let is_win = round_pnl > Decimal::ZERO;
            if is_win {
                winning_rounds += 1;
                consecutive_losses = 0;
            } else if !is_resolved {
                consecutive_losses += 1;
            }
            let win_rate = if total_rounds > 0 {
                Decimal::from(winning_rounds) / Decimal::from(total_rounds) * Decimal::from(100_u32)
            } else {
                Decimal::ZERO
            };

            println!(
                "btc_candle STATS: round_pnl={} daily_pnl={} win_rate={:.1}% ({}/{}) consec_losses={} reason={}",
                round_pnl, daily_pnl, win_rate, winning_rounds, total_rounds, consecutive_losses, exit_reason
            );

            append_round_record(
                round_log_path,
                loop_idx,
                &market.market_slug,
                bought_side.as_ref().map(|s| format!("{:?}", s)).unwrap_or_default(),
                entry_price,
                exit_price,
                pnl_shares,
                round_pnl,
                exit_reason,
            );
        }

        let _ = ws_cancel_tx.send(true);
        ws_handle.abort();

        println!("btc_candle: round #{} complete", loop_idx);
    }
}
