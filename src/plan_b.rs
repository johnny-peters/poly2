use chrono::Utc;
use rust_decimal::Decimal;
use std::path::Path;
use std::str::FromStr;
use tokio::time::{self, Duration};

use poly2::{
    discover_btc_candle_market, ExecutionClient, ExecutionStatus, RiskEngine, StrategyContext,
    StrategyId,
};

use crate::plan_a::execute_sell_with_retry;
use crate::{
    append_round_record, print_execution_report, read_decimal_env_or, read_u64_env_or,
    spawn_ws_price_feed, WsPriceCache,
};

pub(crate) async fn run_plan_b_loop<C>(
    _interval_secs: u64,
    context: &StrategyContext,
    risk_engine: &RiskEngine,
    execution: &C,
    dotenv_path: &Path,
) where
    C: ExecutionClient,
{
    let order_usd = read_decimal_env_or(dotenv_path, "PLAN_B_ORDER_USD", Decimal::from(5_u32));
    let max_entry_price = read_decimal_env_or(
        dotenv_path, "PLAN_B_MAX_ENTRY_PRICE",
        Decimal::from_str("0.55").unwrap_or(Decimal::ONE),
    );
    let min_entry_price = read_decimal_env_or(
        dotenv_path, "PLAN_B_MIN_ENTRY_PRICE",
        Decimal::from_str("0.40").unwrap_or(Decimal::ZERO),
    );
    let take_profit = read_decimal_env_or(
        dotenv_path, "PLAN_B_TAKE_PROFIT",
        Decimal::from_str("0.65").unwrap_or(Decimal::ONE),
    );
    let stop_loss = read_decimal_env_or(
        dotenv_path, "PLAN_B_STOP_LOSS",
        Decimal::from_str("0.39").unwrap_or(Decimal::ZERO),
    );
    let poll_ms = read_u64_env_or(dotenv_path, "PLAN_B_POLL_MS", 1000);
    let max_entry_sum = read_decimal_env_or(
        dotenv_path, "PLAN_B_MAX_ENTRY_SUM",
        Decimal::from_str("1.08").unwrap_or(Decimal::ONE),
    );
    let max_spread = read_decimal_env_or(
        dotenv_path, "PLAN_B_MAX_SPREAD",
        Decimal::from_str("0.05").unwrap_or(Decimal::ONE),
    );
    let early_exit_secs: i64 = read_u64_env_or(dotenv_path, "PLAN_B_EARLY_EXIT_SECS", 45) as i64;
    let max_consec_losses = read_u64_env_or(dotenv_path, "PLAN_B_MAX_CONSEC_LOSSES", 5);
    let loss_cooldown_secs = read_u64_env_or(dotenv_path, "PLAN_B_LOSS_COOLDOWN_SECS", 60);
    let buy_slippage = read_decimal_env_or(
        dotenv_path, "PLAN_B_BUY_SLIPPAGE",
        Decimal::from_str("0.02").unwrap_or(Decimal::ZERO),
    );

    println!("plan_b: order_usd={order_usd} max_entry_price={max_entry_price} min_entry_price={min_entry_price}");
    println!("plan_b: take_profit={take_profit} stop_loss={stop_loss} poll_ms={poll_ms}");
    println!("plan_b: max_entry_sum={max_entry_sum} max_spread={max_spread} early_exit_secs={early_exit_secs}");
    println!("plan_b: max_consec_losses={max_consec_losses} loss_cooldown_secs={loss_cooldown_secs}");

    let mut daily_pnl = context.risk.daily_pnl;
    let mut consecutive_losses: u64 = 0;
    let mut total_rounds: u64 = 0;
    let mut winning_rounds: u64 = 0;
    let round_log_path = Path::new("data/plan_b_rounds.jsonl");

    let mut loop_idx: u64 = 0;
    let mut first_round = true;
    loop {
        loop_idx += 1;

        let now_secs = Utc::now().timestamp();
        let current_window = now_secs - (now_secs.rem_euclid(300));

        let (window_ts, close_ts) = if first_round {
            first_round = false;
            println!(
                "\n================ plan_b round #{} (immediate start) ================\nnow={} window_ts={}",
                loop_idx, Utc::now().to_rfc3339(), current_window,
            );
            (current_window, current_window + 300)
        } else {
            let next_window = current_window + 300;
            let sleep_to_boundary = (next_window - now_secs).max(0) as u64;
            println!(
                "\n================ plan_b round #{} waiting for window ================\nnow={} next_window_ts={} sleeping={}s",
                loop_idx, Utc::now().to_rfc3339(), next_window, sleep_to_boundary
            );
            if sleep_to_boundary > 0 {
                time::sleep(Duration::from_secs(sleep_to_boundary)).await;
            }
            (next_window, next_window + 300)
        };

        let market = match discover_btc_candle_market(window_ts).await {
            Ok(m) => m,
            Err(err) => {
                eprintln!("plan_b: market discovery failed: {err}");
                time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };
        println!(
            "plan_b: market_id={} slug={} up={} down={}",
            market.market_id, market.market_slug, market.up_token_id, market.down_token_id
        );

        if consecutive_losses >= max_consec_losses {
            println!(
                "plan_b: {} consecutive losses >= max {}, cooling down {}s",
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

        let mut bought_up_size = Decimal::ZERO;
        let mut bought_down_size = Decimal::ZERO;
        let mut entry_price_up = Decimal::ZERO;
        let mut entry_price_down = Decimal::ZERO;

        'entry: loop {
            let now = Utc::now().timestamp();
            if now >= close_ts - 60 {
                println!("plan_b: window closing soon, no entry this round");
                break 'entry;
            }

            let up_ask = ws_cache.up_ask().await;
            let down_ask = ws_cache.down_ask().await;
            let ask_sum = up_ask + down_ask;

            println!(
                "plan_b poll: up_ask={} down_ask={} sum={} range=[{},{}] remaining={}s",
                up_ask, down_ask, ask_sum, min_entry_price, max_entry_price, close_ts - now
            );

            if up_ask < min_entry_price || down_ask < min_entry_price {
                time::sleep(Duration::from_millis(poll_ms)).await;
                continue 'entry;
            }
            if up_ask > max_entry_price || down_ask > max_entry_price {
                time::sleep(Duration::from_millis(poll_ms)).await;
                continue 'entry;
            }
            if ask_sum > max_entry_sum {
                println!("plan_b: SKIP — ask sum {} > max_entry_sum {}", ask_sum, max_entry_sum);
                time::sleep(Duration::from_millis(poll_ms)).await;
                continue 'entry;
            }

            let up_bid = ws_cache.bid_for(&market.up_token_id).await;
            let down_bid = ws_cache.bid_for(&market.down_token_id).await;
            if up_bid > Decimal::ZERO && (up_ask - up_bid) > max_spread {
                println!("plan_b: SKIP — UP spread {} > max_spread {}", up_ask - up_bid, max_spread);
                time::sleep(Duration::from_millis(poll_ms)).await;
                continue 'entry;
            }
            if down_bid > Decimal::ZERO && (down_ask - down_bid) > max_spread {
                println!("plan_b: SKIP — DOWN spread {} > max_spread {}", down_ask - down_bid, max_spread);
                time::sleep(Duration::from_millis(poll_ms)).await;
                continue 'entry;
            }

            let buy_price_up = (up_ask + buy_slippage).round_dp(2);
            let buy_price_down = (down_ask + buy_slippage).round_dp(2);
            let min_size = Decimal::from(5_u32);
            let size_up = (order_usd / buy_price_up).round_dp(2).max(min_size);
            let size_down = (order_usd / buy_price_down).round_dp(2).max(min_size);

            let mut risk_ctx = context.clone();
            risk_ctx.risk.daily_pnl = daily_pnl;
            let up_signal = poly2::StrategySignal {
                strategy_id: StrategyId::ProbabilityTrading,
                market_id: market.market_id.clone(),
                yes_token_id: Some(market.up_token_id.clone()),
                no_token_id: Some(market.down_token_id.clone()),
                actions: vec![poly2::OrderIntent {
                    side: poly2::Side::Yes,
                    price: buy_price_up,
                    size: size_up,
                    sell: false,
                    gtd_expiration: None,
                }],
                state: poly2::StrategyState::Implemented,
            };
            if !risk_engine.allow(&up_signal, &risk_ctx) {
                println!("plan_b: BLOCKED by risk engine (daily_pnl={daily_pnl})");
                break 'entry;
            }

            println!(
                "plan_b: ENTRY — buy UP @{} size={} + DOWN @{} size={}",
                buy_price_up, size_up, buy_price_down, size_down
            );

            match execution.submit(&up_signal).await {
                Ok(report) => {
                    print_execution_report(&report);
                    if matches!(report.status, ExecutionStatus::Filled | ExecutionStatus::PartiallyFilled) {
                        let fill_sum: Decimal = report.fills.iter().filter(|f| !f.sell).map(|f| f.size).sum();
                        let raw = if fill_sum > Decimal::ZERO { fill_sum } else { size_up };
                        bought_up_size = (raw * Decimal::new(97, 2)).round_dp(2);
                        entry_price_up = buy_price_up;
                        println!("plan_b: UP bought size={bought_up_size} entry={entry_price_up}");
                    } else {
                        println!("plan_b: UP buy not filled (status={:?}), aborting round", report.status);
                        break 'entry;
                    }
                }
                Err(e) => {
                    eprintln!("plan_b: UP buy failed: {e}");
                    break 'entry;
                }
            }

            let down_signal = poly2::StrategySignal {
                strategy_id: StrategyId::ProbabilityTrading,
                market_id: market.market_id.clone(),
                yes_token_id: Some(market.up_token_id.clone()),
                no_token_id: Some(market.down_token_id.clone()),
                actions: vec![poly2::OrderIntent {
                    side: poly2::Side::No,
                    price: buy_price_down,
                    size: size_down,
                    sell: false,
                    gtd_expiration: None,
                }],
                state: poly2::StrategyState::Implemented,
            };
            match execution.submit(&down_signal).await {
                Ok(report) => {
                    print_execution_report(&report);
                    if matches!(report.status, ExecutionStatus::Filled | ExecutionStatus::PartiallyFilled) {
                        let fill_sum: Decimal = report.fills.iter().filter(|f| !f.sell).map(|f| f.size).sum();
                        let raw = if fill_sum > Decimal::ZERO { fill_sum } else { size_down };
                        bought_down_size = (raw * Decimal::new(97, 2)).round_dp(2);
                        entry_price_down = buy_price_down;
                        println!("plan_b: DOWN bought size={bought_down_size} entry={entry_price_down}");
                    } else {
                        println!("plan_b: DOWN buy not filled (status={:?}), will run with UP only", report.status);
                    }
                }
                Err(e) => {
                    eprintln!("plan_b: DOWN buy failed: {e}, will run with UP only");
                }
            }
            break 'entry;
        }

        let have_position = bought_up_size > Decimal::ZERO || bought_down_size > Decimal::ZERO;

        let mut exit_price_up = Decimal::ZERO;
        let mut exit_price_down = Decimal::ZERO;
        let mut exit_reason = "none";
        let mut pnl_shares_up = bought_up_size;
        let mut pnl_shares_down = bought_down_size;
        let mut up_exited = bought_up_size <= Decimal::ZERO;
        let mut down_exited = bought_down_size <= Decimal::ZERO;

        if have_position {
            println!(
                "plan_b: holding UP(size={} entry={}) DOWN(size={} entry={}) TP={} SL={}",
                bought_up_size, entry_price_up, bought_down_size, entry_price_down, take_profit, stop_loss
            );

            'exit: loop {
                if up_exited && down_exited {
                    break 'exit;
                }

                let now = Utc::now().timestamp();
                let remaining = close_ts - now;

                if remaining <= early_exit_secs {
                    println!("plan_b: window closing in {remaining}s, exiting remaining positions");

                    if !up_exited {
                        let sell_out = execute_sell_with_retry(
                            execution, &ws_cache, &market.market_id,
                            &market.up_token_id, &market.down_token_id,
                            poly2::Side::Yes, &market.up_token_id,
                            bought_up_size, ws_cache.up_ask().await,
                        ).await;
                        exit_price_up = sell_out.exit_price;
                        if sell_out.matched_shares > Decimal::ZERO { pnl_shares_up = sell_out.matched_shares; }
                        up_exited = true;
                    }
                    if !down_exited {
                        let sell_out = execute_sell_with_retry(
                            execution, &ws_cache, &market.market_id,
                            &market.up_token_id, &market.down_token_id,
                            poly2::Side::No, &market.down_token_id,
                            bought_down_size, ws_cache.down_ask().await,
                        ).await;
                        exit_price_down = sell_out.exit_price;
                        if sell_out.matched_shares > Decimal::ZERO { pnl_shares_down = sell_out.matched_shares; }
                        down_exited = true;
                    }
                    exit_reason = "early_exit";
                    break 'exit;
                }

                let up_bid = ws_cache.bid_for(&market.up_token_id).await;
                let down_bid = ws_cache.bid_for(&market.down_token_id).await;

                println!(
                    "plan_b exit: up_bid={} down_bid={} TP={} SL={} up_exited={} down_exited={} remaining={}s",
                    up_bid, down_bid, take_profit, stop_loss, up_exited, down_exited, remaining
                );

                if !up_exited && up_bid > Decimal::ZERO {
                    if up_bid >= take_profit {
                        println!("plan_b: UP TAKE PROFIT — bid={up_bid} >= TP={take_profit}");
                        let sell_out = execute_sell_with_retry(
                            execution, &ws_cache, &market.market_id,
                            &market.up_token_id, &market.down_token_id,
                            poly2::Side::Yes, &market.up_token_id,
                            bought_up_size, ws_cache.up_ask().await,
                        ).await;
                        exit_price_up = sell_out.exit_price;
                        if sell_out.matched_shares > Decimal::ZERO { pnl_shares_up = sell_out.matched_shares; }
                        up_exited = true;
                        if exit_reason == "none" { exit_reason = "up_take_profit"; }
                    } else if up_bid <= stop_loss {
                        println!("plan_b: UP STOP LOSS — bid={up_bid} <= SL={stop_loss}");
                        let sell_out = execute_sell_with_retry(
                            execution, &ws_cache, &market.market_id,
                            &market.up_token_id, &market.down_token_id,
                            poly2::Side::Yes, &market.up_token_id,
                            bought_up_size, ws_cache.up_ask().await,
                        ).await;
                        exit_price_up = sell_out.exit_price;
                        if sell_out.matched_shares > Decimal::ZERO { pnl_shares_up = sell_out.matched_shares; }
                        up_exited = true;
                        if exit_reason == "none" { exit_reason = "up_stop_loss"; }
                    }
                }

                if !down_exited && down_bid > Decimal::ZERO {
                    if down_bid >= take_profit {
                        println!("plan_b: DOWN TAKE PROFIT — bid={down_bid} >= TP={take_profit}");
                        let sell_out = execute_sell_with_retry(
                            execution, &ws_cache, &market.market_id,
                            &market.up_token_id, &market.down_token_id,
                            poly2::Side::No, &market.down_token_id,
                            bought_down_size, ws_cache.down_ask().await,
                        ).await;
                        exit_price_down = sell_out.exit_price;
                        if sell_out.matched_shares > Decimal::ZERO { pnl_shares_down = sell_out.matched_shares; }
                        down_exited = true;
                        if exit_reason == "none" { exit_reason = "down_take_profit"; }
                    } else if down_bid <= stop_loss {
                        println!("plan_b: DOWN STOP LOSS — bid={down_bid} <= SL={stop_loss}");
                        let sell_out = execute_sell_with_retry(
                            execution, &ws_cache, &market.market_id,
                            &market.up_token_id, &market.down_token_id,
                            poly2::Side::No, &market.down_token_id,
                            bought_down_size, ws_cache.down_ask().await,
                        ).await;
                        exit_price_down = sell_out.exit_price;
                        if sell_out.matched_shares > Decimal::ZERO { pnl_shares_down = sell_out.matched_shares; }
                        down_exited = true;
                        if exit_reason == "none" { exit_reason = "down_stop_loss"; }
                    }
                }

                time::sleep(Duration::from_millis(poll_ms)).await;
            }
        }

        if have_position {
            let pnl_up = (exit_price_up - entry_price_up) * pnl_shares_up;
            let pnl_down = (exit_price_down - entry_price_down) * pnl_shares_down;
            let round_pnl = pnl_up + pnl_down;
            daily_pnl += round_pnl;
            total_rounds += 1;
            if round_pnl > Decimal::ZERO {
                winning_rounds += 1;
                consecutive_losses = 0;
            } else {
                consecutive_losses += 1;
            }
            let win_rate = if total_rounds > 0 {
                Decimal::from(winning_rounds) / Decimal::from(total_rounds) * Decimal::from(100_u32)
            } else {
                Decimal::ZERO
            };

            println!(
                "plan_b STATS: pnl_up={} pnl_down={} round_pnl={} daily_pnl={} win_rate={:.1}% ({}/{}) consec_losses={} reason={}",
                pnl_up, pnl_down, round_pnl, daily_pnl, win_rate, winning_rounds, total_rounds, consecutive_losses, exit_reason
            );

            append_round_record(
                round_log_path, loop_idx, &market.market_slug,
                "both".to_string(), entry_price_up, exit_price_up,
                pnl_shares_up + pnl_shares_down, round_pnl, exit_reason,
            );
        }

        let _ = ws_cancel_tx.send(true);
        ws_handle.abort();
        println!("plan_b: round #{loop_idx} complete");
    }
}
