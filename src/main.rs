use anyhow::{anyhow, Context};
use chrono::Utc;
use polymarket_client_sdk::auth::ExposeSecret as _;
use polymarket_client_sdk::auth::{LocalSigner as PmLocalSigner, Signer as _};
use polymarket_client_sdk::clob::types::{Side as PmSide, SignatureType as PmSignatureType};
use polymarket_client_sdk::clob::{Client as PmClobClient, Config as PmClobConfig};
use polymarket_client_sdk::types::{Address as PmAddress, Decimal as PmDecimal, U256 as PmU256};
use polymarket_client_sdk::{
    derive_proxy_wallet, derive_safe_wallet, POLYGON as PM_POLYGON, PRIVATE_KEY_VAR,
};
use futures::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::io::Write as _;
use std::path::Path;
use std::process::Command;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{self, Duration};
use tokio_tungstenite::tungstenite::Message as WsMessage;

use poly2::{
    run_healthcheck, scan_arb_candidates, AppConfig, ArbitrageStrategy,
    ExecutionClient, ExecutionReport, ExecutionSettings, ExecutionStatus,
    MockExecutionClient, RetryingExecutionClient, RiskContext, RiskEngine,
    PolymarketSdkExecutionClient, StrategyContext, StrategyId, StrategyRegistry, TodoStrategy,
    TradingEngine, discover_btc_candle_market,
};

async fn fetch_clob_price(token_id: &str, side: &str) -> anyhow::Result<Decimal> {
    let url = format!(
        "https://clob.polymarket.com/price?token_id={}&side={}",
        token_id.trim(),
        side
    );
    let resp = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(10))
        .build()?
        .get(url)
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(anyhow::anyhow!("clob price failed: status={}", resp.status()));
    }
    let root: serde_json::Value = resp.json().await?;
    let raw = root
        .get("price")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("clob price missing 'price' field"))?;
    Ok(Decimal::from_str(raw.trim())?)
}

async fn fetch_clob_best_bid(token_id: &str) -> anyhow::Result<Decimal> {
    fetch_clob_price(token_id, "sell").await
}

#[derive(Debug, Clone, Default)]
struct TokenPrices {
    best_ask: Decimal,
    best_bid: Decimal,
}

#[derive(Clone)]
struct WsPriceCache {
    up: Arc<RwLock<TokenPrices>>,
    down: Arc<RwLock<TokenPrices>>,
    up_token_id: String,
    down_token_id: String,
}

impl WsPriceCache {
    fn new(up_token_id: &str, down_token_id: &str) -> Self {
        Self {
            up: Arc::new(RwLock::new(TokenPrices::default())),
            down: Arc::new(RwLock::new(TokenPrices::default())),
            up_token_id: up_token_id.to_string(),
            down_token_id: down_token_id.to_string(),
        }
    }

    async fn up_ask(&self) -> Decimal {
        self.up.read().await.best_ask
    }
    async fn down_ask(&self) -> Decimal {
        self.down.read().await.best_ask
    }
    async fn bid_for(&self, token_id: &str) -> Decimal {
        if token_id == self.up_token_id {
            self.up.read().await.best_bid
        } else {
            self.down.read().await.best_bid
        }
    }
    async fn ask_for(&self, token_id: &str) -> Decimal {
        if token_id == self.up_token_id {
            self.up.read().await.best_ask
        } else {
            self.down.read().await.best_ask
        }
    }
}

fn spawn_ws_price_feed(cache: WsPriceCache, cancel: tokio::sync::watch::Receiver<bool>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut cancel = cancel;
        let url = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

        'reconnect: loop {
            if *cancel.borrow() { return; }

            let ws_conn = tokio::select! {
                res = tokio_tungstenite::connect_async(url) => res,
                _ = cancel.changed() => return,
            };
            let (mut ws, _resp) = match ws_conn {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("ws_price: connect failed: {e}, retrying in 2s");
                    tokio::select! {
                        _ = time::sleep(Duration::from_secs(2)) => {},
                        _ = cancel.changed() => return,
                    }
                    continue 'reconnect;
                }
            };

            let sub = serde_json::json!({
                "assets_ids": [&cache.up_token_id, &cache.down_token_id],
                "type": "market",
                "custom_feature_enabled": true
            });
            if ws.send(WsMessage::Text(sub.to_string().into())).await.is_err() {
                eprintln!("ws_price: subscribe send failed, reconnecting");
                continue 'reconnect;
            }
            println!("ws_price: connected & subscribed (up={}, down={})",
                &cache.up_token_id[..8.min(cache.up_token_id.len())],
                &cache.down_token_id[..8.min(cache.down_token_id.len())]);

            let mut last_ping = tokio::time::Instant::now();

            loop {
                if *cancel.borrow() {
                    let _ = ws.close(None).await;
                    return;
                }

                let msg = tokio::select! {
                    m = ws.next() => match m {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => {
                            eprintln!("ws_price: read error: {e}, reconnecting");
                            break; // -> reconnect
                        }
                        None => {
                            eprintln!("ws_price: stream closed, reconnecting");
                            break;
                        }
                    },
                    _ = cancel.changed() => {
                        let _ = ws.close(None).await;
                        return;
                    },
                    _ = time::sleep(Duration::from_secs(30)) => {
                        eprintln!("ws_price: no message for 30s, reconnecting");
                        break;
                    }
                };

                if last_ping.elapsed() >= Duration::from_secs(10) {
                    let _ = ws.send(WsMessage::text("PING")).await;
                    last_ping = tokio::time::Instant::now();
                }

                let text = match &msg {
                    WsMessage::Text(t) => t.clone(),
                    WsMessage::Ping(d) => {
                        let _ = ws.send(WsMessage::Pong(d.clone())).await;
                        continue;
                    }
                    _ => continue,
                };

                let val: serde_json::Value = match serde_json::from_str(text.as_ref()) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                let event = val.get("event_type").and_then(|v| v.as_str()).unwrap_or("");

                match event {
                    "book" => {
                        let asset_id = val.get("asset_id").and_then(|v| v.as_str()).unwrap_or("");
                        let best_ask = val.get("asks")
                            .and_then(|a| a.as_array())
                            .and_then(|a| a.first())
                            .and_then(|o| o.get("price"))
                            .and_then(|p| p.as_str())
                            .and_then(|s| Decimal::from_str(s).ok())
                            .unwrap_or(Decimal::ZERO);
                        let best_bid = val.get("bids")
                            .and_then(|a| a.as_array())
                            .and_then(|a| a.last())
                            .and_then(|o| o.get("price"))
                            .and_then(|p| p.as_str())
                            .and_then(|s| Decimal::from_str(s).ok())
                            .unwrap_or(Decimal::ZERO);
                        let lock = if asset_id == cache.up_token_id { &cache.up } else { &cache.down };
                        let mut w = lock.write().await;
                        w.best_ask = best_ask;
                        w.best_bid = best_bid;
                    }
                    "best_bid_ask" => {
                        let asset_id = val.get("asset_id").and_then(|v| v.as_str()).unwrap_or("");
                        let best_ask = val.get("best_ask")
                            .and_then(|v| v.as_str())
                            .and_then(|s| Decimal::from_str(s).ok())
                            .unwrap_or(Decimal::ZERO);
                        let best_bid = val.get("best_bid")
                            .and_then(|v| v.as_str())
                            .and_then(|s| Decimal::from_str(s).ok())
                            .unwrap_or(Decimal::ZERO);
                        let lock = if asset_id == cache.up_token_id { &cache.up } else { &cache.down };
                        let mut w = lock.write().await;
                        if best_ask > Decimal::ZERO { w.best_ask = best_ask; }
                        if best_bid > Decimal::ZERO { w.best_bid = best_bid; }
                    }
                    "price_change" => {
                        if let Some(changes) = val.get("price_changes").and_then(|v| v.as_array()) {
                            for ch in changes {
                                let asset_id = ch.get("asset_id").and_then(|v| v.as_str()).unwrap_or("");
                                let lock = if asset_id == cache.up_token_id { &cache.up } else { &cache.down };
                                let mut w = lock.write().await;
                                if let Some(ba) = ch.get("best_ask").and_then(|v| v.as_str()).and_then(|s| Decimal::from_str(s).ok()) {
                                    if ba > Decimal::ZERO { w.best_ask = ba; }
                                }
                                if let Some(bb) = ch.get("best_bid").and_then(|v| v.as_str()).and_then(|s| Decimal::from_str(s).ok()) {
                                    if bb > Decimal::ZERO { w.best_bid = bb; }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    })
}

fn read_decimal_env_or(dotenv_path: &Path, key: &str, default_value: Decimal) -> Decimal {
    resolve_runtime_var(key, dotenv_path)
        .and_then(|v| Decimal::from_str(v.trim()).ok())
        .unwrap_or(default_value)
}

fn read_u64_env_or(dotenv_path: &Path, key: &str, default_value: u64) -> u64 {
    resolve_runtime_var(key, dotenv_path)
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(default_value)
}

async fn run_btc_candle_loop<C>(
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

        // --- Phase 2: discover the market for this window ---
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

        // --- Phase 3: risk guard — consecutive loss cooldown ---
        if consecutive_losses >= max_consec_losses {
            println!(
                "btc_candle: {} consecutive losses >= max {}, cooling down {}s",
                consecutive_losses, max_consec_losses, loss_cooldown_secs
            );
            time::sleep(Duration::from_secs(loss_cooldown_secs)).await;
            consecutive_losses = 0;
            continue;
        }

        // --- Phase 3b: start WebSocket price feed for this round ---
        let ws_cache = WsPriceCache::new(&market.up_token_id, &market.down_token_id);
        let (ws_cancel_tx, ws_cancel_rx) = tokio::sync::watch::channel(false);
        let ws_handle = spawn_ws_price_feed(ws_cache.clone(), ws_cancel_rx);
        // wait briefly for initial book snapshot from WS
        time::sleep(Duration::from_millis(1500)).await;

        // --- Phase 4: poll ask prices until one exceeds entry_threshold ---
        let mut bought_side: Option<poly2::Side> = None;
        let mut bought_token_id = String::new();
        let mut bought_size = Decimal::ZERO;
        let mut entry_price = Decimal::ZERO;

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

            if trigger_price <= Decimal::ZERO {
                time::sleep(Duration::from_millis(poll_ms)).await;
                continue 'entry;
            }

            // [P1] Two-sided spread check: reject overpriced markets
            let ask_sum = up_val + down_val;
            if ask_sum > max_entry_sum {
                println!(
                    "btc_candle: SKIP — ask sum {} > max_entry_sum {}, market charging premium",
                    ask_sum, max_entry_sum
                );
                time::sleep(Duration::from_millis(poll_ms)).await;
                continue 'entry;
            }

            // [P2] Bid-ask spread guard: ensure we can exit without huge slippage
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

            // Strict entry: always buy at entry_threshold, never chase higher prices
            let buy_price = entry_threshold;

            if trigger_price > buy_price + max_spread {
                println!(
                    "btc_candle: ask {} too far above buy_price {} (max_spread={}), waiting for pullback",
                    trigger_price, buy_price, max_spread
                );
                time::sleep(Duration::from_millis(poll_ms)).await;
                continue 'entry;
            }

            // [P2] Signal-strength weighted position sizing
            let order_usd = if trigger_price >= signal_boost_threshold {
                let boosted = (base_order_usd * signal_boost_multiplier).round_dp(2);
                println!("btc_candle: strong signal ({} >= {}), boosted order_usd {} -> {}", trigger_price, signal_boost_threshold, base_order_usd, boosted);
                boosted
            } else {
                base_order_usd
            };

            let min_size = Decimal::from(5_u32);
            let size = (order_usd / buy_price).round_dp(2).max(min_size);

            // [P0] Risk engine check before entry
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

                    if matches!(final_status, ExecutionStatus::Pending) {
                        if let Some(ref oid) = order_id_for_poll {
                            println!("btc_candle: order pending, polling up to 5s for fill...");
                            let poll_deadline = Utc::now().timestamp() + 5;
                            loop {
                                time::sleep(Duration::from_millis(1000)).await;
                                match execution.get_order_status(oid).await {
                                    Ok(s) => {
                                        println!("btc_candle: poll status={:?}", s);
                                        final_status = s;
                                        if !matches!(final_status, ExecutionStatus::Pending) {
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
                                        if let Ok(s) = execution.get_order_status(oid).await {
                                            println!("btc_candle: post-cancel status={:?}", s);
                                            final_status = s;
                                        }
                                    }
                                    Err(e) => eprintln!("btc_candle: cancel failed: {e}"),
                                }
                            }
                        }
                    }

                    match final_status {
                        ExecutionStatus::Filled | ExecutionStatus::PartiallyFilled => {
                            bought_side = Some(trigger_side);
                            bought_token_id = trigger_token;
                            entry_price = buy_price;
                            let fill_size_sum: Decimal =
                                report.fills.iter().filter(|f| !f.sell).map(|f| f.size).sum();
                            bought_size = if fill_size_sum > Decimal::ZERO && fill_size_sum < size {
                                (fill_size_sum * Decimal::new(98, 2)).round_dp(2)
                            } else {
                                (size * Decimal::new(98, 2)).round_dp(2)
                            };
                            println!(
                                "btc_candle: adjusted bought_size={} (raw={})",
                                bought_size, size
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

        // --- Phase 5: if we bought, monitor for stop-loss / take-profit ---
        let mut exit_price = Decimal::ZERO;
        let mut exit_reason = "none";

        if let Some(ref exit_side) = bought_side {
            println!(
                "btc_candle: holding {:?} entry={}, SL={}, TP={}",
                exit_side, entry_price, base_stop_loss, take_profit_price
            );

            'exit: loop {
                let now = Utc::now().timestamp();
                let remaining = close_ts - now;

                // [P0] Early exit: sell to lock profits before window closes
                if remaining <= early_exit_secs {
                    let current_bid = ws_cache.bid_for(&bought_token_id).await;
                    let current_ask = ws_cache.ask_for(&bought_token_id).await;

                    if current_ask >= near_certain {
                        println!(
                            "btc_candle: window closing in {}s, price {} >= near_certain {}, letting resolve",
                            remaining, current_ask, near_certain
                        );
                        exit_reason = "resolved_near_certain";
                        exit_price = Decimal::ONE;
                        break 'exit;
                    }

                    if current_bid > entry_price {
                        println!(
                            "btc_candle: EARLY PROFIT EXIT — {}s left, bid {} > entry {}, selling to lock profit",
                            remaining, current_bid, entry_price
                        );
                        let sell_price = if current_bid > Decimal::ZERO { current_bid } else { current_ask };
                        exit_price = sell_price;
                        exit_reason = "early_profit_exit";

                        let signal = poly2::StrategySignal {
                            strategy_id: StrategyId::ProbabilityTrading,
                            market_id: market.market_id.clone(),
                            yes_token_id: Some(market.up_token_id.clone()),
                            no_token_id: Some(market.down_token_id.clone()),
                            actions: vec![poly2::OrderIntent {
                                side: exit_side.clone(),
                                price: sell_price,
                                size: bought_size,
                                sell: true,
                            }],
                            state: poly2::StrategyState::Implemented,
                        };
                        match execution.submit(&signal).await {
                            Ok(report) => print_execution_report(&report),
                            Err(err) => eprintln!("btc_candle: early exit sell failed: {err}"),
                        }
                        break 'exit;
                    }

                    if remaining <= 5 {
                        println!("btc_candle: window closing, bid {} <= entry {}, letting resolve", current_bid, entry_price);
                        exit_reason = "resolved_at_loss";
                        exit_price = Decimal::ZERO;
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

                println!(
                    "btc_candle exit: up_ask={} down_ask={} held_price={} SL={} TP={} remaining={}s",
                    up_val, down_val, current_ask, base_stop_loss, take_profit_price, remaining
                );

                let trigger = if current_ask <= base_stop_loss {
                    println!("btc_candle: STOP LOSS triggered at {} (SL={})", current_ask, base_stop_loss);
                    exit_reason = "stop_loss";
                    true
                } else if current_ask >= take_profit_price {
                    println!("btc_candle: TAKE PROFIT triggered at {}", current_ask);
                    exit_reason = "take_profit";
                    true
                } else {
                    false
                };

                if trigger {
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
                    exit_price = sell_price;

                    println!(
                        "btc_candle: SELL side={:?} price={} size={}",
                        exit_side, sell_price, bought_size
                    );

                    let signal = poly2::StrategySignal {
                        strategy_id: StrategyId::ProbabilityTrading,
                        market_id: market.market_id.clone(),
                        yes_token_id: Some(market.up_token_id.clone()),
                        no_token_id: Some(market.down_token_id.clone()),
                        actions: vec![poly2::OrderIntent {
                            side: exit_side.clone(),
                            price: sell_price,
                            size: bought_size,
                            sell: true,
                        }],
                        state: poly2::StrategyState::Implemented,
                    };

                    match execution.submit(&signal).await {
                        Ok(report) => print_execution_report(&report),
                        Err(err) => {
                            eprintln!("btc_candle: sell execution failed: {err}, retrying with 95% size");
                            let retry_size = (bought_size * Decimal::new(95, 2)).round_dp(2);
                            let retry_signal = poly2::StrategySignal {
                                strategy_id: StrategyId::ProbabilityTrading,
                                market_id: market.market_id.clone(),
                                yes_token_id: Some(market.up_token_id.clone()),
                                no_token_id: Some(market.down_token_id.clone()),
                                actions: vec![poly2::OrderIntent {
                                    side: exit_side.clone(),
                                    price: sell_price,
                                    size: retry_size,
                                    sell: true,
                                }],
                                state: poly2::StrategyState::Implemented,
                            };
                            match execution.submit(&retry_signal).await {
                                Ok(report) => {
                                    print_execution_report(&report);
                                    bought_size = retry_size;
                                }
                                Err(err2) => {
                                    eprintln!("btc_candle: sell retry also failed: {err2}, marking as unresolved");
                                    exit_price = Decimal::ZERO;
                                    exit_reason = "sell_failed";
                                }
                            }
                        }
                    }
                    break 'exit;
                }

                time::sleep(Duration::from_millis(poll_ms)).await;
            }
        }

        // --- Phase 6: round PnL tracking ---
        if bought_side.is_some() {
            let round_pnl = (exit_price - entry_price) * bought_size;
            daily_pnl += round_pnl;
            total_rounds += 1;
            let is_win = round_pnl > Decimal::ZERO;
            if is_win {
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
                bought_size,
                round_pnl,
                exit_reason,
            );
        }

        // shutdown WS price feed for this round
        let _ = ws_cancel_tx.send(true);
        ws_handle.abort();

        println!("btc_candle: round #{} complete", loop_idx);
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::ring::default_provider(),
    );
    let args: Vec<String> = std::env::args().collect();
    let dotenv_path = Path::new("src/.env");
    let app_config = AppConfig::load_from_path_with_env_overlay(
        "config/default.yaml",
        Some(dotenv_path),
    )?;
    if args.get(1).map(|s| s.as_str()) == Some("healthcheck") {
        let report = run_healthcheck(&app_config.execution_settings(), dotenv_path).await;
        let healthcheck_passed = report.passed();
        println!("healthcheck_passed={healthcheck_passed}");
        for c in report.checks {
            println!(
                "- [{}] {} => {}",
                if c.ok { "OK" } else { "FAIL" },
                c.name,
                c.detail
            );
        }
        let strict = args.iter().any(|a| a == "--strict");
        if strict {
            println!("healthcheck_strict=true");
            if !healthcheck_passed {
                return Err(anyhow!("strict healthcheck failed: connectivity checks not passed"));
            }
            let preflight = run_startup_preflight(&app_config, dotenv_path)?;
            print_preflight_report(&preflight);
            if preflight.live_mode && preflight.trading_enabled && preflight.total_capital.is_none() {
                return Err(anyhow!(
                    "strict preflight failed: TOTAL_CAPITAL_USD or TOTAL_CAPITAL_CMD is required"
                ));
            }
        }
        return Ok(());
    }
    if args.get(1).map(|s| s.as_str()) == Some("scan-arb") {
        let execution_settings = app_config.execution_settings();
        let mut top_n: usize = 20;
        let mut max_sum = read_scan_max_sum(Path::new("src/.env"), Decimal::from_str("1.005")?);
        let mut min_edge = read_scan_min_edge(Path::new("src/.env"), Decimal::from_str("0.005")?);
        let mut settle_guard_minutes = execution_settings.scan_min_minutes_to_settle;
        let mut i = 2usize;
        while i < args.len() {
            match args[i].as_str() {
                "--top" if i + 1 < args.len() => {
                    top_n = args[i + 1].parse::<usize>()?;
                    i += 2;
                }
                "--max-sum" if i + 1 < args.len() => {
                    max_sum = Decimal::from_str(&args[i + 1])?;
                    i += 2;
                }
                "--min-edge" if i + 1 < args.len() => {
                    min_edge = Decimal::from_str(&args[i + 1])?;
                    i += 2;
                }
                "--settle-guard-minutes" if i + 1 < args.len() => {
                    settle_guard_minutes = args[i + 1].parse::<u64>()?;
                    i += 2;
                }
                _ => {
                    i += 1;
                }
            }
        }

        run_scan_arb_once(
            &execution_settings,
            top_n,
            max_sum,
            min_edge,
            settle_guard_minutes,
        )
        .await;
        return Ok(());
    }
    if args.get(1).map(|s| s.as_str()) == Some("sdk-order") {
        run_sdk_limit_order(dotenv_path).await?;
        return Ok(());
    }
    if args.get(1).map(|s| s.as_str()) == Some("sdk-auth-check") {
        run_sdk_auth_check(dotenv_path).await?;
        return Ok(());
    }
    if args.get(1).map(|s| s.as_str()) == Some("sdk-auth-export") {
        run_sdk_auth_export(dotenv_path).await?;
        return Ok(());
    }
    let mut registry = StrategyRegistry::new();
    if app_config.strategies.arbitrage.enabled {
        registry.register(Arc::new(ArbitrageStrategy::new(app_config.arbitrage_config()?)));
    }

    registry.register(Arc::new(TodoStrategy::new(StrategyId::ProbabilityTrading)));
    registry.register(Arc::new(TodoStrategy::new(StrategyId::MarketMaking)));
    registry.register(Arc::new(TodoStrategy::new(StrategyId::LatencyArbitrage)));

    let risk_engine = RiskEngine::new(app_config.risk_config()?);
    let _engine = TradingEngine::new(registry, RiskEngine::new(app_config.risk_config()?));

    let execution_settings = app_config.execution_settings();
    println!(
        "execution target={}, retries={}, timeout={}s",
        execution_settings.polymarket_ws_url,
        execution_settings.max_retries,
        execution_settings.timeout_secs
    );
    let fetch_interval_secs = read_fetch_interval_secs(Path::new("src/.env"), 60);
    let _scan_max_sum = read_scan_max_sum(Path::new("src/.env"), Decimal::from_str("1.005")?);
    let _scan_min_edge = read_scan_min_edge(Path::new("src/.env"), Decimal::from_str("0.005")?);
    let _scan_top_n = read_scan_top_n(Path::new("src/.env"), 5);
    let _scan_settle_guard_minutes = execution_settings.scan_min_minutes_to_settle;
    // println!("market scan interval={}s (from FETCH_INTERVAL)", fetch_interval_secs);
    // println!("market scan top_n={} (from SCAN_TOP_N)", scan_top_n);
    // println!("market scan max_sum={} (from SCAN_MAX_SUM)", scan_max_sum);
    // println!("market scan min_edge={} (from SCAN_MIN_EDGE)", scan_min_edge);
    // println!(
    //     "market scan settle_guard_minutes={} (from SCAN_MIN_MINUTES_TO_SETTLE)",
    //     scan_settle_guard_minutes
    // );
    let preflight = run_startup_preflight(&app_config, dotenv_path)?;
    print_preflight_report(&preflight);
    let context = StrategyContext {
        risk: RiskContext {
            total_capital: preflight
                .total_capital
                .unwrap_or_else(|| Decimal::from(100_000_i32)),
            position_per_market: HashMap::new(),
            daily_pnl: Decimal::ZERO,
        },
        external_probability: None,
    };

    if let Some(http_url) = &execution_settings.polymarket_http_url {
        println!("execution mode=HTTP live, target={http_url}");
        let pm_chain_id = read_pm_chain_id(dotenv_path, PM_POLYGON);
        let pm_signature_type = resolve_pm_signature_type(dotenv_path)?;
        let pm_funder = resolve_pm_funder_address(dotenv_path, pm_signature_type, pm_chain_id)?;
        let private_key = resolve_runtime_var(PRIVATE_KEY_VAR, dotenv_path)
            .or_else(|| resolve_runtime_var("PRIVATE_KEY", dotenv_path))
            .ok_or_else(|| anyhow!("missing private key: set {PRIVATE_KEY_VAR} or PRIVATE_KEY"))?;
        let signer = PmLocalSigner::from_str(private_key.trim())
            .context("invalid private key format")?
            .with_chain_id(Some(pm_chain_id));
        let mut auth_builder = PmClobClient::new(http_url, PmClobConfig::default())?
            .authentication_builder(&signer);
        if let Some(sig) = pm_signature_type {
            println!("pm_signature_type={}", pm_signature_type_to_name(sig));
            auth_builder = auth_builder.signature_type(sig);
        }
        if let Some(funder) = pm_funder {
            println!("pm_funder={funder}");
            auth_builder = auth_builder.funder(funder);
        }
        let sdk_client = auth_builder.authenticate().await?;
        let retrying = RetryingExecutionClient::new(
            PolymarketSdkExecutionClient::new(sdk_client, private_key, pm_chain_id),
            execution_settings.max_retries,
            execution_settings.timeout_secs * 100,
        );
        run_btc_candle_loop(fetch_interval_secs, &context, &risk_engine, &retrying, dotenv_path).await;
    } else {
        println!("execution mode=mock (polymarket_http_url is empty)");
        let retrying = RetryingExecutionClient::new(
            MockExecutionClient::default(),
            execution_settings.max_retries,
            execution_settings.timeout_secs * 100,
        );
        run_btc_candle_loop(fetch_interval_secs, &context, &risk_engine, &retrying, dotenv_path).await;
    }

    Ok(())
}

async fn run_scan_arb_once(
    execution_settings: &ExecutionSettings,
    top_n: usize,
    max_sum: Decimal,
    min_edge: Decimal,
    settle_guard_minutes: u64,
) {
    let started = Utc::now();
    println!("scan_started_at={}", started.to_rfc3339());
    match scan_arb_candidates(
        execution_settings,
        top_n,
        max_sum,
        min_edge,
        settle_guard_minutes,
    )
    .await
    {
        Ok(candidates) => {
            println!("arb_candidates_count={}", candidates.len());
            for (i, c) in candidates.iter().enumerate() {
                let volume_24h = c
                    .volume_24h
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "n/a".to_string());
                println!(
                    "#{:02} edge={} sum={} pA={} pB={} vol24h={} slug={} question={}",
                    i + 1,
                    c.edge,
                    c.sum,
                    c.price_a,
                    c.price_b,
                    volume_24h,
                    c.market_slug,
                    c.question
                );
            }
        }
        Err(err) => {
            eprintln!("scan-arb failed: {err}");
        }
    }
}

async fn run_sdk_limit_order(dotenv_path: &Path) -> anyhow::Result<()> {
    let host = resolve_runtime_var("PM_HOST", dotenv_path)
        .unwrap_or_else(|| "https://clob.polymarket.com".to_string());
    let chain_id = resolve_runtime_var("PM_CHAIN_ID", dotenv_path)
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(PM_POLYGON);
    let private_key = resolve_runtime_var(PRIVATE_KEY_VAR, dotenv_path)
        .or_else(|| resolve_runtime_var("PRIVATE_KEY", dotenv_path))
        .ok_or_else(|| anyhow!("missing private key: set {PRIVATE_KEY_VAR} or PRIVATE_KEY"))?;
    let token_id_raw = resolve_runtime_var("PM_TOKEN_ID", dotenv_path)
        .ok_or_else(|| anyhow!("missing PM_TOKEN_ID in env/.env"))?;
    let price_raw = resolve_runtime_var("PM_ORDER_PRICE", dotenv_path)
        .unwrap_or_else(|| "0.50".to_string());
    let size_raw = resolve_runtime_var("PM_ORDER_SIZE", dotenv_path)
        .unwrap_or_else(|| "10".to_string());
    let side_raw = resolve_runtime_var("PM_ORDER_SIDE", dotenv_path)
        .unwrap_or_else(|| "buy".to_string());

    let token_id = PmU256::from_str(token_id_raw.trim())
        .with_context(|| format!("invalid PM_TOKEN_ID: {}", token_id_raw.trim()))?;
    let price = PmDecimal::from_str(price_raw.trim())
        .with_context(|| format!("invalid PM_ORDER_PRICE: {}", price_raw.trim()))?;
    let size = PmDecimal::from_str(size_raw.trim())
        .with_context(|| format!("invalid PM_ORDER_SIZE: {}", size_raw.trim()))?;
    let side = parse_pm_side(side_raw.trim())
        .ok_or_else(|| anyhow!("invalid PM_ORDER_SIDE: {} (expected buy/sell)", side_raw.trim()))?;
    let signer = PmLocalSigner::from_str(private_key.trim())
        .context("invalid private key format")?
        .with_chain_id(Some(chain_id));
    let pm_signature_type = resolve_pm_signature_type(dotenv_path)?;
    let pm_funder = resolve_pm_funder_address(dotenv_path, pm_signature_type, chain_id)?;
    let pm_funder_display = pm_funder
        .as_ref()
        .map(|v| v.to_string())
        .unwrap_or_else(|| "(auto)".to_string());
    let mut auth_builder = PmClobClient::new(&host, PmClobConfig::default())?
        .authentication_builder(&signer);
    if let Some(sig) = pm_signature_type {
        auth_builder = auth_builder.signature_type(sig);
    }
    if let Some(funder) = pm_funder {
        auth_builder = auth_builder.funder(funder);
    }
    let client = auth_builder.authenticate().await?;

    println!(
        "sdk-order: host={}, chain_id={}, token_id={}, side={:?}, price={}, size={}, pm_signature_type={}, pm_funder={}",
        host,
        chain_id,
        token_id,
        side,
        price,
        size,
        pm_signature_type
            .map(pm_signature_type_to_name)
            .unwrap_or("eoa"),
        pm_funder_display
    );

    let order = client
        .limit_order()
        .token_id(token_id)
        .price(price)
        .size(size)
        .side(side)
        .build()
        .await?;
    let signed_order = client.sign(&signer, order).await?;
    let response = client.post_order(signed_order).await?;

    println!("sdk-order: order_id={}", response.order_id);
    println!(
        "sdk-order: status={:?}, success={}, error_msg={:?}",
        response.status, response.success, response.error_msg
    );

    Ok(())
}

async fn run_sdk_auth_check(dotenv_path: &Path) -> anyhow::Result<()> {
    let host = resolve_runtime_var("PM_HOST", dotenv_path)
        .unwrap_or_else(|| "https://clob.polymarket.com".to_string());
    let chain_id = resolve_runtime_var("PM_CHAIN_ID", dotenv_path)
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(PM_POLYGON);
    let private_key = resolve_runtime_var(PRIVATE_KEY_VAR, dotenv_path)
        .or_else(|| resolve_runtime_var("PRIVATE_KEY", dotenv_path))
        .ok_or_else(|| anyhow!("missing private key: set {PRIVATE_KEY_VAR} or PRIVATE_KEY"))?;

    let signer = PmLocalSigner::from_str(private_key.trim())
        .context("invalid private key format")?
        .with_chain_id(Some(chain_id));
    let client = PmClobClient::new(&host, PmClobConfig::default())?;
    let credentials = client.create_or_derive_api_key(&signer, None).await?;
    let derived_key = credentials.key().to_string();
    let configured_key = resolve_runtime_var("POLYMARKET_API_KEY", dotenv_path)
        .or_else(|| resolve_runtime_var("API_KEY", dotenv_path));

    println!("sdk-auth-check: host={host}, chain_id={chain_id}");
    println!("sdk-auth-check: derived_api_key={derived_key}");
    if let Some(configured) = configured_key {
        let matches = configured.trim().eq_ignore_ascii_case(derived_key.as_str());
        println!("sdk-auth-check: configured_api_key={}", configured.trim());
        println!("sdk-auth-check: api_key_match={matches}");
        if !matches {
            return Err(anyhow!(
                "configured POLYMARKET_API_KEY does not match sdk-derived key"
            ));
        }
    } else {
        println!("sdk-auth-check: configured_api_key=(missing)");
    }
    Ok(())
}

async fn run_sdk_auth_export(dotenv_path: &Path) -> anyhow::Result<()> {
    let host = resolve_runtime_var("PM_HOST", dotenv_path)
        .unwrap_or_else(|| "https://clob.polymarket.com".to_string());
    let chain_id = resolve_runtime_var("PM_CHAIN_ID", dotenv_path)
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(PM_POLYGON);
    let private_key = resolve_runtime_var(PRIVATE_KEY_VAR, dotenv_path)
        .or_else(|| resolve_runtime_var("PRIVATE_KEY", dotenv_path))
        .ok_or_else(|| anyhow!("missing private key: set {PRIVATE_KEY_VAR} or PRIVATE_KEY"))?;

    let signer = PmLocalSigner::from_str(private_key.trim())
        .context("invalid private key format")?
        .with_chain_id(Some(chain_id));
    let client = PmClobClient::new(&host, PmClobConfig::default())?;
    let credentials = client.create_or_derive_api_key(&signer, None).await?;

    println!("# copy to src/.env");
    println!("POLYMARKET_API_KEY='{}'", credentials.key());
    println!(
        "POLYMARKET_SIGNATURE='{}'",
        credentials.secret().expose_secret()
    );
    println!(
        "POLYMARKET_PASSPHRASE='{}'",
        credentials.passphrase().expose_secret()
    );
    Ok(())
}

fn append_round_record(
    log_path: &Path,
    round: u64,
    market_slug: &str,
    side: String,
    entry_price: Decimal,
    exit_price: Decimal,
    size: Decimal,
    pnl: Decimal,
    exit_reason: &str,
) {
    if let Some(parent) = log_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let record = serde_json::json!({
        "ts": Utc::now().to_rfc3339(),
        "round": round,
        "market": market_slug,
        "side": side,
        "entry": entry_price.to_string(),
        "exit": exit_price.to_string(),
        "size": size.to_string(),
        "pnl": pnl.to_string(),
        "reason": exit_reason,
    });
    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
    {
        Ok(mut f) => {
            let _ = writeln!(f, "{}", record);
        }
        Err(e) => eprintln!("btc_candle: failed to write round log: {e}"),
    }
}

fn print_execution_report(report: &ExecutionReport) {
    println!(
        "order_report: market_id={}, status={:?}, action_count={}, order_ids={:?}",
        report.market_id, report.status, report.action_count, report.order_ids
    );
    for (idx, fill) in report.fills.iter().enumerate() {
        println!(
            "  fill #{:02}: order_id={:?}, fill_id={:?}, side={:?}, price={}, size={}, fee={}",
            idx + 1,
            fill.order_id,
            fill.fill_id,
            fill.side,
            fill.price,
            fill.size,
            fill.fee
        );
    }
}

fn read_fetch_interval_secs(dotenv_path: &Path, default_secs: u64) -> u64 {
    let Ok(raw) = std::fs::read_to_string(dotenv_path) else {
        return default_secs;
    };
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((k, v)) = trimmed.split_once('=') else {
            continue;
        };
        if k.trim() != "FETCH_INTERVAL" {
            continue;
        }
        if let Ok(sec) = v.trim().parse::<u64>() {
            return sec.max(1);
        }
    }
    default_secs
}

fn read_scan_max_sum(dotenv_path: &Path, default_max_sum: Decimal) -> Decimal {
    let Ok(raw) = std::fs::read_to_string(dotenv_path) else {
        return default_max_sum;
    };
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((k, v)) = trimmed.split_once('=') else {
            continue;
        };
        if k.trim() != "SCAN_MAX_SUM" {
            continue;
        }
        if let Ok(val) = Decimal::from_str(v.trim()) {
            return val;
        }
    }
    default_max_sum
}

fn read_scan_min_edge(dotenv_path: &Path, default_min_edge: Decimal) -> Decimal {
    let Ok(raw) = std::fs::read_to_string(dotenv_path) else {
        return default_min_edge;
    };
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((k, v)) = trimmed.split_once('=') else {
            continue;
        };
        if k.trim() != "SCAN_MIN_EDGE" {
            continue;
        }
        if let Ok(val) = Decimal::from_str(v.trim()) {
            return val;
        }
    }
    default_min_edge
}

fn read_scan_top_n(dotenv_path: &Path, default_top_n: usize) -> usize {
    let Ok(raw) = std::fs::read_to_string(dotenv_path) else {
        return default_top_n.max(1);
    };
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((k, v)) = trimmed.split_once('=') else {
            continue;
        };
        if k.trim() != "SCAN_TOP_N" {
            continue;
        }
        if let Ok(n) = v.trim().parse::<usize>() {
            return n.max(1);
        }
    }
    default_top_n.max(1)
}

#[derive(Clone, Debug)]
struct PreflightReport {
    live_mode: bool,
    trading_enabled: bool,
    total_capital: Option<Decimal>,
    capital_source: String,
}

fn run_startup_preflight(app_config: &AppConfig, dotenv_path: &Path) -> anyhow::Result<PreflightReport> {
    let live_mode = app_config.execution.polymarket_http_url.is_some();
    let trading_enabled = read_trading_enabled(dotenv_path, true);
    let mut total_capital = None;
    let mut capital_source = "n/a".to_string();

    if live_mode && trading_enabled {
        if let Some(api_key_env) = &app_config.execution.api_key_env {
            let val = resolve_runtime_var(api_key_env, dotenv_path);
            if val.as_deref().map(str::trim).unwrap_or_default().is_empty() {
                return Err(anyhow!(
                    "preflight failed: required api key env '{}' is missing/empty",
                    api_key_env
                ));
            }
        }
        if let Some(signature_env) = &app_config.execution.signature_env {
            let val = resolve_runtime_var(signature_env, dotenv_path);
            if val.as_deref().map(str::trim).unwrap_or_default().is_empty() {
                return Err(anyhow!(
                    "preflight failed: required signature env '{}' is missing/empty",
                    signature_env
                ));
            }
        }

        let (capital, source) = resolve_total_capital(dotenv_path)?;
        total_capital = Some(capital);
        capital_source = source;

        let min_total_capital = read_decimal_var(dotenv_path, "MIN_TOTAL_CAPITAL_USD")
            .unwrap_or_else(|| Decimal::ONE);
        if capital < min_total_capital {
            return Err(anyhow!(
                "preflight failed: capital={} below MIN_TOTAL_CAPITAL_USD={}",
                capital,
                min_total_capital
            ));
        }

        if app_config.strategies.arbitrage.enabled {
            let arb = app_config.arbitrage_config()?;
            let min_order_size = read_decimal_var(dotenv_path, "MIN_ORDER_SIZE_USD")
                .unwrap_or_else(|| Decimal::ONE);
            if arb.order_size < min_order_size {
                return Err(anyhow!(
                    "preflight failed: arbitrage order_size={} < MIN_ORDER_SIZE_USD={}",
                    arb.order_size,
                    min_order_size
                ));
            }
        }
    }

    Ok(PreflightReport {
        live_mode,
        trading_enabled,
        total_capital,
        capital_source,
    })
}

fn print_preflight_report(preflight: &PreflightReport) {
    println!(
        "preflight: live_mode={}, trading_enabled={}",
        preflight.live_mode, preflight.trading_enabled
    );
    if let Some(capital) = preflight.total_capital {
        println!(
            "preflight: total_capital={} source={}",
            capital, preflight.capital_source
        );
    }
}

fn resolve_total_capital(dotenv_path: &Path) -> anyhow::Result<(Decimal, String)> {
    if let Some(raw) = resolve_runtime_var("TOTAL_CAPITAL_USD", dotenv_path) {
        let parsed = Decimal::from_str(raw.trim())
            .with_context(|| format!("invalid TOTAL_CAPITAL_USD value: {}", raw.trim()))?;
        if parsed <= Decimal::ZERO {
            return Err(anyhow!("TOTAL_CAPITAL_USD must be > 0"));
        }
        return Ok((parsed, "TOTAL_CAPITAL_USD".to_string()));
    }
    if let Some(cmd) = resolve_runtime_var("TOTAL_CAPITAL_CMD", dotenv_path) {
        let output = if cfg!(target_os = "windows") {
            Command::new("powershell")
                .args(["-NoProfile", "-Command", cmd.trim()])
                .output()
        } else {
            Command::new("sh").args(["-lc", cmd.trim()]).output()
        }
        .context("failed to execute TOTAL_CAPITAL_CMD")?;
        if !output.status.success() {
            return Err(anyhow!(
                "TOTAL_CAPITAL_CMD failed with status {}",
                output.status
            ));
        }
        let stdout = String::from_utf8(output.stdout).context("TOTAL_CAPITAL_CMD stdout is not utf-8")?;
        let line = stdout.lines().next().unwrap_or("").trim();
        if line.is_empty() {
            return Err(anyhow!("TOTAL_CAPITAL_CMD returned empty output"));
        }
        let parsed = Decimal::from_str(line)
            .with_context(|| format!("TOTAL_CAPITAL_CMD output is not decimal: '{line}'"))?;
        if parsed <= Decimal::ZERO {
            return Err(anyhow!("TOTAL_CAPITAL_CMD must output a value > 0"));
        }
        return Ok((parsed, "TOTAL_CAPITAL_CMD".to_string()));
    }
    Err(anyhow!(
        "missing capital source: set TOTAL_CAPITAL_USD or TOTAL_CAPITAL_CMD"
    ))
}

fn read_trading_enabled(dotenv_path: &Path, default_value: bool) -> bool {
    let Some(raw) = resolve_runtime_var("TRADING_ENABLED", dotenv_path) else {
        return default_value;
    };
    parse_bool_flag(raw.trim()).unwrap_or(default_value)
}

fn parse_bool_flag(raw: &str) -> Option<bool> {
    match raw.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn parse_pm_side(raw: &str) -> Option<PmSide> {
    match raw.to_ascii_lowercase().as_str() {
        "buy" | "bid" | "b" => Some(PmSide::Buy),
        "sell" | "ask" | "s" => Some(PmSide::Sell),
        _ => None,
    }
}

fn read_pm_chain_id(dotenv_path: &Path, default_chain_id: u64) -> u64 {
    resolve_runtime_var("PM_CHAIN_ID", dotenv_path)
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(default_chain_id)
}

fn resolve_pm_signature_type(dotenv_path: &Path) -> anyhow::Result<Option<PmSignatureType>> {
    let Some(raw) = resolve_runtime_var("PM_SIGNATURE_TYPE", dotenv_path) else {
        return Ok(None);
    };
    let normalized = raw.trim().to_ascii_lowercase();
    let value = match normalized.as_str() {
        "" => return Ok(None),
        "0" | "eoa" => PmSignatureType::Eoa,
        "1" | "proxy" => PmSignatureType::Proxy,
        "2" | "gnosis_safe" | "gnosissafe" | "safe" => PmSignatureType::GnosisSafe,
        _ => {
            return Err(anyhow!(
                "invalid PM_SIGNATURE_TYPE='{}' (supported: eoa|proxy|gnosis_safe or 0|1|2)",
                raw.trim()
            ));
        }
    };
    Ok(Some(value))
}

fn pm_signature_type_to_name(signature_type: PmSignatureType) -> &'static str {
    match signature_type {
        PmSignatureType::Eoa => "eoa",
        PmSignatureType::Proxy => "proxy",
        PmSignatureType::GnosisSafe => "gnosis_safe",
        _ => "unknown",
    }
}

fn resolve_pm_funder_address(
    dotenv_path: &Path,
    signature_type: Option<PmSignatureType>,
    chain_id: u64,
) -> anyhow::Result<Option<PmAddress>> {
    if let Some(raw) = resolve_runtime_var("PM_FUNDER", dotenv_path) {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            let parsed = PmAddress::from_str(trimmed)
                .with_context(|| format!("invalid PM_FUNDER address: {trimmed}"))?;
            return Ok(Some(parsed));
        }
    }
    let derived = match signature_type {
        Some(PmSignatureType::Proxy) => derive_pm_funder_from_private_key(dotenv_path, chain_id, true)?,
        Some(PmSignatureType::GnosisSafe) => {
            derive_pm_funder_from_private_key(dotenv_path, chain_id, false)?
        }
        _ => None,
    };
    Ok(derived)
}

fn derive_pm_funder_from_private_key(
    dotenv_path: &Path,
    chain_id: u64,
    is_proxy: bool,
) -> anyhow::Result<Option<PmAddress>> {
    let Some(private_key) = resolve_runtime_var(PRIVATE_KEY_VAR, dotenv_path)
        .or_else(|| resolve_runtime_var("PRIVATE_KEY", dotenv_path))
    else {
        return Ok(None);
    };
    let signer = PmLocalSigner::from_str(private_key.trim())
        .context("invalid private key format for PM_FUNDER derivation")?
        .with_chain_id(Some(chain_id));
    let derived = if is_proxy {
        derive_proxy_wallet(signer.address(), chain_id)
    } else {
        derive_safe_wallet(signer.address(), chain_id)
    };
    Ok(derived)
}

fn read_decimal_var(dotenv_path: &Path, key: &str) -> Option<Decimal> {
    resolve_runtime_var(key, dotenv_path).and_then(|v| Decimal::from_str(v.trim()).ok())
}

fn resolve_runtime_var(key: &str, dotenv_path: &Path) -> Option<String> {
    std::env::var(key)
        .ok()
        .or_else(|| load_dotenv_map(dotenv_path).get(key).cloned())
}

fn load_dotenv_map(path: &Path) -> HashMap<String, String> {
    let Ok(raw) = std::fs::read_to_string(path) else {
        return HashMap::new();
    };
    let mut out = HashMap::new();
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((k, v)) = trimmed.split_once('=') else {
            continue;
        };
        let key = k.trim().to_string();
        if key.is_empty() {
            continue;
        }
        let mut val = v.trim().to_string();
        if (val.starts_with('"') && val.ends_with('"'))
            || (val.starts_with('\'') && val.ends_with('\''))
        {
            val = val[1..val.len() - 1].to_string();
        } else if let Some((head, _)) = val.split_once('#') {
            val = head.trim().to_string();
        }
        out.insert(key, val);
    }
    out
}
